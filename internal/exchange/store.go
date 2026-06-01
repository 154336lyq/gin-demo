package exchange

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

type Store struct {
	db      *sql.DB
	chainID int64
}

func NewStore(ctx context.Context, db *sql.DB, chainID int64) (*Store, error) {
	if err := EnsureSchema(ctx, db); err != nil {
		return nil, err
	}
	return &Store{db: db, chainID: chainID}, nil
}

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) BeginTx(ctx context.Context) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, nil)
}

func normToken(token string) string {
	return strings.ToLower(strings.TrimSpace(token))
}

func (s *Store) GetBalance(ctx context.Context, userID, token string) (AccountBalance, error) {
	var b AccountBalance
	err := s.db.QueryRowContext(ctx, `
		SELECT chain_id, user_id, token_address, available_wei, frozen_wei, updated_at
		FROM user_ledger_accounts
		WHERE chain_id = ? AND user_id = ? AND token_address = ?
	`, s.chainID, userID, normToken(token)).Scan(
		&b.ChainID, &b.UserID, &b.TokenAddress, &b.AvailableWei, &b.FrozenWei, &b.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return AccountBalance{
			ChainID: s.chainID, UserID: userID, TokenAddress: normToken(token),
			AvailableWei: "0", FrozenWei: "0", UpdatedAt: time.Now().UTC(),
		}, nil
	}
	return b, err
}

func (s *Store) ListBalances(ctx context.Context, userID string) ([]AccountBalance, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT chain_id, user_id, token_address, available_wei, frozen_wei, updated_at
		FROM user_ledger_accounts WHERE chain_id = ? AND user_id = ?
		ORDER BY token_address ASC
	`, s.chainID, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AccountBalance
	for rows.Next() {
		var b AccountBalance
		if err := rows.Scan(&b.ChainID, &b.UserID, &b.TokenAddress, &b.AvailableWei, &b.FrozenWei, &b.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *Store) ListEntries(ctx context.Context, userID string, limit int) ([]LedgerEntry, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, chain_id, user_id, token_address, entry_type, amount_wei,
			ref_type, ref_id, balance_available_after, balance_frozen_after, created_at
		FROM ledger_entries
		WHERE chain_id = ? AND user_id = ?
		ORDER BY id DESC LIMIT ?
	`, s.chainID, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEntries(rows)
}

func (s *Store) InsertDepositTx(ctx context.Context, tx *sql.Tx, p CaptureDepositParams) (int64, error) {
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `
		INSERT INTO deposits (
			chain_id, user_id, deposit_address, token_address, amount_wei,
			tx_hash, log_index, block_number, status, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, s.chainID, p.UserID, strings.ToLower(p.DepositAddress), normToken(p.TokenAddress),
		p.AmountWei, strings.ToLower(p.TxHash), p.LogIndex, p.BlockNumber, DepositStatusPending, now)
	if err != nil {
		if isDup(err) {
			return 0, nil
		}
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) MarkDepositCreditedTx(ctx context.Context, tx *sql.Tx, depositID int64) error {
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `
		UPDATE deposits SET status = ?, credited_at = ?
		WHERE id = ? AND chain_id = ? AND status IN (?, ?)
	`, DepositStatusCredited, now, depositID, s.chainID, DepositStatusPending, DepositStatusOrphaned)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		var status string
		err := tx.QueryRowContext(ctx, `SELECT status FROM deposits WHERE id = ? AND chain_id = ?`, depositID, s.chainID).Scan(&status)
		if err == nil && status == DepositStatusCredited {
			return nil
		}
	}
	return nil
}

func (s *Store) resetDepositForRecreditTx(ctx context.Context, tx *sql.Tx, depositID int64, blockNum uint64) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE deposits SET status = ?, block_number = ?, credited_at = NULL
		WHERE id = ? AND chain_id = ? AND status = ?
	`, DepositStatusPending, blockNum, depositID, s.chainID, DepositStatusOrphaned)
	return err
}

func (s *Store) hasLedgerEntryTx(ctx context.Context, tx *sql.Tx, depositID int64, entryType string) (bool, error) {
	var id int64
	err := tx.QueryRowContext(ctx, `
		SELECT id FROM ledger_entries
		WHERE chain_id = ? AND ref_type = 'deposit' AND ref_id = ? AND entry_type = ?
		LIMIT 1
	`, s.chainID, depositID, entryType).Scan(&id)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

func (s *Store) GetDepositByKey(ctx context.Context, txHash string, logIndex uint) (Deposit, error) {
	return s.getDepositByKey(ctx, s.db, txHash, logIndex, false)
}

func (s *Store) GetDepositByKeyTx(ctx context.Context, dbTx *sql.Tx, txHash string, logIndex uint) (Deposit, error) {
	return s.getDepositByKey(ctx, dbTx, txHash, logIndex, true)
}

func (s *Store) getDepositByKey(ctx context.Context, q querier, txHash string, logIndex uint, forUpdate bool) (Deposit, error) {
	var d Deposit
	var credited sql.NullTime
	sqlStr := `
		SELECT id, chain_id, user_id, deposit_address, token_address, amount_wei,
			tx_hash, log_index, block_number, status, credited_at, created_at
		FROM deposits WHERE chain_id = ? AND tx_hash = ? AND log_index = ?`
	if forUpdate {
		sqlStr += ` FOR UPDATE`
	}
	err := q.QueryRowContext(ctx, sqlStr, s.chainID, strings.ToLower(txHash), logIndex).Scan(
		&d.ID, &d.ChainID, &d.UserID, &d.DepositAddress, &d.TokenAddress, &d.AmountWei,
		&d.TxHash, &d.LogIndex, &d.BlockNumber, &d.Status, &credited, &d.CreatedAt,
	)
	if credited.Valid {
		d.CreditedAt = &credited.Time
	}
	return d, err
}

type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *Store) ListDeposits(ctx context.Context, userID string, limit int) ([]Deposit, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	q := `SELECT id, chain_id, user_id, deposit_address, token_address, amount_wei,
		tx_hash, log_index, block_number, status, credited_at, created_at
		FROM deposits WHERE chain_id = ?`
	args := []any{s.chainID}
	if userID != "" {
		q += ` AND user_id = ?`
		args = append(args, userID)
	}
	q += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDeposits(rows)
}

func (s *Store) OrphanDepositsFromBlockTx(ctx context.Context, tx *sql.Tx, fromBlock uint64) ([]Deposit, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, chain_id, user_id, deposit_address, token_address, amount_wei,
			tx_hash, log_index, block_number, status, credited_at, created_at
		FROM deposits
		WHERE chain_id = ? AND block_number >= ? AND status IN (?, ?)
	`, s.chainID, fromBlock, DepositStatusPending, DepositStatusCredited)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out, err := scanDeposits(rows)
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `
		UPDATE deposits SET status = ?
		WHERE chain_id = ? AND block_number >= ? AND status IN (?, ?)
	`, DepositStatusOrphaned, s.chainID, fromBlock, DepositStatusPending, DepositStatusCredited)
	return out, err
}

func (s *Store) InsertWithdraw(ctx context.Context, p CreateWithdrawParams, status string) (WithdrawRequest, error) {
	now := time.Now().UTC()
	var idem any
	if p.IdempotencyKey != "" {
		idem = p.IdempotencyKey
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO withdraw_requests (
			chain_id, user_id, token_address, to_address, amount_wei, status,
			idempotency_key, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, s.chainID, p.UserID, normToken(p.TokenAddress), strings.ToLower(p.ToAddress),
		p.AmountWei, status, idem, now, now)
	if err != nil {
		if isDup(err) {
			return s.GetWithdrawByIdempotency(ctx, p.IdempotencyKey)
		}
		return WithdrawRequest{}, err
	}
	id, _ := res.LastInsertId()
	return s.GetWithdraw(ctx, id)
}

func (s *Store) GetWithdrawByIdempotency(ctx context.Context, key string) (WithdrawRequest, error) {
	var w WithdrawRequest
	err := scanWithdrawRow(s.db.QueryRowContext(ctx, withdrawSelect+` AND idempotency_key = ?`, s.chainID, key), &w)
	return w, err
}

func (s *Store) GetWithdraw(ctx context.Context, id int64) (WithdrawRequest, error) {
	var w WithdrawRequest
	err := scanWithdrawRow(s.db.QueryRowContext(ctx, withdrawSelect+` AND id = ?`, s.chainID, id), &w)
	return w, err
}

func (s *Store) GetWithdrawByTxHash(ctx context.Context, txHash string) (WithdrawRequest, error) {
	var w WithdrawRequest
	err := scanWithdrawRow(s.db.QueryRowContext(ctx, withdrawSelect+` AND tx_hash = ?`, s.chainID, strings.ToLower(txHash)), &w)
	return w, err
}

const withdrawSelect = `
	SELECT id, chain_id, user_id, token_address, to_address, amount_wei, status,
		from_wallet, tx_hash, reviewer, reviewed_at, reject_reason, idempotency_key, created_at, updated_at
	FROM withdraw_requests WHERE chain_id = ?`

func scanWithdrawRow(row *sql.Row, w *WithdrawRequest) error {
	var fromWallet, txHash, reviewer, reject, idem sql.NullString
	var reviewed sql.NullTime
	err := row.Scan(
		&w.ID, &w.ChainID, &w.UserID, &w.TokenAddress, &w.ToAddress, &w.AmountWei, &w.Status,
		&fromWallet, &txHash, &reviewer, &reviewed, &reject, &idem, &w.CreatedAt, &w.UpdatedAt,
	)
	if fromWallet.Valid {
		w.FromWallet = fromWallet.String
	}
	if txHash.Valid {
		w.TxHash = txHash.String
	}
	if reviewer.Valid {
		w.Reviewer = reviewer.String
	}
	if reviewed.Valid {
		w.ReviewedAt = &reviewed.Time
	}
	if reject.Valid {
		w.RejectReason = reject.String
	}
	if idem.Valid {
		w.IdempotencyKey = idem.String
	}
	return err
}

func scanWithdrawRows(rows *sql.Rows) ([]WithdrawRequest, error) {
	var out []WithdrawRequest
	for rows.Next() {
		var w WithdrawRequest
		var fromWallet, txHash, reviewer, reject, idem sql.NullString
		var reviewed sql.NullTime
		if err := rows.Scan(
			&w.ID, &w.ChainID, &w.UserID, &w.TokenAddress, &w.ToAddress, &w.AmountWei, &w.Status,
			&fromWallet, &txHash, &reviewer, &reviewed, &reject, &idem, &w.CreatedAt, &w.UpdatedAt,
		); err != nil {
			return nil, err
		}
		if fromWallet.Valid {
			w.FromWallet = fromWallet.String
		}
		if txHash.Valid {
			w.TxHash = txHash.String
		}
		if reviewer.Valid {
			w.Reviewer = reviewer.String
		}
		if reviewed.Valid {
			w.ReviewedAt = &reviewed.Time
		}
		if reject.Valid {
			w.RejectReason = reject.String
		}
		if idem.Valid {
			w.IdempotencyKey = idem.String
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

func (s *Store) ListWithdraws(ctx context.Context, userID, status string, limit int) ([]WithdrawRequest, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	q := withdrawSelect
	args := []any{s.chainID}
	if userID != "" {
		q += ` AND user_id = ?`
		args = append(args, userID)
	}
	if status != "" {
		q += ` AND status = ?`
		args = append(args, status)
	}
	q += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanWithdrawRows(rows)
}

func (s *Store) UpdateWithdrawStatus(ctx context.Context, id int64, status string, fields map[string]any) error {
	return s.updateWithdrawStatus(ctx, s.db, id, status, "", fields)
}

// UpdateWithdrawStatusIf 条件更新，返回是否命中（用于审核/抢占广播）。
func (s *Store) UpdateWithdrawStatusIf(ctx context.Context, id int64, expectStatus, newStatus string, fields map[string]any) (bool, error) {
	return s.updateWithdrawStatusIf(ctx, s.db, id, expectStatus, newStatus, fields)
}

func (s *Store) updateWithdrawStatus(ctx context.Context, q execer, id int64, status, expectStatus string, fields map[string]any) error {
	ok, err := s.updateWithdrawStatusIf(ctx, q, id, expectStatus, status, fields)
	if err != nil {
		return err
	}
	if expectStatus != "" && !ok {
		return ErrInvalidWithdrawState
	}
	return nil
}

func (s *Store) updateWithdrawStatusIf(ctx context.Context, q execer, id int64, expectStatus, newStatus string, fields map[string]any) (bool, error) {
	now := time.Now().UTC()
	set := "status = ?, updated_at = ?"
	args := []any{newStatus, now}
	for k, v := range fields {
		set += ", " + k + " = ?"
		args = append(args, v)
	}
	qstr := `UPDATE withdraw_requests SET ` + set + ` WHERE chain_id = ? AND id = ?`
	args = append(args, s.chainID, id)
	if expectStatus != "" {
		qstr += ` AND status = ?`
		args = append(args, expectStatus)
	}
	res, err := q.ExecContext(ctx, qstr, args...)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (s *Store) GetWithdrawByIdempotencyTx(ctx context.Context, dbTx *sql.Tx, key string) (WithdrawRequest, error) {
	var w WithdrawRequest
	err := scanWithdrawRow(dbTx.QueryRowContext(ctx, withdrawSelect+` AND idempotency_key = ?`, s.chainID, key), &w)
	return w, err
}

// FailWithdrawAndUnfreezeTx 广播失败：解冻并标记 failed。
func (s *Store) FailWithdrawAndUnfreezeTx(ctx context.Context, dbTx *sql.Tx, w WithdrawRequest, reason string) error {
	hasUnfreeze, err := s.hasLedgerEntryTx(ctx, dbTx, w.ID, LedgerWithdrawUnfreeze)
	if err != nil {
		return err
	}
	if !hasUnfreeze {
		hasFreeze, err := s.hasLedgerEntryTx(ctx, dbTx, w.ID, LedgerWithdrawFreeze)
		if err != nil {
			return err
		}
		if hasFreeze {
			if err := s.unfreezeWithdrawTx(ctx, dbTx, w.UserID, w.TokenAddress, w.AmountWei, w.ID); err != nil {
				return err
			}
		}
	}
	now := time.Now().UTC()
	_, err = dbTx.ExecContext(ctx, `
		UPDATE withdraw_requests SET status = ?, reject_reason = ?, updated_at = ?
		WHERE chain_id = ? AND id = ?
	`, WithdrawStatusFailed, reason, now, s.chainID, w.ID)
	return err
}

func (s *Store) SumUserLiabilities(ctx context.Context, token string) (string, error) {
	var sum sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(CAST(available_wei AS DECIMAL(65,0)) + CAST(frozen_wei AS DECIMAL(65,0))), 0)
		FROM user_ledger_accounts
		WHERE chain_id = ? AND token_address = ?
	`, s.chainID, normToken(token)).Scan(&sum)
	if err != nil {
		return "0", err
	}
	if !sum.Valid || sum.String == "" {
		return "0", nil
	}
	return sum.String, nil
}

func scanDeposits(rows *sql.Rows) ([]Deposit, error) {
	var out []Deposit
	for rows.Next() {
		var d Deposit
		var credited sql.NullTime
		if err := rows.Scan(
			&d.ID, &d.ChainID, &d.UserID, &d.DepositAddress, &d.TokenAddress, &d.AmountWei,
			&d.TxHash, &d.LogIndex, &d.BlockNumber, &d.Status, &credited, &d.CreatedAt,
		); err != nil {
			return nil, err
		}
		if credited.Valid {
			d.CreditedAt = &credited.Time
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func scanEntries(rows *sql.Rows) ([]LedgerEntry, error) {
	var out []LedgerEntry
	for rows.Next() {
		var e LedgerEntry
		var refType sql.NullString
		var refID sql.NullInt64
		if err := rows.Scan(
			&e.ID, &e.ChainID, &e.UserID, &e.TokenAddress, &e.EntryType, &e.AmountWei,
			&refType, &refID, &e.BalanceAvailableAfter, &e.BalanceFrozenAfter, &e.CreatedAt,
		); err != nil {
			return nil, err
		}
		if refType.Valid {
			e.RefType = refType.String
		}
		if refID.Valid {
			e.RefID = refID.Int64
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func isDup(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Duplicate") || strings.Contains(err.Error(), "1062")
}
