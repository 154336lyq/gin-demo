package balance

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

// --- account_balances ---

func (s *Store) Upsert(ctx context.Context, address, token, balanceWei, sourceTx string, blockNum uint64) error {
	now := time.Now().UTC()
	var src any
	if sourceTx != "" {
		src = strings.ToLower(sourceTx)
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO account_balances (
			chain_id, address, token_address, balance_wei, block_number, source_tx_hash, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			balance_wei = VALUES(balance_wei),
			block_number = VALUES(block_number),
			source_tx_hash = VALUES(source_tx_hash),
			updated_at = VALUES(updated_at)
	`, s.chainID, strings.ToLower(address), normToken(token), balanceWei, blockNum, src, now)
	return err
}

func (s *Store) Get(ctx context.Context, address, token string) (Row, error) {
	var r Row
	var src sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT chain_id, address, token_address, balance_wei, block_number, source_tx_hash, updated_at
		FROM account_balances
		WHERE chain_id = ? AND address = ? AND token_address = ?
	`, s.chainID, strings.ToLower(address), normToken(token)).Scan(
		&r.ChainID, &r.Address, &r.TokenAddress, &r.BalanceWei, &r.BlockNumber, &src, &r.UpdatedAt,
	)
	if src.Valid {
		r.SourceTxHash = src.String
	}
	return r, err
}

func (s *Store) ListByAddress(ctx context.Context, address string) ([]Row, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT chain_id, address, token_address, balance_wei, block_number, source_tx_hash, updated_at
		FROM account_balances
		WHERE chain_id = ? AND address = ?
		ORDER BY token_address ASC
	`, s.chainID, strings.ToLower(address))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Row
	for rows.Next() {
		var r Row
		var src sql.NullString
		if err := rows.Scan(
			&r.ChainID, &r.Address, &r.TokenAddress, &r.BalanceWei, &r.BlockNumber, &src, &r.UpdatedAt,
		); err != nil {
			return nil, err
		}
		if src.Valid {
			r.SourceTxHash = src.String
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// --- custodial_wallets ---

// RegisterWalletResult 注册结果：区分新建与更新。
type RegisterWalletResult struct {
	Wallet  CustodialWallet
	Created bool
}

// RegisterWallet 注册托管地址。已存在时仅允许更新 label 或 re-enable，禁止变更 user_id / wallet_type。
func (s *Store) RegisterWallet(ctx context.Context, p RegisterWalletParams) (RegisterWalletResult, error) {
	addr := strings.ToLower(strings.TrimSpace(p.Address))
	wt := strings.TrimSpace(p.WalletType)
	if wt == "" {
		wt = WalletTypeDeposit
	}
	userID := strings.TrimSpace(p.UserID)
	label := strings.TrimSpace(p.Label)

	if wt == WalletTypeDeposit && userID == "" {
		return RegisterWalletResult{}, ErrDepositRequiresUserID
	}

	existing, err := s.GetWallet(ctx, addr)
	if err != nil && err != sql.ErrNoRows {
		return RegisterWalletResult{}, err
	}

	now := time.Now().UTC()
	if err == sql.ErrNoRows {
		_, err = s.db.ExecContext(ctx, `
			INSERT INTO custodial_wallets (chain_id, address, user_id, label, wallet_type, enabled, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, 1, ?, ?)
		`, s.chainID, addr, nullStr(userID), nullStr(label), wt, now, now)
		if err != nil {
			return RegisterWalletResult{}, err
		}
		w, err := s.GetWallet(ctx, addr)
		return RegisterWalletResult{Wallet: w, Created: true}, err
	}

	// 已存在：immutable 字段校验
	if existing.UserID != "" && userID != "" && !strings.EqualFold(existing.UserID, userID) {
		return RegisterWalletResult{}, ErrWalletOwnerConflict
	}
	if wt != existing.WalletType {
		return RegisterWalletResult{}, ErrWalletTypeImmutable
	}
	if existing.WalletType == WalletTypeDeposit && existing.UserID == "" && userID == "" {
		return RegisterWalletResult{}, ErrDepositRequiresUserID
	}

	// 保留原 user_id（允许本次请求省略 user_id 仅更新 label）
	keepUser := existing.UserID
	if userID != "" {
		keepUser = userID
	}
	keepLabel := existing.Label
	if label != "" {
		keepLabel = label
	}

	_, err = s.db.ExecContext(ctx, `
		UPDATE custodial_wallets
		SET user_id = ?, label = ?, enabled = 1, updated_at = ?
		WHERE chain_id = ? AND address = ?
	`, nullStr(keepUser), nullStr(keepLabel), now, s.chainID, addr)
	if err != nil {
		return RegisterWalletResult{}, err
	}
	w, err := s.GetWallet(ctx, addr)
	return RegisterWalletResult{Wallet: w, Created: false}, err
}

func (s *Store) GetWallet(ctx context.Context, address string) (CustodialWallet, error) {
	var w CustodialWallet
	var userID, label sql.NullString
	var enabled int
	err := s.db.QueryRowContext(ctx, `
		SELECT chain_id, address, user_id, label, wallet_type, enabled, created_at, updated_at
		FROM custodial_wallets WHERE chain_id = ? AND address = ?
	`, s.chainID, strings.ToLower(address)).Scan(
		&w.ChainID, &w.Address, &userID, &label, &w.WalletType, &enabled, &w.CreatedAt, &w.UpdatedAt,
	)
	if userID.Valid {
		w.UserID = userID.String
	}
	if label.Valid {
		w.Label = label.String
	}
	w.Enabled = enabled == 1
	return w, err
}

func (s *Store) ListWallets(ctx context.Context, userID, walletType string, enabledOnly bool, limit int) ([]CustodialWallet, error) {
	q := `SELECT chain_id, address, user_id, label, wallet_type, enabled, created_at, updated_at
		FROM custodial_wallets WHERE chain_id = ?`
	args := []any{s.chainID}
	if userID != "" {
		q += ` AND user_id = ?`
		args = append(args, userID)
	}
	if walletType != "" {
		q += ` AND wallet_type = ?`
		args = append(args, walletType)
	}
	if enabledOnly {
		q += ` AND enabled = 1`
	}
	q += ` ORDER BY created_at DESC`
	if limit > 0 {
		if limit > 500 {
			limit = 500
		}
		q += ` LIMIT ?`
		args = append(args, limit)
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanWallets(rows)
}

func (s *Store) ListEnabledAddresses(ctx context.Context, limit int) ([]string, error) {
	q := `SELECT address FROM custodial_wallets WHERE chain_id = ? AND enabled = 1 ORDER BY address ASC`
	args := []any{s.chainID}
	if limit > 0 {
		q += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			return nil, err
		}
		out = append(out, addr)
	}
	return out, rows.Err()
}

func (s *Store) SetWalletEnabled(ctx context.Context, address string, enabled bool) error {
	now := time.Now().UTC()
	en := 0
	if enabled {
		en = 1
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE custodial_wallets SET enabled = ?, updated_at = ? WHERE chain_id = ? AND address = ?
	`, en, now, s.chainID, strings.ToLower(address))
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrWalletNotFound
	}
	return nil
}

func scanWallets(rows *sql.Rows) ([]CustodialWallet, error) {
	var out []CustodialWallet
	for rows.Next() {
		var w CustodialWallet
		var userID, label sql.NullString
		var enabled int
		if err := rows.Scan(
			&w.ChainID, &w.Address, &userID, &label, &w.WalletType, &enabled, &w.CreatedAt, &w.UpdatedAt,
		); err != nil {
			return nil, err
		}
		if userID.Valid {
			w.UserID = userID.String
		}
		if label.Valid {
			w.Label = label.String
		}
		w.Enabled = enabled == 1
		out = append(out, w)
	}
	return out, rows.Err()
}

func normToken(token string) string {
	return strings.ToLower(strings.TrimSpace(token))
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
