package tx

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"
)

const txSelectCols = `chain_id, tx_hash, from_addr, to_addr, token_addr, value_wei, nonce, gas_limit,
	gas_price_wei, max_fee_per_gas_wei, max_priority_fee_wei, tx_format, tx_type, status,
	block_number, gas_used, confirmations, error_message, biz_id, biz_type, idempotency_key,
	replaces_hash, replaced_by_hash, signed_raw_hex, broadcast_retry_count, created_at, updated_at`

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

// InsertSubmitting 先落库再广播：status=submitting，保存 signed_raw_hex 供 reconcile 重试。
func (s *Store) InsertSubmitting(ctx context.Context, p InsertParams) error {
	status := p.Status
	if status == "" {
		status = StatusSubmitting
	}
	return s.insertRow(ctx, p, status)
}

func (s *Store) insertRow(ctx context.Context, p InsertParams, status string) error {
	now := time.Now().UTC()
	txType := p.TxType
	if txType == "" {
		txType = TxTypeNative
	}
	txFormat := p.TxFormat
	if txFormat == "" {
		txFormat = TxFormatLegacy
	}
	var idempotency any
	if p.IdempotencyKey != "" {
		idempotency = strings.ToLower(p.IdempotencyKey)
	}
	var token, replaces, signedRaw any
	if p.TokenAddr != "" {
		token = strings.ToLower(p.TokenAddr)
	}
	if p.ReplacesHash != "" {
		replaces = strings.ToLower(p.ReplacesHash)
	}
	if p.SignedRawHex != "" {
		signedRaw = p.SignedRawHex
	}
	var gasPrice, maxFee, maxPrio any
	if p.GasPriceWei != "" {
		gasPrice = p.GasPriceWei
	}
	if p.MaxFeePerGasWei != "" {
		maxFee = p.MaxFeePerGasWei
	}
	if p.MaxPriorityFeeWei != "" {
		maxPrio = p.MaxPriorityFeeWei
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO tx_tracker (
			chain_id, tx_hash, from_addr, to_addr, token_addr, value_wei, nonce, gas_limit,
			gas_price_wei, max_fee_per_gas_wei, max_priority_fee_wei, tx_format, tx_type, status,
			biz_id, biz_type, idempotency_key, replaces_hash, signed_raw_hex, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			status = IF(status IN ('confirmed','failed','dropped','replaced'), status, VALUES(status)),
			signed_raw_hex = IF(VALUES(signed_raw_hex) IS NOT NULL AND VALUES(signed_raw_hex) != '', VALUES(signed_raw_hex), signed_raw_hex),
			updated_at = VALUES(updated_at)
	`, s.chainID, strings.ToLower(p.TxHash), strings.ToLower(p.FromAddr), strings.ToLower(p.ToAddr),
		token, p.ValueWei, p.Nonce, p.GasLimit, gasPrice, maxFee, maxPrio, txFormat, txType, status,
		nullStr(p.BizID), nullStr(p.BizType), idempotency, replaces, signedRaw, now, now)
	return err
}

func (s *Store) MarkBroadcastPending(ctx context.Context, hash string) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
		UPDATE tx_tracker SET status = ?, error_message = NULL, updated_at = ?
		WHERE chain_id = ? AND tx_hash = ? AND status IN (?, ?)
	`, StatusPending, now, s.chainID, strings.ToLower(hash), StatusSubmitting, StatusBroadcastFailed)
	return err
}

func (s *Store) MarkBroadcastFailed(ctx context.Context, hash, errMsg string) error {
	now := time.Now().UTC()
	var em any
	if errMsg != "" {
		em = errMsg
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE tx_tracker SET status = ?, error_message = ?, updated_at = ?
		WHERE chain_id = ? AND tx_hash = ? AND status IN (?, ?)
	`, StatusBroadcastFailed, em, now, s.chainID, strings.ToLower(hash), StatusSubmitting, StatusBroadcastFailed)
	return err
}

func (s *Store) IncrementBroadcastRetry(ctx context.Context, hash string) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
		UPDATE tx_tracker SET broadcast_retry_count = broadcast_retry_count + 1, updated_at = ?
		WHERE chain_id = ? AND tx_hash = ?
	`, now, s.chainID, strings.ToLower(hash))
	return err
}

func (s *Store) ListReconcileBatch(ctx context.Context, graceSec, maxRetries, limit int) ([]Row, error) {
	q := `SELECT ` + txSelectCols + ` FROM tx_tracker
		WHERE chain_id = ? AND status IN (?, ?)
		AND broadcast_retry_count < ?
		AND updated_at <= DATE_SUB(UTC_TIMESTAMP(3), INTERVAL ? SECOND)
		ORDER BY updated_at ASC LIMIT ?`
	rows, err := s.db.QueryContext(ctx, q, s.chainID, StatusSubmitting, StatusBroadcastFailed, maxRetries, graceSec, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

func (s *Store) GetByHash(ctx context.Context, hash string) (Row, error) {
	q := `SELECT ` + txSelectCols + ` FROM tx_tracker WHERE chain_id = ? AND tx_hash = ?`
	return scanOne(s.db.QueryRowContext(ctx, q, s.chainID, strings.ToLower(hash)))
}

func (s *Store) GetByIdempotencyKey(ctx context.Context, key string) (Row, error) {
	q := `SELECT ` + txSelectCols + ` FROM tx_tracker WHERE chain_id = ? AND idempotency_key = ?`
	return scanOne(s.db.QueryRowContext(ctx, q, s.chainID, strings.ToLower(key)))
}

func (s *Store) ListPending(ctx context.Context, limit int) ([]Row, error) {
	q := `SELECT ` + txSelectCols + ` FROM tx_tracker
		WHERE chain_id = ? AND status = ? ORDER BY updated_at ASC LIMIT ?`
	rows, err := s.db.QueryContext(ctx, q, s.chainID, StatusPending, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

// ListTrackable 轮询 pending + 尚未确认广播的中间态。
func (s *Store) ListTrackable(ctx context.Context, limit int) ([]Row, error) {
	q := `SELECT ` + txSelectCols + ` FROM tx_tracker
		WHERE chain_id = ? AND status IN (?, ?, ?) ORDER BY updated_at ASC LIMIT ?`
	rows, err := s.db.QueryContext(ctx, q, s.chainID, StatusPending, StatusSubmitting, StatusBroadcastFailed, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

func (s *Store) ListByFrom(ctx context.Context, from, status string, limit int) ([]Row, error) {
	q := `SELECT ` + txSelectCols + ` FROM tx_tracker WHERE chain_id = ?`
	args := []any{s.chainID}
	if from != "" {
		q += ` AND from_addr = ?`
		args = append(args, strings.ToLower(from))
	}
	if status != "" {
		q += ` AND status = ?`
		args = append(args, status)
	}
	q += ` ORDER BY created_at DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

// TransitionStatus 更新状态；若状态变化则写入 tx_status_outbox。
func (s *Store) TransitionStatus(ctx context.Context, hash, newStatus string, blockNum, confirmations, gasUsed uint64, errMsg string) error {
	old, err := s.GetByHash(ctx, hash)
	if err != nil {
		return err
	}
	if old.Status == newStatus && errMsg == "" {
		return s.updateFields(ctx, hash, newStatus, blockNum, confirmations, gasUsed, errMsg)
	}
	if err := s.updateFields(ctx, hash, newStatus, blockNum, confirmations, gasUsed, errMsg); err != nil {
		return err
	}
	if old.Status != newStatus {
		return s.insertOutbox(ctx, hash, old.BizID, old.Status, newStatus, map[string]any{
			"block_number":  blockNum,
			"confirmations": confirmations,
			"error_message": errMsg,
		})
	}
	return nil
}

func (s *Store) updateFields(ctx context.Context, hash, status string, blockNum, confirmations, gasUsed uint64, errMsg string) error {
	now := time.Now().UTC()
	var gas sql.NullInt64
	if gasUsed > 0 {
		gas = sql.NullInt64{Int64: int64(gasUsed), Valid: true}
	}
	var em sql.NullString
	if errMsg != "" {
		em = sql.NullString{String: errMsg, Valid: true}
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE tx_tracker SET
			status = ?, block_number = ?, gas_used = ?, confirmations = ?,
			error_message = ?, updated_at = ?
		WHERE chain_id = ? AND tx_hash = ?
	`, status, blockNum, gas, confirmations, em, now, s.chainID, strings.ToLower(hash))
	return err
}

// MarkReplacedSiblings 当某 nonce 的交易确认时，将同 from+nonce 的其他 pending 标为 replaced。
func (s *Store) MarkReplacedSiblings(ctx context.Context, from string, nonce uint64, winnerHash string) error {
	now := time.Now().UTC()
	from = strings.ToLower(from)
	winnerHash = strings.ToLower(winnerHash)

	rows, err := s.db.QueryContext(ctx, `
		SELECT tx_hash, status, biz_id FROM tx_tracker
		WHERE chain_id = ? AND from_addr = ? AND nonce = ? AND tx_hash != ? AND status = ?
	`, s.chainID, from, nonce, winnerHash, StatusPending)
	if err != nil {
		return err
	}
	defer rows.Close()

	var pending []struct {
		hash, bizID, status string
	}
	for rows.Next() {
		var h, st string
		var biz sql.NullString
		if err := rows.Scan(&h, &st, &biz); err != nil {
			return err
		}
		b := ""
		if biz.Valid {
			b = biz.String
		}
		pending = append(pending, struct{ hash, bizID, status string }{h, b, st})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, p := range pending {
		if _, err := s.db.ExecContext(ctx, `
			UPDATE tx_tracker SET status = ?, replaced_by_hash = ?, updated_at = ?
			WHERE chain_id = ? AND tx_hash = ?
		`, StatusReplaced, winnerHash, now, s.chainID, p.hash); err != nil {
			return err
		}
		if err := s.insertOutbox(ctx, p.hash, p.bizID, p.status, StatusReplaced, map[string]any{
			"replaced_by_hash": winnerHash,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) insertOutbox(ctx context.Context, txHash, bizID, oldSt, newSt string, extra map[string]any) error {
	payload, _ := json.Marshal(extra)
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO tx_status_outbox (
			chain_id, tx_hash, biz_id, old_status, new_status, payload,
			status, retry_count, next_retry_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
	`, s.chainID, strings.ToLower(txHash), nullStr(bizID), oldSt, newSt, payload, now, now, now)
	return err
}

type OutboxItem struct {
	ID        int64
	ChainID   int64
	TxHash    string
	BizID     string
	OldStatus string
	NewStatus string
	Payload   json.RawMessage
	Retries   int
}

func (s *Store) ClaimOutboxBatch(ctx context.Context, limit int) ([]OutboxItem, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	rows, err := tx.QueryContext(ctx, `
		SELECT id, chain_id, tx_hash, biz_id, old_status, new_status, payload, retry_count
		FROM tx_status_outbox
		WHERE status = 'pending' AND next_retry_at <= ?
		ORDER BY id ASC LIMIT ?
		FOR UPDATE SKIP LOCKED
	`, now, limit)
	if err != nil {
		return nil, err
	}
	var items []OutboxItem
	var ids []int64
	for rows.Next() {
		var it OutboxItem
		var biz sql.NullString
		if err := rows.Scan(&it.ID, &it.ChainID, &it.TxHash, &biz, &it.OldStatus, &it.NewStatus, &it.Payload, &it.Retries); err != nil {
			rows.Close()
			return nil, err
		}
		if biz.Valid {
			it.BizID = biz.String
		}
		items = append(items, it)
		ids = append(ids, it.ID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `UPDATE tx_status_outbox SET status = 'processing', updated_at = ? WHERE id = ?`, now, id); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Store) MarkOutboxDone(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE tx_status_outbox SET status = 'done', updated_at = ? WHERE id = ?`, time.Now().UTC(), id)
	return err
}

func (s *Store) MarkOutboxRetry(ctx context.Context, id int64, retries int, errMsg string) error {
	delay := time.Duration(min(retries+1, 10)) * time.Second
	next := time.Now().UTC().Add(delay)
	payload, _ := json.Marshal(map[string]string{"last_error": errMsg})
	_, err := s.db.ExecContext(ctx, `
		UPDATE tx_status_outbox SET status = 'pending', retry_count = ?, next_retry_at = ?,
			payload = JSON_MERGE_PATCH(COALESCE(payload, '{}'), ?), updated_at = ?
		WHERE id = ?
	`, retries+1, next, payload, time.Now().UTC(), id)
	return err
}

func scanOne(row *sql.Row) (Row, error) {
	var r Row
	var token, gasPrice, maxFee, maxPrio, bizID, bizType, idem, replaces, replacedBy, signedRaw sql.NullString
	var gasUsed sql.NullInt64
	var errMsg sql.NullString
	err := row.Scan(
		&r.ChainID, &r.TxHash, &r.FromAddr, &r.ToAddr, &token, &r.ValueWei, &r.Nonce, &r.GasLimit,
		&gasPrice, &maxFee, &maxPrio, &r.TxFormat, &r.TxType, &r.Status,
		&r.BlockNumber, &gasUsed, &r.Confirmations, &errMsg, &bizID, &bizType, &idem, &replaces, &replacedBy,
		&signedRaw, &r.BroadcastRetryCount, &r.CreatedAt, &r.UpdatedAt,
	)
	if token.Valid {
		r.TokenAddr = token.String
	}
	if gasPrice.Valid {
		r.GasPriceWei = gasPrice.String
	}
	if maxFee.Valid {
		r.MaxFeePerGasWei = maxFee.String
	}
	if maxPrio.Valid {
		r.MaxPriorityFeeWei = maxPrio.String
	}
	if gasUsed.Valid {
		v := uint64(gasUsed.Int64)
		r.GasUsed = &v
	}
	if errMsg.Valid {
		r.ErrorMessage = errMsg.String
	}
	if bizID.Valid {
		r.BizID = bizID.String
	}
	if bizType.Valid {
		r.BizType = bizType.String
	}
	if idem.Valid {
		r.IdempotencyKey = idem.String
	}
	if replaces.Valid {
		r.ReplacesHash = replaces.String
	}
	if replacedBy.Valid {
		r.ReplacedByHash = replacedBy.String
	}
	if signedRaw.Valid {
		r.SignedRawHex = signedRaw.String
	}
	return r, err
}

func scanRows(rows *sql.Rows) ([]Row, error) {
	var out []Row
	for rows.Next() {
		var r Row
		var token, gasPrice, maxFee, maxPrio, bizID, bizType, idem, replaces, replacedBy, signedRaw sql.NullString
		var gasUsed sql.NullInt64
		var errMsg sql.NullString
		if err := rows.Scan(
			&r.ChainID, &r.TxHash, &r.FromAddr, &r.ToAddr, &token, &r.ValueWei, &r.Nonce, &r.GasLimit,
			&gasPrice, &maxFee, &maxPrio, &r.TxFormat, &r.TxType, &r.Status,
			&r.BlockNumber, &gasUsed, &r.Confirmations, &errMsg, &bizID, &bizType, &idem, &replaces, &replacedBy,
			&signedRaw, &r.BroadcastRetryCount, &r.CreatedAt, &r.UpdatedAt,
		); err != nil {
			return nil, err
		}
		if token.Valid {
			r.TokenAddr = token.String
		}
		if gasPrice.Valid {
			r.GasPriceWei = gasPrice.String
		}
		if maxFee.Valid {
			r.MaxFeePerGasWei = maxFee.String
		}
		if maxPrio.Valid {
			r.MaxPriorityFeeWei = maxPrio.String
		}
		if gasUsed.Valid {
			v := uint64(gasUsed.Int64)
			r.GasUsed = &v
		}
		if errMsg.Valid {
			r.ErrorMessage = errMsg.String
		}
		if bizID.Valid {
			r.BizID = bizID.String
		}
		if bizType.Valid {
			r.BizType = bizType.String
		}
		if idem.Valid {
			r.IdempotencyKey = idem.String
		}
		if replaces.Valid {
			r.ReplacesHash = replaces.String
		}
		if replacedBy.Valid {
			r.ReplacedByHash = replacedBy.String
		}
		if signedRaw.Valid {
			r.SignedRawHex = signedRaw.String
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
