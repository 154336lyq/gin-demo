package webhook

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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

func (s *Store) BeginTx(ctx context.Context) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}

func nowUTC() time.Time { return time.Now().UTC() }

func isDup(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Duplicate entry") || strings.Contains(err.Error(), "1062")
}

func (s *Store) UpsertMerchant(ctx context.Context, m Merchant, secret string) error {
	now := nowUTC()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO webhook_merchants (chain_id, merchant_id, name, webhook_url, secret, ledger_user_id, enabled, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			name = VALUES(name),
			webhook_url = VALUES(webhook_url),
			secret = IF(VALUES(secret) = '', secret, VALUES(secret)),
			ledger_user_id = VALUES(ledger_user_id),
			enabled = VALUES(enabled),
			updated_at = VALUES(updated_at)
	`, s.chainID, m.MerchantID, m.Name, m.WebhookURL, secret, m.LedgerUserID, m.Enabled, now, now)
	return err
}

func (s *Store) GetMerchant(ctx context.Context, merchantID string) (Merchant, string, error) {
	var m Merchant
	var secret string
	var enabled int
	err := s.db.QueryRowContext(ctx, `
		SELECT chain_id, merchant_id, name, webhook_url, secret, ledger_user_id, enabled, created_at, updated_at
		FROM webhook_merchants WHERE chain_id = ? AND merchant_id = ?
	`, s.chainID, merchantID).Scan(
		&m.ChainID, &m.MerchantID, &m.Name, &m.WebhookURL, &secret, &m.LedgerUserID, &enabled, &m.CreatedAt, &m.UpdatedAt,
	)
	m.Enabled = enabled == 1
	return m, secret, err
}

func (s *Store) ListMerchants(ctx context.Context, limit int) ([]Merchant, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT chain_id, merchant_id, name, webhook_url, ledger_user_id, enabled, created_at, updated_at
		FROM webhook_merchants WHERE chain_id = ? ORDER BY merchant_id LIMIT ?
	`, s.chainID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Merchant
	for rows.Next() {
		var m Merchant
		var enabled int
		if err := rows.Scan(&m.ChainID, &m.MerchantID, &m.Name, &m.WebhookURL, &m.LedgerUserID, &enabled, &m.CreatedAt, &m.UpdatedAt); err != nil {
			return nil, err
		}
		m.Enabled = enabled == 1
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) AddBinding(ctx context.Context, merchantID, payerUserID string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO webhook_bindings (chain_id, merchant_id, payer_user_id, created_at)
		VALUES (?, ?, ?, ?)
	`, s.chainID, merchantID, payerUserID, nowUTC())
	if isDup(err) {
		return ErrDuplicateBinding
	}
	return err
}

func (s *Store) ListBindingsByPayer(ctx context.Context, payerUserID string) ([]Binding, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT chain_id, merchant_id, payer_user_id, created_at
		FROM webhook_bindings WHERE chain_id = ? AND payer_user_id = ?
	`, s.chainID, payerUserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Binding
	for rows.Next() {
		var b Binding
		if err := rows.Scan(&b.ChainID, &b.MerchantID, &b.PayerUserID, &b.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *Store) InsertOutboxTx(ctx context.Context, tx *sql.Tx, merchantID, eventType, idemKey string, payload any) (bool, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return false, err
	}
	now := nowUTC()
	res, err := tx.ExecContext(ctx, `
		INSERT INTO webhook_outbox (chain_id, merchant_id, event_type, idempotency_key, payload, status, retry_count, next_retry_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
	`, s.chainID, merchantID, eventType, idemKey, raw, now, now, now)
	if isDup(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) ClaimOutboxBatch(ctx context.Context, limit int) ([]OutboxRow, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT id, chain_id, merchant_id, event_type, idempotency_key, payload, status, retry_count, next_retry_at, created_at
		FROM webhook_outbox
		WHERE chain_id = ? AND status = 'pending' AND next_retry_at <= ?
		ORDER BY id ASC LIMIT ?
		FOR UPDATE SKIP LOCKED
	`, s.chainID, nowUTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var batch []OutboxRow
	var ids []uint64
	for rows.Next() {
		var r OutboxRow
		if err := rows.Scan(&r.ID, &r.ChainID, &r.MerchantID, &r.EventType, &r.IdempotencyKey, &r.Payload, &r.Status, &r.RetryCount, &r.NextRetryAt, &r.CreatedAt); err != nil {
			return nil, err
		}
		batch = append(batch, r)
		ids = append(ids, r.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, tx.Commit()
	}
	now := nowUTC()
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `UPDATE webhook_outbox SET status = 'processing', updated_at = ? WHERE id = ?`, now, id); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return batch, nil
}

func (s *Store) MarkOutboxDone(ctx context.Context, id uint64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE webhook_outbox SET status = 'done', updated_at = ? WHERE id = ?`, nowUTC(), id)
	return err
}

func (s *Store) MarkOutboxRetry(ctx context.Context, id uint64, retryCount uint, next time.Time, fail bool) error {
	status := StatusPending
	if fail {
		status = StatusFailed
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE webhook_outbox SET status = ?, retry_count = ?, next_retry_at = ?, updated_at = ? WHERE id = ?
	`, status, retryCount, next, nowUTC(), id)
	return err
}

func (s *Store) CountOutboxByStatus(ctx context.Context, status string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM webhook_outbox WHERE chain_id = ? AND status = ?`, s.chainID, status).Scan(&n)
	return n, err
}

func (s *Store) InsertDelivery(ctx context.Context, outboxID uint64, merchantID string, attempt, httpStatus, latencyMS int, respBody, errMsg string) error {
	if len(respBody) > 500 {
		respBody = respBody[:500]
	}
	if len(errMsg) > 500 {
		errMsg = errMsg[:500]
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO webhook_deliveries (chain_id, outbox_id, merchant_id, attempt, http_status, latency_ms, response_body, error_message, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, s.chainID, outboxID, merchantID, attempt, httpStatus, latencyMS, nullStr(respBody), nullStr(errMsg), nowUTC())
	return err
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (s *Store) ListDeliveries(ctx context.Context, merchantID string, limit int) ([]Delivery, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	q := `
		SELECT id, outbox_id, merchant_id, attempt, http_status, latency_ms,
			COALESCE(response_body,''), COALESCE(error_message,''), created_at
		FROM webhook_deliveries WHERE chain_id = ?`
	args := []any{s.chainID}
	if merchantID != "" {
		q += ` AND merchant_id = ?`
		args = append(args, merchantID)
	}
	q += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Delivery
	for rows.Next() {
		var d Delivery
		if err := rows.Scan(&d.ID, &d.OutboxID, &d.MerchantID, &d.Attempt, &d.HTTPStatus, &d.LatencyMS, &d.ResponseBody, &d.ErrorMessage, &d.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) InsertPaymentTx(ctx context.Context, tx *sql.Tx, p Payment) (int64, bool, error) {
	now := nowUTC()
	res, err := tx.ExecContext(ctx, `
		INSERT INTO merchant_payments (chain_id, merchant_id, order_id, payer_user_id, token_address, amount_wei, status, idempotency_key, created_at)
		VALUES (?, ?, ?, ?, ?, ?, 'completed', ?, ?)
	`, s.chainID, p.MerchantID, p.OrderID, p.PayerUserID, p.TokenAddress, p.AmountWei, p.IdempotencyKey, now)
	if isDup(err) {
		return 0, false, ErrDuplicatePayment
	}
	if err != nil {
		return 0, false, err
	}
	id, _ := res.LastInsertId()
	return id, true, nil
}

func (s *Store) GetOutboxByID(ctx context.Context, id uint64) (OutboxRow, error) {
	var r OutboxRow
	err := s.db.QueryRowContext(ctx, `
		SELECT id, chain_id, merchant_id, event_type, idempotency_key, payload, status, retry_count, next_retry_at, created_at
		FROM webhook_outbox WHERE chain_id = ? AND id = ?
	`, s.chainID, id).Scan(&r.ID, &r.ChainID, &r.MerchantID, &r.EventType, &r.IdempotencyKey, &r.Payload, &r.Status, &r.RetryCount, &r.NextRetryAt, &r.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrOutboxNotFound
	}
	return r, err
}

func (s *Store) RequeueOutbox(ctx context.Context, id uint64) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE webhook_outbox SET status = 'pending', next_retry_at = ?, updated_at = ? WHERE chain_id = ? AND id = ? AND status IN ('failed','processing')
	`, nowUTC(), nowUTC(), s.chainID, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrOutboxNotFound
	}
	return nil
}

// BenchEnqueue 压测：批量写入 outbox（不经业务事务）。
func (s *Store) BenchEnqueue(ctx context.Context, merchantID, eventType string, n int) (int, error) {
	m, _, err := s.GetMerchant(ctx, merchantID)
	if err != nil {
		return 0, err
	}
	if !m.Enabled {
		return 0, ErrMerchantDisabled
	}
	tx, err := s.BeginTx(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	inserted := 0
	now := nowUTC().UnixNano()
	for i := 0; i < n; i++ {
		idem := fmtBenchKey(now, i)
		payload := map[string]any{
			"event_type":  eventType,
			"merchant_id": merchantID,
			"bench_seq":   i,
			"amount":      "1000000",
			"token":       "USDT",
			"status":      "confirmed",
		}
		ok, err := s.InsertOutboxTx(ctx, tx, merchantID, eventType, idem, payload)
		if err != nil {
			return inserted, err
		}
		if ok {
			inserted++
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return inserted, nil
}

func fmtBenchKey(now int64, i int) string {
	return fmt.Sprintf("bench:%d:%d", now, i)
}
