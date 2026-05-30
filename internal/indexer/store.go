package indexer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Checkpoint 记录同步游标与已确认高度（reorg 安全边界）。
type Checkpoint struct {
	ChainID              int64
	LastBlockNumber      uint64
	LastBlockHash        string
	ConfirmedBlockNumber uint64
	UpdatedAt            time.Time
}

// BlockRow 链下区块快照。
type BlockRow struct {
	ChainID        uint64
	BlockNumber    uint64
	BlockHash      string
	ParentHash     string
	BlockTimestamp uint64
	TxCount        uint
	GasUsed        uint64
	Status         string
	SyncedAt       time.Time
}

// TxRow 链下交易快照。
type TxRow struct {
	ChainID     uint64
	TxHash      string
	BlockNumber uint64
	BlockHash   string
	TxIndex     uint
	FromAddr    string
	ToAddr      sql.NullString
	ValueWei    string
	GasUsed     sql.NullInt64
	TxStatus    sql.NullInt64
	SyncedAt    time.Time
}

// OutboxRow 可靠投递队列项（Transactional Outbox）。
type OutboxRow struct {
	ID          uint64
	ChainID     uint64
	EventType   string
	Payload     json.RawMessage
	Status      string
	RetryCount  uint
	NextRetryAt time.Time
}

// Status 对外暴露的 indexer 运行状态。
type Status struct {
	Enabled              bool      `json:"enabled"`
	CacheBackend         string    `json:"cache_backend"`
	ChainID              int64     `json:"chain_id"`
	LastBlockNumber      uint64    `json:"last_block_number"`
	LastBlockHash        string    `json:"last_block_hash"`
	ConfirmedBlockNumber uint64    `json:"confirmed_block_number"`
	HeadBlockNumber      uint64    `json:"head_block_number"`
	LagBlocks            uint64    `json:"lag_blocks"`
	PendingHeaders       int       `json:"pending_headers"`
	PendingOutbox        int       `json:"pending_outbox"`
	BlocksSynced         int64     `json:"blocks_synced"`
	TxsSynced            int64     `json:"txs_synced"`
	EventsSynced         int64     `json:"events_synced"`
	WatchContracts       int       `json:"watch_contracts"`
	ReorgsHandled        uint64    `json:"reorgs_handled"`
	LastGapScanAt        time.Time `json:"last_gap_scan_at"`
	LastGapsFound        int       `json:"last_gaps_found"`
	LastHashMismatches   int       `json:"last_hash_mismatches"`
	LastGapsEnqueued     int       `json:"last_gaps_enqueued"`
}

// BlockMeta 块号与哈希（gap scan 用）。
type BlockMeta struct {
	Number uint64
	Hash   string
}

// GapScanAudit 漏块扫描审计记录。
type GapScanAudit struct {
	ChainID        uint64
	HeadBlock      uint64
	ScanFrom       uint64
	ScanTo         uint64
	GapsFound      int
	HashMismatches int
	GapsEnqueued   int
}

// EventLogRow 通用链上事件行。
type EventLogRow struct {
	ChainID         uint64
	TxHash          string
	LogIndex        uint
	BlockNumber     uint64
	BlockHash       string
	ContractAddress string
	EventName       string
	Topic0          string
	TopicsJSON      string
	ArgsJSON        string
	SyncedAt        time.Time
}

type Store struct {
	db      *sql.DB
	chainID uint64
}

func NewStore(db *sql.DB, chainID int64) *Store {
	return &Store{db: db, chainID: uint64(chainID)}
}

func (s *Store) LoadCheckpoint(ctx context.Context) (Checkpoint, error) {
	var cp Checkpoint
	err := s.db.QueryRowContext(ctx, `
		SELECT chain_id, last_block_number, last_block_hash, confirmed_block_number, updated_at
		FROM indexer_checkpoints WHERE chain_id = ?
	`, s.chainID).Scan(&cp.ChainID, &cp.LastBlockNumber, &cp.LastBlockHash, &cp.ConfirmedBlockNumber, &cp.UpdatedAt)
	if err == sql.ErrNoRows {
		return Checkpoint{ChainID: int64(s.chainID)}, nil
	}
	return cp, err
}

func (s *Store) UpsertCheckpointTx(ctx context.Context, tx *sql.Tx, lastNum uint64, lastHash string, confirmedNum uint64) error {
	now := nowUTC()
	_, err := tx.ExecContext(ctx, `
		INSERT INTO indexer_checkpoints (chain_id, last_block_number, last_block_hash, confirmed_block_number, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			last_block_number = VALUES(last_block_number),
			last_block_hash = VALUES(last_block_hash),
			confirmed_block_number = GREATEST(confirmed_block_number, VALUES(confirmed_block_number)),
			updated_at = VALUES(updated_at)
	`, s.chainID, lastNum, lastHash, confirmedNum, now)
	return err
}

func (s *Store) EnqueuePendingHeader(ctx context.Context, blockNum uint64, blockHash string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO sync_pending_headers (chain_id, block_number, block_hash, enqueued_at)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE block_hash = VALUES(block_hash), enqueued_at = VALUES(enqueued_at)
	`, s.chainID, blockNum, blockHash, nowUTC())
	return err
}

func (s *Store) DeletePendingHeaderTx(ctx context.Context, tx *sql.Tx, blockNum uint64) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM sync_pending_headers WHERE chain_id = ? AND block_number = ?`, s.chainID, blockNum)
	return err
}

func (s *Store) ListPendingHeaders(ctx context.Context, limit int) ([]struct {
	Number uint64
	Hash   string
}, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT block_number, block_hash FROM sync_pending_headers
		WHERE chain_id = ? ORDER BY block_number ASC LIMIT ?
	`, s.chainID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []struct {
		Number uint64
		Hash   string
	}
	for rows.Next() {
		var n uint64
		var h string
		if err := rows.Scan(&n, &h); err != nil {
			return nil, err
		}
		out = append(out, struct {
			Number uint64
			Hash   string
		}{n, h})
	}
	return out, rows.Err()
}

func (s *Store) CountPendingHeaders(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sync_pending_headers WHERE chain_id = ?`, s.chainID).Scan(&n)
	return n, err
}

func (s *Store) GetBlockByNumber(ctx context.Context, num uint64) (BlockRow, error) {
	var b BlockRow
	err := s.db.QueryRowContext(ctx, `
		SELECT chain_id, block_number, block_hash, parent_hash, block_timestamp, tx_count, gas_used, status, synced_at
		FROM synced_blocks WHERE chain_id = ? AND block_number = ? AND status != 'orphaned'
	`, s.chainID, num).Scan(&b.ChainID, &b.BlockNumber, &b.BlockHash, &b.ParentHash, &b.BlockTimestamp, &b.TxCount, &b.GasUsed, &b.Status, &b.SyncedAt)
	return b, err
}

func (s *Store) MarkOrphanedFromTx(ctx context.Context, tx *sql.Tx, fromBlock uint64) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE synced_blocks SET status = 'orphaned'
		WHERE chain_id = ? AND block_number >= ? AND status != 'orphaned'
	`, s.chainID, fromBlock)
	return err
}

func (s *Store) RollbackCheckpointTx(ctx context.Context, tx *sql.Tx, toBlock uint64, hash string) error {
	now := nowUTC()
	_, err := tx.ExecContext(ctx, `
		UPDATE indexer_checkpoints
		SET last_block_number = ?, last_block_hash = ?, confirmed_block_number = LEAST(confirmed_block_number, ?), updated_at = ?
		WHERE chain_id = ?
	`, toBlock, hash, toBlock, now, s.chainID)
	return err
}

func (s *Store) UpsertBlockTx(ctx context.Context, tx *sql.Tx, b BlockRow) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO synced_blocks (chain_id, block_number, block_hash, parent_hash, block_timestamp, tx_count, gas_used, status, synced_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			block_hash = VALUES(block_hash),
			parent_hash = VALUES(parent_hash),
			block_timestamp = VALUES(block_timestamp),
			tx_count = VALUES(tx_count),
			gas_used = VALUES(gas_used),
			status = IF(status = 'orphaned', 'pending', VALUES(status)),
			synced_at = VALUES(synced_at)
	`, b.ChainID, b.BlockNumber, b.BlockHash, b.ParentHash, b.BlockTimestamp, b.TxCount, b.GasUsed, b.Status, b.SyncedAt)
	return err
}

func (s *Store) UpsertTransactionTx(ctx context.Context, tx *sql.Tx, row TxRow) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO synced_transactions (chain_id, tx_hash, block_number, block_hash, tx_index, from_addr, to_addr, value_wei, gas_used, tx_status, synced_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			block_number = VALUES(block_number),
			block_hash = VALUES(block_hash),
			tx_index = VALUES(tx_index),
			gas_used = VALUES(gas_used),
			tx_status = VALUES(tx_status),
			synced_at = VALUES(synced_at)
	`, row.ChainID, row.TxHash, row.BlockNumber, row.BlockHash, row.TxIndex, row.FromAddr, row.ToAddr, row.ValueWei, row.GasUsed, row.TxStatus, row.SyncedAt)
	return err
}

func (s *Store) UpsertTransferLogTx(ctx context.Context, tx *sql.Tx, chainID, blockNum uint64, txHash string, logIndex uint, contract, from, to, value string) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO synced_transfer_logs (chain_id, tx_hash, log_index, block_number, contract_address, from_addr, to_addr, value_wei, synced_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE synced_at = VALUES(synced_at)
	`, chainID, txHash, logIndex, blockNum, contract, from, to, value, nowUTC())
	return err
}

func (s *Store) ConfirmBlocksUpToTx(ctx context.Context, tx *sql.Tx, upTo uint64) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE synced_blocks SET status = 'confirmed'
		WHERE chain_id = ? AND block_number <= ? AND status = 'pending'
	`, s.chainID, upTo)
	return err
}

func (s *Store) InsertOutboxTx(ctx context.Context, tx *sql.Tx, eventType string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	now := nowUTC()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO sync_outbox (chain_id, event_type, payload, status, retry_count, next_retry_at, created_at, updated_at)
		VALUES (?, ?, ?, 'pending', 0, ?, ?, ?)
	`, s.chainID, eventType, raw, now, now, now)
	return err
}

func (s *Store) ClaimOutboxBatch(ctx context.Context, limit int) ([]OutboxRow, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT id, chain_id, event_type, payload, status, retry_count, next_retry_at
		FROM sync_outbox
		WHERE status = 'pending' AND next_retry_at <= ?
		ORDER BY id ASC LIMIT ?
		FOR UPDATE SKIP LOCKED
	`, nowUTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var batch []OutboxRow
	var ids []uint64
	for rows.Next() {
		var r OutboxRow
		if err := rows.Scan(&r.ID, &r.ChainID, &r.EventType, &r.Payload, &r.Status, &r.RetryCount, &r.NextRetryAt); err != nil {
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
		if _, err := tx.ExecContext(ctx, `UPDATE sync_outbox SET status = 'processing', updated_at = ? WHERE id = ?`, now, id); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return batch, nil
}

func (s *Store) MarkOutboxDone(ctx context.Context, id uint64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sync_outbox SET status = 'done', updated_at = ? WHERE id = ?`, nowUTC(), id)
	return err
}

func (s *Store) MarkOutboxRetry(ctx context.Context, id uint64, retryCount uint, next time.Time, fail bool) error {
	status := "pending"
	if fail {
		status = "failed"
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE sync_outbox SET status = ?, retry_count = ?, next_retry_at = ?, updated_at = ? WHERE id = ?
	`, status, retryCount, next, nowUTC(), id)
	return err
}

func (s *Store) CountPendingOutbox(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sync_outbox WHERE chain_id = ? AND status IN ('pending','processing')`, s.chainID).Scan(&n)
	return n, err
}

func (s *Store) CountBlocks(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM synced_blocks WHERE chain_id = ? AND status != 'orphaned'`, s.chainID).Scan(&n)
	return n, err
}

func (s *Store) CountTxs(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM synced_transactions WHERE chain_id = ?`, s.chainID).Scan(&n)
	return n, err
}

func (s *Store) ListBlocks(ctx context.Context, from, limit int) ([]BlockRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT chain_id, block_number, block_hash, parent_hash, block_timestamp, tx_count, gas_used, status, synced_at
		FROM synced_blocks WHERE chain_id = ? AND status != 'orphaned' AND block_number >= ?
		ORDER BY block_number ASC LIMIT ?
	`, s.chainID, from, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BlockRow
	for rows.Next() {
		var b BlockRow
		if err := rows.Scan(&b.ChainID, &b.BlockNumber, &b.BlockHash, &b.ParentHash, &b.BlockTimestamp, &b.TxCount, &b.GasUsed, &b.Status, &b.SyncedAt); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *Store) ListTransactions(ctx context.Context, blockNum uint64, limit int) ([]TxRow, error) {
	q := `
		SELECT chain_id, tx_hash, block_number, block_hash, tx_index, from_addr, to_addr, value_wei, gas_used, tx_status, synced_at
		FROM synced_transactions WHERE chain_id = ?`
	args := []any{s.chainID}
	if blockNum > 0 {
		q += ` AND block_number = ?`
		args = append(args, blockNum)
	}
	q += ` ORDER BY block_number DESC, tx_index ASC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TxRow
	for rows.Next() {
		var t TxRow
		if err := rows.Scan(&t.ChainID, &t.TxHash, &t.BlockNumber, &t.BlockHash, &t.TxIndex, &t.FromAddr, &t.ToAddr, &t.ValueWei, &t.GasUsed, &t.TxStatus, &t.SyncedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) BeginTx(ctx context.Context) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}

func lockKey(chainID uint64, blockNum uint64) string {
	return fmt.Sprintf("indexer:lock:%d:%d", chainID, blockNum)
}

func cacheBlockKey(chainID uint64, blockNum uint64) string {
	return fmt.Sprintf("indexer:block:%d:%d", chainID, blockNum)
}

func cacheHeadKey(chainID uint64) string {
	return fmt.Sprintf("indexer:head:%d", chainID)
}

func (s *Store) MapSyncedBlocksInRange(ctx context.Context, from, to uint64) (map[uint64]BlockMeta, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT block_number, block_hash FROM synced_blocks
		WHERE chain_id = ? AND status != 'orphaned' AND block_number BETWEEN ? AND ?
	`, s.chainID, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[uint64]BlockMeta)
	for rows.Next() {
		var n uint64
		var h string
		if err := rows.Scan(&n, &h); err != nil {
			return nil, err
		}
		out[n] = BlockMeta{Number: n, Hash: h}
	}
	return out, rows.Err()
}

func (s *Store) InsertGapScanAudit(ctx context.Context, a GapScanAudit) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO indexer_gap_scans (chain_id, head_block, scan_from, scan_to, gaps_found, hash_mismatches, gaps_enqueued, scanned_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, a.ChainID, a.HeadBlock, a.ScanFrom, a.ScanTo, a.GapsFound, a.HashMismatches, a.GapsEnqueued, nowUTC())
	return err
}

func (s *Store) ListRecentGapScans(ctx context.Context, limit int) ([]GapScanAudit, time.Time, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT chain_id, head_block, scan_from, scan_to, gaps_found, hash_mismatches, gaps_enqueued, scanned_at
		FROM indexer_gap_scans WHERE chain_id = ? ORDER BY id DESC LIMIT ?
	`, s.chainID, limit)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer rows.Close()
	var out []GapScanAudit
	var lastAt time.Time
	for rows.Next() {
		var a GapScanAudit
		var scannedAt time.Time
		if err := rows.Scan(&a.ChainID, &a.HeadBlock, &a.ScanFrom, &a.ScanTo, &a.GapsFound, &a.HashMismatches, &a.GapsEnqueued, &scannedAt); err != nil {
			return nil, time.Time{}, err
		}
		if lastAt.IsZero() {
			lastAt = scannedAt
		}
		out = append(out, a)
	}
	return out, lastAt, rows.Err()
}

func (s *Store) UpsertEventLogTx(ctx context.Context, tx *sql.Tx, row EventLogRow) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO synced_event_logs (chain_id, tx_hash, log_index, block_number, block_hash, contract_address, event_name, topic0, topics_json, args_json, synced_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			block_number = VALUES(block_number),
			block_hash = VALUES(block_hash),
			event_name = VALUES(event_name),
			args_json = VALUES(args_json),
			synced_at = VALUES(synced_at)
	`, row.ChainID, row.TxHash, row.LogIndex, row.BlockNumber, row.BlockHash, row.ContractAddress, row.EventName, row.Topic0, row.TopicsJSON, row.ArgsJSON, row.SyncedAt)
	return err
}

func (s *Store) CountEvents(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM synced_event_logs el
		INNER JOIN synced_blocks b ON b.chain_id = el.chain_id AND b.block_number = el.block_number AND b.status != 'orphaned'
		WHERE el.chain_id = ?
	`, s.chainID).Scan(&n)
	return n, err
}

func (s *Store) ListEvents(ctx context.Context, contract, eventName string, blockNum uint64, limit int) ([]EventLogRow, error) {
	q := `
		SELECT el.chain_id, el.tx_hash, el.log_index, el.block_number, el.block_hash, el.contract_address, el.event_name, el.topic0, el.topics_json, el.args_json, el.synced_at
		FROM synced_event_logs el
		INNER JOIN synced_blocks b ON b.chain_id = el.chain_id AND b.block_number = el.block_number AND b.status != 'orphaned'
		WHERE el.chain_id = ?`
	args := []any{s.chainID}
	if contract != "" {
		q += ` AND el.contract_address = ?`
		args = append(args, strings.ToLower(contract))
	}
	if eventName != "" {
		q += ` AND el.event_name = ?`
		args = append(args, eventName)
	}
	if blockNum > 0 {
		q += ` AND el.block_number = ?`
		args = append(args, blockNum)
	}
	q += ` ORDER BY el.block_number DESC, el.log_index ASC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EventLogRow
	for rows.Next() {
		var r EventLogRow
		if err := rows.Scan(&r.ChainID, &r.TxHash, &r.LogIndex, &r.BlockNumber, &r.BlockHash, &r.ContractAddress, &r.EventName, &r.Topic0, &r.TopicsJSON, &r.ArgsJSON, &r.SyncedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
