package tx

import (
	"context"
	"database/sql"
	"fmt"
)

// EnsureSchema 创建/迁移 tx 模块所需表（wallet_nonce、tx_tracker、tx_status_outbox）。
func EnsureSchema(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS wallet_nonce (
			chain_id BIGINT NOT NULL,
			address VARCHAR(42) NOT NULL,
			next_nonce BIGINT UNSIGNED NOT NULL,
			updated_at DATETIME(3) NOT NULL,
			PRIMARY KEY (chain_id, address)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS tx_tracker (
			chain_id BIGINT NOT NULL,
			tx_hash VARCHAR(66) NOT NULL,
			from_addr VARCHAR(42) NOT NULL,
			to_addr VARCHAR(42) NOT NULL,
			token_addr VARCHAR(42) NULL,
			value_wei VARCHAR(100) NOT NULL DEFAULT '0',
			nonce BIGINT UNSIGNED NOT NULL,
			gas_limit BIGINT UNSIGNED NOT NULL,
			gas_price_wei VARCHAR(100) NULL,
			max_fee_per_gas_wei VARCHAR(100) NULL,
			max_priority_fee_wei VARCHAR(100) NULL,
			tx_format ENUM('legacy','eip1559') NOT NULL DEFAULT 'legacy',
			tx_type ENUM('native','erc20') NOT NULL DEFAULT 'native',
			status ENUM('submitting','broadcast_failed','pending','confirmed','failed','dropped','replaced') NOT NULL DEFAULT 'pending',
			block_number BIGINT UNSIGNED NOT NULL DEFAULT 0,
			gas_used BIGINT UNSIGNED NULL,
			confirmations INT UNSIGNED NOT NULL DEFAULT 0,
			error_message VARCHAR(512) NULL,
			biz_id VARCHAR(64) NULL,
			biz_type VARCHAR(32) NULL,
			idempotency_key VARCHAR(64) NULL,
			replaces_hash VARCHAR(66) NULL,
			replaced_by_hash VARCHAR(66) NULL,
			created_at DATETIME(3) NOT NULL,
			updated_at DATETIME(3) NOT NULL,
			PRIMARY KEY (chain_id, tx_hash),
			UNIQUE KEY uk_tx_idempotency (chain_id, idempotency_key),
			INDEX idx_tx_from_status (chain_id, from_addr, status),
			INDEX idx_tx_status_time (chain_id, status, updated_at),
			INDEX idx_tx_from_nonce (chain_id, from_addr, nonce)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS tx_status_outbox (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			chain_id BIGINT NOT NULL,
			tx_hash VARCHAR(66) NOT NULL,
			biz_id VARCHAR(64) NULL,
			old_status VARCHAR(32) NOT NULL,
			new_status VARCHAR(32) NOT NULL,
			payload JSON NULL,
			status ENUM('pending','processing','done','failed') NOT NULL DEFAULT 'pending',
			retry_count INT UNSIGNED NOT NULL DEFAULT 0,
			next_retry_at DATETIME(3) NOT NULL,
			created_at DATETIME(3) NOT NULL,
			updated_at DATETIME(3) NOT NULL,
			INDEX idx_tx_outbox_poll (status, next_retry_at),
			INDEX idx_tx_outbox_hash (chain_id, tx_hash)
		) ENGINE=InnoDB`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("tx schema: %w", err)
		}
	}
	return migrateTxTrackerColumns(ctx, db)
}

// migrateTxTrackerColumns 兼容旧库：逐列 ALTER（已存在则忽略）。
func migrateTxTrackerColumns(ctx context.Context, db *sql.DB) error {
	alters := []string{
		`ALTER TABLE tx_tracker ADD COLUMN token_addr VARCHAR(42) NULL AFTER to_addr`,
		`ALTER TABLE tx_tracker ADD COLUMN max_fee_per_gas_wei VARCHAR(100) NULL AFTER gas_price_wei`,
		`ALTER TABLE tx_tracker ADD COLUMN max_priority_fee_wei VARCHAR(100) NULL AFTER max_fee_per_gas_wei`,
		`ALTER TABLE tx_tracker ADD COLUMN tx_format ENUM('legacy','eip1559') NOT NULL DEFAULT 'legacy' AFTER max_priority_fee_wei`,
		`ALTER TABLE tx_tracker ADD COLUMN biz_id VARCHAR(64) NULL AFTER error_message`,
		`ALTER TABLE tx_tracker ADD COLUMN biz_type VARCHAR(32) NULL AFTER biz_id`,
		`ALTER TABLE tx_tracker ADD COLUMN idempotency_key VARCHAR(64) NULL AFTER biz_type`,
		`ALTER TABLE tx_tracker ADD COLUMN replaces_hash VARCHAR(66) NULL AFTER idempotency_key`,
		`ALTER TABLE tx_tracker ADD COLUMN replaced_by_hash VARCHAR(66) NULL AFTER replaces_hash`,
		`ALTER TABLE tx_tracker MODIFY status ENUM('submitting','broadcast_failed','pending','confirmed','failed','dropped','replaced') NOT NULL DEFAULT 'pending'`,
		`ALTER TABLE tx_tracker ADD COLUMN signed_raw_hex MEDIUMTEXT NULL AFTER replaced_by_hash`,
		`ALTER TABLE tx_tracker ADD COLUMN broadcast_retry_count INT UNSIGNED NOT NULL DEFAULT 0 AFTER signed_raw_hex`,
		`ALTER TABLE tx_tracker ADD UNIQUE KEY uk_tx_idempotency (chain_id, idempotency_key)`,
		`ALTER TABLE tx_tracker ADD INDEX idx_tx_from_nonce (chain_id, from_addr, nonce)`,
	}
	for _, stmt := range alters {
		_, _ = db.ExecContext(ctx, stmt)
	}
	return nil
}
