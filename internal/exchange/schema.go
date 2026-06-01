package exchange

import (
	"context"
	"database/sql"
	"fmt"
)

func EnsureSchema(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS user_ledger_accounts (
			chain_id BIGINT NOT NULL,
			user_id VARCHAR(64) NOT NULL,
			token_address VARCHAR(42) NOT NULL DEFAULT '',
			available_wei VARCHAR(100) NOT NULL DEFAULT '0',
			frozen_wei VARCHAR(100) NOT NULL DEFAULT '0',
			updated_at DATETIME(3) NOT NULL,
			PRIMARY KEY (chain_id, user_id, token_address),
			INDEX idx_ledger_user (chain_id, user_id)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS ledger_entries (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			chain_id BIGINT NOT NULL,
			user_id VARCHAR(64) NOT NULL,
			token_address VARCHAR(42) NOT NULL DEFAULT '',
			entry_type VARCHAR(32) NOT NULL,
			amount_wei VARCHAR(100) NOT NULL,
			ref_type VARCHAR(32) NULL,
			ref_id BIGINT NULL,
			balance_available_after VARCHAR(100) NOT NULL,
			balance_frozen_after VARCHAR(100) NOT NULL,
			created_at DATETIME(3) NOT NULL,
			INDEX idx_ledger_ref (chain_id, ref_type, ref_id),
			INDEX idx_ledger_user_time (chain_id, user_id, created_at)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS deposits (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			chain_id BIGINT NOT NULL,
			user_id VARCHAR(64) NOT NULL,
			deposit_address VARCHAR(42) NOT NULL,
			token_address VARCHAR(42) NOT NULL DEFAULT '',
			amount_wei VARCHAR(100) NOT NULL,
			tx_hash VARCHAR(66) NOT NULL,
			log_index INT UNSIGNED NOT NULL DEFAULT 0,
			block_number BIGINT UNSIGNED NOT NULL,
			status ENUM('pending','credited','orphaned') NOT NULL DEFAULT 'pending',
			credited_at DATETIME(3) NULL,
			created_at DATETIME(3) NOT NULL,
			UNIQUE KEY uk_deposit (chain_id, tx_hash, log_index),
			INDEX idx_deposit_user (chain_id, user_id, created_at),
			INDEX idx_deposit_block (chain_id, block_number)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS withdraw_requests (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			chain_id BIGINT NOT NULL,
			user_id VARCHAR(64) NOT NULL,
			token_address VARCHAR(42) NOT NULL DEFAULT '',
			to_address VARCHAR(42) NOT NULL,
			amount_wei VARCHAR(100) NOT NULL,
			status ENUM('pending_review','approved','rejected','broadcasting','confirmed','failed','cancelled') NOT NULL DEFAULT 'pending_review',
			from_wallet VARCHAR(42) NULL,
			tx_hash VARCHAR(66) NULL,
			reviewer VARCHAR(64) NULL,
			reviewed_at DATETIME(3) NULL,
			reject_reason VARCHAR(255) NULL,
			idempotency_key VARCHAR(64) NULL,
			created_at DATETIME(3) NOT NULL,
			updated_at DATETIME(3) NOT NULL,
			UNIQUE KEY uk_withdraw_idem (chain_id, idempotency_key),
			INDEX idx_withdraw_user (chain_id, user_id, created_at),
			INDEX idx_withdraw_status (chain_id, status, updated_at)
		) ENGINE=InnoDB`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exchange schema: %w", err)
		}
	}
	return nil
}
