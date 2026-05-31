package balance

import (
	"context"
	"database/sql"
	"fmt"
)

// EnsureSchema 创建 account_balances 与 custodial_wallets 表。
func EnsureSchema(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS account_balances (
			chain_id BIGINT NOT NULL,
			address VARCHAR(42) NOT NULL,
			token_address VARCHAR(42) NOT NULL DEFAULT '',
			balance_wei VARCHAR(100) NOT NULL,
			block_number BIGINT UNSIGNED NOT NULL DEFAULT 0,
			source_tx_hash VARCHAR(66) NULL,
			updated_at DATETIME(3) NOT NULL,
			PRIMARY KEY (chain_id, address, token_address),
			INDEX idx_bal_address (chain_id, address),
			INDEX idx_bal_updated (chain_id, updated_at)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS custodial_wallets (
			chain_id BIGINT NOT NULL,
			address VARCHAR(42) NOT NULL,
			user_id VARCHAR(64) NULL,
			label VARCHAR(128) NULL,
			wallet_type ENUM('hot','deposit','treasury') NOT NULL DEFAULT 'deposit',
			enabled TINYINT(1) NOT NULL DEFAULT 1,
			created_at DATETIME(3) NOT NULL,
			updated_at DATETIME(3) NOT NULL,
			PRIMARY KEY (chain_id, address),
			INDEX idx_wallet_user (chain_id, user_id),
			INDEX idx_wallet_enabled (chain_id, enabled, updated_at)
		) ENGINE=InnoDB`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("balance schema: %w", err)
		}
	}
	return nil
}
