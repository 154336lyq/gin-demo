package webhook

import (
	"context"
	"database/sql"
	"fmt"
)

func EnsureSchema(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS webhook_merchants (
			chain_id BIGINT NOT NULL,
			merchant_id VARCHAR(64) NOT NULL,
			name VARCHAR(128) NOT NULL DEFAULT '',
			webhook_url VARCHAR(512) NOT NULL,
			secret VARCHAR(128) NOT NULL DEFAULT '',
			ledger_user_id VARCHAR(64) NOT NULL DEFAULT '',
			enabled TINYINT(1) NOT NULL DEFAULT 1,
			created_at DATETIME(3) NOT NULL,
			updated_at DATETIME(3) NOT NULL,
			PRIMARY KEY (chain_id, merchant_id)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS webhook_bindings (
			chain_id BIGINT NOT NULL,
			merchant_id VARCHAR(64) NOT NULL,
			payer_user_id VARCHAR(64) NOT NULL,
			created_at DATETIME(3) NOT NULL,
			PRIMARY KEY (chain_id, merchant_id, payer_user_id),
			INDEX idx_binding_payer (chain_id, payer_user_id)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS webhook_outbox (
			id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
			chain_id BIGINT NOT NULL,
			merchant_id VARCHAR(64) NOT NULL,
			event_type VARCHAR(32) NOT NULL,
			idempotency_key VARCHAR(128) NOT NULL,
			payload JSON NOT NULL,
			status ENUM('pending','processing','done','failed') NOT NULL DEFAULT 'pending',
			retry_count INT UNSIGNED NOT NULL DEFAULT 0,
			next_retry_at DATETIME(3) NOT NULL,
			created_at DATETIME(3) NOT NULL,
			updated_at DATETIME(3) NOT NULL,
			UNIQUE KEY uk_webhook_idem (chain_id, idempotency_key),
			INDEX idx_webhook_claim (status, next_retry_at, id)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS webhook_deliveries (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			chain_id BIGINT NOT NULL,
			outbox_id BIGINT UNSIGNED NOT NULL,
			merchant_id VARCHAR(64) NOT NULL,
			attempt INT NOT NULL DEFAULT 1,
			http_status INT NOT NULL DEFAULT 0,
			latency_ms INT NOT NULL DEFAULT 0,
			response_body VARCHAR(512) NULL,
			error_message VARCHAR(512) NULL,
			created_at DATETIME(3) NOT NULL,
			INDEX idx_delivery_outbox (outbox_id),
			INDEX idx_delivery_merchant_time (chain_id, merchant_id, created_at)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS merchant_payments (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			chain_id BIGINT NOT NULL,
			merchant_id VARCHAR(64) NOT NULL,
			order_id VARCHAR(64) NOT NULL,
			payer_user_id VARCHAR(64) NOT NULL,
			token_address VARCHAR(42) NOT NULL DEFAULT '',
			amount_wei VARCHAR(100) NOT NULL,
			status ENUM('completed','failed') NOT NULL DEFAULT 'completed',
			idempotency_key VARCHAR(128) NOT NULL,
			created_at DATETIME(3) NOT NULL,
			UNIQUE KEY uk_payment_order (chain_id, merchant_id, order_id),
			UNIQUE KEY uk_payment_idem (chain_id, idempotency_key)
		) ENGINE=InnoDB`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("webhook schema: %w", err)
		}
	}
	return nil
}
