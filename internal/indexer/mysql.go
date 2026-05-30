package indexer

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"gin-demo/internal/config"
)

// OpenMySQL 连接 MySQL 并建库；cfg.MySQL.Enabled=false 或连接失败时返回 error。
func OpenMySQL(ctx context.Context, cfg *config.Config) (*sql.DB, error) {
	if !cfg.MySQL.Enabled {
		return nil, fmt.Errorf("mysql disabled in config")
	}
	adminDSN := strings.TrimSpace(cfg.MySQL.AdminDSN)
	if adminDSN == "" {
		adminDSN = "root:123456@tcp(127.0.0.1:3306)/?parseTime=true&multiStatements=true"
	}
	dataDSN := strings.TrimSpace(cfg.MySQL.DSN)
	if dataDSN == "" {
		dataDSN = "root:123456@tcp(127.0.0.1:3306)/chain_indexer?parseTime=true&multiStatements=true"
	}

	adminDB, err := sql.Open("mysql", adminDSN)
	if err != nil {
		return nil, err
	}
	defer adminDB.Close()
	if err := adminDB.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("mysql admin ping: %w", err)
	}
	if _, err := adminDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS chain_indexer CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"); err != nil {
		return nil, fmt.Errorf("create database: %w", err)
	}

	db, err := sql.Open("mysql", dataDSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(cfg.MySQL.MaxOpen)
	db.SetMaxIdleConns(cfg.MySQL.MaxIdle)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("mysql ping: %w", err)
	}
	if err := ensureSchema(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	log.Println("[indexer] MySQL 已连接，schema 就绪")
	return db, nil
}

func ensureSchema(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS indexer_checkpoints (
			chain_id BIGINT NOT NULL PRIMARY KEY,
			last_block_number BIGINT UNSIGNED NOT NULL DEFAULT 0,
			last_block_hash VARCHAR(66) NOT NULL DEFAULT '',
			confirmed_block_number BIGINT UNSIGNED NOT NULL DEFAULT 0,
			updated_at DATETIME(3) NOT NULL
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS sync_pending_headers (
			chain_id BIGINT UNSIGNED NOT NULL,
			block_number BIGINT UNSIGNED NOT NULL,
			block_hash VARCHAR(66) NOT NULL,
			enqueued_at DATETIME(3) NOT NULL,
			PRIMARY KEY (chain_id, block_number),
			INDEX idx_pending_time (enqueued_at)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS synced_blocks (
			chain_id BIGINT UNSIGNED NOT NULL,
			block_number BIGINT UNSIGNED NOT NULL,
			block_hash VARCHAR(66) NOT NULL,
			parent_hash VARCHAR(66) NOT NULL,
			block_timestamp BIGINT UNSIGNED NOT NULL,
			tx_count INT UNSIGNED NOT NULL,
			gas_used BIGINT UNSIGNED NOT NULL,
			status ENUM('pending','confirmed','orphaned') NOT NULL DEFAULT 'pending',
			synced_at DATETIME(3) NOT NULL,
			PRIMARY KEY (chain_id, block_number),
			UNIQUE KEY uk_chain_hash (chain_id, block_hash),
			INDEX idx_chain_status (chain_id, status)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS synced_transactions (
			chain_id BIGINT UNSIGNED NOT NULL,
			tx_hash VARCHAR(66) NOT NULL,
			block_number BIGINT UNSIGNED NOT NULL,
			block_hash VARCHAR(66) NOT NULL,
			tx_index INT UNSIGNED NOT NULL,
			from_addr VARCHAR(66) NOT NULL,
			to_addr VARCHAR(66) NULL,
			value_wei VARCHAR(100) NOT NULL,
			gas_used BIGINT UNSIGNED NULL,
			tx_status TINYINT NULL,
			synced_at DATETIME(3) NOT NULL,
			PRIMARY KEY (chain_id, tx_hash),
			INDEX idx_block (chain_id, block_number),
			INDEX idx_from (chain_id, from_addr),
			INDEX idx_to (chain_id, to_addr)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS synced_transfer_logs (
			chain_id BIGINT UNSIGNED NOT NULL,
			tx_hash VARCHAR(66) NOT NULL,
			log_index INT UNSIGNED NOT NULL,
			block_number BIGINT UNSIGNED NOT NULL,
			contract_address VARCHAR(66) NOT NULL,
			from_addr VARCHAR(66) NOT NULL,
			to_addr VARCHAR(66) NOT NULL,
			value_wei VARCHAR(100) NOT NULL,
			synced_at DATETIME(3) NOT NULL,
			PRIMARY KEY (chain_id, tx_hash, log_index),
			INDEX idx_block (chain_id, block_number)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS sync_outbox (
			id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
			chain_id BIGINT UNSIGNED NOT NULL,
			event_type VARCHAR(32) NOT NULL,
			payload JSON NOT NULL,
			status ENUM('pending','processing','done','failed') NOT NULL DEFAULT 'pending',
			retry_count INT UNSIGNED NOT NULL DEFAULT 0,
			next_retry_at DATETIME(3) NOT NULL,
			created_at DATETIME(3) NOT NULL,
			updated_at DATETIME(3) NOT NULL,
			INDEX idx_outbox_poll (status, next_retry_at),
			INDEX idx_outbox_chain (chain_id, event_type)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS synced_event_logs (
			chain_id BIGINT UNSIGNED NOT NULL,
			tx_hash VARCHAR(66) NOT NULL,
			log_index INT UNSIGNED NOT NULL,
			block_number BIGINT UNSIGNED NOT NULL,
			block_hash VARCHAR(66) NOT NULL,
			contract_address VARCHAR(66) NOT NULL,
			event_name VARCHAR(120) NOT NULL,
			topic0 VARCHAR(66) NOT NULL,
			topics_json JSON NOT NULL,
			args_json JSON NOT NULL,
			synced_at DATETIME(3) NOT NULL,
			PRIMARY KEY (chain_id, tx_hash, log_index),
			INDEX idx_event_filter (chain_id, contract_address, event_name, block_number),
			INDEX idx_topic0 (chain_id, topic0, block_number)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS indexer_gap_scans (
			id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
			chain_id BIGINT UNSIGNED NOT NULL,
			head_block BIGINT UNSIGNED NOT NULL,
			scan_from BIGINT UNSIGNED NOT NULL,
			scan_to BIGINT UNSIGNED NOT NULL,
			gaps_found INT UNSIGNED NOT NULL,
			hash_mismatches INT UNSIGNED NOT NULL,
			gaps_enqueued INT UNSIGNED NOT NULL,
			scanned_at DATETIME(3) NOT NULL,
			INDEX idx_gap_chain_time (chain_id, scanned_at)
		) ENGINE=InnoDB`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("schema: %w", err)
		}
	}
	return nil
}

func nowUTC() time.Time { return time.Now().UTC() }
