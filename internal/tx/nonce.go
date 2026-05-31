package tx

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"gin-demo/internal/eth"
)

// NonceAllocator DB 级 nonce 分配（SELECT FOR UPDATE），支持多实例部署。
type NonceAllocator struct {
	db      *sql.DB
	chainID int64
	eth     *eth.Backend
}

func NewNonceAllocator(db *sql.DB, chainID int64, b *eth.Backend) *NonceAllocator {
	return &NonceAllocator{db: db, chainID: chainID, eth: b}
}

// Allocate 在事务内锁定 wallet_nonce 行并返回可用 nonce，同时递增 next_nonce。
func (n *NonceAllocator) Allocate(ctx context.Context, addr string) (uint64, error) {
	addr = strings.ToLower(addr)
	tx, err := n.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var nextNonce uint64
	err = tx.QueryRowContext(ctx, `
		SELECT next_nonce FROM wallet_nonce
		WHERE chain_id = ? AND address = ? FOR UPDATE
	`, n.chainID, addr).Scan(&nextNonce)

	if err == sql.ErrNoRows {
		onChain, err := n.eth.PendingNonceAt(ctx, common.HexToAddress(addr))
		if err != nil {
			return 0, fmt.Errorf("sync nonce from chain: %w", err)
		}
		now := time.Now().UTC()
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO wallet_nonce (chain_id, address, next_nonce, updated_at)
			VALUES (?, ?, ?, ?)
		`, n.chainID, addr, onChain+1, now); err != nil {
			return 0, err
		}
		if err := tx.Commit(); err != nil {
			return 0, err
		}
		return onChain, nil
	}
	if err != nil {
		return 0, err
	}

	allocated := nextNonce
	if _, err := tx.ExecContext(ctx, `
		UPDATE wallet_nonce SET next_nonce = ?, updated_at = ? WHERE chain_id = ? AND address = ?
	`, allocated+1, time.Now().UTC(), n.chainID, addr); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return allocated, nil
}

// SyncFromChain 将 DB nonce 追平到链上 pending nonce（运维/修复用）。
func (n *NonceAllocator) SyncFromChain(ctx context.Context, addr string) (uint64, error) {
	addr = strings.ToLower(addr)
	onChain, err := n.eth.PendingNonceAt(ctx, common.HexToAddress(addr))
	if err != nil {
		return 0, err
	}
	now := time.Now().UTC()
	_, err = n.db.ExecContext(ctx, `
		INSERT INTO wallet_nonce (chain_id, address, next_nonce, updated_at)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE next_nonce = GREATEST(next_nonce, VALUES(next_nonce)), updated_at = VALUES(updated_at)
	`, n.chainID, addr, onChain, now)
	return onChain, err
}
