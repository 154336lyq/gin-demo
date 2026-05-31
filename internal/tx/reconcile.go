package tx

import (
	"context"
	"log"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"gin-demo/internal/config"
	"gin-demo/internal/eth"
)

// Reconciler 后台重试 submitting/broadcast_failed，并探测链上已存在但未标记 pending 的记录。
type Reconciler struct {
	cfg   *config.Config
	eth   *eth.Backend
	store *Store
}

func NewReconciler(cfg *config.Config, b *eth.Backend, store *Store) *Reconciler {
	return &Reconciler{cfg: cfg, eth: b, store: store}
}

func (r *Reconciler) Start(ctx context.Context) {
	interval := time.Duration(r.cfg.TxTracker.ReconcileIntervalSec) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	log.Printf("[tx/reconcile] 已启动 interval=%ds grace=%ds max_retries=%d",
		r.cfg.TxTracker.ReconcileIntervalSec, r.cfg.TxTracker.ReconcileGraceSec, r.cfg.TxTracker.BroadcastMaxRetries)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.runOnce(ctx)
		}
	}
}

func (r *Reconciler) runOnce(ctx context.Context) {
	batch, err := r.store.ListReconcileBatch(ctx,
		r.cfg.TxTracker.ReconcileGraceSec,
		r.cfg.TxTracker.BroadcastMaxRetries,
		r.cfg.TxTracker.BatchSize,
	)
	if err != nil {
		log.Printf("[tx/reconcile] list: %v", err)
		return
	}
	for _, row := range batch {
		r.reconcileOne(ctx, row)
	}
}

func (r *Reconciler) reconcileOne(ctx context.Context, row Row) {
	hash := common.HexToHash(row.TxHash)

	if _, pending, err := r.eth.TransactionByHash(ctx, hash); err == nil {
		if pending || row.Status == StatusSubmitting || row.Status == StatusBroadcastFailed {
			if err := r.store.MarkBroadcastPending(ctx, row.TxHash); err != nil {
				log.Printf("[tx/reconcile] mark pending %s: %v", row.TxHash, err)
			}
			return
		}
	}

	if row.SignedRawHex == "" {
		return
	}

	tx, err := r.eth.DecodeRawTxHex(row.SignedRawHex)
	if err != nil {
		log.Printf("[tx/reconcile] decode %s: %v", row.TxHash, err)
		return
	}
	if err := r.eth.SendSignedTransaction(ctx, tx); err != nil {
		_ = r.store.IncrementBroadcastRetry(ctx, row.TxHash)
		_ = r.store.MarkBroadcastFailed(ctx, row.TxHash, err.Error())
		log.Printf("[tx/reconcile] broadcast retry %s (count=%d): %v", row.TxHash, row.BroadcastRetryCount+1, err)
		return
	}
	if err := r.store.MarkBroadcastPending(ctx, row.TxHash); err != nil {
		log.Printf("[tx/reconcile] mark pending %s: %v", row.TxHash, err)
	}
}
