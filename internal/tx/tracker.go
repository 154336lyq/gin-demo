package tx

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"gin-demo/internal/balance"
	"gin-demo/internal/config"
	"gin-demo/internal/eth"
)

// Tracker 轮询 pending 交易 receipt，更新 confirmed / failed / dropped / replaced。
type Tracker struct {
	cfg      *config.Config
	eth      *eth.Backend
	store    *Store
	balSync  *balance.Syncer
	withdraw WithdrawHandler
	wakeCh   chan struct{}
}

func NewTracker(cfg *config.Config, b *eth.Backend, store *Store, balSync *balance.Syncer) *Tracker {
	return &Tracker{
		cfg:     cfg,
		eth:     b,
		store:   store,
		balSync: balSync,
		wakeCh:  make(chan struct{}, 1),
	}
}

func (t *Tracker) SetWithdrawHandler(h WithdrawHandler) {
	t.withdraw = h
}

func (t *Tracker) Store() *Store { return t.store }

// Start 启动轮询 worker、WS 唤醒、outbox 消费者与 reconcile。
func (t *Tracker) Start(ctx context.Context) {
	go t.pollLoop(ctx)
	if t.eth.WS() != nil {
		go t.headWakeLoop(ctx)
	}
	workers := t.cfg.TxTracker.OutboxWorkers
	if workers <= 0 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		go t.outboxWorker(ctx, i+1)
	}
	go NewReconciler(t.cfg, t.eth, t.store).Start(ctx)
	log.Printf("[tx/tracker] 已启动 poll=%ds confirm_depth=%d batch=%d outbox_workers=%d reconcile=%ds eip1559=%v",
		t.cfg.TxTracker.PollIntervalSec, t.cfg.TxTracker.ConfirmDepth, t.cfg.TxTracker.BatchSize,
		workers, t.cfg.TxTracker.ReconcileIntervalSec, t.cfg.TxTracker.UseEIP1559)
}

func (t *Tracker) headWakeLoop(ctx context.Context) {
	headers := make(chan *types.Header)
	backoff := time.Duration(t.cfg.Listener.ReconnectBaseMS) * time.Millisecond
	for {
		if ctx.Err() != nil {
			return
		}
		sub, err := t.eth.WS().SubscribeNewHead(ctx, headers)
		if err != nil {
			time.Sleep(backoff)
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Duration(t.cfg.Listener.ReconnectBaseMS) * time.Millisecond
	inner:
		for {
			select {
			case <-ctx.Done():
				sub.Unsubscribe()
				return
			case err := <-sub.Err():
				log.Printf("[tx/tracker] newHead 订阅异常: %v", err)
				sub.Unsubscribe()
				break inner
			case <-headers:
				t.wake()
			}
		}
	}
}

func (t *Tracker) wake() {
	select {
	case t.wakeCh <- struct{}{}:
	default:
	}
}

func (t *Tracker) pollLoop(ctx context.Context) {
	interval := time.Duration(t.cfg.TxTracker.PollIntervalSec) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.runOnce(ctx)
		case <-t.wakeCh:
			t.runOnce(ctx)
		}
	}
}

func (t *Tracker) runOnce(ctx context.Context) {
	batch, err := t.store.ListTrackable(ctx, t.cfg.TxTracker.BatchSize)
	if err != nil {
		log.Printf("[tx/tracker] list pending: %v", err)
		return
	}
	if len(batch) == 0 {
		return
	}
	head, err := t.eth.HTTP().BlockNumber(ctx)
	if err != nil {
		return
	}
	for _, row := range batch {
		t.trackOne(ctx, row, head)
	}
}

func (t *Tracker) trackOne(ctx context.Context, row Row, head uint64) {
	if row.Status == StatusSubmitting || row.Status == StatusBroadcastFailed {
		if _, _, err := t.eth.TransactionByHash(ctx, common.HexToHash(row.TxHash)); err == nil {
			_ = t.store.MarkBroadcastPending(ctx, row.TxHash)
			row.Status = StatusPending
		} else if row.SignedRawHex != "" {
			return
		}
	}

	receipt, recErr := t.eth.TransactionReceipt(ctx, row.TxHash)
	if recErr != nil {
		if t.shouldDrop(row) {
			_ = t.store.TransitionStatus(ctx, row.TxHash, StatusDropped, 0, 0, 0, "not found in mempool or chain before timeout")
			if t.withdraw != nil && row.BizType == BizTypeWithdraw {
				_ = t.withdraw.OnWithdrawTxFailed(ctx, row.TxHash)
			}
		}
		return
	}

	gasUsed := receipt.GasUsed
	blockNum := receipt.BlockNumber.Uint64()
	var conf uint64
	if head >= blockNum {
		conf = head - blockNum + 1
	}

	if receipt.Status == types.ReceiptStatusFailed {
		_ = t.store.TransitionStatus(ctx, row.TxHash, StatusFailed, blockNum, conf, gasUsed, "execution reverted")
		if t.withdraw != nil && row.BizType == BizTypeWithdraw {
			_ = t.withdraw.OnWithdrawTxFailed(ctx, row.TxHash)
		}
		return
	}

	status := StatusPending
	wasConfirmed := false
	if conf >= uint64(t.cfg.TxTracker.ConfirmDepth) {
		status = StatusConfirmed
		wasConfirmed = true
		_ = t.store.MarkReplacedSiblings(ctx, row.FromAddr, row.Nonce, row.TxHash)
	}
	if err := t.store.TransitionStatus(ctx, row.TxHash, status, blockNum, conf, gasUsed, ""); err != nil {
		return
	}
	if wasConfirmed && t.balSync != nil && t.cfg.BalanceSync.Enabled && t.cfg.BalanceSync.OnTxConfirmed {
		t.balSync.RefreshForTxAsync(balance.TxParties{
			TxHash:      row.TxHash,
			From:        row.FromAddr,
			To:          row.ToAddr,
			TokenAddr:   row.TokenAddr,
			TxType:      row.TxType,
			BlockNumber: blockNum,
		})
	}
	if wasConfirmed && t.withdraw != nil && row.BizType == BizTypeWithdraw {
		_ = t.withdraw.OnWithdrawTxConfirmed(ctx, row.TxHash)
	}
}

func (t *Tracker) shouldDrop(row Row) bool {
	maxAge := time.Duration(t.cfg.TxTracker.MaxPendingHours) * time.Hour
	return time.Since(row.CreatedAt) > maxAge
}

// RefreshOne 立即刷新单笔（API 查询时可选调用）。
func (t *Tracker) RefreshOne(ctx context.Context, hash string) (Row, error) {
	row, err := t.store.GetByHash(ctx, hash)
	if err != nil {
		return Row{}, err
	}
	if row.Status != StatusPending && row.Status != StatusSubmitting && row.Status != StatusBroadcastFailed {
		return row, nil
	}
	head, err := t.eth.HTTP().BlockNumber(ctx)
	if err != nil {
		return row, err
	}
	t.trackOne(ctx, row, head)
	return t.store.GetByHash(ctx, hash)
}

func (t *Tracker) outboxWorker(ctx context.Context, id int) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			items, err := t.store.ClaimOutboxBatch(ctx, 20)
			if err != nil {
				log.Printf("[tx/outbox] worker-%d claim: %v", id, err)
				continue
			}
			for _, it := range items {
				t.publishOutbox(ctx, id, it)
			}
		}
	}
}

func (t *Tracker) publishOutbox(ctx context.Context, workerID int, it OutboxItem) {
	// 生产环境在此推送 Webhook / Kafka / 业务回调；演示模式打结构化日志。
	log.Printf("[tx/outbox] worker-%d notify biz_id=%s tx=%s %s→%s payload=%s",
		workerID, it.BizID, it.TxHash, it.OldStatus, it.NewStatus, string(it.Payload))
	if err := t.store.MarkOutboxDone(ctx, it.ID); err != nil {
		log.Printf("[tx/outbox] mark done id=%d: %v", it.ID, err)
	}
}

var ErrNotFound = errors.New("tx not found in tracker")
