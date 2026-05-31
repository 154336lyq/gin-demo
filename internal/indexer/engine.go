package indexer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/core/types"

	"gin-demo/internal/cache"
	"gin-demo/internal/config"
	"gin-demo/internal/balance"
	"gin-demo/internal/eth"
)

// Engine 链上→MySQL 索引器：WAL、事务、reorg、Outbox、通用事件解析、漏块补扫。
type Engine struct {
	cfg    *config.Config
	eth    *eth.Backend
	store  *Store
	cache  cache.Cache
	events *EventRegistry
	balSync *balance.Syncer
	jobs   chan uint64
	reorgs atomic.Uint64

	gapMu            sync.RWMutex
	lastGapScan      time.Time
	lastGapsFound    int
	lastHashMismatch int
	lastGapsEnqueued int
}

func NewEngine(cfg *config.Config, b *eth.Backend, db *sql.DB, c cache.Cache) (*Engine, error) {
	events, err := NewEventRegistry(cfg)
	if err != nil {
		return nil, err
	}
	return &Engine{
		cfg:    cfg,
		eth:    b,
		store:  NewStore(db, cfg.Eth.ChainID),
		cache:  c,
		events: events,
		jobs:   make(chan uint64, cfg.Listener.ChannelBuffer*2),
	}, nil
}

// Start 启动 worker、回填、漏块扫描与 outbox 消费者。
func (e *Engine) Start(ctx context.Context) {
	workers := e.cfg.Listener.WorkerCount
	if workers <= 0 {
		workers = 2
	}
	for i := 0; i < workers; i++ {
		go e.syncWorker(ctx, i+1)
	}
	for i := 0; i < e.cfg.Indexer.OutboxWorkers; i++ {
		go e.outboxWorker(ctx, i+1)
	}
	go e.backfillLoop(ctx)
	go e.recoverPendingLoop(ctx)
	go e.gapScanLoop(ctx)
	log.Printf("[indexer] 已启动：workers=%d confirm_depth=%d gap_scan=%ds watch_contracts=%d cache=%s",
		workers, e.cfg.Indexer.ConfirmDepth, e.cfg.Indexer.GapScanIntervalSec, e.events.WatchCount(), e.cache.BackendName())
}

func (e *Engine) SetBalanceSyncer(s *balance.Syncer) {
	e.balSync = s
}

// EnqueueHeader 将新区块头写入 WAL 并投递 worker。
func (e *Engine) EnqueueHeader(ctx context.Context, h *types.Header) {
	if h == nil {
		return
	}
	num := h.Number.Uint64()
	hash := h.Hash().Hex()
	if err := e.store.EnqueuePendingHeader(ctx, num, hash); err != nil {
		log.Printf("[indexer] 写入 pending 失败 block=%d: %v", num, err)
		return
	}
	select {
	case e.jobs <- num:
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
		log.Printf("[indexer] jobs 队列拥塞，block=%d 保留在 sync_pending_headers", num)
	}
}

// EnqueueTransferLog WS 实时路径：通用 ABI 解析后入库（与 syncBlock 内 receipt 解析互补）。
func (e *Engine) EnqueueTransferLog(ctx context.Context, lg *types.Log) {
	if lg == nil {
		return
	}
	if err := e.persistParsedLog(ctx, nil, *lg, ""); err != nil {
		log.Printf("[indexer] ws log persist: %v", err)
	}
}

func (e *Engine) syncWorker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		case num := <-e.jobs:
			if err := e.syncBlock(ctx, num); err != nil {
				log.Printf("[indexer] worker-%d sync block %d: %v", id, num, err)
			}
		}
	}
}

func (e *Engine) recoverPendingLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pending, err := e.store.ListPendingHeaders(ctx, e.cfg.Indexer.BatchSize)
			if err != nil {
				continue
			}
			for _, p := range pending {
				select {
				case e.jobs <- p.Number:
				default:
				}
			}
		}
	}
}

func (e *Engine) backfillLoop(ctx context.Context) {
	ticker := time.NewTicker(8 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.runBackfill(ctx)
		}
	}
}

func (e *Engine) runBackfill(ctx context.Context) {
	head, err := e.eth.BlockNumber(ctx)
	if err != nil {
		return
	}
	cp, err := e.store.LoadCheckpoint(ctx)
	if err != nil {
		return
	}
	start := cp.LastBlockNumber
	if start > 0 {
		start++
	}
	end := head
	if end-start > uint64(e.cfg.Indexer.BatchSize) {
		end = start + uint64(e.cfg.Indexer.BatchSize) - 1
	}
	for n := start; n <= end; n++ {
		select {
		case e.jobs <- n:
		case <-ctx.Done():
			return
		default:
			return
		}
	}
}

func (e *Engine) syncBlock(ctx context.Context, blockNum uint64) error {
	lockKey := lockKey(e.store.chainID, blockNum)
	ok, err := e.cache.SetNX(ctx, lockKey, "1", 30*time.Second)
	if err != nil {
		return fmt.Errorf("lock: %w", err)
	}
	if !ok {
		return nil
	}
	defer e.cache.Del(ctx, lockKey)

	blk, err := e.eth.BlockByNumber(ctx, blockNum)
	if err != nil {
		return err
	}
	if blk == nil {
		return fmt.Errorf("block %d not found", blockNum)
	}

	if err := e.detectAndHandleReorg(ctx, blk); err != nil {
		return err
	}

	dbTx, err := e.store.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer dbTx.Rollback()

	now := nowUTC()
	blockHash := blk.Hash().Hex()
	row := BlockRow{
		ChainID:        e.store.chainID,
		BlockNumber:    blockNum,
		BlockHash:      blockHash,
		ParentHash:     blk.ParentHash().Hex(),
		BlockTimestamp: blk.Time(),
		TxCount:        uint(len(blk.Transactions())),
		GasUsed:        blk.GasUsed(),
		Status:         "pending",
		SyncedAt:       now,
	}
	if err := e.store.UpsertBlockTx(ctx, dbTx, row); err != nil {
		return err
	}

	signer := types.LatestSignerForChainID(e.eth.ChainID())
	var balParties []balance.TxParties
	for i, tx := range blk.Transactions() {
		from, err := types.Sender(signer, tx)
		if err != nil {
			continue
		}
		txRow := TxRow{
			ChainID:     e.store.chainID,
			TxHash:      tx.Hash().Hex(),
			BlockNumber: blockNum,
			BlockHash:   blockHash,
			TxIndex:     uint(i),
			FromAddr:    strings.ToLower(from.Hex()),
			ValueWei:    tx.Value().String(),
			SyncedAt:    now,
		}
		if to := tx.To(); to != nil {
			txRow.ToAddr = sql.NullString{String: strings.ToLower(to.Hex()), Valid: true}
		}
		rc, rcErr := e.eth.TransactionReceipt(ctx, tx.Hash().Hex())
		if rcErr == nil && rc != nil {
			txRow.GasUsed = sql.NullInt64{Int64: int64(rc.GasUsed), Valid: true}
			txRow.TxStatus = sql.NullInt64{Int64: int64(rc.Status), Valid: true}
			for _, lg := range rc.Logs {
				if err := e.persistParsedLog(ctx, dbTx, *lg, blockHash); err != nil {
					return err
				}
			}
		}
		if err := e.store.UpsertTransactionTx(ctx, dbTx, txRow); err != nil {
			return err
		}
		if e.cfg.BalanceSync.Enabled && e.cfg.BalanceSync.OnIndexerTx {
			party := balance.TxParties{
				TxHash:      tx.Hash().Hex(),
				From:        strings.ToLower(from.Hex()),
				TxType:      "native",
				BlockNumber: blockNum,
			}
			if to := tx.To(); to != nil {
				party.To = strings.ToLower(to.Hex())
			}
			balParties = append(balParties, party)
		}
	}

	head, _ := e.eth.BlockNumber(ctx)
	var confirmedUpTo uint64
	if head > blockNum {
		if head-blockNum >= uint64(e.cfg.Indexer.ConfirmDepth) {
			confirmedUpTo = blockNum
		}
	} else {
		confirmedUpTo = blockNum
	}
	if confirmedUpTo > 0 {
		if err := e.store.ConfirmBlocksUpToTx(ctx, dbTx, confirmedUpTo); err != nil {
			return err
		}
	}

	cp, _ := e.store.LoadCheckpoint(ctx)
	if blockNum > cp.LastBlockNumber {
		if err := e.store.UpsertCheckpointTx(ctx, dbTx, blockNum, blockHash, confirmedUpTo); err != nil {
			return err
		}
	}

	if err := e.store.InsertOutboxTx(ctx, dbTx, "block_cached", map[string]any{
		"number":    blockNum,
		"hash":      blockHash,
		"tx_count":  len(blk.Transactions()),
		"gas_used":  blk.GasUsed(),
		"timestamp": blk.Time(),
	}); err != nil {
		return err
	}

	if err := e.store.DeletePendingHeaderTx(ctx, dbTx, blockNum); err != nil {
		return err
	}

	if err := dbTx.Commit(); err != nil {
		return err
	}
	if e.balSync != nil && len(balParties) > 0 {
		for _, p := range balParties {
			e.balSync.RefreshForTxAsync(p)
		}
	}
	log.Printf("[indexer] 已同步 block #%d txs=%d", blockNum, len(blk.Transactions()))
	return nil
}

func (e *Engine) persistParsedLog(ctx context.Context, dbTx *sql.Tx, lg types.Log, blockHash string) error {
	if e.events == nil || e.events.WatchCount() == 0 {
		return nil
	}
	parsed, err := e.events.ParseLog(lg)
	if err != nil {
		return err
	}
	if parsed == nil {
		return nil
	}
	argsJSON, err := parsed.ArgsJSON()
	if err != nil {
		return err
	}
	topicsJSON, err := json.Marshal(parsed.Topics)
	if err != nil {
		return err
	}
	row := EventLogRow{
		ChainID:         e.store.chainID,
		TxHash:          lg.TxHash.Hex(),
		LogIndex:        uint(lg.Index),
		BlockNumber:     lg.BlockNumber,
		BlockHash:       blockHash,
		ContractAddress: parsed.ContractAddress,
		EventName:       parsed.EventName,
		Topic0:          parsed.Topic0,
		TopicsJSON:      string(topicsJSON),
		ArgsJSON:        argsJSON,
		SyncedAt:        nowUTC(),
	}
	if blockHash == "" {
		if blk, err := e.eth.BlockByNumber(ctx, lg.BlockNumber); err == nil && blk != nil {
			row.BlockHash = blk.Hash().Hex()
		}
	}

	ownTx := dbTx == nil
	if ownTx {
		dbTx, err = e.store.BeginTx(ctx)
		if err != nil {
			return err
		}
		defer dbTx.Rollback()
	}
	if err := e.store.UpsertEventLogTx(ctx, dbTx, row); err != nil {
		return err
	}
	outboxPayload := map[string]any{
		"tx_hash":      row.TxHash,
		"block_number": row.BlockNumber,
		"event_name":   row.EventName,
		"contract":     row.ContractAddress,
		"args":         parsed.Args,
	}
	if err := e.store.InsertOutboxTx(ctx, dbTx, "event_cached", outboxPayload); err != nil {
		return err
	}
	if parsed.EventName == "Transfer" && e.balSync != nil && e.cfg.BalanceSync.Enabled && e.cfg.BalanceSync.OnIndexerTx {
		from, _ := parsed.Args["from"].(string)
		to, _ := parsed.Args["to"].(string)
		e.balSync.RefreshForTxAsync(balance.TxParties{
			TxHash:      lg.TxHash.Hex(),
			From:        from,
			To:          to,
			TokenAddr:   parsed.ContractAddress,
			TxType:      "erc20",
			BlockNumber: lg.BlockNumber,
		})
	}
	if ownTx {
		return dbTx.Commit()
	}
	return nil
}

func (e *Engine) detectAndHandleReorg(ctx context.Context, blk *types.Block) error {
	if blk.NumberU64() == 0 {
		return nil
	}
	prevNum := blk.NumberU64() - 1
	prev, err := e.store.GetBlockByNumber(ctx, prevNum)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if strings.EqualFold(prev.BlockHash, blk.ParentHash().Hex()) {
		return nil
	}

	log.Printf("[indexer] 检测到 reorg：block %d parent 期望 %s 实际库中 %s，回滚自 %d",
		blk.NumberU64(), blk.ParentHash().Hex(), prev.BlockHash, prevNum)

	tx, err := e.store.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := e.store.MarkOrphanedFromTx(ctx, tx, prevNum); err != nil {
		return err
	}
	if err := e.store.RollbackCheckpointTx(ctx, tx, prevNum, blk.ParentHash().Hex()); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	e.cache.Del(ctx, cacheHeadKey(e.store.chainID))
	e.reorgs.Add(1)

	blockNum := blk.NumberU64()
	blockHash := blk.Hash().Hex()
	_ = e.store.EnqueuePendingHeader(ctx, blockNum, blockHash)
	select {
	case e.jobs <- blockNum:
	default:
	}
	return nil
}

func (e *Engine) outboxWorker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		batch, err := e.store.ClaimOutboxBatch(ctx, 10)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		if len(batch) == 0 {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		for _, item := range batch {
			if err := e.processOutbox(ctx, item); err != nil {
				log.Printf("[indexer] outbox worker-%d id=%d: %v", id, item.ID, err)
			}
		}
	}
}

func (e *Engine) processOutbox(ctx context.Context, item OutboxRow) error {
	var err error
	switch item.EventType {
	case "block_cached":
		err = e.cache.Set(ctx, cacheBlockKey(item.ChainID, parseBlockNum(item.Payload)), string(item.Payload), 10*time.Minute)
		if err == nil {
			var p struct {
				Number uint64 `json:"number"`
			}
			_ = json.Unmarshal(item.Payload, &p)
			_ = e.cache.Set(ctx, cacheHeadKey(item.ChainID), fmt.Sprintf("%d", p.Number), 5*time.Minute)
		}
	case "transfer_cached", "event_cached":
		key := fmt.Sprintf("indexer:event:%d:%s:%s", item.ChainID, parseEventName(item.Payload), parseTxHash(item.Payload))
		err = e.cache.Set(ctx, key, string(item.Payload), 30*time.Minute)
	default:
		err = fmt.Errorf("unknown event_type %s", item.EventType)
	}

	if err == nil {
		return e.store.MarkOutboxDone(ctx, item.ID)
	}

	retry := item.RetryCount + 1
	fail := retry >= uint(e.cfg.Indexer.MaxRetries)
	next := time.Now().Add(time.Duration(retry) * time.Second)
	return e.store.MarkOutboxRetry(ctx, item.ID, retry, next, fail)
}

func parseBlockNum(raw json.RawMessage) uint64 {
	var p struct {
		Number uint64 `json:"number"`
	}
	_ = json.Unmarshal(raw, &p)
	return p.Number
}

func parseTxHash(raw json.RawMessage) string {
	var p struct {
		TxHash string `json:"tx_hash"`
	}
	_ = json.Unmarshal(raw, &p)
	return p.TxHash
}

func parseEventName(raw json.RawMessage) string {
	var p struct {
		EventName string `json:"event_name"`
	}
	_ = json.Unmarshal(raw, &p)
	return p.EventName
}

func (e *Engine) Status(ctx context.Context) (Status, error) {
	cp, err := e.store.LoadCheckpoint(ctx)
	if err != nil {
		return Status{}, err
	}
	head, _ := e.eth.BlockNumber(ctx)
	pending, _ := e.store.CountPendingHeaders(ctx)
	outbox, _ := e.store.CountPendingOutbox(ctx)
	blocks, _ := e.store.CountBlocks(ctx)
	txs, _ := e.store.CountTxs(ctx)
	events, _ := e.store.CountEvents(ctx)

	var lag uint64
	if head > cp.LastBlockNumber {
		lag = head - cp.LastBlockNumber
	}

	e.gapMu.RLock()
	lastGap := e.lastGapScan
	gapsFound := e.lastGapsFound
	hashMismatch := e.lastHashMismatch
	gapsEnqueued := e.lastGapsEnqueued
	e.gapMu.RUnlock()

	return Status{
		Enabled:              true,
		CacheBackend:         e.cache.BackendName(),
		ChainID:              e.cfg.Eth.ChainID,
		LastBlockNumber:      cp.LastBlockNumber,
		LastBlockHash:        cp.LastBlockHash,
		ConfirmedBlockNumber: cp.ConfirmedBlockNumber,
		HeadBlockNumber:      head,
		LagBlocks:            lag,
		PendingHeaders:       pending,
		PendingOutbox:        outbox,
		BlocksSynced:         blocks,
		TxsSynced:            txs,
		EventsSynced:         events,
		WatchContracts:       e.events.WatchCount(),
		ReorgsHandled:        e.reorgs.Load(),
		LastGapScanAt:        lastGap,
		LastGapsFound:        gapsFound,
		LastHashMismatches:   hashMismatch,
		LastGapsEnqueued:     gapsEnqueued,
	}, nil
}

func (e *Engine) GetCachedBlock(ctx context.Context, num uint64) (string, error) {
	return e.cache.Get(ctx, cacheBlockKey(e.store.chainID, num))
}

func (e *Engine) Store() *Store { return e.store }
