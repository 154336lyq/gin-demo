package indexer

import (
	"context"
	"log"
	"time"
)

// gapScanLoop 定期检测块号空洞与 hash 不一致，自动补扫（生产 indexer 标配）。
func (e *Engine) gapScanLoop(ctx context.Context) {
	interval := time.Duration(e.cfg.Indexer.GapScanIntervalSec) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.runGapScan(ctx)
		}
	}
}

func (e *Engine) runGapScan(ctx context.Context) {
	head, err := e.eth.BlockNumber(ctx)
	if err != nil {
		return
	}
	cp, err := e.store.LoadCheckpoint(ctx)
	if err != nil {
		return
	}

	window := uint64(e.cfg.Indexer.HashVerifyWindow)
	start := uint64(0)
	if head > window {
		start = head - window
	}
	if cp.LastBlockNumber > 0 && cp.LastBlockNumber+1 < start {
		start = cp.LastBlockNumber + 1
	}
	if start > head {
		return
	}

	synced, err := e.store.MapSyncedBlocksInRange(ctx, start, head)
	if err != nil {
		log.Printf("[indexer/gap] 读取已同步块失败: %v", err)
		return
	}

	var gaps []uint64
	var hashMismatch []uint64
	limit := e.cfg.Indexer.BatchSize

	for n := start; n <= head && len(gaps) < limit; n++ {
		meta, ok := synced[n]
		if !ok {
			gaps = append(gaps, n)
			continue
		}
		blk, err := e.eth.BlockByNumber(ctx, n)
		if err != nil || blk == nil {
			continue
		}
		rpcHash := blk.Hash().Hex()
		if !equalHash(meta.Hash, rpcHash) {
			hashMismatch = append(hashMismatch, n)
			if len(gaps)+len(hashMismatch) >= limit {
				break
			}
		}
	}

	enqueued := 0
	for _, n := range append(gaps, hashMismatch...) {
		hash := ""
		if blk, err := e.eth.BlockByNumber(ctx, n); err == nil && blk != nil {
			hash = blk.Hash().Hex()
		}
		if err := e.store.EnqueuePendingHeader(ctx, n, hash); err != nil {
			continue
		}
		select {
		case e.jobs <- n:
			enqueued++
		default:
		}
	}

	e.gapMu.Lock()
	e.lastGapScan = time.Now().UTC()
	e.lastGapsFound = len(gaps)
	e.lastHashMismatch = len(hashMismatch)
	e.lastGapsEnqueued = enqueued
	e.gapMu.Unlock()

	if len(gaps) > 0 || len(hashMismatch) > 0 {
		log.Printf("[indexer/gap] head=%d range=[%d,%d] missing=%d hash_mismatch=%d enqueued=%d",
			head, start, head, len(gaps), len(hashMismatch), enqueued)
	}

	_ = e.store.InsertGapScanAudit(ctx, GapScanAudit{
		ChainID:        e.store.chainID,
		HeadBlock:      head,
		ScanFrom:       start,
		ScanTo:         head,
		GapsFound:      len(gaps),
		HashMismatches: len(hashMismatch),
		GapsEnqueued:   enqueued,
	})
}

func equalHash(a, b string) bool {
	return stringsEqualFold(trim0x(a), trim0x(b))
}

func trim0x(s string) string {
	if len(s) >= 2 && (s[0:2] == "0x" || s[0:2] == "0X") {
		return s[2:]
	}
	return s
}

func stringsEqualFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'F' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'F' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}
