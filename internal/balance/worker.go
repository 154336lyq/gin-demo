package balance

import (
	"context"
	"log"
	"time"

	"gin-demo/internal/config"
)

// StartBackfillWorker 定时全量刷新所有托管地址余额（交易所 backfill）。
func StartBackfillWorker(ctx context.Context, cfg *config.Config, syncer *Syncer) {
	if syncer == nil || cfg == nil || !cfg.BalanceSync.Enabled {
		return
	}
	sec := cfg.BalanceSync.BackfillIntervalSec
	if sec <= 0 {
		return
	}
	interval := time.Duration(sec) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	log.Printf("[balance/backfill] 已启动 interval=%ds custodial_only=%v",
		sec, cfg.BalanceSync.CustodialOnly)

	run := func() {
		n, err := syncer.RefreshAllRegistered(ctx)
		if err != nil {
			log.Printf("[balance/backfill] error: %v", err)
			return
		}
		if n > 0 {
			log.Printf("[balance/backfill] refreshed %d wallets", n)
		}
	}
	run()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}
