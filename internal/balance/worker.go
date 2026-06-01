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
	if syncer.registry != nil {
		go startRegistryReloadWorker(ctx, cfg, syncer.registry)
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

// startRegistryReloadWorker 多实例部署时定期从 DB 同步 Registry，避免实例间缓存不一致。
func startRegistryReloadWorker(ctx context.Context, cfg *config.Config, registry *Registry) {
	sec := cfg.BalanceSync.RegistryReloadSec
	if sec <= 0 {
		sec = 30
	}
	ticker := time.NewTicker(time.Duration(sec) * time.Second)
	defer ticker.Stop()
	log.Printf("[balance/registry] reload worker started interval=%ds", sec)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := registry.Reload(ctx); err != nil {
				log.Printf("[balance/registry] reload: %v", err)
			}
		}
	}
}
