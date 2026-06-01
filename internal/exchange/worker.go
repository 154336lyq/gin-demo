package exchange

import (
	"context"
	"log"
	"time"

	"gin-demo/internal/config"
)

// StartWorkers 启动提现广播与定时对账 worker。
func StartWorkers(ctx context.Context, cfg *config.Config, svc *Service) {
	if svc == nil || !cfg.Exchange.Enabled {
		return
	}
	go withdrawBroadcastLoop(ctx, cfg, svc)
	if cfg.Exchange.ReconcileIntervalSec > 0 {
		go reconcileLoop(ctx, cfg, svc)
	}
}

func withdrawBroadcastLoop(ctx context.Context, cfg *config.Config, svc *Service) {
	interval := 5 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			batch, err := svc.store.ListWithdraws(ctx, "", WithdrawStatusApproved, 20)
			if err != nil {
				continue
			}
			for _, w := range batch {
				if err := svc.ProcessApprovedWithdraw(ctx, w); err != nil {
					if err != ErrWithdrawAlreadyHandled {
						log.Printf("[exchange/withdraw] broadcast id=%d: %v", w.ID, err)
					}
				}
			}
		}
	}
}

func reconcileLoop(ctx context.Context, cfg *config.Config, svc *Service) {
	interval := time.Duration(cfg.Exchange.ReconcileIntervalSec) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	rec := NewReconciler(svc.store, svc.balStore)
	tokens := cfg.WatchTokenAddresses()
	tokens = append([]string{""}, tokens...)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reports, err := rec.Run(ctx, tokens)
			if err != nil {
				log.Printf("[exchange/reconcile] %v", err)
				continue
			}
			for _, r := range reports {
				if !r.OK {
					log.Printf("[exchange/reconcile] MISMATCH token=%s on_chain=%s liabilities=%s diff=%s",
						r.TokenAddress, r.OnChainCustodial, r.UserLiabilities, r.DiffWei)
				}
			}
		}
	}
}
