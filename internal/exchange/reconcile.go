package exchange

import (
	"context"
	"math/big"
	"time"

	"gin-demo/internal/balance"
)

// Reconciler 对账：链上托管资产 vs 用户负债（available+frozen）。
type Reconciler struct {
	exchangeStore *Store
	balanceStore  *balance.Store
	chainID       int64
}

func NewReconciler(exchangeStore *Store, balanceStore *balance.Store) *Reconciler {
	return &Reconciler{
		exchangeStore: exchangeStore,
		balanceStore:  balanceStore,
		chainID:       exchangeStore.chainID,
	}
}

func (r *Reconciler) Run(ctx context.Context, tokens []string) ([]ReconcileReport, error) {
	if len(tokens) == 0 {
		tokens = []string{""}
	}
	wallets, err := r.balanceStore.ListWallets(ctx, "", "", true, 500)
	if err != nil {
		return nil, err
	}
	var reports []ReconcileReport
	for _, token := range tokens {
		report, err := r.reconcileToken(ctx, token, wallets)
		if err != nil {
			return nil, err
		}
		reports = append(reports, report)
	}
	return reports, nil
}

func (r *Reconciler) reconcileToken(ctx context.Context, token string, wallets []balance.CustodialWallet) (ReconcileReport, error) {
	onChain := weiZero()
	var details []ReconcileDetail
	for _, w := range wallets {
		if w.WalletType != balance.WalletTypeHot && w.WalletType != balance.WalletTypeTreasury {
			continue
		}
		row, err := r.balanceStore.Get(ctx, w.Address, token)
		if err != nil {
			continue
		}
		bal, err := parseWei(row.BalanceWei)
		if err != nil {
			continue
		}
		onChain = weiAdd(onChain, bal)
		details = append(details, ReconcileDetail{
			Address: w.Address, WalletType: w.WalletType, BalanceWei: row.BalanceWei,
		})
	}

	liabilities, err := r.exchangeStore.SumUserLiabilities(ctx, token)
	if err != nil {
		return ReconcileReport{}, err
	}
	liab, _ := parseWei(liabilities)
	diff := new(big.Int).Sub(onChain, liab)

	return ReconcileReport{
		ChainID:          r.chainID,
		TokenAddress:     normToken(token),
		OnChainCustodial: weiString(onChain),
		UserLiabilities:  liabilities,
		DiffWei:          diff.String(),
		OK:               diff.Sign() == 0,
		GeneratedAt:      time.Now().UTC(),
		Details:          details,
	}, nil
}
