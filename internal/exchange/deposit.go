package exchange

import (
	"context"
	"database/sql"
	"log"
	"strings"

	"gin-demo/internal/balance"
	"gin-demo/internal/config"
)

// DepositNotifier 充值入账后触发商户 Webhook Outbox（Transactional Outbox）。
type DepositNotifier interface {
	EnqueueDepositTx(ctx context.Context, dbTx *sql.Tx, dep Deposit) error
}

// DepositProcessor 在 Indexer 确认深度后捕获充值并入账。
type DepositProcessor struct {
	cfg      *config.Config
	store    *Store
	registry *balance.Registry
	tokens   map[string]struct{}
	webhook  DepositNotifier
}

func NewDepositProcessor(cfg *config.Config, store *Store, registry *balance.Registry) *DepositProcessor {
	tokens := make(map[string]struct{})
	for _, t := range cfg.WatchTokenAddresses() {
		tokens[strings.ToLower(t)] = struct{}{}
	}
	return &DepositProcessor{cfg: cfg, store: store, registry: registry, tokens: tokens}
}

func (d *DepositProcessor) Enabled() bool {
	return d != nil && d.cfg.Exchange.Enabled && d.cfg.Exchange.DepositEnabled
}

func (d *DepositProcessor) SetDepositNotifier(n DepositNotifier) {
	d.webhook = n
}

func (d *DepositProcessor) ConfirmDepth() int {
	if d.cfg.Exchange.ConfirmDepth > 0 {
		return d.cfg.Exchange.ConfirmDepth
	}
	return d.cfg.Indexer.ConfirmDepth
}

// CaptureNativeTx 在区块达到确认深度时捕获 native 充值。
func (d *DepositProcessor) CaptureNativeTx(ctx context.Context, dbTx *sql.Tx, blockNum uint64, txHash, from, to, valueWei string, txSuccess bool) error {
	if !d.Enabled() || !txSuccess || to == "" {
		return nil
	}
	if d.registry == nil {
		return nil
	}
	w, ok := d.registry.Get(to)
	if !ok || w.WalletType != balance.WalletTypeDeposit || !w.Enabled {
		return nil
	}
	if w.UserID == "" {
		return nil
	}
	val, err := parseWei(valueWei)
	if err != nil || val.Sign() <= 0 {
		return nil
	}
	return d.captureAndCredit(ctx, dbTx, CaptureDepositParams{
		UserID: w.UserID, DepositAddress: to, TokenAddress: "",
		AmountWei: valueWei, TxHash: txHash, LogIndex: 0, BlockNumber: blockNum,
	})
}

// CaptureERC20Transfer 捕获 ERC-20 Transfer 充值。
func (d *DepositProcessor) CaptureERC20Transfer(ctx context.Context, dbTx *sql.Tx, blockNum uint64, txHash string, logIndex uint, contract, from, to, amountWei string) error {
	if !d.Enabled() || to == "" {
		return nil
	}
	if _, ok := d.tokens[strings.ToLower(contract)]; !ok {
		return nil
	}
	if d.registry == nil {
		return nil
	}
	w, ok := d.registry.Get(to)
	if !ok || w.WalletType != balance.WalletTypeDeposit || !w.Enabled {
		return nil
	}
	if w.UserID == "" {
		return nil
	}
	val, err := parseWei(amountWei)
	if err != nil || val.Sign() <= 0 {
		return nil
	}
	return d.captureAndCredit(ctx, dbTx, CaptureDepositParams{
		UserID: w.UserID, DepositAddress: to, TokenAddress: contract,
		AmountWei: amountWei, TxHash: txHash, LogIndex: logIndex, BlockNumber: blockNum,
	})
}

func (d *DepositProcessor) captureAndCredit(ctx context.Context, dbTx *sql.Tx, p CaptureDepositParams) error {
	depID, err := d.store.InsertDepositTx(ctx, dbTx, p)
	if err != nil {
		return err
	}

	var dep Deposit
	if depID == 0 {
		dep, err = d.store.GetDepositByKeyTx(ctx, dbTx, p.TxHash, p.LogIndex)
		if err != nil {
			return err
		}
		depID = dep.ID
	} else {
		dep, err = d.store.GetDepositByKeyTx(ctx, dbTx, p.TxHash, p.LogIndex)
		if err != nil {
			return err
		}
	}

	switch dep.Status {
	case DepositStatusCredited:
		return nil
	case DepositStatusOrphaned:
		if err := d.store.resetDepositForRecreditTx(ctx, dbTx, depID, p.BlockNumber); err != nil {
			return err
		}
	case DepositStatusPending:
		// continue
	}

	hasCredit, err := d.store.hasLedgerEntryTx(ctx, dbTx, depID, LedgerDepositCredit)
	if err != nil {
		return err
	}
	if hasCredit {
		return d.store.MarkDepositCreditedTx(ctx, dbTx, depID)
	}

	if err := d.store.creditDepositTx(ctx, dbTx, p.UserID, p.TokenAddress, p.AmountWei, depID); err != nil {
		return err
	}
	if err := d.store.MarkDepositCreditedTx(ctx, dbTx, depID); err != nil {
		return err
	}
	if d.webhook != nil {
		dep, err := d.store.GetDepositByKeyTx(ctx, dbTx, p.TxHash, p.LogIndex)
		if err != nil {
			return err
		}
		if err := d.webhook.EnqueueDepositTx(ctx, dbTx, dep); err != nil {
			return err
		}
	}
	return nil
}

// HandleReorgTx 在 Indexer 同一 DB 事务内撤销 reorg 区块的充值入账。
func (d *DepositProcessor) HandleReorgTx(ctx context.Context, dbTx *sql.Tx, fromBlock uint64) error {
	if !d.Enabled() {
		return nil
	}
	deps, err := d.store.OrphanDepositsFromBlockTx(ctx, dbTx, fromBlock)
	if err != nil {
		return err
	}
	for _, dep := range deps {
		if dep.Status != DepositStatusCredited {
			continue
		}
		reversed, err := d.store.hasLedgerEntryTx(ctx, dbTx, dep.ID, LedgerDepositReverse)
		if err != nil {
			return err
		}
		if reversed {
			continue
		}
		hasCredit, err := d.store.hasLedgerEntryTx(ctx, dbTx, dep.ID, LedgerDepositCredit)
		if err != nil {
			return err
		}
		if !hasCredit {
			continue
		}
		if err := d.store.reverseDepositTx(ctx, dbTx, dep.UserID, dep.TokenAddress, dep.AmountWei, dep.ID); err != nil {
			log.Printf("[exchange/deposit] reorg reverse id=%d user=%s: %v (needs manual reconcile)", dep.ID, dep.UserID, err)
			return err
		}
	}
	if len(deps) > 0 {
		log.Printf("[exchange/deposit] reorg orphaned %d deposits from block>=%d", len(deps), fromBlock)
	}
	return nil
}

// HandleReorg 独立事务回滚（兼容非 Indexer 调用路径）。
func (d *DepositProcessor) HandleReorg(ctx context.Context, fromBlock uint64) error {
	if !d.Enabled() {
		return nil
	}
	tx, err := d.store.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := d.HandleReorgTx(ctx, tx, fromBlock); err != nil {
		return err
	}
	return tx.Commit()
}

// IsWatchToken 是否监听该 ERC-20 合约。
func (d *DepositProcessor) IsWatchToken(contract string) bool {
	_, ok := d.tokens[strings.ToLower(contract)]
	return ok
}
