package balance

import (
	"context"
	"log"
	"strings"

	"github.com/ethereum/go-ethereum/common"

	"gin-demo/internal/config"
	"gin-demo/internal/eth"
)

// Syncer 托管/交易所场景：对注册地址 RPC 拉余额写入 account_balances。
type Syncer struct {
	cfg      *config.Config
	eth      *eth.Backend
	store    *Store
	registry *Registry
}

func NewSyncer(cfg *config.Config, b *eth.Backend, store *Store, registry *Registry) *Syncer {
	return &Syncer{cfg: cfg, eth: b, store: store, registry: registry}
}

func (s *Syncer) Store() *Store       { return s.store }
func (s *Syncer) Registry() *Registry { return s.registry }

// RefreshForTx 根据已确认交易刷新 from/to 余额；custodial_only 时仅刷新已注册地址。
func (s *Syncer) RefreshForTx(ctx context.Context, p TxParties) {
	if s == nil || s.store == nil {
		return
	}
	from, to := s.filterParties(p.From, p.To)
	if from == "" && to == "" {
		return
	}
	if p.TxType == "erc20" && p.TokenAddr != "" {
		token := common.HexToAddress(p.TokenAddr)
		if from != "" {
			s.refreshERC20(ctx, token, from, p.TxHash, p.BlockNumber)
		}
		if to != "" {
			s.refreshERC20(ctx, token, to, p.TxHash, p.BlockNumber)
		}
		return
	}
	if from != "" {
		s.refreshNative(ctx, from, p.TxHash, p.BlockNumber)
	}
	if to != "" {
		s.refreshNative(ctx, to, p.TxHash, p.BlockNumber)
	}
}

func (s *Syncer) filterParties(from, to string) (string, string) {
	if s.cfg == nil || !s.cfg.BalanceSync.CustodialOnly || s.registry == nil {
		return from, to
	}
	if from != "" && !s.registry.IsRegistered(from) {
		from = ""
	}
	if to != "" && !s.registry.IsRegistered(to) {
		to = ""
	}
	return from, to
}

// RefreshForTxAsync 异步刷新，不阻塞 Tracker / Indexer。
func (s *Syncer) RefreshForTxAsync(p TxParties) {
	go func() {
		s.RefreshForTx(context.Background(), p)
	}()
}

// RefreshWallet 刷新某托管地址的原生币 + 配置中的 watch_tokens。
func (s *Syncer) RefreshWallet(ctx context.Context, address string) error {
	if s == nil || s.store == nil {
		return nil
	}
	address = strings.TrimSpace(address)
	if !common.IsHexAddress(address) {
		return nil
	}
	if err := s.RefreshNative(ctx, address, "", 0); err != nil {
		return err
	}
	for _, tok := range s.cfg.WatchTokenAddresses() {
		if err := s.RefreshERC20(ctx, common.HexToAddress(tok), address, "", 0); err != nil {
			log.Printf("[balance] refresh wallet erc20 addr=%s token=%s: %v", address, tok, err)
		}
	}
	return nil
}

// RefreshWalletAsync 注册地址后异步做首次快照。
func (s *Syncer) RefreshWalletAsync(address string) {
	go func() {
		if err := s.RefreshWallet(context.Background(), address); err != nil {
			log.Printf("[balance] initial refresh %s: %v", address, err)
		}
	}()
}

// RefreshAllRegistered 全量刷新所有 enabled 托管地址（backfill）。
func (s *Syncer) RefreshAllRegistered(ctx context.Context) (int, error) {
	if s == nil || s.store == nil {
		return 0, nil
	}
	addrs, err := s.store.ListEnabledAddresses(ctx, 0)
	if err != nil {
		return 0, err
	}
	for _, addr := range addrs {
		if err := s.RefreshWallet(ctx, addr); err != nil {
			log.Printf("[balance] backfill %s: %v", addr, err)
		}
	}
	return len(addrs), nil
}

// RefreshNative 刷新单地址原生币余额。
func (s *Syncer) RefreshNative(ctx context.Context, address, sourceTx string, blockNum uint64) error {
	if s == nil || s.store == nil {
		return nil
	}
	address = strings.TrimSpace(address)
	if !common.IsHexAddress(address) {
		return nil
	}
	bal, err := s.eth.BalanceAt(ctx, address)
	if err != nil {
		log.Printf("[balance] refresh native %s: %v", address, err)
		return err
	}
	if err := s.store.Upsert(ctx, address, NativeToken, bal.String(), sourceTx, blockNum); err != nil {
		log.Printf("[balance] upsert native %s: %v", address, err)
		return err
	}
	return nil
}

// RefreshERC20 刷新单地址某 ERC-20 余额。
func (s *Syncer) RefreshERC20(ctx context.Context, token common.Address, holder, sourceTx string, blockNum uint64) error {
	if s == nil || s.store == nil {
		return nil
	}
	holder = strings.TrimSpace(holder)
	if !common.IsHexAddress(holder) {
		return nil
	}
	bal, err := s.eth.ERC20BalanceOf(ctx, token, common.HexToAddress(holder))
	if err != nil {
		log.Printf("[balance] refresh erc20 token=%s holder=%s: %v", token.Hex(), holder, err)
		return err
	}
	if err := s.store.Upsert(ctx, holder, token.Hex(), bal.String(), sourceTx, blockNum); err != nil {
		log.Printf("[balance] upsert erc20 holder=%s: %v", holder, err)
		return err
	}
	return nil
}

func (s *Syncer) refreshNative(ctx context.Context, address, sourceTx string, blockNum uint64) {
	_ = s.RefreshNative(ctx, address, sourceTx, blockNum)
}

func (s *Syncer) refreshERC20(ctx context.Context, token common.Address, holder, sourceTx string, blockNum uint64) {
	_ = s.RefreshERC20(ctx, token, holder, sourceTx, blockNum)
}
