// Package eth 封装 go-ethereum 客户端：区块/交易/余额查询与链 ID 校验。
package eth

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"

	"gin-demo/internal/config"
)

// Backend 同时持有 HTTP 与可选 WebSocket 客户端：
// - 查询类 RPC 优先使用 HTTP，语义稳定；
// - 订阅类必须使用 WS（见 listener.go）。
type Backend struct {
	cfg  *config.Config
	http *ethclient.Client
	ws   *ethclient.Client
}

// NewBackend 连接节点；WS 失败时仅记录警告（仍可查询区块，但无订阅能力）。
func NewBackend(cfg *config.Config) (*Backend, error) {
	httpc, err := ethclient.Dial(cfg.Eth.HTTPRPC)
	if err != nil {
		return nil, fmt.Errorf("dial http rpc %s: %w", cfg.Eth.HTTPRPC, err)
	}

	b := &Backend{cfg: cfg, http: httpc}
	if cfg.Eth.WSRPC != "" {
		wsc, err := ethclient.Dial(cfg.Eth.WSRPC)
		if err != nil {
			return nil, fmt.Errorf("dial ws rpc %s: %w", cfg.Eth.WSRPC, err)
		}
		b.ws = wsc
	}
	return b, nil
}

// Close 释放连接。
func (b *Backend) Close() {
	if b.http != nil {
		b.http.Close()
	}
	if b.ws != nil {
		b.ws.Close()
	}
}

// HTTP 返回用于 eth_call、块查询等的客户端。
func (b *Backend) HTTP() *ethclient.Client { return b.http }

// WS 返回用于订阅的客户端；未配置 WS 时为 nil。
func (b *Backend) WS() *ethclient.Client { return b.ws }

// VerifyChainID 校验节点 chain id 与配置是否一致（防止误连环境）。
func (b *Backend) VerifyChainID(ctx context.Context) error {
	id, err := b.http.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("chain id: %w", err)
	}
	want := big.NewInt(b.cfg.Eth.ChainID)
	if id.Cmp(want) != 0 {
		return fmt.Errorf("chain id mismatch: node=%s want=%s", id.String(), want.String())
	}
	return nil
}

// LatestBlock 拉取最新区块（迷你浏览器数据源）。
func (b *Backend) LatestBlock(ctx context.Context) (*types.Block, error) {
	return b.http.BlockByNumber(ctx, nil)
}

// BlockByNumber 按高度查询区块。
func (b *Backend) BlockByNumber(ctx context.Context, num uint64) (*types.Block, error) {
	return b.http.BlockByNumber(ctx, new(big.Int).SetUint64(num))
}

// BlockByHash 按区块哈希查询完整区块。
func (b *Backend) BlockByHash(ctx context.Context, h common.Hash) (*types.Block, error) {
	return b.http.BlockByHash(ctx, h)
}

// TransactionReceipt 解析交易回执（状态、Gas、日志等）。
func (b *Backend) TransactionReceipt(ctx context.Context, txHash string) (*types.Receipt, error) {
	return b.http.TransactionReceipt(ctx, common.HexToHash(txHash))
}

// BalanceAt 查询账户余额（Wei）。
func (b *Backend) BalanceAt(ctx context.Context, addr string) (*big.Int, error) {
	return b.http.BalanceAt(ctx, common.HexToAddress(addr), nil)
}

// PendingNonceAt 演示 pending 相关 RPC（可选扩展）。
func (b *Backend) PendingNonceAt(ctx context.Context, addr common.Address) (uint64, error) {
	return b.http.PendingNonceAt(ctx, addr)
}

// SendRawTransaction 占位：演示节点转发路径（本项目默认不发交易，避免私钥管理）。
func (b *Backend) SendRawTransaction(ctx context.Context, tx *types.Transaction) error {
	return b.http.SendTransaction(ctx, tx)
}

// TransactionByHash 查询已上链或 pending 交易（依节点实现）。
func (b *Backend) TransactionByHash(ctx context.Context, h common.Hash) (*types.Transaction, bool, error) {
	return b.http.TransactionByHash(ctx, h)
}

// ChainID 返回配置中的链 ID（用于交易签名者还原 from 地址）。
func (b *Backend) ChainID() *big.Int {
	return big.NewInt(b.cfg.Eth.ChainID)
}
