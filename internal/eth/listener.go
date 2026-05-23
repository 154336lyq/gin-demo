// Package eth 的 listener 负责：订阅新区块 / ERC-20 Transfer 日志、断线重连。
//
// 本地测试网（Anvil）说明见 cmd/chain-backend/README.md。
package eth

import (
	"context"
	"log"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"gin-demo/internal/config"
	"gin-demo/internal/pipeline"
)

// ERC20TransferTopic = keccak256("Transfer(address,address,uint256)") —— 事件首 topic。
var erc20TransferTopic = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))

// Listener 将链上事件送入 pipeline（生产者），由 worker 消费。
type Listener struct {
	cfg *config.Config
	b   *Backend
	bus *pipeline.Bus
}

func NewListener(cfg *config.Config, b *Backend, bus *pipeline.Bus) *Listener {
	return &Listener{cfg: cfg, b: b, bus: bus}
}

// Run 阻塞直到 ctx 取消；内部包含「订阅失败 -> 退避重连」循环。
func (l *Listener) Run(ctx context.Context) {
	if l.b.WS() == nil {
		log.Println("[eth/listener] 未配置 ws_rpc，跳过区块/日志订阅（仍可通过 HTTP 查询）。")
		return
	}

	// 并行：新区块头订阅 +（可选）ERC-20 日志订阅。
	go l.loopNewHeads(ctx)
	if addr := l.cfg.Eth.ERC20Contract; addr != "" {
		go l.loopERC20Transfers(ctx, common.HexToAddress(addr))
	}

	<-ctx.Done()
}

func (l *Listener) loopNewHeads(ctx context.Context) {
	headers := make(chan *types.Header)
	backoff := time.Duration(l.cfg.Listener.ReconnectBaseMS) * time.Millisecond

	for {
		if ctx.Err() != nil {
			return
		}
		sub, err := l.b.WS().SubscribeNewHead(ctx, headers)
		if err != nil {
			log.Printf("[eth/listener] SubscribeNewHead 失败: %v，%v 后重试", err, backoff)
			sleepCtx(ctx, backoff)
			backoff = nextBackoff(backoff)
			continue
		}
		backoff = time.Duration(l.cfg.Listener.ReconnectBaseMS) * time.Millisecond
		log.Println("[eth/listener] 已连接 SubscribeNewHead。")

	inner:
		for {
			select {
			case <-ctx.Done():
				sub.Unsubscribe()
				return
			case err := <-sub.Err():
				log.Printf("[eth/listener] 区块订阅异常: %v，准备重连", err)
				sub.Unsubscribe()
				break inner
			case h := <-headers:
				if h == nil {
					continue
				}
				// 生产者：将区块头交给 Channel，交给若干 worker 处理（见 pipeline）。
				l.bus.SubmitHeader(h)
			}
		}
		sleepCtx(ctx, backoff)
	}
}

func (l *Listener) loopERC20Transfers(ctx context.Context, contract common.Address) {
	// 订阅合约日志：Transfer(address indexed from, address indexed to, uint256 value)
	logsCh := make(chan types.Log)
	q := ethereum.FilterQuery{
		Addresses: []common.Address{contract},
		Topics:    [][]common.Hash{{erc20TransferTopic}},
	}
	backoff := time.Duration(l.cfg.Listener.ReconnectBaseMS) * time.Millisecond

	for {
		if ctx.Err() != nil {
			return
		}
		sub, err := l.b.WS().SubscribeFilterLogs(ctx, q, logsCh)
		if err != nil {
			log.Printf("[eth/listener] SubscribeFilterLogs 失败: %v，%v 后重试", err, backoff)
			sleepCtx(ctx, backoff)
			backoff = nextBackoff(backoff)
			continue
		}
		backoff = time.Duration(l.cfg.Listener.ReconnectBaseMS) * time.Millisecond
		log.Printf("[eth/listener] 已订阅 ERC-20 %s 的 Transfer 日志。", contract.Hex())

	inner:
		for {
			select {
			case <-ctx.Done():
				sub.Unsubscribe()
				return
			case err := <-sub.Err():
				log.Printf("[eth/listener] 日志订阅异常: %v，准备重连", err)
				sub.Unsubscribe()
				break inner
			case ev := <-logsCh:
				l.bus.SubmitTransferLog(&ev)
			}
		}
		sleepCtx(ctx, backoff)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func nextBackoff(cur time.Duration) time.Duration {
	const max = 30 * time.Second
	n := cur * 2
	if n > max {
		return max
	}
	return n
}
