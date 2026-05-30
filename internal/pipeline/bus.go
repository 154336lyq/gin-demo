// Package pipeline 演示并发模式：Channel、WaitGroup、Mutex、Context、生产者-消费者。
//
// 链上事件（区块头 / Transfer 日志）作为生产者放入队列，由固定数量 worker 消费并更新统计。
package pipeline

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/core/types"
)

// ChainIndexer 可选的链下同步消费者（见 internal/indexer）。
type ChainIndexer interface {
	EnqueueHeader(ctx context.Context, h *types.Header)
	EnqueueTransferLog(ctx context.Context, l *types.Log)
}

// Bus 作为「并发安全的数据汇聚」：对外暴露原子计数 + mutex 保护的可读快照。
type Bus struct {
	cfg     ListenerConfig
	indexer ChainIndexer

	// jobs 承载区块头任务（消费者池）。
	headerCh chan *types.Header
	// transferCh 承载 ERC-20 日志（单独消费者简化演示）。
	transferCh chan *types.Log

	wg sync.WaitGroup

	// 统计：原子操作适合简单递增；与 Mutex 二选一展示亦可，此处混合演示。
	headersSeen   uint64
	transfersSeen uint64

	mu    sync.Mutex
	lasts []string // 最近处理摘要（Ring 简化：最多保留 20 条）
}

type ListenerConfig struct {
	WorkerCount   int
	ChannelBuffer int
}

func NewBus(lc ListenerConfig, idx ChainIndexer) *Bus {
	if lc.WorkerCount <= 0 {
		lc.WorkerCount = 2
	}
	if lc.ChannelBuffer <= 0 {
		lc.ChannelBuffer = 16
	}
	return &Bus{
		cfg:        lc,
		indexer:    idx,
		headerCh:   make(chan *types.Header, lc.ChannelBuffer),
		transferCh: make(chan *types.Log, lc.ChannelBuffer),
	}
}

// Start 启动 worker；ctx 取消后关闭 channel，worker 通过 range 排空后退出（优雅收尾）。
func (b *Bus) Start(ctx context.Context) {
	for i := 0; i < b.cfg.WorkerCount; i++ {
		b.wg.Add(1)
		id := i + 1
		go b.worker(id)
	}
	b.wg.Add(1)
	go b.transferWorker()

	go func() {
		<-ctx.Done()
		close(b.headerCh)
		close(b.transferCh)
	}()
}

// Wait 阻塞至全部 worker 退出。
func (b *Bus) Wait() { b.wg.Wait() }

func (b *Bus) worker(id int) {
	defer b.wg.Done()
	for h := range b.headerCh {
		atomic.AddUint64(&b.headersSeen, 1)
		line := fmt.Sprintf("%d", h.Number.Uint64())
		b.appendLast(fmt.Sprintf("[worker-%d] header #%s hash=%s", id, line, h.Hash().Hex()))
		log.Printf("[pipeline] worker-%d 处理区块头 #%s", id, line)
	}
}

func (b *Bus) transferWorker() {
	defer b.wg.Done()
	for lg := range b.transferCh {
		atomic.AddUint64(&b.transfersSeen, 1)
		b.appendLast(fmt.Sprintf("ERC20 Transfer tx=%s block=%d", lg.TxHash.Hex(), lg.BlockNumber))
		log.Printf("[pipeline] Transfer 日志 tx=%s", lg.TxHash.Hex())
	}
}

func (b *Bus) appendLast(line string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.lasts) >= 20 {
		b.lasts = b.lasts[1:]
	}
	b.lasts = append(b.lasts, line)
}

// SubmitHeader 优先写入 indexer 待同步队列（防丢失），再投递 pipeline。
func (b *Bus) SubmitHeader(h *types.Header) {
	if b.indexer != nil {
		b.indexer.EnqueueHeader(context.Background(), h)
	}
	select {
	case b.headerCh <- h:
	default:
		log.Println("[pipeline] header channel 已满，indexer 已持久化，统计 worker 跳过一条。")
	}
}

func (b *Bus) SubmitTransferLog(l *types.Log) {
	if b.indexer != nil {
		b.indexer.EnqueueTransferLog(context.Background(), l)
	}
	select {
	case b.transferCh <- l:
	default:
		log.Println("[pipeline] transfer channel 已满，indexer 已处理或持久化。")
	}
}

// Stats 返回计数快照。
func (b *Bus) Stats() (headers, transfers uint64, recent []string) {
	h := atomic.LoadUint64(&b.headersSeen)
	t := atomic.LoadUint64(&b.transfersSeen)
	b.mu.Lock()
	defer b.mu.Unlock()
	cp := append([]string(nil), b.lasts...)
	return h, t, cp
}
