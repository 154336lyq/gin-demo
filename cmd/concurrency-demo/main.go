// Package main 演示 Go 并发常见要素的组合使用：
// Goroutine、Channel、select、WaitGroup、Context、生产者-消费者模型，
// 以及 Mutex、RWMutex、并发安全数据结构（sync.Map 与锁保护下的自定义结构）。
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// job 表示生产者投递、消费者处理的一条任务（演示用整型任务编号）。
type job int

const (
	producerCount = 2 // 生产者 Goroutine 数量
	consumerCount = 3 // 消费者 Goroutine 数量
	jobBuffer     = 4 // 任务 Channel 缓冲容量（有界队列，背压）
	totalJobs     = 12 // 总共投递的任务数
	monitorCount  = 2 // 只读监控 Goroutine 数量（演示 RWMutex 多读）
)

// ---------- Mutex：互斥锁，保护简单标量统计（多写场景用一把互斥锁串行化）----------

type tally struct {
	mu        sync.Mutex
	produced  int // 已成功投递到 Channel 的任务数
	consumed  int // 已处理完成的任务数
}

func (t *tally) incProduced() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.produced++
}

func (t *tally) incConsumed() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.consumed++
}

func (t *tally) snapshot() (produced, consumed int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.produced, t.consumed
}

// ---------- RWMutex：读写锁，允许多个读者并发读，写者独占（适合读多写少）----------

// jobRegistry 记录「哪个消费者完成了哪个任务」，监控协程频繁只读统计。
type jobRegistry struct {
	rw         sync.RWMutex
	byConsumer map[job]int // 任务 -> 完成它的消费者编号
}

func newJobRegistry() *jobRegistry {
	return &jobRegistry{byConsumer: make(map[job]int)}
}

// record 由消费者调用，属于写路径：必须 Lock（与读路径 RLock 互斥）。
func (r *jobRegistry) record(j job, consumerID int) {
	r.rw.Lock()
	defer r.rw.Unlock()
	r.byConsumer[j] = consumerID
}

// countDone 只读：RLock 可与其它 countDone / clone 的 RLock 并发执行。
func (r *jobRegistry) countDone() int {
	r.rw.RLock()
	defer r.rw.RUnlock()
	return len(r.byConsumer)
}

// ---------- 并发安全数据结构：① 标准库 sync.Map（键值并发存取）；② 上文的 jobRegistry（RWMutex+map）----------

func main() {
	// Context：在超时或调用 cancel 时向所有 Goroutine 广播「该停了」。
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jobs := make(chan job, jobBuffer)

	var (
		wgProducers, wgConsumers, wgMonitors sync.WaitGroup // WaitGroup：三组 Goroutine 各自 Add/Wait
		stats                                 tally
		registry                              = newJobRegistry()
		// sync.Map 内建并发安全，适合多协程 Load/Store/Range；键这里用 job 的整型。
		taskMeta sync.Map
	)

	// ---------- 监控协程：只读 registry，演示 RWMutex 的 RLock 与 WaitGroup 组合 ----------
	for m := 1; m <= monitorCount; m++ {
		wgMonitors.Add(1)
		mid := m
		go func() {
			defer wgMonitors.Done()
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					log.Printf("[监控 %d] Context 结束，退出。", mid)
					return
				case <-ticker.C:
					n := registry.countDone()
					p, c := stats.snapshot()
					log.Printf("[监控 %d] 快照: 登记完成=%d, 已投递=%d, 已消费=%d", mid, n, p, c)
				}
			}
		}()
	}

	for c := 1; c <= consumerCount; c++ {
		wgConsumers.Add(1)
		cid := c
		go func() {
			defer wgConsumers.Done()
			consumer(ctx, cid, jobs, registry, &stats, &taskMeta)
		}()
	}

	for p := 1; p <= producerCount; p++ {
		wgProducers.Add(1)
		pid := p
		go func() {
			defer wgProducers.Done()
			producer(ctx, pid, jobs, &stats, &taskMeta)
		}()
	}

	go func() {
		wgProducers.Wait()
		close(jobs)
		log.Println("[主流程] 全部生产者已退出，jobs Channel 已关闭。")
	}()

	wgConsumers.Wait()

	// 业务管道结束后主动 cancel，让监控协程不必等到 Timeout 才退出。
	cancel()
	wgMonitors.Wait()

	p, c := stats.snapshot()
	log.Printf("最终 tally(Mutex): produced=%d consumed=%d", p, c)
	log.Printf("登记簿(RWMutex)中记录条数: %d", registry.countDone())
	taskMeta.Range(func(k, v any) bool {
		log.Printf("sync.Map 元数据: job=%v info=%v", k, v)
		return true
	})
	log.Println("全部 Goroutine 已退出，程序正常结束。")
}

func producer(ctx context.Context, id int, jobs chan<- job, stats *tally, taskMeta *sync.Map) {
	per := totalJobs / producerCount
	start := (id - 1) * per
	end := start + per
	if id == producerCount {
		end = totalJobs
	}

	for j := start; j < end; j++ {
		task := job(j)

		select {
		case <-ctx.Done():
			log.Printf("[生产者 %d] Context 结束: %v，停止投递。", id, ctx.Err())
			return
		case jobs <- task:
			log.Printf("[生产者 %d] 投递任务 %d", id, task)
			stats.incProduced()
			// sync.Map：无外部锁的并发写；演示与 Channel 并行更新元数据。
			taskMeta.Store(int64(task), fmt.Sprintf("produced by %d at %s", id, time.Now().Format("15:04:05.000")))
			time.Sleep(time.Duration(20+rand.Intn(40)) * time.Millisecond)
		}
	}
	log.Printf("[生产者 %d] 本分区内任务已全部投递。", id)
}

func consumer(ctx context.Context, id int, jobs <-chan job, reg *jobRegistry, stats *tally, taskMeta *sync.Map) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("[消费者 %d] Context 结束: %v，退出。", id, ctx.Err())
			return
		case task, ok := <-jobs:
			if !ok {
				log.Printf("[消费者 %d] jobs 已关闭且无剩余数据，退出。", id)
				return
			}
			log.Printf("[消费者 %d] 开始处理任务 %d", id, task)
			time.Sleep(time.Duration(30+rand.Intn(50)) * time.Millisecond)
			log.Printf("[消费者 %d] 完成任务 %d", id, task)

			stats.incConsumed()
			reg.record(task, id)
			taskMeta.Store(int64(task), fmt.Sprintf("consumed by %d at %s", id, time.Now().Format("15:04:05.000")))
		}
	}
}
