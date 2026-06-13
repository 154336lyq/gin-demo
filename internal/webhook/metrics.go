package webhook

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics 进程内投递性能指标（压测/运维看板）。
type Metrics struct {
	delivered      atomic.Uint64
	failed         atomic.Uint64
	duplicates     atomic.Uint64
	enqueueTotal   atomic.Uint64
	latencyMu      sync.Mutex
	latencySamples []int64
	windowStart    time.Time
	windowCount    atomic.Uint64
}

func NewMetrics() *Metrics {
	return &Metrics{windowStart: time.Now()}
}

func (m *Metrics) RecordDelivered(latencyMS int64) {
	m.delivered.Add(1)
	m.windowCount.Add(1)
	m.recordLatency(latencyMS)
}

func (m *Metrics) RecordFailed() {
	m.failed.Add(1)
}

func (m *Metrics) RecordDuplicate() {
	m.duplicates.Add(1)
}

func (m *Metrics) RecordEnqueue() {
	m.enqueueTotal.Add(1)
}

func (m *Metrics) recordLatency(ms int64) {
	m.latencyMu.Lock()
	defer m.latencyMu.Unlock()
	m.latencySamples = append(m.latencySamples, ms)
	const capN = 10000
	if len(m.latencySamples) > capN {
		m.latencySamples = m.latencySamples[len(m.latencySamples)-capN:]
	}
}

func (m *Metrics) P99LatencyMS() int64 {
	m.latencyMu.Lock()
	samples := append([]int64(nil), m.latencySamples...)
	m.latencyMu.Unlock()
	if len(samples) == 0 {
		return 0
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	idx := int(float64(len(samples)) * 0.99)
	if idx >= len(samples) {
		idx = len(samples) - 1
	}
	return samples[idx]
}

func (m *Metrics) AvgLatencyMS() float64 {
	m.latencyMu.Lock()
	defer m.latencyMu.Unlock()
	if len(m.latencySamples) == 0 {
		return 0
	}
	var sum int64
	for _, v := range m.latencySamples {
		sum += v
	}
	return float64(sum) / float64(len(m.latencySamples))
}

func (m *Metrics) DeliverPerSec() float64 {
	elapsed := time.Since(m.windowStart).Seconds()
	if elapsed < 0.1 {
		return 0
	}
	return float64(m.windowCount.Load()) / elapsed
}

func (m *Metrics) ResetWindow() {
	m.windowStart = time.Now()
	m.windowCount.Store(0)
}

func (m *Metrics) Snapshot(pending, processing int) Status {
	enqueued := m.enqueueTotal.Load()
	delivered := m.delivered.Load()
	dup := m.duplicates.Load()
	var missRate, dupRate float64
	if enqueued > 0 {
		missRate = float64(m.failed.Load()) / float64(enqueued) * 100
		dupRate = float64(dup) / float64(enqueued) * 100
	}
	return Status{
		DeliveredTotal:     delivered,
		FailedTotal:        m.failed.Load(),
		DuplicatePrevented: dup,
		DeliverPerSec:      m.DeliverPerSec(),
		P99LatencyMS:       m.P99LatencyMS(),
		AvgLatencyMS:       m.AvgLatencyMS(),
		PendingOutbox:      pending,
		ProcessingOutbox:   processing,
		MissRatePct:        missRate,
		DuplicateRatePct:   dupRate,
	}
}
