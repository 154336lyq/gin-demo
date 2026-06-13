package webhook

import (
	"context"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"gin-demo/internal/config"
)

// Engine 多 Worker 并发消费 Outbox，目标压测 2000/s。
type Engine struct {
	cfg        *config.Config
	store      *Store
	dispatcher *Dispatcher
	metrics    *Metrics
}

func NewEngine(cfg *config.Config, store *Store, metrics *Metrics) *Engine {
	wc := cfg.Webhook
	return &Engine{
		cfg:        cfg,
		store:      store,
		dispatcher: NewDispatcher(wc.HTTPTimeoutMS, wc.MaxConnsPerHost),
		metrics:    metrics,
	}
}

func (e *Engine) Start(ctx context.Context) {
	if e == nil || e.store == nil || e.cfg == nil || !e.cfg.Webhook.Enabled {
		return
	}
	workers := e.cfg.Webhook.Workers
	if workers <= 0 {
		workers = 16
	}
	for i := 0; i < workers; i++ {
		go e.worker(ctx, i+1)
	}
	go e.metricsResetLoop(ctx)
	log.Printf("[webhook] 已启动 workers=%d batch=%d max_retries=%d http_timeout_ms=%d",
		workers, e.cfg.Webhook.BatchSize, e.cfg.Webhook.MaxRetries, e.cfg.Webhook.HTTPTimeoutMS)
}

func (e *Engine) metricsResetLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if e.metrics != nil {
				e.metrics.ResetWindow()
			}
		}
	}
}

func (e *Engine) worker(ctx context.Context, id int) {
	batchSize := e.cfg.Webhook.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	concurrency := e.cfg.Webhook.DispatchConcurrency
	if concurrency <= 0 {
		concurrency = 50
	}
	idleSleep := time.Duration(e.cfg.Webhook.IdlePollMS) * time.Millisecond
	if idleSleep <= 0 {
		idleSleep = 20 * time.Millisecond
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		batch, err := e.store.ClaimOutboxBatch(ctx, batchSize)
		if err != nil {
			log.Printf("[webhook] worker-%d claim: %v", id, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if len(batch) == 0 {
			time.Sleep(idleSleep)
			continue
		}

		sem := make(chan struct{}, concurrency)
		var wg sync.WaitGroup
		for _, item := range batch {
			wg.Add(1)
			sem <- struct{}{}
			go func(it OutboxRow) {
				defer wg.Done()
				defer func() { <-sem }()
				e.deliverOne(ctx, it)
			}(item)
		}
		wg.Wait()
	}
}

func (e *Engine) deliverOne(ctx context.Context, item OutboxRow) {
	m, secret, err := e.store.GetMerchant(ctx, item.MerchantID)
	if err != nil {
		e.failItem(ctx, item, 0, 0, "", err.Error())
		return
	}
	if !m.Enabled {
		e.failItem(ctx, item, 0, 0, "", "merchant disabled")
		return
	}

	targetURL := m.WebhookURL
	if e.cfg.Webhook.MockReceiveURL != "" && (strings.HasPrefix(targetURL, "mock://") || targetURL == "") {
		targetURL = e.cfg.Webhook.MockReceiveURL
	}

	res := e.dispatcher.Post(ctx, targetURL, secret, item.Payload)
	_ = e.store.InsertDelivery(ctx, item.ID, item.MerchantID, int(item.RetryCount)+1, res.HTTPStatus, int(res.LatencyMS), res.ResponseBody, errStr(res.Err))

	if res.Err == nil && res.HTTPStatus >= 200 && res.HTTPStatus < 300 {
		if err := e.store.MarkOutboxDone(ctx, item.ID); err != nil {
			log.Printf("[webhook] mark done id=%d: %v", item.ID, err)
			return
		}
		if e.metrics != nil {
			e.metrics.RecordDelivered(res.LatencyMS)
		}
		return
	}

	errMsg := errStr(res.Err)
	if errMsg == "" {
		errMsg = "http " + itoaStatus(res.HTTPStatus)
	}
	e.failItem(ctx, item, res.HTTPStatus, res.LatencyMS, res.ResponseBody, errMsg)
}

func (e *Engine) failItem(ctx context.Context, item OutboxRow, httpStatus int, latency int64, _, errMsg string) {
	retry := item.RetryCount + 1
	maxRetries := uint(e.cfg.Webhook.MaxRetries)
	if maxRetries == 0 {
		maxRetries = 8
	}
	fail := retry >= maxRetries
	backoff := time.Duration(math.Min(float64(retry*retry)*200, 30000)) * time.Millisecond
	next := time.Now().Add(backoff)
	if err := e.store.MarkOutboxRetry(ctx, item.ID, retry, next, fail); err != nil {
		log.Printf("[webhook] mark retry id=%d: %v", item.ID, err)
	}
	if e.metrics != nil {
		e.metrics.RecordFailed()
	}
	_ = httpStatus
	_ = latency
	_ = errMsg
}

func errStr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func itoaStatus(code int) string {
	if code == 0 {
		return "no response"
	}
	return strconv.Itoa(code)
}
