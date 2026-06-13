package webhook

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"time"
)

// Dispatcher HTTP 出站投递（连接池优化，支撑 2000/s 级吞吐）。
type Dispatcher struct {
	client *http.Client
}

func NewDispatcher(timeoutMS, maxConns int) *Dispatcher {
	if timeoutMS <= 0 {
		timeoutMS = 3000
	}
	if maxConns <= 0 {
		maxConns = 300
	}
	transport := &http.Transport{
		MaxIdleConns:        maxConns,
		MaxIdleConnsPerHost: maxConns,
		MaxConnsPerHost:     maxConns,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
	return &Dispatcher{
		client: &http.Client{
			Timeout:   time.Duration(timeoutMS) * time.Millisecond,
			Transport: transport,
		},
	}
}

type DispatchResult struct {
	HTTPStatus   int
	LatencyMS    int64
	ResponseBody string
	Err          error
}

func (d *Dispatcher) Post(ctx context.Context, url, secret string, body []byte) DispatchResult {
	start := time.Now()
	headers, _ := signatureHeaders(secret, body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return DispatchResult{Err: err, LatencyMS: time.Since(start).Milliseconds()}
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("X-Event-Type", parseEventType(body))

	resp, err := d.client.Do(req)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		return DispatchResult{Err: err, LatencyMS: latency}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return DispatchResult{
		HTTPStatus:   resp.StatusCode,
		LatencyMS:    latency,
		ResponseBody: string(raw),
	}
}

func parseEventType(body []byte) string {
	// 轻量解析，避免完整 JSON decode 开销。
	s := string(body)
	if i := strings.Index(s, `"event_type"`); i >= 0 {
		rest := s[i:]
		if j := strings.Index(rest, ":"); j >= 0 {
			rest = rest[j+1:]
			rest = strings.TrimSpace(rest)
			if len(rest) > 0 && rest[0] == '"' {
				rest = rest[1:]
				if k := strings.Index(rest, `"`); k >= 0 {
					return rest[:k]
				}
			}
		}
	}
	return "webhook"
}
