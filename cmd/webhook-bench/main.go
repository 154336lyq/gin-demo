// webhook-bench：商户 Webhook 推送中心压测工具。
//
// 用法（先启动 chain-backend 并登录拿 JWT）：
//
//	go run ./cmd/webhook-bench -token <jwt> -merchant mch_demo -count 10000
//
// 观察 GET /api/v1/webhook/status 中 deliver_per_sec、p99_latency_ms。
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	base := flag.String("base", "http://127.0.0.1:8080", "API base URL")
	token := flag.String("token", "", "JWT token (required)")
	merchant := flag.String("merchant", "mch_demo", "merchant id")
	count := flag.Int("count", 10000, "outbox items to enqueue")
	setup := flag.Bool("setup", false, "register merchant before bench")
	flag.Parse()

	if *token == "" {
		fmt.Fprintln(os.Stderr, "error: -token required (POST /api/v1/auth/login)")
		os.Exit(1)
	}
	client := &http.Client{Timeout: 120 * time.Second}

	if *setup {
		mockURL := *base + "/api/v1/webhook/mock/receive"
		body := map[string]any{
			"merchant_id":    *merchant,
			"name":           "Bench Merchant",
			"webhook_url":    "mock://local",
			"ledger_user_id": "merchant_" + *merchant,
			"enabled":        true,
		}
		if err := postJSON(client, *base+"/api/v1/webhook/merchants", *token, body); err != nil {
			fmt.Fprintf(os.Stderr, "register merchant: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("merchant %s registered (deliver via mock_receive_url -> %s)\n", *merchant, mockURL)
	}

	start := time.Now()
	enqueueBody := map[string]any{"merchant_id": *merchant, "count": *count}
	if err := postJSON(client, *base+"/api/v1/webhook/bench/enqueue", *token, enqueueBody); err != nil {
		fmt.Fprintf(os.Stderr, "bench enqueue: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("enqueued %d items in %v\n", *count, time.Since(start))

	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		st, err := getStatus(client, *base+"/api/v1/webhook/status", *token)
		if err != nil {
			fmt.Fprintf(os.Stderr, "status: %v\n", err)
			time.Sleep(time.Second)
			continue
		}
		fmt.Printf("[status] pending=%d processing=%d delivered=%d deliver_per_sec=%.1f p99_ms=%d avg_ms=%.1f\n",
			st.PendingOutbox, st.ProcessingOutbox, st.DeliveredTotal, st.DeliverPerSec, st.P99LatencyMS, st.AvgLatencyMS)
		if st.PendingOutbox == 0 && st.ProcessingOutbox == 0 && st.DeliveredTotal >= uint64(*count) {
			fmt.Println("bench complete")
			return
		}
		time.Sleep(2 * time.Second)
	}
	fmt.Println("bench timeout — check /api/v1/webhook/status")
}

type statusResp struct {
	PendingOutbox    int     `json:"pending_outbox"`
	ProcessingOutbox int     `json:"processing_outbox"`
	DeliveredTotal   uint64  `json:"delivered_total"`
	DeliverPerSec    float64 `json:"deliver_per_sec"`
	P99LatencyMS     int64   `json:"p99_latency_ms"`
	AvgLatencyMS     float64 `json:"avg_latency_ms"`
}

func postJSON(client *http.Client, url, token string, body any) error {
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("http %d: %s", resp.StatusCode, string(b))
	}
	fmt.Println(string(b))
	return nil
}

func getStatus(client *http.Client, url, token string) (statusResp, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return statusResp{}, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		return statusResp{}, err
	}
	defer resp.Body.Close()
	var st statusResp
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return statusResp{}, err
	}
	return st, nil
}
