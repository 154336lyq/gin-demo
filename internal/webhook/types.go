package webhook

import (
	"encoding/json"
	"time"
)

const (
	EventDepositConfirmed = "deposit.confirmed"
	EventPaymentSuccess   = "payment.success"

	StatusPending    = "pending"
	StatusProcessing = "processing"
	StatusDone       = "done"
	StatusFailed     = "failed"
)

// Merchant B 端商户配置。
type Merchant struct {
	ChainID      int64     `json:"chain_id"`
	MerchantID   string    `json:"merchant_id"`
	Name         string    `json:"name"`
	WebhookURL   string    `json:"webhook_url"`
	LedgerUserID string    `json:"ledger_user_id"`
	Enabled      bool      `json:"enabled"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// Binding 充值用户与商户映射（玩家 user_id 充值 → 通知对应商户）。
type Binding struct {
	ChainID     int64     `json:"chain_id"`
	MerchantID  string    `json:"merchant_id"`
	PayerUserID string    `json:"payer_user_id"`
	CreatedAt   time.Time `json:"created_at"`
}

// OutboxRow Transactional Outbox 待投递项。
type OutboxRow struct {
	ID             uint64
	ChainID        int64
	MerchantID     string
	EventType      string
	IdempotencyKey string
	Payload        json.RawMessage
	Status         string
	RetryCount     uint
	NextRetryAt    time.Time
	CreatedAt      time.Time
}

// Delivery 单次 HTTP 投递审计。
type Delivery struct {
	ID           int64     `json:"id"`
	OutboxID     uint64    `json:"outbox_id"`
	MerchantID   string    `json:"merchant_id"`
	Attempt      int       `json:"attempt"`
	HTTPStatus   int       `json:"http_status"`
	LatencyMS    int       `json:"latency_ms"`
	ResponseBody string    `json:"response_body,omitempty"`
	ErrorMessage string    `json:"error_message,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

// Payment 平台内余额支付记录。
type Payment struct {
	ID             int64     `json:"id"`
	ChainID        int64     `json:"chain_id"`
	MerchantID     string    `json:"merchant_id"`
	OrderID        string    `json:"order_id"`
	PayerUserID    string    `json:"payer_user_id"`
	TokenAddress   string    `json:"token_address"`
	AmountWei      string    `json:"amount_wei"`
	Status         string    `json:"status"`
	IdempotencyKey string    `json:"idempotency_key"`
	CreatedAt      time.Time `json:"created_at"`
}

// Status 运行状态与性能指标（对外 API）。
type Status struct {
	Enabled            bool    `json:"enabled"`
	Workers            int     `json:"workers"`
	BatchSize          int     `json:"batch_size"`
	PendingOutbox      int     `json:"pending_outbox"`
	ProcessingOutbox   int     `json:"processing_outbox"`
	DeliveredTotal     uint64  `json:"delivered_total"`
	FailedTotal        uint64  `json:"failed_total"`
	DuplicatePrevented uint64  `json:"duplicate_prevented"`
	DeliverPerSec      float64 `json:"deliver_per_sec"`
	P99LatencyMS       int64   `json:"p99_latency_ms"`
	AvgLatencyMS       float64 `json:"avg_latency_ms"`
	MissRatePct        float64 `json:"miss_rate_pct"`
	DuplicateRatePct   float64 `json:"duplicate_rate_pct"`
}

// DepositPayload 充值确认回调 body。
type DepositPayload struct {
	EventType   string `json:"event_type"`
	MerchantID  string `json:"merchant_id"`
	OrderID     string `json:"order_id,omitempty"`
	UserID      string `json:"user_id"`
	TxHash      string `json:"tx_hash"`
	AmountWei   string `json:"amount"`
	Token       string `json:"token"`
	Status      string `json:"status"`
	BlockNumber uint64 `json:"block_number"`
	ConfirmedAt string `json:"confirmed_at"`
}

// PaymentPayload 内部支付成功回调 body。
type PaymentPayload struct {
	EventType  string `json:"event_type"`
	MerchantID string `json:"merchant_id"`
	OrderID    string `json:"order_id"`
	PayerID    string `json:"payer_user_id"`
	AmountWei  string `json:"amount"`
	Token      string `json:"token"`
	Status     string `json:"status"`
	PaidAt     string `json:"paid_at"`
}
