package tx

import "time"

const (
	StatusSubmitting      = "submitting"
	StatusBroadcastFailed = "broadcast_failed"
	StatusPending         = "pending"
	StatusConfirmed       = "confirmed"
	StatusFailed          = "failed"
	StatusDropped         = "dropped"
	StatusReplaced        = "replaced"

	TxTypeNative = "native"
	TxTypeERC20  = "erc20"

	TxFormatLegacy   = "legacy"
	TxFormatEIP1559  = "eip1559"
)

// Row 对应 tx_tracker 表一行。
type Row struct {
	ChainID            int64     `json:"chain_id"`
	TxHash             string    `json:"tx_hash"`
	FromAddr           string    `json:"from_addr"`
	ToAddr             string    `json:"to_addr"`
	TokenAddr          string    `json:"token_addr,omitempty"`
	ValueWei           string    `json:"value_wei"`
	Nonce              uint64    `json:"nonce"`
	GasLimit           uint64    `json:"gas_limit"`
	GasPriceWei        string    `json:"gas_price_wei,omitempty"`
	MaxFeePerGasWei    string    `json:"max_fee_per_gas_wei,omitempty"`
	MaxPriorityFeeWei  string    `json:"max_priority_fee_wei,omitempty"`
	TxFormat           string    `json:"tx_format"`
	TxType             string    `json:"tx_type"`
	Status             string    `json:"status"`
	BlockNumber        uint64    `json:"block_number"`
	GasUsed            *uint64   `json:"gas_used,omitempty"`
	Confirmations      uint64    `json:"confirmations"`
	ErrorMessage       string    `json:"error_message,omitempty"`
	BizID              string    `json:"biz_id,omitempty"`
	BizType            string    `json:"biz_type,omitempty"`
	IdempotencyKey     string    `json:"idempotency_key,omitempty"`
	ReplacesHash       string    `json:"replaces_hash,omitempty"`
	ReplacedByHash      string    `json:"replaced_by_hash,omitempty"`
	SignedRawHex        string    `json:"-"`
	BroadcastRetryCount int       `json:"broadcast_retry_count,omitempty"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// InsertParams 广播成功后写入跟踪表。
type InsertParams struct {
	TxHash            string
	FromAddr          string
	ToAddr            string
	TokenAddr         string
	ValueWei          string
	Nonce             uint64
	GasLimit          uint64
	GasPriceWei       string
	MaxFeePerGasWei   string
	MaxPriorityFeeWei string
	TxFormat          string
	TxType            string
	BizID             string
	BizType           string
	IdempotencyKey    string
	ReplacesHash      string
	SignedRawHex      string
	Status            string // 默认 submitting；MarkBroadcastPending 后变为 pending
}

// SubmitRequest 生产路径：客户端/KMS 签名后提交 raw tx。
type SubmitRequest struct {
	SignedRawTx    string
	BizID          string
	BizType        string
	IdempotencyKey string
	ReplacesHash   string
}

// SendMeta 业务绑定与幂等（发交易时附带）。
type SendMeta struct {
	BizID          string
	BizType        string
	IdempotencyKey string
	ReplacesHash   string
}
