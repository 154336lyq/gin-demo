package exchange

import "time"

const (
	DepositStatusPending   = "pending"
	DepositStatusCredited  = "credited"
	DepositStatusOrphaned  = "orphaned"

	WithdrawStatusPendingReview = "pending_review"
	WithdrawStatusApproved      = "approved"
	WithdrawStatusRejected      = "rejected"
	WithdrawStatusBroadcasting  = "broadcasting"
	WithdrawStatusConfirmed     = "confirmed"
	WithdrawStatusFailed        = "failed"
	WithdrawStatusCancelled     = "cancelled"

	LedgerDepositCredit    = "deposit_credit"
	LedgerDepositReverse   = "deposit_reverse"
	LedgerWithdrawFreeze   = "withdraw_freeze"
	LedgerWithdrawUnfreeze = "withdraw_unfreeze"
	LedgerWithdrawDebit    = "withdraw_debit"
	LedgerPaymentDebit     = "payment_debit"
	LedgerPaymentCredit    = "payment_credit"
)

// AccountBalance 链下账本：可用 + 冻结。
type AccountBalance struct {
	ChainID      int64     `json:"chain_id"`
	UserID       string    `json:"user_id"`
	TokenAddress string    `json:"token_address"`
	AvailableWei string    `json:"available_wei"`
	FrozenWei    string    `json:"frozen_wei"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// LedgerEntry 账本流水。
type LedgerEntry struct {
	ID                     int64     `json:"id"`
	ChainID                int64     `json:"chain_id"`
	UserID                 string    `json:"user_id"`
	TokenAddress           string    `json:"token_address"`
	EntryType              string    `json:"entry_type"`
	AmountWei              string    `json:"amount_wei"`
	RefType                string    `json:"ref_type,omitempty"`
	RefID                  int64     `json:"ref_id,omitempty"`
	BalanceAvailableAfter  string    `json:"balance_available_after"`
	BalanceFrozenAfter     string    `json:"balance_frozen_after"`
	CreatedAt              time.Time `json:"created_at"`
}

// Deposit 充值记录。
type Deposit struct {
	ID             int64      `json:"id"`
	ChainID        int64      `json:"chain_id"`
	UserID         string     `json:"user_id"`
	DepositAddress string     `json:"deposit_address"`
	TokenAddress   string     `json:"token_address"`
	AmountWei      string     `json:"amount_wei"`
	TxHash         string     `json:"tx_hash"`
	LogIndex       uint       `json:"log_index"`
	BlockNumber    uint64     `json:"block_number"`
	Status         string     `json:"status"`
	CreditedAt     *time.Time `json:"credited_at,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
}

// WithdrawRequest 提现申请。
type WithdrawRequest struct {
	ID               int64      `json:"id"`
	ChainID          int64      `json:"chain_id"`
	UserID           string     `json:"user_id"`
	TokenAddress     string     `json:"token_address"`
	ToAddress        string     `json:"to_address"`
	AmountWei        string     `json:"amount_wei"`
	Status           string     `json:"status"`
	FromWallet       string     `json:"from_wallet,omitempty"`
	TxHash           string     `json:"tx_hash,omitempty"`
	Reviewer         string     `json:"reviewer,omitempty"`
	ReviewedAt       *time.Time `json:"reviewed_at,omitempty"`
	RejectReason     string     `json:"reject_reason,omitempty"`
	IdempotencyKey   string     `json:"idempotency_key,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

// ReconcileReport 对账结果：链上托管资产 vs 用户负债。
type ReconcileReport struct {
	ChainID           int64              `json:"chain_id"`
	TokenAddress      string             `json:"token_address"`
	OnChainCustodial  string             `json:"on_chain_custodial_wei"`
	UserLiabilities   string             `json:"user_liabilities_wei"`
	DiffWei           string             `json:"diff_wei"`
	OK                bool               `json:"ok"`
	GeneratedAt       time.Time          `json:"generated_at"`
	Details           []ReconcileDetail  `json:"details,omitempty"`
}

type ReconcileDetail struct {
	Address    string `json:"address"`
	WalletType string `json:"wallet_type"`
	BalanceWei string `json:"balance_wei"`
}

type CreateWithdrawParams struct {
	UserID         string
	TokenAddress   string
	ToAddress      string
	AmountWei      string
	IdempotencyKey string
}

type CaptureDepositParams struct {
	UserID         string
	DepositAddress string
	TokenAddress   string
	AmountWei      string
	TxHash         string
	LogIndex       uint
	BlockNumber    uint64
}
