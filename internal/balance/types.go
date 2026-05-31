package balance

import "time"

// NativeToken 表示原生币（ETH）在库中的 token 占位（空字符串）。
const NativeToken = ""

const (
	WalletTypeHot     = "hot"
	WalletTypeDeposit = "deposit"
	WalletTypeTreasury = "treasury"
)

// Row 对应 account_balances 表一行。
type Row struct {
	ChainID      int64     `json:"chain_id"`
	Address      string    `json:"address"`
	TokenAddress string    `json:"token_address"` // 空 = 原生币
	BalanceWei   string    `json:"balance_wei"`
	BlockNumber  uint64    `json:"block_number"`
	SourceTxHash string    `json:"source_tx_hash,omitempty"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// CustodialWallet 交易所/托管场景下需同步余额的链上地址。
type CustodialWallet struct {
	ChainID    int64     `json:"chain_id"`
	Address    string    `json:"address"`
	UserID     string    `json:"user_id,omitempty"`
	Label      string    `json:"label,omitempty"`
	WalletType string    `json:"wallet_type"`
	Enabled    bool      `json:"enabled"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// RegisterWalletParams 注册托管地址。
type RegisterWalletParams struct {
	Address    string
	UserID     string
	Label      string
	WalletType string
}

// TxParties 交易确认后需要刷新余额的地址集合。
type TxParties struct {
	TxHash      string
	From        string
	To          string
	TokenAddr   string // ERC-20 合约；native 为空
	TxType      string // native | erc20
	BlockNumber uint64
}
