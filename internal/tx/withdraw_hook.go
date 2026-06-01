package tx

import "context"

// WithdrawHandler 提现交易链上状态回调（由 exchange 包实现，避免循环依赖）。
type WithdrawHandler interface {
	OnWithdrawTxConfirmed(ctx context.Context, txHash string) error
	OnWithdrawTxFailed(ctx context.Context, txHash string) error
}
