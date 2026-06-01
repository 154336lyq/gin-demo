package exchange

import "errors"

var (
	ErrInvalidWei           = errors.New("invalid wei amount")
	ErrInsufficientBalance  = errors.New("insufficient available balance")
	ErrInvalidWithdrawState    = errors.New("invalid withdraw state transition")
	ErrNoHotWallet             = errors.New("no hot wallet configured")
	ErrWithdrawAlreadyHandled  = errors.New("withdraw already being processed or completed")
	errDepositNotFound      = errors.New("deposit not found")
	errWithdrawNotFound     = errors.New("withdraw request not found")
	errDuplicateIdempotency = errors.New("duplicate idempotency key")
)
