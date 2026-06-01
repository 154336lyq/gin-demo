package balance

import "errors"

var (
	ErrWalletNotFound        = errors.New("wallet not registered")
	ErrWalletDisabled        = errors.New("wallet is disabled")
	ErrDepositRequiresUserID = errors.New("deposit wallet requires user_id")
	ErrWalletOwnerConflict   = errors.New("address already registered to another user")
	ErrWalletTypeImmutable   = errors.New("wallet_type cannot be changed after registration")
)
