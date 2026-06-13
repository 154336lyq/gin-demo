package webhook

import "errors"

var (
	ErrNotEnabled         = errors.New("webhook module not enabled")
	ErrMerchantNotFound   = errors.New("merchant not found")
	ErrMerchantDisabled   = errors.New("merchant disabled")
	ErrDuplicateMerchant  = errors.New("merchant already exists")
	ErrDuplicateBinding   = errors.New("binding already exists")
	ErrDuplicatePayment   = errors.New("duplicate payment order")
	ErrInsufficientFunds  = errors.New("insufficient available balance")
	ErrInvalidWebhookURL  = errors.New("invalid webhook url")
	ErrOutboxNotFound     = errors.New("outbox item not found")
)
