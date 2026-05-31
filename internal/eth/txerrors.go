package eth

import "strings"

// IsAlreadyKnown 节点返回「交易已在 mempool/链上」时视为广播成功（幂等重试）。
func IsAlreadyKnown(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already known") ||
		strings.Contains(msg, "known transaction") ||
		strings.Contains(msg, "duplicate") && strings.Contains(msg, "transaction")
}
