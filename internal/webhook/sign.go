package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

func signPayload(secret string, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(fmt.Sprintf("%d.", timestamp)))
	_, _ = mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func verifySignature(secret string, timestamp int64, body []byte, sig string) bool {
	if secret == "" || sig == "" {
		return false
	}
	expected := signPayload(secret, timestamp, body)
	return hmac.Equal([]byte(expected), []byte(sig))
}

func signatureHeaders(secret string, body []byte) (map[string]string, int64) {
	ts := time.Now().Unix()
	return map[string]string{
		"X-Webhook-Timestamp": strconv.FormatInt(ts, 10),
		"X-Webhook-Signature": "sha256=" + signPayload(secret, ts, body),
		"Content-Type":        "application/json",
	}, ts
}
