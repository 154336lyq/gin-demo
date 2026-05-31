package api

import (
	"fmt"
	"math/big"
	"strings"
)

func parseTransferValue(weiStr, ethStr string) (*big.Int, error) {
	if weiStr != "" {
		v := new(big.Int)
		if _, ok := v.SetString(weiStr, 10); !ok || v.Sign() <= 0 {
			return nil, fmt.Errorf("value_wei must be a positive decimal string")
		}
		return v, nil
	}
	if ethStr == "" {
		return nil, fmt.Errorf("provide value_wei or value_eth")
	}
	r := new(big.Rat)
	if _, ok := r.SetString(ethStr); !ok {
		return nil, fmt.Errorf("invalid value_eth")
	}
	if r.Sign() <= 0 {
		return nil, fmt.Errorf("value_eth must be positive")
	}
	weiPerEth := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	num := new(big.Int).Mul(r.Num(), weiPerEth)
	v := new(big.Int).Div(num, r.Denom())
	if v.Sign() <= 0 {
		return nil, fmt.Errorf("amount too small")
	}
	return v, nil
}

func normalizeTxHash(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return raw
	}
	if !strings.HasPrefix(raw, "0x") && !strings.HasPrefix(raw, "0X") {
		return "0x" + raw
	}
	return raw
}
