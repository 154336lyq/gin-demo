package exchange

import (
	"math/big"
)

func weiZero() *big.Int { return big.NewInt(0) }

func parseWei(s string) (*big.Int, error) {
	v := new(big.Int)
	if _, ok := v.SetString(s, 10); !ok {
		return nil, ErrInvalidWei
	}
	if v.Sign() < 0 {
		return nil, ErrInvalidWei
	}
	return v, nil
}

func weiString(v *big.Int) string {
	if v == nil {
		return "0"
	}
	return v.String()
}

func weiAdd(a, b *big.Int) *big.Int {
	return new(big.Int).Add(a, b)
}

func weiSub(a, b *big.Int) (*big.Int, error) {
	out := new(big.Int).Sub(a, b)
	if out.Sign() < 0 {
		return nil, ErrInsufficientBalance
	}
	return out, nil
}

func weiCmp(a, b *big.Int) int {
	return a.Cmp(b)
}

func weiGte(a, b *big.Int) bool {
	return a.Cmp(b) >= 0
}
