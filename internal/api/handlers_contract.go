package api

import (
	"math/big"
	"net/http"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gin-gonic/gin"

	"gin-demo/internal/eth"
)

// HandleERC20Balance 通过 ABI 调用 ERC-20 balanceOf（eth_call，不上链）。
// Query: token=合约地址&holder=持币地址
func HandleERC20Balance(b *eth.Backend) gin.HandlerFunc {
	return func(c *gin.Context) {
		tok := strings.TrimSpace(c.Query("token"))
		hold := strings.TrimSpace(c.Query("holder"))
		if !common.IsHexAddress(tok) || !common.IsHexAddress(hold) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "query token and holder must be valid hex addresses"})
			return
		}
		ctx := c.Request.Context()
		bal, err := b.ERC20BalanceOf(ctx, common.HexToAddress(tok), common.HexToAddress(hold))
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		out := gin.H{
			"token":       tok,
			"holder":      hold,
			"balance_raw": bal.String(),
		}
		if dec, sym, err := b.ERC20DecimalsSymbol(ctx, common.HexToAddress(tok)); err == nil {
			out["symbol"] = sym
			out["decimals"] = dec
			denom := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(dec)), nil)
			if denom.Sign() > 0 {
				s := new(big.Rat).SetFrac(bal, denom).FloatString(8)
				s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
				out["balance_display"] = s
			}
		}
		c.JSON(http.StatusOK, out)
	}
}

// HandleERC20TokenInfo 调用 decimals、symbol（只读）。
// Query: token=合约地址
func HandleERC20TokenInfo(b *eth.Backend) gin.HandlerFunc {
	return func(c *gin.Context) {
		tok := strings.TrimSpace(c.Query("token"))
		if !common.IsHexAddress(tok) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid token address"})
			return
		}
		dec, sym, err := b.ERC20DecimalsSymbol(c.Request.Context(), common.HexToAddress(tok))
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"token":    tok,
			"symbol":   sym,
			"decimals": dec,
		})
	}
}

// HandleCounterNumber 调用 Counter.number()（与 contracts/Counter.sol 配套）。
// Query: contract=已部署的 Counter 地址
func HandleCounterNumber(b *eth.Backend) gin.HandlerFunc {
	return func(c *gin.Context) {
		addr := strings.TrimSpace(c.Query("contract"))
		if !common.IsHexAddress(addr) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid contract address"})
			return
		}
		n, err := b.CounterNumber(c.Request.Context(), common.HexToAddress(addr))
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"contract": addr,
			"number":   n.String(),
		})
	}
}
