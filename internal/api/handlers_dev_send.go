package api

import (
	"fmt"
	"math/big"
	"net/http"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/gin-gonic/gin"

	"gin-demo/internal/config"
	"gin-demo/internal/eth"
)

type sendEthBody struct {
	FromPrivateKey string `json:"from_private_key" binding:"required"`
	To             string `json:"to" binding:"required"`
	ValueWei       string `json:"value_wei"`
	ValueETH       string `json:"value_eth"`
}

// HandleDevSendETH 仅在配置 eth.dev_send_enabled=true 时可用：用给定私钥发起一笔原生币转账（本地测试；勿对主网开启）。
func HandleDevSendETH(cfg *config.Config, b *eth.Backend) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !cfg.Eth.DevSendEnabled {
			c.JSON(http.StatusForbidden, gin.H{
				"error": "dev send disabled: set eth.dev_send_enabled to true in configs/config.yaml (local Anvil only; never on production)",
			})
			return
		}

		var req sendEthBody
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		to := strings.TrimSpace(req.To)
		if !common.IsHexAddress(to) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid to address"})
			return
		}

		val, err := parseTransferValue(strings.TrimSpace(req.ValueWei), strings.TrimSpace(req.ValueETH))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		pkHex := strings.TrimSpace(req.FromPrivateKey)
		if strings.HasPrefix(pkHex, "0x") || strings.HasPrefix(pkHex, "0X") {
			pkHex = pkHex[2:]
		}
		key, err := crypto.HexToECDSA(pkHex)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid from_private_key"})
			return
		}

		hash, err := b.SendETHTransfer(c.Request.Context(), key, common.HexToAddress(to), val)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}

		from := crypto.PubkeyToAddress(key.PublicKey)
		c.JSON(http.StatusOK, gin.H{
			"tx_hash": hash.Hex(),
			"from":    from.Hex(),
			"to":      to,
			"value_wei": val.String(),
		})
	}
}

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
