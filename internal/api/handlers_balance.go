package api

import (
	"database/sql"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gin-gonic/gin"

	"gin-demo/internal/balance"
	"gin-demo/internal/config"
	"gin-demo/internal/eth"
)

// HandleBalanceCached 查余额：默认先读 DB 快照，?source=rpc 强制查链，?source=db 仅读库。
func HandleBalanceCached(cfg *config.Config, b *eth.Backend, balStore *balance.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		addr := strings.TrimSpace(c.Param("addr"))
		if !common.IsHexAddress(addr) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid hex address"})
			return
		}
		source := strings.ToLower(strings.TrimSpace(c.DefaultQuery("source", "auto")))
		token := strings.TrimSpace(c.DefaultQuery("token", balance.NativeToken))

		if balStore != nil && cfg.BalanceSync.Enabled && source != "rpc" {
			row, err := balStore.Get(c.Request.Context(), addr, token)
			if err == nil {
				stale := time.Since(row.UpdatedAt) > time.Duration(cfg.BalanceSync.StaleSec)*time.Second
				if source == "db" || !stale {
					out := gin.H{
						"address":        addr,
						"token_address":  displayToken(token),
						"balance_wei":    row.BalanceWei,
						"block_number":   row.BlockNumber,
						"source_tx_hash": row.SourceTxHash,
						"updated_at":     row.UpdatedAt,
						"source":         "database",
						"stale":          stale,
					}
					if token == balance.NativeToken {
						if w, ok := new(big.Int).SetString(row.BalanceWei, 10); ok {
							out["balance_eth"] = weiToETHString(w)
						}
					}
					c.JSON(http.StatusOK, out)
					return
				}
			} else if err != sql.ErrNoRows && source == "db" {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			} else if source == "db" {
				c.JSON(http.StatusNotFound, gin.H{"error": "balance snapshot not found"})
				return
			}
		}

		if token == balance.NativeToken || token == "" {
			w, err := b.BalanceAt(c.Request.Context(), addr)
			if err != nil {
				c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{
				"address":       addr,
				"token_address": "native",
				"balance_wei":   w.String(),
				"balance_eth":   weiToETHString(w),
				"source":        "rpc",
			})
			return
		}
		if !common.IsHexAddress(token) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid token address"})
			return
		}
		w, err := b.ERC20BalanceOf(c.Request.Context(), common.HexToAddress(token), common.HexToAddress(addr))
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"address":       addr,
			"token_address": token,
			"balance_wei":   w.String(),
			"source":        "rpc",
		})
	}
}

// HandleBalancesList 返回某地址在库中已同步的所有代币余额快照。
func HandleBalancesList(balStore *balance.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		if balStore == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "balance sync not enabled"})
			return
		}
		addr := strings.TrimSpace(c.Param("addr"))
		if !common.IsHexAddress(addr) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid hex address"})
			return
		}
		rows, err := balStore.ListByAddress(c.Request.Context(), addr)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"address": addr, "balances": rows, "count": len(rows)})
	}
}

// HandleBalanceRefresh 手动 RPC 刷新并写库。
func HandleBalanceRefresh(b *eth.Backend, syncer *balance.Syncer) gin.HandlerFunc {
	return func(c *gin.Context) {
		if syncer == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "balance sync not enabled"})
			return
		}
		addr := strings.TrimSpace(c.Param("addr"))
		if !common.IsHexAddress(addr) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid hex address"})
			return
		}
		token := strings.TrimSpace(c.DefaultQuery("token", balance.NativeToken))
		ctx := c.Request.Context()
		if token == balance.NativeToken || token == "" {
			if err := syncer.RefreshNative(ctx, addr, "", 0); err != nil {
				c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
				return
			}
		} else {
			if !common.IsHexAddress(token) {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid token"})
				return
			}
			if err := syncer.RefreshERC20(ctx, common.HexToAddress(token), addr, "", 0); err != nil {
				c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
				return
			}
		}
		row, err := syncer.Store().Get(ctx, addr, token)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"message": "refreshed", "address": addr})
			return
		}
		c.JSON(http.StatusOK, gin.H{"message": "refreshed", "balance": row})
	}
}

func displayToken(token string) string {
	if token == "" {
		return "native"
	}
	return token
}
