package api

import (
	"database/sql"
	"net/http"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gin-gonic/gin"

	"gin-demo/internal/balance"
)

type registerWalletBody struct {
	Address    string `json:"address" binding:"required"`
	UserID     string `json:"user_id"`
	Label      string `json:"label"`
	WalletType string `json:"wallet_type"`
}

// HandleWalletRegister 注册托管地址（热钱包/用户充值地址），注册后异步拉取链上余额快照。
func HandleWalletRegister(store *balance.Store, syncer *balance.Syncer, registry *balance.Registry) gin.HandlerFunc {
	return func(c *gin.Context) {
		if store == nil || syncer == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "balance sync not enabled"})
			return
		}
		var req registerWalletBody
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		addr := strings.TrimSpace(req.Address)
		if !common.IsHexAddress(addr) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid address"})
			return
		}
		wt := strings.TrimSpace(req.WalletType)
		if wt != "" && wt != balance.WalletTypeHot && wt != balance.WalletTypeDeposit && wt != balance.WalletTypeTreasury {
			c.JSON(http.StatusBadRequest, gin.H{"error": "wallet_type must be hot, deposit, or treasury"})
			return
		}
		w, err := store.RegisterWallet(c.Request.Context(), balance.RegisterWalletParams{
			Address: addr, UserID: req.UserID, Label: req.Label, WalletType: wt,
		})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if registry != nil {
			registry.Add(w.Address)
		}
		syncer.RefreshWalletAsync(w.Address)
		c.JSON(http.StatusCreated, gin.H{"message": "wallet registered", "wallet": w})
	}
}

// HandleWalletList 列出托管地址。
func HandleWalletList(store *balance.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		if store == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "balance sync not enabled"})
			return
		}
		userID := strings.TrimSpace(c.Query("user_id"))
		enabledOnly := c.DefaultQuery("enabled", "true") != "false"
		limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
		rows, err := store.ListWallets(c.Request.Context(), userID, enabledOnly, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"wallets": rows, "count": len(rows)})
	}
}

// HandleWalletGet 查询单个托管地址注册信息。
func HandleWalletGet(store *balance.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		if store == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "balance sync not enabled"})
			return
		}
		addr := strings.TrimSpace(c.Param("addr"))
		if !common.IsHexAddress(addr) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid address"})
			return
		}
		w, err := store.GetWallet(c.Request.Context(), addr)
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "wallet not registered"})
			return
		}
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"wallet": w})
	}
}

// HandleWalletRefresh 手动刷新某托管地址全部余额（native + watch_tokens）。
func HandleWalletRefresh(syncer *balance.Syncer) gin.HandlerFunc {
	return func(c *gin.Context) {
		if syncer == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "balance sync not enabled"})
			return
		}
		addr := strings.TrimSpace(c.Param("addr"))
		if !common.IsHexAddress(addr) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid address"})
			return
		}
		if err := syncer.RefreshWallet(c.Request.Context(), addr); err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		rows, err := syncer.Store().ListByAddress(c.Request.Context(), addr)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"message": "refreshed", "address": addr})
			return
		}
		c.JSON(http.StatusOK, gin.H{"message": "refreshed", "address": addr, "balances": rows})
	}
}

// HandleWalletBackfill 触发全量 backfill（所有 enabled 托管地址）。
func HandleWalletBackfill(syncer *balance.Syncer) gin.HandlerFunc {
	return func(c *gin.Context) {
		if syncer == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "balance sync not enabled"})
			return
		}
		n, err := syncer.RefreshAllRegistered(c.Request.Context())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"message": "backfill done", "wallets_refreshed": n})
	}
}
