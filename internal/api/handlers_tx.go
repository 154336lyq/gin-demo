package api

import (
	"crypto/ecdsa"
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/gin-gonic/gin"

	"gin-demo/internal/config"
	"gin-demo/internal/tx"
)

type sendTxBody struct {
	FromPrivateKey string `json:"from_private_key"`
	To             string `json:"to" binding:"required"`
	ValueWei       string `json:"value_wei"`
	ValueETH       string `json:"value_eth"`
	BizID          string `json:"biz_id"`
	BizType        string `json:"biz_type"`
	IdempotencyKey string `json:"idempotency_key"`
}

type submitTxBody struct {
	SignedRawTx    string `json:"signed_raw_tx" binding:"required"`
	BizID          string `json:"biz_id"`
	BizType        string `json:"biz_type"`
	IdempotencyKey string `json:"idempotency_key"`
	ReplacesHash   string `json:"replaces_hash"`
}

type sendERC20Body struct {
	FromPrivateKey string `json:"from_private_key" binding:"required"`
	Token          string `json:"token" binding:"required"`
	To             string `json:"to" binding:"required"`
	AmountWei      string `json:"amount_wei" binding:"required"`
	BizID          string `json:"biz_id"`
	BizType        string `json:"biz_type"`
	IdempotencyKey string `json:"idempotency_key"`
}

type speedUpBody struct {
	FromPrivateKey string `json:"from_private_key" binding:"required"`
}

func sendMetaFromRequest(c *gin.Context, bizID, bizType, idempotency string) tx.SendMeta {
	if idempotency == "" {
		idempotency = strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	}
	return tx.SendMeta{BizID: bizID, BizType: bizType, IdempotencyKey: idempotency}
}

// HandleTxSubmit 生产主路径：提交已签名 raw tx（MetaMask/KMS 签名后调用）。
func HandleTxSubmit(cfg *config.Config, svc *tx.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "tx tracker not enabled"})
			return
		}
		var req submitTxBody
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		idem := req.IdempotencyKey
		if idem == "" {
			idem = strings.TrimSpace(c.GetHeader("Idempotency-Key"))
		}
		if cfg.TxTracker.RequireIdempotencyKey && idem == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": tx.ErrIdempotencyRequired.Error()})
			return
		}
		row, err := svc.SubmitRaw(c.Request.Context(), tx.SubmitRequest{
			SignedRawTx:    req.SignedRawTx,
			BizID:          req.BizID,
			BizType:        req.BizType,
			IdempotencyKey: idem,
			ReplacesHash:   normalizeTxHash(req.ReplacesHash),
		})
		if errors.Is(err, tx.ErrIdempotencyConflict) {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			return
		}
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, gin.H{"message": "transaction submitted", "tx": row})
	}
}

// HandleTxSend 托管发原生币（仅 dev_send_enabled；本地 Anvil 联调）。
func HandleTxSend(cfg *config.Config, svc *tx.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !cfg.Eth.DevSendEnabled {
			c.JSON(http.StatusForbidden, gin.H{
				"error": "custodial send disabled: use POST /tx/submit with signed_raw_tx, or set eth.dev_send_enabled=true for local Anvil",
			})
			return
		}
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "tx tracker not enabled"})
			return
		}
		var req sendTxBody
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if strings.TrimSpace(req.FromPrivateKey) == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "from_private_key required for custodial send"})
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
		key, err := parsePrivateKey(req.FromPrivateKey)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		meta := sendMetaFromRequest(c, req.BizID, req.BizType, req.IdempotencyKey)
		row, err := svc.SendNative(c.Request.Context(), key, common.HexToAddress(to), val.String(), meta)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, gin.H{"message": "transaction submitted", "tx": row})
	}
}

// HandleTxSendERC20 托管发 ERC-20（dev_send_enabled）。
func HandleTxSendERC20(cfg *config.Config, svc *tx.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !cfg.Eth.DevSendEnabled {
			c.JSON(http.StatusForbidden, gin.H{"error": "custodial send disabled"})
			return
		}
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "tx tracker not enabled"})
			return
		}
		var req sendERC20Body
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		key, err := parsePrivateKey(req.FromPrivateKey)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		token := strings.TrimSpace(req.Token)
		to := strings.TrimSpace(req.To)
		if !common.IsHexAddress(token) || !common.IsHexAddress(to) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid token or to address"})
			return
		}
		meta := sendMetaFromRequest(c, req.BizID, req.BizType, req.IdempotencyKey)
		row, err := svc.SendERC20(c.Request.Context(), key, common.HexToAddress(token), common.HexToAddress(to), req.AmountWei, meta)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, gin.H{"message": "erc20 transfer submitted", "tx": row})
	}
}

// HandleTxSpeedUp 加速 pending tx（RBF，托管/dev）。
func HandleTxSpeedUp(cfg *config.Config, svc *tx.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !cfg.Eth.DevSendEnabled {
			c.JSON(http.StatusForbidden, gin.H{
				"error": "custodial speed-up disabled; use POST /tx/submit with replaces_hash",
			})
			return
		}
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "tx tracker not enabled"})
			return
		}
		hash := normalizeTxHash(c.Param("hash"))
		var req speedUpBody
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		key, err := parsePrivateKey(req.FromPrivateKey)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		row, err := svc.SpeedUp(c.Request.Context(), key, hash)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, gin.H{"message": "speed-up transaction submitted", "tx": row})
	}
}

// HandleTxGet 查询单笔；pending 时即时刷新。
func HandleTxGet(tr *tx.Tracker) gin.HandlerFunc {
	return func(c *gin.Context) {
		if tr == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "tx tracker not enabled"})
			return
		}
		raw := normalizeTxHash(c.Param("hash"))
		if raw == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "hash required"})
			return
		}
		row, err := tr.RefreshOne(c.Request.Context(), raw)
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "tx not found"})
			return
		}
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"tx": row})
	}
}

// HandleTxList 按 from / status 分页查询。
func HandleTxList(tr *tx.Tracker) gin.HandlerFunc {
	return func(c *gin.Context) {
		if tr == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "tx tracker not enabled"})
			return
		}
		from := strings.TrimSpace(c.Query("from"))
		status := strings.TrimSpace(c.Query("status"))
		limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
		if limit <= 0 || limit > 200 {
			limit = 50
		}
		rows, err := tr.Store().ListByFrom(c.Request.Context(), from, status, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"from": from, "status": status, "limit": limit, "txs": rows})
	}
}

func parsePrivateKey(pkHex string) (*ecdsa.PrivateKey, error) {
	pkHex = strings.TrimSpace(pkHex)
	if strings.HasPrefix(pkHex, "0x") || strings.HasPrefix(pkHex, "0X") {
		pkHex = pkHex[2:]
	}
	return crypto.HexToECDSA(pkHex)
}
