package api

import (
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gin-gonic/gin"

	"gin-demo/internal/exchange"
)

type createWithdrawBody struct {
	UserID         string `json:"user_id" binding:"required"`
	TokenAddress   string `json:"token_address"`
	To             string `json:"to" binding:"required"`
	AmountWei      string `json:"amount_wei" binding:"required"`
	IdempotencyKey string `json:"idempotency_key"`
}

type reviewWithdrawBody struct {
	Reviewer     string `json:"reviewer"`
	RejectReason string `json:"reject_reason"`
}

func HandleLedgerBalances(svc *exchange.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "exchange not enabled"})
			return
		}
		userID := strings.TrimSpace(c.Param("user_id"))
		if userID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "user_id required"})
			return
		}
		rows, err := svc.GetUserBalances(c.Request.Context(), userID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"user_id": userID, "balances": rows})
	}
}

func HandleLedgerEntries(svc *exchange.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "exchange not enabled"})
			return
		}
		userID := strings.TrimSpace(c.Param("user_id"))
		limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
		rows, err := svc.GetUserEntries(c.Request.Context(), userID, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"user_id": userID, "entries": rows})
	}
}

func HandleDepositList(svc *exchange.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "exchange not enabled"})
			return
		}
		userID := strings.TrimSpace(c.Query("user_id"))
		limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
		rows, err := svc.ListDeposits(c.Request.Context(), userID, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"deposits": rows, "count": len(rows)})
	}
}

func HandleWithdrawCreate(svc *exchange.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "exchange not enabled"})
			return
		}
		var req createWithdrawBody
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if !common.IsHexAddress(req.To) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid to address"})
			return
		}
		idem := req.IdempotencyKey
		if idem == "" {
			idem = strings.TrimSpace(c.GetHeader("Idempotency-Key"))
		}
		w, err := svc.CreateWithdraw(c.Request.Context(), exchange.CreateWithdrawParams{
			UserID: req.UserID, TokenAddress: req.TokenAddress,
			ToAddress: req.To, AmountWei: req.AmountWei, IdempotencyKey: idem,
		})
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, exchange.ErrInsufficientBalance) || errors.Is(err, exchange.ErrInvalidWei) {
				status = http.StatusBadRequest
			}
			c.JSON(status, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusCreated, gin.H{"withdraw": w})
	}
}

func HandleWithdrawList(svc *exchange.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "exchange not enabled"})
			return
		}
		userID := strings.TrimSpace(c.Query("user_id"))
		status := strings.TrimSpace(c.Query("status"))
		limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
		rows, err := svc.ListWithdraws(c.Request.Context(), userID, status, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"withdraws": rows, "count": len(rows)})
	}
}

func HandleWithdrawGet(svc *exchange.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "exchange not enabled"})
			return
		}
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
			return
		}
		w, err := svc.Store().GetWithdraw(c.Request.Context(), id)
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"withdraw": w})
	}
}

func HandleWithdrawApprove(svc *exchange.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "exchange not enabled"})
			return
		}
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
			return
		}
		var req reviewWithdrawBody
		_ = c.ShouldBindJSON(&req)
		reviewer := req.Reviewer
		if reviewer == "" {
			reviewer = c.GetString("username")
		}
		w, err := svc.ApproveWithdraw(c.Request.Context(), id, reviewer)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"withdraw": w})
	}
}

func HandleWithdrawReject(svc *exchange.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "exchange not enabled"})
			return
		}
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
			return
		}
		var req reviewWithdrawBody
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		reviewer := req.Reviewer
		if reviewer == "" {
			reviewer = c.GetString("username")
		}
		w, err := svc.RejectWithdraw(c.Request.Context(), id, reviewer, req.RejectReason)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"withdraw": w})
	}
}

func HandleReconcileReport(svc *exchange.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "exchange not enabled"})
			return
		}
		reports, err := svc.RunReconcile(c.Request.Context())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"reports": reports})
	}
}
