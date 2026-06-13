package api

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"gin-demo/internal/exchange"
	"gin-demo/internal/webhook"
)

type registerMerchantBody struct {
	MerchantID   string `json:"merchant_id" binding:"required"`
	Name         string `json:"name"`
	WebhookURL   string `json:"webhook_url" binding:"required"`
	Secret       string `json:"secret"`
	LedgerUserID string `json:"ledger_user_id"`
	Enabled      *bool  `json:"enabled"`
}

type bindingBody struct {
	PayerUserID string `json:"payer_user_id" binding:"required"`
}

type createPaymentBody struct {
	MerchantID     string `json:"merchant_id" binding:"required"`
	OrderID        string `json:"order_id" binding:"required"`
	PayerUserID    string `json:"payer_user_id" binding:"required"`
	AmountWei      string `json:"amount_wei" binding:"required"`
	TokenAddress   string `json:"token_address"`
	IdempotencyKey string `json:"idempotency_key"`
}

type benchEnqueueBody struct {
	MerchantID string `json:"merchant_id" binding:"required"`
	Count      int    `json:"count" binding:"required"`
}

func HandleWebhookStatus(svc *webhook.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "webhook not enabled"})
			return
		}
		st, err := svc.Status(c.Request.Context())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, st)
	}
}

func HandleMerchantRegister(svc *webhook.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "webhook not enabled"})
			return
		}
		var body registerMerchantBody
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		enabled := true
		if body.Enabled != nil {
			enabled = *body.Enabled
		}
		m, err := svc.RegisterMerchant(c.Request.Context(), webhook.RegisterMerchantParams{
			MerchantID: body.MerchantID, Name: body.Name, WebhookURL: body.WebhookURL,
			Secret: body.Secret, LedgerUserID: body.LedgerUserID, Enabled: enabled,
		})
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusCreated, gin.H{"merchant": m})
	}
}

func HandleMerchantList(svc *webhook.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "webhook not enabled"})
			return
		}
		limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
		rows, err := svc.ListMerchants(c.Request.Context(), limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"merchants": rows, "count": len(rows)})
	}
}

func HandleMerchantBind(svc *webhook.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "webhook not enabled"})
			return
		}
		merchantID := strings.TrimSpace(c.Param("merchant_id"))
		var body bindingBody
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if err := svc.AddBinding(c.Request.Context(), merchantID, body.PayerUserID); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"merchant_id": merchantID, "payer_user_id": body.PayerUserID, "status": "bound"})
	}
}

func HandleMerchantPayment(svc *webhook.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "webhook not enabled"})
			return
		}
		var body createPaymentBody
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		pay, err := svc.CreatePayment(c.Request.Context(), webhook.CreatePaymentParams{
			MerchantID: body.MerchantID, OrderID: body.OrderID, PayerUserID: body.PayerUserID,
			TokenAddress: body.TokenAddress, AmountWei: body.AmountWei, IdempotencyKey: body.IdempotencyKey,
		})
		if err != nil {
			if errors.Is(err, exchange.ErrInsufficientBalance) || errors.Is(err, webhook.ErrInsufficientFunds) {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusCreated, gin.H{"payment": pay})
	}
}

func HandleWebhookDeliveries(svc *webhook.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "webhook not enabled"})
			return
		}
		merchantID := strings.TrimSpace(c.Query("merchant_id"))
		limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
		rows, err := svc.ListDeliveries(c.Request.Context(), merchantID, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"deliveries": rows, "count": len(rows)})
	}
}

func HandleWebhookRequeue(svc *webhook.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "webhook not enabled"})
			return
		}
		id, err := strconv.ParseUint(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid outbox id"})
			return
		}
		if err := svc.Requeue(c.Request.Context(), id); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"outbox_id": id, "status": "pending"})
	}
}

func HandleWebhookBenchEnqueue(svc *webhook.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "webhook not enabled"})
			return
		}
		var body benchEnqueueBody
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if body.Count <= 0 || body.Count > 50000 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "count must be 1..50000"})
			return
		}
		n, err := svc.BenchEnqueue(c.Request.Context(), body.MerchantID, body.Count)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, gin.H{
			"merchant_id": body.MerchantID,
			"enqueued":    n,
			"message":     "outbox items created; workers will deliver asynchronously",
		})
	}
}

// HandleWebhookMockReceive 内置 Mock 商户回调（压测目标端点，极速 200）。
func HandleWebhookMockReceive() gin.HandlerFunc {
	return func(c *gin.Context) {
		_, _ = io.ReadAll(io.LimitReader(c.Request.Body, 4096))
		c.JSON(http.StatusOK, gin.H{"ok": true, "received": true})
	}
}
