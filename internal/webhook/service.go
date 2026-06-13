package webhook

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	"gin-demo/internal/config"
	"gin-demo/internal/exchange"
)

// Service 商户 Webhook 业务：注册、绑定、充值/支付触发 Outbox。
type Service struct {
	cfg      *config.Config
	store    *Store
	exStore  *exchange.Store
	metrics  *Metrics
	engine   *Engine
}

func NewService(cfg *config.Config, store *Store, exStore *exchange.Store) *Service {
	m := NewMetrics()
	return &Service{
		cfg:     cfg,
		store:   store,
		exStore: exStore,
		metrics: m,
		engine:  NewEngine(cfg, store, m),
	}
}

func (s *Service) Start(ctx context.Context) {
	if s != nil && s.engine != nil {
		s.engine.Start(ctx)
	}
}

func (s *Service) Metrics() *Metrics { return s.metrics }
func (s *Service) Store() *Store   { return s.store }

func (s *Service) Enabled() bool {
	return s != nil && s.cfg != nil && s.cfg.Webhook.Enabled
}

type RegisterMerchantParams struct {
	MerchantID   string
	Name         string
	WebhookURL   string
	Secret       string
	LedgerUserID string
	Enabled      bool
}

func (s *Service) RegisterMerchant(ctx context.Context, p RegisterMerchantParams) (Merchant, error) {
	if !s.Enabled() {
		return Merchant{}, ErrNotEnabled
	}
	p.MerchantID = strings.TrimSpace(p.MerchantID)
	p.WebhookURL = strings.TrimSpace(p.WebhookURL)
	if p.MerchantID == "" {
		return Merchant{}, fmt.Errorf("merchant_id required")
	}
	if p.WebhookURL == "" {
		return Merchant{}, ErrInvalidWebhookURL
	}
	if _, err := url.ParseRequestURI(p.WebhookURL); err != nil && !strings.HasPrefix(p.WebhookURL, "mock://") {
		return Merchant{}, ErrInvalidWebhookURL
	}
	if p.Secret == "" {
		p.Secret = "whsec-" + p.MerchantID
	}
	m := Merchant{
		ChainID:      s.store.chainID,
		MerchantID:   p.MerchantID,
		Name:         p.Name,
		WebhookURL:   p.WebhookURL,
		LedgerUserID: strings.TrimSpace(p.LedgerUserID),
		Enabled:      p.Enabled,
	}
	if err := s.store.UpsertMerchant(ctx, m, p.Secret); err != nil {
		return Merchant{}, err
	}
	return s.store.GetMerchantPublic(ctx, p.MerchantID)
}

func (s *Store) GetMerchantPublic(ctx context.Context, merchantID string) (Merchant, error) {
	m, _, err := s.GetMerchant(ctx, merchantID)
	return m, err
}

func (s *Service) AddBinding(ctx context.Context, merchantID, payerUserID string) error {
	if !s.Enabled() {
		return ErrNotEnabled
	}
	if _, _, err := s.store.GetMerchant(ctx, merchantID); err != nil {
		return ErrMerchantNotFound
	}
	return s.store.AddBinding(ctx, merchantID, strings.TrimSpace(payerUserID))
}

// EnqueueDepositTx 充值入账同一事务内写入 Webhook Outbox（Transactional Outbox）。
func (s *Service) EnqueueDepositTx(ctx context.Context, dbTx *sql.Tx, dep exchange.Deposit) error {
	if !s.Enabled() || s.store == nil {
		return nil
	}
	bindings, err := s.store.ListBindingsByPayer(ctx, dep.UserID)
	if err != nil || len(bindings) == 0 {
		return err
	}
	token := dep.TokenAddress
	if token == "" {
		token = "native"
	}
	confirmedAt := time.Now().UTC().Format(time.RFC3339)
	if dep.CreditedAt != nil {
		confirmedAt = dep.CreditedAt.UTC().Format(time.RFC3339)
	}
	for _, b := range bindings {
		payload := DepositPayload{
			EventType:   EventDepositConfirmed,
			MerchantID:  b.MerchantID,
			UserID:      dep.UserID,
			TxHash:      dep.TxHash,
			AmountWei:   dep.AmountWei,
			Token:       token,
			Status:      "confirmed",
			BlockNumber: dep.BlockNumber,
			ConfirmedAt: confirmedAt,
		}
		idem := fmt.Sprintf("deposit:%d", dep.ID)
		inserted, err := s.store.InsertOutboxTx(ctx, dbTx, b.MerchantID, EventDepositConfirmed, idem, payload)
		if err != nil {
			return err
		}
		if s.metrics != nil {
			if inserted {
				s.metrics.RecordEnqueue()
			} else {
				s.metrics.RecordDuplicate()
			}
		}
	}
	return nil
}

type CreatePaymentParams struct {
	MerchantID     string
	OrderID        string
	PayerUserID    string
	TokenAddress   string
	AmountWei      string
	IdempotencyKey string
}

// CreatePayment 平台内余额支付商家（链下双边账本 + Outbox 同事务）。
func (s *Service) CreatePayment(ctx context.Context, p CreatePaymentParams) (Payment, error) {
	if !s.Enabled() {
		return Payment{}, ErrNotEnabled
	}
	if s.exStore == nil {
		return Payment{}, fmt.Errorf("exchange store required")
	}
	m, _, err := s.store.GetMerchant(ctx, p.MerchantID)
	if err != nil {
		return Payment{}, ErrMerchantNotFound
	}
	if !m.Enabled {
		return Payment{}, ErrMerchantDisabled
	}
	if m.LedgerUserID == "" {
		return Payment{}, fmt.Errorf("merchant ledger_user_id not configured")
	}
	p.PayerUserID = strings.TrimSpace(p.PayerUserID)
	p.OrderID = strings.TrimSpace(p.OrderID)
	if p.IdempotencyKey == "" {
		p.IdempotencyKey = "pay:" + p.MerchantID + ":" + p.OrderID
	}

	dbTx, err := s.exStore.BeginTx(ctx)
	if err != nil {
		return Payment{}, err
	}
	defer dbTx.Rollback()

	pay := Payment{
		ChainID:        s.store.chainID,
		MerchantID:     p.MerchantID,
		OrderID:        p.OrderID,
		PayerUserID:    p.PayerUserID,
		TokenAddress:   normToken(p.TokenAddress),
		AmountWei:      p.AmountWei,
		IdempotencyKey: p.IdempotencyKey,
	}
	payID, inserted, err := s.store.InsertPaymentTx(ctx, dbTx, pay)
	if err != nil {
		if err == ErrDuplicatePayment {
			// 幂等：已存在则直接返回（不重复扣款）
			if s.metrics != nil {
				s.metrics.RecordDuplicate()
			}
			return pay, nil
		}
		return Payment{}, err
	}
	pay.ID = payID

	if inserted {
		if err := s.exStore.TransferInternalTx(ctx, dbTx, p.PayerUserID, m.LedgerUserID, pay.TokenAddress, pay.AmountWei, payID); err != nil {
			return Payment{}, err
		}
		token := pay.TokenAddress
		if token == "" {
			token = "native"
		}
		payload := PaymentPayload{
			EventType:  EventPaymentSuccess,
			MerchantID: p.MerchantID,
			OrderID:    p.OrderID,
			PayerID:    p.PayerUserID,
			AmountWei:  pay.AmountWei,
			Token:      token,
			Status:     "completed",
			PaidAt:     time.Now().UTC().Format(time.RFC3339),
		}
		ok, err := s.store.InsertOutboxTx(ctx, dbTx, p.MerchantID, EventPaymentSuccess, p.IdempotencyKey, payload)
		if err != nil {
			return Payment{}, err
		}
		if s.metrics != nil {
			if ok {
				s.metrics.RecordEnqueue()
			} else {
				s.metrics.RecordDuplicate()
			}
		}
	}

	if err := dbTx.Commit(); err != nil {
		return Payment{}, err
	}
	pay.Status = "completed"
	pay.CreatedAt = time.Now().UTC()
	return pay, nil
}

func normToken(t string) string {
	return strings.TrimSpace(strings.ToLower(t))
}

func (s *Service) Status(ctx context.Context) (Status, error) {
	if !s.Enabled() {
		return Status{Enabled: false}, nil
	}
	pending, _ := s.store.CountOutboxByStatus(ctx, StatusPending)
	processing, _ := s.store.CountOutboxByStatus(ctx, StatusProcessing)
	st := s.metrics.Snapshot(pending, processing)
	st.Enabled = true
	st.Workers = s.cfg.Webhook.Workers
	st.BatchSize = s.cfg.Webhook.BatchSize
	return st, nil
}

func (s *Service) BenchEnqueue(ctx context.Context, merchantID string, n int) (int, error) {
	if !s.Enabled() {
		return 0, ErrNotEnabled
	}
	inserted, err := s.store.BenchEnqueue(ctx, merchantID, EventDepositConfirmed, n)
	if s.metrics != nil && inserted > 0 {
		for i := 0; i < inserted; i++ {
			s.metrics.RecordEnqueue()
		}
	}
	return inserted, err
}

func (s *Service) Requeue(ctx context.Context, outboxID uint64) error {
	return s.store.RequeueOutbox(ctx, outboxID)
}

func (s *Service) ListDeliveries(ctx context.Context, merchantID string, limit int) ([]Delivery, error) {
	return s.store.ListDeliveries(ctx, merchantID, limit)
}

func (s *Service) ListMerchants(ctx context.Context, limit int) ([]Merchant, error) {
	return s.store.ListMerchants(ctx, limit)
}
