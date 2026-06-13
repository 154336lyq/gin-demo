// Package api 注册 Gin 路由：RESTful 分组、JWT 中间件、链上查询与文件接口。
package api

import (
	"github.com/gin-gonic/gin"

	"gin-demo/internal/balance"
	"gin-demo/internal/config"
	"gin-demo/internal/eth"
	"gin-demo/internal/exchange"
	"gin-demo/internal/indexer"
	"gin-demo/internal/pipeline"
	"gin-demo/internal/store"
	"gin-demo/internal/tx"
	"gin-demo/internal/webhook"
)

// NewRouter 构造 HTTP 服务。
func NewRouter(cfg *config.Config, b *eth.Backend, bus *pipeline.Bus, users *store.UserStore, idx *indexer.Engine, txTr *tx.Tracker, txSvc *tx.Service, balStore *balance.Store, balSync *balance.Syncer, balRegistry *balance.Registry, exchangeSvc *exchange.Service, webhookSvc *webhook.Service) *gin.Engine {
	if cfg.Server.GinMode == "release" {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok", "service": "chain-backend"})
	})

	v1 := r.Group("/api/v1")
	v1.POST("/auth/login", HandleLogin(cfg, users))

	authz := v1.Group("")
	authz.Use(JWTMiddleware(cfg))
	authz.GET("/users", HandleListUsers(users))

	authz.GET("/blocks/latest", HandleLatestBlock(b))
	authz.GET("/blocks/hash/:hash", HandleBlockByHash(b))
	authz.GET("/blocks/:number", HandleBlockByNumber(b))
	authz.GET("/transactions/:hash/receipt", HandleTxReceipt(b))
	authz.GET("/transactions/:hash", HandleTransaction(b))

	authz.GET("/accounts/:addr/transactions", HandleAccountTransactions(b))
	authz.GET("/accounts/:addr/transaction", HandleAccountTransactions(b)) // 与 Apifox 误写单数兼容
	authz.GET("/accounts/:addr/balance", HandleBalanceCached(cfg, b, balStore))
	authz.GET("/accounts/:addr/balances", HandleBalancesList(balStore))
	authz.POST("/accounts/:addr/balance/refresh", HandleBalanceRefresh(b, balSync))
	authz.GET("/accounts/:addr", HandleAccountInfo(cfg, b, balStore))

	authz.POST("/wallets", HandleWalletRegister(balStore, balSync, balRegistry))
	authz.GET("/wallets", HandleWalletList(balStore))
	authz.POST("/wallets/backfill", HandleWalletBackfill(balSync))
	authz.GET("/wallets/:addr", HandleWalletGet(balStore))
	authz.PATCH("/wallets/:addr", HandleWalletSetEnabled(balStore, balRegistry))
	authz.POST("/wallets/:addr/refresh", HandleWalletRefresh(balStore, balSync))

	// Mock 商户回调（压测接收端，无需 JWT）
	v1.POST("/webhook/mock/receive", HandleWebhookMockReceive())

	if webhookSvc != nil {
		authz.GET("/webhook/status", HandleWebhookStatus(webhookSvc))
		authz.POST("/webhook/merchants", HandleMerchantRegister(webhookSvc))
		authz.GET("/webhook/merchants", HandleMerchantList(webhookSvc))
		authz.POST("/webhook/merchants/:merchant_id/bindings", HandleMerchantBind(webhookSvc))
		authz.POST("/webhook/payments", HandleMerchantPayment(webhookSvc))
		authz.GET("/webhook/deliveries", HandleWebhookDeliveries(webhookSvc))
		authz.POST("/webhook/outbox/:id/requeue", HandleWebhookRequeue(webhookSvc))
		authz.POST("/webhook/bench/enqueue", HandleWebhookBenchEnqueue(webhookSvc))
	}

	if exchangeSvc != nil {
		authz.GET("/ledger/:user_id/balances", HandleLedgerBalances(exchangeSvc))
		authz.GET("/ledger/:user_id/entries", HandleLedgerEntries(exchangeSvc))
		authz.GET("/deposits", HandleDepositList(exchangeSvc))
		authz.POST("/withdrawals", HandleWithdrawCreate(exchangeSvc))
		authz.GET("/withdrawals", HandleWithdrawList(exchangeSvc))
		authz.GET("/withdrawals/:id", HandleWithdrawGet(exchangeSvc))
		authz.POST("/withdrawals/:id/approve", HandleWithdrawApprove(exchangeSvc))
		authz.POST("/withdrawals/:id/reject", HandleWithdrawReject(exchangeSvc))
		authz.GET("/reconcile", HandleReconcileReport(exchangeSvc))
	}

	// Solidity / ABI：只读 eth_call（ERC-20、示例 Counter）
	authz.GET("/contracts/erc20/balance", HandleERC20Balance(b))
	authz.GET("/contracts/erc20/info", HandleERC20TokenInfo(b))
	authz.GET("/contracts/counter/number", HandleCounterNumber(b))

	authz.GET("/pipeline/stats", HandlePipelineStats(bus))

	if idx != nil {
		authz.GET("/indexer/status", HandleIndexerStatus(idx))
		authz.GET("/indexer/blocks", HandleIndexerBlocks(idx))
		authz.GET("/indexer/blocks/:number", HandleIndexerBlockByNumber(idx))
		authz.GET("/indexer/transactions", HandleIndexerTransactions(idx))
		authz.GET("/indexer/events", HandleIndexerEvents(idx))
		authz.GET("/indexer/gap-scans", HandleIndexerGapScans(idx))
	}

	// 网络请求探测（可在此扩展为「爬取 + 解析」实战）。
	authz.GET("/tools/http-probe", HandleHTTPProbe())

	authz.POST("/tx/submit", HandleTxSubmit(cfg, txSvc))
	authz.POST("/tx/send", HandleTxSend(cfg, txSvc))
	authz.POST("/tx/send-erc20", HandleTxSendERC20(cfg, txSvc))
	authz.POST("/tx/:hash/speed-up", HandleTxSpeedUp(cfg, txSvc))
	authz.GET("/tx/:hash", HandleTxGet(txTr))
	authz.GET("/tx", HandleTxList(txTr))

	authz.POST("/files/upload", HandleUpload(cfg))
	authz.GET("/files/by-sha/:sha", HandleDownloadBySHA(cfg))

	return r
}
