// Package api 注册 Gin 路由：RESTful 分组、JWT 中间件、链上查询与文件接口。
package api

import (
	"github.com/gin-gonic/gin"

	"gin-demo/internal/config"
	"gin-demo/internal/eth"
	"gin-demo/internal/indexer"
	"gin-demo/internal/pipeline"
	"gin-demo/internal/store"
)

// NewRouter 构造 HTTP 服务。
func NewRouter(cfg *config.Config, b *eth.Backend, bus *pipeline.Bus, users *store.UserStore, idx *indexer.Engine) *gin.Engine {
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
	authz.GET("/accounts/:addr/balance", HandleBalance(b))
	authz.GET("/accounts/:addr", HandleAccountInfo(b))

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
	authz.POST("/tools/send-eth", HandleDevSendETH(cfg, b))

	authz.POST("/files/upload", HandleUpload(cfg))
	authz.GET("/files/by-sha/:sha", HandleDownloadBySHA(cfg))

	return r
}
