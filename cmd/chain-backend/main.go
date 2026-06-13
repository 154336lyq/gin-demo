// chain-backend：小型区块链后端实战演示入口。
//
// 聚合能力概览（对应课程知识点）：
// - Gin：路由分组、RESTful、中间件、JWT、文件上传下载（SHA-256）
// - Viper：configs/config.yaml
// - Protocol Buffers / gRPC：pb/chain.proto 生成的 ChainQuery 服务
// - go-ethereum：HTTP RPC 查询 +（可选）WS 订阅新区块与 ERC-20 Transfer 日志
// - 并发：pipeline 包内 Channel + WaitGroup + Mutex/原子操作；sync.Map 见文件上传索引
// - 网络：internal/crawler 提供 HTTP 探测扩展位（课程「爬虫实战」可从此演进）
//
// 启动前请阅读 cmd/chain-backend/README.md（本地 Anvil 测试网步骤）。

package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"gin-demo/internal/api"
	"gin-demo/internal/balance"
	"gin-demo/internal/cache"
	"gin-demo/internal/config"
	"gin-demo/internal/eth"
	"gin-demo/internal/exchange"
	"gin-demo/internal/grpcsvc"
	"gin-demo/internal/indexer"
	"gin-demo/internal/pipeline"
	"gin-demo/internal/signer"
	"gin-demo/internal/store"
	"gin-demo/internal/tx"
	"gin-demo/internal/webhook"
	"gin-demo/pb/chainpb"
)

func main() {
	cfgPath := os.Getenv("CHAIN_CONFIG")
	if cfgPath == "" {
		cfgPath = "configs/config.yaml"
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("加载配置失败（请先复制 configs/config.example.yaml -> configs/config.yaml）: %v", err)
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	backend, err := eth.NewBackend(cfg)
	if err != nil {
		log.Fatalf("连接以太坊节点失败: %v", err)
	}
	defer backend.Close()

	if err := backend.VerifyChainID(rootCtx); err != nil {
		log.Fatalf("Chain ID 校验失败（请确认 configs 中 chain_id 与节点一致，Anvil 默认 31337）: %v", err)
	}

	var idx *indexer.Engine
	var txTr *tx.Tracker
	var txSvc *tx.Service
	var balStore *balance.Store
	var balSync *balance.Syncer
	var balRegistry *balance.Registry
	var exchangeSvc *exchange.Service
	var depositProc *exchange.DepositProcessor
	var exStore *exchange.Store
	var webhookSvc *webhook.Service
	if cfg.MySQL.Enabled {
		db, err := indexer.OpenMySQL(rootCtx, cfg)
		if err != nil {
			log.Printf("[mysql] 不可用（indexer/tx_tracker 关闭）: %v", err)
		} else {
			defer db.Close()
			if cfg.BalanceSync.Enabled {
				balStore, err = balance.NewStore(rootCtx, db, cfg.Eth.ChainID)
				if err != nil {
					log.Printf("[balance] 初始化失败: %v", err)
				} else {
					balRegistry = balance.NewRegistry(balStore)
					if err := balRegistry.Reload(rootCtx); err != nil {
						log.Printf("[balance] registry reload: %v", err)
					}
					balSync = balance.NewSyncer(cfg, backend, balStore, balRegistry)
					go balance.StartBackfillWorker(rootCtx, cfg, balSync)
					log.Println("[balance] 托管余额同步已启用 (custodial_wallets + account_balances)")
				}
			}
			if cfg.Exchange.Enabled && balStore != nil {
				exStore, err = exchange.NewStore(rootCtx, db, cfg.Eth.ChainID)
				if err != nil {
					log.Printf("[exchange] 初始化失败: %v", err)
				} else {
					depositProc = exchange.NewDepositProcessor(cfg, exStore, balRegistry)
					log.Printf("[exchange] 业务层 schema 就绪 deposit=%v auto_approve=%v",
						cfg.Exchange.DepositEnabled, cfg.Exchange.AutoApproveWithdraw)
				}
			}
			if cfg.Webhook.Enabled && exStore != nil {
				whStore, err := webhook.NewStore(rootCtx, db, cfg.Eth.ChainID)
				if err != nil {
					log.Printf("[webhook] 初始化失败: %v", err)
				} else {
					webhookSvc = webhook.NewService(cfg, whStore, exStore)
					webhookSvc.Start(rootCtx)
					if depositProc != nil {
						depositProc.SetDepositNotifier(webhookSvc)
					}
					log.Printf("[webhook] 商户推送中心已启用 workers=%d batch=%d mock=%s",
						cfg.Webhook.Workers, cfg.Webhook.BatchSize, cfg.Webhook.MockReceiveURL)
				}
			}
			if cfg.Indexer.Enabled {
				c := cache.New(cfg)
				idx, err = indexer.NewEngine(cfg, backend, db, c)
				if err != nil {
					log.Printf("[indexer] 初始化失败: %v", err)
				} else {
					if balSync != nil {
						idx.SetBalanceSyncer(balSync)
					}
					if depositProc != nil {
						idx.SetDepositProcessor(depositProc)
					}
					idx.Start(rootCtx)
				}
			}
			if cfg.TxTracker.Enabled {
				store, err := tx.NewStore(rootCtx, db, cfg.Eth.ChainID)
				if err != nil {
					log.Printf("[tx/tracker] schema/init failed: %v", err)
				} else {
					txTr = tx.NewTracker(cfg, backend, store, balSync)
					txTr.Start(rootCtx)
					txSvc = tx.NewService(cfg, backend, store)
					if exStore != nil && balStore != nil {
						sig := signer.New(cfg)
						exchangeSvc = exchange.NewService(cfg, exStore, balStore, sig, txSvc)
						txTr.SetWithdrawHandler(exchangeSvc)
						exchange.StartWorkers(rootCtx, cfg, exchangeSvc)
						log.Printf("[exchange] 已启用 signer=%s withdraw_worker=on", sig.BackendName())
					}
					log.Println("[tx/tracker] 已启用 (submit-raw + db-nonce + outbox)")
				}
			}
		}
	} else {
		log.Println("[mysql] mysql.enabled=false，跳过 indexer 与 tx_tracker")
	}

	if cfg.Indexer.Enabled && idx == nil {
		log.Println("[indexer] 未启动（MySQL 不可用或初始化失败）")
	}

	bus := pipeline.NewBus(pipeline.ListenerConfig{
		WorkerCount:   cfg.Listener.WorkerCount,
		ChannelBuffer: cfg.Listener.ChannelBuffer,
	}, idx)
	bus.Start(rootCtx)

	listener := eth.NewListener(cfg, backend, bus)
	go listener.Run(rootCtx)

	users := store.NewUserStore(cfg.Users)
	engine := api.NewRouter(cfg, backend, bus, users, idx, txTr, txSvc, balStore, balSync, balRegistry, exchangeSvc, webhookSvc)

	httpSrv := &http.Server{
		Addr:              cfg.Server.HTTPAddr,
		Handler:           engine,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
	}

	go func() {
		log.Printf("HTTP(Gin) 监听 %s （健康检查 GET /health）", cfg.Server.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	lis, err := net.Listen("tcp", cfg.Server.GRPCAddr)
	if err != nil {
		log.Fatalf("grpc listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	chainpb.RegisterChainQueryServer(grpcSrv, grpcsvc.NewChainServer(backend))
	go func() {
		log.Printf("gRPC 监听 %s （ChainQuery/GetLatestBlock）", cfg.Server.GRPCAddr)
		if err := grpcSrv.Serve(lis); err != nil {
			log.Printf("grpc 退出: %v", err)
		}
	}()

	<-rootCtx.Done()
	log.Println("收到退出信号，开始优雅停机…")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	grpcSrv.GracefulStop()

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP Shutdown: %v", err)
	}

	// 等待流水线 worker 排空（ctx 已取消 -> Bus 关闭 channel）。
	bus.Wait()

	log.Println("chain-backend 已退出。")
}
