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
	"gin-demo/internal/cache"
	"gin-demo/internal/config"
	"gin-demo/internal/eth"
	"gin-demo/internal/grpcsvc"
	"gin-demo/internal/indexer"
	"gin-demo/internal/pipeline"
	"gin-demo/internal/store"
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
	if cfg.Indexer.Enabled {
		if cfg.MySQL.Enabled {
			db, err := indexer.OpenMySQL(rootCtx, cfg)
			if err != nil {
				log.Printf("[indexer] MySQL 不可用，链下同步已关闭（主服务继续）: %v", err)
			} else {
				defer db.Close()
				c := cache.New(cfg)
				var err error
				idx, err = indexer.NewEngine(cfg, backend, db, c)
				if err != nil {
					log.Printf("[indexer] 初始化失败（ABI/配置错误）: %v", err)
				} else {
					idx.Start(rootCtx)
				}
			}
		} else {
			log.Println("[indexer] mysql.enabled=false，跳过链下同步")
		}
	}

	bus := pipeline.NewBus(pipeline.ListenerConfig{
		WorkerCount:   cfg.Listener.WorkerCount,
		ChannelBuffer: cfg.Listener.ChannelBuffer,
	}, idx)
	bus.Start(rootCtx)

	listener := eth.NewListener(cfg, backend, bus)
	go listener.Run(rootCtx)

	users := store.NewUserStore(cfg.Users)
	engine := api.NewRouter(cfg, backend, bus, users, idx)

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
