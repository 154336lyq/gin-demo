// Package config 使用 Viper 加载 YAML，演示「配置与代码分离」。
package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// Config 聚合服务所需的全部可调参数。
type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	JWT      JWTConfig      `mapstructure:"jwt"`
	Users    []UserSeed     `mapstructure:"users"`
	Eth      EthConfig      `mapstructure:"eth"`
	Listener ListenerConfig `mapstructure:"listener"`
	Files    FilesConfig    `mapstructure:"files"`
	MySQL    MySQLConfig    `mapstructure:"mysql"`
	Redis    RedisConfig    `mapstructure:"redis"`
	Indexer     IndexerConfig     `mapstructure:"indexer"`
	TxTracker   TxTrackerConfig   `mapstructure:"tx_tracker"`
	BalanceSync BalanceSyncConfig `mapstructure:"balance_sync"`
	Exchange    ExchangeConfig    `mapstructure:"exchange"`
}

type ServerConfig struct {
	GinMode       string `mapstructure:"gin_mode"`
	HTTPAddr      string `mapstructure:"http_addr"`
	GRPCAddr      string `mapstructure:"grpc_addr"`
	PublicBaseURL string `mapstructure:"public_base_url"`
}

type JWTConfig struct {
	Secret      string `mapstructure:"secret"`
	ExpireHours int    `mapstructure:"expire_hours"`
}

// UserSeed 内存登录演示账号（非生产）。
type UserSeed struct {
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

type EthConfig struct {
	ChainID       int64  `mapstructure:"chain_id"`
	HTTPRPC       string `mapstructure:"http_rpc"`
	WSRPC         string `mapstructure:"ws_rpc"`
	ERC20Contract string `mapstructure:"erc20_contract"`
	// DevSendEnabled 为 true 时开放 POST /api/v1/tools/send-eth（仅本地测试；切勿在生产环境开启）。
	DevSendEnabled bool `mapstructure:"dev_send_enabled"`
}

type ListenerConfig struct {
	WorkerCount      int `mapstructure:"worker_count"`
	ChannelBuffer    int `mapstructure:"channel_buffer"`
	ReconnectBaseMS  int `mapstructure:"reconnect_base_ms"`
}

type FilesConfig struct {
	UploadDir string `mapstructure:"upload_dir"`
}

// MySQLConfig 链下索引库；连接失败时 indexer 自动降级关闭，主服务仍可启动。
type MySQLConfig struct {
	Enabled  bool   `mapstructure:"enabled"`
	AdminDSN string `mapstructure:"admin_dsn"`
	DSN      string `mapstructure:"dsn"`
	MaxOpen  int    `mapstructure:"max_open"`
	MaxIdle  int    `mapstructure:"max_idle"`
}

// RedisConfig 缓存 / 去重 / 分布式锁；不可用时自动降级为进程内内存实现。
type RedisConfig struct {
	Enabled  bool   `mapstructure:"enabled"`
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

// IndexerConfig 控制链上→链下同步：回填、确认深度、并发与 outbox 重试。
type IndexerConfig struct {
	Enabled            bool             `mapstructure:"enabled"`
	ConfirmDepth       int              `mapstructure:"confirm_depth"`
	BatchSize          int              `mapstructure:"batch_size"`
	OutboxWorkers      int              `mapstructure:"outbox_workers"`
	MaxRetries         int              `mapstructure:"max_retries"`
	GapScanIntervalSec int              `mapstructure:"gap_scan_interval_sec"`
	HashVerifyWindow   int              `mapstructure:"hash_verify_window"`
	WatchContracts     []WatchContract  `mapstructure:"watch_contracts"`
}

// WatchContract 声明要监听的合约地址、ABI 文件与事件名（生产 indexer 配置驱动）。
type WatchContract struct {
	Address string   `mapstructure:"address"`
	ABI     string   `mapstructure:"abi"`
	Events  []string `mapstructure:"events"`
}

// TxTrackerConfig 控制广播后的 tx 状态轮询与确认深度（钱包/发交易路径）。
type TxTrackerConfig struct {
	Enabled             bool `mapstructure:"enabled"`
	PollIntervalSec     int  `mapstructure:"poll_interval_sec"`
	ConfirmDepth        int  `mapstructure:"confirm_depth"`
	BatchSize           int  `mapstructure:"batch_size"`
	MaxPendingHours     int  `mapstructure:"max_pending_hours"`
	UseEIP1559          bool `mapstructure:"use_eip1559"`
	SpeedUpGasBumpPercent int `mapstructure:"speed_up_gas_bump_percent"`
	RequireIdempotencyKey   bool `mapstructure:"require_idempotency_key"`
	ReconcileIntervalSec    int  `mapstructure:"reconcile_interval_sec"`
	ReconcileGraceSec       int  `mapstructure:"reconcile_grace_sec"`
	BroadcastMaxRetries     int  `mapstructure:"broadcast_max_retries"`
	OutboxWorkers           int  `mapstructure:"outbox_workers"`
}

// BalanceSyncConfig 托管/交易所：链上余额快照同步到 account_balances。
type BalanceSyncConfig struct {
	Enabled              bool     `mapstructure:"enabled"`
	CustodialOnly        bool     `mapstructure:"custodial_only"`
	OnTxConfirmed        bool     `mapstructure:"on_tx_confirmed"`
	OnIndexerTx          bool     `mapstructure:"on_indexer_tx"`
	StaleSec             int      `mapstructure:"stale_sec"`
	BackfillIntervalSec  int      `mapstructure:"backfill_interval_sec"`
	RegistryReloadSec    int      `mapstructure:"registry_reload_sec"`
	WatchTokens          []string `mapstructure:"watch_tokens"`
}

// ExchangeConfig 交易所业务层：链下账本、充值确认、提现审核、对账。
type ExchangeConfig struct {
	Enabled               bool         `mapstructure:"enabled"`
	DepositEnabled        bool         `mapstructure:"deposit_enabled"`
	ConfirmDepth          int          `mapstructure:"confirm_depth"`
	AutoApproveWithdraw   bool         `mapstructure:"auto_approve_withdraw"`
	HotWithdrawMaxWei     string       `mapstructure:"hot_withdraw_max_wei"`
	ReconcileIntervalSec  int          `mapstructure:"reconcile_interval_sec"`
	Signer                SignerConfig `mapstructure:"signer"`
}

// SignerConfig 签名后端：dev（本地私钥）或 kms/hsm/mpc（生产占位）。
type SignerConfig struct {
	Type          string `mapstructure:"type"`
	DevPrivateKey string `mapstructure:"dev_private_key"`
}

// WatchTokenAddresses 返回需同步的 ERC-20 合约列表（含 eth.erc20_contract）。
func (c *Config) WatchTokenAddresses() []string {
	seen := make(map[string]struct{})
	var out []string
	add := func(addr string) {
		addr = strings.TrimSpace(addr)
		if addr == "" || !strings.HasPrefix(strings.ToLower(addr), "0x") {
			return
		}
		key := strings.ToLower(addr)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, addr)
	}
	for _, t := range c.BalanceSync.WatchTokens {
		add(t)
	}
	add(c.Eth.ERC20Contract)
	return out
}

// Load 从 path 读取配置文件（支持相对仓库根目录）。
func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetEnvPrefix("CHAIN")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var c Config
	if err := v.Unmarshal(&c); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}

	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) validate() error {
	if c.Server.HTTPAddr == "" {
		c.Server.HTTPAddr = ":8080"
	}
	if c.Server.GRPCAddr == "" {
		c.Server.GRPCAddr = ":9090"
	}
	if c.JWT.Secret == "" || c.JWT.Secret == "please-change-me-use-long-random-string" {
		// 仍允许本地演示；真实部署必须替换。
	}
	if c.Listener.WorkerCount <= 0 {
		c.Listener.WorkerCount = 2
	}
	if c.Listener.ChannelBuffer <= 0 {
		c.Listener.ChannelBuffer = 16
	}
	if c.Listener.ReconnectBaseMS <= 0 {
		c.Listener.ReconnectBaseMS = 500
	}
	if c.Files.UploadDir == "" {
		c.Files.UploadDir = "./data/uploads"
	}
	if c.MySQL.MaxOpen <= 0 {
		c.MySQL.MaxOpen = 25
	}
	if c.MySQL.MaxIdle <= 0 {
		c.MySQL.MaxIdle = 10
	}
	if c.Redis.Addr == "" {
		c.Redis.Addr = "127.0.0.1:6379"
	}
	if c.Indexer.ConfirmDepth <= 0 {
		c.Indexer.ConfirmDepth = 2
	}
	if c.Indexer.BatchSize <= 0 {
		c.Indexer.BatchSize = 20
	}
	if c.Indexer.OutboxWorkers <= 0 {
		c.Indexer.OutboxWorkers = 2
	}
	if c.Indexer.MaxRetries <= 0 {
		c.Indexer.MaxRetries = 5
	}
	if c.Indexer.GapScanIntervalSec <= 0 {
		c.Indexer.GapScanIntervalSec = 15
	}
	if c.Indexer.HashVerifyWindow <= 0 {
		c.Indexer.HashVerifyWindow = 64
	}
	if c.TxTracker.PollIntervalSec <= 0 {
		c.TxTracker.PollIntervalSec = 3
	}
	if c.TxTracker.ConfirmDepth <= 0 {
		c.TxTracker.ConfirmDepth = c.Indexer.ConfirmDepth
		if c.TxTracker.ConfirmDepth <= 0 {
			c.TxTracker.ConfirmDepth = 2
		}
	}
	if c.TxTracker.BatchSize <= 0 {
		c.TxTracker.BatchSize = 50
	}
	if c.TxTracker.MaxPendingHours <= 0 {
		c.TxTracker.MaxPendingHours = 24
	}
	if c.TxTracker.SpeedUpGasBumpPercent <= 0 {
		c.TxTracker.SpeedUpGasBumpPercent = 20
	}
	if c.TxTracker.OutboxWorkers <= 0 {
		c.TxTracker.OutboxWorkers = 1
	}
	if c.TxTracker.ReconcileIntervalSec <= 0 {
		c.TxTracker.ReconcileIntervalSec = 10
	}
	if c.TxTracker.ReconcileGraceSec <= 0 {
		c.TxTracker.ReconcileGraceSec = 5
	}
	if c.TxTracker.BroadcastMaxRetries <= 0 {
		c.TxTracker.BroadcastMaxRetries = 5
	}
	if c.BalanceSync.StaleSec <= 0 {
		c.BalanceSync.StaleSec = 300
	}
	if c.BalanceSync.BackfillIntervalSec <= 0 {
		c.BalanceSync.BackfillIntervalSec = 300
	}
	if c.BalanceSync.RegistryReloadSec <= 0 {
		c.BalanceSync.RegistryReloadSec = 30
	}
	if c.BalanceSync.Enabled {
		if !c.BalanceSync.OnTxConfirmed && !c.BalanceSync.OnIndexerTx {
			c.BalanceSync.OnTxConfirmed = true
			c.BalanceSync.OnIndexerTx = true
		}
	}
	if c.Exchange.Enabled {
		if c.Exchange.ReconcileIntervalSec <= 0 {
			c.Exchange.ReconcileIntervalSec = 300
		}
		if !c.Exchange.DepositEnabled {
			c.Exchange.DepositEnabled = true
		}
	}
	return nil
}
