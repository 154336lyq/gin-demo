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
	return nil
}
