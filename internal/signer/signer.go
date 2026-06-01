package signer

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"

	"gin-demo/internal/config"
)

var ErrKMSNotConfigured = errors.New("KMS signer not configured (production should use HSM/MPC)")

// Signer 生产环境应对接 KMS/HSM/MPC；本地 dev 可用内存私钥。
type Signer interface {
	// PrivateKey 返回指定托管地址的签名密钥（dev 模式）；KMS 实现可返回代理 key 或 error。
	PrivateKey(ctx context.Context, fromAddress string) (*ecdsa.PrivateKey, error)
	BackendName() string
}

func New(cfg *config.Config) Signer {
	switch strings.ToLower(cfg.Exchange.Signer.Type) {
	case "kms", "hsm", "mpc":
		return &KMSStub{}
	default:
		return NewDev(cfg)
	}
}

// DevSigner 本地 Anvil 演示：配置单一热钱包私钥。
type DevSigner struct {
	key     *ecdsa.PrivateKey
	address string
}

func NewDev(cfg *config.Config) Signer {
	hexKey := strings.TrimPrefix(strings.TrimSpace(cfg.Exchange.Signer.DevPrivateKey), "0x")
	if hexKey == "" {
		return &DevSigner{}
	}
	key, err := crypto.HexToECDSA(hexKey)
	if err != nil {
		return &DevSigner{}
	}
	addr := crypto.PubkeyToAddress(key.PublicKey).Hex()
	return &DevSigner{key: key, address: strings.ToLower(addr)}
}

func (d *DevSigner) PrivateKey(_ context.Context, fromAddress string) (*ecdsa.PrivateKey, error) {
	if d.key == nil {
		return nil, errors.New("dev signer: exchange.signer.dev_private_key not configured")
	}
	if d.address != "" && !strings.EqualFold(d.address, fromAddress) {
		return nil, errors.New("dev signer: only configured hot wallet address can sign")
	}
	return d.key, nil
}

func (d *DevSigner) BackendName() string { return "dev-private-key" }

// KMSStub 生产占位：提醒对接真实 KMS/HSM/MPC。
type KMSStub struct{}

func (k *KMSStub) PrivateKey(_ context.Context, _ string) (*ecdsa.PrivateKey, error) {
	return nil, ErrKMSNotConfigured
}

func (k *KMSStub) BackendName() string { return "kms-stub" }
