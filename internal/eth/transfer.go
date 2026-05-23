package eth

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

// SuggestGasPrice 返回节点建议的 Gas 价格（Legacy 交易用）。
func (b *Backend) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	return b.http.SuggestGasPrice(ctx)
}

// SendETHTransfer 构造并广播一笔原生币 Legacy 转账（from 由私钥推导；适用于本地 Anvil 等）。
func (b *Backend) SendETHTransfer(ctx context.Context, key *ecdsa.PrivateKey, to common.Address, value *big.Int) (common.Hash, error) {
	if value == nil || value.Sign() <= 0 {
		return common.Hash{}, fmt.Errorf("value must be positive")
	}
	fromAddr := crypto.PubkeyToAddress(key.PublicKey)

	nonce, err := b.PendingNonceAt(ctx, fromAddr)
	if err != nil {
		return common.Hash{}, fmt.Errorf("nonce: %w", err)
	}
	gasPrice, err := b.SuggestGasPrice(ctx)
	if err != nil {
		return common.Hash{}, fmt.Errorf("gas price: %w", err)
	}

	msg := ethereum.CallMsg{From: fromAddr, To: &to, Value: value}
	gasLimit, err := b.http.EstimateGas(ctx, msg)
	if err != nil {
		gasLimit = 21000
	}
	if gasLimit < 21000 {
		gasLimit = 21000
	}

	tx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		GasPrice: gasPrice,
		Gas:      gasLimit,
		To:       &to,
		Value:    value,
		Data:     nil,
	})
	signed, err := types.SignTx(tx, types.LatestSignerForChainID(b.ChainID()), key)
	if err != nil {
		return common.Hash{}, fmt.Errorf("sign: %w", err)
	}
	if err := b.http.SendTransaction(ctx, signed); err != nil {
		return common.Hash{}, err
	}
	return signed.Hash(), nil
}
