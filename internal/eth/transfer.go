package eth

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

// SendResult 广播成功后返回的元数据（供 tx_tracker 持久化）。
type SendResult struct {
	Hash              common.Hash
	From              common.Address
	To                common.Address
	Token             common.Address // ERC-20 合约地址；native 为空
	Value             *big.Int
	Nonce             uint64
	GasLimit          uint64
	GasPrice          *big.Int // Legacy
	MaxFeePerGas      *big.Int // EIP-1559
	MaxPriorityFee    *big.Int // EIP-1559
	TxFormat          string   // legacy | eip1559
	TxType            string   // native | erc20
	Data              []byte
}

// SuggestGasPrice 返回节点建议的 Gas 价格（Legacy 交易用）。
func (b *Backend) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	return b.http.SuggestGasPrice(ctx)
}

// SuggestGasTipCap 返回 EIP-1559 priority fee 建议值。
func (b *Backend) SuggestGasTipCap(ctx context.Context) (*big.Int, error) {
	return b.http.SuggestGasTipCap(ctx)
}

// SendETHTransfer 构造并广播原生币转账（Legacy，兼容旧调用）。
func (b *Backend) SendETHTransfer(ctx context.Context, key *ecdsa.PrivateKey, to common.Address, value *big.Int) (SendResult, error) {
	if value == nil || value.Sign() <= 0 {
		return SendResult{}, fmt.Errorf("value must be positive")
	}
	from := crypto.PubkeyToAddress(key.PublicKey)
	nonce, err := b.PendingNonceAt(ctx, from)
	if err != nil {
		return SendResult{}, fmt.Errorf("nonce: %w", err)
	}
	return b.SendNativeTransfer(ctx, key, to, value, nonce, false)
}

// SendETHTransferWei 接受十进制 wei 字符串（Legacy + 节点 nonce）。
func (b *Backend) SendETHTransferWei(ctx context.Context, key *ecdsa.PrivateKey, to common.Address, valueWei string) (SendResult, error) {
	v := new(big.Int)
	if _, ok := v.SetString(valueWei, 10); !ok || v.Sign() <= 0 {
		return SendResult{}, fmt.Errorf("invalid value_wei")
	}
	return b.SendETHTransfer(ctx, key, to, v)
}

// SignNativeTransfer 构造并签名原生币转账（不广播）。
func (b *Backend) SignNativeTransfer(ctx context.Context, key *ecdsa.PrivateKey, to common.Address, value *big.Int, nonce uint64, useEIP1559 bool) (*types.Transaction, SendResult, error) {
	from := crypto.PubkeyToAddress(key.PublicKey)
	msg := ethereum.CallMsg{From: from, To: &to, Value: value}
	gasLimit, err := b.http.EstimateGas(ctx, msg)
	if err != nil || gasLimit < 21000 {
		gasLimit = 21000
	}

	var signed *types.Transaction
	result := SendResult{
		From: from, To: to, Value: new(big.Int).Set(value),
		Nonce: nonce, GasLimit: gasLimit, TxType: "native",
	}

	if useEIP1559 {
		tip, err := b.SuggestGasTipCap(ctx)
		if err != nil {
			return nil, SendResult{}, fmt.Errorf("gas tip: %w", err)
		}
		header, err := b.http.HeaderByNumber(ctx, nil)
		if err != nil {
			return nil, SendResult{}, fmt.Errorf("header: %w", err)
		}
		baseFee := header.BaseFee
		if baseFee == nil {
			baseFee = big.NewInt(0)
		}
		feeCap := new(big.Int).Add(new(big.Int).Mul(baseFee, big.NewInt(2)), tip)
		tx := types.NewTx(&types.DynamicFeeTx{
			ChainID: b.ChainID(), Nonce: nonce, GasTipCap: tip, GasFeeCap: feeCap,
			Gas: gasLimit, To: &to, Value: value,
		})
		signed, err = types.SignTx(tx, types.LatestSignerForChainID(b.ChainID()), key)
		if err != nil {
			return nil, SendResult{}, fmt.Errorf("sign: %w", err)
		}
		result.TxFormat = "eip1559"
		result.MaxPriorityFee = new(big.Int).Set(tip)
		result.MaxFeePerGas = new(big.Int).Set(feeCap)
	} else {
		gasPrice, err := b.SuggestGasPrice(ctx)
		if err != nil {
			return nil, SendResult{}, fmt.Errorf("gas price: %w", err)
		}
		tx := types.NewTx(&types.LegacyTx{
			Nonce: nonce, GasPrice: gasPrice, Gas: gasLimit, To: &to, Value: value,
		})
		signed, err = types.SignTx(tx, types.LatestSignerForChainID(b.ChainID()), key)
		if err != nil {
			return nil, SendResult{}, fmt.Errorf("sign: %w", err)
		}
		result.TxFormat = "legacy"
		result.GasPrice = new(big.Int).Set(gasPrice)
	}
	result.Hash = signed.Hash()
	return signed, result, nil
}

// SendNativeTransfer 指定 nonce 广播原生币；useEIP1559 为 true 时使用 DynamicFeeTx。
func (b *Backend) SendNativeTransfer(ctx context.Context, key *ecdsa.PrivateKey, to common.Address, value *big.Int, nonce uint64, useEIP1559 bool) (SendResult, error) {
	signed, result, err := b.SignNativeTransfer(ctx, key, to, value, nonce, useEIP1559)
	if err != nil {
		return SendResult{}, err
	}
	if err := b.SendSignedTransaction(ctx, signed); err != nil {
		return SendResult{}, err
	}
	return result, nil
}

// SignERC20Transfer 构造并签名 ERC-20 transfer（不广播）。
func (b *Backend) SignERC20Transfer(ctx context.Context, key *ecdsa.PrivateKey, token, to common.Address, amount *big.Int, nonce uint64, useEIP1559 bool) (*types.Transaction, SendResult, error) {
	if amount == nil || amount.Sign() <= 0 {
		return nil, SendResult{}, fmt.Errorf("amount must be positive")
	}
	from := crypto.PubkeyToAddress(key.PublicKey)
	data, err := packERC20Transfer(to, amount)
	if err != nil {
		return nil, SendResult{}, err
	}
	msg := ethereum.CallMsg{From: from, To: &token, Data: data}
	gasLimit, err := b.http.EstimateGas(ctx, msg)
	if err != nil || gasLimit < 65000 {
		gasLimit = 100000
	}

	var signed *types.Transaction
	result := SendResult{
		From: from, To: to, Token: token, Value: big.NewInt(0),
		Nonce: nonce, GasLimit: gasLimit, TxType: "erc20", Data: data,
	}

	if useEIP1559 {
		tip, err := b.SuggestGasTipCap(ctx)
		if err != nil {
			return nil, SendResult{}, fmt.Errorf("gas tip: %w", err)
		}
		header, err := b.http.HeaderByNumber(ctx, nil)
		if err != nil {
			return nil, SendResult{}, fmt.Errorf("header: %w", err)
		}
		baseFee := header.BaseFee
		if baseFee == nil {
			baseFee = big.NewInt(0)
		}
		feeCap := new(big.Int).Add(new(big.Int).Mul(baseFee, big.NewInt(2)), tip)
		tx := types.NewTx(&types.DynamicFeeTx{
			ChainID: b.ChainID(), Nonce: nonce, GasTipCap: tip, GasFeeCap: feeCap,
			Gas: gasLimit, To: &token, Value: big.NewInt(0), Data: data,
		})
		signed, err = types.SignTx(tx, types.LatestSignerForChainID(b.ChainID()), key)
		if err != nil {
			return nil, SendResult{}, fmt.Errorf("sign: %w", err)
		}
		result.TxFormat = "eip1559"
		result.MaxPriorityFee = new(big.Int).Set(tip)
		result.MaxFeePerGas = new(big.Int).Set(feeCap)
	} else {
		gasPrice, err := b.SuggestGasPrice(ctx)
		if err != nil {
			return nil, SendResult{}, fmt.Errorf("gas price: %w", err)
		}
		tx := types.NewTx(&types.LegacyTx{
			Nonce: nonce, GasPrice: gasPrice, Gas: gasLimit, To: &token, Value: big.NewInt(0), Data: data,
		})
		signed, err = types.SignTx(tx, types.LatestSignerForChainID(b.ChainID()), key)
		if err != nil {
			return nil, SendResult{}, fmt.Errorf("sign: %w", err)
		}
		result.TxFormat = "legacy"
		result.GasPrice = new(big.Int).Set(gasPrice)
	}
	result.Hash = signed.Hash()
	return signed, result, nil
}

// SendERC20Transfer 构造并广播 ERC-20 transfer(to, amount)。
func (b *Backend) SendERC20Transfer(ctx context.Context, key *ecdsa.PrivateKey, token, to common.Address, amount *big.Int, nonce uint64, useEIP1559 bool) (SendResult, error) {
	signed, result, err := b.SignERC20Transfer(ctx, key, token, to, amount, nonce, useEIP1559)
	if err != nil {
		return SendResult{}, err
	}
	if err := b.SendSignedTransaction(ctx, signed); err != nil {
		return SendResult{}, err
	}
	return result, nil
}

const erc20TransferABI = `[{"type":"function","name":"transfer","stateMutability":"nonpayable","inputs":[{"name":"to","type":"address"},{"name":"amount","type":"uint256"}],"outputs":[{"name":"","type":"bool"}]}]`

func packERC20Transfer(to common.Address, amount *big.Int) ([]byte, error) {
	parsed, err := parseABI(erc20TransferABI)
	if err != nil {
		return nil, err
	}
	return parsed.Pack("transfer", to, amount)
}

// SignSpeedUpNative 构造更高 gas 的替换交易（仅签名，不广播）。
func (b *Backend) SignSpeedUpNative(ctx context.Context, key *ecdsa.PrivateKey, to common.Address, value *big.Int, nonce, gasLimit uint64, bumpPercent int, wasEIP1559 bool) (*types.Transaction, SendResult, error) {
	if bumpPercent <= 0 {
		bumpPercent = 20
	}
	mult := big.NewInt(int64(100 + bumpPercent))
	div := big.NewInt(100)

	from := crypto.PubkeyToAddress(key.PublicKey)
	result := SendResult{
		From: from, To: to, Value: value, Nonce: nonce, GasLimit: gasLimit, TxType: "native",
	}

	if wasEIP1559 {
		tip, err := b.SuggestGasTipCap(ctx)
		if err != nil {
			return nil, SendResult{}, err
		}
		tip = new(big.Int).Div(new(big.Int).Mul(tip, mult), div)
		header, err := b.http.HeaderByNumber(ctx, nil)
		if err != nil {
			return nil, SendResult{}, err
		}
		baseFee := header.BaseFee
		if baseFee == nil {
			baseFee = big.NewInt(0)
		}
		feeCap := new(big.Int).Add(new(big.Int).Mul(baseFee, big.NewInt(2)), tip)
		tx := types.NewTx(&types.DynamicFeeTx{
			ChainID: b.ChainID(), Nonce: nonce, GasTipCap: tip, GasFeeCap: feeCap,
			Gas: gasLimit, To: &to, Value: value,
		})
		signed, err := types.SignTx(tx, types.LatestSignerForChainID(b.ChainID()), key)
		if err != nil {
			return nil, SendResult{}, err
		}
		result.TxFormat = "eip1559"
		result.MaxPriorityFee = tip
		result.MaxFeePerGas = feeCap
		result.Hash = signed.Hash()
		return signed, result, nil
	}

	gasPrice, err := b.SuggestGasPrice(ctx)
	if err != nil {
		return nil, SendResult{}, err
	}
	gasPrice = new(big.Int).Div(new(big.Int).Mul(gasPrice, mult), div)
	tx := types.NewTx(&types.LegacyTx{
		Nonce: nonce, GasPrice: gasPrice, Gas: gasLimit, To: &to, Value: value,
	})
	signed, err := types.SignTx(tx, types.LatestSignerForChainID(b.ChainID()), key)
	if err != nil {
		return nil, SendResult{}, err
	}
	result.TxFormat = "legacy"
	result.GasPrice = gasPrice
	result.Hash = signed.Hash()
	return signed, result, nil
}

// BuildSpeedUpNative 基于 pending tx 构造更高 gas 的替换交易并广播。
func (b *Backend) BuildSpeedUpNative(ctx context.Context, key *ecdsa.PrivateKey, to common.Address, value *big.Int, nonce, gasLimit uint64, bumpPercent int, wasEIP1559 bool) (SendResult, error) {
	signed, result, err := b.SignSpeedUpNative(ctx, key, to, value, nonce, gasLimit, bumpPercent, wasEIP1559)
	if err != nil {
		return SendResult{}, err
	}
	if err := b.SendSignedTransaction(ctx, signed); err != nil {
		return SendResult{}, err
	}
	return result, nil
}

// SignSpeedUpERC20 同 nonce 更高 gas 的 ERC-20 替换交易（仅签名）。
func (b *Backend) SignSpeedUpERC20(ctx context.Context, key *ecdsa.PrivateKey, token, to common.Address, amount *big.Int, nonce, gasLimit uint64, bumpPercent int, wasEIP1559 bool) (*types.Transaction, SendResult, error) {
	if bumpPercent <= 0 {
		bumpPercent = 20
	}
	mult := big.NewInt(int64(100 + bumpPercent))
	div := big.NewInt(100)
	from := crypto.PubkeyToAddress(key.PublicKey)
	data, err := packERC20Transfer(to, amount)
	if err != nil {
		return nil, SendResult{}, err
	}
	result := SendResult{
		From: from, To: to, Token: token, Value: big.NewInt(0),
		Nonce: nonce, GasLimit: gasLimit, TxType: "erc20", Data: data,
	}

	if wasEIP1559 {
		tip, err := b.SuggestGasTipCap(ctx)
		if err != nil {
			return nil, SendResult{}, err
		}
		tip = new(big.Int).Div(new(big.Int).Mul(tip, mult), div)
		header, err := b.http.HeaderByNumber(ctx, nil)
		if err != nil {
			return nil, SendResult{}, err
		}
		baseFee := header.BaseFee
		if baseFee == nil {
			baseFee = big.NewInt(0)
		}
		feeCap := new(big.Int).Add(new(big.Int).Mul(baseFee, big.NewInt(2)), tip)
		tx := types.NewTx(&types.DynamicFeeTx{
			ChainID: b.ChainID(), Nonce: nonce, GasTipCap: tip, GasFeeCap: feeCap,
			Gas: gasLimit, To: &token, Value: big.NewInt(0), Data: data,
		})
		signed, err := types.SignTx(tx, types.LatestSignerForChainID(b.ChainID()), key)
		if err != nil {
			return nil, SendResult{}, err
		}
		result.TxFormat = "eip1559"
		result.MaxPriorityFee = tip
		result.MaxFeePerGas = feeCap
		result.Hash = signed.Hash()
		return signed, result, nil
	}

	gasPrice, err := b.SuggestGasPrice(ctx)
	if err != nil {
		return nil, SendResult{}, err
	}
	gasPrice = new(big.Int).Div(new(big.Int).Mul(gasPrice, mult), div)
	tx := types.NewTx(&types.LegacyTx{
		Nonce: nonce, GasPrice: gasPrice, Gas: gasLimit, To: &token, Value: big.NewInt(0), Data: data,
	})
	signed, err := types.SignTx(tx, types.LatestSignerForChainID(b.ChainID()), key)
	if err != nil {
		return nil, SendResult{}, err
	}
	result.TxFormat = "legacy"
	result.GasPrice = gasPrice
	result.Hash = signed.Hash()
	return signed, result, nil
}

// BuildSpeedUpERC20 同 nonce 更高 gas 的 ERC-20 替换交易并广播。
func (b *Backend) BuildSpeedUpERC20(ctx context.Context, key *ecdsa.PrivateKey, token, to common.Address, amount *big.Int, nonce, gasLimit uint64, bumpPercent int, wasEIP1559 bool) (SendResult, error) {
	signed, result, err := b.SignSpeedUpERC20(ctx, key, token, to, amount, nonce, gasLimit, bumpPercent, wasEIP1559)
	if err != nil {
		return SendResult{}, err
	}
	if err := b.SendSignedTransaction(ctx, signed); err != nil {
		return SendResult{}, err
	}
	return result, nil
}

// RecoverSender 从已签名交易恢复 from 地址。
func RecoverSender(tx *types.Transaction, chainID *big.Int) (common.Address, error) {
	signer := types.LatestSignerForChainID(chainID)
	return types.Sender(signer, tx)
}

// SendResultFromSigned 从已签名交易提取跟踪元数据。
func SendResultFromSigned(tx *types.Transaction, chainID *big.Int) (SendResult, error) {
	from, err := RecoverSender(tx, chainID)
	if err != nil {
		return SendResult{}, fmt.Errorf("recover sender: %w", err)
	}
	result := SendResult{
		Hash:  tx.Hash(),
		From:  from,
		Nonce: tx.Nonce(),
		GasLimit: tx.Gas(),
		Data:  tx.Data(),
		Value: tx.Value(),
	}
	if tx.To() != nil {
		result.To = *tx.To()
	}
	switch tx.Type() {
	case types.DynamicFeeTxType:
		result.TxFormat = "eip1559"
		result.MaxFeePerGas = tx.GasFeeCap()
		result.MaxPriorityFee = tx.GasTipCap()
	default:
		result.TxFormat = "legacy"
		result.GasPrice = tx.GasPrice()
	}
	if len(tx.Data()) >= 4 && strings.EqualFold(result.To.Hex(), "") == false {
		// ERC-20 transfer selector 0xa9059cbb
		if len(tx.Data()) >= 68 && tx.Value().Sign() == 0 {
			methodID := tx.Data()[:4]
			if methodID[0] == 0xa9 && methodID[1] == 0x05 && methodID[2] == 0x9c && methodID[3] == 0xbb {
				result.TxType = "erc20"
				result.Token = *tx.To()
				result.To = common.BytesToAddress(tx.Data()[16:36])
				result.Value = new(big.Int).SetBytes(tx.Data()[36:68])
			}
		}
	}
	if result.TxType == "" {
		result.TxType = "native"
	}
	return result, nil
}
