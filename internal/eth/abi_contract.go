package eth

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

// erc20ReadABI 最小 ERC-20 只读片段（balanceOf / decimals / symbol）。
const erc20ReadABI = `[
  {"type":"function","name":"balanceOf","stateMutability":"view","inputs":[{"name":"account","type":"address"}],"outputs":[{"name":"","type":"uint256"}]},
  {"type":"function","name":"decimals","stateMutability":"view","inputs":[],"outputs":[{"name":"","type":"uint8"}]},
  {"type":"function","name":"symbol","stateMutability":"view","inputs":[],"outputs":[{"name":"","type":"string"}]}
]`

// counterABI 与 contracts/Counter.sol 中 public number 及函数对应。
const counterABI = `[
  {"type":"function","name":"number","stateMutability":"view","inputs":[],"outputs":[{"name":"","type":"uint256"}]},
  {"type":"function","name":"setNumber","stateMutability":"nonpayable","inputs":[{"name":"newNumber","type":"uint256"}],"outputs":[]},
  {"type":"function","name":"increment","stateMutability":"nonpayable","inputs":[],"outputs":[]}
]`

func parseABI(json string) (abi.ABI, error) {
	return abi.JSON(strings.NewReader(json))
}

// ERC20BalanceOf 使用 eth_call + ABI 编码 balanceOf(address)，返回 Wei 最小单位的余额。
func (b *Backend) ERC20BalanceOf(ctx context.Context, token, holder common.Address) (*big.Int, error) {
	parsed, err := parseABI(erc20ReadABI)
	if err != nil {
		return nil, err
	}
	data, err := parsed.Pack("balanceOf", holder)
	if err != nil {
		return nil, err
	}
	out, err := b.http.CallContract(ctx, ethereum.CallMsg{To: &token, Data: data}, nil)
	if err != nil {
		return nil, err
	}
	vals, err := parsed.Unpack("balanceOf", out)
	if err != nil || len(vals) == 0 {
		return nil, fmt.Errorf("unpack balanceOf: %w", err)
	}
	bal, ok := vals[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("balanceOf: unexpected type %T", vals[0])
	}
	return bal, nil
}

// ERC20DecimalsSymbol 读取代币 decimals 与 symbol（非标准合约可能失败）。
func (b *Backend) ERC20DecimalsSymbol(ctx context.Context, token common.Address) (decimals uint8, symbol string, err error) {
	parsed, err := parseABI(erc20ReadABI)
	if err != nil {
		return 0, "", err
	}

	dData, err := parsed.Pack("decimals")
	if err != nil {
		return 0, "", err
	}
	dOut, err := b.http.CallContract(ctx, ethereum.CallMsg{To: &token, Data: dData}, nil)
	if err != nil {
		return 0, "", err
	}
	dVals, err := parsed.Unpack("decimals", dOut)
	if err != nil || len(dVals) == 0 {
		return 0, "", fmt.Errorf("unpack decimals: %w", err)
	}
	switch v := dVals[0].(type) {
	case uint8:
		decimals = v
	default:
		return 0, "", fmt.Errorf("decimals: unexpected type %T", dVals[0])
	}

	sData, err := parsed.Pack("symbol")
	if err != nil {
		return 0, "", err
	}
	sOut, err := b.http.CallContract(ctx, ethereum.CallMsg{To: &token, Data: sData}, nil)
	if err != nil {
		return 0, "", err
	}
	sVals, err := parsed.Unpack("symbol", sOut)
	if err != nil || len(sVals) == 0 {
		return 0, "", fmt.Errorf("unpack symbol: %w", err)
	}
	sym, ok := sVals[0].(string)
	if !ok {
		return 0, "", fmt.Errorf("symbol: unexpected type %T", sVals[0])
	}
	return decimals, sym, nil
}

// CounterNumber 调用 Counter.number()（view），用于演示自定义合约 ABI。
func (b *Backend) CounterNumber(ctx context.Context, contract common.Address) (*big.Int, error) {
	parsed, err := parseABI(counterABI)
	if err != nil {
		return nil, err
	}
	data, err := parsed.Pack("number")
	if err != nil {
		return nil, err
	}
	out, err := b.http.CallContract(ctx, ethereum.CallMsg{To: &contract, Data: data}, nil)
	if err != nil {
		return nil, err
	}
	vals, err := parsed.Unpack("number", out)
	if err != nil || len(vals) == 0 {
		return nil, fmt.Errorf("unpack number: %w", err)
	}
	n, ok := vals[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("number: unexpected type %T", vals[0])
	}
	return n, nil
}
