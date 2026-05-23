package eth

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// 账户交易扫描上限（防止主网全链扫描拖垮服务；本地调试可调大需改代码）。
const (
	MaxAccountTxBlockSpan = 2000
	MaxAccountTxResults   = 500
)

// AccountTxRef 描述一笔与某地址相关（from / to）的交易摘要。
type AccountTxRef struct {
	TxHash      common.Hash
	BlockNumber uint64
	BlockHash   common.Hash
	TxIndex     uint
	From        common.Address
	To          *common.Address
	Value       *big.Int
}

// BlockNumber 返回当前规范链链尖高度（用于默认 to_block）。
func (b *Backend) BlockNumber(ctx context.Context) (uint64, error) {
	return b.http.BlockNumber(ctx)
}

// NonceAt 返回该地址在最新已确认状态下的交易序号（下一次发送应使用的 nonce 可参考 PendingNonceAt）。
func (b *Backend) NonceAt(ctx context.Context, addr common.Address) (uint64, error) {
	return b.http.NonceAt(ctx, addr, nil)
}

// CodeAt 返回合约字节码；EOA 为空切片。
func (b *Backend) CodeAt(ctx context.Context, addr common.Address) ([]byte, error) {
	return b.http.CodeAt(ctx, addr, nil)
}

// TransactionsForAddress 在 [fromBlock,toBlock] 内扫描区块体，收集 from/to 涉及 addr 的交易；
// 若 addr 为某次合约创建的 contractAddress，也会匹配对应的部署交易（to 为空的交易）。
// limit 为 0 或与 MaxAccountTxResults 取较小值作为返回条数上限。
func (b *Backend) TransactionsForAddress(ctx context.Context, addr common.Address, fromBlock, toBlock uint64, limit int) ([]AccountTxRef, error) {
	if toBlock < fromBlock {
		return nil, fmt.Errorf("to_block < from_block")
	}
	span := toBlock - fromBlock + 1
	if span > MaxAccountTxBlockSpan {
		return nil, fmt.Errorf("block span %d exceeds max %d", span, MaxAccountTxBlockSpan)
	}
	if limit <= 0 || limit > MaxAccountTxResults {
		limit = MaxAccountTxResults
	}

	signer := types.LatestSignerForChainID(b.ChainID())

	out := make([]AccountTxRef, 0, 32)
	for n := fromBlock; n <= toBlock; n++ {
		blk, err := b.BlockByNumber(ctx, n)
		if err != nil {
			return nil, err
		}
		if blk == nil {
			continue
		}
		bh := blk.Hash()
		txs := blk.Transactions()
		for i, tx := range txs {
			from, err := types.Sender(signer, tx)
			if err != nil {
				continue
			}
			match := from == addr
			if t := tx.To(); t != nil && *t == addr {
				match = true
			}
			if !match && tx.To() == nil {
				rc, err := b.TransactionReceipt(ctx, tx.Hash().Hex())
				if err == nil && rc != nil && rc.ContractAddress != (common.Address{}) && rc.ContractAddress == addr {
					match = true
				}
			}
			if !match {
				continue
			}
			var toPtr *common.Address
			if t := tx.To(); t != nil {
				tCopy := *t
				toPtr = &tCopy
			}
			out = append(out, AccountTxRef{
				TxHash:      tx.Hash(),
				BlockNumber: n,
				BlockHash:   bh,
				TxIndex:     uint(i),
				From:        from,
				To:          toPtr,
				Value:       new(big.Int).Set(tx.Value()),
			})
			if len(out) >= limit {
				return out, nil
			}
		}
	}
	return out, nil
}
