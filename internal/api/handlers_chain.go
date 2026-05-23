package api

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/gin-gonic/gin"

	"gin-demo/internal/eth"
)

func weiToETHString(w *big.Int) string {
	if w.Sign() == 0 {
		return "0"
	}
	denom := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	s := new(big.Rat).SetFrac(w, denom).FloatString(18)
	if !strings.Contains(s, ".") {
		return s
	}
	s = strings.TrimRight(s, "0")
	return strings.TrimSuffix(s, ".")
}

func queryIncludeTxHashes(c *gin.Context) bool {
	q := strings.TrimSpace(strings.ToLower(c.Query("tx_hashes")))
	return q == "1" || q == "true" || q == "yes"
}

// blockDetailJSON 将完整区块转为 REST 响应（与节点 eth_getBlockByNumber 字段对齐度较高）。
func blockDetailJSON(blk *types.Block, includeTxHashes bool) gin.H {
	out := gin.H{
		"number":             blk.NumberU64(),
		"hash":               blk.Hash().Hex(),
		"parent_hash":        blk.ParentHash().Hex(),
		"timestamp":          blk.Time(),
		"nonce":              fmt.Sprintf("0x%016x", blk.Nonce()),
		"miner":              blk.Coinbase().Hex(),
		"difficulty":         blk.Difficulty().String(),
		"extra_data":         "0x" + common.Bytes2Hex(blk.Extra()),
		"gas_limit":          blk.GasLimit(),
		"gas_used":           blk.GasUsed(),
		"state_root":         blk.Root().Hex(),
		"transactions_root":  blk.TxHash().Hex(),
		"receipts_root":      blk.ReceiptHash().Hex(),
		"uncles_hash":        blk.UncleHash().Hex(),
		"mix_hash":           blk.MixDigest().Hex(),
		"logs_bloom":         "0x" + common.Bytes2Hex(blk.Bloom().Bytes()),
		"tx_count":           len(blk.Transactions()),
	}
	if bf := blk.BaseFee(); bf != nil {
		out["base_fee_per_gas"] = bf.String()
	}
	if includeTxHashes {
		txs := blk.Transactions()
		hashes := make([]string, len(txs))
		for i, tx := range txs {
			hashes[i] = tx.Hash().Hex()
		}
		out["transaction_hashes"] = hashes
	}
	return out
}

func HandleLatestBlock(b *eth.Backend) gin.HandlerFunc {
	return func(c *gin.Context) {
		blk, err := b.LatestBlock(c.Request.Context())
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, blockDetailJSON(blk, queryIncludeTxHashes(c)))
	}
}

func HandleBlockByNumber(b *eth.Backend) gin.HandlerFunc {
	return func(c *gin.Context) {
		n, err := strconv.ParseUint(c.Param("number"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid block number"})
			return
		}
		blk, err := b.BlockByNumber(c.Request.Context(), n)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, blockDetailJSON(blk, queryIncludeTxHashes(c)))
	}
}

func HandleBlockByHash(b *eth.Backend) gin.HandlerFunc {
	return func(c *gin.Context) {
		raw := strings.TrimSpace(c.Param("hash"))
		if !strings.HasPrefix(raw, "0x") {
			raw = "0x" + raw
		}
		if !common.IsHexHash(raw) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid block hash"})
			return
		}
		blk, err := b.BlockByHash(c.Request.Context(), common.HexToHash(raw))
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		if blk == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "block not found"})
			return
		}
		c.JSON(http.StatusOK, blockDetailJSON(blk, queryIncludeTxHashes(c)))
	}
}

func HandleTxReceipt(b *eth.Backend) gin.HandlerFunc {
	return func(c *gin.Context) {
		hash := c.Param("hash")
		rc, err := b.TransactionReceipt(c.Request.Context(), hash)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"tx_hash":   rc.TxHash.Hex(),
			"status":    rc.Status,
			"gas_used":  rc.GasUsed,
			"block":     rc.BlockNumber.Uint64(),
			"logs_count": len(rc.Logs),
		})
	}
}

func transactionDetailJSON(ctx context.Context, b *eth.Backend, tx *types.Transaction, isPending bool) (gin.H, error) {
	signer := types.LatestSignerForChainID(b.ChainID())
	from, err := types.Sender(signer, tx)
	if err != nil {
		return nil, err
	}
	out := gin.H{
		"hash":       tx.Hash().Hex(),
		"from":       from.Hex(),
		"nonce":      tx.Nonce(),
		"value_wei":  tx.Value().String(),
		"value_eth":  weiToETHString(tx.Value()),
		"gas":        tx.Gas(),
		"input":      "0x" + common.Bytes2Hex(tx.Data()),
		"type":       tx.Type(),
		"pending":    isPending,
		"chain_id":   b.ChainID().String(),
	}
	if t := tx.To(); t != nil {
		out["to"] = t.Hex()
	} else {
		out["to"] = nil
		out["contract_creation"] = true
	}
	switch tx.Type() {
	case types.LegacyTxType, types.AccessListTxType:
		if tx.GasPrice() != nil {
			out["gas_price"] = tx.GasPrice().String()
		}
	case types.DynamicFeeTxType, types.BlobTxType:
		if tx.GasFeeCap() != nil {
			out["max_fee_per_gas"] = tx.GasFeeCap().String()
		}
		if tx.GasTipCap() != nil {
			out["max_priority_fee_per_gas"] = tx.GasTipCap().String()
		}
	}
	if !isPending {
		rc, err := b.TransactionReceipt(ctx, tx.Hash().Hex())
		if err == nil && rc != nil {
			out["block_number"] = rc.BlockNumber.Uint64()
			out["block_hash"] = rc.BlockHash.Hex()
			out["tx_index"] = rc.TransactionIndex
			out["status"] = rc.Status
			out["gas_used"] = rc.GasUsed
			if rc.ContractAddress != (common.Address{}) {
				out["contract_address"] = rc.ContractAddress.Hex()
			}
		}
	}
	return out, nil
}

// HandleTransaction 按哈希查询完整交易信息（含已确认时的区块位置与回执摘要字段）。
func HandleTransaction(b *eth.Backend) gin.HandlerFunc {
	return func(c *gin.Context) {
		raw := strings.TrimSpace(c.Param("hash"))
		if !strings.HasPrefix(raw, "0x") {
			raw = "0x" + raw
		}
		if !common.IsHexHash(raw) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid tx hash"})
			return
		}
		tx, pending, err := b.TransactionByHash(c.Request.Context(), common.HexToHash(raw))
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		if tx == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "transaction not found"})
			return
		}
		out, err := transactionDetailJSON(c.Request.Context(), b, tx, pending)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, out)
	}
}

// HandleAccountInfo 查询账户概要：余额、nonce、是否为合约。
func HandleAccountInfo(b *eth.Backend) gin.HandlerFunc {
	return func(c *gin.Context) {
		addr := strings.TrimSpace(c.Param("addr"))
		if !common.IsHexAddress(addr) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid hex address"})
			return
		}
		a := common.HexToAddress(addr)
		ctx := c.Request.Context()

		bal, err := b.BalanceAt(ctx, addr)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		nonce, err := b.NonceAt(ctx, a)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		code, err := b.CodeAt(ctx, a)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"address":      addr,
			"balance_wei":  bal.String(),
			"balance_eth":  weiToETHString(bal),
			"nonce":        nonce,
			"is_contract": len(code) > 0,
			"code_size":    len(code),
			"chain_id":     b.ChainID().String(),
		})
	}
}

// HandleAccountTransactions 在区块区间内扫描与地址相关的交易（适合本地链；主网大区间请勿调用）。
func HandleAccountTransactions(b *eth.Backend) gin.HandlerFunc {
	return func(c *gin.Context) {
		addr := strings.TrimSpace(c.Param("addr"))
		if !common.IsHexAddress(addr) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid hex address"})
			return
		}
		a := common.HexToAddress(addr)
		ctx := c.Request.Context()

		fromStr := strings.TrimSpace(c.DefaultQuery("from_block", "0"))
		fromBlock, err := strconv.ParseUint(fromStr, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid from_block"})
			return
		}

		var toBlock uint64
		if raw := strings.TrimSpace(c.Query("to_block")); raw == "" || strings.EqualFold(raw, "latest") {
			toBlock, err = b.BlockNumber(ctx)
			if err != nil {
				c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
				return
			}
		} else {
			toBlock, err = strconv.ParseUint(raw, 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid to_block"})
				return
			}
		}

		if toBlock < fromBlock {
			c.JSON(http.StatusBadRequest, gin.H{"error": "to_block must be >= from_block"})
			return
		}
		if toBlock-fromBlock+1 > eth.MaxAccountTxBlockSpan {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":      "block range too large",
				"max_blocks": eth.MaxAccountTxBlockSpan,
			})
			return
		}

		limit := eth.MaxAccountTxResults
		if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n <= 0 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid limit"})
				return
			}
			if n > eth.MaxAccountTxResults {
				n = eth.MaxAccountTxResults
			}
			limit = n
		}

		refs, err := b.TransactionsForAddress(ctx, a, fromBlock, toBlock, limit)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}

		txs := make([]gin.H, 0, len(refs))
		for _, r := range refs {
			row := gin.H{
				"tx_hash":      r.TxHash.Hex(),
				"block_number": r.BlockNumber,
				"block_hash":   r.BlockHash.Hex(),
				"tx_index":     r.TxIndex,
				"from":         r.From.Hex(),
				"value_wei":    r.Value.String(),
			}
			if r.To != nil {
				row["to"] = r.To.Hex()
			}
			txs = append(txs, row)
		}

		c.JSON(http.StatusOK, gin.H{
			"address":      addr,
			"from_block":   fromBlock,
			"to_block":     toBlock,
			"limit":        limit,
			"returned":     len(txs),
			"transactions": txs,
		})
	}
}

func HandleBalance(b *eth.Backend) gin.HandlerFunc {
	return func(c *gin.Context) {
		addr := c.Param("addr")
		if !common.IsHexAddress(addr) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid hex address"})
			return
		}
		w, err := b.BalanceAt(c.Request.Context(), addr)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"address": addr, "eth": weiToETHString(w)})
	}
}
