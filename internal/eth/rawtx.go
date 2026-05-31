package eth

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/core/types"
)

// DecodeRawTxHex 仅解码已签名交易（不广播）；hash 在广播前即可确定。
func (b *Backend) DecodeRawTxHex(rawHex string) (*types.Transaction, error) {
	rawHex = normalizeHex(rawHex)
	data, err := hex.DecodeString(rawHex)
	if err != nil {
		return nil, fmt.Errorf("invalid raw tx hex: %w", err)
	}
	tx := new(types.Transaction)
	if err := tx.UnmarshalBinary(data); err != nil {
		return nil, fmt.Errorf("decode tx: %w", err)
	}
	return tx, nil
}

// BroadcastRawTxHex 解码并广播已签名 raw transaction（生产主路径）。
func (b *Backend) BroadcastRawTxHex(ctx context.Context, rawHex string) (*types.Transaction, error) {
	tx, err := b.DecodeRawTxHex(rawHex)
	if err != nil {
		return nil, err
	}
	if err := b.SendSignedTransaction(ctx, tx); err != nil {
		return nil, err
	}
	return tx, nil
}

// SendSignedTransaction 广播已签名交易；already known 视为成功。
func (b *Backend) SendSignedTransaction(ctx context.Context, tx *types.Transaction) error {
	if err := b.http.SendTransaction(ctx, tx); err != nil {
		if IsAlreadyKnown(err) {
			return nil
		}
		return err
	}
	return nil
}

// SignedTxToHex RLP 编码为 hex（用于落库重试广播）。
func SignedTxToHex(tx *types.Transaction) (string, error) {
	raw, err := tx.MarshalBinary()
	if err != nil {
		return "", err
	}
	return "0x" + hex.EncodeToString(raw), nil
}

// ValidateSignedTxChainID 校验签名交易所属链。
func (b *Backend) ValidateSignedTxChainID(tx *types.Transaction) error {
	signer := types.LatestSignerForChainID(b.ChainID())
	if _, err := types.Sender(signer, tx); err != nil {
		return fmt.Errorf("invalid signature for chain %d: %w", b.ChainID(), err)
	}
	return nil
}

func normalizeHex(rawHex string) string {
	rawHex = strings.TrimSpace(rawHex)
	rawHex = strings.TrimPrefix(rawHex, "0x")
	rawHex = strings.TrimPrefix(rawHex, "0X")
	return rawHex
}
