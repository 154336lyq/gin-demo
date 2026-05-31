package tx

import (
	"context"
	"crypto/ecdsa"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"gin-demo/internal/config"
	"gin-demo/internal/eth"
)

var (
	ErrIdempotencyConflict  = errors.New("idempotency key already used by another transaction")
	ErrIdempotencyRequired  = errors.New("idempotency key required")
)

// Service 封装发交易、raw tx 提交、ERC-20 与加速替换。
type Service struct {
	cfg    *config.Config
	eth    *eth.Backend
	store  *Store
	nonces *NonceAllocator
}

func NewService(cfg *config.Config, b *eth.Backend, store *Store) *Service {
	return &Service{
		cfg:    cfg,
		eth:    b,
		store:  store,
		nonces: NewNonceAllocator(store.DB(), cfg.Eth.ChainID, b),
	}
}

// SubmitRaw 生产主路径：先落库 submitting，再广播，避免「链上成功、DB 丢失」。
func (s *Service) SubmitRaw(ctx context.Context, req SubmitRequest) (Row, error) {
	if req.IdempotencyKey != "" {
		if row, err := s.store.GetByIdempotencyKey(ctx, req.IdempotencyKey); err == nil {
			return s.resumeBroadcast(ctx, row, req.SignedRawTx)
		} else if err != sql.ErrNoRows {
			return Row{}, err
		}
	}

	tx, err := s.eth.DecodeRawTxHex(req.SignedRawTx)
	if err != nil {
		return Row{}, err
	}
	if err := s.eth.ValidateSignedTxChainID(tx); err != nil {
		return Row{}, err
	}
	result, err := eth.SendResultFromSigned(tx, s.eth.ChainID())
	if err != nil {
		return Row{}, err
	}
	p := insertFromSendResult(result, SendMeta{
		BizID: req.BizID, BizType: req.BizType,
		IdempotencyKey: req.IdempotencyKey, ReplacesHash: req.ReplacesHash,
	})
	return s.persistAndBroadcast(ctx, p, tx, req.SignedRawTx, false)
}

// SendNative 托管发原生币（dev/local）：DB nonce + 先落库再广播。
func (s *Service) SendNative(ctx context.Context, key *ecdsa.PrivateKey, to common.Address, valueWei string, meta SendMeta) (Row, error) {
	if row, ok, err := s.checkIdempotency(ctx, meta.IdempotencyKey); ok || err != nil {
		return row, err
	}

	v := new(big.Int)
	if _, ok := v.SetString(valueWei, 10); !ok || v.Sign() <= 0 {
		return Row{}, fmt.Errorf("invalid value_wei")
	}
	from := crypto.PubkeyToAddress(key.PublicKey)
	nonce, err := s.nonces.Allocate(ctx, from.Hex())
	if err != nil {
		return Row{}, fmt.Errorf("allocate nonce: %w", err)
	}

	signed, result, err := s.eth.SignNativeTransfer(ctx, key, to, v, nonce, s.cfg.TxTracker.UseEIP1559)
	if err != nil {
		return Row{}, err
	}
	p := insertFromSendResult(result, meta)
	row, err := s.persistAndBroadcast(ctx, p, signed, "", true)
	if err != nil {
		_, _ = s.nonces.SyncFromChain(ctx, from.Hex())
		return Row{}, err
	}
	return row, nil
}

// SendERC20 托管发 ERC-20 transfer（dev/local）。
func (s *Service) SendERC20(ctx context.Context, key *ecdsa.PrivateKey, token, to common.Address, amountWei string, meta SendMeta) (Row, error) {
	if row, ok, err := s.checkIdempotency(ctx, meta.IdempotencyKey); ok || err != nil {
		return row, err
	}

	amt := new(big.Int)
	if _, ok := amt.SetString(amountWei, 10); !ok || amt.Sign() <= 0 {
		return Row{}, fmt.Errorf("invalid amount_wei")
	}
	from := crypto.PubkeyToAddress(key.PublicKey)
	nonce, err := s.nonces.Allocate(ctx, from.Hex())
	if err != nil {
		return Row{}, fmt.Errorf("allocate nonce: %w", err)
	}

	signed, result, err := s.eth.SignERC20Transfer(ctx, key, token, to, amt, nonce, s.cfg.TxTracker.UseEIP1559)
	if err != nil {
		return Row{}, err
	}
	p := insertFromSendResult(result, meta)
	row, err := s.persistAndBroadcast(ctx, p, signed, "", true)
	if err != nil {
		_, _ = s.nonces.SyncFromChain(ctx, from.Hex())
		return Row{}, err
	}
	return row, nil
}

// SpeedUp 对 pending tx 做 RBF（托管/dev）；生产推荐 SubmitRaw + replaces_hash。
func (s *Service) SpeedUp(ctx context.Context, key *ecdsa.PrivateKey, pendingHash string) (Row, error) {
	old, err := s.store.GetByHash(ctx, pendingHash)
	if err != nil {
		return Row{}, err
	}
	if old.Status != StatusPending {
		return Row{}, fmt.Errorf("tx status is %s, not pending", old.Status)
	}

	to := common.HexToAddress(old.ToAddr)
	value := new(big.Int)
	value.SetString(old.ValueWei, 10)
	wasEIP1559 := old.TxFormat == TxFormatEIP1559
	bump := s.cfg.TxTracker.SpeedUpGasBumpPercent

	var signed *types.Transaction
	var result eth.SendResult
	switch old.TxType {
	case TxTypeERC20:
		token := common.HexToAddress(old.TokenAddr)
		signed, result, err = s.eth.SignSpeedUpERC20(ctx, key, token, to, value, old.Nonce, old.GasLimit, bump, wasEIP1559)
	default:
		signed, result, err = s.eth.SignSpeedUpNative(ctx, key, to, value, old.Nonce, old.GasLimit, bump, wasEIP1559)
	}
	if err != nil {
		return Row{}, err
	}

	meta := SendMeta{ReplacesHash: old.TxHash, BizID: old.BizID, BizType: old.BizType}
	p := insertFromSendResult(result, meta)
	return s.persistAndBroadcast(ctx, p, signed, "", false)
}

// persistAndBroadcast 先 INSERT submitting，再 SendTransaction；already known 视为成功。
func (s *Service) persistAndBroadcast(ctx context.Context, p InsertParams, signed *types.Transaction, signedRaw string, syncNonceOnFail bool) (Row, error) {
	if signedRaw == "" {
		var err error
		signedRaw, err = eth.SignedTxToHex(signed)
		if err != nil {
			return Row{}, err
		}
	}
	p.SignedRawHex = signedRaw

	if existing, err := s.store.GetByHash(ctx, p.TxHash); err == nil {
		return s.resumeBroadcast(ctx, existing, signedRaw)
	} else if err != sql.ErrNoRows {
		return Row{}, err
	}

	if err := s.store.InsertSubmitting(ctx, p); err != nil {
		if p.IdempotencyKey != "" {
			if row, gerr := s.store.GetByIdempotencyKey(ctx, p.IdempotencyKey); gerr == nil {
				if !strings.EqualFold(row.TxHash, p.TxHash) {
					return Row{}, ErrIdempotencyConflict
				}
				return s.resumeBroadcast(ctx, row, signedRaw)
			}
		}
		return Row{}, fmt.Errorf("track insert: %w", err)
	}

	if err := s.eth.SendSignedTransaction(ctx, signed); err != nil {
		_ = s.store.MarkBroadcastFailed(ctx, p.TxHash, err.Error())
		if syncNonceOnFail {
			_, _ = s.nonces.SyncFromChain(ctx, p.FromAddr)
		}
		return Row{}, err
	}
	if err := s.store.MarkBroadcastPending(ctx, p.TxHash); err != nil {
		return Row{}, err
	}
	return s.store.GetByHash(ctx, p.TxHash)
}

func (s *Service) resumeBroadcast(ctx context.Context, row Row, signedRaw string) (Row, error) {
	switch row.Status {
	case StatusPending, StatusConfirmed, StatusFailed, StatusDropped, StatusReplaced:
		return row, nil
	case StatusSubmitting, StatusBroadcastFailed:
		if signedRaw == "" {
			signedRaw = row.SignedRawHex
		}
		if signedRaw == "" {
			return row, nil
		}
		tx, err := s.eth.DecodeRawTxHex(signedRaw)
		if err != nil {
			return row, err
		}
		if err := s.eth.SendSignedTransaction(ctx, tx); err != nil {
			_ = s.store.MarkBroadcastFailed(ctx, row.TxHash, err.Error())
			return row, err
		}
		if err := s.store.MarkBroadcastPending(ctx, row.TxHash); err != nil {
			return row, err
		}
		return s.store.GetByHash(ctx, row.TxHash)
	default:
		return row, nil
	}
}

func (s *Service) checkIdempotency(ctx context.Context, key string) (Row, bool, error) {
	if key == "" {
		return Row{}, false, nil
	}
	row, err := s.store.GetByIdempotencyKey(ctx, key)
	if err == sql.ErrNoRows {
		return Row{}, false, nil
	}
	if err != nil {
		return Row{}, false, err
	}
	row, err = s.resumeBroadcast(ctx, row, row.SignedRawHex)
	return row, true, err
}

func insertFromSendResult(r eth.SendResult, meta SendMeta) InsertParams {
	p := InsertParams{
		TxHash:         r.Hash.Hex(),
		FromAddr:       r.From.Hex(),
		ToAddr:         r.To.Hex(),
		ValueWei:       r.Value.String(),
		Nonce:          r.Nonce,
		GasLimit:       r.GasLimit,
		TxFormat:       r.TxFormat,
		TxType:         r.TxType,
		BizID:          meta.BizID,
		BizType:        meta.BizType,
		IdempotencyKey: meta.IdempotencyKey,
		ReplacesHash:   meta.ReplacesHash,
	}
	if r.Token != (common.Address{}) {
		p.TokenAddr = r.Token.Hex()
	}
	if r.GasPrice != nil {
		p.GasPriceWei = r.GasPrice.String()
	}
	if r.MaxFeePerGas != nil {
		p.MaxFeePerGasWei = r.MaxFeePerGas.String()
	}
	if r.MaxPriorityFee != nil {
		p.MaxPriorityFeeWei = r.MaxPriorityFee.String()
	}
	if p.TxFormat == "" {
		p.TxFormat = TxFormatLegacy
	}
	if p.TxType == "" {
		p.TxType = TxTypeNative
	}
	return p
}
