package exchange

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"gin-demo/internal/balance"
	"gin-demo/internal/config"
	"gin-demo/internal/signer"
	"gin-demo/internal/tx"
)

// Service 交易所业务门面：账本、充值、提现、对账。
type Service struct {
	cfg      *config.Config
	store    *Store
	balStore *balance.Store
	signer   signer.Signer
	txSvc    *tx.Service
}

func NewService(cfg *config.Config, store *Store, balStore *balance.Store, sig signer.Signer, txSvc *tx.Service) *Service {
	return &Service{cfg: cfg, store: store, balStore: balStore, signer: sig, txSvc: txSvc}
}

func (s *Service) Store() *Store { return s.store }

func (s *Service) GetUserBalances(ctx context.Context, userID string) ([]AccountBalance, error) {
	return s.store.ListBalances(ctx, userID)
}

func (s *Service) GetUserEntries(ctx context.Context, userID string, limit int) ([]LedgerEntry, error) {
	return s.store.ListEntries(ctx, userID, limit)
}

func (s *Service) ListDeposits(ctx context.Context, userID string, limit int) ([]Deposit, error) {
	return s.store.ListDeposits(ctx, userID, limit)
}

// --- 提现 ---

func (s *Service) CreateWithdraw(ctx context.Context, p CreateWithdrawParams) (WithdrawRequest, error) {
	amt, err := parseWei(p.AmountWei)
	if err != nil || amt.Sign() <= 0 {
		return WithdrawRequest{}, ErrInvalidWei
	}
	if !common.IsHexAddress(p.ToAddress) {
		return WithdrawRequest{}, fmt.Errorf("invalid to address")
	}

	status := WithdrawStatusPendingReview
	if s.cfg.Exchange.AutoApproveWithdraw {
		status = WithdrawStatusApproved
	}

	dbTx, err := s.store.BeginTx(ctx)
	if err != nil {
		return WithdrawRequest{}, err
	}
	defer dbTx.Rollback()

	w, created, err := s.insertWithdrawTx(ctx, dbTx, p, status)
	if err != nil {
		return WithdrawRequest{}, err
	}

	if !created {
		hasFreeze, err := s.store.hasLedgerEntryTx(ctx, dbTx, w.ID, LedgerWithdrawFreeze)
		if err != nil {
			return WithdrawRequest{}, err
		}
		if hasFreeze {
			if err := dbTx.Commit(); err != nil {
				return WithdrawRequest{}, err
			}
			return s.store.GetWithdraw(ctx, w.ID)
		}
	}

	// 在同事务内 freeze（FOR UPDATE 防并发超提）
	if err := s.store.freezeWithdrawTx(ctx, dbTx, p.UserID, p.TokenAddress, p.AmountWei, w.ID); err != nil {
		return WithdrawRequest{}, err
	}
	if err := dbTx.Commit(); err != nil {
		return WithdrawRequest{}, err
	}
	return s.store.GetWithdraw(ctx, w.ID)
}

func (s *Service) insertWithdrawTx(ctx context.Context, dbTx *sql.Tx, p CreateWithdrawParams, status string) (WithdrawRequest, bool, error) {
	now := time.Now().UTC()
	var idem any
	if p.IdempotencyKey != "" {
		idem = p.IdempotencyKey
	}
	res, err := dbTx.ExecContext(ctx, `
		INSERT INTO withdraw_requests (
			chain_id, user_id, token_address, to_address, amount_wei, status,
			idempotency_key, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, s.store.chainID, p.UserID, normToken(p.TokenAddress), strings.ToLower(p.ToAddress),
		p.AmountWei, status, idem, now, now)
	if err != nil {
		if isDup(err) && p.IdempotencyKey != "" {
			w, gerr := s.store.GetWithdrawByIdempotencyTx(ctx, dbTx, p.IdempotencyKey)
			return w, false, gerr
		}
		return WithdrawRequest{}, false, err
	}
	id, _ := res.LastInsertId()
	w, err := s.store.GetWithdraw(ctx, id)
	return w, true, err
}

func (s *Service) ApproveWithdraw(ctx context.Context, id int64, reviewer string) (WithdrawRequest, error) {
	now := time.Now().UTC()
	ok, err := s.store.UpdateWithdrawStatusIf(ctx, id, WithdrawStatusPendingReview, WithdrawStatusApproved, map[string]any{
		"reviewer": reviewer, "reviewed_at": now,
	})
	if err != nil {
		return WithdrawRequest{}, err
	}
	if !ok {
		return WithdrawRequest{}, ErrInvalidWithdrawState
	}
	return s.store.GetWithdraw(ctx, id)
}

func (s *Service) RejectWithdraw(ctx context.Context, id int64, reviewer, reason string) (WithdrawRequest, error) {
	w, err := s.store.GetWithdraw(ctx, id)
	if err != nil {
		return WithdrawRequest{}, err
	}
	if w.Status != WithdrawStatusPendingReview {
		return WithdrawRequest{}, ErrInvalidWithdrawState
	}

	dbTx, err := s.store.BeginTx(ctx)
	if err != nil {
		return WithdrawRequest{}, err
	}
	defer dbTx.Rollback()

	now := time.Now().UTC()
	res, err := dbTx.ExecContext(ctx, `
		UPDATE withdraw_requests SET status = ?, reviewer = ?, reviewed_at = ?, reject_reason = ?, updated_at = ?
		WHERE chain_id = ? AND id = ? AND status = ?
	`, WithdrawStatusRejected, reviewer, now, reason, now, s.store.chainID, id, WithdrawStatusPendingReview)
	if err != nil {
		return WithdrawRequest{}, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return WithdrawRequest{}, ErrInvalidWithdrawState
	}
	if err := s.store.unfreezeWithdrawTx(ctx, dbTx, w.UserID, w.TokenAddress, w.AmountWei, w.ID); err != nil {
		return WithdrawRequest{}, err
	}
	if err := dbTx.Commit(); err != nil {
		return WithdrawRequest{}, err
	}
	return s.store.GetWithdraw(ctx, id)
}

func (s *Service) ListWithdraws(ctx context.Context, userID, status string, limit int) ([]WithdrawRequest, error) {
	return s.store.ListWithdraws(ctx, userID, status, limit)
}

// ProcessApprovedWithdraw 将已审核提现广播上链（热/冷钱包路由）。
func (s *Service) ProcessApprovedWithdraw(ctx context.Context, w WithdrawRequest) error {
	if w.Status != WithdrawStatusApproved {
		return ErrInvalidWithdrawState
	}
	if s.signer == nil || s.txSvc == nil {
		return fmt.Errorf("signer or tx service not configured")
	}

	// 抢占：approved → broadcasting，防多 worker 重复广播
	ok, err := s.store.UpdateWithdrawStatusIf(ctx, w.ID, WithdrawStatusApproved, WithdrawStatusBroadcasting, nil)
	if err != nil {
		return err
	}
	if !ok {
		return ErrWithdrawAlreadyHandled
	}

	w, err = s.store.GetWithdraw(ctx, w.ID)
	if err != nil {
		return err
	}

	fromWallet, err := s.selectFromWallet(ctx, w)
	if err != nil {
		return s.failBroadcast(ctx, w, err.Error())
	}

	meta := tx.SendMeta{
		BizID:          fmt.Sprintf("withdraw:%d", w.ID),
		BizType:        tx.BizTypeWithdraw,
		IdempotencyKey: fmt.Sprintf("withdraw-broadcast:%d", w.ID),
	}

	var txRow tx.Row
	key, err := s.signer.PrivateKey(ctx, fromWallet)
	if err != nil {
		return s.failBroadcast(ctx, w, err.Error())
	}
	if w.TokenAddress == "" {
		txRow, err = s.txSvc.SendNative(ctx, key, common.HexToAddress(w.ToAddress), w.AmountWei, meta)
	} else {
		txRow, err = s.txSvc.SendERC20(ctx, key,
			common.HexToAddress(w.TokenAddress), common.HexToAddress(w.ToAddress), w.AmountWei, meta)
	}
	if err != nil {
		return s.failBroadcast(ctx, w, err.Error())
	}

	return s.store.UpdateWithdrawStatus(ctx, w.ID, WithdrawStatusBroadcasting, map[string]any{
		"from_wallet": strings.ToLower(fromWallet),
		"tx_hash":     strings.ToLower(txRow.TxHash),
	})
}

func (s *Service) failBroadcast(ctx context.Context, w WithdrawRequest, reason string) error {
	dbTx, err := s.store.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer dbTx.Rollback()
	if err := s.store.FailWithdrawAndUnfreezeTx(ctx, dbTx, w, reason); err != nil {
		return err
	}
	return dbTx.Commit()
}

func (s *Service) selectFromWallet(ctx context.Context, w WithdrawRequest) (string, error) {
	amt, err := parseWei(w.AmountWei)
	if err != nil {
		return "", err
	}
	threshold, _ := parseWei(s.cfg.Exchange.HotWithdrawMaxWei)
	useHot := threshold.Sign() == 0 || weiCmp(amt, threshold) <= 0

	wallets, err := s.balStore.ListWallets(ctx, "", "", true, 500)
	if err != nil {
		return "", err
	}
	var hot, cold string
	for _, cw := range wallets {
		if cw.WalletType == balance.WalletTypeHot && hot == "" {
			hot = cw.Address
		}
		if cw.WalletType == balance.WalletTypeTreasury && cold == "" {
			cold = cw.Address
		}
	}
	if useHot && hot != "" {
		return hot, nil
	}
	if cold != "" {
		return cold, nil
	}
	if hot != "" {
		return hot, nil
	}
	return "", ErrNoHotWallet
}

func (s *Service) OnWithdrawTxConfirmed(ctx context.Context, txHash string) error {
	w, err := s.store.GetWithdrawByTxHash(ctx, txHash)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if w.Status == WithdrawStatusConfirmed {
		return nil
	}
	if w.Status != WithdrawStatusBroadcasting && w.Status != WithdrawStatusApproved {
		return nil
	}

	dbTx, err := s.store.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer dbTx.Rollback()

	hasDebit, err := s.store.hasLedgerEntryTx(ctx, dbTx, w.ID, LedgerWithdrawDebit)
	if err != nil {
		return err
	}
	if !hasDebit {
		if err := s.store.debitWithdrawTx(ctx, dbTx, w.UserID, w.TokenAddress, w.AmountWei, w.ID); err != nil {
			return err
		}
	}
	now := time.Now().UTC()
	_, err = dbTx.ExecContext(ctx, `
		UPDATE withdraw_requests SET status = ?, updated_at = ? WHERE chain_id = ? AND id = ?
	`, WithdrawStatusConfirmed, now, s.store.chainID, w.ID)
	if err != nil {
		return err
	}
	return dbTx.Commit()
}

func (s *Service) OnWithdrawTxFailed(ctx context.Context, txHash string) error {
	w, err := s.store.GetWithdrawByTxHash(ctx, txHash)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if w.Status == WithdrawStatusFailed || w.Status == WithdrawStatusConfirmed {
		return nil
	}

	dbTx, err := s.store.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer dbTx.Rollback()

	if err := s.store.FailWithdrawAndUnfreezeTx(ctx, dbTx, w, "on-chain execution failed"); err != nil {
		return err
	}
	return dbTx.Commit()
}

func (s *Service) HotWithdrawMax() *big.Int {
	v, _ := parseWei(s.cfg.Exchange.HotWithdrawMaxWei)
	return v
}

func (s *Service) RunReconcile(ctx context.Context) ([]ReconcileReport, error) {
	rec := NewReconciler(s.store, s.balStore)
	tokens := append([]string{""}, s.cfg.WatchTokenAddresses()...)
	return rec.Run(ctx, tokens)
}
