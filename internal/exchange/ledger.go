package exchange

import (
	"context"
	"database/sql"
	"math/big"
	"time"
)

type ledgerOp struct {
	entryType   string
	amount      *big.Int
	refType     string
	refID       int64
	deltaAvail  *big.Int
	deltaFrozen *big.Int
}

func (s *Store) creditDepositTx(ctx context.Context, tx *sql.Tx, userID, token, amount string, depositID int64) error {
	amt, err := parseWei(amount)
	if err != nil {
		return err
	}
	return s.applyLedgerTx(ctx, tx, userID, token, ledgerOp{
		entryType: LedgerDepositCredit, amount: amt,
		refType: "deposit", refID: depositID,
		deltaAvail: amt, deltaFrozen: weiZero(),
	})
}

func (s *Store) reverseDepositTx(ctx context.Context, tx *sql.Tx, userID, token, amount string, depositID int64) error {
	amt, err := parseWei(amount)
	if err != nil {
		return err
	}
	neg := new(big.Int).Neg(amt)
	return s.applyLedgerTx(ctx, tx, userID, token, ledgerOp{
		entryType: LedgerDepositReverse, amount: amt,
		refType: "deposit", refID: depositID,
		deltaAvail: neg, deltaFrozen: weiZero(),
	})
}

func (s *Store) freezeWithdrawTx(ctx context.Context, tx *sql.Tx, userID, token, amount string, withdrawID int64) error {
	amt, err := parseWei(amount)
	if err != nil {
		return err
	}
	neg := new(big.Int).Neg(amt)
	return s.applyLedgerTx(ctx, tx, userID, token, ledgerOp{
		entryType: LedgerWithdrawFreeze, amount: amt,
		refType: "withdraw", refID: withdrawID,
		deltaAvail: neg, deltaFrozen: amt,
	})
}

func (s *Store) unfreezeWithdrawTx(ctx context.Context, tx *sql.Tx, userID, token, amount string, withdrawID int64) error {
	amt, err := parseWei(amount)
	if err != nil {
		return err
	}
	neg := new(big.Int).Neg(amt)
	return s.applyLedgerTx(ctx, tx, userID, token, ledgerOp{
		entryType: LedgerWithdrawUnfreeze, amount: amt,
		refType: "withdraw", refID: withdrawID,
		deltaAvail: amt, deltaFrozen: neg,
	})
}

func (s *Store) debitWithdrawTx(ctx context.Context, tx *sql.Tx, userID, token, amount string, withdrawID int64) error {
	amt, err := parseWei(amount)
	if err != nil {
		return err
	}
	neg := new(big.Int).Neg(amt)
	return s.applyLedgerTx(ctx, tx, userID, token, ledgerOp{
		entryType: LedgerWithdrawDebit, amount: amt,
		refType: "withdraw", refID: withdrawID,
		deltaAvail: weiZero(), deltaFrozen: neg,
	})
}

func (s *Store) applyLedgerTx(ctx context.Context, tx *sql.Tx, userID, token string, op ledgerOp) error {
	token = normToken(token)
	now := time.Now().UTC()

	var availStr, frozenStr string
	err := tx.QueryRowContext(ctx, `
		SELECT available_wei, frozen_wei FROM user_ledger_accounts
		WHERE chain_id = ? AND user_id = ? AND token_address = ?
		FOR UPDATE
	`, s.chainID, userID, token).Scan(&availStr, &frozenStr)
	if err == sql.ErrNoRows {
		availStr, frozenStr = "0", "0"
	} else if err != nil {
		return err
	}

	avail, err := parseWei(availStr)
	if err != nil {
		return err
	}
	frozen, err := parseWei(frozenStr)
	if err != nil {
		return err
	}

	newAvail := weiAdd(avail, op.deltaAvail)
	if newAvail.Sign() < 0 {
		return ErrInsufficientBalance
	}

	newFrozen := weiAdd(frozen, op.deltaFrozen)
	if newFrozen.Sign() < 0 {
		return ErrInsufficientBalance
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO user_ledger_accounts (chain_id, user_id, token_address, available_wei, frozen_wei, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			available_wei = VALUES(available_wei),
			frozen_wei = VALUES(frozen_wei),
			updated_at = VALUES(updated_at)
	`, s.chainID, userID, token, weiString(newAvail), weiString(newFrozen), now)
	if err != nil {
		return err
	}

	var refType any
	var refID any
	if op.refType != "" {
		refType = op.refType
		refID = op.refID
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO ledger_entries (
			chain_id, user_id, token_address, entry_type, amount_wei,
			ref_type, ref_id, balance_available_after, balance_frozen_after, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, s.chainID, userID, token, op.entryType, weiString(op.amount),
		refType, refID, weiString(newAvail), weiString(newFrozen), now)
	return err
}
