package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"gin-demo/pb"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type ethTxService struct {
	pb.UnimplementedEthTxServiceServer
	rpcClient *ethclient.Client
	rpcChain  uint64
	db        *sql.DB
	mu        sync.RWMutex
	accounts  map[string]*accountState
	localTx   map[string]*pb.EthTransaction
	users     map[string]*pb.User
	userAcc   map[string]map[string]struct{}
	eventLogs []*pb.LogEvent
}

type accountState struct {
	Address   string
	Balance   *big.Int
	Nonce     uint64
	ChainID   uint64
	UpdatedAt string
}

var mockTxByHash = map[string]*pb.EthTransaction{
	"0xabc123": {
		Hash:        "0xabc123",
		From:        "0x1111111111111111111111111111111111111111",
		To:          "0x2222222222222222222222222222222222222222",
		Value:       "1556465265266",
		BlockNumber: 123123123,
		Gas:         21000,
		Nonce:       1,
		Success:     true,
		ChainId:     1,
		State:       pb.TxState_TX_STATE_FINALIZED,
	},
	"0xdef456": {
		Hash:        "0xdef456",
		From:        "0xaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaa",
		To:          "0xbbccccccccccccccccccccccccccccccccccccbb",
		Value:       "250000000000000000",
		BlockNumber: 456456456,
		Gas:         35000,
		Nonce:       8,
		Success:     true,
		ChainId:     1,
		State:       pb.TxState_TX_STATE_CONFIRMED,
	},
	"0xdeadbeef": {
		Hash:        "0xdeadbeef",
		From:        "0x9999999999999999999999999999999999999999",
		To:          "0x8888888888888888888888888888888888888888",
		Value:       "42000000000000000",
		BlockNumber: 0xbeefbeefbeef,
		Gas:         70000,
		Nonce:       19,
		Success:     false,
		ChainId:     1,
		State:       pb.TxState_TX_STATE_FAILED,
	},
	"0xpending01": {
		Hash:        "0xpending01",
		From:        "0x1234512345123451234512345123451234512345",
		To:          "0x5432154321543215432154321543215432154321",
		Value:       "9999999999999",
		BlockNumber: 0101010101,
		Gas:         21000,
		Nonce:       28,
		Success:     false,
		ChainId:     1,
		State:       pb.TxState_TX_STATE_PENDING,
	},
}

func normalizeHash(hash string) string {
	return strings.ToLower(strings.TrimSpace(hash))
}

func normalizeAddress(addr string) string {
	return strings.ToLower(strings.TrimSpace(addr))
}

func parsePositiveInt(value string) (*big.Int, error) {
	v := strings.TrimSpace(value)
	if v == "" {
		return nil, status.Error(codes.InvalidArgument, "value is required")
	}
	n, ok := new(big.Int).SetString(v, 10)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid numeric string: %s", value)
	}
	if n.Sign() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "value must be > 0")
	}
	return n, nil
}

func parseNonNegativeInt(value string) (*big.Int, error) {
	v := strings.TrimSpace(value)
	if v == "" {
		return big.NewInt(0), nil
	}
	n, ok := new(big.Int).SetString(v, 10)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid numeric string: %s", value)
	}
	if n.Sign() < 0 {
		return nil, status.Error(codes.InvalidArgument, "value must be >= 0")
	}
	return n, nil
}

func dbTimestamp() time.Time {
	// Older MySQL variants without fractional DATETIME support
	// reject values carrying nanoseconds from Go.
	return time.Now().UTC().Truncate(time.Second)
}

func initMySQL(ctx context.Context) (*sql.DB, error) {
	adminDSN := strings.TrimSpace(os.Getenv("MYSQL_ADMIN_DSN"))
	if adminDSN == "" {
		adminDSN = "root:123456@tcp(127.0.0.1:3306)/?parseTime=true"
	}
	dataDSN := strings.TrimSpace(os.Getenv("MYSQL_DSN"))
	if dataDSN == "" {
		dataDSN = "root:123456@tcp(127.0.0.1:3306)/web3lite?parseTime=true"
	}

	adminDB, err := sql.Open("mysql", adminDSN)
	if err != nil {
		return nil, err
	}
	defer adminDB.Close()
	if err := adminDB.PingContext(ctx); err != nil {
		return nil, err
	}
	if _, err := adminDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS web3lite CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"); err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", dataDSN)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func validateHashRequest(req *pb.TxHashRequest) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "request is required")
	}
	hash := normalizeHash(req.Hash)
	if hash == "" || !strings.HasPrefix(hash, "0x") {
		return "", status.Error(codes.InvalidArgument, "hash must be a hex string prefixed with 0x")
	}
	return hash, nil
}

func deriveState(tx *pb.EthTransaction) *pb.EthTransaction {
	derived := proto.Clone(tx).(*pb.EthTransaction)
	switch derived.State {
	case pb.TxState_TX_STATE_PENDING:
		derived.Confirmations = 0
		derived.BlockNumber = 0
		derived.Success = false
	case pb.TxState_TX_STATE_CONFIRMED:
		derived.Confirmations = 8
		derived.Success = true
	case pb.TxState_TX_STATE_FINALIZED:
		derived.Confirmations = 64
		derived.Success = true
	case pb.TxState_TX_STATE_FAILED:
		derived.Confirmations = 3
		derived.Success = false
	default:
		derived.State = pb.TxState_TX_STATE_UNSPECIFIED
	}
	derived.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	return derived
}

func toLightweight(tx *pb.EthTransaction) *pb.EthTransaction {
	if tx == nil {
		return nil
	}
	return &pb.EthTransaction{
		Hash:          tx.Hash,
		BlockNumber:   tx.BlockNumber,
		ChainId:       tx.ChainId,
		Confirmations: tx.Confirmations,
		State:         tx.State,
		UpdatedAt:     tx.UpdatedAt,
		Success:       tx.Success,
	}
}

func stateByConfirmations(confirmations uint64, success bool, pending bool) pb.TxState {
	if pending {
		return pb.TxState_TX_STATE_PENDING
	}
	if !success {
		return pb.TxState_TX_STATE_FAILED
	}
	if confirmations >= 64 {
		return pb.TxState_TX_STATE_FINALIZED
	}
	return pb.TxState_TX_STATE_CONFIRMED
}

func (s *ethTxService) getFromAddress(tx *types.Transaction) string {
	signer := types.LatestSignerForChainID(tx.ChainId())
	from, err := types.Sender(signer, tx)
	if err != nil {
		return ""
	}
	return strings.ToLower(from.Hex())
}

func (s *ethTxService) getLiveTransaction(ctx context.Context, hash string, reqChainID uint64) (*pb.EthTransaction, error) {
	if s.rpcClient == nil {
		return nil, nil
	}
	if reqChainID != 0 && reqChainID != s.rpcChain {
		return nil, status.Errorf(codes.InvalidArgument, "chain_id %d does not match RPC chain %d", reqChainID, s.rpcChain)
	}

	txHash := common.HexToHash(hash)
	tx, isPending, err := s.rpcClient.TransactionByHash(ctx, txHash)
	if err != nil {
		return nil, nil
	}

	var blockNumber uint64
	var confirmations uint64
	success := !isPending
	if !isPending {
		receipt, err := s.rpcClient.TransactionReceipt(ctx, txHash)
		if err == nil {
			blockNumber = receipt.BlockNumber.Uint64()
			success = receipt.Status == 1
			latestHeader, hErr := s.rpcClient.HeaderByNumber(ctx, nil)
			if hErr == nil && latestHeader.Number.Uint64() >= blockNumber {
				confirmations = latestHeader.Number.Uint64() - blockNumber + 1
			}
		}
	}

	to := ""
	if tx.To() != nil {
		to = strings.ToLower(tx.To().Hex())
	}
	out := &pb.EthTransaction{
		Hash:          hash,
		From:          s.getFromAddress(tx),
		To:            to,
		Value:         tx.Value().String(),
		BlockNumber:   blockNumber,
		Gas:           tx.Gas(),
		Nonce:         tx.Nonce(),
		Success:       success,
		ChainId:       s.rpcChain,
		Confirmations: confirmations,
		State:         stateByConfirmations(confirmations, success, isPending),
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339),
	}
	return out, nil
}

func (s *ethTxService) getLiveReceipt(ctx context.Context, hash string, reqChainID uint64) (*pb.TxReceipt, error) {
	if s.rpcClient == nil {
		return nil, nil
	}
	if reqChainID != 0 && reqChainID != s.rpcChain {
		return nil, status.Errorf(codes.InvalidArgument, "chain_id %d does not match RPC chain %d", reqChainID, s.rpcChain)
	}
	receipt, err := s.rpcClient.TransactionReceipt(ctx, common.HexToHash(hash))
	if err != nil {
		return nil, nil
	}
	latestHeader, _ := s.rpcClient.HeaderByNumber(ctx, nil)
	confirmations := uint64(0)
	if latestHeader != nil && latestHeader.Number.Uint64() >= receipt.BlockNumber.Uint64() {
		confirmations = latestHeader.Number.Uint64() - receipt.BlockNumber.Uint64() + 1
	}
	success := receipt.Status == 1
	return &pb.TxReceipt{
		Hash:        hash,
		BlockNumber: receipt.BlockNumber.Uint64(),
		GasUsed:     receipt.GasUsed,
		Success:     success,
		State:       stateByConfirmations(confirmations, success, false),
		LogsBloom:   fmt.Sprintf("0x%x", receipt.Bloom.Bytes()),
	}, nil
}

func (s *ethTxService) defaultChainID() uint64 {
	if s.rpcChain != 0 {
		return s.rpcChain
	}
	return 1
}

func (s *ethTxService) ensureSchema(ctx context.Context) error {
	if s.db == nil {
		return nil
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS accounts (
			address VARCHAR(66) PRIMARY KEY,
			balance VARCHAR(100) NOT NULL,
			nonce BIGINT UNSIGNED NOT NULL,
			chain_id BIGINT UNSIGNED NOT NULL,
			updated_at DATETIME NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS transactions (
			hash VARCHAR(80) PRIMARY KEY,
			from_addr VARCHAR(66) NOT NULL,
			to_addr VARCHAR(66) NOT NULL,
			value VARCHAR(100) NOT NULL,
			block_number BIGINT UNSIGNED NOT NULL,
			gas BIGINT UNSIGNED NOT NULL,
			nonce BIGINT UNSIGNED NOT NULL,
			success BOOLEAN NOT NULL,
			chain_id BIGINT UNSIGNED NOT NULL,
			confirmations BIGINT UNSIGNED NOT NULL,
			state INT NOT NULL,
			updated_at DATETIME NOT NULL,
			INDEX idx_from_addr (from_addr),
			INDEX idx_to_addr (to_addr)
		)`,
		`CREATE TABLE IF NOT EXISTS users (
			user_id VARCHAR(64) PRIMARY KEY,
			name VARCHAR(120) NOT NULL,
			email VARCHAR(160) NOT NULL UNIQUE,
			chain_id BIGINT UNSIGNED NOT NULL,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS user_accounts (
			user_id VARCHAR(64) NOT NULL,
			address VARCHAR(66) NOT NULL,
			chain_id BIGINT UNSIGNED NOT NULL,
			created_at DATETIME NOT NULL,
			PRIMARY KEY(user_id, address),
			INDEX idx_ua_address(address)
		)`,
		`CREATE TABLE IF NOT EXISTS event_logs (
			id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
			tx_hash VARCHAR(80) NOT NULL,
			block_number BIGINT UNSIGNED NOT NULL,
			chain_id BIGINT UNSIGNED NOT NULL,
			contract_address VARCHAR(66) NOT NULL,
			topic0 VARCHAR(120) NOT NULL,
			topics_json TEXT NOT NULL,
			data TEXT NOT NULL,
			log_index BIGINT UNSIGNED NOT NULL,
			event_name VARCHAR(120) NOT NULL,
			updated_at DATETIME NOT NULL,
			INDEX idx_event_filter(chain_id, contract_address, topic0, id)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (s *ethTxService) seedDefaultAccountsDB(ctx context.Context) error {
	if s.db == nil {
		return nil
	}
	now := dbTimestamp()
	for _, seed := range []struct {
		addr    string
		balance string
	}{
		{"0x1111111111111111111111111111111111111111", "5000000000000000000"},
		{"0x2222222222222222222222222222222222222222", "3000000000000000000"},
		{"0x9999999999999999999999999999999999999999", "1000000000000000000"},
	} {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO accounts(address, balance, nonce, chain_id, updated_at)
			VALUES(?,?,?,?,?)
			ON DUPLICATE KEY UPDATE address=address
		`, seed.addr, seed.balance, 0, s.defaultChainID(), now)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ethTxService) toPBAccount(a *accountState) *pb.Account {
	return &pb.Account{
		Address:   a.Address,
		Balance:   a.Balance.String(),
		Nonce:     a.Nonce,
		ChainId:   a.ChainID,
		UpdatedAt: a.UpdatedAt,
	}
}

func accountFromRow(address, balance string, nonce, chainID uint64, updatedAt time.Time) (*accountState, error) {
	bal, ok := new(big.Int).SetString(balance, 10)
	if !ok {
		return nil, status.Errorf(codes.Internal, "invalid balance in storage: %s", balance)
	}
	return &accountState{
		Address:   address,
		Balance:   bal,
		Nonce:     nonce,
		ChainID:   chainID,
		UpdatedAt: updatedAt.UTC().Format(time.RFC3339),
	}, nil
}

func (s *ethTxService) getAccountDB(ctx context.Context, address string) (*accountState, error) {
	var balance string
	var nonce, chainID uint64
	var updatedAt time.Time
	err := s.db.QueryRowContext(ctx, `
		SELECT address, balance, nonce, chain_id, updated_at
		FROM accounts WHERE address = ?
	`, address).Scan(&address, &balance, &nonce, &chainID, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, status.Errorf(codes.NotFound, "account not found: %s", address)
		}
		return nil, err
	}
	return accountFromRow(address, balance, nonce, chainID, updatedAt)
}

func (s *ethTxService) createAccountDB(ctx context.Context, addr string, bal *big.Int, chainID uint64) (*pb.Account, error) {
	now := dbTimestamp()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO accounts(address, balance, nonce, chain_id, updated_at)
		VALUES(?,?,?,?,?)
	`, addr, bal.String(), 0, chainID, now)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "duplicate") {
			return nil, status.Errorf(codes.AlreadyExists, "account already exists: %s", addr)
		}
		return nil, err
	}
	return &pb.Account{
		Address:   addr,
		Balance:   bal.String(),
		Nonce:     0,
		ChainId:   chainID,
		UpdatedAt: now.Format(time.RFC3339),
	}, nil
}

func (s *ethTxService) generateAccountAddress() string {
	return fmt.Sprintf("0x%040x", time.Now().UnixNano())
}

func (s *ethTxService) generateTxHash() string {
	return fmt.Sprintf("0x%064x", time.Now().UnixNano())
}

func (s *ethTxService) generateUserID() string {
	return fmt.Sprintf("u-%d", time.Now().UnixNano())
}

func (s *ethTxService) buildTransferLog(tx *pb.EthTransaction) *pb.LogEvent {
	return &pb.LogEvent{
		TxHash:          tx.Hash,
		BlockNumber:     tx.BlockNumber,
		ChainId:         tx.ChainId,
		ContractAddress: "0x0000000000000000000000000000000000000000",
		Topics: []string{
			"0xddf252ad00000000000000000000000000000000000000000000000000000000",
			tx.From,
			tx.To,
		},
		Data:      tx.Value,
		LogIndex:  0,
		EventName: "Transfer",
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}
}

func (s *ethTxService) appendEventLog(ctx context.Context, ev *pb.LogEvent) error {
	if ev == nil {
		return nil
	}
	if s.db != nil {
		topicsJSON, _ := json.Marshal(ev.Topics)
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO event_logs(tx_hash, block_number, chain_id, contract_address, topic0, topics_json, data, log_index, event_name, updated_at)
			VALUES(?,?,?,?,?,?,?,?,?,?)
		`, ev.TxHash, ev.BlockNumber, ev.ChainId, ev.ContractAddress, firstTopic(ev.Topics), string(topicsJSON), ev.Data, ev.LogIndex, ev.EventName, dbTimestamp())
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.eventLogs = append(s.eventLogs, ev)
	return nil
}

func firstTopic(topics []string) string {
	if len(topics) == 0 {
		return ""
	}
	return topics[0]
}

func (s *ethTxService) getLocalTxDB(ctx context.Context, hash string) (*pb.EthTransaction, error) {
	var tx pb.EthTransaction
	var state int32
	var updatedAt time.Time
	err := s.db.QueryRowContext(ctx, `
		SELECT hash, from_addr, to_addr, value, block_number, gas, nonce, success, chain_id, confirmations, state, updated_at
		FROM transactions WHERE hash = ?
	`, hash).Scan(
		&tx.Hash, &tx.From, &tx.To, &tx.Value, &tx.BlockNumber, &tx.Gas, &tx.Nonce, &tx.Success,
		&tx.ChainId, &tx.Confirmations, &state, &updatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	tx.State = pb.TxState(state)
	tx.UpdatedAt = updatedAt.UTC().Format(time.RFC3339)
	return &tx, nil
}

func (s *ethTxService) transferDB(ctx context.Context, req *pb.TransferRequest, fromAddr, toAddr string, amount *big.Int, chainID uint64) (*pb.TransferResponse, error) {
	txDB, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer txDB.Rollback()

	var fromBalanceStr string
	var fromNonce, fromChain uint64
	var fromUpdated time.Time
	err = txDB.QueryRowContext(ctx, `
		SELECT balance, nonce, chain_id, updated_at FROM accounts WHERE address = ? FOR UPDATE
	`, fromAddr).Scan(&fromBalanceStr, &fromNonce, &fromChain, &fromUpdated)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, status.Errorf(codes.NotFound, "from account not found: %s", req.From)
		}
		return nil, err
	}
	if fromChain != chainID {
		return nil, status.Errorf(codes.FailedPrecondition, "from account chain mismatch: %d", fromChain)
	}
	fromBalance, ok := new(big.Int).SetString(fromBalanceStr, 10)
	if !ok {
		return nil, status.Errorf(codes.Internal, "invalid balance in storage: %s", fromBalanceStr)
	}
	if fromBalance.Cmp(amount) < 0 {
		return nil, status.Error(codes.FailedPrecondition, "insufficient balance")
	}

	var toBalanceStr string
	var toNonce, toChain uint64
	var toUpdated time.Time
	err = txDB.QueryRowContext(ctx, `
		SELECT balance, nonce, chain_id, updated_at FROM accounts WHERE address = ? FOR UPDATE
	`, toAddr).Scan(&toBalanceStr, &toNonce, &toChain, &toUpdated)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	toExists := err != sql.ErrNoRows
	toBalance := big.NewInt(0)
	if toExists {
		if toChain != chainID {
			return nil, status.Errorf(codes.FailedPrecondition, "to account chain mismatch: %d", toChain)
		}
		parsed, ok := new(big.Int).SetString(toBalanceStr, 10)
		if !ok {
			return nil, status.Errorf(codes.Internal, "invalid balance in storage: %s", toBalanceStr)
		}
		toBalance = parsed
	}

	now := dbTimestamp()
	newFromBalance := new(big.Int).Sub(fromBalance, amount)
	newToBalance := new(big.Int).Add(toBalance, amount)
	newFromNonce := fromNonce + 1

	if _, err := txDB.ExecContext(ctx, `
		UPDATE accounts SET balance=?, nonce=?, updated_at=? WHERE address=?
	`, newFromBalance.String(), newFromNonce, now, fromAddr); err != nil {
		return nil, err
	}
	if toExists {
		if _, err := txDB.ExecContext(ctx, `
			UPDATE accounts SET balance=?, updated_at=? WHERE address=?
		`, newToBalance.String(), now, toAddr); err != nil {
			return nil, err
		}
	} else {
		if _, err := txDB.ExecContext(ctx, `
			INSERT INTO accounts(address, balance, nonce, chain_id, updated_at)
			VALUES(?,?,?,?,?)
		`, toAddr, newToBalance.String(), 0, chainID, now); err != nil {
			return nil, err
		}
	}

	tx := &pb.EthTransaction{
		Hash:          s.generateTxHash(),
		From:          fromAddr,
		To:            toAddr,
		Value:         amount.String(),
		BlockNumber:   uint64(time.Now().Unix()),
		Gas:           21000,
		Nonce:         newFromNonce,
		Success:       true,
		ChainId:       chainID,
		Confirmations: 1,
		State:         pb.TxState_TX_STATE_CONFIRMED,
		UpdatedAt:     now.Format(time.RFC3339),
	}
	if _, err := txDB.ExecContext(ctx, `
		INSERT INTO transactions(hash, from_addr, to_addr, value, block_number, gas, nonce, success, chain_id, confirmations, state, updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
	`, tx.Hash, tx.From, tx.To, tx.Value, tx.BlockNumber, tx.Gas, tx.Nonce, tx.Success, tx.ChainId, tx.Confirmations, int32(tx.State), now); err != nil {
		return nil, err
	}

	if err := txDB.Commit(); err != nil {
		return nil, err
	}
	if err := s.appendEventLog(ctx, s.buildTransferLog(tx)); err != nil {
		log.Printf("appendEventLog(db) failed: %v", err)
	}
	return &pb.TransferResponse{
		Tx: tx,
		FromAccount: &pb.Account{
			Address:   fromAddr,
			Balance:   newFromBalance.String(),
			Nonce:     newFromNonce,
			ChainId:   chainID,
			UpdatedAt: now.Format(time.RFC3339),
		},
		ToAccount: &pb.Account{
			Address:   toAddr,
			Balance:   newToBalance.String(),
			Nonce:     toNonce,
			ChainId:   chainID,
			UpdatedAt: now.Format(time.RFC3339),
		},
	}, nil
}

func (s *ethTxService) seedDefaultAccounts() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.accounts == nil {
		s.accounts = map[string]*accountState{}
	}
	if s.localTx == nil {
		s.localTx = map[string]*pb.EthTransaction{}
	}
	now := time.Now().UTC().Format(time.RFC3339)
	for _, seed := range []struct {
		addr    string
		balance string
	}{
		{"0x1111111111111111111111111111111111111111", "5000000000000000000"},
		{"0x2222222222222222222222222222222222222222", "3000000000000000000"},
		{"0x9999999999999999999999999999999999999999", "1000000000000000000"},
	} {
		bal, _ := new(big.Int).SetString(seed.balance, 10)
		s.accounts[seed.addr] = &accountState{
			Address:   seed.addr,
			Balance:   bal,
			Nonce:     0,
			ChainID:   s.defaultChainID(),
			UpdatedAt: now,
		}
	}
}

func (s *ethTxService) CreateAccount(ctx context.Context, req *pb.CreateAccountRequest) (*pb.Account, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	chainID := s.defaultChainID()
	if req.ChainId != 0 && req.ChainId != chainID {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported chain_id: %d", req.ChainId)
	}

	addr := normalizeAddress(req.Address)
	if addr == "" {
		addr = s.generateAccountAddress()
	}
	if !strings.HasPrefix(addr, "0x") {
		return nil, status.Error(codes.InvalidArgument, "address must be hex prefixed with 0x")
	}
	bal, err := parseNonNegativeInt(req.InitialBalance)
	if err != nil {
		return nil, err
	}
	if s.db != nil {
		return s.createAccountDB(ctx, addr, bal, chainID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.accounts[addr]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "account already exists: %s", addr)
	}
	acc := &accountState{
		Address:   addr,
		Balance:   bal,
		Nonce:     0,
		ChainID:   chainID,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	s.accounts[addr] = acc
	return s.toPBAccount(acc), nil
}

func (s *ethTxService) GetAccount(ctx context.Context, req *pb.GetAccountRequest) (*pb.Account, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	chainID := s.defaultChainID()
	if req.ChainId != 0 && req.ChainId != chainID {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported chain_id: %d", req.ChainId)
	}
	addr := normalizeAddress(req.Address)
	if addr == "" {
		return nil, status.Error(codes.InvalidArgument, "address is required")
	}
	if s.db != nil {
		acc, err := s.getAccountDB(ctx, addr)
		if err != nil {
			return nil, err
		}
		return s.toPBAccount(acc), nil
	}

	s.mu.RLock()
	acc, ok := s.accounts[addr]
	s.mu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "account not found: %s", req.Address)
	}
	return s.toPBAccount(acc), nil
}

func (s *ethTxService) Transfer(ctx context.Context, req *pb.TransferRequest) (*pb.TransferResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	chainID := s.defaultChainID()
	if req.ChainId != 0 && req.ChainId != chainID {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported chain_id: %d", req.ChainId)
	}
	fromAddr := normalizeAddress(req.From)
	toAddr := normalizeAddress(req.To)
	if fromAddr == "" || toAddr == "" {
		return nil, status.Error(codes.InvalidArgument, "from and to are required")
	}
	if fromAddr == toAddr {
		return nil, status.Error(codes.InvalidArgument, "from and to must be different")
	}
	amount, err := parsePositiveInt(req.Value)
	if err != nil {
		return nil, err
	}
	if s.db != nil {
		return s.transferDB(ctx, req, fromAddr, toAddr, amount, chainID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	fromAcc, ok := s.accounts[fromAddr]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "from account not found: %s", req.From)
	}
	toAcc, ok := s.accounts[toAddr]
	if !ok {
		toAcc = &accountState{
			Address:   toAddr,
			Balance:   big.NewInt(0),
			Nonce:     0,
			ChainID:   chainID,
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		}
		s.accounts[toAddr] = toAcc
	}
	if fromAcc.Balance.Cmp(amount) < 0 {
		return nil, status.Error(codes.FailedPrecondition, "insufficient balance")
	}

	fromAcc.Balance = new(big.Int).Sub(fromAcc.Balance, amount)
	fromAcc.Nonce++
	fromAcc.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	toAcc.Balance = new(big.Int).Add(toAcc.Balance, amount)
	toAcc.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	tx := &pb.EthTransaction{
		Hash:          s.generateTxHash(),
		From:          fromAddr,
		To:            toAddr,
		Value:         amount.String(),
		BlockNumber:   uint64(time.Now().Unix()),
		Gas:           21000,
		Nonce:         fromAcc.Nonce,
		Success:       true,
		ChainId:       chainID,
		Confirmations: 1,
		State:         pb.TxState_TX_STATE_CONFIRMED,
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339),
	}
	s.localTx[tx.Hash] = tx
	if err := s.appendEventLog(ctx, s.buildTransferLog(tx)); err != nil {
		log.Printf("appendEventLog(mem) failed: %v", err)
	}

	return &pb.TransferResponse{
		Tx:          tx,
		FromAccount: s.toPBAccount(fromAcc),
		ToAccount:   s.toPBAccount(toAcc),
	}, nil
}

func (s *ethTxService) streamLiveTransactions(req *pb.SubscribeRequest, stream grpc.ServerStreamingServer[pb.EthTransaction]) error {
	ctx := stream.Context()
	latest, err := s.rpcClient.HeaderByNumber(ctx, nil)
	if err != nil {
		return status.Errorf(codes.Unavailable, "read latest header failed: %v", err)
	}
	lastProcessed := latest.Number.Uint64()
	seen := map[string]struct{}{}
	poll := time.NewTicker(2 * time.Second)
	defer poll.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-poll.C:
			head, err := s.rpcClient.HeaderByNumber(ctx, nil)
			if err != nil {
				log.Printf("[SubscribeTransactions] header poll failed: %v", err)
				continue
			}
			latestNum := head.Number.Uint64()
			if latestNum <= lastProcessed {
				continue
			}

			for n := lastProcessed + 1; n <= latestNum; n++ {
				block, err := s.rpcClient.BlockByNumber(ctx, new(big.Int).SetUint64(n))
				if err != nil {
					log.Printf("[SubscribeTransactions] block fetch failed num=%d err=%v", n, err)
					continue
				}
				for _, tx := range block.Transactions() {
					hash := strings.ToLower(tx.Hash().Hex())
					if _, ok := seen[hash]; ok {
						continue
					}
					seen[hash] = struct{}{}

					receipt, err := s.rpcClient.TransactionReceipt(ctx, tx.Hash())
					if err != nil {
						log.Printf("[SubscribeTransactions] receipt fetch failed hash=%s err=%v", hash, err)
						continue
					}

					from := s.getFromAddress(tx)
					to := ""
					if tx.To() != nil {
						to = strings.ToLower(tx.To().Hex())
					}
					success := receipt.Status == 1
					confirmations := latestNum - n + 1
					item := &pb.EthTransaction{
						Hash:          hash,
						From:          from,
						To:            to,
						Value:         tx.Value().String(),
						BlockNumber:   n,
						Gas:           tx.Gas(),
						Nonce:         tx.Nonce(),
						Success:       success,
						ChainId:       s.rpcChain,
						Confirmations: confirmations,
						State:         stateByConfirmations(confirmations, success, false),
						UpdatedAt:     time.Now().UTC().Format(time.RFC3339),
					}

					if !req.IncludeFailed && item.State == pb.TxState_TX_STATE_FAILED {
						continue
					}
					if req.Address != "" {
						addr := strings.ToLower(strings.TrimSpace(req.Address))
						if addr != strings.ToLower(item.From) && addr != strings.ToLower(item.To) {
							continue
						}
					}
					if req.Lightweight {
						item = toLightweight(item)
					}
					if err := stream.Send(item); err != nil {
						return err
					}
				}
			}
			lastProcessed = latestNum
		}
	}
}

func (s *ethTxService) GetTransaction(ctx context.Context, req *pb.TxHashRequest) (*pb.EthTransaction, error) {
	hash, err := validateHashRequest(req)
	if err != nil {
		return nil, err
	}
	// 模拟“真实 web3 节点”会做的事：日志里展示二进制/JSON 的序列化与反序列化。
	bin, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request (binary): %w", err)
	}
	var req2 pb.TxHashRequest
	if err := proto.Unmarshal(bin, &req2); err != nil {
		return nil, fmt.Errorf("unmarshal request (binary): %w", err)
	}

	j, err := protojson.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request (json): %w", err)
	}
	var req3 pb.TxHashRequest
	if err := protojson.Unmarshal(j, &req3); err != nil {
		return nil, fmt.Errorf("unmarshal request (json): %w", err)
	}

	log.Printf("[GetTransaction] hash=%q bin_len=%d req2.hash=%q req3.hash=%q json=%s", req.Hash, len(bin), req2.Hash, req3.Hash, string(j))

	live, err := s.getLiveTransaction(ctx, hash, req.ChainId)
	if err != nil {
		return nil, err
	}
	if live != nil {
		return live, nil
	}

	var tx *pb.EthTransaction
	var ok bool
	if s.db != nil {
		localTx, dbErr := s.getLocalTxDB(ctx, hash)
		if dbErr != nil {
			return nil, dbErr
		}
		if localTx != nil {
			tx = localTx
			ok = true
		}
	} else {
		s.mu.RLock()
		tx, ok = s.localTx[hash]
		s.mu.RUnlock()
	}
	if !ok {
		tx, ok = mockTxByHash[hash]
	}
	if !ok {
		return nil, status.Errorf(codes.NotFound, "transaction not found: %s", req.Hash)
	}
	out := deriveState(tx)

	// 同样演示 response 的序列化/反序列化
	respBin, err := proto.Marshal(out)
	if err == nil {
		respJSON, _ := protojson.Marshal(out)
		log.Printf("[GetTransaction] resp_bin_len=%d resp_json=%s", len(respBin), string(respJSON))
	}

	return out, nil
}

func (s *ethTxService) GetTransactionReceipt(ctx context.Context, req *pb.TxHashRequest) (*pb.TxReceipt, error) {
	hash, err := validateHashRequest(req)
	if err != nil {
		return nil, err
	}
	live, err := s.getLiveReceipt(ctx, hash, req.ChainId)
	if err != nil {
		return nil, err
	}
	if live != nil {
		return live, nil
	}

	tx, err := s.GetTransaction(ctx, req)
	if err != nil {
		return nil, err
	}
	return &pb.TxReceipt{
		Hash:        tx.Hash,
		BlockNumber: tx.BlockNumber,
		GasUsed:     tx.Gas,
		Success:     tx.Success,
		State:       tx.State,
		LogsBloom:   "0x0123deadbeefcafe",
	}, nil
}

func (s *ethTxService) SubscribeTransactions(req *pb.SubscribeRequest, stream grpc.ServerStreamingServer[pb.EthTransaction]) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	if s.rpcClient != nil {
		if req.ChainId != 0 && req.ChainId != s.rpcChain {
			return status.Errorf(codes.InvalidArgument, "chain_id %d does not match RPC chain %d", req.ChainId, s.rpcChain)
		}
		return s.streamLiveTransactions(req, stream)
	}

	if req.ChainId != 0 && req.ChainId != 1 {
		return status.Errorf(codes.InvalidArgument, "unsupported chain_id: %d", req.ChainId)
	}
	// 以固定序列模拟状态推进：pending -> confirmed -> finalized
	sequence := []string{"0xpending01", "0xdef456", "0xabc123"}
	for _, h := range sequence {
		tx := deriveState(mockTxByHash[h])
		if !req.IncludeFailed && tx.State == pb.TxState_TX_STATE_FAILED {
			continue
		}
		if req.Address != "" {
			addr := strings.ToLower(strings.TrimSpace(req.Address))
			if addr != strings.ToLower(tx.From) && addr != strings.ToLower(tx.To) {
				continue
			}
		}
		if req.Lightweight {
			tx = toLightweight(tx)
		}
		if err := stream.Send(tx); err != nil {
			return err
		}
		time.Sleep(500 * time.Millisecond)
	}
	return nil
}

func (s *ethTxService) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.User, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	name := strings.TrimSpace(req.Name)
	email := strings.ToLower(strings.TrimSpace(req.Email))
	if name == "" || email == "" {
		return nil, status.Error(codes.InvalidArgument, "name and email are required")
	}
	chainID := s.defaultChainID()
	if req.ChainId != 0 && req.ChainId != chainID {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported chain_id: %d", req.ChainId)
	}
	user := &pb.User{
		UserId:    s.generateUserID(),
		Name:      name,
		Email:     email,
		ChainId:   chainID,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if s.db != nil {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO users(user_id, name, email, chain_id, created_at, updated_at)
			VALUES(?,?,?,?,?,?)
		`, user.UserId, user.Name, user.Email, user.ChainId, dbTimestamp(), dbTimestamp())
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "duplicate") {
				return nil, status.Errorf(codes.AlreadyExists, "email already exists: %s", user.Email)
			}
			return nil, err
		}
		return user, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.users == nil {
		s.users = map[string]*pb.User{}
	}
	s.users[user.UserId] = user
	return user, nil
}

func (s *ethTxService) GetUser(ctx context.Context, req *pb.GetUserRequest) (*pb.User, error) {
	if req == nil || strings.TrimSpace(req.UserId) == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	if s.db != nil {
		var user pb.User
		var createdAt, updatedAt time.Time
		err := s.db.QueryRowContext(ctx, `
			SELECT user_id, name, email, chain_id, created_at, updated_at
			FROM users WHERE user_id = ?
		`, req.UserId).Scan(&user.UserId, &user.Name, &user.Email, &user.ChainId, &createdAt, &updatedAt)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, status.Errorf(codes.NotFound, "user not found: %s", req.UserId)
			}
			return nil, err
		}
		user.CreatedAt = createdAt.UTC().Format(time.RFC3339)
		user.UpdatedAt = updatedAt.UTC().Format(time.RFC3339)
		return &user, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	user, ok := s.users[req.UserId]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "user not found: %s", req.UserId)
	}
	return user, nil
}

func (s *ethTxService) BindUserAccount(ctx context.Context, req *pb.BindUserAccountRequest) (*pb.BindUserAccountResponse, error) {
	if req == nil || strings.TrimSpace(req.UserId) == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	address := normalizeAddress(req.Address)
	if address == "" {
		return nil, status.Error(codes.InvalidArgument, "address is required")
	}
	user, err := s.GetUser(ctx, &pb.GetUserRequest{UserId: req.UserId})
	if err != nil {
		return nil, err
	}
	account, err := s.GetAccount(ctx, &pb.GetAccountRequest{Address: address, ChainId: req.ChainId})
	if err != nil {
		return nil, err
	}
	if s.db != nil {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO user_accounts(user_id, address, chain_id, created_at)
			VALUES(?,?,?,?)
			ON DUPLICATE KEY UPDATE user_id=user_id
		`, req.UserId, address, account.ChainId, dbTimestamp())
		if err != nil {
			return nil, err
		}
		return &pb.BindUserAccountResponse{User: user, Account: account}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.userAcc == nil {
		s.userAcc = map[string]map[string]struct{}{}
	}
	if s.userAcc[req.UserId] == nil {
		s.userAcc[req.UserId] = map[string]struct{}{}
	}
	s.userAcc[req.UserId][address] = struct{}{}
	return &pb.BindUserAccountResponse{User: user, Account: account}, nil
}

func (s *ethTxService) ListUserAccounts(ctx context.Context, req *pb.ListUserAccountsRequest) (*pb.ListUserAccountsResponse, error) {
	if req == nil || strings.TrimSpace(req.UserId) == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	user, err := s.GetUser(ctx, &pb.GetUserRequest{UserId: req.UserId})
	if err != nil {
		return nil, err
	}
	resp := &pb.ListUserAccountsResponse{User: user, Accounts: []*pb.Account{}}
	if s.db != nil {
		rows, err := s.db.QueryContext(ctx, `
			SELECT a.address, a.balance, a.nonce, a.chain_id, a.updated_at
			FROM user_accounts ua
			JOIN accounts a ON a.address = ua.address
			WHERE ua.user_id = ? AND (? = 0 OR ua.chain_id = ?)
			ORDER BY ua.created_at ASC
		`, req.UserId, req.ChainId, req.ChainId)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var addr, balance string
			var nonce, chainID uint64
			var updatedAt time.Time
			if err := rows.Scan(&addr, &balance, &nonce, &chainID, &updatedAt); err != nil {
				return nil, err
			}
			resp.Accounts = append(resp.Accounts, &pb.Account{
				Address:   addr,
				Balance:   balance,
				Nonce:     nonce,
				ChainId:   chainID,
				UpdatedAt: updatedAt.UTC().Format(time.RFC3339),
			})
		}
		return resp, nil
	}

	s.mu.RLock()
	addresses := s.userAcc[req.UserId]
	s.mu.RUnlock()
	for addr := range addresses {
		acc, err := s.GetAccount(ctx, &pb.GetAccountRequest{Address: addr, ChainId: req.ChainId})
		if err == nil {
			resp.Accounts = append(resp.Accounts, acc)
		}
	}
	return resp, nil
}

func (s *ethTxService) SubscribeLogs(req *pb.SubscribeLogsRequest, stream grpc.ServerStreamingServer[pb.LogEvent]) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	if s.db == nil {
		s.mu.RLock()
		logs := make([]*pb.LogEvent, len(s.eventLogs))
		copy(logs, s.eventLogs)
		s.mu.RUnlock()
		for _, ev := range logs {
			if !matchLogFilter(ev, req) {
				continue
			}
			out := ev
			if req.Lightweight {
				out = &pb.LogEvent{
					TxHash:      ev.TxHash,
					BlockNumber: ev.BlockNumber,
					ChainId:     ev.ChainId,
					Topics:      []string{firstTopic(ev.Topics)},
					EventName:   ev.EventName,
					UpdatedAt:   ev.UpdatedAt,
				}
			}
			if err := stream.Send(out); err != nil {
				return err
			}
		}
		return nil
	}

	var lastID uint64
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
			rows, err := s.db.QueryContext(stream.Context(), `
				SELECT id, tx_hash, block_number, chain_id, contract_address, topics_json, data, log_index, event_name, updated_at
				FROM event_logs
				WHERE id > ? AND (? = 0 OR chain_id = ?) AND (? = '' OR contract_address = ?) AND (? = '' OR topic0 = ?)
				ORDER BY id ASC LIMIT 200
			`, lastID, req.ChainId, req.ChainId, normalizeAddress(req.ContractAddress), normalizeAddress(req.ContractAddress), req.Topic0, req.Topic0)
			if err != nil {
				return status.Errorf(codes.Unavailable, "query logs failed: %v", err)
			}
			for rows.Next() {
				var id, blockNumber, chainID, logIndex uint64
				var txHash, contractAddr, topicsJSON, data, eventName string
				var updatedAt time.Time
				if err := rows.Scan(&id, &txHash, &blockNumber, &chainID, &contractAddr, &topicsJSON, &data, &logIndex, &eventName, &updatedAt); err != nil {
					rows.Close()
					return err
				}
				lastID = id
				var topics []string
				_ = json.Unmarshal([]byte(topicsJSON), &topics)
				ev := &pb.LogEvent{
					TxHash:          txHash,
					BlockNumber:     blockNumber,
					ChainId:         chainID,
					ContractAddress: contractAddr,
					Topics:          topics,
					Data:            data,
					LogIndex:        logIndex,
					EventName:       eventName,
					UpdatedAt:       updatedAt.UTC().Format(time.RFC3339),
				}
				if req.Lightweight {
					ev = &pb.LogEvent{
						TxHash:      ev.TxHash,
						BlockNumber: ev.BlockNumber,
						ChainId:     ev.ChainId,
						Topics:      []string{firstTopic(ev.Topics)},
						EventName:   ev.EventName,
						UpdatedAt:   ev.UpdatedAt,
					}
				}
				if err := stream.Send(ev); err != nil {
					rows.Close()
					return err
				}
			}
			rows.Close()
		}
	}
}

func matchLogFilter(ev *pb.LogEvent, req *pb.SubscribeLogsRequest) bool {
	if req.ChainId != 0 && ev.ChainId != req.ChainId {
		return false
	}
	if normalizeAddress(req.ContractAddress) != "" && normalizeAddress(ev.ContractAddress) != normalizeAddress(req.ContractAddress) {
		return false
	}
	if strings.TrimSpace(req.Topic0) != "" && firstTopic(ev.Topics) != strings.TrimSpace(req.Topic0) {
		return false
	}
	return true
}

func unaryAuthLoggingInterceptor(apiKey string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		if err := authorize(ctx, apiKey); err != nil {
			return nil, err
		}
		resp, err := handler(ctx, req)
		log.Printf("[unary] method=%s duration=%s err=%v", info.FullMethod, time.Since(start), err)
		return resp, err
	}
}

func streamAuthLoggingInterceptor(apiKey string) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		if err := authorize(ss.Context(), apiKey); err != nil {
			return err
		}
		err := handler(srv, ss)
		log.Printf("[stream] method=%s duration=%s err=%v", info.FullMethod, time.Since(start), err)
		return err
	}
}

func authorize(ctx context.Context, apiKey string) error {
	apiKey = strings.TrimSpace(apiKey)
	md, _ := metadata.FromIncomingContext(ctx)
	got := ""
	if vals := md.Get("x-api-key"); len(vals) > 0 {
		got = vals[0]
	}
	got = strings.TrimSpace(got)
	if got == "" && apiKey == "" {
		return nil
	}
	if got != apiKey {
		return status.Error(codes.Unauthenticated, "invalid x-api-key")
	}
	if p, ok := peer.FromContext(ctx); ok {
		log.Printf("[auth] peer=%s", p.Addr.String())
	}
	return nil
}

func main() {
	addr := flag.String("addr", ":50051", "gRPC listen address")
	apiKey := flag.String("api-key", "web3-lite-key", "required x-api-key metadata")
	flag.Parse()
	*apiKey = strings.TrimSpace(*apiKey)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}

	var rpcClient *ethclient.Client
	var rpcChain uint64
	if rpcURL := strings.TrimSpace(os.Getenv("RPC_URL")); rpcURL != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		client, err := ethclient.DialContext(ctx, rpcURL)
		if err != nil {
			log.Printf("RPC_URL configured but dial failed, fallback to mock mode: %v", err)
		} else {
			chainID, cErr := client.ChainID(ctx)
			if cErr != nil {
				log.Printf("RPC chain id read failed, fallback to mock mode: %v", cErr)
				client.Close()
			} else {
				rpcClient = client
				rpcChain = chainID.Uint64()
				log.Printf("connected RPC_URL, chain_id=%d", rpcChain)
			}
		}
	}

	var mysqlDB *sql.DB
	{
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		db, err := initMySQL(ctx)
		if err != nil {
			log.Printf("mysql init failed, fallback to memory mode: %v", err)
		} else {
			mysqlDB = db
			log.Printf("mysql connected")
		}
	}

	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(unaryAuthLoggingInterceptor(*apiKey)),
		grpc.ChainStreamInterceptor(streamAuthLoggingInterceptor(*apiKey)),
	)
	svc := &ethTxService{
		rpcClient: rpcClient,
		rpcChain:  rpcChain,
		db:        mysqlDB,
		accounts:  map[string]*accountState{},
		localTx:   map[string]*pb.EthTransaction{},
		users:     map[string]*pb.User{},
		userAcc:   map[string]map[string]struct{}{},
		eventLogs: []*pb.LogEvent{},
	}
	if svc.db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		if err := svc.ensureSchema(ctx); err != nil {
			log.Printf("mysql schema init failed, fallback to memory mode: %v", err)
			svc.db.Close()
			svc.db = nil
		}
	}
	if svc.db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		if err := svc.seedDefaultAccountsDB(ctx); err != nil {
			log.Printf("mysql seed failed, fallback to memory mode: %v", err)
			svc.db.Close()
			svc.db = nil
		}
	}
	if svc.db == nil {
		svc.seedDefaultAccounts()
	}
	pb.RegisterEthTxServiceServer(s, svc)
	// 让 Postman / grpcurl 这类工具可以通过 reflection 发现服务
	reflection.Register(s)

	log.Printf("web3 gRPC server listening on %s", *addr)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
