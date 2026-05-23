package main

import (
	"context"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"gin-demo/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "gRPC server address")
	apiKey := flag.String("api-key", "web3-lite-key", "x-api-key metadata")
	chainID := flag.Uint64("chain-id", 1, "chain id")
	workers := flag.Int("workers", 4, "concurrent detail fetch workers")
	pollInterval := flag.Duration("poll-interval", 4*time.Second, "detail refresh interval per tx")
	maxPolls := flag.Int("max-polls", 30, "max detail polls per tx")
	includeFailed := flag.Bool("include-failed", true, "include failed txs in subscription")
	filterAddress := flag.String("address", "", "optional from/to address filter")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer conn.Close()

	client := pb.NewEthTxServiceClient(conn)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	hashCh := make(chan string, 256)
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case hash, ok := <-hashCh:
					if !ok {
						return
					}
					var lastState pb.TxState
					var lastConf uint64
					for attempt := 1; attempt <= *maxPolls; attempt++ {
						reqCtx, reqCancel := context.WithTimeout(withAPIKey(ctx, *apiKey), 6*time.Second)
						tx, err := client.GetTransaction(reqCtx, &pb.TxHashRequest{
							Hash:    hash,
							ChainId: *chainID,
						})
						reqCancel()
						if err != nil {
							log.Printf("[worker-%d] GetTransaction hash=%s attempt=%d err=%v", workerID, hash, attempt, err)
						} else {
							changed := attempt == 1 || tx.State != lastState || tx.Confirmations != lastConf
							if changed {
								log.Printf("[worker-%d] track hash=%s attempt=%d state=%s conf=%d block=%d from=%s to=%s value=%s",
									workerID, tx.Hash, attempt, tx.State.String(), tx.Confirmations, tx.BlockNumber, tx.From, tx.To, tx.Value)
							}
							lastState = tx.State
							lastConf = tx.Confirmations
							if isTerminal(tx.State) {
								log.Printf("[worker-%d] hash=%s reached terminal state=%s", workerID, tx.Hash, tx.State.String())
								break
							}
						}

						if attempt == *maxPolls {
							log.Printf("[worker-%d] hash=%s stop tracking after max-polls=%d", workerID, hash, *maxPolls)
							break
						}
						select {
						case <-ctx.Done():
							return
						case <-time.After(*pollInterval):
						}
					}
				}
			}
		}(i + 1)
	}

	subCtx := withAPIKey(ctx, *apiKey)
	stream, err := client.SubscribeTransactions(subCtx, &pb.SubscribeRequest{
		ChainId:       *chainID,
		Address:       *filterAddress,
		IncludeFailed: *includeFailed,
		Lightweight:   true,
	})
	if err != nil {
		log.Fatalf("SubscribeTransactions: %v", err)
	}
	log.Printf("subscribed lightweight stream addr=%s chain_id=%d", *addr, *chainID)

	seen := map[string]struct{}{}
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("stream recv error: %v", err)
			break
		}
		log.Printf("[stream] hash=%s state=%s block=%d conf=%d updated_at=%s",
			msg.Hash, msg.State.String(), msg.BlockNumber, msg.Confirmations, msg.UpdatedAt)

		if msg.Hash == "" {
			continue
		}
		if _, ok := seen[msg.Hash]; ok {
			continue
		}
		seen[msg.Hash] = struct{}{}

		select {
		case hashCh <- msg.Hash:
		case <-ctx.Done():
			close(hashCh)
			wg.Wait()
			return
		}
	}

	close(hashCh)
	wg.Wait()
}

func withAPIKey(ctx context.Context, apiKey string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "x-api-key", apiKey)
}

func isTerminal(state pb.TxState) bool {
	return state == pb.TxState_TX_STATE_FINALIZED || state == pb.TxState_TX_STATE_FAILED
}

