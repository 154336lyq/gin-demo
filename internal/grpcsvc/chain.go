// Package grpcsvc 实现 protobuf 定义的 ChainQuery 服务（与 Gin 共用 eth.Backend）。
package grpcsvc

import (
	"context"

	"gin-demo/internal/eth"
	"gin-demo/pb/chainpb"
)

// ChainServer gRPC 侧查询入口。
type ChainServer struct {
	chainpb.UnimplementedChainQueryServer
	B *eth.Backend
}

func NewChainServer(b *eth.Backend) *ChainServer {
	return &ChainServer{B: b}
}

func (s *ChainServer) Ping(ctx context.Context, _ *chainpb.Empty) (*chainpb.Empty, error) {
	return &chainpb.Empty{}, ctx.Err()
}

func (s *ChainServer) GetLatestBlock(ctx context.Context, _ *chainpb.Empty) (*chainpb.BlockSummary, error) {
	blk, err := s.B.LatestBlock(ctx)
	if err != nil {
		return nil, err
	}
	return &chainpb.BlockSummary{
		Number:    blk.NumberU64(),
		Hash:      blk.Hash().Hex(),
		Timestamp: blk.Time(),
		TxCount:   uint32(len(blk.Transactions())),
	}, nil
}
