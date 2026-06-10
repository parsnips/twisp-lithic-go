package twisplithic

import (
	"context"

	corev1grpc "buf.build/gen/go/twisp/api/grpc/go/twisp/core/v1/corev1grpc"
	corev1 "buf.build/gen/go/twisp/api/protocolbuffers/go/twisp/core/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type TwispAPI interface {
	PostTransaction(context.Context, *corev1.PostTransactionRequest) (*corev1.PostTransactionResponse, error)
	ReadBalance(context.Context, *corev1.ReadBalanceRequest) (*corev1.ReadBalanceResponse, error)
	OpenAnyStream(context.Context) (AnySession, error)
}

type AnySession interface {
	Do(*corev1.AnyRequest) (*corev1.AnyResponse, error)
	Close() error
}

type GRPCClient struct {
	conn         *grpc.ClientConn
	accountID    string
	transactions corev1grpc.TransactionServiceClient
	tranCodes    corev1grpc.TranCodeServiceClient
	balances     corev1grpc.BalanceServiceClient
	any          corev1grpc.AnyServiceClient
}

func DialLocal(ctx context.Context, target, accountID string) (*GRPCClient, error) {
	conn, err := grpc.DialContext(ctx, target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &GRPCClient{
		conn:         conn,
		accountID:    accountID,
		transactions: corev1grpc.NewTransactionServiceClient(conn),
		tranCodes:    corev1grpc.NewTranCodeServiceClient(conn),
		balances:     corev1grpc.NewBalanceServiceClient(conn),
		any:          corev1grpc.NewAnyServiceClient(conn),
	}, nil
}

func (c *GRPCClient) Close() error {
	return c.conn.Close()
}

func (c *GRPCClient) context(ctx context.Context) context.Context {
	if c.accountID == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "x-twisp-account-id", c.accountID)
}

func (c *GRPCClient) PostTransaction(ctx context.Context, req *corev1.PostTransactionRequest) (*corev1.PostTransactionResponse, error) {
	return c.transactions.PostTransaction(c.context(ctx), req)
}

func (c *GRPCClient) VoidTransaction(ctx context.Context, req *corev1.VoidTransactionRequest) (*corev1.VoidTransactionResponse, error) {
	return c.transactions.VoidTransaction(c.context(ctx), req)
}

func (c *GRPCClient) ReadTransaction(ctx context.Context, req *corev1.ReadTransactionRequest) (*corev1.ReadTransactionResponse, error) {
	return c.transactions.ReadTransaction(c.context(ctx), req)
}

func (c *GRPCClient) ReadTranCode(ctx context.Context, req *corev1.ReadTranCodeRequest) (*corev1.ReadTranCodeResponse, error) {
	return c.tranCodes.ReadTranCode(c.context(ctx), req)
}

func (c *GRPCClient) ListTransactions(ctx context.Context, req *corev1.ListTransactionsRequest) (*corev1.ListTransactionsResponse, error) {
	return c.transactions.ListTransactions(c.context(ctx), req)
}

func (c *GRPCClient) ReadBalance(ctx context.Context, req *corev1.ReadBalanceRequest) (*corev1.ReadBalanceResponse, error) {
	return c.balances.ReadBalance(c.context(ctx), req)
}

func (c *GRPCClient) OpenAnyStream(ctx context.Context) (AnySession, error) {
	stream, err := c.any.AnyStream(c.context(ctx))
	if err != nil {
		return nil, err
	}
	return &grpcAnySession{stream: stream}, nil
}

type grpcAnySession struct {
	stream grpc.BidiStreamingClient[corev1.AnyRequest, corev1.AnyResponse]
}

func (s *grpcAnySession) Do(req *corev1.AnyRequest) (*corev1.AnyResponse, error) {
	if err := s.stream.Send(req); err != nil {
		return nil, err
	}
	return s.stream.Recv()
}

func (s *grpcAnySession) Close() error {
	return s.stream.CloseSend()
}
