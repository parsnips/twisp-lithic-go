package twisplithic

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	corev1 "buf.build/gen/go/twisp/api/protocolbuffers/go/twisp/core/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestProcessVoidsAndReplacesAuthorization(t *testing.T) {
	accountID := uuid.New()
	journalID := uuid.New()
	settlementID := uuid.New()
	correlationID := uuid.New()
	authID := uuid.New()
	adviceID := uuid.New()

	authHook := Webhook{
		Amount:              1000,
		Token:               correlationID.String(),
		Status:              "PENDING",
		Created:             "2026-06-09T10:00:00Z",
		AuthorizationAmount: 1000,
		Events: []WebhookEvent{
			{Amount: 1000, Created: "2026-06-09T10:00:00Z", Type: "AUTHORIZATION", Token: authID.String()},
		},
	}
	adviceHook := Webhook{
		Amount:              2000,
		Token:               correlationID.String(),
		Status:              "PENDING",
		Created:             "2026-06-09T10:02:00Z",
		AuthorizationAmount: 2000,
		Events: []WebhookEvent{
			{Amount: 1000, Created: "2026-06-09T10:00:00Z", Type: "AUTHORIZATION", Token: authID.String()},
			{Amount: 2000, Created: "2026-06-09T10:02:00Z", Type: "AUTHORIZATION_ADVICE", Token: adviceID.String()},
		},
	}
	api := newFakeTwispAPI()
	processor := NewProcessor(api)
	_, err := processor.Process(context.Background(), lithicReq(accountID, journalID, settlementID, &authHook))
	require.NoError(t, err)

	result, err := processor.Process(context.Background(), lithicReq(accountID, journalID, settlementID, &adviceHook))
	require.NoError(t, err)
	require.NotNil(t, result.Transaction)
	require.Len(t, result.Voids, 1)

	require.Len(t, api.posts, 3)
	require.Equal(t, TranCodeCardHold, api.posts[0].TranCode)
	require.Equal(t, TranCodeCardHold, api.posts[1].TranCode)
	require.Equal(t, TranCodeCardHold, api.posts[2].TranCode)
	require.True(t, api.posts[0].GetProperties().GetIdempotent())
	require.True(t, api.posts[1].GetProperties().GetIdempotent())
	require.True(t, api.posts[2].GetProperties().GetIdempotent())
	requireVelocityWarn(t, api.posts[0])
	requireVelocityWarn(t, api.posts[1])
	requireVelocityWarn(t, api.posts[2])
	require.Equal(t, "0.00", api.posts[0].Params["amount"])
	require.Equal(t, "10.00", api.posts[1].Params["amount"])
	require.Equal(t, "20.00", api.posts[2].Params["amount"])
	require.Equal(t, debitOrCreditString(corev1.DebitOrCredit_DEBIT_OR_CREDIT_DEBIT_UNSPECIFIED), api.posts[2].Params["direction"])

	require.Len(t, api.voids, 2)
	require.Equal(t, authID, uuidFromProto(api.voids[1].GetTransactionId()))
	require.True(t, api.voids[1].GetProperties().GetIdempotent())
	require.Equal(t, 5, api.anyStreams)
	require.Equal(t, 3, api.postTransactionBatches)
}

func TestProcessReplaysExistingTransactionThroughIdempotentPost(t *testing.T) {
	accountID := uuid.New()
	journalID := uuid.New()
	settlementID := uuid.New()
	txID := uuid.New()
	hook := Webhook{
		Token:               txID.String(),
		Status:              "AUTHORIZATION",
		Created:             "2026-06-09T10:00:00Z",
		AuthorizationAmount: 1000,
	}
	api := newFakeTwispAPI()
	processor := NewProcessor(api)
	first, err := processor.Process(context.Background(), lithicReq(accountID, journalID, settlementID, &hook))
	require.NoError(t, err)
	require.NotNil(t, first.Transaction)

	replayed, err := processor.Process(context.Background(), lithicReq(accountID, journalID, settlementID, &hook))
	require.NoError(t, err)
	require.Equal(t, txID, uuidFromProto(replayed.Transaction.GetTransaction().GetTransactionId()))
	require.Len(t, api.posts, 1)
	require.True(t, api.posts[0].GetProperties().GetIdempotent())
	requireNoVelocityOverride(t, api.posts[0])
}

func TestProcessBackfillASAOverridesVelocityToWarn(t *testing.T) {
	accountID := uuid.New()
	journalID := uuid.New()
	settlementID := uuid.New()
	correlationID := uuid.New()
	authID := uuid.New()
	hook := Webhook{
		Token:               correlationID.String(),
		Status:              "PENDING",
		Created:             "2026-06-09T10:00:00Z",
		AuthorizationAmount: 1000,
		Events: []WebhookEvent{
			{Amount: 1000, Created: "2026-06-09T10:00:00Z", Type: "AUTHORIZATION", Token: authID.String()},
		},
	}

	api := newFakeTwispAPI()
	processor := NewProcessor(api)
	_, err := processor.Process(context.Background(), lithicReq(accountID, journalID, settlementID, &hook))
	require.NoError(t, err)

	require.Len(t, api.posts, 2)
	require.Equal(t, "0.00", api.posts[0].Params["amount"])
	requireVelocityWarn(t, api.posts[0])
	requireVelocityWarn(t, api.posts[1])
}

func TestProcessBalanceInquiryOverridesVelocityToWarn(t *testing.T) {
	accountID := uuid.New()
	journalID := uuid.New()
	settlementID := uuid.New()
	txID := uuid.New()
	hook := Webhook{
		Token:               txID.String(),
		Status:              "BALANCE_INQUIRY",
		Created:             "2026-06-09T10:00:00Z",
		AuthorizationAmount: 0,
	}

	api := newFakeTwispAPI()
	processor := NewProcessor(api)
	_, err := processor.Process(context.Background(), lithicReq(accountID, journalID, settlementID, &hook))
	require.NoError(t, err)

	require.Len(t, api.posts, 1)
	requireVelocityWarn(t, api.posts[0])
}

func TestBackfillUsesFutureWebhookTransactionIDs(t *testing.T) {
	correlationID := uuid.New()
	authID := uuid.New()
	clearingID := uuid.New()
	hook := Webhook{
		Token:               correlationID.String(),
		Status:              "SETTLED",
		Created:             "2026-06-09T10:00:00Z",
		AuthorizationAmount: 1000,
		SettledAmount:       1000,
		Events: []WebhookEvent{
			{Amount: 1000, Created: "2026-06-09T10:00:00Z", Type: "AUTHORIZATION", Token: authID.String()},
			{Amount: 1000, Created: "2026-06-09T10:02:00Z", Type: "CLEARING", Token: clearingID.String()},
		},
	}

	backfills := hook.BackfillWebhooks()
	require.Len(t, backfills, 2)

	firstID, err := backfills[0].TransactionID()
	require.NoError(t, err)
	require.Equal(t, correlationID, firstID)
	require.Empty(t, backfills[0].Events)
	require.Zero(t, backfills[0].AuthorizationAmount)

	secondID, err := backfills[1].TransactionID()
	require.NoError(t, err)
	require.Equal(t, authID, secondID)
	require.Len(t, backfills[1].Events, 1)

	currentID, err := hook.TransactionID()
	require.NoError(t, err)
	require.Equal(t, clearingID, currentID)
}

func TestHTTPHandlerAcceptsLithicRequest(t *testing.T) {
	accountID := uuid.New()
	journalID := uuid.New()
	settlementID := uuid.New()
	txID := uuid.New()
	hook := Webhook{
		Token:               txID.String(),
		Status:              "AUTHORIZATION",
		Created:             "2026-06-09T10:00:00Z",
		AuthorizationAmount: 1000,
	}
	body, err := json.Marshal(lithicReq(accountID, journalID, settlementID, &hook))
	require.NoError(t, err)

	server := httptest.NewServer(NewHandler(NewProcessor(newFakeTwispAPI())))
	defer server.Close()

	resp, err := http.Post(server.URL, "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func lithicReq(accountID, journalID, settlementID uuid.UUID, webhook *Webhook) LithicRequest {
	return LithicRequest{
		AccountID:           accountID,
		JournalID:           journalID,
		SettlementAccountID: settlementID,
		Webhook:             webhook,
	}
}

func requireVelocityWarn(t *testing.T, req *corev1.PostTransactionRequest) {
	t.Helper()
	require.NotNil(t, req.GetProperties().GetOverrideVelocityEnforcement())
	require.Equal(
		t,
		corev1.VelocityEnforcementAction_VELOCITY_ENFORCEMENT_ACTION_WARN_UNSPECIFIED,
		req.GetProperties().GetOverrideVelocityEnforcement().GetAction(),
	)
}

func requireNoVelocityOverride(t *testing.T, req *corev1.PostTransactionRequest) {
	t.Helper()
	require.Nil(t, req.GetProperties().GetOverrideVelocityEnforcement())
}

type fakeTwispAPI struct {
	posts                  []*corev1.PostTransactionRequest
	voids                  []*corev1.VoidTransactionRequest
	transactions           map[uuid.UUID]*corev1.Transaction
	tranCodeIDs            map[string]uuid.UUID
	tranCodes              map[uuid.UUID]string
	voidSeq                int
	anyStreams             int
	postTransactionBatches int
}

func newFakeTwispAPI() *fakeTwispAPI {
	f := &fakeTwispAPI{
		transactions: map[uuid.UUID]*corev1.Transaction{},
		tranCodeIDs:  map[string]uuid.UUID{},
		tranCodes:    map[uuid.UUID]string{},
	}
	for _, code := range []string{
		TranCodeCardHold,
		TranCodeCardHoldReplace,
		TranCodeCardSettle,
		TranCodeCardBalanceInquiry,
		TranCodeCardDecline,
	} {
		id := derivedTransactionID(uuid.NameSpaceOID, code)
		f.tranCodeIDs[code] = id
		f.tranCodes[id] = code
	}
	return f
}

func (f *fakeTwispAPI) PostTransaction(_ context.Context, req *corev1.PostTransactionRequest) (*corev1.PostTransactionResponse, error) {
	txID := uuidFromProto(req.GetTransactionId())
	if tx, ok := f.transactions[txID]; ok && req.GetProperties().GetIdempotent() {
		return &corev1.PostTransactionResponse{Transaction: tx}, nil
	}

	f.posts = append(f.posts, req)
	tranCodeID := f.tranCodeIDs[req.GetTranCode()]
	tx := &corev1.Transaction{
		TransactionId: req.GetTransactionId(),
		TranCodeId:    newUUID(tranCodeID),
		JournalId:     newUUID(uuid.MustParse(req.Params["journal"])),
		CorrelationId: req.Params["correlation"],
	}
	f.transactions[txID] = tx
	return &corev1.PostTransactionResponse{
		Transaction: tx,
	}, nil
}

func (f *fakeTwispAPI) VoidTransaction(_ context.Context, req *corev1.VoidTransactionRequest) (*corev1.VoidTransactionResponse, error) {
	f.voids = append(f.voids, req)
	txID := uuidFromProto(req.GetTransactionId())
	tx := f.transactions[txID]
	if tx == nil {
		return &corev1.VoidTransactionResponse{}, nil
	}
	if tx.GetVoidedBy() == nil {
		f.voidSeq++
		tx.VoidedBy = newUUID(derivedTransactionID(txID, "void"))
	}
	return &corev1.VoidTransactionResponse{Transaction: tx}, nil
}

func (f *fakeTwispAPI) postTransactions(ctx context.Context, req *corev1.PostTransactionsRequest) (*corev1.PostTransactionsResponse, error) {
	f.postTransactionBatches++
	resp := &corev1.PostTransactionsResponse{}
	for _, operation := range req.GetOperations() {
		if postReq := operation.GetPost(); postReq != nil {
			postResp, err := f.PostTransaction(ctx, postReq)
			if err != nil {
				return nil, err
			}
			op := &corev1.PostTransactionsResponseOperation{}
			op.SetPost(postResp)
			resp.Responses = append(resp.Responses, op)
			continue
		}
		if voidReq := operation.GetVoid(); voidReq != nil {
			voidResp, err := f.VoidTransaction(ctx, voidReq)
			if err != nil {
				return nil, err
			}
			op := &corev1.PostTransactionsResponseOperation{}
			op.SetVoid(voidResp)
			resp.Responses = append(resp.Responses, op)
		}
	}
	return resp, nil
}

func (f *fakeTwispAPI) ReadTransaction(_ context.Context, req *corev1.ReadTransactionRequest) (*corev1.ReadTransactionResponse, error) {
	txID := uuidFromProto(req.GetTransactionId())
	if tx, ok := f.transactions[txID]; ok {
		return &corev1.ReadTransactionResponse{Transaction: tx}, nil
	}
	return nil, status.Error(codes.NotFound, "not found")
}

func (f *fakeTwispAPI) ReadTranCode(_ context.Context, req *corev1.ReadTranCodeRequest) (*corev1.ReadTranCodeResponse, error) {
	code := f.tranCodes[uuidFromProto(req.GetTranCodeId())]
	if code == "" {
		return nil, status.Error(codes.NotFound, "not found")
	}
	return &corev1.ReadTranCodeResponse{
		TranCode: &corev1.TranCode{
			TranCodeId: req.GetTranCodeId(),
			Code:       code,
		},
	}, nil
}

func (f *fakeTwispAPI) ListTransactions(_ context.Context, req *corev1.ListTransactionsRequest) (*corev1.ListTransactionsResponse, error) {
	correlationID := req.GetWhere().GetCorrelationId().GetEq()
	edges := []*corev1.ListTransactionsResponse_Edge{}
	for _, tx := range f.transactions {
		if tx.GetCorrelationId() != correlationID {
			continue
		}
		edges = append(edges, &corev1.ListTransactionsResponse_Edge{Node: &corev1.Transaction{TransactionId: tx.GetTransactionId()}})
	}
	return &corev1.ListTransactionsResponse{PageInfo: &corev1.PageInfo{}, Edges: edges}, nil
}

func (f *fakeTwispAPI) ReadBalance(context.Context, *corev1.ReadBalanceRequest) (*corev1.ReadBalanceResponse, error) {
	return &corev1.ReadBalanceResponse{Balance: &corev1.Balance{}}, nil
}

func (f *fakeTwispAPI) OpenAnyStream(ctx context.Context) (AnySession, error) {
	f.anyStreams++
	return &fakeAnySession{ctx: ctx, api: f}, nil
}

type fakeAnySession struct {
	ctx  context.Context
	api  *fakeTwispAPI
	open bool
}

func (s *fakeAnySession) Do(req *corev1.AnyRequest) (*corev1.AnyResponse, error) {
	resp := &corev1.AnyResponse{}
	for _, operation := range req.GetOperations() {
		response, err := s.doOperation(operation)
		if err != nil {
			return nil, err
		}
		resp.Operations = append(resp.Operations, response)
	}
	return resp, nil
}

func (s *fakeAnySession) Close() error {
	s.open = false
	return nil
}

func (s *fakeAnySession) doOperation(operation *corev1.AnyRequestOperation) (*corev1.AnyResponseOperation, error) {
	switch {
	case operation.GetBeginTransaction() != nil:
		if s.open {
			return nil, status.Error(codes.FailedPrecondition, "transaction already open")
		}
		s.open = true
		return &corev1.AnyResponseOperation{BeginTransaction: &corev1.BeginTransactionResponse{}}, nil
	case operation.GetCommitTransaction() != nil:
		if !s.open {
			return nil, status.Error(codes.FailedPrecondition, "transaction is not open")
		}
		s.open = false
		return &corev1.AnyResponseOperation{CommitTransaction: &corev1.CommitTransactionResponse{}}, nil
	case operation.GetRollbackTransaction() != nil:
		if !s.open {
			return nil, status.Error(codes.FailedPrecondition, "transaction is not open")
		}
		s.open = false
		return &corev1.AnyResponseOperation{RollbackTransaction: &corev1.RollbackTransactionResponse{}}, nil
	case operation.GetListTransactions() != nil:
		if !s.open {
			return nil, status.Error(codes.FailedPrecondition, "transaction is not open")
		}
		resp, err := s.api.ListTransactions(s.ctx, operation.GetListTransactions())
		if err != nil {
			return nil, err
		}
		return &corev1.AnyResponseOperation{ListTransactions: resp}, nil
	case operation.GetReadTransaction() != nil:
		if !s.open {
			return nil, status.Error(codes.FailedPrecondition, "transaction is not open")
		}
		resp, err := s.api.ReadTransaction(s.ctx, operation.GetReadTransaction())
		if err != nil {
			return nil, err
		}
		return &corev1.AnyResponseOperation{ReadTransaction: resp}, nil
	case operation.GetReadTranCode() != nil:
		if !s.open {
			return nil, status.Error(codes.FailedPrecondition, "transaction is not open")
		}
		resp, err := s.api.ReadTranCode(s.ctx, operation.GetReadTranCode())
		if err != nil {
			return nil, err
		}
		return &corev1.AnyResponseOperation{ReadTranCode: resp}, nil
	case operation.GetPostTransactions() != nil:
		if !s.open {
			return nil, status.Error(codes.FailedPrecondition, "transaction is not open")
		}
		resp, err := s.api.postTransactions(s.ctx, operation.GetPostTransactions())
		if err != nil {
			return nil, err
		}
		return &corev1.AnyResponseOperation{PostTransactions: resp}, nil
	case operation.GetReadBalance() != nil:
		if !s.open {
			return nil, status.Error(codes.FailedPrecondition, "transaction is not open")
		}
		resp, err := s.api.ReadBalance(s.ctx, operation.GetReadBalance())
		if err != nil {
			return nil, err
		}
		return &corev1.AnyResponseOperation{ReadBalance: resp}, nil
	default:
		return nil, status.Error(codes.Unimplemented, "unsupported Any operation")
	}
}
