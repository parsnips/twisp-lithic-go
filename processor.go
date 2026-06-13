package twisplithic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	corev1 "buf.build/gen/go/twisp/api/protocolbuffers/go/twisp/core/v1"
	typev1 "buf.build/gen/go/twisp/api/protocolbuffers/go/twisp/type/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	TranCodeCardHold           = "SYS_CARD_HOLD"
	TranCodeCardHoldReplace    = "SYS_CARD_HOLD_REPLACE"
	TranCodeCardSettle         = "SYS_CARD_SETTLE"
	TranCodeCardBalanceInquiry = "SYS_CARD_BALANCE_INQUIRY"
	TranCodeCardDecline        = "SYS_CARD_DECLINE"
)

type Processor struct {
	twisp         TwispAPI
	tranCodeMu    sync.Mutex
	tranCodeCache map[uuid.UUID]string
}

func NewProcessor(twisp TwispAPI) *Processor {
	return &Processor{
		twisp:         twisp,
		tranCodeCache: make(map[uuid.UUID]string),
	}
}

type Result struct {
	Transaction *corev1.PostTransactionResponse   `json:"transaction,omitempty"`
	Voids       []*corev1.VoidTransactionResponse `json:"voids,omitempty"`
	Balance     *corev1.ReadBalanceResponse       `json:"balance,omitempty"`
}

type LithicRequest struct {
	AccountID           uuid.UUID `json:"accountId"`
	JournalID           uuid.UUID `json:"journalId"`
	SettlementAccountID uuid.UUID `json:"settlementAccountId"`
	Webhook             *Webhook  `json:"webhook"`
}

func (p *Processor) Process(ctx context.Context, input LithicRequest) (*Result, error) {
	return p.process(ctx, input, false)
}

func (p *Processor) process(ctx context.Context, input LithicRequest, isBackfill bool) (*Result, error) {
	if input.Webhook == nil {
		return nil, errors.New("webhook is required")
	}

	if !isBackfill {
		for _, hook := range input.Webhook.BackfillWebhooks() {
			hook := hook
			_, err := p.process(ctx, LithicRequest{
				AccountID:           input.AccountID,
				JournalID:           input.JournalID,
				SettlementAccountID: input.SettlementAccountID,
				Webhook:             &hook,
			}, true)
			if err != nil {
				return nil, err
			}
		}
	}

	hookBytes, err := json.Marshal(input.Webhook)
	if err != nil {
		return nil, err
	}

	correlationID, err := input.Webhook.CorrelationID()
	if err != nil {
		return nil, err
	}

	transactionID, err := input.Webhook.TransactionID()
	if err != nil {
		return nil, err
	}

	amount, err := input.Webhook.AmountAsMoney()
	if err != nil {
		return nil, err
	}

	direction := corev1.DebitOrCredit_DEBIT_OR_CREDIT_DEBIT_UNSPECIFIED
	if input.Webhook.IsCredit() {
		direction = corev1.DebitOrCredit_DEBIT_OR_CREDIT_CREDIT
	}

	params := map[string]string{
		"account":           input.AccountID.String(),
		"settlementAccount": input.SettlementAccountID.String(),
		"journal":           input.JournalID.String(),
		"amount":            amount.Number(),
		"currency":          amount.CurrencyCode(),
		"correlation":       correlationID.String(),
		"effective":         input.Webhook.Created[:10],
		"direction":         debitOrCreditString(direction),
		"metadata":          string(hookBytes),
	}

	if input.Webhook.IsBalanceInquiry() {
		return p.postTransaction(ctx, input, transactionID, TranCodeCardBalanceInquiry, params, true)
	}
	if input.Webhook.IsTransactionASA() {
		return p.postTransaction(ctx, input, transactionID, TranCodeCardHold, params, isBackfill)
	}

	switch {
	case input.Webhook.IsTransactionDecline():
		return p.voidActiveAuthorization(ctx, input, params, transactionID)
	case input.Webhook.IsAuthOrAdvice():
		return p.replaceActiveAuthorization(ctx, input, params, transactionID)
	case input.Webhook.IsVoid():
		return p.replaceActiveAuthorization(ctx, input, params, transactionID)
	case input.Webhook.IsClearing():
		return p.clearAndReplaceAuthorization(ctx, input, params, transactionID)
	case input.Webhook.IsReturn():
		if input.Webhook.IsReturnOfCreditAuthorization() {
			return p.clearAndReplaceAuthorization(ctx, input, params, transactionID)
		}
		return p.postTransaction(ctx, input, transactionID, TranCodeCardSettle, params, true)
	case input.Webhook.IsFinancialAuthorization():
		return p.postTransaction(ctx, input, transactionID, TranCodeCardSettle, params, true)
	case input.Webhook.IsCreditAuthorization():
		return p.replaceActiveAuthorization(ctx, input, params, transactionID)
	case input.Webhook.IsFinancialCreditAuthorization():
		return p.postTransaction(ctx, input, transactionID, TranCodeCardSettle, params, true)
	default:
		return nil, fmt.Errorf("unsupported event type")
	}
}

func (p *Processor) voidActiveAuthorization(ctx context.Context, input LithicRequest, params map[string]string, transactionID uuid.UUID) (*Result, error) {
	return p.processAuthorizationMutation(ctx, input, transactionID, func(activeAuths []*corev1.Transaction) ([]*corev1.PostTransactionsOperation, error) {
		operations := voidOperations(activeAuths, params["metadata"])
		operations = append(operations, postOperation(transactionID, TranCodeCardDecline, markerParams(params), true))
		return operations, nil
	})
}

func (p *Processor) replaceActiveAuthorization(ctx context.Context, input LithicRequest, params map[string]string, transactionID uuid.UUID) (*Result, error) {
	remaining, isCredit, err := input.Webhook.RemainingAuthorizationAmountAsMoney()
	if err != nil {
		return nil, err
	}

	return p.processAuthorizationMutation(ctx, input, transactionID, func(activeAuths []*corev1.Transaction) ([]*corev1.PostTransactionsOperation, error) {
		operations := voidOperations(activeAuths, params["metadata"])
		if remaining.IsZero() {
			if input.Webhook.IsVoid() {
				operations = append(operations, postOperation(transactionID, TranCodeCardDecline, markerParams(params), true))
			}
			return operations, nil
		}

		holdParams := copyParams(params)
		applyAmount(holdParams, remaining, isCredit)
		operations = append(operations, postOperation(transactionID, TranCodeCardHold, holdParams, true))
		return operations, nil
	})
}

func (p *Processor) clearAndReplaceAuthorization(ctx context.Context, input LithicRequest, params map[string]string, transactionID uuid.UUID) (*Result, error) {
	remaining, isCredit, err := input.Webhook.RemainingAuthorizationAmountAsMoney()
	if err != nil {
		return nil, err
	}

	return p.processAuthorizationMutation(ctx, input, transactionID, func(activeAuths []*corev1.Transaction) ([]*corev1.PostTransactionsOperation, error) {
		operations := voidOperations(activeAuths, params["metadata"])
		operations = append(operations, postOperation(transactionID, TranCodeCardSettle, params, true))
		if remaining.IsZero() {
			return operations, nil
		}

		holdParams := copyParams(params)
		applyAmount(holdParams, remaining, isCredit)
		holdID := derivedTransactionID(transactionID, "remaining-authorization")
		operations = append(operations, postOperation(holdID, TranCodeCardHold, holdParams, true))
		return operations, nil
	})
}

func (p *Processor) postTransaction(ctx context.Context, input LithicRequest, transactionID uuid.UUID, tranCode string, params map[string]string, overrideVelocity bool) (*Result, error) {
	result := &Result{}
	err := p.inTransaction(ctx, func(session AnySession) error {
		existing, exists, err := p.transactionAlreadyProcessedInSession(session, input, transactionID)
		if err != nil {
			return err
		}

		journalID := newUUID(input.JournalID)
		if exists {
			result.Transaction = &corev1.PostTransactionResponse{Transaction: existing}
		} else {
			resp, err := p.postTransactionsInSession(session, []*corev1.PostTransactionsOperation{
				postOperation(transactionID, tranCode, params, overrideVelocity),
			})
			if err != nil {
				return err
			}
			journalID = applyPostTransactionsResponse(result, resp, journalID)
		}

		balanceResp, err := p.readBalanceInSession(session, &corev1.ReadBalanceRequest{
			JournalId: journalID,
			AccountId: newUUID(input.AccountID),
			Currency:  "USD",
		})
		if err != nil {
			return err
		}
		result.Balance = balanceResp
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (p *Processor) transactionAlreadyProcessedInSession(session AnySession, input LithicRequest, transactionID uuid.UUID) (*corev1.Transaction, bool, error) {
	transactions, err := p.listCorrelatedTransactionsInSession(session, input)
	if err != nil {
		return nil, false, err
	}
	fullTransactions, err := p.readTransactionsInSession(session, transactions.GetEdges())
	if err != nil {
		return nil, false, err
	}

	var existing *corev1.Transaction
	for _, tx := range fullTransactions {
		if uuidFromProto(tx.GetTransactionId()) != transactionID {
			continue
		}
		existing = tx
		break
	}
	superseded := existing == nil && webhookSupersededByTransactions(fullTransactions, input.Webhook)
	return existing, existing != nil || superseded, nil
}

func (p *Processor) processAuthorizationMutation(ctx context.Context, input LithicRequest, transactionID uuid.UUID, build func([]*corev1.Transaction) ([]*corev1.PostTransactionsOperation, error)) (*Result, error) {
	result := &Result{}
	err := p.inTransaction(ctx, func(session AnySession) error {
		activeAuths, transactions, err := p.activeAuthorizationTransactionsInSession(session, input)
		if err != nil {
			return err
		}

		journalID := newUUID(input.JournalID)
		if !transactionExists(transactions, transactionID) && !webhookSupersededByTransactions(transactions, input.Webhook) {
			operations, err := build(activeAuths)
			if err != nil {
				return err
			}
			if len(operations) > 0 {
				resp, err := p.postTransactionsInSession(session, operations)
				if err != nil {
					return err
				}
				journalID = applyPostTransactionsResponse(result, resp, journalID)
			}
		}

		balanceResp, err := p.readBalanceInSession(session, &corev1.ReadBalanceRequest{
			JournalId: journalID,
			AccountId: newUUID(input.AccountID),
			Currency:  "USD",
		})
		if err != nil {
			return err
		}
		result.Balance = balanceResp
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// inTransaction opens an Any stream, begins an interactive transaction, runs fn
// against the open session, and commits. If fn returns an error the transaction
// is rolled back. The stream is always closed. Routing reads and writes through
// a single committed transaction gives callers a consistent snapshot — e.g. a
// balance read after a post reflects exactly that post and nothing concurrent.
func (p *Processor) inTransaction(ctx context.Context, fn func(AnySession) error) error {
	session, err := p.twisp.OpenAnyStream(ctx)
	if err != nil {
		return err
	}
	defer session.Close()

	if err := p.beginTransaction(session); err != nil {
		return err
	}

	committed := false
	defer func() {
		if !committed {
			_, _ = session.Do(anyRequest(&corev1.AnyRequestOperation{RollbackTransaction: &corev1.RollbackTransactionRequest{}}))
		}
	}()

	if err := fn(session); err != nil {
		return err
	}

	if err := p.commitTransaction(session); err != nil {
		return err
	}
	committed = true
	return nil
}

func (p *Processor) beginTransaction(session AnySession) error {
	resp, err := session.Do(anyRequest(&corev1.AnyRequestOperation{BeginTransaction: &corev1.BeginTransactionRequest{}}))
	if err != nil {
		return err
	}
	op, err := singleAnyOperation(resp)
	if err != nil {
		return err
	}
	if op.GetBeginTransaction() == nil {
		return fmt.Errorf("expected BeginTransaction response")
	}
	return nil
}

func (p *Processor) commitTransaction(session AnySession) error {
	resp, err := session.Do(anyRequest(&corev1.AnyRequestOperation{CommitTransaction: &corev1.CommitTransactionRequest{}}))
	if err != nil {
		return err
	}
	op, err := singleAnyOperation(resp)
	if err != nil {
		return err
	}
	if op.GetCommitTransaction() == nil {
		return fmt.Errorf("expected CommitTransaction response")
	}
	return nil
}

func (p *Processor) postTransactionsInSession(session AnySession, operations []*corev1.PostTransactionsOperation) (*corev1.PostTransactionsResponse, error) {
	resp, err := session.Do(anyRequest(&corev1.AnyRequestOperation{
		PostTransactions: &corev1.PostTransactionsRequest{Operations: operations},
	}))
	if err != nil {
		return nil, err
	}
	op, err := singleAnyOperation(resp)
	if err != nil {
		return nil, err
	}
	if op.GetPostTransactions() == nil {
		return nil, fmt.Errorf("expected PostTransactions response")
	}
	return op.GetPostTransactions(), nil
}

func (p *Processor) readBalanceInSession(session AnySession, req *corev1.ReadBalanceRequest) (*corev1.ReadBalanceResponse, error) {
	resp, err := session.Do(anyRequest(&corev1.AnyRequestOperation{ReadBalance: req}))
	if err != nil {
		return nil, err
	}
	op, err := singleAnyOperation(resp)
	if err != nil {
		return nil, err
	}
	if op.GetReadBalance() == nil {
		return nil, fmt.Errorf("expected ReadBalance response")
	}
	return op.GetReadBalance(), nil
}

func (p *Processor) activeAuthorizationTransactionsInSession(session AnySession, input LithicRequest) ([]*corev1.Transaction, []*corev1.Transaction, error) {
	transactions, err := p.listCorrelatedTransactionsInSession(session, input)
	if err != nil {
		return nil, nil, err
	}

	fullTransactions, err := p.readTransactionsInSession(session, transactions.GetEdges())
	if err != nil {
		return nil, nil, err
	}
	if err := p.cacheTranCodesInSession(session, fullTransactions); err != nil {
		return nil, nil, err
	}

	active := make([]*corev1.Transaction, 0, len(fullTransactions))
	for _, tx := range fullTransactions {
		if tx.GetVoidedBy() != nil || tx.GetVoidOf() != nil {
			continue
		}
		code, ok := p.cachedTranCode(tx.GetTranCodeId())
		if !ok {
			return nil, nil, fmt.Errorf("tran code %s was not loaded", uuidString(tx.GetTranCodeId()))
		}
		if isAuthorizationTranCode(code) {
			active = append(active, tx)
		}
	}
	return active, fullTransactions, nil
}

func (p *Processor) listCorrelatedTransactionsInSession(session AnySession, input LithicRequest) (*corev1.ListTransactionsResponse, error) {
	correlationID, err := input.Webhook.CorrelationID()
	if err != nil {
		return nil, err
	}
	resp, err := session.Do(anyRequest(&corev1.AnyRequestOperation{
		ListTransactions: &corev1.ListTransactionsRequest{
			Index: corev1.ListTransactionsRequest_INDEX_CORRELATION_ID,
			Where: &corev1.ListTransactionsRequest_Filters{
				JournalId: &corev1.FilterValue{Value: &corev1.FilterValue_Eq{Eq: input.JournalID.String()}},
				CorrelationId: &corev1.FilterValue{
					Value: &corev1.FilterValue_Eq{Eq: correlationID.String()},
				},
			},
			Paging: &corev1.Paginate{First: 100},
		},
	}))
	if err != nil {
		return nil, err
	}
	listOp, err := singleAnyOperation(resp)
	if err != nil {
		return nil, err
	}
	transactions := listOp.GetListTransactions()
	if transactions == nil {
		return nil, fmt.Errorf("expected ListTransactions response")
	}
	if transactions.GetPageInfo().GetHasNextPage() {
		return nil, fmt.Errorf("more than 100 correlated transactions")
	}
	return transactions, nil
}

func (p *Processor) readTransactionsInSession(session AnySession, edges []*corev1.ListTransactionsResponse_Edge) ([]*corev1.Transaction, error) {
	ops := make([]*corev1.AnyRequestOperation, 0, len(edges))
	for _, edge := range edges {
		txID := edge.GetNode().GetTransactionId()
		if txID == nil {
			continue
		}
		ops = append(ops, &corev1.AnyRequestOperation{
			ReadTransaction: &corev1.ReadTransactionRequest{TransactionId: txID},
		})
	}
	if len(ops) == 0 {
		return nil, nil
	}

	resp, err := session.Do(&corev1.AnyRequest{Operations: ops})
	if err != nil {
		return nil, err
	}
	if len(resp.GetOperations()) != len(ops) {
		return nil, fmt.Errorf("expected %d ReadTransaction responses, got %d", len(ops), len(resp.GetOperations()))
	}

	out := make([]*corev1.Transaction, 0, len(resp.GetOperations()))
	for _, op := range resp.GetOperations() {
		readResp := op.GetReadTransaction()
		if readResp == nil {
			return nil, fmt.Errorf("expected ReadTransaction response")
		}
		if readResp.GetTransaction() != nil {
			out = append(out, readResp.GetTransaction())
		}
	}
	return out, nil
}

func (p *Processor) cacheTranCodesInSession(session AnySession, transactions []*corev1.Transaction) error {
	missing := map[uuid.UUID]*typev1.UUID{}
	for _, tx := range transactions {
		tranCodeID := tx.GetTranCodeId()
		if tranCodeID == nil {
			continue
		}
		if _, ok := p.cachedTranCode(tranCodeID); ok {
			continue
		}
		missing[uuidFromProto(tranCodeID)] = tranCodeID
	}
	if len(missing) == 0 {
		return nil
	}

	ops := make([]*corev1.AnyRequestOperation, 0, len(missing))
	for _, tranCodeID := range missing {
		ops = append(ops, &corev1.AnyRequestOperation{
			ReadTranCode: &corev1.ReadTranCodeRequest{Identifier: &corev1.ReadTranCodeRequest_TranCodeId{TranCodeId: tranCodeID}},
		})
	}
	resp, err := session.Do(&corev1.AnyRequest{Operations: ops})
	if err != nil {
		return err
	}
	if len(resp.GetOperations()) != len(ops) {
		return fmt.Errorf("expected %d ReadTranCode responses, got %d", len(ops), len(resp.GetOperations()))
	}
	for _, op := range resp.GetOperations() {
		readResp := op.GetReadTranCode()
		if readResp == nil {
			return fmt.Errorf("expected ReadTranCode response")
		}
		p.cacheTranCode(readResp.GetTranCode().GetTranCodeId(), readResp.GetTranCode().GetCode())
	}
	return nil
}

func (p *Processor) cachedTranCode(tranCodeID *typev1.UUID) (string, bool) {
	id := uuidFromProto(tranCodeID)
	p.tranCodeMu.Lock()
	code, ok := p.tranCodeCache[id]
	p.tranCodeMu.Unlock()
	return code, ok
}

func (p *Processor) cacheTranCode(tranCodeID *typev1.UUID, code string) {
	id := uuidFromProto(tranCodeID)
	p.tranCodeMu.Lock()
	p.tranCodeCache[id] = code
	p.tranCodeMu.Unlock()
}

func isAuthorizationTranCode(code string) bool {
	switch code {
	case TranCodeCardHold, TranCodeCardHoldReplace:
		return true
	default:
		return false
	}
}

func transactionExists(transactions []*corev1.Transaction, transactionID uuid.UUID) bool {
	for _, tx := range transactions {
		if uuidFromProto(tx.GetTransactionId()) == transactionID {
			return true
		}
	}
	return false
}

func webhookSupersededByTransactions(transactions []*corev1.Transaction, hook *Webhook) bool {
	if hook == nil {
		return false
	}
	for _, tx := range transactions {
		eventCount, ok := transactionWebhookEventCount(tx)
		if ok && eventCount > len(hook.Events) {
			return true
		}
	}
	return false
}

func transactionWebhookEventCount(tx *corev1.Transaction) (int, bool) {
	metadata := tx.GetMetadata()
	if metadata == nil {
		return 0, false
	}
	if metadata.GetStringValue() != "" {
		var hook Webhook
		if err := json.Unmarshal([]byte(metadata.GetStringValue()), &hook); err != nil {
			return 0, false
		}
		return len(hook.Events), true
	}
	raw, err := metadata.MarshalJSON()
	if err != nil {
		return 0, false
	}
	var hook Webhook
	if err := json.Unmarshal(raw, &hook); err != nil {
		return 0, false
	}
	return len(hook.Events), true
}

func copyParams(params map[string]string) map[string]string {
	out := make(map[string]string, len(params))
	for key, value := range params {
		out[key] = value
	}
	return out
}

func applyAmount(params map[string]string, amount Amount, isCredit bool) {
	params["amount"] = amount.Number()
	params["currency"] = amount.CurrencyCode()
	if isCredit {
		params["direction"] = debitOrCreditString(corev1.DebitOrCredit_DEBIT_OR_CREDIT_CREDIT)
		return
	}
	params["direction"] = debitOrCreditString(corev1.DebitOrCredit_DEBIT_OR_CREDIT_DEBIT_UNSPECIFIED)
}

func markerParams(params map[string]string) map[string]string {
	out := copyParams(params)
	out["amount"] = "0.00"
	out["voidAmount"] = "0.00"
	out["voidDirection"] = debitOrCreditString(corev1.DebitOrCredit_DEBIT_OR_CREDIT_DEBIT_UNSPECIFIED)
	return out
}

func derivedTransactionID(base uuid.UUID, suffix string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(base.String()+":"+suffix))
}

func anyRequest(op *corev1.AnyRequestOperation) *corev1.AnyRequest {
	return &corev1.AnyRequest{Operations: []*corev1.AnyRequestOperation{op}}
}

func singleAnyOperation(resp *corev1.AnyResponse) (*corev1.AnyResponseOperation, error) {
	if len(resp.GetOperations()) != 1 {
		return nil, fmt.Errorf("expected 1 Any response operation, got %d", len(resp.GetOperations()))
	}
	return resp.GetOperations()[0], nil
}

func applyPostTransactionsResponse(result *Result, resp *corev1.PostTransactionsResponse, journalID *typev1.UUID) *typev1.UUID {
	for _, response := range resp.GetResponses() {
		if voidResp := response.GetVoid(); voidResp != nil {
			result.Voids = append(result.Voids, voidResp)
			if voidResp.GetTransaction().GetJournalId() != nil {
				journalID = voidResp.GetTransaction().GetJournalId()
			}
			continue
		}
		if postResp := response.GetPost(); postResp != nil {
			result.Transaction = postResp
			if postResp.GetTransaction().GetJournalId() != nil {
				journalID = postResp.GetTransaction().GetJournalId()
			}
		}
	}
	return journalID
}

func postRequest(transactionID uuid.UUID, tranCode string, params map[string]string, overrideVelocity bool) *corev1.PostTransactionRequest {
	properties := &corev1.PostTransactionRequestProperties{Idempotent: true}
	if overrideVelocity {
		properties.OverrideVelocityEnforcement = &corev1.VelocityEnforcement{
			Action: corev1.VelocityEnforcementAction_VELOCITY_ENFORCEMENT_ACTION_WARN_UNSPECIFIED,
		}
	}
	return &corev1.PostTransactionRequest{
		TransactionId: newUUID(transactionID),
		TranCode:      tranCode,
		Params:        params,
		Properties:    properties,
	}
}

func postOperation(transactionID uuid.UUID, tranCode string, params map[string]string, overrideVelocity bool) *corev1.PostTransactionsOperation {
	op := &corev1.PostTransactionsOperation{}
	op.SetPost(postRequest(transactionID, tranCode, params, overrideVelocity))
	return op
}

func voidOperations(transactions []*corev1.Transaction, metadata string) []*corev1.PostTransactionsOperation {
	operations := make([]*corev1.PostTransactionsOperation, 0, len(transactions))
	for _, tx := range transactions {
		if tx.GetTransactionId() == nil {
			continue
		}
		properties := &corev1.VoidTransactionProperties{Idempotent: true}
		if metadata != "" {
			properties.Metadata = structpb.NewStringValue(metadata)
		}
		op := &corev1.PostTransactionsOperation{}
		op.SetVoid(&corev1.VoidTransactionRequest{
			TransactionId: tx.GetTransactionId(),
			Properties:    properties,
		})
		operations = append(operations, op)
	}
	return operations
}
