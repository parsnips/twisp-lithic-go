package twisplithic

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

type Webhook struct {
	Amount  int            `json:"amount"`
	Events  []WebhookEvent `json:"events"`
	Token   string         `json:"token"`
	Status  string         `json:"status"`
	Result  string         `json:"result"`
	Created string         `json:"created"`

	AuthorizationAmount         int    `json:"authorization_amount"`
	SettledAmount               int    `json:"settled_amount"`
	Network                     string `json:"network"`
	MerchantAmount              int    `json:"merchant_amount"`
	MerchantAuthorizationAmount int    `json:"merchant_authorization_amount"`
	MerchantCurrency            string `json:"merchant_currency"`
	AcquirerFee                 int    `json:"acquirer_fee"`
}

type WebhookEvent struct {
	Amount  int    `json:"amount"`
	Created string `json:"created"`
	Result  string `json:"result"`
	Type    string `json:"type"`
	Token   string `json:"token"`
}

func ParseWebhook(raw string) (*Webhook, error) {
	var webhook Webhook
	if err := json.Unmarshal([]byte(raw), &webhook); err != nil {
		return nil, err
	}
	return &webhook, nil
}

func (w Webhook) BackfillWebhooks() []Webhook {
	if len(w.Events) == 0 {
		return nil
	}

	hooks := make([]Webhook, 0, len(w.Events))
	for i := range w.Events {
		hook := w
		if i == 0 {
			hook.Events = nil
			hook.AuthorizationAmount = 0
			hook.SettledAmount = 0
		} else {
			hook.Events = append([]WebhookEvent(nil), w.Events[:i]...)
		}
		hooks = append(hooks, hook)
	}
	return hooks
}

func (w Webhook) IsAuthOrAdvice() bool {
	if !w.HasEvents() {
		return w.Status == "AUTHORIZATION"
	}
	lastEvent := w.LastEvent()
	return lastEvent.Type == "AUTHORIZATION_ADVICE" || lastEvent.Type == "AUTHORIZATION"
}

func (w Webhook) IsFinancialAuthorization() bool {
	return w.LastEvent().Type == "FINANCIAL_AUTHORIZATION"
}

func (w Webhook) IsFinancialCreditAuthorization() bool {
	return w.LastEvent().Type == "FINANCIAL_CREDIT_AUTHORIZATION"
}

func (w Webhook) IsCreditAuthorization() bool {
	return w.LastEvent().Type == "CREDIT_AUTHORIZATION" || w.LastEvent().Type == "CREDIT_AUTHORIZATION_ADVICE"
}

func (w Webhook) IsVoid() bool {
	if len(w.Events) == 0 {
		return false
	}
	lastEvent := w.LastEvent()
	return lastEvent.Type == "VOID" ||
		lastEvent.Type == "AUTHORIZATION_REVERSAL" ||
		lastEvent.Type == "AUTHORIZATION_EXPIRY"
}

func (w Webhook) IsClearing() bool {
	return w.HasEvents() && w.LastEvent().Type == "CLEARING"
}

func (w Webhook) IsReturn() bool {
	if !w.HasEvents() {
		return false
	}
	lastEvent := w.LastEvent()
	return lastEvent.Type == "RETURN" || lastEvent.Type == "RETURN_REVERSAL"
}

func (w Webhook) IsReturnOfCreditAuthorization() bool {
	if !w.HasEvents() {
		return false
	}
	for _, event := range w.Events {
		if event.Type == "CREDIT_AUTHORIZATION" || event.Type == "CREDIT_AUTHORIZATION_ADVICE" {
			return true
		}
	}
	return false
}

func (w Webhook) IsTransactionDecline() bool {
	return w.Status == "DECLINED"
}

func (w Webhook) IsTransactionSettled() bool {
	return w.Status == "SETTLED"
}

func (w Webhook) IsTransactionVoided() bool {
	return w.Status == "VOIDED" || w.Status == "EXPIRED"
}

func (w Webhook) IsTransactionASA() bool {
	return !w.HasEvents() && !w.IsBalanceInquiry()
}

func (w Webhook) IsBalanceInquiry() bool {
	return w.Status == "BALANCE_INQUIRY" || w.LastEvent().Type == "BALANCE_INQUIRY"
}

func (w Webhook) CorrelationID() (uuid.UUID, error) {
	return uuid.Parse(w.Token)
}

func (w Webhook) TransactionID() (uuid.UUID, error) {
	if len(w.Events) > 0 {
		return uuid.Parse(w.Events[len(w.Events)-1].Token)
	}
	return uuid.Parse(w.Token)
}

func (w Webhook) AmountAsMoney() (Amount, error) {
	return NewAmount(centsToDecimalStr(w.GetAmount()), "USD")
}

func (w Webhook) RemainingAuthorizationAmountAsMoney() (Amount, bool, error) {
	amount := w.AuthorizationAmount
	if (w.IsVoid() && (w.IsTransactionSettled() || w.IsTransactionVoided())) || (w.IsClearing() && amount == w.GetAmount()) {
		amount = 0
	}
	parsed, err := NewAmount(centsToDecimalStr(amount), "USD")
	return parsed, amount < 0, err
}

func (w Webhook) GetAmount() int {
	if len(w.Events) > 0 {
		return w.Events[len(w.Events)-1].Amount
	}
	return w.AuthorizationAmount
}

func (w Webhook) IsCredit() bool {
	return w.GetAmount() < 0
}

func (w Webhook) HasEvents() bool {
	return len(w.Events) > 0
}

func (w Webhook) LastEvent() WebhookEvent {
	if w.HasEvents() {
		return w.Events[len(w.Events)-1]
	}
	return WebhookEvent{}
}

func centsToDecimalStr(amt int) string {
	dollars := amt / 100
	cents := amt % 100
	if dollars < 0 {
		dollars *= -1
	}
	if cents < 0 {
		cents *= -1
	}
	return fmt.Sprintf("%d.%02d", dollars, cents)
}
