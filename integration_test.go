package twisplithic

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	corev1grpc "buf.build/gen/go/twisp/api/grpc/go/twisp/core/v1/corev1grpc"
	corev1 "buf.build/gen/go/twisp/api/protocolbuffers/go/twisp/core/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type webhookFixtureTestCase struct {
	name                  string
	expectedPending       Amount
	expectedSettled       Amount
	expectedPendingOffset Amount
	expectedSettledOffset Amount
}

var webhookFixtureTestCases = []webhookFixtureTestCase{
	{"asa", MustNewAmount("-10.00", "USD"), MustNewAmount("0", "USD"), MustNewAmount("10.00", "USD"), MustNewAmount("0", "USD")},
	{"asa_authorization", MustNewAmount("-10.00", "USD"), MustNewAmount("0", "USD"), MustNewAmount("10.00", "USD"), MustNewAmount("0", "USD")},
	{"asa_decline", MustNewAmount("0.00", "USD"), MustNewAmount("0", "USD"), MustNewAmount("0.00", "USD"), MustNewAmount("0", "USD")},
	{"asa_auth_clear", MustNewAmount("0.00", "USD"), MustNewAmount("-10.00", "USD"), MustNewAmount("0.00", "USD"), MustNewAmount("10.00", "USD")},
	{"asa_partial_clear", MustNewAmount("-9.00", "USD"), MustNewAmount("-1.00", "USD"), MustNewAmount("9.00", "USD"), MustNewAmount("1.00", "USD")},
	{"asa_auth_void_full", MustNewAmount("0.00", "USD"), MustNewAmount("0", "USD"), MustNewAmount("0.00", "USD"), MustNewAmount("0", "USD")},
	{"asa_auth_void_full_authorization_expiry", MustNewAmount("0.00", "USD"), MustNewAmount("0", "USD"), MustNewAmount("0.00", "USD"), MustNewAmount("0", "USD")},
	{"asa_auth_void_full_authorization_reversal", MustNewAmount("0.00", "USD"), MustNewAmount("0", "USD"), MustNewAmount("0.00", "USD"), MustNewAmount("0", "USD")},
	{"asa_auth_void_partial", MustNewAmount("-1.00", "USD"), MustNewAmount("0", "USD"), MustNewAmount("1.00", "USD"), MustNewAmount("0", "USD")},
	{"asa_multiple_completion_with_voids_and_returns", MustNewAmount("0.00", "USD"), MustNewAmount("-1.00", "USD"), MustNewAmount("0.00", "USD"), MustNewAmount("1.00", "USD")},
	{"asa_auth_auth_advice", MustNewAmount("-20.00", "USD"), MustNewAmount("0", "USD"), MustNewAmount("20.00", "USD"), MustNewAmount("0", "USD")},
	{"multiple_completion_full_return", MustNewAmount("0.00", "USD"), MustNewAmount("0.00", "USD"), MustNewAmount("0.00", "USD"), MustNewAmount("0.00", "USD")},
	{"multiple_completion_full_return_reversal", MustNewAmount("0.00", "USD"), MustNewAmount("-10.00", "USD"), MustNewAmount("0.00", "USD"), MustNewAmount("10.00", "USD")},
	{"force_post", MustNewAmount("0", "USD"), MustNewAmount("-10.00", "USD"), MustNewAmount("0", "USD"), MustNewAmount("10.00", "USD")},
	{"unmatched_return", MustNewAmount("0.00", "USD"), MustNewAmount("10.00", "USD"), MustNewAmount("0.00", "USD"), MustNewAmount("-10.00", "USD")},
	{"return_reversal", MustNewAmount("0.00", "USD"), MustNewAmount("0.00", "USD"), MustNewAmount("0.00", "USD"), MustNewAmount("0.00", "USD")},
	{"balance_inquiry", MustNewAmount("-10.00", "USD"), MustNewAmount("0", "USD"), MustNewAmount("10.00", "USD"), MustNewAmount("0", "USD")},
	{"credit_authorization_advice", MustNewAmount("10.00", "USD"), MustNewAmount("0", "USD"), MustNewAmount("-10.00", "USD"), MustNewAmount("0", "USD")},
	{"credit_authorization_advice_reversal", MustNewAmount("0.00", "USD"), MustNewAmount("0", "USD"), MustNewAmount("0.00", "USD"), MustNewAmount("0", "USD")},
	{"multi_completion_return_doesnt_drop_hold", MustNewAmount("0.00", "USD"), MustNewAmount("-3.67", "USD"), MustNewAmount("0.00", "USD"), MustNewAmount("3.67", "USD")},
}

func TestWebhookFixtureAgainstTwispContainer(t *testing.T) {
	ctx, conn := openTwispContainer(t)
	fixture := loadWebhookFixture(t)
	testCases := webhookFixtureTestCases

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runWebhookFixtureCase(t, ctx, conn, tc, fixture[tc.name])
		})
	}
}

func TestWebhookFixturePermutationsAgainstTwispContainer(t *testing.T) {
	ctx, conn := openTwispContainer(t)
	fixture := loadWebhookFixture(t)

	for _, tc := range webhookFixtureTestCases {
		hooks := fixture[tc.name]
		for i, hooks := range webhookPermutationOrders(hooks) {
			i, hooks := i, hooks
			t.Run(fmt.Sprintf("%s/order_%02d", tc.name, i), func(t *testing.T) {
				runWebhookFixtureCase(t, ctx, conn, tc, hooks)
			})
		}
	}
}

func openTwispContainer(t *testing.T) (context.Context, *grpc.ClientConn) {
	t.Helper()
	if os.Getenv("TWISP_TESTCONTAINERS") != "1" {
		t.Skip("set TWISP_TESTCONTAINERS=1 and TWISP_LOCAL_IMAGE to run the real Twisp container test")
	}

	ctx := context.Background()
	image := os.Getenv("TWISP_LOCAL_IMAGE")
	if image == "" {
		image = fmt.Sprintf("bazel/services/local:local-server-%s", localArch(t))
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{"8080/tcp", "8081/tcp"},
			WaitingFor: wait.ForHTTP("/healthcheck").
				WithPort("8080/tcp").
				WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)
	grpcPort, err := container.MappedPort(ctx, "8081/tcp")
	require.NoError(t, err)

	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", host, grpcPort.Port()), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})
	return ctx, conn
}

func runWebhookFixtureCase(t *testing.T, ctx context.Context, conn *grpc.ClientConn, tc webhookFixtureTestCase, hooks []Webhook) {
	t.Helper()
	accountHeader := uuid.NewString()
	reqCtx := metadata.AppendToOutgoingContext(ctx, "x-twisp-account-id", accountHeader)
	accountClient := corev1grpc.NewAccountServiceClient(conn)
	journalClient := corev1grpc.NewJournalServiceClient(conn)
	balanceClient := corev1grpc.NewBalanceServiceClient(conn)
	processor := NewProcessor(&GRPCClient{
		conn:         conn,
		accountID:    accountHeader,
		transactions: corev1grpc.NewTransactionServiceClient(conn),
		tranCodes:    corev1grpc.NewTranCodeServiceClient(conn),
		balances:     balanceClient,
		any:          corev1grpc.NewAnyServiceClient(conn),
	})

	journalID := uuid.New()
	accountID := uuid.New()
	settlementID := uuid.New()

	_, err := journalClient.CreateJournal(reqCtx, &corev1.CreateJournalRequest{
		JournalId: newUUID(journalID),
		Name:      "Lithic reference journal " + tc.name,
		Status:    corev1.JournalStatus_JOURNAL_STATUS_ACTIVE_UNSPECIFIED,
	})
	require.NoError(t, err)

	_, err = accountClient.CreateAccount(reqCtx, &corev1.CreateAccountRequest{
		AccountId:         newUUID(settlementID),
		Name:              "Card Settlement Account " + tc.name,
		Code:              "Liabilities.Settlement.Card." + tc.name + "." + uuid.NewString(),
		Status:            corev1.AccountStatus_ACCOUNT_STATUS_ACTIVE_UNSPECIFIED,
		NormalBalanceType: corev1.DebitOrCredit_DEBIT_OR_CREDIT_CREDIT,
	})
	require.NoError(t, err)

	_, err = accountClient.CreateAccount(reqCtx, &corev1.CreateAccountRequest{
		AccountId:         newUUID(accountID),
		Name:              "Cardholder " + tc.name,
		Code:              "Cardholder." + tc.name + "." + uuid.NewString(),
		Status:            corev1.AccountStatus_ACCOUNT_STATUS_ACTIVE_UNSPECIFIED,
		NormalBalanceType: corev1.DebitOrCredit_DEBIT_OR_CREDIT_CREDIT,
	})
	require.NoError(t, err)

	for _, hook := range hooks {
		hook := hook
		_, err = processor.Process(ctx, LithicRequest{
			AccountID:           accountID,
			JournalID:           journalID,
			SettlementAccountID: settlementID,
			Webhook:             &hook,
		})
		require.NoError(t, err)
	}

	cardholder, err := balanceClient.ReadBalance(reqCtx, &corev1.ReadBalanceRequest{
		JournalId: newUUID(journalID),
		AccountId: newUUID(accountID),
		Currency:  "USD",
	})
	require.NoError(t, err)
	settlement, err := balanceClient.ReadBalance(reqCtx, &corev1.ReadBalanceRequest{
		JournalId: newUUID(journalID),
		AccountId: newUUID(settlementID),
		Currency:  "USD",
	})
	require.NoError(t, err)

	assertBalance(t, tc.expectedPending, cardholder.GetBalance().GetPending())
	assertBalance(t, tc.expectedSettled, cardholder.GetBalance().GetSettled())
	assertBalance(t, tc.expectedPendingOffset, settlement.GetBalance().GetPending())
	assertBalance(t, tc.expectedSettledOffset, settlement.GetBalance().GetSettled())
}

func webhookPermutationOrders(hooks []Webhook) [][]Webhook {
	if len(hooks) <= 1 {
		return [][]Webhook{copyWebhooks(hooks)}
	}
	if len(hooks) <= 4 {
		var out [][]Webhook
		permuteWebhooks(copyWebhooks(hooks), 0, &out)
		return out
	}

	orders := [][]Webhook{
		copyWebhooks(hooks),
		reverseWebhooks(hooks),
		rotateWebhooks(hooks, 1),
		rotateWebhooks(hooks, len(hooks)-1),
		moveLastFirst(hooks),
	}
	return dedupeWebhookOrders(orders)
}

func permuteWebhooks(hooks []Webhook, start int, out *[][]Webhook) {
	if start == len(hooks) {
		*out = append(*out, copyWebhooks(hooks))
		return
	}
	for i := start; i < len(hooks); i++ {
		hooks[start], hooks[i] = hooks[i], hooks[start]
		permuteWebhooks(hooks, start+1, out)
		hooks[start], hooks[i] = hooks[i], hooks[start]
	}
}

func copyWebhooks(hooks []Webhook) []Webhook {
	out := make([]Webhook, len(hooks))
	copy(out, hooks)
	return out
}

func reverseWebhooks(hooks []Webhook) []Webhook {
	out := copyWebhooks(hooks)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func rotateWebhooks(hooks []Webhook, offset int) []Webhook {
	out := make([]Webhook, 0, len(hooks))
	out = append(out, hooks[offset:]...)
	out = append(out, hooks[:offset]...)
	return copyWebhooks(out)
}

func moveLastFirst(hooks []Webhook) []Webhook {
	out := make([]Webhook, 0, len(hooks))
	out = append(out, hooks[len(hooks)-1])
	out = append(out, hooks[:len(hooks)-1]...)
	return copyWebhooks(out)
}

func dedupeWebhookOrders(orders [][]Webhook) [][]Webhook {
	seen := map[string]bool{}
	out := make([][]Webhook, 0, len(orders))
	for _, order := range orders {
		key := webhookOrderKey(order)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, order)
	}
	return out
}

func webhookOrderKey(hooks []Webhook) string {
	var b strings.Builder
	for _, hook := range hooks {
		b.WriteString(hook.Token)
		b.WriteByte(':')
		b.WriteString(fmt.Sprint(len(hook.Events)))
		b.WriteByte('|')
	}
	return b.String()
}

func loadWebhookFixture(t *testing.T) map[string][]Webhook {
	t.Helper()
	data, err := os.ReadFile("testdata/webhooks.json")
	require.NoError(t, err)
	var fixture map[string][]Webhook
	require.NoError(t, json.Unmarshal(data, &fixture))
	return fixture
}

func assertBalance(t *testing.T, expected Amount, balance *corev1.BalanceAmount) {
	t.Helper()
	cr, err := AmountFromMoney(balance.GetCrBalance())
	require.NoError(t, err)
	dr, err := AmountFromMoney(balance.GetDrBalance())
	require.NoError(t, err)
	actual, err := cr.Sub(dr)
	require.NoError(t, err)
	require.True(t, expected.Equal(actual), "expected %s, got %s (cr=%s dr=%s)", expected.Number(), actual.Number(), cr.Number(), dr.Number())
}

func localArch(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("uname", "-m").Output()
	require.NoError(t, err)
	switch strings.TrimSpace(string(out)) {
	case "arm64", "aarch64":
		return "arm64"
	case "amd64", "x86_64":
		return "amd64"
	default:
		t.Fatalf("unsupported arch %q", strings.TrimSpace(string(out)))
		return ""
	}
}
