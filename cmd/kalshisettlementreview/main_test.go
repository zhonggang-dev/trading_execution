package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/kalshi"
	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func validArguments(extra ...string) []string {
	return append([]string{"--account", "kalshi:main", "--ticker", "KXREV-1", "--actor", "ops", "--reason", "late fill booked"}, extra...)
}

func TestParseOptionsDefaultsToDryRunAndRequiresAccountability(t *testing.T) {
	opts, err := parseOptions(validArguments())
	if err != nil {
		t.Fatal(err)
	}
	if opts.apply || opts.account != "kalshi:main" || opts.ticker != "KXREV-1" || opts.actor != "ops" ||
		opts.reason != "late fill booked" || opts.expectedCashDiff != "0" || opts.cashTolerance != "0" || opts.timeout != 30*time.Second {
		t.Fatalf("options=%#v", opts)
	}
	for name, arguments := range map[string][]string{
		"no ticker":          {"--account", "kalshi:main", "--actor", "ops", "--reason", "r"},
		"no account":         {"--ticker", "KX", "--actor", "ops", "--reason", "r"},
		"no actor":           {"--account", "kalshi:main", "--ticker", "KX", "--reason", "r"},
		"no reason":          {"--account", "kalshi:main", "--ticker", "KX", "--actor", "ops"},
		"blank actor":        {"--account", "kalshi:main", "--ticker", "KX", "--actor", " ", "--reason", "r"},
		"polymarket account": {"--account", "main", "--ticker", "KX", "--actor", "ops", "--reason", "r"},
		"token as ticker":    {"--account", "kalshi:main", "--ticker", "kalshi:KX:YES", "--actor", "ops", "--reason", "r"},
		"positional":         validArguments("extra"),
		"apply without fp":   validArguments("--apply"),
		"fp without apply":   validArguments("--confirmation", "abc"),
		"bad cash diff":      validArguments("--expected-cash-diff", "ten"),
		"negative tolerance": validArguments("--cash-tolerance", "-0.01"),
		"wide tolerance":     validArguments("--cash-tolerance", "0.011"),
		"short timeout":      validArguments("--timeout", "1ms"),
	} {
		if _, err := parseOptions(arguments); err == nil {
			t.Errorf("%s: parseOptions(%q) error=nil", name, arguments)
		}
	}
	opts, err = parseOptions(validArguments("--apply", "--confirmation", "abc", "--cash-tolerance", "0.01", "--expected-cash-diff=-2"))
	if err != nil || !opts.apply || opts.confirmation != "abc" || opts.cashTolerance != "0.01" || opts.expectedCashDiff != "-2" {
		t.Fatalf("apply options=%#v error=%v", opts, err)
	}
}

func TestReviewHTTPClientNeverFollowsRedirects(t *testing.T) {
	client := reviewHTTPClient(5 * time.Second)
	if err := client.CheckRedirect(&http.Request{}, nil); client.Timeout != 5*time.Second || !errors.Is(err, http.ErrUseLastResponse) {
		t.Fatalf("client=%#v redirect error=%v", client, err)
	}
}

func TestValidateKalshiAPIURLRestrictsApplyToProduction(t *testing.T) {
	if err := validateKalshiAPIURL("https://external-api.kalshi.com", true); err != nil {
		t.Fatalf("production apply URL rejected: %v", err)
	}
	if err := validateKalshiAPIURL("https://demo-api.kalshi.co", true); err == nil {
		t.Fatal("a demo endpoint must not be allowed to apply")
	}
	for _, value := range []string{"http://external-api.kalshi.com", "https://evil.example", "https://external-api.kalshi.com:444"} {
		if err := validateKalshiAPIURL(value, false); err == nil {
			t.Fatalf("validateKalshiAPIURL(%q) error=nil", value)
		}
	}
}

func TestSelectBindingRequiresTheExactKalshiAccount(t *testing.T) {
	encoded := `[{"execution_account_id":"kalshi:main","api_key_id":"key","private_key_path":"/run/key.pem"}]`
	if selected, err := selectBinding(encoded, "kalshi:main"); err != nil || selected.APIKeyID != "key" {
		t.Fatalf("selected=%#v error=%v", selected, err)
	}
	// A retired account without a configured credential cannot be reviewed.
	if _, err := selectBinding(encoded, "kalshi:wallet-7"); err == nil {
		t.Fatal("an unconfigured account must fail")
	}
}

func TestRedactHidesTheDatabaseURL(t *testing.T) {
	t.Setenv("TRADING_EXECUTION_DATABASE_URL", "postgres://user:secret@db/trading")
	if got := redact("connect postgres://user:secret@db/trading: refused"); strings.Contains(got, "secret") {
		t.Fatalf("redact() = %q", got)
	}
}

type fakeEvidence struct {
	calls []string
	errAt string
}

func (source *fakeEvidence) step(name string) error {
	source.calls = append(source.calls, name)
	if source.errAt == name {
		return errors.New(name + " unavailable")
	}
	return nil
}

func (source *fakeEvidence) ListSettlements(context.Context, string) ([]domain.KalshiSettlement, error) {
	return []domain.KalshiSettlement{{Ticker: "KXREV-1", MarketResult: "yes"}}, source.step("settlements")
}

func (source *fakeEvidence) ListMarketFills(context.Context, string) ([]kalshi.Fill, error) {
	return []kalshi.Fill{{FillID: "f1", Count: "10.00"}}, source.step("fills")
}

func (source *fakeEvidence) GetMarketPosition(context.Context, string) (domain.Decimal, error) {
	return "0", source.step("position")
}

func (source *fakeEvidence) AvailableBalance(context.Context) (domain.Decimal, error) {
	return "110", source.step("balance")
}

type fakeStore struct {
	request postgresadapter.KalshiSettlementReviewRequest
	report  postgresadapter.KalshiSettlementReviewReport
	err     error
	calls   int
}

func (store *fakeStore) Run(_ context.Context, request postgresadapter.KalshiSettlementReviewRequest) (postgresadapter.KalshiSettlementReviewReport, error) {
	store.calls++
	store.request = request
	return store.report, store.err
}

func TestReviewReadsAllEvidenceFirstAndPrintsTheRefusedPlan(t *testing.T) {
	refused := errors.New("fills differ")
	source := &fakeEvidence{}
	store := &fakeStore{report: postgresadapter.KalshiSettlementReviewReport{ExecutionAccountID: "kalshi:main", Fingerprint: "abc"}, err: refused}
	var output bytes.Buffer
	opts, err := parseOptions(validArguments("--apply", "--confirmation", "abc", "--cash-tolerance", "0.01"))
	if err != nil {
		t.Fatal(err)
	}
	if err := review(context.Background(), opts, source, store, &output); !errors.Is(err, refused) {
		t.Fatalf("review error = %v", err)
	}
	if strings.Join(source.calls, ",") != "settlements,fills,position,balance" {
		t.Fatalf("evidence reads = %v", source.calls)
	}
	if !strings.Contains(output.String(), `"fingerprint": "abc"`) {
		t.Fatalf("the refused plan was not printed: %s", output.String())
	}
	request := store.request
	if request.ExecutionAccountID != "kalshi:main" || request.Ticker != "KXREV-1" || request.Actor != "ops" ||
		request.Reason != "late fill booked" || len(request.Evidence.Settlements) != 1 || len(request.Evidence.Fills) != 1 ||
		request.Evidence.Fills[0].FillID != "f1" || request.Evidence.Fills[0].Count != "10.00" ||
		request.Evidence.PositionFP != "0" || request.Evidence.Cash != "110" || request.CashTolerance != "0.01" ||
		!request.Apply || request.Confirmation != "abc" {
		t.Fatalf("request = %#v", request)
	}
}

func TestReviewNeverTouchesTheLedgerWithoutCompleteEvidence(t *testing.T) {
	for _, step := range []string{"settlements", "fills", "position", "balance"} {
		store := &fakeStore{}
		opts, _ := parseOptions(validArguments())
		err := review(context.Background(), opts, &fakeEvidence{errAt: step}, store, &bytes.Buffer{})
		if err == nil || store.calls != 0 {
			t.Fatalf("%s failure: error=%v store calls=%d", step, err, store.calls)
		}
	}
}
