package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestParseOptionsDefaultsToDryRunAndRequiresExactScope(t *testing.T) {
	opts, err := parseOptions([]string{"--account", "kalshi:main", "--ticker", "KXRT-GOD-92"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.apply || opts.account != "kalshi:main" || opts.ticker != "KXRT-GOD-92" ||
		opts.expectedCashDiff != "0" || opts.cashTolerance != "0" || opts.timeout != 20*time.Second {
		t.Fatalf("options=%#v", opts)
	}
	for _, arguments := range [][]string{
		{"--account", "kalshi:main"},
		{"--ticker", "KX"},
		{"--account", "main", "--ticker", "KX"},
		{"--account", "kalshi:main", "--ticker", "kalshi:KX:YES"},
		{"--account", "kalshi:main", "--ticker", "KX", "extra"},
		{"--account", "kalshi:main", "--ticker", "KX", "--apply"},
		{"--account", "kalshi:main", "--ticker", "KX", "--expected-cash-diff", "fourteen"},
		{"--account", "kalshi:main", "--ticker", "KX", "--cash-tolerance", "-0.01"},
		{"--account", "kalshi:main", "--ticker", "KX", "--timeout", "1ms"},
	} {
		if _, err := parseOptions(arguments); err == nil {
			t.Fatalf("parseOptions(%q) error=nil", arguments)
		}
	}
	opts, err = parseOptions([]string{
		"--account", "kalshi:main", "--ticker", "KX", "--expected-cash-diff=-1.5", "--cash-tolerance", "0.01",
		"--apply", "--confirmation", "abc",
	})
	if err != nil || !opts.apply || opts.confirmation != "abc" || opts.expectedCashDiff != "-1.5" || opts.cashTolerance != "0.01" {
		t.Fatalf("apply options=%#v error=%v", opts, err)
	}
}

func TestParseOptionsCapsTheCashToleranceAtOneCent(t *testing.T) {
	for _, value := range []string{"0", "0.0", "0.001", "0.01", "0.0100"} {
		opts, err := parseOptions([]string{"--account", "kalshi:main", "--ticker", "KX", "--cash-tolerance", value})
		if err != nil {
			t.Fatalf("--cash-tolerance %s error = %v", value, err)
		}
		// The text is passed on untouched because the plan's fingerprint covers it.
		if opts.cashTolerance.String() != value {
			t.Fatalf("--cash-tolerance %s became %s", value, opts.cashTolerance)
		}
	}
	for _, value := range []string{"0.0100001", "0.011", "0.02", "1", "14", "-0.01", "-0.0000001", "cents", ""} {
		if _, err := parseOptions([]string{"--account", "kalshi:main", "--ticker", "KX", "--cash-tolerance", value}); err == nil {
			t.Fatalf("--cash-tolerance %q error = nil", value)
		}
	}
}

func TestRepairHTTPClientNeverFollowsRedirects(t *testing.T) {
	client := repairHTTPClient(5 * time.Second)
	if client.Timeout != 5*time.Second || client.CheckRedirect == nil {
		t.Fatalf("client=%#v", client)
	}
	if err := client.CheckRedirect(&http.Request{}, nil); !errors.Is(err, http.ErrUseLastResponse) {
		t.Fatalf("redirect error=%v", err)
	}
}

func TestValidateKalshiAPIURLRestrictsApplyToProduction(t *testing.T) {
	if err := validateKalshiAPIURL("https://external-api.kalshi.com", true); err != nil {
		t.Fatalf("production apply URL rejected: %v", err)
	}
	if err := validateKalshiAPIURL("https://demo-api.kalshi.co", false); err != nil {
		t.Fatalf("demo dry-run URL rejected: %v", err)
	}
	if err := validateKalshiAPIURL("https://demo-api.kalshi.co", true); err == nil {
		t.Fatal("a demo endpoint must not be allowed to apply")
	}
	for _, value := range []string{"http://external-api.kalshi.com", "https://evil.example", "https://external-api.kalshi.com:444", "not-a-url"} {
		if err := validateKalshiAPIURL(value, false); err == nil {
			t.Fatalf("validateKalshiAPIURL(%q) error=nil", value)
		}
	}
}

func TestSelectBindingRequiresOneCompleteExactAccount(t *testing.T) {
	encoded := `[{"execution_account_id":"kalshi:main","api_key_id":"key","private_key_path":"/run/key.pem"}]`
	selected, err := selectBinding(encoded, "kalshi:main")
	if err != nil || selected.APIKeyID != "key" {
		t.Fatalf("selected=%#v error=%v", selected, err)
	}
	// The wallet name alone is not the Kalshi execution account.
	if _, err := selectBinding(encoded, "main"); err == nil {
		t.Fatal("a wallet name must not select a Kalshi credential")
	}
	if _, err := selectBinding(encoded, "kalshi:other"); err == nil {
		t.Fatal("a missing account binding must fail")
	}
}

type fakeEvidence struct {
	position, cash domain.Decimal
	positionErr    error
	balanceErr     error
}

func (source fakeEvidence) GetMarketPosition(context.Context, string) (domain.Decimal, error) {
	return source.position, source.positionErr
}

func (source fakeEvidence) AvailableBalance(context.Context) (domain.Decimal, error) {
	return source.cash, source.balanceErr
}

type fakeStore struct {
	request postgresadapter.KalshiNettingRepairRequest
	report  postgresadapter.KalshiNettingRepairReport
	err     error
	calls   int
}

func (store *fakeStore) Run(_ context.Context, request postgresadapter.KalshiNettingRepairRequest) (postgresadapter.KalshiNettingRepairReport, error) {
	store.calls++
	store.request = request
	return store.report, store.err
}

func TestRepairPassesTheExchangeEvidenceAndPrintsTheRefusedPlan(t *testing.T) {
	refused := errors.New("local net position does not match")
	store := &fakeStore{report: postgresadapter.KalshiNettingRepairReport{ExecutionAccountID: "kalshi:main", Fingerprint: "abc"}, err: refused}
	var output bytes.Buffer
	err := repair(context.Background(), options{
		account: "kalshi:main", ticker: "KX", expectedCashDiff: "1.5", cashTolerance: "0.01", apply: true, confirmation: "abc",
	}, fakeEvidence{position: "1.00", cash: "23.0471"}, store, &output)
	if !errors.Is(err, refused) {
		t.Fatalf("repair error = %v", err)
	}
	if !strings.Contains(output.String(), `"fingerprint": "abc"`) {
		t.Fatalf("the refused plan was not printed: %s", output.String())
	}
	request := store.request
	if request.ExecutionAccountID != "kalshi:main" || request.Ticker != "KX" || request.Evidence.PositionFP != "1.00" ||
		request.Evidence.Cash != "23.0471" || request.ExpectedCashDiff != "1.5" || request.CashTolerance != "0.01" ||
		!request.Apply || request.Confirmation != "abc" {
		t.Fatalf("request = %#v", request)
	}
}

func TestRepairNeverTouchesTheLedgerWithoutEvidence(t *testing.T) {
	unavailable := errors.New("unavailable")
	for name, source := range map[string]fakeEvidence{
		"position": {positionErr: unavailable},
		"balance":  {position: "1.00", balanceErr: unavailable},
	} {
		store := &fakeStore{}
		err := repair(context.Background(), options{account: "kalshi:main", ticker: "KX"}, source, store, &bytes.Buffer{})
		if !errors.Is(err, unavailable) || store.calls != 0 {
			t.Fatalf("%s: error=%v store calls=%d", name, err, store.calls)
		}
	}
}
