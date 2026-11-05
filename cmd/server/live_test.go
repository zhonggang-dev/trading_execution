package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/accountmaintenance"
	"github.com/UniPat-AI/trading_execution/internal/service/reconciliation"
)

func TestOptionalQuarantineReadinessDoesNotBoxNilPointer(t *testing.T) {
	if checker := optionalQuarantineReadiness(nil); checker != nil {
		t.Fatalf("optionalQuarantineReadiness(nil) = %#v, want nil interface", checker)
	}

	concrete := &postgresadapter.ExecutionAccountQuarantineChecker{}
	if checker := optionalQuarantineReadiness(concrete); checker != concrete {
		t.Fatalf("optionalQuarantineReadiness(non-nil) = %#v, want original checker", checker)
	}
}

type livePreflightProbeClient struct {
	account      polymarket.AccountProbe
	accountErr   error
	funding      polymarket.FundingProbe
	fundingErr   error
	accountCalls []string
	fundingCalls []string
}

func (client *livePreflightProbeClient) ProbeAccount(
	_ context.Context,
	executionAccountID string,
) (polymarket.AccountProbe, error) {
	client.accountCalls = append(client.accountCalls, executionAccountID)
	return client.account, client.accountErr
}

func (client *livePreflightProbeClient) ProbeFunding(
	_ context.Context,
	executionAccountID string,
) (polymarket.FundingProbe, error) {
	client.fundingCalls = append(client.fundingCalls, executionAccountID)
	return client.funding, client.fundingErr
}

type livePreflightEligibility struct {
	result polymarket.GeographicEligibility
	err    error
	calls  []string
}

func (checker *livePreflightEligibility) Check(
	_ context.Context,
	executionAccountID string,
) (polymarket.GeographicEligibility, error) {
	checker.calls = append(checker.calls, executionAccountID)
	return checker.result, checker.err
}

func TestValidateStartupReconciliationAllowsAccountLocalAttention(t *testing.T) {
	completed := reconciliation.SweepResult{Runs: []reconciliation.Result{{
		Run: domain.ReconciliationRun{
			ExecutionAccountID: "wallet-1",
			Status:             domain.ReconciliationRunCompleted,
		},
	}}}
	if failed, err := validateStartupReconciliation(completed, []string{"wallet-1"}); err != nil || len(failed) != 0 {
		t.Fatalf("validate completed startup reconciliation: %#v, %v", failed, err)
	}
	attention := reconciliation.SweepResult{Runs: []reconciliation.Result{{
		Run: domain.ReconciliationRun{
			ExecutionAccountID: "main",
			Status:             domain.ReconciliationRunAttentionRequired,
		},
		Issues: []domain.ReconciliationIssue{{}},
	}}}
	if failed, err := validateStartupReconciliation(attention, []string{"main"}); err != nil || len(failed) != 0 {
		t.Fatalf("validate account-local attention startup reconciliation: %#v, %v", failed, err)
	}
	attentionWithPersistedError := reconciliation.SweepResult{
		Runs: []reconciliation.Result{{
			Run: domain.ReconciliationRun{
				ExecutionAccountID: "wallet-6",
				Status:             domain.ReconciliationRunAttentionRequired,
				Error:              "CLOB fill evidence is inconsistent",
			},
			Issues: []domain.ReconciliationIssue{{}},
		}},
		Errors: []error{errors.New("reconcile wallet-6: CLOB fill evidence is inconsistent")},
	}
	if failed, err := validateStartupReconciliation(attentionWithPersistedError, []string{"wallet-6"}); err != nil || len(failed) != 0 {
		t.Fatalf("validate durably gated account-local startup error: %#v, %v", failed, err)
	}

	fatal := []struct {
		name   string
		result reconciliation.SweepResult
		want   string
	}{
		{name: "missing account run", result: reconciliation.SweepResult{}, want: "want 1"},
		{
			name: "unattributed source error",
			result: reconciliation.SweepResult{
				Runs:   completed.Runs,
				Errors: []error{errors.New("position source unavailable")},
			},
			want: "position source unavailable",
		},
		{
			name: "error for an unknown account",
			result: reconciliation.SweepResult{
				Runs:   completed.Runs,
				Errors: []error{errors.New("reconcile wallet-9: boom")},
			},
			want: "wallet-9",
		},
	}
	for _, test := range fatal {
		t.Run(test.name, func(t *testing.T) {
			_, err := validateStartupReconciliation(test.result, []string{"wallet-1"})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateStartupReconciliation() = %v, want error containing %q", err, test.want)
			}
		})
	}
}

// TestValidateStartupReconciliationDegradesFailedAccounts covers C-05: a FAILED
// run, a run blocked by an unexpired RUNNING lease, and an unpersisted
// account error put only that account in maintenance.
func TestValidateStartupReconciliationDegradesFailedAccounts(t *testing.T) {
	result := reconciliation.SweepResult{
		Runs: []reconciliation.Result{
			{Run: domain.ReconciliationRun{ExecutionAccountID: "main", Status: domain.ReconciliationRunCompleted}},
			{Run: domain.ReconciliationRun{ExecutionAccountID: "wallet-1", Status: domain.ReconciliationRunFailed},
				Issues: []domain.ReconciliationIssue{{}}},
			// An unexpired RUNNING row makes Start fail; the run keeps RUNNING.
			{Run: domain.ReconciliationRun{ExecutionAccountID: "wallet-6", Status: domain.ReconciliationRunRunning}},
			{Run: domain.ReconciliationRun{ExecutionAccountID: "wallet-7", Status: domain.ReconciliationRunAttentionRequired,
				Error: "persisted"}},
			// A run that never started has no identity; Sweep keeps account order.
			{},
		},
		Errors: []error{
			errors.New("reconcile wallet-6: insert reconciliation run: duplicate key value violates unique constraint"),
			errors.New("reconcile wallet-7: a different, unpersisted failure"),
			errors.New("reconcile wallet-8: timed out waiting for the execution account lock; reconciliation did not run"),
		},
	}
	failed, err := validateStartupReconciliation(result, []string{"main", "wallet-1", "wallet-6", "wallet-7", "wallet-8"})
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]string, len(failed))
	for _, failure := range failed {
		got[failure.accountID] = failure.detail
	}
	if len(got) != 4 {
		t.Fatalf("failed accounts = %#v, want wallet-1, wallet-6, wallet-7, wallet-8", failed)
	}
	if !strings.Contains(got["wallet-1"], "status FAILED") ||
		!strings.Contains(got["wallet-6"], "status RUNNING") || !strings.Contains(got["wallet-6"], "unique constraint") ||
		!strings.Contains(got["wallet-7"], "unpersisted failure") || !strings.Contains(got["wallet-8"], "NOT_RUN") {
		t.Fatalf("failure details = %#v", got)
	}
	if _, healthy := got["main"]; healthy {
		t.Fatal("a completed account was degraded")
	}
}

func TestPartitionReconciliationAccountsQuarantinesOneOfFour(t *testing.T) {
	active, quarantined, err := partitionReconciliationAccounts(
		[]string{"main", "wallet-1", "wallet-6", "wallet-7"},
		[]string{"wallet-7"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"main", "wallet-1", "wallet-6"}; !reflect.DeepEqual(active, want) {
		t.Fatalf("active accounts = %#v, want %#v", active, want)
	}
	if want := []string{"wallet-7"}; !reflect.DeepEqual(quarantined, want) {
		t.Fatalf("quarantined accounts = %#v, want %#v", quarantined, want)
	}
}

func TestLiveAccountAuthorizationsFollowConfiguredPolymarketAccounts(t *testing.T) {
	bindings := []domain.StrategyExecutionBinding{
		{ModelID: "v4.1 flash", StrategyID: "brier_mid_edge_once", ExecutionAccountID: "main"},
		{ModelID: "v4.1 flash", StrategyID: "strategy_pending_b", ExecutionAccountID: "wallet-1"},
		{ModelID: "v4.1 flash", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "wallet-6"},
		{ModelID: "v4.1 flash", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet-7"},
		{ModelID: "v4.1 flash", StrategyID: "brier_mid_edge_once", ExecutionAccountID: "kalshi:main", MarketSource: domain.MarketSourceKalshi},
	}
	authorizations, err := liveAccountAuthorizations([]string{"main", "wallet-1", "wallet-6", "wallet-7"}, bindings)
	if err != nil {
		t.Fatal(err)
	}
	want := []postgresadapter.ExpectedActiveExecutionAccount{
		{ExecutionAccountID: "main", StrategyID: "brier_mid_edge_once"},
		{ExecutionAccountID: "wallet-1", StrategyID: "strategy_pending_b"},
		{ExecutionAccountID: "wallet-6", StrategyID: domain.StrategyIDMultfactorV1},
		{ExecutionAccountID: "wallet-7", StrategyID: domain.StrategyIDMultfactorV2},
	}
	if !reflect.DeepEqual(authorizations, want) {
		t.Fatalf("active authorizations = %#v, want %#v", authorizations, want)
	}
	// Any model and any account set: no masked whitelist, no fixed wallets.
	for i := range bindings {
		bindings[i].ModelID = "kimi-k2"
	}
	if _, err := liveAccountAuthorizations([]string{"wallet-6"}, bindings); err != nil {
		t.Fatalf("model change broke authorization: %v", err)
	}
	if _, err := liveAccountAuthorizations([]string{"wallet-2"}, bindings); err == nil ||
		!strings.Contains(err.Error(), "no configured Polymarket account row") {
		t.Fatalf("retired authorization error = %v", err)
	}
	if _, err := liveAccountAuthorizations([]string{"kalshi:main"}, bindings); err == nil {
		t.Fatal("a Kalshi account must never be authorized as a Polymarket wallet")
	}
	if _, err := liveAccountAuthorizations([]string{"main", "main"}, bindings); err == nil {
		t.Fatal("duplicated active account accepted")
	}
	if _, err := liveAccountAuthorizations(nil, bindings); err == nil {
		t.Fatal("empty active account set accepted")
	}
}

func TestPartitionReconciliationAccountsFailsClosed(t *testing.T) {
	tests := []struct {
		name        string
		configured  []string
		quarantined []string
		want        string
	}{
		{name: "unknown", configured: []string{"wallet-1"}, quarantined: []string{"wallet-7"}, want: "not configured"},
		{name: "duplicate", configured: []string{"wallet-1", "wallet-6"}, quarantined: []string{"wallet-6", "wallet-6"}, want: "duplicated"},
		{name: "all", configured: []string{"wallet-7"}, quarantined: []string{"wallet-7"}, want: "cannot exclude every"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := partitionReconciliationAccounts(test.configured, test.quarantined)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("partitionReconciliationAccounts() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestPreflightLiveAccountAllowsUnfundedQuarantine(t *testing.T) {
	client := &livePreflightProbeClient{
		account:    polymarket.AccountProbe{ExecutionAccountID: "wallet-6", FunderAddress: "0xfunder"},
		fundingErr: errors.New("funding probe must be skipped"),
	}
	eligibility := &livePreflightEligibility{err: errors.New("eligibility probe must be skipped")}

	result, err := preflightLiveAccount(context.Background(), "wallet-6", true, client, eligibility)
	if err != nil {
		t.Fatal(err)
	}
	if result.account.FunderAddress != "0xfunder" || result.funding != nil || result.eligibility != nil {
		t.Fatalf("quarantined preflight result = %#v", result)
	}
	if want := []string{"wallet-6"}; !reflect.DeepEqual(client.accountCalls, want) {
		t.Fatalf("account probe calls = %#v, want %#v", client.accountCalls, want)
	}
	if len(client.fundingCalls) != 0 || len(eligibility.calls) != 0 {
		t.Fatalf("quarantined trading-capability probes were called: funding=%#v eligibility=%#v", client.fundingCalls, eligibility.calls)
	}
}

func TestPreflightLiveAccountRejectsQuarantinedOpenOrder(t *testing.T) {
	client := &livePreflightProbeClient{
		account: polymarket.AccountProbe{
			ExecutionAccountID: "wallet-6", FunderAddress: "0xfunder", OpenOrderCount: 1,
		},
	}
	eligibility := &livePreflightEligibility{}

	_, err := preflightLiveAccount(context.Background(), "wallet-6", true, client, eligibility)
	if err == nil || !strings.Contains(err.Error(), "has 1 open CLOB order") {
		t.Fatalf("preflightLiveAccount() error = %v, want quarantined open-order rejection", err)
	}
	var accountErr *liveAccountPreflightError
	if errors.As(err, &accountErr) {
		t.Fatal("the inert quarantine rejection must stay fatal, not a maintenance reason")
	}
	if len(client.fundingCalls) != 0 || len(eligibility.calls) != 0 {
		t.Fatalf("rejected quarantine ran trading-capability probes: funding=%#v eligibility=%#v", client.fundingCalls, eligibility.calls)
	}
}

// TestPreflightLiveAccountAcceptsZeroCashActiveAccount covers design D2: an
// ACTIVE account fully invested in positions (zero pUSD cash) no longer
// blocks startup. Missing allowances still fail.
func TestPreflightLiveAccountAcceptsZeroCashActiveAccount(t *testing.T) {
	client := &livePreflightProbeClient{
		account: polymarket.AccountProbe{ExecutionAccountID: "main", FunderAddress: "0xfunder"},
		funding: polymarket.FundingProbe{
			ExecutionAccountID: "main", CollateralBalancePositive: false, RequiredAllowancesPositive: true,
		},
	}
	eligibility := &livePreflightEligibility{}
	result, err := preflightLiveAccount(context.Background(), "main", false, client, eligibility)
	if err != nil || result.funding == nil || result.eligibility == nil {
		t.Fatalf("preflightLiveAccount() = %#v, error %v, want a zero-cash ACTIVE account accepted", result, err)
	}
	client.funding.RequiredAllowancesPositive = false
	_, err = preflightLiveAccount(context.Background(), "main", false, client, eligibility)
	assertPreflightReason(t, err, accountmaintenance.ReasonPreflightAllowanceMissing, "missing a positive pUSD allowance")
}

func assertPreflightReason(t *testing.T, err error, reason, text string) {
	t.Helper()
	var accountErr *liveAccountPreflightError
	if !errors.As(err, &accountErr) || accountErr.reason != reason || !strings.Contains(err.Error(), text) {
		t.Fatalf("preflight error = %#v (%v), want reason %s containing %q", accountErr, err, reason, text)
	}
}

func TestPreflightLiveAccountClassifiesEveryFailure(t *testing.T) {
	healthyFunding := polymarket.FundingProbe{CollateralBalancePositive: true, RequiredAllowancesPositive: true}
	for _, test := range []struct {
		name        string
		client      *livePreflightProbeClient
		eligibility *livePreflightEligibility
		reason      string
		text        string
	}{
		{"CLOB auth", &livePreflightProbeClient{accountErr: errors.New("401 unauthorized")}, &livePreflightEligibility{},
			accountmaintenance.ReasonPreflightCLOBAuth, "authenticated CLOB preflight"},
		{"funding read", &livePreflightProbeClient{fundingErr: errors.New("502")}, &livePreflightEligibility{},
			accountmaintenance.ReasonPreflightFundingUnavailable, "funding preflight"},
		{"allowance", &livePreflightProbeClient{funding: polymarket.FundingProbe{CollateralBalancePositive: true}}, &livePreflightEligibility{},
			accountmaintenance.ReasonPreflightAllowanceMissing, "allowance"},
		{"eligibility read", &livePreflightProbeClient{funding: healthyFunding}, &livePreflightEligibility{err: errors.New("geoblock down")},
			accountmaintenance.ReasonPreflightEligibilityUnavailable, "eligibility preflight"},
		{"not eligible", &livePreflightProbeClient{funding: healthyFunding},
			&livePreflightEligibility{result: polymarket.GeographicEligibility{Blocked: true, Reason: "closed_only"}},
			accountmaintenance.ReasonPreflightNotEligible, "closed_only"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := preflightLiveAccount(context.Background(), "wallet-7", false, test.client, test.eligibility)
			assertPreflightReason(t, err, test.reason, test.text)
		})
	}
}

// accountKeyedProbeClient fails the preflight of selected accounts.
type accountKeyedProbeClient struct {
	failAccount map[string]error
	calls       []string
}

func (client *accountKeyedProbeClient) ProbeAccount(_ context.Context, accountID string) (polymarket.AccountProbe, error) {
	client.calls = append(client.calls, accountID)
	if err := client.failAccount[accountID]; err != nil {
		return polymarket.AccountProbe{}, err
	}
	return polymarket.AccountProbe{ExecutionAccountID: accountID}, nil
}

func (*accountKeyedProbeClient) ProbeFunding(_ context.Context, accountID string) (polymarket.FundingProbe, error) {
	return polymarket.FundingProbe{ExecutionAccountID: accountID, RequiredAllowancesPositive: true}, nil
}

// TestPreflightLiveAccountsDegradesOnlyTheFailedAccount is the startup half of
// acceptance 1: one PM account fails its CLOB preflight, startup continues,
// that account is in maintenance, and every account (including it) still
// takes part in the global wallet-binding and ledger checks.
func TestPreflightLiveAccountsDegradesOnlyTheFailedAccount(t *testing.T) {
	client := &accountKeyedProbeClient{failAccount: map[string]error{"wallet-7": errors.New("401 invalid api key")}}
	registry := accountmaintenance.New()
	var logs bytes.Buffer
	expected, err := preflightLiveAccounts(liveAccountPreflightParams{
		ctx: context.Background(),
		accounts: []polymarket.TradingAccount{
			{ExecutionAccountID: "main", FunderAddress: "0xAAAA"},
			{ExecutionAccountID: "wallet-7", FunderAddress: " 0xBBBB "},
		},
		client: client, eligibility: &livePreflightEligibility{}, maintenance: registry,
		logger: slog.New(slog.NewJSONHandler(&logs, nil)),
	})
	if err != nil {
		t.Fatalf("preflightLiveAccounts() error = %v, want startup to continue", err)
	}
	want := []postgresadapter.ExpectedExecutionAccount{
		{ExecutionAccountID: "main", WalletAddress: "0xaaaa"},
		{ExecutionAccountID: "wallet-7", WalletAddress: "0xbbbb"},
	}
	if !reflect.DeepEqual(expected, want) {
		t.Fatalf("expected bindings = %#v, want %#v", expected, want)
	}
	if reason, ok := registry.MaintenanceReason("wallet-7"); !ok || reason != accountmaintenance.ReasonPreflightCLOBAuth {
		t.Fatalf("wallet-7 maintenance = %q, %v", reason, ok)
	}
	if _, ok := registry.MaintenanceReason("main"); ok {
		t.Fatal("healthy account entered maintenance")
	}
	// Zero cash on the ACTIVE account is only a warning (design D2).
	for _, text := range []string{"execution account entered maintenance", "PREFLIGHT_CLOB_AUTH",
		"active account has no positive pUSD collateral", "Polymarket live account preflight passed"} {
		if !strings.Contains(logs.String(), text) {
			t.Fatalf("logs do not contain %q:\n%s", text, logs.String())
		}
	}
}

func TestPreflightLiveAccountsStopsOnStartupDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := &accountKeyedProbeClient{failAccount: map[string]error{"main": context.Canceled}}
	_, err := preflightLiveAccounts(liveAccountPreflightParams{
		ctx: ctx, accounts: []polymarket.TradingAccount{{ExecutionAccountID: "main"}},
		client: client, eligibility: &livePreflightEligibility{}, maintenance: accountmaintenance.New(),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("preflightLiveAccounts() error = %v, want the startup deadline to stay fatal", err)
	}
}

type failingProtocolClient struct{ err error }

func (client failingProtocolClient) ProbeProtocol(context.Context, time.Duration) (polymarket.ProtocolProbe, error) {
	return polymarket.ProtocolProbe{Version: 2}, client.err
}

// TestPreflightLiveProtocolFailureStaysFatal covers design D4: the global
// CLOB protocol preflight is not degraded to account maintenance.
func TestPreflightLiveProtocolFailureStaysFatal(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := preflightLiveProtocol(context.Background(), failingProtocolClient{err: errors.New("CLOB /ok probe: 503")}, time.Second, logger)
	if err == nil || !strings.Contains(err.Error(), "Polymarket CLOB V2 protocol preflight") {
		t.Fatalf("preflightLiveProtocol() = %v, want a fatal startup error", err)
	}
	var accountErr *liveAccountPreflightError
	if errors.As(err, &accountErr) {
		t.Fatal("protocol failure must not be classified as an account-local preflight failure")
	}
	if err := preflightLiveProtocol(context.Background(), failingProtocolClient{}, time.Second, logger); err != nil {
		t.Fatalf("healthy protocol preflight = %v", err)
	}
}
