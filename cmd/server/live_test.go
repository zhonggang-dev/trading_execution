package main

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/domain"
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
	if err := validateStartupReconciliation(completed, 1); err != nil {
		t.Fatalf("validate completed startup reconciliation: %v", err)
	}
	attention := reconciliation.SweepResult{Runs: []reconciliation.Result{{
		Run: domain.ReconciliationRun{
			ExecutionAccountID: "main",
			Status:             domain.ReconciliationRunAttentionRequired,
		},
		Issues: []domain.ReconciliationIssue{{}},
	}}}
	if err := validateStartupReconciliation(attention, 1); err != nil {
		t.Fatalf("validate account-local attention startup reconciliation: %v", err)
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
	if err := validateStartupReconciliation(attentionWithPersistedError, 1); err != nil {
		t.Fatalf("validate durably gated account-local startup error: %v", err)
	}

	tests := []struct {
		name   string
		result reconciliation.SweepResult
		want   string
	}{
		{
			name:   "missing account run",
			result: reconciliation.SweepResult{},
			want:   "want 1",
		},
		{
			name: "source error",
			result: reconciliation.SweepResult{
				Runs:   completed.Runs,
				Errors: []error{errors.New("position source unavailable")},
			},
			want: "position source unavailable",
		},
		{
			name: "attention error does not match durable run",
			result: reconciliation.SweepResult{
				Runs: attentionWithPersistedError.Runs,
				Errors: []error{
					errors.New("reconcile wallet-6: different failure"),
				},
			},
			want: "different failure",
		},
		{
			name: "failed run",
			result: reconciliation.SweepResult{Runs: []reconciliation.Result{{
				Run: domain.ReconciliationRun{
					ExecutionAccountID: "wallet-1",
					Status:             domain.ReconciliationRunFailed,
				},
				Issues: []domain.ReconciliationIssue{{}},
			}}},
			want: "failed closed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateStartupReconciliation(test.result, 1)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateStartupReconciliation() = %v, want error containing %q", err, test.want)
			}
		})
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

	result, err := preflightLiveAccount(
		context.Background(), "wallet-6", true, false, client, eligibility,
	)
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

	_, err := preflightLiveAccount(
		context.Background(), "wallet-6", true, false, client, eligibility,
	)
	if err == nil || !strings.Contains(err.Error(), "has 1 open CLOB order") {
		t.Fatalf("preflightLiveAccount() error = %v, want quarantined open-order rejection", err)
	}
	if len(client.fundingCalls) != 0 || len(eligibility.calls) != 0 {
		t.Fatalf("rejected quarantine ran trading-capability probes: funding=%#v eligibility=%#v", client.fundingCalls, eligibility.calls)
	}
}

func TestPreflightLiveAccountAllowsEmptyBuyDisabledAccount(t *testing.T) {
	client := &livePreflightProbeClient{
		account: polymarket.AccountProbe{ExecutionAccountID: "wallet-1", FunderAddress: "0xfunder"},
		funding: polymarket.FundingProbe{
			ExecutionAccountID: "wallet-1", CollateralBalancePositive: false, RequiredAllowancesPositive: true,
		},
	}
	eligibility := &livePreflightEligibility{}
	result, err := preflightLiveAccount(context.Background(), "wallet-1", false, true, client, eligibility)
	if err != nil || result.funding == nil || result.eligibility == nil {
		t.Fatalf("preflightLiveAccount() = %#v, error %v, want an empty BUY-disabled account accepted", result, err)
	}
	// Missing allowances still fail: only the balance requirement is relaxed.
	client.funding.RequiredAllowancesPositive = false
	if _, err := preflightLiveAccount(context.Background(), "wallet-1", false, true, client, eligibility); err == nil ||
		!strings.Contains(err.Error(), "missing a positive pUSD allowance") {
		t.Fatalf("preflightLiveAccount() without allowance error = %v", err)
	}
}

func TestPreflightLiveAccountRejectsUnfundedActiveAccount(t *testing.T) {
	client := &livePreflightProbeClient{
		account: polymarket.AccountProbe{ExecutionAccountID: "main", FunderAddress: "0xfunder"},
		funding: polymarket.FundingProbe{
			ExecutionAccountID: "main", CollateralBalancePositive: false,
			RequiredAllowancesPositive: false,
		},
	}
	eligibility := &livePreflightEligibility{}

	_, err := preflightLiveAccount(
		context.Background(), "main", false, false, client, eligibility,
	)
	if err == nil || !strings.Contains(err.Error(), "has no positive CLOB pUSD collateral balance") {
		t.Fatalf("preflightLiveAccount() error = %v, want active collateral rejection", err)
	}
	if want := []string{"main"}; !reflect.DeepEqual(client.fundingCalls, want) {
		t.Fatalf("funding probe calls = %#v, want %#v", client.fundingCalls, want)
	}
	if len(eligibility.calls) != 0 {
		t.Fatalf("eligibility calls = %#v, want none after funding rejection", eligibility.calls)
	}
}
