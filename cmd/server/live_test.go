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

func TestCurrentLiveReleaseRejectsRetiredWalletOrUnsupportedQuarantineBeforePreflight(t *testing.T) {
	tests := []struct {
		name        string
		configured  []string
		quarantined []string
		want        string
	}{
		{
			name:        "decision cycle disabled with retired wallets",
			configured:  []string{"main", "wallet-1", "wallet-2", "wallet-3"},
			quarantined: []string{"wallet-6", "wallet-7"},
			want:        "wallet file must contain exactly",
		},
		{
			name:        "wallet remains quarantined",
			configured:  []string{"main", "wallet-1", "wallet-6", "wallet-7"},
			quarantined: []string{"wallet-6"},
			want:        "empty submission-disabled account list",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &livePreflightProbeClient{}
			err := validateCurrentLiveWallet67Release(test.configured, test.quarantined)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateCurrentLiveWallet67Release() error = %v, want %q", err, test.want)
			}
			if len(client.accountCalls) != 0 || len(client.fundingCalls) != 0 {
				t.Fatalf("release validation reached venue probes: %#v/%#v", client.accountCalls, client.fundingCalls)
			}
		})
	}
}

func TestCurrentLiveReleaseAcceptsOnlyEmptyQuarantine(t *testing.T) {
	configured := []string{"main", "wallet-1", "wallet-6", "wallet-7"}
	if err := validateCurrentLiveWallet67Release(configured, nil); err != nil {
		t.Fatalf("validateCurrentLiveWallet67Release() error = %v", err)
	}
}

func TestCurrentLiveWallet67AuthorizationsExcludeRetiredAccounts(t *testing.T) {
	authorizations, err := currentLiveWallet67Authorizations(
		[]string{"main", "wallet-1", "wallet-6", "wallet-7"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(authorizations) != 4 ||
		authorizations[2].ExecutionAccountID != "wallet-6" ||
		authorizations[2].ModelID != "gemini_masked" ||
		authorizations[2].StrategyID != domain.StrategyIDMultfactorV1 ||
		authorizations[3].ExecutionAccountID != "wallet-7" ||
		authorizations[3].StrategyID != domain.StrategyIDMultfactorV2 {
		t.Fatalf("active authorizations = %#v", authorizations)
	}
	if _, err := currentLiveWallet67Authorizations([]string{"wallet-2"}); err == nil ||
		!strings.Contains(err.Error(), "no current live route") {
		t.Fatalf("retired authorization error = %v", err)
	}
}

func TestCurrentLiveReleaseKeepsAllFourAccountsActive(t *testing.T) {
	configured := []string{"main", "wallet-1", "wallet-6", "wallet-7"}
	var quarantined []string
	if err := validateCurrentLiveWallet67Release(configured, quarantined); err != nil {
		t.Fatal(err)
	}
	active, _, err := partitionReconciliationAccounts(configured, quarantined)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"main", "wallet-1", "wallet-6", "wallet-7"}; !reflect.DeepEqual(active, want) {
		t.Fatalf("automatic operations accounts = %#v, want %#v", active, want)
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
		context.Background(), "wallet-6", true, client, eligibility,
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
		context.Background(), "wallet-6", true, client, eligibility,
	)
	if err == nil || !strings.Contains(err.Error(), "has 1 open CLOB order") {
		t.Fatalf("preflightLiveAccount() error = %v, want quarantined open-order rejection", err)
	}
	if len(client.fundingCalls) != 0 || len(eligibility.calls) != 0 {
		t.Fatalf("rejected quarantine ran trading-capability probes: funding=%#v eligibility=%#v", client.fundingCalls, eligibility.calls)
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
		context.Background(), "main", false, client, eligibility,
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
