package reconciliation

import (
	"context"
	"errors"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

func newBaselineDustTestService(
	t *testing.T,
	ledger *fakeLedger,
	baseline domain.Decimal,
	remote []domain.ExternalPosition,
	chain knownBalanceFunc,
) *Service {
	t.Helper()
	return newTestService(t, Params{
		Orders: &fakeOrders{}, Venue: &fakeVenue{}, Ledger: ledger,
		Fills: &fakeFills{}, OrderRefresher: &fakeRefresher{},
		PositionBaselines: positionBaselineSourceFunc(func(context.Context, string) ([]domain.ExternalPositionBaseline, error) {
			return []domain.ExternalPositionBaseline{testExternalPositionBaseline(baseline)}, nil
		}),
		PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
			return append([]domain.ExternalPosition(nil), remote...), nil
		})},
		BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
			return domain.ExternalBalance{Asset: "USDC", Amount: "100", Source: "CHAIN", ObservedAt: testNow}, nil
		})},
		KnownPositionBalances: chain,
	})
}

func TestOmittedBaselineDustRequiresExactChainEvidence(t *testing.T) {
	tests := []struct {
		name       string
		baseline   domain.Decimal
		managed    domain.Decimal
		chain      domain.Decimal
		chainErr   error
		wantCalls  int
		wantStatus domain.ReconciliationRunStatus
		wantIssue  domain.ReconciliationIssueType
		wantRemote domain.Decimal
		wantSource string
	}{
		{name: "exact baseline dust", baseline: "0.02", chain: "0.020000", wantCalls: 1, wantStatus: domain.ReconciliationRunCompleted},
		{name: "exact baseline plus managed", baseline: "0.025174", managed: "0.05", chain: "0.075174", wantCalls: 1, wantStatus: domain.ReconciliationRunCompleted},
		// Production shape: the residual sits beside a CLOSED zero-share managed row.
		{name: "exact beside closed managed row", baseline: "0.02", managed: "0", chain: "0.02", wantCalls: 1, wantStatus: domain.ReconciliationRunCompleted},
		{name: "cap is inclusive", baseline: "0.1", chain: "0.1", wantCalls: 1, wantStatus: domain.ReconciliationRunCompleted},
		{name: "chain differs", baseline: "0.02", chain: "0.019999", wantCalls: 1, wantStatus: domain.ReconciliationRunAttentionRequired,
			wantIssue: domain.ReconciliationIssueExternalPositionBaselineDrift, wantRemote: "0.019999", wantSource: "EVM_ERC1155_ETH_CALL"},
		// Without any local position an infrastructure error makes the run FAILED;
		// either way it must not complete and the issue stays token scoped.
		{name: "chain unavailable", baseline: "0.02", chainErr: errors.New("RPC offline"), wantCalls: 1,
			wantIssue: domain.ReconciliationIssueSourceUnavailable, wantSource: "EVM_ERC1155_ETH_CALL"},
		{name: "above cap stays a drift", baseline: "0.1000001", chain: "0.1000001", wantCalls: 0, wantStatus: domain.ReconciliationRunAttentionRequired,
			wantIssue: domain.ReconciliationIssueExternalPositionBaselineDrift, wantRemote: "0", wantSource: "DATA_API"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ledger := &fakeLedger{balance: testBalance("100")}
			if test.managed != "" {
				lifecycle := domain.PositionLifecycleOpen
				if test.managed == "0" {
					lifecycle = domain.PositionLifecycleClosed
				}
				ledger.positions = []domain.Position{{
					ExecutionAccountID: "account-1", MarketID: "market-1", ConditionID: "condition-1",
					TokenID: "token-1", OutcomeIndex: intPointer(0), OutcomeName: "YES",
					TotalShares: test.managed, AvailableShares: test.managed, ReservedShares: "0",
					LifecycleStatus: lifecycle,
				}}
			}
			calls := 0
			service := newBaselineDustTestService(t, ledger, test.baseline, nil, func(_ context.Context, wallet, token string) (domain.Decimal, error) {
				calls++
				if token != "token-1" {
					t.Fatalf("chain read for token %q", token)
				}
				return test.chain, test.chainErr
			})
			result, _ := service.RunAccount(context.Background(), RunAccountParams{
				ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled,
			})
			statusOK := result.Run.Status == test.wantStatus
			if test.wantStatus == "" {
				statusOK = result.Run.Status != domain.ReconciliationRunCompleted
			}
			if !statusOK || calls != test.wantCalls {
				t.Fatalf("status = %s, calls = %d, issues = %#v; want %s and %d calls", result.Run.Status, calls, result.Issues, test.wantStatus, test.wantCalls)
			}
			if test.wantIssue == "" {
				if len(result.Issues) != 0 || result.Run.Summary["baseline_dust_verified_onchain"] != 1 ||
					result.Run.Summary["verified_position:token-1"] != 1 {
					t.Fatalf("issues = %#v, summary = %#v; want a verified token", result.Issues, result.Run.Summary)
				}
				if test.managed != "" && (ledger.positions[0].TotalShares != test.managed || len(ledger.settled) != 0) {
					t.Fatalf("verification changed managed state: %#v %#v", ledger.positions, ledger.settled)
				}
				return
			}
			issue, found := findIssue(result.Issues, test.wantIssue)
			if !found || issue.Source != test.wantSource || issue.TokenID != "token-1" || issue.ImpactScope != domain.ReconciliationImpactToken ||
				(test.wantRemote != "" && !issue.RemoteValue.Equal(test.wantRemote)) {
				t.Fatalf("issues = %#v; want %s from %s with remote %s", result.Issues, test.wantIssue, test.wantSource, test.wantRemote)
			}
			if result.Run.Summary["verified_position:token-1"] != 0 {
				t.Fatalf("unverified token marked verified: %#v", result.Run.Summary)
			}
		})
	}
}

func TestBaselineDustPresentInSnapshotSkipsChainRead(t *testing.T) {
	service := newBaselineDustTestService(t, &fakeLedger{balance: testBalance("100")}, "0.02",
		[]domain.ExternalPosition{testExternalPosition("0.02")},
		func(context.Context, string, string) (domain.Decimal, error) {
			t.Fatal("chain read for a token present in the snapshot")
			return "", nil
		})
	result, err := service.RunAccount(context.Background(), RunAccountParams{
		ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled,
	})
	if err != nil || result.Run.Status != domain.ReconciliationRunCompleted || len(result.Issues) != 0 {
		t.Fatalf("RunAccount() = %#v, %v", result, err)
	}
}
