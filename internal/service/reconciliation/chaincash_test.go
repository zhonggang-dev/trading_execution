package reconciliation

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/chaincash"
)

type fakeChainCash struct {
	result chaincash.SyncResult
	err    error
	calls  []chaincash.SyncParams
}

func (ledger *fakeChainCash) Sync(_ context.Context, params chaincash.SyncParams) (chaincash.SyncResult, error) {
	ledger.calls = append(ledger.calls, params)
	return ledger.result, ledger.err
}

func enabledChainCash(ledgerTotal, offset domain.Decimal) *fakeChainCash {
	return &fakeChainCash{result: chaincash.SyncResult{
		Enabled: true, CaughtUp: true, ProcessedBlock: 936, ConfirmedBlock: 936,
		LedgerTotal: ledgerTotal, PersistedOffset: offset, PendingOffset: "0", Offset: offset,
	}}
}

func chainCashTestService(t *testing.T, ledger *fakeLedger, chain domain.Decimal, cash ChainCashLedger) *Service {
	t.Helper()
	return newTestService(t, Params{
		Orders: &fakeOrders{}, Venue: &fakeVenue{}, Ledger: ledger,
		Fills: &fakeFills{}, OrderRefresher: &fakeRefresher{}, ChainCash: cash,
		PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
			return nil, nil
		})},
		BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
			return domain.ExternalBalance{Asset: "USDC", Amount: chain, Source: "EVM_ERC20_ETH_CALL", BlockNumber: 1000, ObservedAt: testNow}, nil
		})},
	})
}

func runChainCashAccount(t *testing.T, service *Service) Result {
	t.Helper()
	result, err := service.RunAccount(context.Background(), RunAccountParams{ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled})
	if err != nil && result.Run.Status == "" {
		t.Fatalf("RunAccount() error = %v", err)
	}
	return result
}

func issuesOfType(issues []domain.ReconciliationIssue, issueType domain.ReconciliationIssueType) []domain.ReconciliationIssue {
	result := make([]domain.ReconciliationIssue, 0)
	for _, issue := range issues {
		if issue.Type == issueType {
			result = append(result, issue)
		}
	}
	return result
}

func TestChainCashBalanceUsesPostCreditLedgerTotal(t *testing.T) {
	// The ledger read at the start of the run still shows 1.700022; the credit
	// transaction booked the 0.0015 reward and returned 1.701522.
	cash := enabledChainCash("1.701522", "0")
	service := chainCashTestService(t, &fakeLedger{balance: testBalance("1.700022")}, "1.701522", cash)
	result := runChainCashAccount(t, service)
	if result.Run.Status != domain.ReconciliationRunCompleted || len(result.Issues) != 0 {
		t.Fatalf("run = %s issues = %#v", result.Run.Status, result.Issues)
	}
	if result.Run.Summary["verified_balance:"] != 1 || result.Run.Summary["verified_source:"+chainCashSource] != 1 {
		t.Fatalf("summary = %#v", result.Run.Summary)
	}
	if len(cash.calls) != 1 || cash.calls[0].SnapshotBlock != 1000 || cash.calls[0].ExecutionAccountID != "account-1" {
		t.Fatalf("sync calls = %#v", cash.calls)
	}
}

func TestChainCashBalanceDirectionalOutcomes(t *testing.T) {
	for _, test := range []struct {
		name       string
		chain      domain.Decimal
		offset     domain.Decimal
		wantIssue  bool
		resolution domain.ReconciliationResolution
		impact     domain.ReconciliationImpactScope
		local      domain.Decimal
	}{
		{name: "reconciled with known unattributed inflow", chain: "105", offset: "5"},
		{name: "more money than expected is observation only", chain: "100.5", offset: "0", wantIssue: true,
			resolution: domain.ReconciliationResolutionObserved, impact: domain.ReconciliationImpactNone, local: "100"},
		{name: "less money than expected blocks the account", chain: "99.5", offset: "0", wantIssue: true,
			resolution: domain.ReconciliationResolutionManual, impact: domain.ReconciliationImpactAccount, local: "100"},
		{name: "an unexplained outflow offset still exposes a further deficit", chain: "97", offset: "-2", wantIssue: true,
			resolution: domain.ReconciliationResolutionManual, impact: domain.ReconciliationImpactAccount, local: "98"},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := chainCashTestService(t, &fakeLedger{balance: testBalance("100")}, test.chain, enabledChainCash("100", test.offset))
			result := runChainCashAccount(t, service)
			drifts := issuesOfType(result.Issues, domain.ReconciliationIssueBalanceDrift)
			if !test.wantIssue {
				if len(drifts) != 0 || result.Run.Summary["verified_balance:"] != 1 {
					t.Fatalf("drifts = %#v summary = %#v", drifts, result.Run.Summary)
				}
				return
			}
			if len(drifts) != 1 {
				t.Fatalf("drifts = %#v", drifts)
			}
			drift := drifts[0]
			if drift.Resolution != test.resolution || drift.ImpactScope != test.impact || !drift.LocalValue.Equal(test.local) ||
				!drift.RemoteValue.Equal(test.chain) || drift.RemoteBlockNumber != 1000 || drift.Source != "EVM_ERC20_ETH_CALL" {
				t.Fatalf("drift = %#v", drift)
			}
			if result.Run.Summary["verified_balance:"] != 0 {
				t.Fatal("drift run verified the balance")
			}
			if test.impact == domain.ReconciliationImpactAccount && !result.Impact.AccountWide {
				t.Fatalf("impact = %#v", result.Impact)
			}
			if test.impact == domain.ReconciliationImpactNone && result.Impact.AccountWide {
				t.Fatalf("surplus blocked the account: %#v", result.Impact)
			}
		})
	}
}

func TestChainCashDeficitExplainedByFinalityPendingFillIsNotBlocked(t *testing.T) {
	ledger := &fakeLedger{
		balance:   testBalance("100"),
		positions: []domain.Position{openPosition("token-1", "10")},
		finalityPending: []domain.Fill{
			finalityPendingFill("buy-1", "token-1", domain.SideBuy, "5", "-2.6", testNow.Add(-time.Minute)),
		},
	}
	service := newTestService(t, Params{
		Orders: &fakeOrders{}, Venue: &fakeVenue{}, Ledger: ledger, Fills: &fakeFills{}, OrderRefresher: &fakeRefresher{},
		ChainCash: enabledChainCash("100", "0.5"),
		PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
			return []domain.ExternalPosition{{ConditionID: "condition-1", TokenID: "token-1", OutcomeName: "YES", Shares: "15", Source: "POLYMARKET_DATA_API", ObservedAt: testNow}}, nil
		})},
		BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
			// ledger 100 + known offset 0.5 - pending BUY 2.6
			return domain.ExternalBalance{Asset: "USDC", Amount: "97.9", Source: "EVM_ERC20_ETH_CALL", BlockNumber: 1000, ObservedAt: testNow}, nil
		})},
	})
	result := runChainCashAccount(t, service)
	assertNoIssue(t, result.Issues, domain.ReconciliationIssueBalanceDrift)
	if result.Run.Summary["balance_explained_by_finality_pending_fills"] != 1 {
		t.Fatalf("summary = %#v", result.Run.Summary)
	}
}

func TestChainCashSyncFailureFailsClosedWithoutComparing(t *testing.T) {
	for _, test := range []struct {
		name string
		cash *fakeChainCash
	}{
		{name: "log read failure", cash: &fakeChainCash{result: chaincash.SyncResult{Enabled: true}, err: errors.New("eth_getLogs chunk [1001, 1100] failed")}},
		{name: "catching up", cash: &fakeChainCash{result: chaincash.SyncResult{
			Enabled: true, CaughtUp: false, ProcessedBlock: 4000, ConfirmedBlock: 9000, LedgerTotal: "100", Offset: "0",
		}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := chainCashTestService(t, &fakeLedger{balance: testBalance("100")}, "90", test.cash)
			result := runChainCashAccount(t, service)
			assertNoIssue(t, result.Issues, domain.ReconciliationIssueBalanceDrift)
			unavailable := issuesOfType(result.Issues, domain.ReconciliationIssueSourceUnavailable)
			if len(unavailable) != 1 || unavailable[0].Source != chainCashSource ||
				unavailable[0].ImpactScope != domain.ReconciliationImpactAccount || !result.Impact.AccountWide {
				t.Fatalf("unavailable = %#v impact = %#v", unavailable, result.Impact)
			}
			if result.Run.Summary["verified_balance:"] != 0 || result.Run.Summary["verified_source:"+chainCashSource] != 0 {
				t.Fatalf("summary = %#v", result.Run.Summary)
			}
		})
	}
}

func TestChainCashUnreportedTransfersBecomeAuditIssues(t *testing.T) {
	cash := enabledChainCash("100", "0.5")
	cash.result.Unreported = []domain.ChainCashTransfer{
		{ExecutionAccountID: "account-1", TransactionHash: "0x" + strings.Repeat("a", 64), LogIndex: 3, BlockNumber: 900,
			Direction: domain.ChainCashIn, Counterparty: "0x" + strings.Repeat("9", 40), Amount: "0.75", Classification: domain.ChainCashUnattributedIn},
		{ExecutionAccountID: "account-1", TransactionHash: "0x" + strings.Repeat("b", 64), LogIndex: 4, BlockNumber: 901,
			Direction: domain.ChainCashOut, Counterparty: "0x" + strings.Repeat("8", 40), Amount: "0.25", Classification: domain.ChainCashUnattributedOut},
	}
	service := chainCashTestService(t, &fakeLedger{balance: testBalance("100")}, "100.5", cash)
	result := runChainCashAccount(t, service)
	in := issuesOfType(result.Issues, domain.ReconciliationIssueUnattributedCashIn)
	out := issuesOfType(result.Issues, domain.ReconciliationIssueUnattributedCashOut)
	if len(in) != 1 || in[0].Resolution != domain.ReconciliationResolutionObserved || in[0].ImpactScope != domain.ReconciliationImpactNone ||
		in[0].VenueTradeID != "0x"+strings.Repeat("a", 64)+":3" || !in[0].RemoteValue.Equal("0.75") {
		t.Fatalf("in = %#v", in)
	}
	if len(out) != 1 || out[0].Resolution != domain.ReconciliationResolutionManual || out[0].ImpactScope != domain.ReconciliationImpactAccount ||
		out[0].VenueTradeID != "0x"+strings.Repeat("b", 64)+":4" {
		t.Fatalf("out = %#v", out)
	}
	if in[0].Fingerprint == out[0].Fingerprint {
		t.Fatal("audit issues must have per-transfer identities")
	}
	// The balance itself reconciles; only the outgoing transfer gates trading.
	if result.Run.Summary["verified_balance:"] != 1 || !result.Impact.AccountWide {
		t.Fatalf("summary=%#v impact=%#v", result.Run.Summary, result.Impact)
	}
}

func TestChainCashDisabledAccountKeepsLegacyComparison(t *testing.T) {
	cash := &fakeChainCash{} // no cursor: Enabled=false
	service := chainCashTestService(t, &fakeLedger{balance: testBalance("100")}, "100.5", cash)
	result := runChainCashAccount(t, service)
	drifts := issuesOfType(result.Issues, domain.ReconciliationIssueBalanceDrift)
	if len(cash.calls) != 1 || len(drifts) != 1 || drifts[0].Resolution != domain.ReconciliationResolutionManual ||
		drifts[0].ImpactScope != domain.ReconciliationImpactAccount || !drifts[0].LocalValue.Equal("100") {
		t.Fatalf("legacy drift = %#v", drifts)
	}
	if result.Run.Summary["verified_source:"+chainCashSource] != 0 {
		t.Fatalf("disabled ledger marked verified: %#v", result.Run.Summary)
	}
}

func TestBalanceDriftFingerprintExcludesAmounts(t *testing.T) {
	base := domain.ReconciliationIssue{
		Type: domain.ReconciliationIssueBalanceDrift, ExecutionAccountID: "wallet-6", Source: "EVM_ERC20_ETH_CALL",
		LocalValue: "1.700022", RemoteValue: "1.701522",
	}
	changed := base
	changed.LocalValue, changed.RemoteValue = "1.701522", "1.703022"
	if issueFingerprint(base) != issueFingerprint(changed) {
		t.Fatal("balance drift fingerprint depends on the observed amounts")
	}
	otherSource := base
	otherSource.Source = "ORDER_RECOVERY"
	if issueFingerprint(base) == issueFingerprint(otherSource) {
		t.Fatal("balance drift fingerprint ignores the source")
	}
	position := domain.ReconciliationIssue{Type: domain.ReconciliationIssuePositionDrift, ExecutionAccountID: "wallet-6", TokenID: "1", LocalValue: "1", RemoteValue: "2"}
	positionChanged := position
	positionChanged.RemoteValue = "3"
	if issueFingerprint(position) == issueFingerprint(positionChanged) {
		t.Fatal("other issue types must keep amount-bound fingerprints")
	}
}
