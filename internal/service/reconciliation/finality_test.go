package reconciliation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// finalityPendingFill builds a CLOB-confirmed fill whose Polygon receipt is
// canonical but still below the configured confirmation depth.
func finalityPendingFill(id, tokenID string, side domain.Side, shares, netCash domain.Decimal, matchedAt time.Time) domain.Fill {
	return domain.Fill{
		Key: "fill-" + id, Venue: "polymarket", VenueFillID: id, OrderID: "order-" + id, VenueOrderID: "0xvenue-" + id,
		ExecutionAccountID: "account-1", MarketID: "market-1", ConditionID: "condition-1", TokenID: tokenID,
		Side: side, LiquidityRole: domain.LiquidityRoleTaker, Status: domain.FillStatusMined,
		Shares: shares, Price: "0.5", GrossNotional: "2", TotalFee: "0.1", NetCashDelta: netCash,
		FeeSource: domain.FeeSourcePolygonV2OrderFilled, TransactionHash: "0xtx-" + id,
		MatchedAt: matchedAt, ObservedAt: testNow,
		SettlementEvidence: &domain.SettlementEvidence{
			BlockNumber: 100, BlockHash: "0xblock", Confirmations: 17, TransactionHash: "0xtx-" + id,
		},
	}
}

func assertNoIssue(t *testing.T, issues []domain.ReconciliationIssue, issueType domain.ReconciliationIssueType) {
	t.Helper()
	for _, issue := range issues {
		if issue.Type == issueType {
			t.Fatalf("unexpected issue %s: %#v", issueType, issue)
		}
	}
}

func finalityTestService(t *testing.T, ledger *fakeLedger, external []domain.ExternalPosition, balance domain.Decimal) *Service {
	t.Helper()
	return newTestService(t, Params{
		Orders: &fakeOrders{}, Venue: &fakeVenue{}, Ledger: ledger,
		Fills: &fakeFills{}, OrderRefresher: &fakeRefresher{},
		PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
			return external, nil
		})},
		BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
			return domain.ExternalBalance{Asset: "USDC", Amount: balance, Source: "CHAIN", ObservedAt: testNow}, nil
		})},
	})
}

func openPosition(tokenID string, shares domain.Decimal) domain.Position {
	return domain.Position{
		ExecutionAccountID: "account-1", MarketID: "market-1", ConditionID: "condition-1",
		TokenID: tokenID, OutcomeName: "YES", TotalShares: shares, AvailableShares: shares, ReservedShares: "0",
		LifecycleStatus: domain.PositionLifecycleOpen,
	}
}

func TestRunAccountExplainsSellDriftByFinalityPendingFill(t *testing.T) {
	ledger := &fakeLedger{
		balance:   testBalance("100"),
		positions: []domain.Position{openPosition("token-1", "10")},
		finalityPending: []domain.Fill{
			finalityPendingFill("sell-1", "token-1", domain.SideSell, "4", "1.9", testNow.Add(-90*time.Second)),
		},
	}
	// The chain already shows the SELL: four fewer shares and 1.9 more collateral.
	service := finalityTestService(t, ledger, []domain.ExternalPosition{{
		ConditionID: "condition-1", TokenID: "token-1", OutcomeName: "YES", Shares: "6", Source: "POLYMARKET_DATA_API", ObservedAt: testNow,
	}}, "101.9")

	result, err := service.RunAccount(context.Background(), RunAccountParams{ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled})
	if err != nil {
		t.Fatalf("RunAccount() error = %v", err)
	}
	if result.Run.Status != domain.ReconciliationRunCompleted {
		t.Fatalf("run status = %s, want COMPLETED; issues = %#v", result.Run.Status, result.Issues)
	}
	assertNoIssue(t, result.Issues, domain.ReconciliationIssuePositionDrift)
	assertNoIssue(t, result.Issues, domain.ReconciliationIssueBalanceDrift)
	assertNoIssue(t, result.Issues, domain.ReconciliationIssueFillFinalityStalled)
	if result.Run.Summary["finality_pending_fills"] != 1 ||
		result.Run.Summary["positions_explained_by_finality_pending_fills"] != 1 ||
		result.Run.Summary["balance_explained_by_finality_pending_fills"] != 1 {
		t.Fatalf("summary = %#v", result.Run.Summary)
	}
}

func TestRunAccountExplainsPhantomPositionByFinalityPendingBuyFill(t *testing.T) {
	ledger := &fakeLedger{
		balance: testBalance("100"),
		finalityPending: []domain.Fill{
			finalityPendingFill("buy-1", "token-2", domain.SideBuy, "5", "-2.6", testNow.Add(-time.Minute)),
		},
	}
	service := finalityTestService(t, ledger, []domain.ExternalPosition{{
		ConditionID: "condition-1", TokenID: "token-2", OutcomeName: "NO", Shares: "5", Source: "POLYMARKET_DATA_API", ObservedAt: testNow,
	}}, "97.4")

	result, err := service.RunAccount(context.Background(), RunAccountParams{ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled})
	if err != nil {
		t.Fatalf("RunAccount() error = %v", err)
	}
	if result.Run.Status != domain.ReconciliationRunCompleted {
		t.Fatalf("run status = %s, want COMPLETED; issues = %#v", result.Run.Status, result.Issues)
	}
	assertNoIssue(t, result.Issues, domain.ReconciliationIssuePhantomPosition)
	assertNoIssue(t, result.Issues, domain.ReconciliationIssueBalanceDrift)
}

func TestRunAccountExplainsMissingExternalPositionByFinalityPendingSellFill(t *testing.T) {
	ledger := &fakeLedger{
		balance:   testBalance("100"),
		positions: []domain.Position{openPosition("token-1", "4")},
		finalityPending: []domain.Fill{
			finalityPendingFill("sell-all", "token-1", domain.SideSell, "4", "1.9", testNow.Add(-time.Minute)),
		},
	}
	service := finalityTestService(t, ledger, nil, "101.9")

	result, err := service.RunAccount(context.Background(), RunAccountParams{ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled})
	if err != nil {
		t.Fatalf("RunAccount() error = %v", err)
	}
	if result.Run.Status != domain.ReconciliationRunCompleted {
		t.Fatalf("run status = %s, want COMPLETED; issues = %#v", result.Run.Status, result.Issues)
	}
	assertNoIssue(t, result.Issues, domain.ReconciliationIssuePositionDrift)
}

func TestRunAccountKeepsDriftNotAccountedByFinalityPendingFills(t *testing.T) {
	ledger := &fakeLedger{
		balance:   testBalance("100"),
		positions: []domain.Position{openPosition("token-1", "10")},
		finalityPending: []domain.Fill{
			finalityPendingFill("sell-1", "token-1", domain.SideSell, "4", "1.9", testNow.Add(-time.Minute)),
		},
	}
	// Pending SELL explains four shares and 1.9 collateral; the chain shows five
	// shares and five collateral missing. A coincidentally similar aggregate
	// difference must not be absorbed.
	service := finalityTestService(t, ledger, []domain.ExternalPosition{{
		ConditionID: "condition-1", TokenID: "token-1", OutcomeName: "YES", Shares: "5", Source: "POLYMARKET_DATA_API", ObservedAt: testNow,
	}}, "105")

	result, err := service.RunAccount(context.Background(), RunAccountParams{ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled})
	if err != nil {
		t.Fatalf("RunAccount() error = %v", err)
	}
	if result.Run.Status != domain.ReconciliationRunAttentionRequired {
		t.Fatalf("run status = %s, want ATTENTION_REQUIRED", result.Run.Status)
	}
	assertIssue(t, result.Issues, domain.ReconciliationIssuePositionDrift, domain.ReconciliationIssueOpen)
	assertIssue(t, result.Issues, domain.ReconciliationIssueBalanceDrift, domain.ReconciliationIssueOpen)
}

func TestRunAccountEscalatesStalledFinalityPendingFill(t *testing.T) {
	ledger := &fakeLedger{
		balance:   testBalance("100"),
		positions: []domain.Position{openPosition("token-1", "10")},
		finalityPending: []domain.Fill{
			finalityPendingFill("stale", "token-1", domain.SideSell, "4", "1.9", testNow.Add(-31*time.Minute)),
		},
	}
	service := finalityTestService(t, ledger, []domain.ExternalPosition{{
		ConditionID: "condition-1", TokenID: "token-1", OutcomeName: "YES", Shares: "6", Source: "POLYMARKET_DATA_API", ObservedAt: testNow,
	}}, "101.9")

	result, err := service.RunAccount(context.Background(), RunAccountParams{ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled})
	if err != nil {
		t.Fatalf("RunAccount() error = %v", err)
	}
	if result.Run.Status != domain.ReconciliationRunAttentionRequired {
		t.Fatalf("run status = %s, want ATTENTION_REQUIRED", result.Run.Status)
	}
	assertIssue(t, result.Issues, domain.ReconciliationIssueFillFinalityStalled, domain.ReconciliationIssueOpen)
	for _, issue := range result.Issues {
		if issue.Type != domain.ReconciliationIssueFillFinalityStalled {
			continue
		}
		if issue.Resolution != domain.ReconciliationResolutionManual || issue.VenueTradeID != "stale" ||
			issue.OrderID != "order-stale" || issue.TokenID != "token-1" {
			t.Fatalf("stalled issue = %#v", issue)
		}
	}
	// The drift itself is still exactly accounted for; the stall is the gate.
	assertNoIssue(t, result.Issues, domain.ReconciliationIssuePositionDrift)
	assertNoIssue(t, result.Issues, domain.ReconciliationIssueBalanceDrift)
}

func TestRunAccountSkipsAssetComparisonWhenFinalityPendingFillsUnreadable(t *testing.T) {
	ledger := &fakeLedger{
		balance:     testBalance("100"),
		positions:   []domain.Position{openPosition("token-1", "10")},
		finalityErr: errors.New("postgres unavailable"),
	}
	service := finalityTestService(t, ledger, []domain.ExternalPosition{{
		ConditionID: "condition-1", TokenID: "token-1", OutcomeName: "YES", Shares: "6", Source: "POLYMARKET_DATA_API", ObservedAt: testNow,
	}}, "101.9")

	result, err := service.RunAccount(context.Background(), RunAccountParams{ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled})
	if err == nil {
		t.Fatal("RunAccount() must surface the infrastructure failure")
	}
	assertIssue(t, result.Issues, domain.ReconciliationIssueSourceUnavailable, domain.ReconciliationIssueOpen)
	assertNoIssue(t, result.Issues, domain.ReconciliationIssuePositionDrift)
	assertNoIssue(t, result.Issues, domain.ReconciliationIssueBalanceDrift)
}

func TestRunAccountRejectsInvalidFinalityPendingFill(t *testing.T) {
	applied := finalityPendingFill("applied", "token-1", domain.SideSell, "4", "1.9", testNow)
	appliedAt := testNow
	applied.AppliedAt = &appliedAt
	ledger := &fakeLedger{balance: testBalance("100"), finalityPending: []domain.Fill{applied}}
	service := finalityTestService(t, ledger, nil, "100")

	result, err := service.RunAccount(context.Background(), RunAccountParams{ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled})
	if err == nil {
		t.Fatal("an applied fill returned as finality-pending must fail closed")
	}
	assertIssue(t, result.Issues, domain.ReconciliationIssueSourceUnavailable, domain.ReconciliationIssueOpen)
}

func TestNewRejectsShortFillFinalityMaxAge(t *testing.T) {
	ledger := &fakeLedger{balance: testBalance("100")}
	_, err := New(Params{
		Orders: &fakeOrders{}, Venue: &fakeVenue{}, Ledger: ledger, Fills: &fakeFills{}, OrderRefresher: &fakeRefresher{},
		Recorder: &fakeRecorder{},
		PositionBaselines: positionBaselineSourceFunc(func(context.Context, string) ([]domain.ExternalPositionBaseline, error) {
			return nil, nil
		}),
		PositionDispositionTrades: positionDispositionTradeSourceFunc(func(context.Context, string) ([]domain.ExternalPositionDispositionTrade, error) {
			return nil, nil
		}),
		PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
			return nil, nil
		})},
		BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
			return domain.ExternalBalance{}, nil
		})},
		FillFinalityMaxAge: 10 * time.Second,
	})
	if err == nil {
		t.Fatal("fill finality max age below one minute was accepted")
	}
}
