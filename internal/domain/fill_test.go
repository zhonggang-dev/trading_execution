package domain

import (
	"testing"
	"time"
)

// TestFillValidateAccountingRejectsInconsistentCash 验证 Fill Validate Accounting Rejects Inconsistent Cash 场景下的行为。
func TestFillValidateAccountingRejectsInconsistentCash(t *testing.T) {
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	fill := Fill{
		Key: "fill-1", Venue: "polymarket", VenueFillID: "trade-1", OrderID: "order-1",
		VenueOrderID: "venue-1", ExecutionAccountID: "account-1", MarketID: "market-1",
		TokenID: "token-1", Side: SideBuy, LiquidityRole: LiquidityRoleTaker,
		Status: FillStatusConfirmed, Shares: "10", Price: "0.5", GrossNotional: "5",
		FeeRateBPS: "10", PlatformFee: "0.0025", BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0.0025",
		NetCashDelta: "-5", MatchedAt: now, ObservedAt: now, ConfirmedAt: &now,
	}
	if err := fill.ValidateAccounting(); err == nil {
		t.Fatal("inconsistent BUY net cash delta was accepted")
	}
	fill.NetCashDelta = "-5.0025"
	if err := fill.ValidateAccounting(); err != nil {
		t.Fatalf("consistent accounting rejected: %v", err)
	}
}

// TestFillStatusDoesNotLeaveTerminalState 验证 Fill Status Does Not Leave Terminal State 场景下的行为。
func TestFillStatusDoesNotLeaveTerminalState(t *testing.T) {
	if FillStatusConfirmed.CanTransitionTo(FillStatusMatched) || FillStatusFailed.CanTransitionTo(FillStatusConfirmed) {
		t.Fatal("terminal fill status was allowed to transition")
	}
	if !FillStatusRetrying.CanTransitionTo(FillStatusConfirmed) {
		t.Fatal("retrying fill cannot become confirmed")
	}
}
