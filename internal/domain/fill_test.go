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
		FeeRateBPS: "10", PlatformFeeRate: "0.001", FeeExponent: "1", PlatformFee: "0.0025", BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0.0025",
		NetCashDelta: "-5", MatchedAt: now, ObservedAt: now, ConfirmedAt: &now,
		FeeSource: "TEST_CALCULATED",
	}
	if err := fill.ValidateAccounting(); err == nil {
		t.Fatal("inconsistent BUY net cash delta was accepted")
	}
	fill.NetCashDelta = "-5.0025"
	if err := fill.ValidateAccounting(); err != nil {
		t.Fatalf("consistent accounting rejected: %v", err)
	}
}

func TestFillValidateAccountingAllowsOnlySubBaseUnitEventRounding(t *testing.T) {
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	fill := Fill{
		Key: "fill-rounding", Venue: "polymarket", VenueFillID: "trade-1", OrderID: "order-1",
		VenueOrderID: "0x" + repeatHex("22", 32), ExecutionAccountID: "account-1", MarketID: "market-1",
		TokenID: "token-1", Side: SideSell, LiquidityRole: LiquidityRoleMaker,
		Status: FillStatusConfirmed, Shares: "3", Price: "0.3333333", GrossNotional: "0.999999",
		PriceTickSize: "0.0000001",
		FeeRateBPS:    "0", PlatformFeeRate: "0", FeeExponent: "0", PlatformFee: "0", BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0",
		NetCashDelta: "0.999999", MatchedAt: now, ObservedAt: now, ConfirmedAt: &now,
		FeeSource: FeeSourcePolygonV2OrderFilled, TransactionHash: "0x" + repeatHex("cd", 32),
	}
	fill.SettlementEvidence = &SettlementEvidence{
		SchemaVersion: SettlementEvidenceSchemaV1, Source: FeeSourcePolygonV2OrderFilled,
		ChainID: SettlementEvidencePolygonChainID, ExchangeAddress: "0x" + repeatHex("ab", 20),
		TransactionHash: fill.TransactionHash, BlockNumber: 123, BlockHash: "0x" + repeatHex("ef", 32),
		LogIndex: 7, Confirmations: 64, OrderHash: fill.VenueOrderID,
		MakerAddress: "0x" + repeatHex("11", 20), TokenID: fill.TokenID, Side: fill.Side,
		MakerAmountBaseUnits: "3000000", TakerAmountBaseUnits: "999999", TotalFeeBaseUnits: "0",
		BuilderCode: settlementZeroBytes32, BuilderFeeKnown: true, BuilderFeeBaseUnits: "0",
		BuilderFeeSource: SettlementEvidenceZeroBuilder, CollateralDecimals: 6, OutcomeTokenDecimals: 6,
	}
	if err := fill.ValidateAccounting(); err != nil {
		t.Fatalf("valid V2 integer-floor discrepancy rejected: %v", err)
	}
	fill.GrossNotional = "0.999998"
	fill.NetCashDelta = "0.999998"
	if err := fill.ValidateAccounting(); err == nil {
		t.Fatal("discrepancy of at least one pUSD base unit was accepted")
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
