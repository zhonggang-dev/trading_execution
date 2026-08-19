package polymarket

import (
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// TestMapTradeToOrderFillSeparatesTakerAndMakerComponents 验证 Map Trade To Order Fill Separates Taker And Maker Components 场景下的行为。
func TestMapTradeToOrderFillSeparatesTakerAndMakerComponents(t *testing.T) {
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	trade := Trade{
		ID: "trade-1", TakerOrderID: "other-order", Market: "condition-1", TokenID: "token-1",
		Side: "BUY", Size: "20", Price: "0.6", Status: "CONFIRMED", FeeRateBPS: "12",
		MatchTime: "2026-08-18T09:59:58Z", LastUpdate: "2026-08-18T09:59:59Z",
		MakerOrders: []MakerOrder{{
			OrderID: "0xvenue", TokenID: "token-1", Side: "SELL",
			MatchedAmount: "4", Price: "0.4", FeeRateBPS: "75", BuilderFee: "999",
		}},
	}
	order := adapterOrder()
	order.VenueOrderID = "0xvenue"
	fill, matched, err := mapTradeToOrderFill(trade, order, now)
	if err != nil || !matched {
		t.Fatalf("map result matched=%v err=%v", matched, err)
	}
	if fill.LiquidityRole != domain.LiquidityRoleMaker || !fill.Shares.Equal("4") ||
		!fill.Price.Equal("0.4") || !fill.FeeRateBPS.Equal("75") || !fill.BuilderFeeRateBPS.IsEmpty() ||
		fill.Status != domain.FillStatusConfirmed || fill.ConfirmedAt == nil {
		t.Fatalf("maker fill = %#v", fill)
	}
}

// TestMapTradeToOrderFillKeepsNonFinalStatusUnbooked 验证 Map Trade To Order Fill Keeps Non Final Status Unbooked 场景下的行为。
func TestMapTradeToOrderFillKeepsNonFinalStatusUnbooked(t *testing.T) {
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	order := adapterOrder()
	order.VenueOrderID = "0xvenue"
	fill, matched, err := mapTradeToOrderFill(Trade{
		ID: "trade-1", TakerOrderID: "0xvenue", Size: "2", Price: "0.5",
		Status: "MATCHED", FeeRateBPS: "10", MatchTime: "1787047198", LastUpdate: "1787047199",
	}, order, now)
	if err != nil || !matched {
		t.Fatalf("map result matched=%v err=%v", matched, err)
	}
	if fill.Status != domain.FillStatusMatched || fill.ConfirmedAt != nil {
		t.Fatalf("non-final fill = %#v", fill)
	}
}

// TestPreferTradeObservationKeepsConfirmedDuplicate 验证 Prefer Trade Observation Keeps Confirmed Duplicate 场景下的行为。
func TestPreferTradeObservationKeepsConfirmedDuplicate(t *testing.T) {
	existing := domain.Fill{
		VenueFillID: "trade-1", OrderID: "order-1", LiquidityRole: domain.LiquidityRoleTaker,
		Shares: "2", Price: "0.5", Status: domain.FillStatusConfirmed,
	}
	incoming := existing
	incoming.Status = domain.FillStatusMatched
	merged, err := preferTradeObservation(existing, incoming)
	if err != nil || merged.Status != domain.FillStatusConfirmed {
		t.Fatalf("merged=%#v err=%v", merged, err)
	}
}
