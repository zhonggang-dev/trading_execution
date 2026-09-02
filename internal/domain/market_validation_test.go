package domain

import (
	"strings"
	"testing"
	"time"
)

func validMarketValidationParams() MarketValidationParams {
	now := time.Date(2026, 9, 2, 8, 0, 0, 0, time.UTC)
	return MarketValidationParams{
		Mode: "LIVE_CHECK", ValidatedAt: now, MarketObservedAt: now, StrategySnapshotAt: now,
		LatestBookSourceAt: now, LatestBookObservedAt: now, OutcomeIndex: 0, OutcomeName: "Yes",
		TokenID: "token-yes", TickSize: "0.01", MinOrderSize: "5", BestBid: "0.49", BestAsk: "0.50", WorstPrice: "0.52",
	}
}

// TestMarketValidationBuildAcceptsTickAlignedExecutionPrice 验证执行价可选且必须在 tick 上。
func TestMarketValidationBuildAcceptsTickAlignedExecutionPrice(t *testing.T) {
	params := validMarketValidationParams()
	if _, err := params.Build(); err != nil {
		t.Fatalf("Build() without execution price error = %v", err)
	}
	params.ExecutionPrice = "0.50"
	validation, err := params.Build()
	if err != nil || validation.ExecutionPrice != "0.50" {
		t.Fatalf("Build() = %#v, err = %v", validation, err)
	}
}

// TestMarketValidationBuildRejectsInvalidExecutionPrice 验证非法执行价 fail closed。
func TestMarketValidationBuildRejectsInvalidExecutionPrice(t *testing.T) {
	tests := map[string]Decimal{
		"zero": "0", "negative": "-0.5", "one": "1", "off tick": "0.505", "not decimal": "abc",
	}
	for name, price := range tests {
		params := validMarketValidationParams()
		params.ExecutionPrice = price
		_, err := params.Build()
		if err == nil || !strings.Contains(err.Error(), "execution_price") {
			t.Fatalf("%s: Build() error = %v, want execution_price rejection", name, err)
		}
	}
}
