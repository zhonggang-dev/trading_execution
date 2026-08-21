package polymarket

import (
	"errors"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// TestBuildRawAmountsUsesExactDecimalArithmetic 验证 Build Raw Amounts Uses Exact Decimal Arithmetic 场景下的行为。
func TestBuildRawAmountsUsesExactDecimalArithmetic(t *testing.T) {
	intent := adapterIntent()
	intent.Price = "0.17"
	intent.WorstPrice = "0.17"
	intent.Size = "16.90"
	amounts, err := buildRawAmounts(intent, "0.01", "5", "1")
	if err != nil {
		t.Fatalf("buildRawAmounts() error = %v", err)
	}
	if amounts.MakerAmount != "2873000" || amounts.TakerAmount != "16900000" {
		t.Fatalf("amounts = %#v, want exact 2.873 pUSD and 16.90 shares", amounts)
	}
}

// TestBuildRawAmountsRejectsInsteadOfRoundingStrategySize 验证 Build Raw Amounts Rejects Instead Of Rounding Strategy Size 场景下的行为。
func TestBuildRawAmountsRejectsInsteadOfRoundingStrategySize(t *testing.T) {
	intent := adapterIntent()
	intent.Size = "16.901"
	_, err := buildRawAmounts(intent, "0.01", "5", "1")
	var venueError *port.VenueError
	if !errors.As(err, &venueError) || venueError.Code != "INVALID_SIZE_PRECISION" {
		t.Fatalf("buildRawAmounts() error = %v", err)
	}
}

// TestBuildRawAmountsAllowsFourDecimalFAKBuyNotional 验证 FAK BUY 允许四位小数的 maker notional。
func TestBuildRawAmountsAllowsFourDecimalFAKBuyNotional(t *testing.T) {
	intent := adapterIntent()
	intent.Price = "0.17"
	intent.WorstPrice = "0.17"
	intent.Size = "5.89"
	intent.TimeInForce = domain.TimeInForceFAK
	amounts, err := buildRawAmounts(intent, "0.01", "5", "1")
	if err != nil {
		t.Fatalf("buildRawAmounts() error = %v", err)
	}
	if amounts.MakerAmount != "1001300" || amounts.TakerAmount != "5890000" {
		t.Fatalf("amounts = %#v, want exact 1.0013 pUSD and 5.89 shares", amounts)
	}
}

// TestBuildRawAmountsRejectsFAKBuyNotionalBeyondFourDecimals 验证 FAK BUY 仍拒绝超过四位小数的 maker notional。
func TestBuildRawAmountsRejectsFAKBuyNotionalBeyondFourDecimals(t *testing.T) {
	intent := adapterIntent()
	intent.Price = "0.501"
	intent.WorstPrice = "0.501"
	intent.Size = "19.97"
	intent.TimeInForce = domain.TimeInForceFAK
	_, err := buildRawAmounts(intent, "0.001", "5", "1")
	var venueError *port.VenueError
	if !errors.As(err, &venueError) || venueError.Code != "INVALID_FAK_FOK_PRECISION" {
		t.Fatalf("buildRawAmounts() error = %v", err)
	}
}

// adapterIntent 实现当前测试场景所需的辅助行为。
func adapterIntent() domain.OrderIntent {
	return domain.OrderIntent{
		ModelID:            "model-1",
		StrategyID:         "strategy-1",
		ExecutionAccountID: "account-1",
		SignalID:           "signal-1",
		ClientOrderID:      "client-1",
		Venue:              "polymarket",
		MarketID:           "market-1",
		TokenID:            "123456789012345678901234567890",
		Side:               domain.SideBuy,
		Type:               domain.OrderTypeLimit,
		Price:              "0.50",
		WorstPrice:         "0.50",
		Size:               "10",
		TimeInForce:        domain.TimeInForceGTC,
	}
}
