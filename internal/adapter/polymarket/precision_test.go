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

// TestPlacementIntentCapsEmulatedIOCToExecutableSize 验证模拟 IOC 的签名数量为校验时保护价内可成交量，
// 价格保持 worst_price。
func TestPlacementIntentCapsEmulatedIOCToExecutableSize(t *testing.T) {
	intent := adapterIntent()
	intent.TimeInForce = domain.TimeInForceIOC
	intent.Size = "42"
	order := domain.Order{Intent: intent, MarketValidation: &domain.MarketValidation{
		Mode: "LIVE_CHECK", TickSize: "0.01", MinOrderSize: "5", WorstPrice: "0.50", ExecutableSize: "17.5",
	}}
	wire, err := placementIntent(order)
	if err != nil {
		t.Fatalf("placementIntent() error = %v", err)
	}
	if wire.Size != "17.5" || wire.Price != "0.50" || wire.TimeInForce != domain.TimeInForceIOC {
		t.Fatalf("wire intent = %#v, want 17.5 shares at worst_price", wire)
	}
	amounts, err := buildRawAmounts(wire, "0.01", "5", "1")
	if err != nil {
		t.Fatalf("buildRawAmounts() error = %v", err)
	}
	if amounts.MakerAmount != "8750000" || amounts.TakerAmount != "17500000" {
		t.Fatalf("amounts = %#v, want 8.75 pUSD for exactly 17.5 shares", amounts)
	}

	order.MarketValidation.ExecutableSize = ""
	wire, err = placementIntent(order)
	if err != nil || wire.Size != "42" {
		t.Fatalf("placementIntent() without executable size = %#v, err = %v", wire, err)
	}
	order.MarketValidation.ExecutableSize = "50"
	if _, err := placementIntent(order); err == nil {
		t.Fatal("executable size above the strategy size was accepted")
	}
	order.MarketValidation.ExecutableSize = "4"
	if _, err := placementIntent(order); err == nil {
		t.Fatal("executable size below min_order_size was accepted")
	}

	fok := adapterIntent()
	fok.TimeInForce = domain.TimeInForceFOK
	order.Intent = fok
	order.MarketValidation.ExecutableSize = "7"
	wire, err = placementIntent(order)
	if err != nil || wire.Size != "10" {
		t.Fatalf("FOK placementIntent() = %#v, err = %v, want strategy size untouched", wire, err)
	}
}

// TestClobOrderTypeEmulatesIOCAsGTC 验证 IOC 以按股数计的 GTC 签名，而不是预算式 FAK。
func TestClobOrderTypeEmulatesIOCAsGTC(t *testing.T) {
	orderType, err := clobOrderType(domain.TimeInForceIOC)
	if err != nil || orderType != "GTC" {
		t.Fatalf("clobOrderType(IOC) = %q, %v", orderType, err)
	}
	client := &TradingClient{}
	if client.SupportsTimeInForce(domain.TimeInForceIOC) || !client.SupportsTimeInForce(domain.TimeInForceGTC) ||
		!client.SupportsTimeInForce(domain.TimeInForceFOK) {
		t.Fatal("SupportsTimeInForce must report IOC as emulated and GTC/FOK as native")
	}
}
