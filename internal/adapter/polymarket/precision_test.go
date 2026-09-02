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

// TestPlacementIntentPinsMarketableBuyToExecutionPrice 验证 FOK BUY 的签名价格来自校验时的最新 best ask，
// 使 size*price 预算恰好等于 size 股，而不是按 worst_price 预算买入更多股。
func TestPlacementIntentPinsMarketableBuyToExecutionPrice(t *testing.T) {
	intent := adapterIntent()
	intent.Price = "0.52"
	intent.WorstPrice = "0.52"
	intent.TimeInForce = domain.TimeInForceFOK
	order := domain.Order{Intent: intent, MarketValidation: &domain.MarketValidation{
		Mode: "LIVE_CHECK", TickSize: "0.01", MinOrderSize: "5", BestAsk: "0.50", WorstPrice: "0.52", ExecutionPrice: "0.50",
	}}
	wire, err := placementIntent(order)
	if err != nil {
		t.Fatalf("placementIntent() error = %v", err)
	}
	if wire.Price != "0.50" || wire.WorstPrice != "0.52" || wire.Size != "10" || wire.Type != domain.OrderTypeLimit {
		t.Fatalf("wire intent = %#v, want price pinned to 0.50 with worst_price retained", wire)
	}
	amounts, err := buildRawAmounts(wire, "0.01", "5", "1")
	if err != nil {
		t.Fatalf("buildRawAmounts() error = %v", err)
	}
	if amounts.MakerAmount != "5000000" || amounts.TakerAmount != "10000000" {
		t.Fatalf("amounts = %#v, want a 5 pUSD budget for exactly 10 shares", amounts)
	}
}

// TestPlacementIntentRejectsMarketableBuyWithoutExecutionPrice 验证缺少执行价时 FOK/FAK BUY fail closed，
// 不会退回到 worst_price 预算。
func TestPlacementIntentRejectsMarketableBuyWithoutExecutionPrice(t *testing.T) {
	for _, timeInForce := range []domain.TimeInForce{domain.TimeInForceFOK, domain.TimeInForceFAK} {
		intent := adapterIntent()
		intent.TimeInForce = timeInForce
		order := domain.Order{Intent: intent, MarketValidation: &domain.MarketValidation{
			Mode: "LIVE_CHECK", TickSize: "0.01", MinOrderSize: "5", BestAsk: "0.49", WorstPrice: "0.50",
		}}
		_, err := placementIntent(order)
		var venueError *port.VenueError
		if !errors.As(err, &venueError) || venueError.Kind != port.VenueErrorInvalid || venueError.Code != "BUY_EXECUTION_PRICE_REQUIRED" {
			t.Fatalf("%s placementIntent() error = %v, want BUY_EXECUTION_PRICE_REQUIRED", timeInForce, err)
		}
	}
}

// TestPlacementIntentRejectsExecutionPriceAboveWorstPrice 验证执行价绝不能越过策略 worst_price 上限。
func TestPlacementIntentRejectsExecutionPriceAboveWorstPrice(t *testing.T) {
	intent := adapterIntent()
	intent.TimeInForce = domain.TimeInForceFOK
	order := domain.Order{Intent: intent, MarketValidation: &domain.MarketValidation{
		Mode: "LIVE_CHECK", TickSize: "0.01", MinOrderSize: "5", BestAsk: "0.51", WorstPrice: "0.50", ExecutionPrice: "0.51",
	}}
	_, err := placementIntent(order)
	var venueError *port.VenueError
	if !errors.As(err, &venueError) || venueError.Code != "BUY_EXECUTION_PRICE_EXCEEDS_WORST_PRICE" {
		t.Fatalf("placementIntent() error = %v, want BUY_EXECUTION_PRICE_EXCEEDS_WORST_PRICE", err)
	}
}

// TestPlacementIntentLeavesSellAndRestingBuyUnchanged 验证 SELL 与 GTC/GTD BUY 仍按策略价格签名：
// 它们在 CLOB 中按股数成交，不存在预算超买问题。
func TestPlacementIntentLeavesSellAndRestingBuyUnchanged(t *testing.T) {
	validation := &domain.MarketValidation{
		Mode: "LIVE_CHECK", TickSize: "0.01", MinOrderSize: "5", BestBid: "0.50", BestAsk: "0.51", WorstPrice: "0.50", ExecutionPrice: "0.49",
	}
	sell := adapterIntent()
	sell.Side = domain.SideSell
	sell.TimeInForce = domain.TimeInForceFOK
	wire, err := placementIntent(domain.Order{Intent: sell, MarketValidation: validation})
	if err != nil || wire.Price != "0.50" {
		t.Fatalf("SELL wire intent = %#v, err = %v, want strategy price retained", wire, err)
	}
	resting := adapterIntent()
	resting.TimeInForce = domain.TimeInForceGTC
	wire, err = placementIntent(domain.Order{Intent: resting, MarketValidation: validation})
	if err != nil || wire.Price != "0.50" {
		t.Fatalf("GTC BUY wire intent = %#v, err = %v, want strategy price retained", wire, err)
	}
}
