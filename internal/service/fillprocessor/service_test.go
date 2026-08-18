package fillprocessor

import (
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// TestCalculateMoneyUsesExactTakerFeeAndSide 验证 Calculate Money Uses Exact Taker Fee And Side 场景下的行为。
func TestCalculateMoneyUsesExactTakerFeeAndSide(t *testing.T) {
	tests := []struct {
		name    string
		side    domain.Side
		wantNet domain.Decimal
	}{
		{name: "buy", side: domain.SideBuy, wantNet: "-5.0025"},
		{name: "sell", side: domain.SideSell, wantNet: "4.9975"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fill, err := calculateMoney(domain.Fill{
				Side: test.side, LiquidityRole: domain.LiquidityRoleTaker,
				Shares: "10", Price: "0.5", FeeRateBPS: "10",
			})
			if err != nil {
				t.Fatal(err)
			}
			if !fill.GrossNotional.Equal("5") || !fill.PlatformFee.Equal("0.0025") ||
				!fill.TotalFee.Equal("0.0025") || !fill.NetCashDelta.Equal(test.wantNet) {
				t.Fatalf("calculated fill = %#v", fill)
			}
		})
	}
}

// TestCalculateMoneyMakerDoesNotInventTakerFee 验证 Calculate Money Maker Does Not Invent Taker Fee 场景下的行为。
func TestCalculateMoneyMakerDoesNotInventTakerFee(t *testing.T) {
	fill, err := calculateMoney(domain.Fill{
		Side: domain.SideBuy, LiquidityRole: domain.LiquidityRoleMaker,
		Shares: "3", Price: "0.4",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !fill.PlatformFee.Equal("0") || !fill.NetCashDelta.Equal("-1.2") || fill.FeeSource != "PROTOCOL_MAKER_ZERO+BUILDER_NONE" {
		t.Fatalf("calculated maker fill = %#v", fill)
	}
}

// TestCalculateMoneyAddsBuilderFeeForMaker 验证 Calculate Money Adds Builder Fee For Maker 场景下的行为。
func TestCalculateMoneyAddsBuilderFeeForMaker(t *testing.T) {
	fill, err := calculateMoney(domain.Fill{
		Side: domain.SideSell, LiquidityRole: domain.LiquidityRoleMaker,
		Shares: "10", Price: "0.5", BuilderFeeRateBPS: "50",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !fill.BuilderFee.Equal("0.025") || !fill.TotalFee.Equal("0.025") ||
		!fill.NetCashDelta.Equal("4.975") {
		t.Fatalf("maker builder fee = %#v", fill)
	}
}

// TestCalculateMoneyRoundsProtocolFeeHalfUpToFiveDecimals 验证 Calculate Money Rounds Protocol Fee Half Up To Five Decimals 场景下的行为。
func TestCalculateMoneyRoundsProtocolFeeHalfUpToFiveDecimals(t *testing.T) {
	fill, err := calculateMoney(domain.Fill{
		Side: domain.SideBuy, LiquidityRole: domain.LiquidityRoleTaker,
		Shares: "1", Price: "0.333333", FeeRateBPS: "17",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !fill.PlatformFee.Equal("0.00038") {
		t.Fatalf("platform fee = %s, want 0.00038", fill.PlatformFee)
	}
}

// TestFillKeyIsOrderComponentScoped 验证 Fill Key Is Order Component Scoped 场景下的行为。
func TestFillKeyIsOrderComponentScoped(t *testing.T) {
	first := FillKey("Polymarket", "trade-1", "order-a")
	if first != FillKey("polymarket", "trade-1", "order-a") {
		t.Fatal("venue normalization changed fill identity")
	}
	if first == FillKey("polymarket", "trade-1", "order-b") {
		t.Fatal("one venue trade must produce distinct order-component fill keys")
	}
}
