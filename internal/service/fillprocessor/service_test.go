package fillprocessor

import (
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// TestCalculateMoneyUsesAuthoritativeFeeAndSide verifies that the processor
// derives cash movement without replacing finalized event amounts.
func TestCalculateMoneyUsesAuthoritativeFeeAndSide(t *testing.T) {
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
			fill := authoritativeMoneyFill(test.side, domain.LiquidityRoleTaker)
			var err error
			fill, err = calculateMoney(fill)
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

// TestCalculateMoneyMakerDoesNotTreatMakerRateAsBuilderFee protects the V2
// distinction between maker fee_rate_bps and builder_fee.
func TestCalculateMoneyMakerDoesNotInventTakerFee(t *testing.T) {
	raw := authoritativeMoneyFill(domain.SideBuy, domain.LiquidityRoleMaker)
	raw.FeeRateBPS = "750"
	raw.PlatformFeeRate = "0.075"
	raw.FeeExponent = "2"
	raw.PlatformFee = "0"
	raw.BuilderFeeRateBPS = "0"
	raw.BuilderFee = "0"
	raw.TotalFee = "0"
	fill, err := calculateMoney(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !fill.PlatformFee.Equal("0") || !fill.BuilderFee.Equal("0") || !fill.NetCashDelta.Equal("-5") ||
		fill.FeeSource != domain.FeeSourcePolygonV2OrderFilled {
		t.Fatalf("calculated maker fill = %#v", fill)
	}
}

// TestCalculateMoneyUsesReportedBuilderFeeForMaker verifies that an actual
// builder allocation is consumed but never reconstructed from its rate.
func TestCalculateMoneyUsesReportedBuilderFeeForMaker(t *testing.T) {
	raw := authoritativeMoneyFill(domain.SideSell, domain.LiquidityRoleMaker)
	raw.FeeRateBPS = "750"
	raw.PlatformFeeRate = "0.075"
	raw.FeeExponent = "2"
	raw.PlatformFee = "0"
	raw.BuilderFeeRateBPS = "50"
	raw.BuilderFee = "0.025"
	raw.TotalFee = "0.025"
	fill, err := calculateMoney(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !fill.BuilderFee.Equal("0.025") || !fill.TotalFee.Equal("0.025") ||
		!fill.NetCashDelta.Equal("4.975") {
		t.Fatalf("maker builder fee = %#v", fill)
	}
}

// TestCalculateMoneyValidatesFeeExponent verifies the V2 exponent is included
// in the curve cross-check rather than assuming the legacy exponent of one.
func TestCalculateMoneyValidatesFeeExponent(t *testing.T) {
	raw := authoritativeMoneyFill(domain.SideBuy, domain.LiquidityRoleTaker)
	raw.Shares = "100"
	raw.Price = "0.5"
	raw.GrossNotional = "50"
	raw.FeeRateBPS = "2500"
	raw.PlatformFeeRate = "0.25"
	raw.FeeExponent = "2"
	raw.PlatformFee = "1.5625"
	raw.TotalFee = "1.5625"
	fill, err := calculateMoney(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !fill.PlatformFee.Equal("1.5625") {
		t.Fatalf("platform fee = %s, want 1.5625", fill.PlatformFee)
	}
}

func TestCalculateMoneyValidatesOfficialFiveDecimalFeeTruncation(t *testing.T) {
	raw := authoritativeMoneyFill(domain.SideBuy, domain.LiquidityRoleTaker)
	raw.Shares = "1"
	raw.Price = "0.333333"
	raw.GrossNotional = "0.333333"
	raw.FeeRateBPS = "17"
	raw.PlatformFeeRate = "0.0017"
	raw.FeeExponent = "1"
	raw.PlatformFee = "0.00037"
	raw.TotalFee = "0.00037"
	if _, err := calculateMoney(raw); err != nil {
		t.Fatalf("official five-decimal truncated fee was rejected: %v", err)
	}

	raw.Shares = "0.000001"
	raw.Price = "0.5"
	raw.GrossNotional = "0.0000005"
	raw.PlatformFee = "0"
	raw.TotalFee = "0"
	if _, err := calculateMoney(raw); err != nil {
		t.Fatalf("verified sub-minimum zero fee was rejected: %v", err)
	}
}

func TestCalculateMoneyAcceptsProductionV2SellFeeQuantum(t *testing.T) {
	raw := authoritativeMoneyFill(domain.SideSell, domain.LiquidityRoleTaker)
	raw.Shares = "5"
	raw.Price = "0.216"
	raw.GrossNotional = "1.08"
	raw.FeeRateBPS = "0"
	raw.PlatformFeeRate = "0.04"
	raw.FeeExponent = "1"
	raw.PlatformFee = "0.03386"
	raw.TotalFee = "0.03386"
	fill, err := calculateMoney(raw)
	if err != nil {
		t.Fatalf("production V2 fee quantum was rejected: %v", err)
	}
	if !fill.NetCashDelta.Equal("1.04614") {
		t.Fatalf("net cash delta = %s, want 1.04614", fill.NetCashDelta)
	}

	raw.PlatformFee = "0.03387"
	raw.TotalFee = "0.03387"
	if _, err := calculateMoney(raw); err == nil {
		t.Fatal("half-up fee quantum was accepted instead of the protocol-truncated fee")
	}
}

func TestCalculateMoneyFailsClosedWithoutEvidenceEvenForZeroFee(t *testing.T) {
	raw := authoritativeMoneyFill(domain.SideBuy, domain.LiquidityRoleMaker)
	raw.PlatformFee = "0"
	raw.TotalFee = "0"
	raw.FeeSource = ""
	if _, err := calculateMoney(raw); err == nil {
		t.Fatal("zero-fee fill without finalized evidence was accepted")
	}
	raw.FeeSource = domain.FeeSourcePolygonV2OrderFilled
	raw.TotalFee = ""
	if _, err := calculateMoney(raw); err == nil {
		t.Fatal("fill without authoritative total fee was accepted")
	}
}

func TestCalculateMoneyRejectsFeeCurveMismatch(t *testing.T) {
	raw := authoritativeMoneyFill(domain.SideBuy, domain.LiquidityRoleTaker)
	raw.PlatformFee = "0.01"
	raw.TotalFee = "0.01"
	if _, err := calculateMoney(raw); err == nil {
		t.Fatal("event fee inconsistent with the V2 fee schedule was accepted")
	}
}

func authoritativeMoneyFill(side domain.Side, role domain.LiquidityRole) domain.Fill {
	return domain.Fill{
		Venue: "polymarket", Side: side, LiquidityRole: role,
		Shares: "10", Price: "0.5", GrossNotional: "5",
		FeeRateBPS: "10", PlatformFeeRate: "0.001", FeeExponent: "1", PlatformFee: "0.0025",
		BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0.0025",
		FeeSource: domain.FeeSourcePolygonV2OrderFilled,
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
