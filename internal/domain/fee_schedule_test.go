package domain

import (
	"math/big"
	"testing"
)

// TestMaxBuyFeePerShareUsesCurveMaximumInsideWorstPrice 验证最坏手续费取的是
// 可成交价格区间上的曲线最大值，而不是 worst_price 处的值。
func TestMaxBuyFeePerShareUsesCurveMaximumInsideWorstPrice(t *testing.T) {
	cases := []struct {
		name       string
		worstPrice Decimal
		rate       Decimal
		exponent   Decimal
		want       Decimal
	}{
		// 0.2 * (0.3 * 0.7)^1 = 0.042
		{name: "below half uses worst price", worstPrice: "0.3", rate: "0.2", exponent: "1", want: "0.042"},
		// worst 0.8 允许成交到 0.5，曲线最大值 0.25：0.2 * 0.25 = 0.05
		{name: "above half uses curve peak", worstPrice: "0.8", rate: "0.2", exponent: "1", want: "0.05"},
		// 0.25 * (0.25)^2 = 0.015625
		{name: "exponent two at half", worstPrice: "0.5", rate: "0.25", exponent: "2", want: "0.015625"},
		{name: "exponent zero is flat", worstPrice: "0.01", rate: "0.02", exponent: "0", want: "0.02"},
		{name: "zero rate", worstPrice: "0.6", rate: "0", exponent: "2", want: "0"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := MaxBuyFeePerShare(testCase.worstPrice, MarketFeeSchedule{
				PlatformFeeRate: testCase.rate, FeeExponent: testCase.exponent,
			})
			if err != nil {
				t.Fatalf("MaxBuyFeePerShare() error = %v", err)
			}
			if !got.Equal(testCase.want) {
				t.Fatalf("MaxBuyFeePerShare() = %s, want %s", got, testCase.want)
			}
		})
	}
}

// TestMaxBuyFeePerShareRejectsInvalidInputs 验证非法价格和费率参数 fail closed。
func TestMaxBuyFeePerShareRejectsInvalidInputs(t *testing.T) {
	invalid := []struct {
		name     string
		price    Decimal
		schedule MarketFeeSchedule
	}{
		{name: "zero price", price: "0", schedule: MarketFeeSchedule{PlatformFeeRate: "0.1", FeeExponent: "1"}},
		{name: "negative rate", price: "0.5", schedule: MarketFeeSchedule{PlatformFeeRate: "-0.1", FeeExponent: "1"}},
		{name: "fractional exponent", price: "0.5", schedule: MarketFeeSchedule{PlatformFeeRate: "0.1", FeeExponent: "1.5"}},
		{name: "negative exponent", price: "0.5", schedule: MarketFeeSchedule{PlatformFeeRate: "0.1", FeeExponent: "-1"}},
		{name: "empty rate", price: "0.5", schedule: MarketFeeSchedule{FeeExponent: "1"}},
	}
	for _, testCase := range invalid {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := MaxBuyFeePerShare(testCase.price, testCase.schedule); err == nil {
				t.Fatal("MaxBuyFeePerShare() error = nil, want rejection")
			}
		})
	}
}

// TestBuyFeeReserveVenueMaxFeePerShare 验证只有官方费率来源才会提供每股手续费，
// 配置上限回退必须让调用方走 cap 路径。
func TestBuyFeeReserveVenueMaxFeePerShare(t *testing.T) {
	var missing *BuyFeeReserve
	if _, ok := missing.VenueMaxFeePerShare(); ok {
		t.Fatal("nil reserve must not provide a venue fee")
	}
	fallback := &BuyFeeReserve{Source: BuyFeeReserveSourceConfigCap, Reason: "schedule unavailable"}
	if err := fallback.Validate(); err != nil {
		t.Fatalf("fallback Validate() error = %v", err)
	}
	if _, ok := fallback.VenueMaxFeePerShare(); ok {
		t.Fatal("config cap reserve must not provide a venue fee")
	}
	venue := &BuyFeeReserve{
		Source: BuyFeeReserveSourceVenueFeeSchedule, PlatformFeeRate: "0.2", FeeExponent: "1", MaxFeePerShare: "0.05",
	}
	if err := venue.Validate(); err != nil {
		t.Fatalf("venue Validate() error = %v", err)
	}
	if fee, ok := venue.VenueMaxFeePerShare(); !ok || !fee.Equal("0.05") {
		t.Fatalf("VenueMaxFeePerShare() = %s, %v", fee, ok)
	}
	bad := BuyFeeReserve{Source: BuyFeeReserveSourceVenueFeeSchedule, PlatformFeeRate: "0.2", FeeExponent: "1"}
	if err := bad.Validate(); err == nil {
		t.Fatal("venue reserve without max_fee_per_share must fail validation")
	}
	if err := (BuyFeeReserve{Source: "GUESS"}).Validate(); err == nil {
		t.Fatal("unknown source must fail validation")
	}
}

// TestRatDecimalRoundedUp 验证有理数转 decimal 精确或向上取整。
func TestRatDecimalRoundedUp(t *testing.T) {
	exact, _ := Decimal("0.125").rat()
	if got := ratDecimalRoundedUp(exact, 18); got != "0.125" {
		t.Fatalf("exact = %s", got)
	}
	third, _ := Decimal("1").rat()
	third.Quo(third, new(big.Rat).SetInt64(3))
	if got := ratDecimalRoundedUp(third, 3); got != "0.334" {
		t.Fatalf("rounded up = %s", got)
	}
}
