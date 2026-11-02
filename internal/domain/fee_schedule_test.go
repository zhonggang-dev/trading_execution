package domain

import (
	"errors"
	"math/big"
	"testing"
	"time"
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

// TestMaxBuyFeePerShareAddsRoundingAllowance 验证按订单取整的 venue（Kalshi）
// 在曲线最大值之外再加每股取整余量，负余量 fail closed。
func TestMaxBuyFeePerShareAddsRoundingAllowance(t *testing.T) {
	schedule := MarketFeeSchedule{PlatformFeeRate: "0.07", FeeExponent: "1", RoundingAllowancePerShare: "0.01"}
	// 0.07 * 0.42 * 0.58 + 0.01 = 0.027052
	got, err := MaxBuyFeePerShare("0.42", schedule)
	if err != nil || !got.Equal("0.027052") {
		t.Fatalf("MaxBuyFeePerShare(0.42) = %s, %v", got, err)
	}
	// worst 0.54 允许成交到 0.5：0.07 * 0.25 + 0.01 = 0.0275
	got, err = MaxBuyFeePerShare("0.54", schedule)
	if err != nil || !got.Equal("0.0275") {
		t.Fatalf("MaxBuyFeePerShare(0.54) = %s, %v", got, err)
	}
	schedule.RoundingAllowancePerShare = "-0.01"
	if _, err := MaxBuyFeePerShare("0.42", schedule); err == nil {
		t.Fatal("negative rounding allowance must fail closed")
	}
}

// TestResolveBuyFeeReserve 验证费率确认时记录官方曲线依据，读取失败或曲线无效时
// 退回配置上限并记录原因。
func TestResolveBuyFeeReserve(t *testing.T) {
	fetchedAt := time.Date(2026, 11, 2, 1, 2, 3, 0, time.UTC)
	reserve := ResolveBuyFeeReserve("0.49", MarketFeeSchedule{
		PlatformFeeRate: "0.07", FeeExponent: "1", RoundingAllowancePerShare: "0.01", FetchedAt: fetchedAt,
	}, nil)
	if err := reserve.Validate(); err != nil {
		t.Fatalf("venue reserve Validate() error = %v", err)
	}
	// 0.07 * 0.49 * 0.51 + 0.01 = 0.027493
	if fee, ok := reserve.VenueMaxFeePerShare(); !ok || !fee.Equal("0.027493") ||
		!reserve.RoundingAllowancePerShare.Equal("0.01") || reserve.ScheduleFetchedAt == nil || !reserve.ScheduleFetchedAt.Equal(fetchedAt) {
		t.Fatalf("venue reserve = %#v", reserve)
	}
	unavailable := ResolveBuyFeeReserve("0.49", MarketFeeSchedule{}, errors.New("schedule unavailable"))
	if unavailable.Source != BuyFeeReserveSourceConfigCap || unavailable.Reason != "schedule unavailable" {
		t.Fatalf("unavailable reserve = %#v", unavailable)
	}
	invalid := ResolveBuyFeeReserve("0.49", MarketFeeSchedule{PlatformFeeRate: "-1", FeeExponent: "1"}, nil)
	if invalid.Source != BuyFeeReserveSourceConfigCap || invalid.Reason == "" {
		t.Fatalf("invalid reserve = %#v", invalid)
	}
	if err := (BuyFeeReserve{Source: BuyFeeReserveSourceVenueFeeSchedule, PlatformFeeRate: "0.07", FeeExponent: "1",
		MaxFeePerShare: "0.03", RoundingAllowancePerShare: "-0.01"}).Validate(); err == nil {
		t.Fatal("negative rounding allowance must fail validation")
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
