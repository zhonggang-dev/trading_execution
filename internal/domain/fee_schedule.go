package domain

import (
	"fmt"
	"math/big"
	"strings"
	"time"
)

// MarketFeeSchedule 是 venue 公布的某个 token 的官方手续费曲线。
// Polymarket V2 的 taker 每股费用为 rate * (price * (1 - price))^exponent。
type MarketFeeSchedule struct {
	PlatformFeeRate Decimal
	FeeExponent     Decimal
	TakerOnly       bool
	FetchedAt       time.Time
}

const (
	// BuyFeeReserveSourceVenueFeeSchedule 表示预占手续费按 venue 官方费率曲线在
	// 可成交价格区间上的最大值计算。
	BuyFeeReserveSourceVenueFeeSchedule = "VENUE_FEE_SCHEDULE"
	// BuyFeeReserveSourceConfigCap 表示官方费率无法确认，预占退回执行侧配置的
	// 手续费率上限，绝不按零处理。
	BuyFeeReserveSourceConfigCap = "CONFIG_MAX_FEE_RATE_CAP"

	maxFeeExponentBits = 16
)

// BuyFeeReserve 记录 BUY 预占所使用的手续费依据，随 MarketValidation 持久化供审计。
type BuyFeeReserve struct {
	Source            string     `json:"source"`
	PlatformFeeRate   Decimal    `json:"platform_fee_rate,omitempty"`
	FeeExponent       Decimal    `json:"fee_exponent,omitempty"`
	MaxFeePerShare    Decimal    `json:"max_fee_per_share,omitempty"`
	ScheduleFetchedAt *time.Time `json:"schedule_fetched_at,omitempty"`
	Reason            string     `json:"reason,omitempty"`
}

// Validate 校验手续费预占依据的内部一致性。
func (reserve BuyFeeReserve) Validate() error {
	switch strings.TrimSpace(reserve.Source) {
	case BuyFeeReserveSourceVenueFeeSchedule:
		if sign, err := reserve.MaxFeePerShare.Sign(); err != nil || sign < 0 {
			return fmt.Errorf("venue fee schedule reserve requires a non-negative max_fee_per_share")
		}
		if sign, err := reserve.PlatformFeeRate.Sign(); err != nil || sign < 0 {
			return fmt.Errorf("venue fee schedule reserve requires a non-negative platform_fee_rate")
		}
		if err := validateFeeExponentDecimal(reserve.FeeExponent); err != nil {
			return err
		}
	case BuyFeeReserveSourceConfigCap:
		if !reserve.MaxFeePerShare.IsEmpty() {
			return fmt.Errorf("config cap fee reserve must not carry a venue max_fee_per_share")
		}
	default:
		return fmt.Errorf("unsupported buy fee reserve source %q", reserve.Source)
	}
	return nil
}

// VenueMaxFeePerShare 返回按 venue 官方费率确认的每股最坏手续费；官方费率未确认时返回 false，
// 调用方必须退回配置上限而不是按零预占。
func (reserve *BuyFeeReserve) VenueMaxFeePerShare() (Decimal, bool) {
	if reserve == nil || strings.TrimSpace(reserve.Source) != BuyFeeReserveSourceVenueFeeSchedule ||
		reserve.MaxFeePerShare.IsEmpty() {
		return "", false
	}
	return reserve.MaxFeePerShare, true
}

// MaxBuyFeePerShare 计算一个 BUY 在 worst_price 保护下可能支付的每股最坏手续费。
// 成交价只能落在 (0, worst_price]，而费率曲线 (p(1-p))^e 在 0.5 处取最大值，因此
// 最坏点是 min(worst_price, 0.5)，不是 worst_price 本身。exponent 为 0 时费用与价格无关。
func MaxBuyFeePerShare(worstPrice Decimal, schedule MarketFeeSchedule) (Decimal, error) {
	price, err := worstPrice.rat()
	if err != nil || price.Sign() <= 0 {
		return "", fmt.Errorf("worst_price must be a positive decimal")
	}
	rate, err := schedule.PlatformFeeRate.rat()
	if err != nil || rate.Sign() < 0 {
		return "", fmt.Errorf("platform_fee_rate must be a non-negative decimal")
	}
	if err := validateFeeExponentDecimal(schedule.FeeExponent); err != nil {
		return "", err
	}
	exponent, _ := schedule.FeeExponent.rat()
	half := big.NewRat(1, 2)
	if price.Cmp(half) > 0 {
		price = half
	}
	curve := new(big.Rat).Mul(price, new(big.Rat).Sub(big.NewRat(1, 1), price))
	powered := big.NewRat(1, 1)
	for step := uint64(0); step < exponent.Num().Uint64(); step++ {
		powered.Mul(powered, curve)
	}
	fee := new(big.Rat).Mul(rate, powered)
	return ratDecimalRoundedUp(fee, 18), nil
}

func validateFeeExponentDecimal(exponent Decimal) error {
	value, err := exponent.rat()
	if err != nil || value.Sign() < 0 || !value.IsInt() || value.Num().BitLen() > maxFeeExponentBits {
		return fmt.Errorf("fee_exponent must be a small non-negative integer")
	}
	return nil
}

// ratDecimalRoundedUp 把有理数转成不超过 maxScale 位小数的 decimal string。
// 能精确表示时保持精确；否则向上取整，保证预占永远不低于真实费用。
func ratDecimalRoundedUp(value *big.Rat, maxScale int) Decimal {
	if value.Sign() == 0 {
		return "0"
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(maxScale)), nil)
	scaled := new(big.Rat).Mul(value, new(big.Rat).SetInt(scale))
	numerator := new(big.Int).Set(scaled.Num())
	denominator := scaled.Denom()
	quotient, remainder := new(big.Int).QuoRem(numerator, denominator, new(big.Int))
	if remainder.Sign() > 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	text := quotient.String()
	negative := strings.HasPrefix(text, "-")
	text = strings.TrimPrefix(text, "-")
	for len(text) <= maxScale {
		text = "0" + text
	}
	integerPart := text[:len(text)-maxScale]
	fractionPart := strings.TrimRight(text[len(text)-maxScale:], "0")
	result := integerPart
	if fractionPart != "" {
		result += "." + fractionPart
	}
	if negative {
		result = "-" + result
	}
	return Decimal(result)
}
