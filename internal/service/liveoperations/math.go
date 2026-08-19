package liveoperations

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// decimalRat 将领域十进制数解析为精确有理数。
func decimalRat(value domain.Decimal) (*big.Rat, error) {
	parsed, err := domain.ParseDecimal(value.String())
	if err != nil {
		return nil, err
	}
	result, ok := new(big.Rat).SetString(parsed.String())
	if !ok {
		return nil, fmt.Errorf("invalid decimal %q", value)
	}
	return result, nil
}

// numberFromDecimal 将领域十进制数转换为运维 JSON 数字。
func numberFromDecimal(value domain.Decimal) (domain.LiveNumber, error) {
	if value.IsEmpty() {
		value = "0"
	}
	return domain.NewLiveNumber(value)
}

// numberFromRat 将计算结果编码为最多十二位小数的 JSON 数字。
func numberFromRat(value *big.Rat) (domain.LiveNumber, error) {
	if value == nil {
		return "", fmt.Errorf("live number value is nil")
	}
	text := value.FloatString(12)
	text = strings.TrimRight(strings.TrimRight(text, "0"), ".")
	if text == "" || text == "-0" {
		text = "0"
	}
	return domain.NewLiveNumber(domain.Decimal(text))
}

// addDecimal 把一个十进制数累加到目标值。
func addDecimal(total *big.Rat, value domain.Decimal) error {
	parsed, err := decimalRat(value)
	if err != nil {
		return err
	}
	total.Add(total, parsed)
	return nil
}

// multiplyDecimal 精确计算两个领域十进制数的乘积。
func multiplyDecimal(left domain.Decimal, right domain.Decimal) (*big.Rat, error) {
	leftValue, err := decimalRat(left)
	if err != nil {
		return nil, err
	}
	rightValue, err := decimalRat(right)
	if err != nil {
		return nil, err
	}
	return new(big.Rat).Mul(leftValue, rightValue), nil
}

// midpoint 计算盘口最佳买卖价的算术中点。
func midpoint(bid domain.Decimal, ask domain.Decimal) (domain.Decimal, error) {
	left, err := decimalRat(bid)
	if err != nil {
		return "", err
	}
	right, err := decimalRat(ask)
	if err != nil {
		return "", err
	}
	value := new(big.Rat).Quo(new(big.Rat).Add(left, right), big.NewRat(2, 1))
	number, err := numberFromRat(value)
	return domain.Decimal(number), err
}

// ratio 计算两个非负数的比例，分母为零时返回零。
func ratio(numerator *big.Rat, denominator *big.Rat) *big.Rat {
	if numerator == nil || denominator == nil || denominator.Sign() == 0 {
		return new(big.Rat)
	}
	return new(big.Rat).Quo(numerator, denominator)
}
