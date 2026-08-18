package domain

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
)

// Decimal 表示以 JSON 字符串传输的十进制数，避免 float64 的二进制舍入改变价格、数量或成交结果。
type Decimal string

// ParseDecimal 解析并规范化一个十进制字符串。
func ParseDecimal(value string) (Decimal, error) {
	decimal := Decimal(strings.TrimSpace(value))
	if _, err := decimal.rat(); err != nil {
		return "", err
	}
	return decimal, nil
}

// String 返回当前值的字符串表示。
func (d Decimal) String() string {
	return string(d)
}

// IsEmpty 判断十进制值是否为空。
func (d Decimal) IsEmpty() bool {
	return strings.TrimSpace(string(d)) == ""
}

// Sign 解析十进制值并返回其正负符号。
func (d Decimal) Sign() (int, error) {
	value, err := d.rat()
	if err != nil {
		return 0, err
	}
	return value.Sign(), nil
}

// Compare 精确比较两个十进制值的大小。
func (d Decimal) Compare(other Decimal) (int, error) {
	left, err := d.rat()
	if err != nil {
		return 0, err
	}
	right, err := other.rat()
	if err != nil {
		return 0, err
	}
	return left.Cmp(right), nil
}

// Multiply 使用有理数精确计算两个十进制值的乘积。
func (d Decimal) Multiply(other Decimal) (*big.Rat, error) {
	left, err := d.rat()
	if err != nil {
		return nil, err
	}
	right, err := other.rat()
	if err != nil {
		return nil, err
	}
	return new(big.Rat).Mul(left, right), nil
}

// IsMultipleOf 精确判断当前值是否为指定步长的整数倍。
func (d Decimal) IsMultipleOf(step Decimal) (bool, error) {
	value, err := d.rat()
	if err != nil {
		return false, err
	}
	stepValue, err := step.rat()
	if err != nil {
		return false, err
	}
	if stepValue.Sign() <= 0 {
		return false, fmt.Errorf("decimal step must be positive")
	}
	return new(big.Rat).Quo(value, stepValue).IsInt(), nil
}

// Equal 判断两个值在规范化后是否相等。
func (d Decimal) Equal(other Decimal) bool {
	if d.IsEmpty() || other.IsEmpty() {
		return d.IsEmpty() && other.IsEmpty()
	}
	comparison, err := d.Compare(other)
	return err == nil && comparison == 0
}

// MarshalJSON 将十进制值编码为 JSON 字符串。
func (d Decimal) MarshalJSON() ([]byte, error) {
	if d.IsEmpty() {
		return []byte("null"), nil
	}
	if _, err := d.rat(); err != nil {
		return nil, err
	}
	return json.Marshal(string(d))
}

// UnmarshalJSON 从 JSON 字符串解码并校验十进制值。
func (d *Decimal) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*d = ""
		return nil
	}
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("decimal must be encoded as a JSON string: %w", err)
	}
	decimal, err := ParseDecimal(value)
	if err != nil {
		return err
	}
	*d = decimal
	return nil
}

// rat 将十进制值转换为精确有理数。
func (d Decimal) rat() (*big.Rat, error) {
	value := strings.TrimSpace(string(d))
	if value == "" {
		return nil, fmt.Errorf("decimal is required")
	}
	parsed, ok := new(big.Rat).SetString(value)
	if !ok || strings.ContainsAny(value, "/eE") {
		return nil, fmt.Errorf("invalid base-10 decimal %q", value)
	}
	return parsed, nil
}
