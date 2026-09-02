package polymarket

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// roundConfig 表示后端使用的 roundConfig 类型。
type roundConfig struct {
	price  int
	size   int
	amount int
}

var roundingByTick = map[string]roundConfig{
	"0.1":    {price: 1, size: 2, amount: 3},
	"0.01":   {price: 2, size: 2, amount: 4},
	"0.005":  {price: 3, size: 2, amount: 5},
	"0.0025": {price: 4, size: 2, amount: 6},
	"0.001":  {price: 3, size: 2, amount: 5},
	"0.0001": {price: 4, size: 2, amount: 6},
}

// rawAmounts 表示后端使用的 rawAmounts 类型。
type rawAmounts struct {
	Price       domain.Decimal
	MakerAmount string
	TakerAmount string
	Side        uint8
}

// placementIntent derives the wire intent that is actually signed. The
// persisted OrderIntent keeps the strategy's size and worst_price for audit,
// reservation, and fill accounting. A marketable CLOB BUY (FOK/FAK) is a
// collateral budget: the exchange spends the full maker amount at any ask
// inside the limit and returns the resulting shares, so signing
// size*worst_price buys more than size shares when the book is better. The
// wire BUY is therefore priced at the market validation execution price (the
// fresh best ask), which makes the signed budget equal exactly size shares at
// the price that will match. worst_price only caps that execution price.
func placementIntent(order domain.Order) (domain.OrderIntent, error) {
	intent := order.Intent.Normalize()
	if intent.Side != domain.SideBuy ||
		(intent.TimeInForce != domain.TimeInForceFOK && intent.TimeInForce != domain.TimeInForceFAK) {
		return intent, nil
	}
	validation := order.MarketValidation
	if validation == nil || validation.ExecutionPrice.IsEmpty() {
		return domain.OrderIntent{}, newInvalidError("BUY_EXECUTION_PRICE_REQUIRED",
			"marketable Polymarket BUY requires the market validation execution_price; a size*worst_price budget can buy more than size shares")
	}
	if sign, err := validation.ExecutionPrice.Sign(); err != nil || sign <= 0 {
		return domain.OrderIntent{}, newInvalidError("BUY_EXECUTION_PRICE_INVALID", "market validation execution_price is not a positive decimal")
	}
	if comparison, err := validation.ExecutionPrice.Compare(intent.WorstPrice); err != nil || comparison > 0 {
		return domain.OrderIntent{}, newInvalidError("BUY_EXECUTION_PRICE_EXCEEDS_WORST_PRICE",
			"market validation execution_price is above the strategy worst_price protection ceiling")
	}
	intent.Type = domain.OrderTypeLimit
	intent.Price = validation.ExecutionPrice
	return intent, nil
}

// buildRawAmounts 根据参数构建 原始数据 Amounts。
func buildRawAmounts(intent domain.OrderIntent, tickSize, minOrderSize, minBuyNotional domain.Decimal) (rawAmounts, error) {
	config, exists := roundingByTick[canonicalDecimal(tickSize)]
	if !exists {
		return rawAmounts{}, newInvalidError("UNSUPPORTED_TICK_SIZE", "tick_size is not supported by the current CLOB V2 rounding table")
	}
	price := intent.Price
	if intent.Type == domain.OrderTypeMarket {
		price = intent.WorstPrice
	}
	if price.IsEmpty() {
		return rawAmounts{}, newInvalidError("PRICE_REQUIRED", "limit price or protected market worst_price is required")
	}
	if multiple, err := price.IsMultipleOf(tickSize); err != nil || !multiple {
		return rawAmounts{}, newInvalidError("PRICE_TICK_MISMATCH", "price is not an exact multiple of current tick_size")
	}
	priceRat, err := decimalRat(price)
	if err != nil {
		return rawAmounts{}, newInvalidError("INVALID_PRICE", err.Error())
	}
	tickRat, err := decimalRat(tickSize)
	if err != nil {
		return rawAmounts{}, newInvalidError("INVALID_TICK_SIZE", err.Error())
	}
	maximumPrice := new(big.Rat).Sub(big.NewRat(1, 1), tickRat)
	if priceRat.Cmp(tickRat) < 0 || priceRat.Cmp(maximumPrice) > 0 {
		return rawAmounts{}, newInvalidError("PRICE_OUT_OF_RANGE", "price must be between tick_size and 1-tick_size")
	}
	if decimalPlaces(price) > config.price {
		return rawAmounts{}, newInvalidError("INVALID_PRICE_PRECISION", fmt.Sprintf("price supports at most %d decimal places for tick_size %s", config.price, tickSize))
	}
	if decimalPlaces(intent.Size) > config.size {
		return rawAmounts{}, newInvalidError("INVALID_SIZE_PRECISION", fmt.Sprintf("size supports at most %d decimal places", config.size))
	}
	if !minOrderSize.IsEmpty() {
		if comparison, err := intent.Size.Compare(minOrderSize); err != nil || comparison < 0 {
			return rawAmounts{}, newInvalidError("MIN_ORDER_SIZE", "size is below the market min_order_size")
		}
	}

	notional, err := multiply(intent.Size, price)
	if err != nil {
		return rawAmounts{}, newInvalidError("INVALID_AMOUNTS", err.Error())
	}
	if intent.Side == domain.SideBuy && !minBuyNotional.IsEmpty() {
		minimum, err := decimalRat(minBuyNotional)
		if err != nil {
			return rawAmounts{}, newInvalidError("INVALID_MIN_NOTIONAL", err.Error())
		}
		if notional.Cmp(minimum) < 0 {
			return rawAmounts{}, newInvalidError("MIN_BUY_NOTIONAL", "BUY notional is below the configured CLOB minimum")
		}
	}

	amountPrecision := config.amount
	if intent.TimeInForce == domain.TimeInForceFAK || intent.TimeInForce == domain.TimeInForceFOK {
		// The matching endpoint applies a flat precision constraint to
		// marketable FAK/FOK amounts. Reject instead of silently changing size.
		if intent.Side == domain.SideBuy {
			if ratDecimalPlaces(notional) > 4 || decimalPlaces(intent.Size) > 4 {
				return rawAmounts{}, newInvalidError("INVALID_FAK_FOK_PRECISION", "FAK/FOK BUY requires maker notional <=4 decimals and shares <=4 decimals")
			}
		} else if decimalPlaces(intent.Size) > 2 || ratDecimalPlaces(notional) > 4 {
			return rawAmounts{}, newInvalidError("INVALID_FAK_FOK_PRECISION", "FAK/FOK SELL requires shares <=2 decimals and taker notional <=4 decimals")
		}
		amountPrecision = 4
	}
	if ratDecimalPlaces(notional) > amountPrecision {
		return rawAmounts{}, newInvalidError("INVALID_AMOUNT_PRECISION", fmt.Sprintf("price*size supports at most %d decimal places for this order", amountPrecision))
	}

	sizeRaw, err := ratToScaledInteger(mustDecimalRat(intent.Size), 6)
	if err != nil {
		return rawAmounts{}, newInvalidError("INVALID_SIZE_PRECISION", err.Error())
	}
	notionalRaw, err := ratToScaledInteger(notional, 6)
	if err != nil {
		return rawAmounts{}, newInvalidError("INVALID_AMOUNT_PRECISION", err.Error())
	}
	result := rawAmounts{Price: price}
	switch intent.Side {
	case domain.SideBuy:
		result.Side = 0
		result.MakerAmount = notionalRaw.String()
		result.TakerAmount = sizeRaw.String()
	case domain.SideSell:
		result.Side = 1
		result.MakerAmount = sizeRaw.String()
		result.TakerAmount = notionalRaw.String()
	default:
		return rawAmounts{}, newInvalidError("INVALID_SIDE", "side must be BUY or SELL")
	}
	return result, nil
}

// canonicalDecimal 生成 Decimal 的规范化表示。
func canonicalDecimal(value domain.Decimal) string {
	text := strings.TrimSpace(value.String())
	if !strings.Contains(text, ".") {
		return text
	}
	text = strings.TrimRight(text, "0")
	text = strings.TrimRight(text, ".")
	return text
}

// decimalPlaces 计算十进制值所需的小数位数。
func decimalPlaces(value domain.Decimal) int {
	text := canonicalDecimal(value)
	point := strings.IndexByte(text, '.')
	if point < 0 {
		return 0
	}
	return len(text) - point - 1
}

// decimalRat 将十进制值转换为精确有理数。
func decimalRat(value domain.Decimal) (*big.Rat, error) {
	text := strings.TrimSpace(value.String())
	parsed, ok := new(big.Rat).SetString(text)
	if !ok || text == "" || strings.ContainsAny(text, "/eE") {
		return nil, fmt.Errorf("invalid decimal %q", text)
	}
	return parsed, nil
}

// mustDecimalRat 将十进制值转换为精确有理数。
func mustDecimalRat(value domain.Decimal) *big.Rat {
	parsed, _ := decimalRat(value)
	return parsed
}

// multiply 使用有理数精确计算两个十进制值的乘积。
func multiply(left, right domain.Decimal) (*big.Rat, error) {
	l, err := decimalRat(left)
	if err != nil {
		return nil, err
	}
	r, err := decimalRat(right)
	if err != nil {
		return nil, err
	}
	return new(big.Rat).Mul(l, r), nil
}

// ratToScaledInteger 将有理数精确转换为指定位数的缩放整数。
func ratToScaledInteger(value *big.Rat, scale int) (*big.Int, error) {
	multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	scaled := new(big.Rat).Mul(value, new(big.Rat).SetInt(multiplier))
	if !scaled.IsInt() {
		return nil, fmt.Errorf("amount cannot be represented exactly with %d token decimals", scale)
	}
	return new(big.Int).Set(scaled.Num()), nil
}

// ratDecimalPlaces 计算十进制值所需的小数位数。
func ratDecimalPlaces(value *big.Rat) int {
	for scale := 0; scale <= 18; scale++ {
		if _, err := ratToScaledInteger(value, scale); err == nil {
			return scale
		}
	}
	return 19
}
