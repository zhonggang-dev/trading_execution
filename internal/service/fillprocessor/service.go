package fillprocessor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const feeScale = 5

// Params 表示后端使用的 Params 类型。
type Params struct {
	Orders port.OrderRepository
	Source port.FillSource
	Ledger port.FillLedger
	Now    func() time.Time
}

// Service 表示后端使用的 Service 类型。
type Service struct {
	orders port.OrderRepository
	source port.FillSource
	ledger port.FillLedger
	now    func() time.Time
}

// SyncResult 表示后端使用的 SyncResult 类型。
type SyncResult = port.FillSyncResult

// New 校验依赖和配置后创建当前服务实例。
func New(params Params) (*Service, error) {
	if params.Orders == nil || params.Source == nil || params.Ledger == nil {
		return nil, fmt.Errorf("orders, fill source, and fill ledger are required")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &Service{orders: params.Orders, source: params.Source, ledger: params.Ledger, now: params.Now}, nil
}

// SyncOrder 按事件时间同步订单真实成交并通过权威账本幂等入账。
func (service *Service) SyncOrder(ctx context.Context, orderID string) (SyncResult, error) {
	order, err := service.orders.Get(ctx, strings.TrimSpace(orderID))
	if err != nil {
		return SyncResult{}, err
	}
	fills, err := service.source.ListOrderFills(ctx, order)
	if err != nil {
		return SyncResult{OrderID: order.ID}, fmt.Errorf("list real fills: %w", err)
	}
	sort.Slice(fills, func(left, right int) bool {
		if fills[left].MatchedAt.Equal(fills[right].MatchedAt) {
			return fills[left].VenueFillID < fills[right].VenueFillID
		}
		return fills[left].MatchedAt.Before(fills[right].MatchedAt)
	})
	result := SyncResult{OrderID: order.ID, Observed: len(fills)}
	for _, raw := range fills {
		var application domain.FillApplication
		for revisionAttempt := 0; revisionAttempt < 3; revisionAttempt++ {
			application, err = service.Process(ctx, order, raw)
			if !errors.Is(err, port.ErrOrderRevisionConflict) {
				break
			}
			order, err = service.orders.Get(ctx, order.ID)
			if err != nil {
				break
			}
		}
		if err != nil {
			return result, fmt.Errorf("process fill %s: %w", raw.VenueFillID, err)
		}
		order = application.Order
		result.Applications = append(result.Applications, application)
		if application.Applied {
			result.Applied++
		}
		if application.Duplicate {
			result.Duplicates++
		}
	}
	return result, nil
}

// Sync 同步指定订单的真实成交并仅返回执行错误。
func (service *Service) Sync(ctx context.Context, orderID string) error {
	_, err := service.SyncOrder(ctx, orderID)
	return err
}

// Process 补全并校验原始成交后提交权威账本入账。
func (service *Service) Process(ctx context.Context, order domain.Order, raw domain.Fill) (domain.FillApplication, error) {
	fill, err := prepareFill(order, raw, service.now().UTC())
	if err != nil {
		return domain.FillApplication{}, err
	}
	return service.ledger.Record(ctx, order, fill)
}

// prepareFill 补全并校验 Fill。
func prepareFill(order domain.Order, fill domain.Fill, now time.Time) (domain.Fill, error) {
	fill = fill.Normalize()
	if fill.OrderID == "" {
		fill.OrderID = order.ID
	}
	if fill.Venue == "" {
		fill.Venue = order.Intent.Venue
	}
	if fill.VenueOrderID == "" {
		fill.VenueOrderID = order.VenueOrderID
	}
	if fill.ExecutionAccountID == "" {
		fill.ExecutionAccountID = order.Intent.ExecutionAccountID
	}
	if fill.MarketID == "" {
		fill.MarketID = order.Intent.MarketID
	}
	if fill.ConditionID == "" {
		fill.ConditionID = order.Intent.ConditionID
	}
	if fill.TokenID == "" {
		fill.TokenID = order.Intent.TokenID
	}
	if fill.Side == "" {
		fill.Side = order.Intent.Side
	}
	if fill.ObservedAt.IsZero() {
		fill.ObservedAt = now
	}
	if fill.Status == domain.FillStatusConfirmed && fill.ConfirmedAt == nil {
		value := fill.VenueUpdatedAt
		if value.IsZero() {
			value = fill.ObservedAt
		}
		fill.ConfirmedAt = &value
	}
	if fill.Key == "" {
		fill.Key = FillKey(fill.Venue, fill.VenueFillID, fill.OrderID)
	}
	if fill.OrderID != order.ID || fill.ExecutionAccountID != order.Intent.ExecutionAccountID ||
		fill.MarketID != order.Intent.MarketID || fill.TokenID != order.Intent.TokenID ||
		fill.Side != order.Intent.Side || !strings.EqualFold(fill.VenueOrderID, order.VenueOrderID) {
		return domain.Fill{}, port.ErrFillConflict
	}
	if err := fill.Validate(); err != nil {
		return domain.Fill{}, err
	}
	return calculateMoney(fill)
}

// FillKey 根据交易所、成交和订单身份计算稳定成交幂等键。
func FillKey(venue, venueFillID, orderID string) string {
	digest := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(venue)) + "\x00" + strings.TrimSpace(venueFillID) + "\x00" + strings.TrimSpace(orderID)))
	return "fill-" + hex.EncodeToString(digest[:])
}

// calculateMoney validates authoritative settlement amounts and derives only
// the direction-dependent cash delta. It must never reconstruct a final fee
// from CLOB rate metadata: even a zero fee requires settlement evidence.
func calculateMoney(fill domain.Fill) (domain.Fill, error) {
	for name, value := range map[string]domain.Decimal{
		"gross_notional":       fill.GrossNotional,
		"platform_fee_rate":    fill.PlatformFeeRate,
		"fee_exponent":         fill.FeeExponent,
		"platform_fee":         fill.PlatformFee,
		"builder_fee_rate_bps": fill.BuilderFeeRateBPS,
		"builder_fee":          fill.BuilderFee,
		"total_fee":            fill.TotalFee,
	} {
		if value.IsEmpty() {
			return domain.Fill{}, fmt.Errorf("authoritative fee evidence omitted %s", name)
		}
		if sign, err := value.Sign(); err != nil || sign < 0 {
			return domain.Fill{}, fmt.Errorf("%s must be a non-negative decimal", name)
		}
	}
	fill.FeeSource = strings.ToUpper(strings.TrimSpace(fill.FeeSource))
	if fill.FeeSource == "" {
		return domain.Fill{}, fmt.Errorf("authoritative fee_source is required even when total_fee is zero")
	}
	if strings.EqualFold(fill.Venue, "polymarket") && fill.FeeSource != domain.FeeSourcePolygonV2OrderFilled {
		return domain.Fill{}, fmt.Errorf("Polymarket fill requires finalized V2 OrderFilled fee evidence")
	}

	shares, err := decimalRat(fill.Shares)
	if err != nil {
		return domain.Fill{}, err
	}
	price, err := decimalRat(fill.Price)
	if err != nil {
		return domain.Fill{}, err
	}
	gross, err := decimalRat(fill.GrossNotional)
	if err != nil {
		return domain.Fill{}, err
	}
	if err := validateGrossEvidence(shares, price, gross, fill.FeeSource); err != nil {
		return domain.Fill{}, err
	}

	platformFee, _ := decimalRat(fill.PlatformFee)
	builderFee, _ := decimalRat(fill.BuilderFee)
	totalFee, _ := decimalRat(fill.TotalFee)
	if new(big.Rat).Add(platformFee, builderFee).Cmp(totalFee) != 0 {
		return domain.Fill{}, fmt.Errorf("authoritative total_fee must equal platform_fee plus builder_fee")
	}
	if fill.LiquidityRole == domain.LiquidityRoleMaker {
		if platformFee.Sign() != 0 {
			return domain.Fill{}, fmt.Errorf("V2 maker fill cannot contain a platform fee")
		}
	} else if fill.FeeSource == domain.FeeSourcePolygonV2OrderFilled {
		expected, err := expectedV2PlatformFee(shares, price, fill.PlatformFeeRate, fill.FeeExponent)
		if err != nil {
			return domain.Fill{}, err
		}
		if !fill.PlatformFee.Equal(expected) {
			return domain.Fill{}, fmt.Errorf("authoritative platform_fee %s does not match V2 fee curve %s", fill.PlatformFee, expected)
		}
	}

	change := new(big.Rat)
	switch fill.Side {
	case domain.SideBuy:
		change.Neg(new(big.Rat).Add(gross, totalFee))
	case domain.SideSell:
		change.Sub(gross, totalFee)
		if change.Sign() < 0 {
			return domain.Fill{}, fmt.Errorf("sell fees exceed gross proceeds")
		}
	default:
		return domain.Fill{}, fmt.Errorf("unsupported fill side %q", fill.Side)
	}
	fill.NetCashDelta, err = exactDecimal(change, 18)
	if err != nil {
		return domain.Fill{}, err
	}
	return fill.Normalize(), nil
}

func validateGrossEvidence(shares, price, gross *big.Rat, source string) error {
	calculated := new(big.Rat).Mul(shares, price)
	difference := new(big.Rat).Sub(calculated, gross)
	if difference.Sign() < 0 {
		difference.Neg(difference)
	}
	if source == domain.FeeSourcePolygonV2OrderFilled {
		if difference.Cmp(big.NewRat(1, 1_000_000)) >= 0 {
			return fmt.Errorf("OrderFilled gross_notional differs from shares multiplied by price by at least one pUSD base unit")
		}
		return nil
	}
	if difference.Sign() != 0 {
		return fmt.Errorf("gross_notional must equal shares multiplied by price")
	}
	return nil
}

func expectedV2PlatformFee(
	shares *big.Rat,
	price *big.Rat,
	platformFeeRate domain.Decimal,
	feeExponent domain.Decimal,
) (domain.Decimal, error) {
	rate, err := decimalRat(platformFeeRate)
	if err != nil || rate.Sign() < 0 {
		return "", fmt.Errorf("platform_fee_rate must be non-negative")
	}
	exponent, err := decimalRat(feeExponent)
	if err != nil || exponent.Sign() < 0 || !exponent.IsInt() || exponent.Num().BitLen() > 16 {
		return "", fmt.Errorf("fee_exponent must be a non-negative integer")
	}
	curve := new(big.Rat).Mul(price, new(big.Rat).Sub(big.NewRat(1, 1), price))
	powered := big.NewRat(1, 1)
	for value := uint64(0); value < exponent.Num().Uint64(); value++ {
		powered.Mul(powered, curve)
	}
	fee := new(big.Rat).Mul(shares, rate)
	fee.Mul(fee, powered)
	return roundedDecimal(fee, feeScale), nil
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

// exactDecimal 将有理数精确转换为不超过指定位数的十进制值。
func exactDecimal(value *big.Rat, maxScale int) (domain.Decimal, error) {
	for scale := 0; scale <= maxScale; scale++ {
		text := value.FloatString(scale)
		parsed, _ := new(big.Rat).SetString(text)
		if parsed.Cmp(value) == 0 {
			return domain.Decimal(canonical(text)), nil
		}
	}
	return "", fmt.Errorf("value cannot be represented exactly within %d decimals", maxScale)
}

// roundedDecimal 按指定位数对有理数进行半远离零舍入。
func roundedDecimal(value *big.Rat, scale int) domain.Decimal {
	multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	scaledNumerator := new(big.Int).Mul(value.Num(), multiplier)
	quotient, remainder := new(big.Int).QuoRem(scaledNumerator, value.Denom(), new(big.Int))
	if new(big.Int).Mul(new(big.Int).Abs(remainder), big.NewInt(2)).Cmp(value.Denom()) >= 0 {
		if value.Sign() >= 0 {
			quotient.Add(quotient, big.NewInt(1))
		} else {
			quotient.Sub(quotient, big.NewInt(1))
		}
	}
	negative := quotient.Sign() < 0
	digits := new(big.Int).Abs(quotient).String()
	for len(digits) <= scale {
		digits = "0" + digits
	}
	if scale > 0 {
		digits = digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
	}
	if negative {
		digits = "-" + digits
	}
	return domain.Decimal(canonical(digits))
}

// canonical 生成 对应数据 的规范化表示。
func canonical(value string) string {
	if strings.Contains(value, ".") {
		value = strings.TrimRight(value, "0")
		value = strings.TrimRight(value, ".")
	}
	if value == "-0" || value == "" {
		return "0"
	}
	return value
}
