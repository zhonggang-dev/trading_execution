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

// calculateMoney 精确计算 Money。
func calculateMoney(fill domain.Fill) (domain.Fill, error) {
	shares, err := decimalRat(fill.Shares)
	if err != nil {
		return domain.Fill{}, err
	}
	price, err := decimalRat(fill.Price)
	if err != nil {
		return domain.Fill{}, err
	}
	gross := new(big.Rat).Mul(shares, price)
	fill.GrossNotional, err = exactDecimal(gross, 18)
	if err != nil {
		return domain.Fill{}, fmt.Errorf("gross notional: %w", err)
	}

	builderFeeSource := "BUILDER_REPORTED"
	if fill.BuilderFeeRateBPS.IsEmpty() {
		fill.BuilderFeeRateBPS = "0"
	}
	if fill.BuilderFee.IsEmpty() {
		builderRate, rateErr := decimalRat(fill.BuilderFeeRateBPS)
		if rateErr != nil || builderRate.Sign() < 0 {
			return domain.Fill{}, fmt.Errorf("builder_fee_rate_bps must be non-negative")
		}
		builderFee := new(big.Rat).Mul(gross, builderRate)
		builderFee.Quo(builderFee, big.NewRat(10000, 1))
		fill.BuilderFee = roundedDecimal(builderFee, 6)
		builderFeeSource = "BUILDER_RATE"
		if builderRate.Sign() == 0 {
			builderFeeSource = "BUILDER_NONE"
		}
	}
	if sign, err := fill.BuilderFee.Sign(); err != nil || sign < 0 {
		return domain.Fill{}, fmt.Errorf("builder_fee must be non-negative")
	}
	platformFeeSource := "VENUE_REPORTED"
	if fill.PlatformFee.IsEmpty() {
		if fill.LiquidityRole == domain.LiquidityRoleMaker {
			fill.PlatformFee = "0"
			fill.FeeRateBPS = "0"
			platformFeeSource = "PROTOCOL_MAKER_ZERO"
		} else {
			if fill.FeeRateBPS.IsEmpty() {
				return domain.Fill{}, fmt.Errorf("taker fill requires fee_rate_bps or reported platform_fee")
			}
			rate, err := decimalRat(fill.FeeRateBPS)
			if err != nil || rate.Sign() < 0 {
				return domain.Fill{}, fmt.Errorf("fee_rate_bps must be non-negative")
			}
			fee := new(big.Rat).Mul(shares, price)
			fee.Mul(fee, new(big.Rat).Sub(big.NewRat(1, 1), price))
			fee.Mul(fee, rate)
			fee.Quo(fee, big.NewRat(10000, 1))
			fill.PlatformFee = roundedDecimal(fee, feeScale)
			platformFeeSource = "CALCULATED_FROM_TRADE_FEE_RATE"
		}
	} else {
		if sign, err := fill.PlatformFee.Sign(); err != nil || sign < 0 {
			return domain.Fill{}, fmt.Errorf("platform_fee must be non-negative")
		}
		if fill.FeeRateBPS.IsEmpty() {
			fill.FeeRateBPS = "0"
		}
	}
	platformFee, _ := decimalRat(fill.PlatformFee)
	builderFee, _ := decimalRat(fill.BuilderFee)
	totalFee := new(big.Rat).Add(platformFee, builderFee)
	fill.TotalFee, err = exactDecimal(totalFee, 18)
	if err != nil {
		return domain.Fill{}, err
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
	fill.FeeSource = platformFeeSource + "+" + builderFeeSource
	return fill.Normalize(), nil
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
