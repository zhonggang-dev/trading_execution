package domain

import (
	"fmt"
	"math/big"
	"strings"
	"time"
)

// FillStatus 表示后端使用的 FillStatus 类型。
type FillStatus string

const (
	FillStatusMatched   FillStatus = "MATCHED"
	FillStatusMined     FillStatus = "MINED"
	FillStatusConfirmed FillStatus = "CONFIRMED"
	FillStatusRetrying  FillStatus = "RETRYING"
	FillStatusFailed    FillStatus = "FAILED"
)

// LiquidityRole 表示后端使用的 LiquidityRole 类型。
type LiquidityRole string

const (
	LiquidityRoleMaker LiquidityRole = "MAKER"
	LiquidityRoleTaker LiquidityRole = "TAKER"

	// FeeSourcePolygonV2OrderFilled means final money fields were verified
	// against a finalized Polygon CLOB V2 OrderFilled event.
	FeeSourcePolygonV2OrderFilled = "POLYGON_V2_ORDER_FILLED"
)

// Fill 表示一个订单在 Polymarket 交易中的成交分量，唯一身份由 venue、venue_fill_id 和 order_id 共同确定。
type Fill struct {
	Key                string        `json:"fill_key"`
	Venue              string        `json:"venue"`
	VenueFillID        string        `json:"venue_fill_id"`
	OrderID            string        `json:"order_id"`
	VenueOrderID       string        `json:"venue_order_id"`
	ExecutionAccountID string        `json:"execution_account_id"`
	MarketID           string        `json:"market_id"`
	ConditionID        string        `json:"condition_id,omitempty"`
	TokenID            string        `json:"token_id"`
	Side               Side          `json:"side"`
	LiquidityRole      LiquidityRole `json:"liquidity_role"`
	Status             FillStatus    `json:"status"`
	Shares             Decimal       `json:"shares"`
	Price              Decimal       `json:"price"`
	// PriceTickSize comes from the order's persisted market validation. It is
	// intentionally not part of public fill JSON or settlement evidence: every
	// reconciliation reconstructs it from the immutable order context.
	PriceTickSize Decimal `json:"-"`
	GrossNotional Decimal `json:"gross_notional"`
	// FeeRateBPS is the CLOB trade response metadata. V2 final fee accounting
	// uses PlatformFeeRate and FeeExponent from /clob-markets instead.
	FeeRateBPS        Decimal    `json:"fee_rate_bps"`
	PlatformFeeRate   Decimal    `json:"platform_fee_rate"`
	FeeExponent       Decimal    `json:"fee_exponent"`
	PlatformFee       Decimal    `json:"platform_fee"`
	BuilderFeeRateBPS Decimal    `json:"builder_fee_rate_bps"`
	BuilderFee        Decimal    `json:"builder_fee"`
	TotalFee          Decimal    `json:"total_fee"`
	NetCashDelta      Decimal    `json:"net_cash_delta"`
	TransactionHash   string     `json:"transaction_hash,omitempty"`
	MatchedAt         time.Time  `json:"matched_at"`
	VenueUpdatedAt    time.Time  `json:"venue_updated_at,omitempty"`
	ObservedAt        time.Time  `json:"observed_at"`
	ConfirmedAt       *time.Time `json:"confirmed_at,omitempty"`
	AppliedAt         *time.Time `json:"applied_at,omitempty"`
	FeeSource         string     `json:"fee_source"`
	RawPayloadSHA256  string     `json:"raw_payload_sha256,omitempty"`
	// SettlementEvidence is intentionally excluded from public JSON. The
	// PostgreSQL fill ledger persists it as the immutable on-chain proof for
	// authoritative Polygon settlements.
	SettlementEvidence *SettlementEvidence `json:"-"`
}

// Normalize 规范化当前模型的文本、时间和可变字段。
func (fill Fill) Normalize() Fill {
	fill.Key = strings.TrimSpace(fill.Key)
	fill.Venue = strings.ToLower(strings.TrimSpace(fill.Venue))
	fill.VenueFillID = strings.TrimSpace(fill.VenueFillID)
	fill.OrderID = strings.TrimSpace(fill.OrderID)
	fill.VenueOrderID = strings.TrimSpace(fill.VenueOrderID)
	fill.ExecutionAccountID = strings.TrimSpace(fill.ExecutionAccountID)
	fill.MarketID = strings.TrimSpace(fill.MarketID)
	fill.ConditionID = strings.TrimSpace(fill.ConditionID)
	fill.TokenID = strings.TrimSpace(fill.TokenID)
	fill.Side = Side(strings.ToUpper(strings.TrimSpace(string(fill.Side))))
	fill.LiquidityRole = LiquidityRole(strings.ToUpper(strings.TrimSpace(string(fill.LiquidityRole))))
	fill.Status = NormalizeFillStatus(fill.Status)
	fill.TransactionHash = strings.TrimSpace(fill.TransactionHash)
	fill.FeeSource = strings.ToUpper(strings.TrimSpace(fill.FeeSource))
	fill.PriceTickSize = Decimal(strings.TrimSpace(fill.PriceTickSize.String()))
	fill.RawPayloadSHA256 = strings.ToLower(strings.TrimSpace(fill.RawPayloadSHA256))
	if fill.SettlementEvidence != nil {
		value := fill.SettlementEvidence.Normalize()
		fill.SettlementEvidence = &value
	}
	fill.MatchedAt = fill.MatchedAt.UTC()
	fill.VenueUpdatedAt = fill.VenueUpdatedAt.UTC()
	fill.ObservedAt = fill.ObservedAt.UTC()
	if fill.ConfirmedAt != nil {
		value := fill.ConfirmedAt.UTC()
		fill.ConfirmedAt = &value
	}
	if fill.AppliedAt != nil {
		value := fill.AppliedAt.UTC()
		fill.AppliedAt = &value
	}
	return fill
}

// Validate 校验当前模型的字段完整性和业务约束。
func (fill Fill) Validate() error {
	fill = fill.Normalize()
	if fill.Venue == "" || fill.VenueFillID == "" || fill.OrderID == "" || fill.VenueOrderID == "" {
		return fmt.Errorf("venue, venue_fill_id, order_id, and venue_order_id are required")
	}
	if fill.ExecutionAccountID == "" || fill.MarketID == "" || fill.TokenID == "" {
		return fmt.Errorf("execution_account_id, market_id, and token_id are required")
	}
	if fill.Side != SideBuy && fill.Side != SideSell {
		return fmt.Errorf("fill side must be BUY or SELL")
	}
	if fill.LiquidityRole != LiquidityRoleMaker && fill.LiquidityRole != LiquidityRoleTaker {
		return fmt.Errorf("fill liquidity_role must be MAKER or TAKER")
	}
	if !fill.Status.Valid() {
		return fmt.Errorf("unsupported fill status %q", fill.Status)
	}
	if sign, err := fill.Shares.Sign(); err != nil || sign <= 0 {
		return fmt.Errorf("fill shares must be positive")
	}
	if sign, err := fill.Price.Sign(); err != nil || sign <= 0 {
		return fmt.Errorf("fill price must be positive")
	}
	if comparison, err := fill.Price.Compare("1"); err != nil || comparison >= 0 {
		return fmt.Errorf("fill price must be below 1")
	}
	if fill.MatchedAt.IsZero() || fill.ObservedAt.IsZero() {
		return fmt.Errorf("matched_at and observed_at are required")
	}
	if fill.Status == FillStatusConfirmed && fill.ConfirmedAt == nil {
		return fmt.Errorf("confirmed fill requires confirmed_at")
	}
	return nil
}

// ValidateAccounting 校验成交金额、费用和净现金变化的精确会计恒等式。
func (fill Fill) ValidateAccounting() error {
	if err := fill.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(fill.Key) == "" {
		return fmt.Errorf("fill_key is required at the ledger boundary")
	}
	if strings.TrimSpace(fill.FeeSource) == "" {
		return fmt.Errorf("fee_source is required even when total_fee is zero")
	}
	for name, value := range map[string]Decimal{
		"gross_notional":       fill.GrossNotional,
		"platform_fee_rate":    fill.PlatformFeeRate,
		"fee_exponent":         fill.FeeExponent,
		"platform_fee":         fill.PlatformFee,
		"builder_fee_rate_bps": fill.BuilderFeeRateBPS,
		"builder_fee":          fill.BuilderFee,
		"total_fee":            fill.TotalFee,
	} {
		if sign, err := value.Sign(); err != nil || sign < 0 {
			return fmt.Errorf("%s must be a non-negative decimal", name)
		}
	}
	if _, err := fill.NetCashDelta.Sign(); err != nil {
		return fmt.Errorf("net_cash_delta must be a decimal")
	}
	exponent, _ := fill.FeeExponent.rat()
	if !exponent.IsInt() {
		return fmt.Errorf("fee_exponent must be an integer")
	}
	if fill.FeeSource == FeeSourcePolygonV2OrderFilled {
		if fill.SettlementEvidence == nil {
			return fmt.Errorf("Polygon V2 fill requires durable settlement evidence")
		}
		if err := fill.SettlementEvidence.ValidateAgainst(fill); err != nil {
			return fmt.Errorf("settlement evidence: %w", err)
		}
	} else if fill.SettlementEvidence != nil {
		return fmt.Errorf("settlement evidence is only supported for authoritative Polygon V2 fills")
	} else {
		shares, _ := fill.Shares.rat()
		price, _ := fill.Price.rat()
		gross, _ := fill.GrossNotional.rat()
		if new(big.Rat).Mul(shares, price).Cmp(gross) != 0 {
			return fmt.Errorf("gross_notional must equal shares multiplied by price")
		}
	}
	gross, _ := fill.GrossNotional.rat()
	platformFee, _ := fill.PlatformFee.rat()
	builderFee, _ := fill.BuilderFee.rat()
	totalFee, _ := fill.TotalFee.rat()
	if new(big.Rat).Add(platformFee, builderFee).Cmp(totalFee) != 0 {
		return fmt.Errorf("total_fee must equal platform_fee plus builder_fee")
	}
	wantNet := new(big.Rat)
	if fill.Side == SideBuy {
		wantNet.Neg(new(big.Rat).Add(gross, totalFee))
	} else {
		wantNet.Sub(gross, totalFee)
	}
	actualNet, _ := fill.NetCashDelta.rat()
	if wantNet.Cmp(actualNet) != 0 {
		return fmt.Errorf("net_cash_delta is inconsistent with side, gross_notional, and fees")
	}
	return nil
}

// Valid 判断当前枚举值是否受支持。
func (status FillStatus) Valid() bool {
	switch NormalizeFillStatus(status) {
	case FillStatusMatched, FillStatusMined, FillStatusConfirmed, FillStatusRetrying, FillStatusFailed:
		return true
	default:
		return false
	}
}

// Terminal 判断当前状态是否为终态。
func (status FillStatus) Terminal() bool {
	status = NormalizeFillStatus(status)
	return status == FillStatusConfirmed || status == FillStatusFailed
}

// CanTransitionTo 判断当前状态是否允许迁移到目标状态。
func (status FillStatus) CanTransitionTo(next FillStatus) bool {
	status = NormalizeFillStatus(status)
	next = NormalizeFillStatus(next)
	if status == next {
		return true
	}
	if status.Terminal() {
		return false
	}
	switch status {
	case FillStatusMatched:
		return next == FillStatusMined || next == FillStatusRetrying || next == FillStatusConfirmed || next == FillStatusFailed
	case FillStatusMined:
		return next == FillStatusRetrying || next == FillStatusConfirmed || next == FillStatusFailed
	case FillStatusRetrying:
		return next == FillStatusMatched || next == FillStatusMined || next == FillStatusConfirmed || next == FillStatusFailed
	default:
		return false
	}
}

// NormalizeFillStatus 将交易所成交状态规范化为领域枚举。
func NormalizeFillStatus(status FillStatus) FillStatus {
	value := strings.ToUpper(strings.TrimSpace(string(status)))
	value = strings.TrimPrefix(value, "TRADE_STATUS_")
	return FillStatus(value)
}

// FillApplication 表示后端使用的 FillApplication 类型。
type FillApplication struct {
	Fill           Fill            `json:"fill"`
	Order          Order           `json:"order"`
	Position       *Position       `json:"position,omitempty"`
	PositionEvents []PositionEvent `json:"position_events,omitempty"`
	Applied        bool            `json:"applied"`
	Duplicate      bool            `json:"duplicate"`
	OutboxEventID  string          `json:"outbox_event_id,omitempty"`
}
