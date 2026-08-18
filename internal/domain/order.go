package domain

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

// Side 表示后端使用的 Side 类型。
type Side string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

// OrderType 表示后端使用的 OrderType 类型。
type OrderType string

const (
	OrderTypeLimit  OrderType = "LIMIT"
	OrderTypeMarket OrderType = "MARKET"
)

// TimeInForce 表示后端使用的 TimeInForce 类型。
type TimeInForce string

const (
	TimeInForceGTC TimeInForce = "GTC"
	TimeInForceGTD TimeInForce = "GTD"
	TimeInForceIOC TimeInForce = "IOC"
	TimeInForceFOK TimeInForce = "FOK"
	TimeInForceFAK TimeInForce = "FAK"
)

// OrderStatus 表示后端使用的 OrderStatus 类型。
type OrderStatus string

const (
	OrderStatusReceived        OrderStatus = "RECEIVED"
	OrderStatusValidating      OrderStatus = "VALIDATING"
	OrderStatusReserved        OrderStatus = "RESERVED"
	OrderStatusSubmitting      OrderStatus = "SUBMITTING"
	OrderStatusAcknowledged    OrderStatus = "ACKNOWLEDGED"
	OrderStatusLive            OrderStatus = "LIVE"
	OrderStatusPartiallyFilled OrderStatus = "PARTIALLY_FILLED"
	OrderStatusFilled          OrderStatus = "FILLED"
	OrderStatusRejected        OrderStatus = "REJECTED"
	OrderStatusUnknown         OrderStatus = "UNKNOWN"
	OrderStatusCancelPending   OrderStatus = "CANCEL_PENDING"
	OrderStatusCancelled       OrderStatus = "CANCELLED"
	OrderStatusReconciling     OrderStatus = "RECONCILING"
	OrderStatusManualReview    OrderStatus = "MANUAL_REVIEW"

	// 以下废弃别名仅用于保持本地开发适配器的源码兼容；新代码必须使用上面的完整生命周期状态。
	OrderStatusAccepted  = OrderStatusReceived
	OrderStatusSubmitted = OrderStatusSubmitting
	OrderStatusOpen      = OrderStatusLive
	OrderStatusCanceled  = OrderStatusCancelled
	OrderStatusFailed    = OrderStatusManualReview
)

// OrderIntent 表示策略与执行之间的完整边界，不包含概率、Edge 阈值或仓位计算公式等策略决策。
type OrderIntent struct {
	ModelID            string            `json:"model_id"`
	StrategyID         string            `json:"strategy_id"`
	ExecutionAccountID string            `json:"execution_account_id"`
	SignalID           string            `json:"signal_id"`
	ClientOrderID      string            `json:"client_order_id"`
	Venue              string            `json:"venue"`
	MarketID           string            `json:"market_id"`
	ConditionID        string            `json:"condition_id,omitempty"`
	OutcomeIndex       *int              `json:"outcome_index,omitempty"`
	OutcomeName        string            `json:"outcome_name,omitempty"`
	TokenID            string            `json:"token_id"`
	TargetLotID        string            `json:"target_lot_id,omitempty"`
	ExpectedNegRisk    *bool             `json:"expected_neg_risk,omitempty"`
	MarketSnapshotAt   *time.Time        `json:"market_snapshot_at,omitempty"`
	SignalAt           *time.Time        `json:"signal_at,omitempty"`
	Side               Side              `json:"side"`
	Type               OrderType         `json:"type"`
	Price              Decimal           `json:"price,omitempty"`
	WorstPrice         Decimal           `json:"worst_price,omitempty"`
	Size               Decimal           `json:"size"`
	TimeInForce        TimeInForce       `json:"time_in_force"`
	ExpiresAt          *time.Time        `json:"expires_at,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
}

// Normalize 规范化当前模型的文本、时间和可变字段。
func (intent OrderIntent) Normalize() OrderIntent {
	intent.ModelID = strings.TrimSpace(intent.ModelID)
	intent.StrategyID = strings.TrimSpace(intent.StrategyID)
	intent.ExecutionAccountID = strings.TrimSpace(intent.ExecutionAccountID)
	intent.SignalID = strings.TrimSpace(intent.SignalID)
	intent.ClientOrderID = strings.TrimSpace(intent.ClientOrderID)
	intent.Venue = strings.ToLower(strings.TrimSpace(intent.Venue))
	intent.MarketID = strings.TrimSpace(intent.MarketID)
	intent.ConditionID = strings.TrimSpace(intent.ConditionID)
	intent.OutcomeName = strings.TrimSpace(intent.OutcomeName)
	intent.TokenID = strings.TrimSpace(intent.TokenID)
	intent.TargetLotID = strings.TrimSpace(intent.TargetLotID)
	intent.Side = Side(strings.ToUpper(strings.TrimSpace(string(intent.Side))))
	intent.Type = OrderType(strings.ToUpper(strings.TrimSpace(string(intent.Type))))
	intent.Price = Decimal(strings.TrimSpace(intent.Price.String()))
	intent.WorstPrice = Decimal(strings.TrimSpace(intent.WorstPrice.String()))
	intent.Size = Decimal(strings.TrimSpace(intent.Size.String()))
	intent.TimeInForce = TimeInForce(strings.ToUpper(strings.TrimSpace(string(intent.TimeInForce))))
	if intent.ExpiresAt != nil {
		value := intent.ExpiresAt.UTC()
		intent.ExpiresAt = &value
	}
	if intent.MarketSnapshotAt != nil {
		value := intent.MarketSnapshotAt.UTC()
		intent.MarketSnapshotAt = &value
	}
	if intent.SignalAt != nil {
		value := intent.SignalAt.UTC()
		intent.SignalAt = &value
	}
	intent.Metadata = maps.Clone(intent.Metadata)
	if intent.OutcomeIndex != nil {
		value := *intent.OutcomeIndex
		intent.OutcomeIndex = &value
	}
	if intent.ExpectedNegRisk != nil {
		value := *intent.ExpectedNegRisk
		intent.ExpectedNegRisk = &value
	}
	return intent
}

// Validate 校验当前模型的字段完整性和业务约束。
func (intent OrderIntent) Validate() error {
	intent = intent.Normalize()
	if intent.ModelID == "" || intent.StrategyID == "" || intent.ExecutionAccountID == "" || intent.SignalID == "" || intent.ClientOrderID == "" {
		return fmt.Errorf("model_id, strategy_id, execution_account_id, signal_id, and client_order_id are required")
	}
	if intent.Venue == "" || intent.MarketID == "" || intent.TokenID == "" {
		return fmt.Errorf("venue, market_id, and token_id are required")
	}
	if intent.OutcomeIndex != nil && *intent.OutcomeIndex != 0 && *intent.OutcomeIndex != 1 {
		return fmt.Errorf("outcome_index must be 0 or 1")
	}
	if intent.MarketSnapshotAt != nil && intent.MarketSnapshotAt.IsZero() {
		return fmt.Errorf("market_snapshot_at must not be zero")
	}
	if intent.SignalAt != nil && intent.SignalAt.IsZero() {
		return fmt.Errorf("signal_at must not be zero")
	}
	if !intent.WorstPrice.IsEmpty() {
		if sign, err := intent.WorstPrice.Sign(); err != nil || sign <= 0 {
			return fmt.Errorf("worst_price must be a positive decimal string")
		}
	}
	if intent.Side != SideBuy && intent.Side != SideSell {
		return fmt.Errorf("unsupported side %q", intent.Side)
	}
	if intent.Side == SideBuy && intent.TargetLotID != "" {
		return fmt.Errorf("BUY order must not contain target_lot_id")
	}
	if intent.Side == SideSell && intent.TargetLotID == "" {
		return fmt.Errorf("SELL order requires target_lot_id")
	}
	if intent.Type != OrderTypeLimit && intent.Type != OrderTypeMarket {
		return fmt.Errorf("unsupported order type %q", intent.Type)
	}
	if sign, err := intent.Size.Sign(); err != nil || sign <= 0 {
		return fmt.Errorf("size must be a positive decimal string")
	}
	if intent.Type == OrderTypeLimit {
		if sign, err := intent.Price.Sign(); err != nil || sign <= 0 {
			return fmt.Errorf("limit price must be a positive decimal string")
		}
	} else if !intent.Price.IsEmpty() {
		return fmt.Errorf("market order must not include price")
	}
	if intent.TimeInForce != TimeInForceGTC && intent.TimeInForce != TimeInForceGTD &&
		intent.TimeInForce != TimeInForceIOC && intent.TimeInForce != TimeInForceFOK &&
		intent.TimeInForce != TimeInForceFAK {
		return fmt.Errorf("unsupported time_in_force %q", intent.TimeInForce)
	}
	if intent.Type == OrderTypeMarket && (intent.TimeInForce == TimeInForceGTC || intent.TimeInForce == TimeInForceGTD) {
		return fmt.Errorf("market order does not support %s", intent.TimeInForce)
	}
	if intent.TimeInForce == TimeInForceGTD && intent.ExpiresAt == nil {
		return fmt.Errorf("GTD order requires expires_at")
	}
	for key := range intent.Metadata {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("metadata keys must not be empty")
		}
	}
	return nil
}

// Equivalent 判断两个订单意图在规范化后是否具有相同业务语义。
func (intent OrderIntent) Equivalent(other OrderIntent) bool {
	left := intent.Normalize()
	right := other.Normalize()
	if left.ModelID != right.ModelID || left.StrategyID != right.StrategyID || left.ExecutionAccountID != right.ExecutionAccountID || left.SignalID != right.SignalID ||
		left.ClientOrderID != right.ClientOrderID || left.Venue != right.Venue ||
		left.MarketID != right.MarketID || left.ConditionID != right.ConditionID ||
		!sameInt(left.OutcomeIndex, right.OutcomeIndex) || left.OutcomeName != right.OutcomeName ||
		left.TokenID != right.TokenID || left.TargetLotID != right.TargetLotID || !sameBool(left.ExpectedNegRisk, right.ExpectedNegRisk) ||
		!sameTime(left.MarketSnapshotAt, right.MarketSnapshotAt) || !sameTime(left.SignalAt, right.SignalAt) || left.Side != right.Side || left.Type != right.Type ||
		!left.Price.Equal(right.Price) || !left.WorstPrice.Equal(right.WorstPrice) || !left.Size.Equal(right.Size) ||
		left.TimeInForce != right.TimeInForce || !sameTime(left.ExpiresAt, right.ExpiresAt) {
		return false
	}
	return maps.Equal(left.Metadata, right.Metadata)
}

// sameInt 判断 Int 是否相等。
func sameInt(left, right *int) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

// sameBool 判断 Bool 是否相等。
func sameBool(left, right *bool) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

// sameTime 判断 Time 是否相等。
func sameTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

// Order 表示后端使用的 Order 类型。
type Order struct {
	ID                  string            `json:"order_id"`
	Intent              OrderIntent       `json:"intent"`
	MarketValidation    *MarketValidation `json:"market_validation,omitempty"`
	VenueOrderID        string            `json:"venue_order_id,omitempty"`
	Status              OrderStatus       `json:"status"`
	FilledSize          Decimal           `json:"filled_size"`
	FilledNotional      Decimal           `json:"filled_notional"`
	TotalFees           Decimal           `json:"total_fees"`
	AverageFillPrice    Decimal           `json:"average_fill_price,omitempty"`
	FailureCode         string            `json:"failure_code,omitempty"`
	FailureReason       string            `json:"failure_reason,omitempty"`
	CreatedAt           time.Time         `json:"created_at"`
	UpdatedAt           time.Time         `json:"updated_at"`
	VenueLastObservedAt *time.Time        `json:"venue_last_observed_at,omitempty"`
	Revision            int64             `json:"revision"`
}

// Terminal 判断当前状态是否为终态。
func (order Order) Terminal() bool {
	return slices.Contains([]OrderStatus{
		OrderStatusFilled,
		OrderStatusCancelled,
		OrderStatusRejected,
		OrderStatusManualReview,
	}, order.Status)
}

// CanTransitionTo 判断当前状态是否允许迁移到目标状态。
func (order Order) CanTransitionTo(next OrderStatus) bool {
	if order.Status == OrderStatusCancelled {
		// A confirmed trade can become visible after a successful cancel
		// response. Partial late fills remain CANCELLED with a higher cumulative
		// fill; a full late fill corrects the terminal state to FILLED.
		return next == OrderStatusCancelled || next == OrderStatusFilled
	}
	if order.Status == OrderStatusManualReview {
		// Operators may discover a real confirmed fill after automated
		// reconciliation gave up. Keep partial orders in MANUAL_REVIEW and
		// allow a complete fill to correct the terminal result.
		return next == OrderStatusManualReview || next == OrderStatusFilled
	}
	if order.Terminal() {
		return next == order.Status
	}
	if order.Status == next {
		return true
	}
	switch order.Status {
	case OrderStatusReceived:
		return next == OrderStatusValidating
	case OrderStatusValidating:
		return next == OrderStatusReserved || next == OrderStatusRejected
	case OrderStatusReserved:
		return next == OrderStatusSubmitting || next == OrderStatusRejected
	case OrderStatusSubmitting:
		return slices.Contains([]OrderStatus{
			OrderStatusAcknowledged,
			OrderStatusUnknown,
			OrderStatusRejected,
		}, next)
	case OrderStatusAcknowledged:
		return slices.Contains([]OrderStatus{
			OrderStatusLive,
			OrderStatusPartiallyFilled,
			OrderStatusFilled,
			OrderStatusCancelled,
			OrderStatusCancelPending,
			OrderStatusUnknown,
			OrderStatusReconciling,
		}, next)
	case OrderStatusLive:
		return slices.Contains([]OrderStatus{
			OrderStatusPartiallyFilled,
			OrderStatusFilled,
			OrderStatusCancelled,
			OrderStatusCancelPending,
			OrderStatusUnknown,
			OrderStatusReconciling,
		}, next)
	case OrderStatusPartiallyFilled:
		return slices.Contains([]OrderStatus{
			OrderStatusFilled,
			// IOC/FAK may atomically fill part of an order and cancel its
			// remainder without a client-initiated CANCEL_PENDING state.
			OrderStatusCancelled,
			OrderStatusCancelPending,
			OrderStatusUnknown,
			OrderStatusReconciling,
		}, next)
	case OrderStatusCancelPending:
		// A fill and a cancel may cross at the venue. LIVE means the cancel was
		// definitively not accepted; UNKNOWN means the request outcome is not
		// known and therefore must not be blindly retried.
		return slices.Contains([]OrderStatus{
			OrderStatusCancelled,
			OrderStatusLive,
			OrderStatusPartiallyFilled,
			OrderStatusFilled,
			OrderStatusUnknown,
			OrderStatusReconciling,
		}, next)
	case OrderStatusUnknown:
		return next == OrderStatusReconciling
	case OrderStatusReconciling:
		// CANCELLED and PARTIALLY_FILLED are necessary additions to the main
		// diagram: an ambiguous cancel can resolve to either state.
		return slices.Contains([]OrderStatus{
			OrderStatusAcknowledged,
			OrderStatusLive,
			OrderStatusPartiallyFilled,
			OrderStatusFilled,
			OrderStatusCancelled,
			OrderStatusRejected,
			OrderStatusUnknown,
			OrderStatusManualReview,
		}, next)
	default:
		return false
	}
}

// CanApplyVenueStatus 判断订单是否可应用映射后的交易所状态。
func (order Order) CanApplyVenueStatus(next OrderStatus) bool {
	return order.CanTransitionTo(next)
}

// CloneOrder 深复制订单中的指针和可变字段。
func CloneOrder(order Order) Order {
	order.Intent = order.Intent.Normalize()
	if order.VenueLastObservedAt != nil {
		value := *order.VenueLastObservedAt
		order.VenueLastObservedAt = &value
	}
	if order.MarketValidation != nil {
		validation := *order.MarketValidation
		order.MarketValidation = &validation
	}
	return order
}
