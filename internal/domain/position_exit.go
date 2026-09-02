package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	PositionExitInputSchemaVersion  = "trading.position_exit_input.v2"
	PositionExitOutputSchemaVersion = "trading.position_exit_output.v2"
)

// PositionExitMarketStatus 表示后端使用的 PositionExitMarketStatus 类型。
type PositionExitMarketStatus string

const (
	PositionExitMarketOpen               PositionExitMarketStatus = "OPEN"
	PositionExitMarketPaused             PositionExitMarketStatus = "PAUSED"
	PositionExitMarketInactive           PositionExitMarketStatus = "INACTIVE"
	PositionExitMarketNotAcceptingOrders PositionExitMarketStatus = "NOT_ACCEPTING_ORDERS"
	PositionExitMarketClosed             PositionExitMarketStatus = "CLOSED"
	PositionExitMarketResolved           PositionExitMarketStatus = "RESOLVED"
)

// PositionExitTrade 表示后端使用的 PositionExitTrade 类型。
type PositionExitTrade struct {
	LotID              string    `json:"lot_id"`
	VenueTradeID       string    `json:"venue_trade_id"`
	OpeningOrderID     string    `json:"opening_order_id"`
	MarketID           string    `json:"market_id"`
	ConditionID        string    `json:"condition_id"`
	OutcomeIndex       int       `json:"outcome_index"`
	OutcomeName        string    `json:"outcome_name"`
	TokenID            string    `json:"token_id"`
	NegRisk            bool      `json:"neg_risk"`
	EnteredAt          time.Time `json:"entered_at"`
	OriginalShares     Decimal   `json:"original_shares"`
	RemainingShares    Decimal   `json:"remaining_shares"`
	AvailableShares    Decimal   `json:"available_shares"`
	ReservedShares     Decimal   `json:"reserved_shares"`
	EntryPrice         Decimal   `json:"entry_price"`
	RemainingCost      Decimal   `json:"remaining_cost"`
	OriginModelID      string    `json:"-"`
	ModelID            string    `json:"-"`
	StrategyID         string    `json:"-"`
	ExecutionAccountID string    `json:"-"`
}

// Validate 校验当前模型的字段完整性和业务约束。
func (trade PositionExitTrade) Validate(decisionAt time.Time) error {
	if strings.TrimSpace(trade.LotID) == "" || strings.TrimSpace(trade.VenueTradeID) == "" ||
		strings.TrimSpace(trade.OpeningOrderID) == "" || strings.TrimSpace(trade.MarketID) == "" ||
		strings.TrimSpace(trade.ConditionID) == "" || strings.TrimSpace(trade.OutcomeName) == "" ||
		strings.TrimSpace(trade.TokenID) == "" {
		return fmt.Errorf("position exit trade identity is required")
	}
	if trade.OutcomeIndex != 0 && trade.OutcomeIndex != 1 {
		return fmt.Errorf("position exit trade outcome_index must be 0 or 1")
	}
	if trade.EnteredAt.IsZero() || trade.EnteredAt.After(decisionAt.UTC()) {
		return fmt.Errorf("position exit trade entered_at is invalid")
	}
	for name, value := range map[string]Decimal{
		"original_shares": trade.OriginalShares, "remaining_shares": trade.RemainingShares,
		"entry_price": trade.EntryPrice, "remaining_cost": trade.RemainingCost,
	} {
		if sign, err := value.Sign(); err != nil || sign <= 0 {
			return fmt.Errorf("position exit trade %s must be positive", name)
		}
	}
	for name, value := range map[string]Decimal{
		"available_shares": trade.AvailableShares, "reserved_shares": trade.ReservedShares,
	} {
		if sign, err := value.Sign(); err != nil || sign < 0 {
			return fmt.Errorf("position exit trade %s must be non-negative", name)
		}
	}
	if comparison, err := trade.EntryPrice.Compare("1"); err != nil || comparison > 0 {
		return fmt.Errorf("position exit trade entry_price must not exceed one")
	}
	if comparison, err := trade.RemainingShares.Compare(trade.OriginalShares); err != nil || comparison > 0 {
		return fmt.Errorf("position exit trade remaining_shares exceeds original_shares")
	}
	availableAndReserved, err := addDecimals(trade.AvailableShares, trade.ReservedShares)
	if err != nil || !availableAndReserved.Equal(trade.RemainingShares) {
		return fmt.Errorf("position exit trade shares do not satisfy remaining=available+reserved")
	}
	return nil
}

// addDecimals 累加或记录 Decimals。
func addDecimals(left, right Decimal) (Decimal, error) {
	leftRat, err := left.Multiply("1")
	if err != nil {
		return "", err
	}
	rightRat, err := right.Multiply("1")
	if err != nil {
		return "", err
	}
	value := leftRat.Add(leftRat, rightRat)
	return Decimal(value.FloatString(max(left.Scale(), right.Scale()))), nil
}

// PositionExitMarketData 表示后端使用的 PositionExitMarketData 类型。
type PositionExitMarketData struct {
	MarketID         string                   `json:"market_id"`
	ConditionID      string                   `json:"condition_id"`
	OutcomeIndex     int                      `json:"outcome_index"`
	TokenID          string                   `json:"token_id"`
	MarketStatus     PositionExitMarketStatus `json:"market_status"`
	ClosedAt         *time.Time               `json:"closed_at"`
	MarketObservedAt time.Time                `json:"market_observed_at"`
	OrderBook        OrderBookSnapshot        `json:"orderbook"`
	MidPriceHistory  MidPriceHistory          `json:"mid_price_history"`
}

// Validate 校验当前模型的字段完整性和业务约束。
func (marketData PositionExitMarketData) Validate() error {
	if strings.TrimSpace(marketData.MarketID) == "" || strings.TrimSpace(marketData.ConditionID) == "" || strings.TrimSpace(marketData.TokenID) == "" {
		return fmt.Errorf("position exit market data identity is required")
	}
	if marketData.OutcomeIndex != 0 && marketData.OutcomeIndex != 1 {
		return fmt.Errorf("position exit market data outcome_index must be 0 or 1")
	}
	switch marketData.MarketStatus {
	case PositionExitMarketOpen, PositionExitMarketPaused, PositionExitMarketInactive,
		PositionExitMarketNotAcceptingOrders:
		if marketData.ClosedAt != nil {
			return fmt.Errorf("open/non-terminal position exit market must have closed_at=null")
		}
	case PositionExitMarketClosed, PositionExitMarketResolved:
		if marketData.ClosedAt == nil || marketData.ClosedAt.IsZero() {
			return fmt.Errorf("closed/resolved position exit market requires closed_at")
		}
	default:
		return fmt.Errorf("unsupported position exit market_status %q", marketData.MarketStatus)
	}
	if marketData.MarketObservedAt.IsZero() {
		return fmt.Errorf("position exit market_observed_at is required")
	}
	if marketData.ClosedAt != nil && marketData.ClosedAt.After(marketData.MarketObservedAt) {
		return fmt.Errorf("position exit closed_at must not be after market_observed_at")
	}
	if marketData.OrderBook.MarketID != marketData.MarketID || marketData.OrderBook.ConditionID != marketData.ConditionID ||
		marketData.OrderBook.OutcomeIndex != marketData.OutcomeIndex || marketData.OrderBook.TokenID != marketData.TokenID {
		return fmt.Errorf("position exit orderbook identity mismatch")
	}
	if marketData.MidPriceHistory.MarketID != marketData.MarketID || marketData.MidPriceHistory.ConditionID != marketData.ConditionID ||
		marketData.MidPriceHistory.OutcomeIndex != marketData.OutcomeIndex || marketData.MidPriceHistory.TokenID != marketData.TokenID {
		return fmt.Errorf("position exit mid-price history identity mismatch")
	}
	if marketData.OrderBook.DepthLimit != StrategyOrderBookDepth {
		return fmt.Errorf("position exit orderbook depth_limit must be %d", StrategyOrderBookDepth)
	}
	if marketData.OrderBook.Status == OrderBookStatusOK &&
		(marketData.OrderBook.BestBid.IsEmpty() || marketData.OrderBook.BestAsk.IsEmpty()) {
		return fmt.Errorf("position exit OK orderbook requires explicit best_bid and best_ask")
	}
	if err := marketData.OrderBook.Validate(); err != nil {
		return err
	}
	return marketData.MidPriceHistory.Validate()
}

// PositionExitExecutionConstraints 表示后端使用的 PositionExitExecutionConstraints 类型。
type PositionExitExecutionConstraints struct {
	SellSizeUnit          string        `json:"sell_size_unit"`
	SellSizeDecimalPlaces int           `json:"sell_size_decimal_places"`
	AllowedTimeInForce    []TimeInForce `json:"allowed_time_in_force"`
	PriceProtectionPolicy string        `json:"price_protection_policy"`
}

// DefaultPositionExitExecutionConstraints 返回持仓退出协议的默认执行约束。
func DefaultPositionExitExecutionConstraints() PositionExitExecutionConstraints {
	return PositionExitExecutionConstraints{
		SellSizeUnit: "SHARES", SellSizeDecimalPlaces: 2,
		AllowedTimeInForce:    []TimeInForce{TimeInForceFOK},
		PriceProtectionPolicy: "PYTHON_SUPPLIED_EXACT_BEST_BID",
	}
}

// PositionExitRequest 表示后端使用的 PositionExitRequest 类型。
type PositionExitRequest struct {
	SchemaVersion        string                           `json:"schema_version"`
	CycleID              string                           `json:"cycle_id"`
	InputID              string                           `json:"input_id"`
	DecisionAt           time.Time                        `json:"decision_at"`
	GeneratedAt          time.Time                        `json:"generated_at"`
	Context              StrategyExecutionContext         `json:"context"`
	PredictionSnapshotID string                           `json:"prediction_snapshot_id"`
	PredictionScope      PredictionScope                  `json:"prediction_scope"`
	Predictions          []Prediction                     `json:"predictions"`
	Trades               []PositionExitTrade              `json:"trades"`
	MarketData           []PositionExitMarketData         `json:"market_data"`
	ExecutionConstraints PositionExitExecutionConstraints `json:"execution_constraints"`
}

// ComputePositionExitInputID 对决策相关的冻结退出输入计算稳定内容哈希。
func ComputePositionExitInputID(request PositionExitRequest) (string, error) {
	identity := struct {
		SchemaVersion        string                           `json:"schema_version"`
		CycleID              string                           `json:"cycle_id"`
		DecisionAt           time.Time                        `json:"decision_at"`
		Context              StrategyExecutionContext         `json:"context"`
		PredictionSnapshotID string                           `json:"prediction_snapshot_id"`
		PredictionScope      PredictionScope                  `json:"prediction_scope"`
		Predictions          []Prediction                     `json:"predictions"`
		Trades               []PositionExitTrade              `json:"trades"`
		MarketData           []PositionExitMarketData         `json:"market_data"`
		ExecutionConstraints PositionExitExecutionConstraints `json:"execution_constraints"`
	}{
		SchemaVersion: request.SchemaVersion, CycleID: request.CycleID, DecisionAt: request.DecisionAt,
		Context: request.Context.Normalize(), PredictionSnapshotID: request.PredictionSnapshotID,
		PredictionScope: request.PredictionScope, Predictions: request.Predictions,
		Trades: request.Trades, MarketData: request.MarketData,
		ExecutionConstraints: request.ExecutionConstraints,
	}
	payload, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("encode position exit input identity: %w", err)
	}
	digest := sha256.Sum256(payload)
	return "exit-input-" + hex.EncodeToString(digest[:]), nil
}

// PositionExitAction 表示后端使用的 PositionExitAction 类型。
type PositionExitAction string

const (
	PositionExitActionHold PositionExitAction = "HOLD"
	PositionExitActionSell PositionExitAction = "SELL"
)

// PositionExitReasonCode 表示后端使用的 PositionExitReasonCode 类型。
type PositionExitReasonCode string

const (
	PositionExitReasonHoldNotDue        PositionExitReasonCode = "HOLD_NOT_DUE"
	PositionExitReasonHoldSignal        PositionExitReasonCode = "HOLD_SIGNAL"
	PositionExitReasonTimeExit48H       PositionExitReasonCode = "TIME_EXIT_48H"
	PositionExitReasonTakeProfit        PositionExitReasonCode = "TAKE_PROFIT"
	PositionExitReasonStopLoss          PositionExitReasonCode = "STOP_LOSS"
	PositionExitReasonLiquidityTooLow   PositionExitReasonCode = "LIQUIDITY_TOO_LOW"
	PositionExitReasonPriceOutOfRange   PositionExitReasonCode = "PRICE_OUT_OF_RANGE"
	PositionExitReasonStaleData         PositionExitReasonCode = "STALE_DATA"
	PositionExitReasonInvalidBook       PositionExitReasonCode = "INVALID_BOOK"
	PositionExitReasonMarketNotTradable PositionExitReasonCode = "MARKET_NOT_TRADABLE"
)

// PositionExitEvidence 表示后端使用的 PositionExitEvidence 类型。
type PositionExitEvidence struct {
	HeldSeconds int64             `json:"held_seconds"`
	BestBid     Decimal           `json:"best_bid,omitempty"`
	Metrics     map[string]string `json:"metrics,omitempty"`
}

// PositionExitEvaluation 表示后端使用的 PositionExitEvaluation 类型。
type PositionExitEvaluation struct {
	DecisionID string                 `json:"decision_id"`
	LotID      string                 `json:"lot_id"`
	Action     PositionExitAction     `json:"action"`
	ReasonCode PositionExitReasonCode `json:"reason_code"`
	Reason     string                 `json:"reason,omitempty"`
	Evidence   PositionExitEvidence   `json:"evidence"`
	Order      *StrategyOrderParams   `json:"order,omitempty"`
}

// ValidateEvidence 校验策略证据与冻结输入和决策时点保持一致。
func (evaluation PositionExitEvaluation) ValidateEvidence(trade PositionExitTrade, decisionAt time.Time) error {
	expectedHeld := int64(decisionAt.UTC().Sub(trade.EnteredAt.UTC()) / time.Second)
	if evaluation.Evidence.HeldSeconds != expectedHeld {
		return fmt.Errorf("held_seconds does not match entered_at and decision_at")
	}
	for key, value := range evaluation.Evidence.Metrics {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("exit evidence metric key is empty")
		}
		if _, err := ParseDecimal(value); err != nil {
			return fmt.Errorf("exit evidence metric %q must be a decimal string", key)
		}
	}
	if !evaluation.Evidence.BestBid.IsEmpty() {
		if sign, err := evaluation.Evidence.BestBid.Sign(); err != nil || sign <= 0 {
			return fmt.Errorf("exit evidence best_bid must be positive")
		}
	}
	return nil
}

// PositionExitResponse 表示后端使用的 PositionExitResponse 类型。
type PositionExitResponse struct {
	SchemaVersion string                   `json:"schema_version"`
	CycleID       string                   `json:"cycle_id"`
	InputID       string                   `json:"input_id"`
	Context       StrategyExecutionContext `json:"context"`
	DecidedAt     time.Time                `json:"decided_at"`
	Evaluations   []PositionExitEvaluation `json:"evaluations"`
}

// ValidPositionExitReason 判断持仓退出动作与原因码的组合是否合法。
func ValidPositionExitReason(action PositionExitAction, reason PositionExitReasonCode) bool {
	switch action {
	case PositionExitActionHold:
		switch reason {
		case PositionExitReasonHoldNotDue, PositionExitReasonHoldSignal, PositionExitReasonLiquidityTooLow,
			PositionExitReasonPriceOutOfRange, PositionExitReasonStaleData, PositionExitReasonInvalidBook,
			PositionExitReasonMarketNotTradable:
			return true
		}
	case PositionExitActionSell:
		switch reason {
		case PositionExitReasonTimeExit48H, PositionExitReasonTakeProfit, PositionExitReasonStopLoss:
			return true
		}
	}
	return false
}
