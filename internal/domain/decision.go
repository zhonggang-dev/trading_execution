package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	PredictionSnapshotSchemaVersion = "prediction.live_snapshot.v1"
	StrategyInputSchemaVersion      = "trading.strategy_input.v4"
	StrategyOutputSchemaVersion     = "trading.strategy_output.v4"
	StrategyOrderBookDepth          = 15
	StrategyMinimumHistorySeconds   = 2 * 60 * 60
)

// PredictionScope 表示后端使用的 PredictionScope 类型。
type PredictionScope string

const PredictionScopeAllEffective PredictionScope = "ALL_EFFECTIVE_AT_DECISION_AT"

// PredictionOutcome 表示后端使用的 PredictionOutcome 类型。
type PredictionOutcome struct {
	Index       int     `json:"index"`
	Name        string  `json:"name"`
	TokenID     string  `json:"token_id"`
	Probability float64 `json:"probability"`
}

// PredictionModel 表示后端使用的 PredictionModel 类型。
type PredictionModel struct {
	Name             string `json:"name"`
	PredictorVersion string `json:"predictor_version,omitempty"`
	PromptVersion    string `json:"prompt_version,omitempty"`
}

// Prediction 表示单个模型生产者的预测结果，同一 condition 的多条预测在进入策略请求前始终保持独立。
type Prediction struct {
	PredictionID   string              `json:"prediction_id"`
	SourceJobID    string              `json:"source_job_id"`
	SandboxID      string              `json:"sandbox_id"`
	MarketID       string              `json:"market_id"`
	ConditionID    string              `json:"condition_id"`
	EventID        string              `json:"event_id,omitempty"`
	Question       string              `json:"question"`
	EventSlug      string              `json:"event_slug,omitempty"`
	MarketSlug     string              `json:"market_slug,omitempty"`
	Domains        []string            `json:"domains"`
	EndAt          *time.Time          `json:"end_at,omitempty"`
	NegRisk        bool                `json:"neg_risk"`
	Outcomes       []PredictionOutcome `json:"outcomes"`
	PredictionAsOf time.Time           `json:"prediction_as_of"`
	CompletedAt    time.Time           `json:"completed_at"`
	AvailableAt    time.Time           `json:"available_at"`
	Model          PredictionModel     `json:"model"`
}

// PredictionSnapshot 表示后端使用的 PredictionSnapshot 类型。
type PredictionSnapshot struct {
	SchemaVersion  string       `json:"schema_version"`
	SnapshotID     string       `json:"snapshot_id"`
	DecisionAt     time.Time    `json:"decision_at"`
	CompletedAfter time.Time    `json:"completed_after"`
	GeneratedAt    time.Time    `json:"generated_at"`
	Predictions    []Prediction `json:"predictions"`
}

// Validate 校验当前模型的字段完整性和业务约束。
func (snapshot PredictionSnapshot) Validate(expectedDecisionAt time.Time) error {
	if snapshot.SchemaVersion != PredictionSnapshotSchemaVersion || strings.TrimSpace(snapshot.SnapshotID) == "" {
		return fmt.Errorf("prediction snapshot schema version and id are required")
	}
	if !snapshot.DecisionAt.Equal(expectedDecisionAt.UTC()) {
		return fmt.Errorf("prediction snapshot decision_at does not match requested boundary")
	}
	seenPredictions := make(map[string]struct{}, len(snapshot.Predictions))
	for index, prediction := range snapshot.Predictions {
		if err := prediction.Validate(snapshot.DecisionAt); err != nil {
			return fmt.Errorf("prediction %d: %w", index, err)
		}
		if _, exists := seenPredictions[prediction.PredictionID]; exists {
			return fmt.Errorf("prediction snapshot contains duplicate prediction_id %q", prediction.PredictionID)
		}
		seenPredictions[prediction.PredictionID] = struct{}{}
	}
	return nil
}

// Validate 校验当前模型的字段完整性和业务约束。
func (prediction Prediction) Validate(decisionAt time.Time) error {
	if strings.TrimSpace(prediction.PredictionID) == "" || strings.TrimSpace(prediction.MarketID) == "" || strings.TrimSpace(prediction.ConditionID) == "" {
		return fmt.Errorf("prediction, market, and condition identifiers are required")
	}
	if strings.TrimSpace(prediction.Model.Name) == "" {
		return fmt.Errorf("prediction model name is required")
	}
	if len(prediction.Outcomes) != 2 {
		return fmt.Errorf("exactly two prediction outcomes are required")
	}
	probabilitySum := 0.0
	seenTokens := make(map[string]struct{}, len(prediction.Outcomes))
	for index, outcome := range prediction.Outcomes {
		if outcome.Index != index || strings.TrimSpace(outcome.Name) == "" || strings.TrimSpace(outcome.TokenID) == "" {
			return fmt.Errorf("outcome %d identity is invalid", index)
		}
		if math.IsNaN(outcome.Probability) || math.IsInf(outcome.Probability, 0) || outcome.Probability < 0 || outcome.Probability > 1 {
			return fmt.Errorf("outcome %d probability is outside [0,1]", index)
		}
		if _, exists := seenTokens[outcome.TokenID]; exists {
			return fmt.Errorf("prediction contains duplicate token ids")
		}
		seenTokens[outcome.TokenID] = struct{}{}
		probabilitySum += outcome.Probability
	}
	if probabilitySum < 0.999999 || probabilitySum > 1.000001 {
		return fmt.Errorf("prediction outcome probabilities must sum to one")
	}
	decisionAt = decisionAt.UTC()
	if prediction.PredictionAsOf.IsZero() || prediction.CompletedAt.IsZero() || prediction.AvailableAt.IsZero() ||
		prediction.PredictionAsOf.After(decisionAt) || prediction.CompletedAt.After(decisionAt) || prediction.AvailableAt.After(decisionAt) {
		return fmt.Errorf("prediction is not point-in-time available at the decision boundary")
	}
	return nil
}

// PriceLevel 表示后端使用的 PriceLevel 类型。
type PriceLevel struct {
	Price Decimal `json:"price"`
	Size  Decimal `json:"size"`
}

// OrderBookStatus 表示后端使用的 OrderBookStatus 类型。
type OrderBookStatus string

const (
	OrderBookStatusOK      OrderBookStatus = "OK"
	OrderBookStatusEmpty   OrderBookStatus = "EMPTY"
	OrderBookStatusMissing OrderBookStatus = "MISSING"
	OrderBookStatusError   OrderBookStatus = "ERROR"
)

// OrderBookSnapshot 表示标准化的前 N 档市场数据观察结果，供策略使用而不是执行风控读取的最新报价。
type OrderBookSnapshot struct {
	MarketID     string          `json:"market_id"`
	ConditionID  string          `json:"condition_id"`
	OutcomeIndex int             `json:"outcome_index"`
	TokenID      string          `json:"token_id"`
	Status       OrderBookStatus `json:"status"`
	SourceAt     time.Time       `json:"source_at,omitempty"`
	ObservedAt   time.Time       `json:"observed_at"`
	TickSize     Decimal         `json:"tick_size,omitempty"`
	MinOrderSize Decimal         `json:"min_order_size,omitempty"`
	DepthLimit   int             `json:"depth_limit"`
	BestBid      Decimal         `json:"best_bid,omitempty"`
	BestAsk      Decimal         `json:"best_ask,omitempty"`
	Bids         []PriceLevel    `json:"bids"`
	Asks         []PriceLevel    `json:"asks"`
	ErrorCode    string          `json:"error_code,omitempty"`
}

// Validate 校验当前模型的字段完整性和业务约束。
func (book OrderBookSnapshot) Validate() error {
	if strings.TrimSpace(book.MarketID) == "" || strings.TrimSpace(book.ConditionID) == "" || strings.TrimSpace(book.TokenID) == "" {
		return fmt.Errorf("orderbook market, condition, and token identifiers are required")
	}
	if book.OutcomeIndex != 0 && book.OutcomeIndex != 1 {
		return fmt.Errorf("orderbook outcome index must be 0 or 1")
	}
	if book.ObservedAt.IsZero() {
		return fmt.Errorf("orderbook observed_at is required")
	}
	// 仅供执行使用的旧版最新盘口适配器允许 DepthLimit 为零，策略请求会另行严格校验 15 档深度。
	if book.DepthLimit < 0 || book.DepthLimit > StrategyOrderBookDepth {
		return fmt.Errorf("orderbook depth_limit must be between 0 and %d", StrategyOrderBookDepth)
	}
	if !book.TickSize.IsEmpty() {
		if sign, err := book.TickSize.Sign(); err != nil || sign <= 0 {
			return fmt.Errorf("orderbook tick_size must be positive")
		}
	}
	if !book.MinOrderSize.IsEmpty() {
		if sign, err := book.MinOrderSize.Sign(); err != nil || sign <= 0 {
			return fmt.Errorf("orderbook min_order_size must be positive")
		}
	}
	switch book.Status {
	case OrderBookStatusOK:
		if book.SourceAt.IsZero() || len(book.Bids) == 0 || len(book.Asks) == 0 {
			return fmt.Errorf("OK orderbook requires source time and both sides")
		}
	case OrderBookStatusEmpty, OrderBookStatusMissing, OrderBookStatusError:
	default:
		return fmt.Errorf("unsupported orderbook status %q", book.Status)
	}
	if len(book.Bids) > 15 || len(book.Asks) > 15 {
		return fmt.Errorf("orderbook cannot exceed 15 levels per side")
	}
	if err := validateLevels(book.Bids, true); err != nil {
		return fmt.Errorf("invalid bids: %w", err)
	}
	if err := validateLevels(book.Asks, false); err != nil {
		return fmt.Errorf("invalid asks: %w", err)
	}
	if !book.BestBid.IsEmpty() && (len(book.Bids) == 0 || !book.BestBid.Equal(book.Bids[0].Price)) {
		return fmt.Errorf("best_bid must equal bids[0].price")
	}
	if !book.BestAsk.IsEmpty() && (len(book.Asks) == 0 || !book.BestAsk.Equal(book.Asks[0].Price)) {
		return fmt.Errorf("best_ask must equal asks[0].price")
	}
	return nil
}

// validateLevels 校验 Levels 的字段和业务约束。
func validateLevels(levels []PriceLevel, descending bool) error {
	for index, level := range levels {
		priceSign, err := level.Price.Sign()
		if err != nil || priceSign <= 0 {
			return fmt.Errorf("level %d price must be positive", index)
		}
		sizeSign, err := level.Size.Sign()
		if err != nil || sizeSign <= 0 {
			return fmt.Errorf("level %d size must be positive", index)
		}
		if index == 0 {
			continue
		}
		comparison, err := levels[index-1].Price.Compare(level.Price)
		if err != nil || (descending && comparison < 0) || (!descending && comparison > 0) {
			return fmt.Errorf("levels are not sorted")
		}
	}
	return nil
}

// BookTarget 表示后端使用的 BookTarget 类型。
type BookTarget struct {
	MarketID     string
	ConditionID  string
	OutcomeIndex int
	TokenID      string
}

// MidPriceHistoryStatus 表示后端使用的 MidPriceHistoryStatus 类型。
type MidPriceHistoryStatus string

const (
	MidPriceHistoryStatusOK      MidPriceHistoryStatus = "OK"
	MidPriceHistoryStatusPartial MidPriceHistoryStatus = "PARTIAL"
	MidPriceHistoryStatusEmpty   MidPriceHistoryStatus = "EMPTY"
	MidPriceHistoryStatusMissing MidPriceHistoryStatus = "MISSING"
	MidPriceHistoryStatusError   MidPriceHistoryStatus = "ERROR"
)

// MidPriceSampling 表示后端使用的 MidPriceSampling 类型。
type MidPriceSampling string

// MidPriceMissingValuePolicy 表示后端使用的 MidPriceMissingValuePolicy 类型。
type MidPriceMissingValuePolicy string

// MidPriceTimestampSemantics 表示后端使用的 MidPriceTimestampSemantics 类型。
type MidPriceTimestampSemantics string

const (
	MidPriceSamplingUpstreamRaw              MidPriceSampling           = "UPSTREAM_RAW"
	MidPriceMissingValuePolicyNoFill         MidPriceMissingValuePolicy = "NO_FILL"
	MidPriceTimestampSemanticsIntervalEndUTC MidPriceTimestampSemantics = "INTERVAL_END_UTC"
)

// MidPricePoint 表示 Polymarket /prices-history 的一个原始点，P 直接映射上游 p，仅将时间规范到 UTC 分钟区间末端。
// 该过程不重新计算买卖盘中点，也不进行重采样、插值或前向填充。
type MidPricePoint struct {
	IntervalEndAt time.Time `json:"interval_end_at"`
	P             Decimal   `json:"p"`
}

// MidPriceHistory 表示单个 Outcome Token 的冻结历史中间价序列，请求窗口始终以周期决策时间为上界。
type MidPriceHistory struct {
	MarketID           string                     `json:"market_id"`
	ConditionID        string                     `json:"condition_id"`
	OutcomeIndex       int                        `json:"outcome_index"`
	TokenID            string                     `json:"token_id"`
	Status             MidPriceHistoryStatus      `json:"status"`
	WindowStart        time.Time                  `json:"window_start"`
	WindowEnd          time.Time                  `json:"window_end"`
	FidelitySeconds    int                        `json:"fidelity_seconds"`
	Sampling           MidPriceSampling           `json:"sampling"`
	MissingValues      MidPriceMissingValuePolicy `json:"missing_value_policy"`
	TimestampSemantics MidPriceTimestampSemantics `json:"timestamp_semantics"`
	FetchedAt          time.Time                  `json:"fetched_at"`
	CoverageStart      time.Time                  `json:"coverage_start,omitempty"`
	CoverageEnd        time.Time                  `json:"coverage_end,omitempty"`
	MidPrices          []MidPricePoint            `json:"mid_prices"`
	ErrorCode          string                     `json:"error_code,omitempty"`
}

// Validate 校验当前模型的字段完整性和业务约束。
func (history MidPriceHistory) Validate() error {
	if strings.TrimSpace(history.MarketID) == "" || strings.TrimSpace(history.ConditionID) == "" || strings.TrimSpace(history.TokenID) == "" {
		return fmt.Errorf("mid-price history market, condition, and token identifiers are required")
	}
	if history.OutcomeIndex != 0 && history.OutcomeIndex != 1 {
		return fmt.Errorf("mid-price history outcome index must be 0 or 1")
	}
	if history.WindowStart.IsZero() || history.WindowEnd.IsZero() || !history.WindowStart.Before(history.WindowEnd) {
		return fmt.Errorf("mid-price history window is invalid")
	}
	if history.FidelitySeconds != 60 || history.FetchedAt.IsZero() {
		return fmt.Errorf("mid-price history fidelity and fetched_at are required")
	}
	if history.Sampling != MidPriceSamplingUpstreamRaw || history.MissingValues != MidPriceMissingValuePolicyNoFill ||
		history.TimestampSemantics != MidPriceTimestampSemanticsIntervalEndUTC {
		return fmt.Errorf("mid-price history must be raw, unfilled, interval-end UTC data")
	}
	switch history.Status {
	case MidPriceHistoryStatusOK, MidPriceHistoryStatusPartial:
		if len(history.MidPrices) == 0 || history.CoverageStart.IsZero() || history.CoverageEnd.IsZero() {
			return fmt.Errorf("%s mid-price history requires points and coverage", history.Status)
		}
	case MidPriceHistoryStatusEmpty:
		if len(history.MidPrices) != 0 {
			return fmt.Errorf("EMPTY mid-price history must not contain points")
		}
	case MidPriceHistoryStatusMissing, MidPriceHistoryStatusError:
		if len(history.MidPrices) != 0 || strings.TrimSpace(history.ErrorCode) == "" {
			return fmt.Errorf("%s mid-price history requires an error code and no points", history.Status)
		}
	default:
		return fmt.Errorf("unsupported mid-price history status %q", history.Status)
	}
	previous := time.Time{}
	for index, point := range history.MidPrices {
		if point.IntervalEndAt.IsZero() || point.IntervalEndAt.Before(history.WindowStart) || point.IntervalEndAt.After(history.WindowEnd) {
			return fmt.Errorf("mid-price point %d timestamp is outside the requested window", index)
		}
		if !previous.IsZero() && !point.IntervalEndAt.After(previous) {
			return fmt.Errorf("mid-price points must be strictly ordered and unique")
		}
		if sign, err := point.P.Sign(); err != nil || sign < 0 {
			return fmt.Errorf("mid-price point %d must be a decimal in [0,1]", index)
		}
		if comparison, err := point.P.Compare(Decimal("1")); err != nil || comparison > 0 {
			return fmt.Errorf("mid-price point %d must be a decimal in [0,1]", index)
		}
		previous = point.IntervalEndAt
	}
	if len(history.MidPrices) > 0 {
		if !history.CoverageStart.Equal(history.MidPrices[0].IntervalEndAt) ||
			!history.CoverageEnd.Equal(history.MidPrices[len(history.MidPrices)-1].IntervalEndAt) {
			return fmt.Errorf("mid-price history coverage does not match its points")
		}
	}
	return nil
}

// StrategyExecutionContext 表示服务端维护的执行绑定：ModelID 标识预测生产者，StrategyID 选择 Python 策略，ExecutionAccountID 选择隔离账户。
type StrategyExecutionContext struct {
	ModelID            string `json:"model_id"`
	StrategyID         string `json:"strategy_id"`
	ExecutionAccountID string `json:"execution_account_id"`
}

const (
	StrategyIDMultfactorV1 = "multfactor_v1"
	StrategyIDMultfactorV2 = "multfactor_v2"
)

// Normalize 规范化当前模型的文本、时间和可变字段。
func (executionContext StrategyExecutionContext) Normalize() StrategyExecutionContext {
	executionContext.ModelID = strings.TrimSpace(executionContext.ModelID)
	executionContext.StrategyID = CanonicalStrategyID(executionContext.StrategyID)
	executionContext.ExecutionAccountID = strings.TrimSpace(executionContext.ExecutionAccountID)
	return executionContext
}

// CanonicalStrategyID 将历史策略别名规范化为当前协议标识。
func CanonicalStrategyID(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "strategy-v1", "mult-factor-v1", StrategyIDMultfactorV1:
		return StrategyIDMultfactorV1
	case "strategy-v2", "mult-factor-v2", StrategyIDMultfactorV2:
		return StrategyIDMultfactorV2
	default:
		return strings.TrimSpace(value)
	}
}

// StrategyPositionLot 表示发送给 Python 的逐成交仓位批次，Shares 是该批次的精确剩余数量而不是 Token 净仓位。
type StrategyPositionLot struct {
	LotID        string    `json:"lot_id"`
	MarketID     string    `json:"market_id"`
	ConditionID  string    `json:"condition_id"`
	OutcomeIndex int       `json:"outcome_index"`
	OutcomeName  string    `json:"outcome_name"`
	TokenID      string    `json:"token_id"`
	NegRisk      bool      `json:"neg_risk"`
	EnteredAt    time.Time `json:"entered_at"`
	Shares       Decimal   `json:"shares"`
	EntryPrice   Decimal   `json:"entry_price"`
}

// Validate 校验当前模型的字段完整性和业务约束。
func (lot StrategyPositionLot) Validate(decisionAt time.Time) error {
	if strings.TrimSpace(lot.LotID) == "" || strings.TrimSpace(lot.MarketID) == "" ||
		strings.TrimSpace(lot.ConditionID) == "" || strings.TrimSpace(lot.OutcomeName) == "" || strings.TrimSpace(lot.TokenID) == "" {
		return fmt.Errorf("position lot identity is required")
	}
	if lot.OutcomeIndex != 0 && lot.OutcomeIndex != 1 {
		return fmt.Errorf("position lot outcome_index must be 0 or 1")
	}
	if lot.EnteredAt.IsZero() || lot.EnteredAt.After(decisionAt.UTC()) {
		return fmt.Errorf("position lot entered_at is invalid")
	}
	if sign, err := lot.Shares.Sign(); err != nil || sign <= 0 {
		return fmt.Errorf("position lot shares must be positive")
	}
	if sign, err := lot.EntryPrice.Sign(); err != nil || sign <= 0 {
		return fmt.Errorf("position lot entry_price must be positive")
	}
	if comparison, err := lot.EntryPrice.Compare("1"); err != nil || comparison > 0 {
		return fmt.Errorf("position lot entry_price must not exceed one")
	}
	return nil
}

// StrategyExecutionConstraints 表示后端使用的 StrategyExecutionConstraints 类型。
type StrategyExecutionConstraints struct {
	SizeUnit              string        `json:"size_unit"`
	SizeDecimalPlaces     int           `json:"size_decimal_places"`
	BuyNotionalDecimals   int           `json:"buy_notional_decimal_places"`
	MinimumBuyNotional    Decimal       `json:"minimum_buy_notional"`
	AllowedTimeInForce    []TimeInForce `json:"allowed_time_in_force"`
	PriceProtectionPolicy string        `json:"price_protection_policy"`
}

// DefaultStrategyExecutionConstraints 返回策略下单协议的默认执行约束。
func DefaultStrategyExecutionConstraints() StrategyExecutionConstraints {
	return StrategyExecutionConstraints{
		SizeUnit: "SHARES", SizeDecimalPlaces: 2, BuyNotionalDecimals: 2,
		MinimumBuyNotional: "1", AllowedTimeInForce: []TimeInForce{TimeInForceFOK},
		PriceProtectionPolicy: "EXACT_TOP_OF_BOOK",
	}
}

// Validate 校验当前模型的字段完整性和业务约束。
func (executionContext StrategyExecutionContext) Validate() error {
	executionContext = executionContext.Normalize()
	if executionContext.ModelID == "" || executionContext.StrategyID == "" || executionContext.ExecutionAccountID == "" {
		return fmt.Errorf("model_id, strategy_id, and execution_account_id are required")
	}
	return nil
}

// Equal 判断两个值在规范化后是否相等。
func (executionContext StrategyExecutionContext) Equal(other StrategyExecutionContext) bool {
	return executionContext.Normalize() == other.Normalize()
}

// StrategyDecisionRequest 表示后端使用的 StrategyDecisionRequest 类型。
type StrategyDecisionRequest struct {
	SchemaVersion        string                       `json:"schema_version"`
	CycleID              string                       `json:"cycle_id"`
	InputID              string                       `json:"input_id"`
	Context              StrategyExecutionContext     `json:"context"`
	DecisionAt           time.Time                    `json:"decision_at"`
	GeneratedAt          time.Time                    `json:"generated_at"`
	PredictionSnapshotID string                       `json:"prediction_snapshot_id"`
	PredictionScope      PredictionScope              `json:"prediction_scope"`
	Predictions          []Prediction                 `json:"predictions"`
	Positions            []StrategyPositionLot        `json:"positions"`
	OrderBooks           []OrderBookSnapshot          `json:"orderbooks"`
	MidPriceHistories    []MidPriceHistory            `json:"mid_price_histories"`
	ExecutionConstraints StrategyExecutionConstraints `json:"execution_constraints"`
}

// ComputeStrategyInputID 对决策相关的冻结策略输入计算稳定内容哈希。
func ComputeStrategyInputID(request StrategyDecisionRequest) (string, error) {
	identity := struct {
		SchemaVersion        string                       `json:"schema_version"`
		CycleID              string                       `json:"cycle_id"`
		Context              StrategyExecutionContext     `json:"context"`
		DecisionAt           time.Time                    `json:"decision_at"`
		PredictionSnapshotID string                       `json:"prediction_snapshot_id"`
		PredictionScope      PredictionScope              `json:"prediction_scope"`
		Predictions          []Prediction                 `json:"predictions"`
		Positions            []StrategyPositionLot        `json:"positions"`
		OrderBooks           []OrderBookSnapshot          `json:"orderbooks"`
		MidPriceHistories    []MidPriceHistory            `json:"mid_price_histories"`
		ExecutionConstraints StrategyExecutionConstraints `json:"execution_constraints"`
	}{
		SchemaVersion:        request.SchemaVersion,
		CycleID:              request.CycleID,
		Context:              request.Context.Normalize(),
		DecisionAt:           request.DecisionAt,
		PredictionSnapshotID: request.PredictionSnapshotID,
		PredictionScope:      request.PredictionScope,
		Predictions:          request.Predictions,
		Positions:            request.Positions,
		OrderBooks:           request.OrderBooks,
		MidPriceHistories:    request.MidPriceHistories,
		ExecutionConstraints: request.ExecutionConstraints,
	}
	payload, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("encode strategy input identity: %w", err)
	}
	digest := sha256.Sum256(payload)
	return "strategy-input-" + hex.EncodeToString(digest[:]), nil
}

// StrategyAction 表示后端使用的 StrategyAction 类型。
type StrategyAction string

const (
	StrategyActionSkip   StrategyAction = "SKIP"
	StrategyActionSubmit StrategyAction = "SUBMIT"
)

// StrategyReasonCode 表示后端使用的 StrategyReasonCode 类型。
type StrategyReasonCode string

const (
	StrategyReasonEntrySignal     StrategyReasonCode = "ENTRY_SIGNAL"
	StrategyReasonEdgeTooLow      StrategyReasonCode = "EDGE_TOO_LOW"
	StrategyReasonSpreadTooWide   StrategyReasonCode = "SPREAD_TOO_WIDE"
	StrategyReasonLiquidityTooLow StrategyReasonCode = "LIQUIDITY_TOO_LOW"
	StrategyReasonPriceOutOfRange StrategyReasonCode = "PRICE_OUT_OF_RANGE"
	StrategyReasonHourlyVeto      StrategyReasonCode = "HOURLY_VETO"
	StrategyReasonFactorWarmup    StrategyReasonCode = "FACTOR_WARMUP"
	StrategyReasonStaleData       StrategyReasonCode = "STALE_DATA"
	StrategyReasonInvalidBook     StrategyReasonCode = "INVALID_BOOK"
	StrategyReasonHold48H         StrategyReasonCode = "HOLD_48H"
)

// StrategyEvidence 保存形成策略决策的证据值，Metrics 使用字符串小数以保持执行精度和传输协议稳定。
type StrategyEvidence struct {
	Probability float64           `json:"probability"`
	Edge        Decimal           `json:"edge,omitempty"`
	Metrics     map[string]string `json:"metrics,omitempty"`
}

// StrategyOrderParams 只包含策略负责的订单选择，交易所、策略身份、信号身份和 client_order_id 由执行服务根据认证上下文赋值。
type StrategyOrderParams struct {
	Side        Side        `json:"side"`
	Type        OrderType   `json:"type"`
	WorstPrice  Decimal     `json:"worst_price"`
	Size        Decimal     `json:"size"`
	TimeInForce TimeInForce `json:"time_in_force"`
	ExpiresAt   *time.Time  `json:"expires_at,omitempty"`
}

// StrategyEvaluation 表示每个预测与 Outcome 组合的评估结果，SKIP 是可审计的正式决策而不是被省略的订单。
type StrategyEvaluation struct {
	DecisionID   string               `json:"decision_id"`
	PredictionID string               `json:"prediction_id"`
	MarketID     string               `json:"market_id"`
	ConditionID  string               `json:"condition_id"`
	OutcomeIndex int                  `json:"outcome_index"`
	TokenID      string               `json:"token_id"`
	Action       StrategyAction       `json:"action"`
	ReasonCode   StrategyReasonCode   `json:"reason_code"`
	Reason       string               `json:"reason,omitempty"`
	Evidence     StrategyEvidence     `json:"evidence"`
	Order        *StrategyOrderParams `json:"order,omitempty"`
}

// StrategyExit 表示针对某个指定仓位批次提交的 SELL；未出现在 exits 中的批次视为 HOLD。
type StrategyExit struct {
	DecisionID string               `json:"decision_id"`
	LotID      string               `json:"lot_id"`
	TokenID    string               `json:"token_id"`
	ReasonCode StrategyReasonCode   `json:"reason_code"`
	Reason     string               `json:"reason,omitempty"`
	Order      *StrategyOrderParams `json:"order"`
}

// StrategyDecisionResponse 表示后端使用的 StrategyDecisionResponse 类型。
type StrategyDecisionResponse struct {
	SchemaVersion string                   `json:"schema_version"`
	CycleID       string                   `json:"cycle_id"`
	InputID       string                   `json:"input_id"`
	Context       StrategyExecutionContext `json:"context"`
	DecidedAt     time.Time                `json:"decided_at"`
	Evaluations   []StrategyEvaluation     `json:"evaluations"`
	Exits         []StrategyExit           `json:"exits"`
}
