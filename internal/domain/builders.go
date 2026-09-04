package domain

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

// OrderIntentParams 承载订单意图的构建参数并隔离调用方与领域模型的初始化细节。
type OrderIntentParams OrderIntent

// Build 规范化并校验参数后构建可进入执行链路的订单意图。
func (params OrderIntentParams) Build() (OrderIntent, error) {
	intent := OrderIntent(params).Normalize()
	if err := intent.Validate(); err != nil {
		return OrderIntent{}, err
	}
	return intent, nil
}

// OrderParams 承载新订单聚合的最小构建参数。
type OrderParams struct {
	ID        string
	Intent    OrderIntent
	CreatedAt time.Time
}

// Build 校验订单身份和意图后构建初始状态为 RECEIVED 的订单聚合。
func (params OrderParams) Build() (Order, error) {
	id := strings.TrimSpace(params.ID)
	if id == "" {
		return Order{}, fmt.Errorf("order id is required")
	}
	intent, err := OrderIntentParams(params.Intent).Build()
	if err != nil {
		return Order{}, fmt.Errorf("order intent: %w", err)
	}
	if params.CreatedAt.IsZero() {
		return Order{}, fmt.Errorf("order created_at is required")
	}
	createdAt := params.CreatedAt.UTC()
	return Order{
		ID:             id,
		Intent:         intent,
		Status:         OrderStatusReceived,
		FilledSize:     "0",
		FilledNotional: "0",
		TotalFees:      "0",
		CreatedAt:      createdAt,
		UpdatedAt:      createdAt,
		Revision:       1,
	}, nil
}

// OrderAttemptParams 承载一次订单外部操作尝试的构建参数。
type OrderAttemptParams struct {
	ID                 string
	OrderID            string
	Sequence           int
	Kind               OrderAttemptKind
	RequestFingerprint string
	VenueOrderID       string
	StartedAt          time.Time
}

// Build 校验尝试序号和类型后构建处于 STARTED 状态的订单尝试。
func (params OrderAttemptParams) Build() (OrderAttempt, error) {
	params.ID = strings.TrimSpace(params.ID)
	params.OrderID = strings.TrimSpace(params.OrderID)
	params.RequestFingerprint = strings.TrimSpace(params.RequestFingerprint)
	params.VenueOrderID = strings.TrimSpace(params.VenueOrderID)
	if params.ID == "" || params.OrderID == "" || params.Sequence < 1 || params.StartedAt.IsZero() {
		return OrderAttempt{}, fmt.Errorf("attempt id, order id, positive sequence, and started_at are required")
	}
	switch params.Kind {
	case OrderAttemptSubmit, OrderAttemptCancel, OrderAttemptReconcile:
	default:
		return OrderAttempt{}, fmt.Errorf("unsupported order attempt kind %q", params.Kind)
	}
	return OrderAttempt{
		ID:                 params.ID,
		OrderID:            params.OrderID,
		Sequence:           params.Sequence,
		Kind:               params.Kind,
		Outcome:            AttemptOutcomeStarted,
		RequestFingerprint: params.RequestFingerprint,
		VenueOrderID:       params.VenueOrderID,
		StartedAt:          params.StartedAt.UTC(),
	}, nil
}

// BookTargetParams 承载一个行情采集目标的构建参数。
type BookTargetParams BookTarget

// Build 规范化并校验市场、条件、结果和 token 的联合身份。
func (params BookTargetParams) Build() (BookTarget, error) {
	target := BookTarget(params)
	target.MarketSource = inferredMarketSource(target.MarketSource, target.ConditionID)
	target.MarketID = strings.TrimSpace(target.MarketID)
	target.ConditionID = strings.TrimSpace(target.ConditionID)
	target.OutcomeID = strings.ToUpper(strings.TrimSpace(target.OutcomeID))
	target.TokenID = strings.TrimSpace(target.TokenID)
	instrumentID, err := CanonicalInstrumentID(
		target.MarketSource, target.MarketID, target.ConditionID, target.OutcomeID, target.TokenID,
	)
	if err != nil || instrumentID != target.TokenID {
		return BookTarget{}, fmt.Errorf("book target venue identity is invalid")
	}
	if target.OutcomeIndex != 0 && target.OutcomeIndex != 1 {
		return BookTarget{}, fmt.Errorf("book target outcome index must be 0 or 1")
	}
	return target, nil
}

// StrategyPositionLotParams 承载策略持仓批次视图的构建参数。
type StrategyPositionLotParams StrategyPositionLot

// Build 规范化并校验指定决策时点可见的策略持仓批次。
func (params StrategyPositionLotParams) Build(decisionAt time.Time) (StrategyPositionLot, error) {
	lot := StrategyPositionLot(params)
	lot.LotID = strings.TrimSpace(lot.LotID)
	lot.MarketID = strings.TrimSpace(lot.MarketID)
	lot.ConditionID = strings.TrimSpace(lot.ConditionID)
	lot.OutcomeName = strings.TrimSpace(lot.OutcomeName)
	lot.TokenID = strings.TrimSpace(lot.TokenID)
	lot.EnteredAt = lot.EnteredAt.UTC()
	lot.Shares = Decimal(strings.TrimSpace(lot.Shares.String()))
	lot.EntryPrice = Decimal(strings.TrimSpace(lot.EntryPrice.String()))
	if err := lot.Validate(decisionAt); err != nil {
		return StrategyPositionLot{}, err
	}
	return lot, nil
}

// StrategyDecisionRequestParams 承载冻结策略输入的构建参数。
type StrategyDecisionRequestParams struct {
	CycleID              string
	Context              StrategyExecutionContext
	DecisionAt           time.Time
	GeneratedAt          time.Time
	PredictionSnapshotID string
	Predictions          []Prediction
	Positions            []StrategyPositionLot
	OrderBooks           []OrderBookSnapshot
}

// Build 填充固定协议字段并计算内容寻址的策略输入 ID。
func (params StrategyDecisionRequestParams) Build() (StrategyDecisionRequest, error) {
	request := StrategyDecisionRequest{
		SchemaVersion:        StrategyInputSchemaVersion,
		CycleID:              strings.TrimSpace(params.CycleID),
		Context:              params.Context.Normalize(),
		DecisionAt:           params.DecisionAt.UTC(),
		GeneratedAt:          params.GeneratedAt.UTC(),
		PredictionSnapshotID: strings.TrimSpace(params.PredictionSnapshotID),
		PredictionScope:      PredictionScopeAllEffective,
		Predictions:          slices.Clone(params.Predictions),
		Positions:            slices.Clone(params.Positions),
		OrderBooks:           slices.Clone(params.OrderBooks),
		ExecutionConstraints: DefaultStrategyExecutionConstraints(),
	}
	if request.CycleID == "" || request.PredictionSnapshotID == "" || request.DecisionAt.IsZero() || request.GeneratedAt.IsZero() {
		return StrategyDecisionRequest{}, fmt.Errorf("strategy cycle, snapshot, decision_at, and generated_at are required")
	}
	if err := request.Context.Validate(); err != nil {
		return StrategyDecisionRequest{}, err
	}
	inputID, err := ComputeStrategyInputID(request)
	if err != nil {
		return StrategyDecisionRequest{}, err
	}
	request.InputID = inputID
	return request, nil
}

// PositionExitRequestParams 承载冻结持仓退出输入的构建参数。
type PositionExitRequestParams struct {
	CycleID              string
	DecisionAt           time.Time
	GeneratedAt          time.Time
	Context              StrategyExecutionContext
	PredictionSnapshotID string
	Predictions          []Prediction
	Trades               []PositionExitTrade
	MarketData           []PositionExitMarketData
}

// PositionExitMarketDataParams 承载一个持仓退出行情快照的构建参数。
type PositionExitMarketDataParams PositionExitMarketData

// Build 规范化时间并校验持仓退出行情快照的联合身份和数据完整性。
func (params PositionExitMarketDataParams) Build() (PositionExitMarketData, error) {
	marketData := PositionExitMarketData(params)
	marketData.MarketID = strings.TrimSpace(marketData.MarketID)
	marketData.ConditionID = strings.TrimSpace(marketData.ConditionID)
	marketData.TokenID = strings.TrimSpace(marketData.TokenID)
	marketData.MarketObservedAt = marketData.MarketObservedAt.UTC()
	if marketData.ClosedAt != nil {
		closedAt := marketData.ClosedAt.UTC()
		marketData.ClosedAt = &closedAt
	}
	if err := marketData.Validate(); err != nil {
		return PositionExitMarketData{}, err
	}
	return marketData, nil
}

// Build 填充固定协议字段并计算内容寻址的持仓退出输入 ID。
func (params PositionExitRequestParams) Build() (PositionExitRequest, error) {
	request := PositionExitRequest{
		SchemaVersion:        PositionExitInputSchemaVersion,
		CycleID:              strings.TrimSpace(params.CycleID),
		DecisionAt:           params.DecisionAt.UTC(),
		GeneratedAt:          params.GeneratedAt.UTC(),
		Context:              params.Context.Normalize(),
		PredictionSnapshotID: strings.TrimSpace(params.PredictionSnapshotID),
		PredictionScope:      PredictionScopeAllEffective,
		Predictions:          slices.Clone(params.Predictions),
		Trades:               slices.Clone(params.Trades),
		MarketData:           slices.Clone(params.MarketData),
		ExecutionConstraints: DefaultPositionExitExecutionConstraints(),
	}
	if request.CycleID == "" || request.PredictionSnapshotID == "" || request.DecisionAt.IsZero() || request.GeneratedAt.IsZero() {
		return PositionExitRequest{}, fmt.Errorf("position exit cycle, snapshot, decision_at, and generated_at are required")
	}
	if err := request.Context.Validate(); err != nil {
		return PositionExitRequest{}, err
	}
	inputID, err := ComputePositionExitInputID(request)
	if err != nil {
		return PositionExitRequest{}, err
	}
	request.InputID = inputID
	return request, nil
}

// MarketValidationParams 承载执行前市场校验凭据的构建参数。
type MarketValidationParams MarketValidation

// Build 规范化并校验执行前市场凭据的核心身份、时间和盘口字段。
func (params MarketValidationParams) Build() (MarketValidation, error) {
	validation := MarketValidation(params)
	validation.Mode = strings.TrimSpace(validation.Mode)
	validation.OutcomeName = strings.TrimSpace(validation.OutcomeName)
	validation.TokenID = strings.TrimSpace(validation.TokenID)
	validation.ValidatedAt = validation.ValidatedAt.UTC()
	validation.MarketObservedAt = validation.MarketObservedAt.UTC()
	validation.StrategySnapshotAt = validation.StrategySnapshotAt.UTC()
	validation.LatestBookSourceAt = validation.LatestBookSourceAt.UTC()
	validation.LatestBookObservedAt = validation.LatestBookObservedAt.UTC()
	if validation.Mode == "" || validation.TokenID == "" || validation.OutcomeName == "" ||
		validation.ValidatedAt.IsZero() || validation.MarketObservedAt.IsZero() ||
		validation.StrategySnapshotAt.IsZero() || validation.LatestBookSourceAt.IsZero() ||
		validation.LatestBookObservedAt.IsZero() {
		return MarketValidation{}, fmt.Errorf("market validation identity and timestamps are required")
	}
	if validation.OutcomeIndex != 0 && validation.OutcomeIndex != 1 {
		return MarketValidation{}, fmt.Errorf("market validation outcome index must be 0 or 1")
	}
	for name, value := range map[string]Decimal{
		"tick_size": validation.TickSize, "best_bid": validation.BestBid,
		"best_ask": validation.BestAsk, "worst_price": validation.WorstPrice,
	} {
		if sign, err := value.Sign(); err != nil || sign <= 0 {
			return MarketValidation{}, fmt.Errorf("market validation %s must be positive", name)
		}
	}
	if !validation.MinOrderSize.IsEmpty() {
		if sign, err := validation.MinOrderSize.Sign(); err != nil || sign <= 0 {
			return MarketValidation{}, fmt.Errorf("market validation min_order_size must be positive")
		}
	}
	if !validation.ExecutableSize.IsEmpty() {
		if sign, err := validation.ExecutableSize.Sign(); err != nil || sign <= 0 {
			return MarketValidation{}, fmt.Errorf("market validation executable_size must be positive")
		}
		if !validation.MinOrderSize.IsEmpty() {
			if comparison, err := validation.ExecutableSize.Compare(validation.MinOrderSize); err != nil || comparison < 0 {
				return MarketValidation{}, fmt.Errorf("market validation executable_size is below min_order_size")
			}
		}
	}
	if validation.BuyFeeReserve != nil {
		reserve := *validation.BuyFeeReserve
		reserve.Source = strings.TrimSpace(reserve.Source)
		reserve.Reason = strings.TrimSpace(reserve.Reason)
		if err := reserve.Validate(); err != nil {
			return MarketValidation{}, fmt.Errorf("market validation buy_fee_reserve: %w", err)
		}
		validation.BuyFeeReserve = &reserve
	}
	return validation, nil
}

// ReconciliationRunParams 承载一次账户对账运行的构建参数。
type ReconciliationRunParams struct {
	RunID              string
	ExecutionAccountID string
	Trigger            ReconciliationTrigger
	FocusOrderID       string
	StartedAt          time.Time
}

// Build 校验对账身份后构建处于 RUNNING 状态的运行记录。
func (params ReconciliationRunParams) Build() (ReconciliationRun, error) {
	params.RunID = strings.TrimSpace(params.RunID)
	params.ExecutionAccountID = strings.TrimSpace(params.ExecutionAccountID)
	params.FocusOrderID = strings.TrimSpace(params.FocusOrderID)
	if params.RunID == "" || params.ExecutionAccountID == "" || params.StartedAt.IsZero() {
		return ReconciliationRun{}, fmt.Errorf("reconciliation run id, account id, and started_at are required")
	}
	return ReconciliationRun{
		RunID:              params.RunID,
		ExecutionAccountID: params.ExecutionAccountID,
		Trigger:            params.Trigger,
		FocusOrderID:       params.FocusOrderID,
		Status:             ReconciliationRunRunning,
		StartedAt:          params.StartedAt.UTC(),
		Summary:            map[string]int{},
	}, nil
}

// ReconciliationIssueParams 承载对账问题的构建参数并保留调用方给出的业务字段。
type ReconciliationIssueParams ReconciliationIssue

// Build 规范化对账问题的文本、时间和摘要映射后构建领域模型。
func (params ReconciliationIssueParams) Build() (ReconciliationIssue, error) {
	issue := ReconciliationIssue(params)
	issue.IssueID = strings.TrimSpace(issue.IssueID)
	issue.RunID = strings.TrimSpace(issue.RunID)
	issue.Fingerprint = strings.TrimSpace(issue.Fingerprint)
	issue.ExecutionAccountID = strings.TrimSpace(issue.ExecutionAccountID)
	issue.OrderID = strings.TrimSpace(issue.OrderID)
	issue.VenueOrderID = strings.TrimSpace(issue.VenueOrderID)
	issue.VenueTradeID = strings.TrimSpace(issue.VenueTradeID)
	issue.MarketID = strings.TrimSpace(issue.MarketID)
	issue.ConditionID = strings.TrimSpace(issue.ConditionID)
	issue.TokenID = strings.TrimSpace(issue.TokenID)
	issue.Source = strings.TrimSpace(issue.Source)
	issue.Details = strings.TrimSpace(issue.Details)
	if issue.Type == "" || issue.Resolution == "" || issue.Status == "" || issue.Details == "" {
		return ReconciliationIssue{}, fmt.Errorf("reconciliation issue type, resolution, status, and details are required")
	}
	if !issue.ObservedAt.IsZero() {
		issue.ObservedAt = issue.ObservedAt.UTC()
	}
	if issue.ResolvedAt != nil {
		resolvedAt := issue.ResolvedAt.UTC()
		issue.ResolvedAt = &resolvedAt
	}
	return issue, nil
}
