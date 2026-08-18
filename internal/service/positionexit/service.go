package positionexit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const (
	Interval                  = 10 * time.Minute
	defaultLookback           = 48 * time.Hour
	defaultPredictionLookback = 3 * time.Hour
	responseClockLeeway       = 5 * time.Second
)

var (
	ErrInvalidBoundary = errors.New("position exit decision_at is not an exact 10-minute UTC boundary")
	ErrInvalidResponse = errors.New("position exit strategy response is invalid")
)

// Params 表示后端使用的 Params 类型。
type Params struct {
	PredictionSource   port.PredictionSource
	TradeSource        port.PositionExitTradeSource
	MarketUniverse     port.MarketUniverse
	OrderBookSource    port.OrderBookSource
	MidPriceSource     port.MidPriceHistorySource
	Strategy           port.PositionExitStrategyClient
	Recorder           port.PositionExitRecorder
	Executor           port.OrderExecutor
	Bindings           []domain.StrategyExecutionContext
	Venue              string
	Lookback           time.Duration
	PredictionLookback time.Duration
	Now                func() time.Time
}

// Service 表示后端使用的 Service 类型。
type Service struct {
	predictionSource   port.PredictionSource
	tradeSource        port.PositionExitTradeSource
	marketUniverse     port.MarketUniverse
	orderBookSource    port.OrderBookSource
	midPriceSource     port.MidPriceHistorySource
	strategy           port.PositionExitStrategyClient
	recorder           port.PositionExitRecorder
	executor           port.OrderExecutor
	bindings           []domain.StrategyExecutionContext
	venue              string
	lookback           time.Duration
	predictionLookback time.Duration
	now                func() time.Time
}

// IntentResult 表示后端使用的 IntentResult 类型。
type IntentResult struct {
	Intent domain.OrderIntent     `json:"intent"`
	Result port.OrderSubmitResult `json:"result"`
	Error  string                 `json:"error,omitempty"`
}

// BindingRunResult 表示后端使用的 BindingRunResult 类型。
type BindingRunResult struct {
	Context  domain.StrategyExecutionContext `json:"context"`
	Status   string                          `json:"status"`
	Request  domain.PositionExitRequest      `json:"request"`
	Response domain.PositionExitResponse     `json:"response,omitempty"`
	Intents  []IntentResult                  `json:"intents"`
	Error    string                          `json:"error,omitempty"`
}

// RunResult 表示后端使用的 RunResult 类型。
type RunResult struct {
	DecisionAt time.Time          `json:"decision_at"`
	Runs       []BindingRunResult `json:"runs"`
}

// preparedBinding 表示后端使用的 preparedBinding 类型。
type preparedBinding struct {
	binding domain.StrategyExecutionContext
	request domain.PositionExitRequest
	trades  []domain.PositionExitTrade
	stored  bool
	err     error
}

// New 校验依赖和配置后创建当前服务实例。
func New(params Params) (*Service, error) {
	if params.PredictionSource == nil || params.TradeSource == nil || params.MarketUniverse == nil ||
		params.OrderBookSource == nil || params.MidPriceSource == nil ||
		params.Strategy == nil || params.Recorder == nil || params.Executor == nil {
		return nil, fmt.Errorf("prediction, trade, market universe, orderbook, mid-price, strategy, recorder, and executor dependencies are required")
	}
	bindings, err := normalizeBindings(params.Bindings)
	if err != nil {
		return nil, err
	}
	params.Venue = strings.ToLower(strings.TrimSpace(params.Venue))
	if params.Venue == "" {
		return nil, fmt.Errorf("execution venue is required")
	}
	if params.Lookback == 0 {
		params.Lookback = defaultLookback
	}
	if params.Lookback != defaultLookback {
		return nil, fmt.Errorf("position exit mid-price lookback must be exactly 48h")
	}
	if params.PredictionLookback == 0 {
		params.PredictionLookback = defaultPredictionLookback
	}
	if params.PredictionLookback < Interval {
		return nil, fmt.Errorf("position exit prediction lookback must be at least %s", Interval)
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &Service{
		predictionSource: params.PredictionSource, tradeSource: params.TradeSource,
		marketUniverse: params.MarketUniverse, orderBookSource: params.OrderBookSource,
		midPriceSource: params.MidPriceSource, strategy: params.Strategy,
		recorder: params.Recorder, executor: params.Executor, bindings: bindings,
		venue: params.Venue, lookback: params.Lookback,
		predictionLookback: params.PredictionLookback, now: params.Now,
	}, nil
}

// Run 采集持仓退出快照并逐个隔离账户评估和提交卖出订单。
func (service *Service) Run(ctx context.Context, decisionAt time.Time) (RunResult, error) {
	decisionAt = decisionAt.UTC()
	if decisionAt.IsZero() || decisionAt.Unix()%int64(Interval/time.Second) != 0 {
		return RunResult{}, ErrInvalidBoundary
	}
	prepared := make([]preparedBinding, 0, len(service.bindings))
	allTargets := make(map[string]domain.BookTarget)
	needsNewInput := false
	for _, binding := range service.bindings {
		item := service.prepareBinding(ctx, binding, decisionAt)
		if item.err != nil || item.stored {
			prepared = append(prepared, item)
			continue
		}
		needsNewInput = true
		for _, trade := range item.trades {
			target := targetForTrade(trade)
			if existing, ok := allTargets[target.TokenID]; ok && existing != target {
				item.err = fmt.Errorf("token %q has conflicting position identities", target.TokenID)
				continue
			}
			allTargets[target.TokenID] = target
		}
		prepared = append(prepared, item)
	}
	var predictionSnapshot domain.PredictionSnapshot
	var predictionErr error
	if needsNewInput {
		predictionSnapshot, predictionErr = service.predictionSource.Snapshot(ctx, decisionAt, service.predictionLookback)
		if predictionErr == nil {
			predictionErr = predictionSnapshot.Validate(decisionAt)
		}
		if predictionErr != nil {
			predictionErr = fmt.Errorf("load point-in-time prediction snapshot: %w", predictionErr)
		}
	}
	targets := sortedTargets(allTargets)
	marketByCondition, marketErr := service.captureMarketSnapshots(ctx, targets)
	books, histories, captureErr := service.captureMarketData(ctx, decisionAt, targets)
	bookByToken := make(map[string]domain.OrderBookSnapshot, len(books))
	historyByToken := make(map[string]domain.MidPriceHistory, len(histories))
	if captureErr == nil {
		for _, book := range books {
			bookByToken[book.TokenID] = book
		}
		for _, history := range histories {
			historyByToken[history.TokenID] = history
		}
	}
	result := RunResult{DecisionAt: decisionAt, Runs: make([]BindingRunResult, 0, len(prepared))}
	var runErrors []error
	for _, item := range prepared {
		if item.err != nil {
			run := failedRun(item.binding, item.err)
			result.Runs = append(result.Runs, run)
			runErrors = append(runErrors, item.err)
			continue
		}
		request := item.request
		var err error
		if !item.stored {
			if predictionErr != nil || marketErr != nil || captureErr != nil {
				err := fmt.Errorf("capture exit decision data: %w", errors.Join(predictionErr, marketErr, captureErr))
				result.Runs = append(result.Runs, failedRun(item.binding, err))
				runErrors = append(runErrors, err)
				continue
			}
			request, err = buildRequest(
				item.binding, decisionAt, service.now().UTC(), predictionSnapshot.SnapshotID,
				predictionsForExitModel(predictionSnapshot.Predictions, item.binding.ModelID),
				item.trades, marketByCondition, bookByToken, historyByToken,
			)
			if err == nil {
				request, _, err = service.recorder.ClaimInput(ctx, request)
			}
			if err != nil {
				err = fmt.Errorf("record position exit input: %w", err)
				result.Runs = append(result.Runs, failedRun(item.binding, err))
				runErrors = append(runErrors, err)
				continue
			}
		}
		run, err := service.runBinding(ctx, request)
		if err != nil {
			run.Error = err.Error()
			runErrors = append(runErrors, fmt.Errorf("position exit %s/%s/%s: %w",
				item.binding.ModelID, item.binding.StrategyID, item.binding.ExecutionAccountID, err))
		}
		result.Runs = append(result.Runs, run)
	}
	return result, errors.Join(runErrors...)
}

// prepareBinding 补全并校验 Binding。
func (service *Service) prepareBinding(ctx context.Context, binding domain.StrategyExecutionContext, decisionAt time.Time) preparedBinding {
	cycle := cycleID(binding, decisionAt)
	stored, err := service.recorder.GetInput(ctx, cycle)
	if err == nil {
		if validateErr := validateInput(stored, cycle, binding, decisionAt); validateErr != nil {
			return preparedBinding{binding: binding, err: validateErr}
		}
		return preparedBinding{binding: binding, request: stored, stored: true}
	}
	if !errors.Is(err, port.ErrPositionExitRunNotFound) {
		return preparedBinding{binding: binding, err: fmt.Errorf("read stored position exit input: %w", err)}
	}
	trades, err := service.tradeSource.ListOpenPositionExitTrades(ctx, binding.ExecutionAccountID)
	if err != nil {
		return preparedBinding{binding: binding, err: fmt.Errorf("load open position trades: %w", err)}
	}
	if trades == nil {
		trades = []domain.PositionExitTrade{}
	}
	seen := make(map[string]struct{}, len(trades))
	for index := range trades {
		trade := &trades[index]
		if trade.ExecutionAccountID != binding.ExecutionAccountID || strings.TrimSpace(trade.ModelID) != binding.ModelID ||
			domain.CanonicalStrategyID(trade.StrategyID) != binding.StrategyID {
			return preparedBinding{binding: binding, err: fmt.Errorf("position lot %q does not belong to the configured binding", trade.LotID)}
		}
		if _, exists := seen[trade.LotID]; exists {
			return preparedBinding{binding: binding, err: fmt.Errorf("duplicate position lot %q", trade.LotID)}
		}
		seen[trade.LotID] = struct{}{}
		if err := trade.Validate(decisionAt); err != nil {
			return preparedBinding{binding: binding, err: fmt.Errorf("position lot %q: %w", trade.LotID, err)}
		}
	}
	sort.Slice(trades, func(i, j int) bool {
		if !trades[i].EnteredAt.Equal(trades[j].EnteredAt) {
			return trades[i].EnteredAt.Before(trades[j].EnteredAt)
		}
		return trades[i].LotID < trades[j].LotID
	})
	return preparedBinding{binding: binding, trades: trades}
}

// captureMarketSnapshots 采集 Market Snapshots。
func (service *Service) captureMarketSnapshots(
	ctx context.Context,
	targets []domain.BookTarget,
) (map[string]domain.MarketSnapshot, error) {
	result := make(map[string]domain.MarketSnapshot)
	for _, target := range targets {
		if _, exists := result[target.ConditionID]; exists {
			continue
		}
		market, found, err := service.marketUniverse.FindByCondition(ctx, target.ConditionID)
		if err != nil {
			return nil, fmt.Errorf("load market %s: %w", target.ConditionID, err)
		}
		if !found {
			return nil, fmt.Errorf("market %s is missing from Market Universe Service", target.ConditionID)
		}
		if market.MarketID != target.MarketID || market.ConditionID != target.ConditionID || market.ObservedAt.IsZero() {
			return nil, fmt.Errorf("market %s returned mismatched or incomplete identity", target.ConditionID)
		}
		if (market.Closed || market.Resolved) && (market.ClosedAt == nil || market.ClosedAt.IsZero()) {
			return nil, fmt.Errorf("closed/resolved market %s is missing closed_at", target.ConditionID)
		}
		if !market.Closed && !market.Resolved && market.ClosedAt != nil {
			return nil, fmt.Errorf("open market %s unexpectedly contains closed_at", target.ConditionID)
		}
		result[target.ConditionID] = market
	}
	for _, target := range targets {
		market := result[target.ConditionID]
		matched := false
		for _, outcome := range market.Outcomes {
			if outcome.Index == target.OutcomeIndex && outcome.TokenID == target.TokenID {
				matched = true
				break
			}
		}
		if !matched {
			return nil, fmt.Errorf("market %s no longer maps outcome %d to token %s", target.ConditionID, target.OutcomeIndex, target.TokenID)
		}
	}
	return result, nil
}

// captureMarketData 采集 Market Data。
func (service *Service) captureMarketData(
	ctx context.Context,
	decisionAt time.Time,
	targets []domain.BookTarget,
) ([]domain.OrderBookSnapshot, []domain.MidPriceHistory, error) {
	if len(targets) == 0 {
		return []domain.OrderBookSnapshot{}, []domain.MidPriceHistory{}, nil
	}
	type booksResult struct {
		value []domain.OrderBookSnapshot
		err   error
	}
	type historiesResult struct {
		value []domain.MidPriceHistory
		err   error
	}
	booksChannel := make(chan booksResult, 1)
	historiesChannel := make(chan historiesResult, 1)
	go func() {
		value, err := service.orderBookSource.Capture(ctx, decisionAt, targets)
		booksChannel <- booksResult{value: value, err: err}
	}()
	go func() {
		value, err := service.midPriceSource.Capture(ctx, decisionAt, service.lookback, targets)
		historiesChannel <- historiesResult{value: value, err: err}
	}()
	bookResult, historyResult := <-booksChannel, <-historiesChannel
	if bookResult.err != nil || historyResult.err != nil {
		return nil, nil, errors.Join(bookResult.err, historyResult.err)
	}
	books, err := alignBooks(targets, bookResult.value, service.now().UTC())
	if err != nil {
		return nil, nil, err
	}
	histories, err := alignHistories(targets, historyResult.value, decisionAt, service.lookback, service.now().UTC())
	if err != nil {
		return nil, nil, err
	}
	return books, histories, nil
}

// runBinding 校验单个账户的退出响应并持久化后逐笔提交卖出意图。
func (service *Service) runBinding(ctx context.Context, request domain.PositionExitRequest) (BindingRunResult, error) {
	run := BindingRunResult{Context: request.Context, Request: request, Intents: []IntentResult{}}
	if err := validateInput(request, request.CycleID, request.Context, request.DecisionAt); err != nil {
		return run, err
	}
	if len(request.Trades) == 0 {
		run.Status = "NO_OPEN_TRADES"
		return run, nil
	}
	response, err := service.recorder.GetOutput(ctx, request.CycleID)
	if errors.Is(err, port.ErrPositionExitRunNotFound) {
		response, err = service.strategy.EvaluatePositionExits(ctx, request)
		if err != nil {
			return run, fmt.Errorf("request Python position exit decision: %w", err)
		}
		if _, err := validateResponse(request, response, service.venue, service.now().UTC()); err != nil {
			return run, err
		}
		response, _, err = service.recorder.ClaimOutput(ctx, response)
		if err != nil {
			return run, fmt.Errorf("record position exit output: %w", err)
		}
	} else if err != nil {
		return run, fmt.Errorf("read stored position exit output: %w", err)
	}
	run.Response = response
	intents, err := validateResponse(request, response, service.venue, service.now().UTC())
	if err != nil {
		return run, err
	}
	run.Status = "EVALUATED"
	var executionErrors []error
	for _, intent := range intents {
		item := IntentResult{Intent: intent}
		result, submitErr := service.executor.Submit(ctx, intent)
		item.Result = result
		if submitErr != nil {
			item.Error = submitErr.Error()
			executionErrors = append(executionErrors, fmt.Errorf("submit lot %s: %w", intent.TargetLotID, submitErr))
		}
		run.Intents = append(run.Intents, item)
	}
	return run, errors.Join(executionErrors...)
}

// buildRequest 为单个账户构建内容寻址的冻结持仓退出请求。
func buildRequest(
	binding domain.StrategyExecutionContext,
	decisionAt time.Time,
	generatedAt time.Time,
	predictionSnapshotID string,
	predictions []domain.Prediction,
	trades []domain.PositionExitTrade,
	marketByCondition map[string]domain.MarketSnapshot,
	bookByToken map[string]domain.OrderBookSnapshot,
	historyByToken map[string]domain.MidPriceHistory,
) (domain.PositionExitRequest, error) {
	targets := make(map[string]domain.BookTarget)
	for _, trade := range trades {
		targets[trade.TokenID] = targetForTrade(trade)
	}
	marketData := make([]domain.PositionExitMarketData, 0, len(targets))
	for _, target := range sortedTargets(targets) {
		market := marketByCondition[target.ConditionID]
		item, err := (domain.PositionExitMarketDataParams{
			MarketID: target.MarketID, ConditionID: target.ConditionID,
			OutcomeIndex: target.OutcomeIndex, TokenID: target.TokenID,
			MarketStatus: positionExitMarketStatus(market), ClosedAt: market.ClosedAt,
			MarketObservedAt: market.ObservedAt.UTC(),
			OrderBook:        bookByToken[target.TokenID], MidPriceHistory: historyByToken[target.TokenID],
		}).Build()
		if err != nil {
			return domain.PositionExitRequest{}, err
		}
		marketData = append(marketData, item)
	}
	return (domain.PositionExitRequestParams{
		CycleID: cycleID(binding, decisionAt), DecisionAt: decisionAt, GeneratedAt: generatedAt,
		Context: binding, PredictionSnapshotID: predictionSnapshotID,
		Predictions: predictions,
		Trades:      trades, MarketData: marketData,
	}).Build()
}

// predictionsForExitModel 筛选属于指定退出模型的预测记录。
func predictionsForExitModel(predictions []domain.Prediction, modelID string) []domain.Prediction {
	result := make([]domain.Prediction, 0)
	for _, prediction := range predictions {
		if strings.TrimSpace(prediction.Model.Name) == strings.TrimSpace(modelID) {
			result = append(result, prediction)
		}
	}
	return result
}

// positionExitMarketStatus 将外部或订单状态映射为目标领域状态。
func positionExitMarketStatus(market domain.MarketSnapshot) domain.PositionExitMarketStatus {
	switch {
	case market.Resolved:
		return domain.PositionExitMarketResolved
	case market.Closed:
		return domain.PositionExitMarketClosed
	case market.Paused:
		return domain.PositionExitMarketPaused
	case !market.Active:
		return domain.PositionExitMarketInactive
	case !market.AcceptingOrders:
		return domain.PositionExitMarketNotAcceptingOrders
	default:
		return domain.PositionExitMarketOpen
	}
}

// validateInput 校验 Input 的字段和业务约束。
func validateInput(
	request domain.PositionExitRequest,
	expectedCycleID string,
	expectedContext domain.StrategyExecutionContext,
	expectedDecisionAt time.Time,
) error {
	if request.SchemaVersion != domain.PositionExitInputSchemaVersion || request.CycleID != expectedCycleID ||
		request.InputID == "" || !request.Context.Equal(expectedContext) || !request.DecisionAt.Equal(expectedDecisionAt) || request.GeneratedAt.IsZero() {
		return fmt.Errorf("claimed position exit input identity is invalid")
	}
	constraints := request.ExecutionConstraints
	if constraints.SellSizeUnit != "SHARES" || constraints.SellSizeDecimalPlaces != 2 ||
		constraints.PriceProtectionPolicy != "PYTHON_SUPPLIED_EXACT_BEST_BID" || len(constraints.AllowedTimeInForce) != 1 ||
		constraints.AllowedTimeInForce[0] != domain.TimeInForceFOK {
		return fmt.Errorf("claimed position exit constraints are unsupported")
	}
	if strings.TrimSpace(request.PredictionSnapshotID) == "" || request.PredictionScope != domain.PredictionScopeAllEffective {
		return fmt.Errorf("claimed position exit input requires an all-effective prediction snapshot")
	}
	seenPredictions := make(map[string]struct{}, len(request.Predictions))
	for _, prediction := range request.Predictions {
		if strings.TrimSpace(prediction.Model.Name) != request.Context.ModelID {
			return fmt.Errorf("claimed position exit input contains prediction from another model")
		}
		if err := prediction.Validate(request.DecisionAt); err != nil {
			return fmt.Errorf("claimed position exit prediction %q: %w", prediction.PredictionID, err)
		}
		if _, exists := seenPredictions[prediction.PredictionID]; exists {
			return fmt.Errorf("claimed position exit input contains duplicate prediction %q", prediction.PredictionID)
		}
		seenPredictions[prediction.PredictionID] = struct{}{}
	}
	targets := make(map[string]domain.BookTarget)
	seenLots := make(map[string]struct{}, len(request.Trades))
	for _, trade := range request.Trades {
		if err := trade.Validate(request.DecisionAt); err != nil {
			return fmt.Errorf("claimed position exit lot %q: %w", trade.LotID, err)
		}
		if _, exists := seenLots[trade.LotID]; exists {
			return fmt.Errorf("claimed position exit input has duplicate lot %q", trade.LotID)
		}
		seenLots[trade.LotID] = struct{}{}
		target := targetForTrade(trade)
		if existing, exists := targets[target.TokenID]; exists && existing != target {
			return fmt.Errorf("claimed position exit token %q has conflicting identity", target.TokenID)
		}
		targets[target.TokenID] = target
	}
	if len(request.MarketData) != len(targets) {
		return fmt.Errorf("claimed position exit input must contain one market_data object per token")
	}
	for _, marketData := range request.MarketData {
		if err := marketData.Validate(); err != nil {
			return fmt.Errorf("claimed position exit market data %q: %w", marketData.TokenID, err)
		}
		if marketData.MarketObservedAt.After(request.GeneratedAt) {
			return fmt.Errorf("claimed position exit market data %q was observed after generated_at", marketData.TokenID)
		}
		target, exists := targets[marketData.TokenID]
		if !exists || target != (domain.BookTarget{MarketID: marketData.MarketID, ConditionID: marketData.ConditionID, OutcomeIndex: marketData.OutcomeIndex, TokenID: marketData.TokenID}) {
			return fmt.Errorf("claimed position exit market data %q has unexpected identity", marketData.TokenID)
		}
		delete(targets, marketData.TokenID)
	}
	if len(targets) != 0 {
		return fmt.Errorf("claimed position exit input is missing market data")
	}
	inputID, err := domain.ComputePositionExitInputID(request)
	if err != nil || inputID != request.InputID {
		return fmt.Errorf("claimed position exit input hash does not match payload")
	}
	return nil
}

// validateResponse 校验 Response 的字段和业务约束。
func validateResponse(
	request domain.PositionExitRequest,
	response domain.PositionExitResponse,
	venue string,
	now time.Time,
) ([]domain.OrderIntent, error) {
	if response.SchemaVersion != domain.PositionExitOutputSchemaVersion || response.CycleID != request.CycleID ||
		response.InputID != request.InputID || !response.Context.Equal(request.Context) {
		return nil, fmt.Errorf("%w: schema, cycle, input, or context mismatch", ErrInvalidResponse)
	}
	if response.DecidedAt.IsZero() || response.DecidedAt.Before(request.DecisionAt) || response.DecidedAt.After(now.Add(responseClockLeeway)) {
		return nil, fmt.Errorf("%w: decided_at is invalid", ErrInvalidResponse)
	}
	if len(response.Evaluations) != len(request.Trades) {
		return nil, fmt.Errorf("%w: expected %d lot evaluations, got %d", ErrInvalidResponse, len(request.Trades), len(response.Evaluations))
	}
	trades := make(map[string]domain.PositionExitTrade, len(request.Trades))
	for _, trade := range request.Trades {
		trades[trade.LotID] = trade
	}
	seenLots := make(map[string]struct{}, len(response.Evaluations))
	seenDecisions := make(map[string]struct{}, len(response.Evaluations))
	intents := make([]domain.OrderIntent, 0)
	for index, evaluation := range response.Evaluations {
		evaluation.DecisionID = strings.TrimSpace(evaluation.DecisionID)
		evaluation.LotID = strings.TrimSpace(evaluation.LotID)
		trade, exists := trades[evaluation.LotID]
		if !exists {
			return nil, fmt.Errorf("%w: evaluation %d references an unknown lot", ErrInvalidResponse, index)
		}
		if _, exists := seenLots[evaluation.LotID]; exists {
			return nil, fmt.Errorf("%w: duplicate evaluation for lot %q", ErrInvalidResponse, evaluation.LotID)
		}
		seenLots[evaluation.LotID] = struct{}{}
		if evaluation.DecisionID == "" {
			return nil, fmt.Errorf("%w: evaluation %d requires decision_id", ErrInvalidResponse, index)
		}
		if _, exists := seenDecisions[evaluation.DecisionID]; exists {
			return nil, fmt.Errorf("%w: duplicate decision_id %q", ErrInvalidResponse, evaluation.DecisionID)
		}
		seenDecisions[evaluation.DecisionID] = struct{}{}
		if !domain.ValidPositionExitReason(evaluation.Action, evaluation.ReasonCode) {
			return nil, fmt.Errorf("%w: invalid action/reason_code for lot %q", ErrInvalidResponse, evaluation.LotID)
		}
		if err := evaluation.ValidateEvidence(trade, request.DecisionAt); err != nil {
			return nil, fmt.Errorf("%w: lot %q evidence: %v", ErrInvalidResponse, evaluation.LotID, err)
		}
		marketData, found := marketDataForToken(request.MarketData, trade.TokenID)
		if !found {
			return nil, fmt.Errorf("%w: no market data for lot %q", ErrInvalidResponse, trade.LotID)
		}
		requiredHold := requiredHoldReason(marketData)
		switch evaluation.Action {
		case domain.PositionExitActionHold:
			if evaluation.Order != nil {
				return nil, fmt.Errorf("%w: HOLD lot %q must not contain an order", ErrInvalidResponse, trade.LotID)
			}
			if requiredHold != "" && evaluation.ReasonCode != requiredHold {
				return nil, fmt.Errorf("%w: lot %q must map unusable input to %s", ErrInvalidResponse, trade.LotID, requiredHold)
			}
		case domain.PositionExitActionSell:
			if requiredHold != "" {
				return nil, fmt.Errorf("%w: lot %q cannot SELL with %s input", ErrInvalidResponse, trade.LotID, requiredHold)
			}
			intent, err := buildSellIntent(request, response.DecidedAt, evaluation, trade, marketData, venue)
			if err != nil {
				return nil, fmt.Errorf("%w: lot %q: %v", ErrInvalidResponse, trade.LotID, err)
			}
			intents = append(intents, intent)
		default:
			return nil, fmt.Errorf("%w: unsupported action %q", ErrInvalidResponse, evaluation.Action)
		}
	}
	return intents, nil
}

// buildSellIntent 根据已验证的退出评估构建卖出订单意图。
func buildSellIntent(
	request domain.PositionExitRequest,
	signalAt time.Time,
	evaluation domain.PositionExitEvaluation,
	trade domain.PositionExitTrade,
	marketData domain.PositionExitMarketData,
	venue string,
) (domain.OrderIntent, error) {
	if evaluation.Order == nil {
		return domain.OrderIntent{}, fmt.Errorf("SELL requires order parameters")
	}
	order := *evaluation.Order
	if order.Side != domain.SideSell || order.Type != domain.OrderTypeLimit || order.TimeInForce != domain.TimeInForceFOK || order.ExpiresAt != nil {
		return domain.OrderIntent{}, fmt.Errorf("order must be SELL LIMIT FOK without expires_at")
	}
	book := marketData.OrderBook
	if len(book.Bids) == 0 || book.BestBid.IsEmpty() || !order.WorstPrice.Equal(book.BestBid) {
		return domain.OrderIntent{}, fmt.Errorf("order.worst_price must equal snapshot best_bid")
	}
	if !evaluation.Evidence.BestBid.Equal(book.BestBid) {
		return domain.OrderIntent{}, fmt.Errorf("evidence.best_bid must equal snapshot best_bid")
	}
	if sign, err := order.Size.Sign(); err != nil || sign <= 0 {
		return domain.OrderIntent{}, fmt.Errorf("order.size must be positive shares")
	}
	if decimalPlaces(order.Size) > request.ExecutionConstraints.SellSizeDecimalPlaces {
		return domain.OrderIntent{}, fmt.Errorf("order.size exceeds %d decimal places", request.ExecutionConstraints.SellSizeDecimalPlaces)
	}
	if comparison, err := order.Size.Compare(trade.AvailableShares); err != nil || comparison > 0 {
		return domain.OrderIntent{}, fmt.Errorf("order.size exceeds the lot's available shares")
	}
	if !book.MinOrderSize.IsEmpty() {
		if comparison, err := order.Size.Compare(book.MinOrderSize); err != nil || comparison < 0 {
			return domain.OrderIntent{}, fmt.Errorf("order.size is below orderbook min_order_size")
		}
	}
	outcomeIndex := trade.OutcomeIndex
	negRisk := trade.NegRisk
	snapshotAt := book.SourceAt.UTC()
	signalAt = signalAt.UTC()
	intent, err := (domain.OrderIntentParams{
		ModelID: request.Context.ModelID, StrategyID: request.Context.StrategyID,
		ExecutionAccountID: request.Context.ExecutionAccountID,
		SignalID:           evaluation.DecisionID, ClientOrderID: clientOrderID(request.CycleID, evaluation.LotID, evaluation.DecisionID),
		Venue: venue, MarketID: trade.MarketID, ConditionID: trade.ConditionID,
		OutcomeIndex: &outcomeIndex, OutcomeName: trade.OutcomeName, TokenID: trade.TokenID,
		TargetLotID: trade.LotID, ExpectedNegRisk: &negRisk,
		MarketSnapshotAt: &snapshotAt, SignalAt: &signalAt,
		Side: domain.SideSell, Type: domain.OrderTypeLimit,
		Price: order.WorstPrice, WorstPrice: order.WorstPrice, Size: order.Size,
		TimeInForce: domain.TimeInForceFOK,
		Metadata: map[string]string{
			"position_exit_cycle_id":    request.CycleID,
			"position_exit_input_id":    request.InputID,
			"position_exit_decision_id": evaluation.DecisionID,
			"position_exit_reason_code": string(evaluation.ReasonCode),
			"strategy_reference_price":  order.WorstPrice.String(),
			"target_lot_id":             trade.LotID,
			"opening_venue_trade_id":    trade.VenueTradeID,
		},
	}).Build()
	return intent, err
}

// requiredHoldReason 检查并要求 d Hold Reason 完整。
func requiredHoldReason(marketData domain.PositionExitMarketData) domain.PositionExitReasonCode {
	if marketData.MarketStatus != domain.PositionExitMarketOpen {
		return domain.PositionExitReasonMarketNotTradable
	}
	if marketData.OrderBook.Status != domain.OrderBookStatusOK || len(marketData.OrderBook.Bids) == 0 {
		return domain.PositionExitReasonInvalidBook
	}
	if marketData.MidPriceHistory.Status != domain.MidPriceHistoryStatusOK {
		return domain.PositionExitReasonStaleData
	}
	return ""
}

// targetForTrade 计算对应业务操作所需的目标数值。
func targetForTrade(trade domain.PositionExitTrade) domain.BookTarget {
	return domain.BookTarget{MarketID: trade.MarketID, ConditionID: trade.ConditionID, OutcomeIndex: trade.OutcomeIndex, TokenID: trade.TokenID}
}

// sortedTargets 稳定排序 ed Targets。
func sortedTargets(byToken map[string]domain.BookTarget) []domain.BookTarget {
	result := make([]domain.BookTarget, 0, len(byToken))
	for _, target := range byToken {
		result = append(result, target)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ConditionID != result[j].ConditionID {
			return result[i].ConditionID < result[j].ConditionID
		}
		if result[i].OutcomeIndex != result[j].OutcomeIndex {
			return result[i].OutcomeIndex < result[j].OutcomeIndex
		}
		return result[i].TokenID < result[j].TokenID
	})
	return result
}

// alignBooks 按目标身份对齐 Books。
func alignBooks(targets []domain.BookTarget, books []domain.OrderBookSnapshot, observedAt time.Time) ([]domain.OrderBookSnapshot, error) {
	byToken := make(map[string]domain.OrderBookSnapshot, len(books))
	for _, book := range books {
		if _, exists := byToken[book.TokenID]; exists {
			return nil, fmt.Errorf("orderbook source returned duplicate token %q", book.TokenID)
		}
		byToken[book.TokenID] = book
	}
	result := make([]domain.OrderBookSnapshot, 0, len(targets))
	for _, target := range targets {
		book, found := byToken[target.TokenID]
		if !found {
			book = domain.OrderBookSnapshot{
				MarketID: target.MarketID, ConditionID: target.ConditionID,
				OutcomeIndex: target.OutcomeIndex, TokenID: target.TokenID,
				Status: domain.OrderBookStatusMissing, ObservedAt: observedAt,
				DepthLimit: domain.StrategyOrderBookDepth, Bids: []domain.PriceLevel{}, Asks: []domain.PriceLevel{},
				ErrorCode: "SOURCE_DID_NOT_RETURN_TOKEN",
			}
		} else if book.MarketID != target.MarketID || book.ConditionID != target.ConditionID || book.OutcomeIndex != target.OutcomeIndex {
			return nil, fmt.Errorf("orderbook token %q has mismatched market identity", target.TokenID)
		}
		if len(book.Bids) > 0 {
			book.BestBid = book.Bids[0].Price
		}
		if len(book.Asks) > 0 {
			book.BestAsk = book.Asks[0].Price
		}
		if book.DepthLimit != domain.StrategyOrderBookDepth {
			return nil, fmt.Errorf("orderbook token %q depth_limit must be %d", target.TokenID, domain.StrategyOrderBookDepth)
		}
		if err := book.Validate(); err != nil {
			return nil, fmt.Errorf("invalid orderbook token %q: %w", target.TokenID, err)
		}
		result = append(result, book)
		delete(byToken, target.TokenID)
	}
	if len(byToken) != 0 {
		return nil, fmt.Errorf("orderbook source returned an unexpected token")
	}
	return result, nil
}

// alignHistories 按目标身份对齐 Histories。
func alignHistories(
	targets []domain.BookTarget,
	histories []domain.MidPriceHistory,
	decisionAt time.Time,
	lookback time.Duration,
	fetchedAt time.Time,
) ([]domain.MidPriceHistory, error) {
	byToken := make(map[string]domain.MidPriceHistory, len(histories))
	for _, history := range histories {
		if _, exists := byToken[history.TokenID]; exists {
			return nil, fmt.Errorf("mid-price source returned duplicate token %q", history.TokenID)
		}
		byToken[history.TokenID] = history
	}
	result := make([]domain.MidPriceHistory, 0, len(targets))
	for _, target := range targets {
		history, found := byToken[target.TokenID]
		if !found {
			history = domain.MidPriceHistory{
				MarketID: target.MarketID, ConditionID: target.ConditionID,
				OutcomeIndex: target.OutcomeIndex, TokenID: target.TokenID,
				Status:      domain.MidPriceHistoryStatusMissing,
				WindowStart: decisionAt.Add(-lookback), WindowEnd: decisionAt,
				FidelitySeconds: 60, Sampling: domain.MidPriceSamplingUpstreamRaw,
				MissingValues:      domain.MidPriceMissingValuePolicyNoFill,
				TimestampSemantics: domain.MidPriceTimestampSemanticsIntervalEndUTC,
				FetchedAt:          fetchedAt, MidPrices: []domain.MidPricePoint{}, ErrorCode: "SOURCE_DID_NOT_RETURN_TOKEN",
			}
		} else if history.MarketID != target.MarketID || history.ConditionID != target.ConditionID || history.OutcomeIndex != target.OutcomeIndex {
			return nil, fmt.Errorf("mid-price token %q has mismatched market identity", target.TokenID)
		}
		if !history.WindowStart.Equal(decisionAt.Add(-lookback)) || !history.WindowEnd.Equal(decisionAt) {
			return nil, fmt.Errorf("mid-price token %q has mismatched 48h window", target.TokenID)
		}
		if err := history.Validate(); err != nil {
			return nil, fmt.Errorf("invalid mid-price token %q: %w", target.TokenID, err)
		}
		result = append(result, history)
		delete(byToken, target.TokenID)
	}
	if len(byToken) != 0 {
		return nil, fmt.Errorf("mid-price source returned an unexpected token")
	}
	return result, nil
}

// normalizeBindings 规范化 Bindings 的字段和表示。
func normalizeBindings(bindings []domain.StrategyExecutionContext) ([]domain.StrategyExecutionContext, error) {
	if len(bindings) == 0 {
		return nil, fmt.Errorf("at least one model/strategy/execution account binding is required")
	}
	result := make([]domain.StrategyExecutionContext, len(bindings))
	seenPairs := make(map[string]struct{}, len(bindings))
	seenAccounts := make(map[string]struct{}, len(bindings))
	for index, binding := range bindings {
		binding = binding.Normalize()
		if err := binding.Validate(); err != nil {
			return nil, fmt.Errorf("binding %d: %w", index, err)
		}
		pair := binding.ModelID + "\x00" + binding.StrategyID
		if _, exists := seenPairs[pair]; exists {
			return nil, fmt.Errorf("duplicate model/strategy binding %q/%q", binding.ModelID, binding.StrategyID)
		}
		if _, exists := seenAccounts[binding.ExecutionAccountID]; exists {
			return nil, fmt.Errorf("execution account %q is bound more than once", binding.ExecutionAccountID)
		}
		seenPairs[pair] = struct{}{}
		seenAccounts[binding.ExecutionAccountID] = struct{}{}
		result[index] = binding
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ModelID != result[j].ModelID {
			return result[i].ModelID < result[j].ModelID
		}
		if result[i].StrategyID != result[j].StrategyID {
			return result[i].StrategyID < result[j].StrategyID
		}
		return result[i].ExecutionAccountID < result[j].ExecutionAccountID
	})
	return result, nil
}

// marketDataForToken 按 token 查找对应的持仓退出行情快照。
func marketDataForToken(values []domain.PositionExitMarketData, tokenID string) (domain.PositionExitMarketData, bool) {
	for _, value := range values {
		if value.TokenID == tokenID {
			return value, true
		}
	}
	return domain.PositionExitMarketData{}, false
}

// decimalPlaces 计算十进制值所需的小数位数。
func decimalPlaces(value domain.Decimal) int {
	text := strings.TrimSpace(value.String())
	if point := strings.IndexByte(text, '.'); point >= 0 {
		return len(strings.TrimRight(text[point+1:], "0"))
	}
	return 0
}

// cycleID 根据稳定业务身份生成幂等标识。
func cycleID(binding domain.StrategyExecutionContext, decisionAt time.Time) string {
	return "position-exit:" + binding.ExecutionAccountID + ":" + decisionAt.UTC().Format("20060102T150405Z")
}

// clientOrderID 根据稳定业务身份生成幂等标识。
func clientOrderID(cycle, lotID, decisionID string) string {
	digest := sha256.Sum256([]byte(cycle + "\x00" + lotID + "\x00" + decisionID))
	return "position-exit-order-" + hex.EncodeToString(digest[:16])
}

// failedRun 根据绑定和错误构建失败的账户运行结果。
func failedRun(binding domain.StrategyExecutionContext, err error) BindingRunResult {
	return BindingRunResult{Context: binding, Status: "FAILED", Intents: []IntentResult{}, Error: err.Error()}
}
