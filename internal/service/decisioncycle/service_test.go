package decisioncycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/execution"
)

// fakePredictionSource 表示后端使用的 fakePredictionSource 类型。
type fakePredictionSource struct {
	snapshot domain.PredictionSnapshot
}

// fakePositionSource 表示后端使用的 fakePositionSource 类型。
type fakePositionSource struct {
	lots []domain.PositionLot
}

// ListOpenLots 返回模拟数据源中的测试列表。
func (source fakePositionSource) ListOpenLots(context.Context, string) ([]domain.PositionLot, error) {
	return source.lots, nil
}

// Snapshot 返回模拟预测快照。
func (source fakePredictionSource) Snapshot(context.Context, time.Time, time.Duration) (domain.PredictionSnapshot, error) {
	return source.snapshot, nil
}

// fakeOrderBookSource 表示后端使用的 fakeOrderBookSource 类型。
type fakeOrderBookSource struct {
	targets []domain.BookTarget
	books   []domain.OrderBookSnapshot
}

// fakeMidPriceHistorySource 表示后端使用的 fakeMidPriceHistorySource 类型。
type fakeMidPriceHistorySource struct {
	targets   []domain.BookTarget
	lookback  time.Duration
	histories []domain.MidPriceHistory
	err       error
	calls     int
}

// Capture 返回模拟行情快照。
func (source *fakeMidPriceHistorySource) Capture(_ context.Context, _ time.Time, lookback time.Duration, targets []domain.BookTarget) ([]domain.MidPriceHistory, error) {
	source.calls++
	source.targets = targets
	source.lookback = lookback
	return source.histories, source.err
}

// Capture 返回模拟行情快照。
func (source *fakeOrderBookSource) Capture(_ context.Context, _ time.Time, targets []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	source.targets = targets
	return source.books, nil
}

// fakeStrategy 表示后端使用的 fakeStrategy 类型。
type fakeStrategy struct {
	request  domain.StrategyDecisionRequest
	response domain.StrategyDecisionResponse
}

// matrixStrategy 表示后端使用的 matrixStrategy 类型。
type matrixStrategy struct {
	requests []domain.StrategyDecisionRequest
}

// contextHijackStrategy 表示后端使用的 contextHijackStrategy 类型。
type contextHijackStrategy struct{}

// Decide 记录策略输入并返回模拟决策。
func (contextHijackStrategy) Decide(_ context.Context, request domain.StrategyDecisionRequest) (domain.StrategyDecisionResponse, error) {
	responseContext := request.Context
	responseContext.ExecutionAccountID = "another-account"
	return domain.StrategyDecisionResponse{
		SchemaVersion: domain.StrategyOutputSchemaVersion,
		CycleID:       request.CycleID,
		InputID:       request.InputID,
		Context:       responseContext,
		DecidedAt:     request.DecisionAt.Add(time.Second),
		Evaluations:   []domain.StrategyEvaluation{},
	}, nil
}

// Decide 记录策略输入并返回模拟决策。
func (strategy *matrixStrategy) Decide(_ context.Context, request domain.StrategyDecisionRequest) (domain.StrategyDecisionResponse, error) {
	strategy.requests = append(strategy.requests, request)
	evaluations := make([]domain.StrategyEvaluation, 0)
	for _, prediction := range request.Predictions {
		for _, outcome := range prediction.Outcomes {
			evaluations = append(evaluations, domain.StrategyEvaluation{
				DecisionID:   request.Context.StrategyID + ":" + prediction.PredictionID + ":" + outcome.TokenID,
				PredictionID: prediction.PredictionID,
				MarketID:     prediction.MarketID,
				ConditionID:  prediction.ConditionID,
				OutcomeIndex: outcome.Index,
				TokenID:      outcome.TokenID,
				Action:       domain.StrategyActionSkip,
				ReasonCode:   inputFailureReason(request, outcome.TokenID),
				Evidence:     domain.StrategyEvidence{Probability: outcome.Probability},
			})
			if evaluations[len(evaluations)-1].ReasonCode == "" {
				evaluations[len(evaluations)-1].ReasonCode = domain.StrategyReasonEdgeTooLow
			}
		}
	}
	return domain.StrategyDecisionResponse{
		SchemaVersion: domain.StrategyOutputSchemaVersion,
		CycleID:       request.CycleID,
		InputID:       request.InputID,
		Context:       request.Context,
		DecidedAt:     request.DecisionAt.Add(time.Second),
		Evaluations:   evaluations,
	}, nil
}

// Decide 记录策略输入并返回模拟决策。
func (strategy *fakeStrategy) Decide(_ context.Context, request domain.StrategyDecisionRequest) (domain.StrategyDecisionResponse, error) {
	strategy.request = request
	strategy.response.CycleID = request.CycleID
	strategy.response.InputID = request.InputID
	strategy.response.Context = request.Context
	return strategy.response, nil
}

// fakeRecorder 表示后端使用的 fakeRecorder 类型。
type fakeRecorder struct {
	inputRecorded  bool
	outputRecorded bool
	inputError     error
	deliveries     []domain.DecisionIntentDelivery
	requeueCalls   int
}

// ClaimInput 模拟幂等认领并保存测试数据。
func (recorder *fakeRecorder) ClaimInput(_ context.Context, request domain.StrategyDecisionRequest) (domain.StrategyDecisionRequest, bool, error) {
	recorder.inputRecorded = true
	return request, true, recorder.inputError
}

// ClaimOutput 模拟幂等认领并保存测试数据。
func (recorder *fakeRecorder) ClaimOutput(_ context.Context, response domain.StrategyDecisionResponse, intents []domain.OrderIntent, submissionEnabled bool) (domain.StrategyDecisionResponse, bool, error) {
	recorder.outputRecorded = true
	if submissionEnabled && recorder.deliveries == nil {
		recorder.deliveries = make([]domain.DecisionIntentDelivery, len(intents))
		for index, intent := range intents {
			recorder.deliveries[index] = domain.DecisionIntentDelivery{
				CycleID: response.CycleID, ClientOrderID: intent.ClientOrderID,
				Sequence: index, Intent: intent, Status: domain.DecisionIntentPending,
			}
		}
	}
	return response, true, nil
}

func (recorder *fakeRecorder) ClaimPendingIntents(_ context.Context, cycleID string, limit int) ([]domain.DecisionIntentDelivery, error) {
	result := make([]domain.DecisionIntentDelivery, 0)
	for index := range recorder.deliveries {
		delivery := &recorder.deliveries[index]
		if delivery.Status != domain.DecisionIntentPending || (cycleID != "" && delivery.CycleID != cycleID) || len(result) >= limit {
			continue
		}
		delivery.Status = domain.DecisionIntentSubmitting
		delivery.Attempt++
		result = append(result, *delivery)
	}
	return result, nil
}

func (recorder *fakeRecorder) RequeueStaleSubmitting(_ context.Context, _ time.Time, limit int) (int, error) {
	recorder.requeueCalls++
	requeued := 0
	for index := range recorder.deliveries {
		if requeued >= limit {
			break
		}
		delivery := &recorder.deliveries[index]
		if delivery.Status != domain.DecisionIntentSubmitting {
			continue
		}
		delivery.Status = domain.DecisionIntentPending
		delivery.ClaimedAt = nil
		requeued++
	}
	return requeued, nil
}

func (recorder *fakeRecorder) CompleteIntent(_ context.Context, clientOrderID string, attempt int, completion domain.DecisionIntentCompletion) error {
	for index := range recorder.deliveries {
		delivery := &recorder.deliveries[index]
		if delivery.ClientOrderID == clientOrderID && delivery.Attempt == attempt {
			delivery.Status = completion.Status
			delivery.OrderID = completion.OrderID
			delivery.OrderStatus = completion.OrderStatus
			delivery.LastError = completion.LastError
			return nil
		}
	}
	return port.ErrDecisionIntentConflict
}

func (recorder *fakeRecorder) ListIntents(_ context.Context, cycleID string) ([]domain.DecisionIntentDelivery, error) {
	result := make([]domain.DecisionIntentDelivery, 0)
	for _, delivery := range recorder.deliveries {
		if delivery.CycleID == cycleID {
			result = append(result, delivery)
		}
	}
	return result, nil
}

// fakeExecutor 表示后端使用的 fakeExecutor 类型。
type fakeExecutor struct {
	intents []domain.OrderIntent
}

// Submit 记录模拟订单提交。
func (executor *fakeExecutor) Submit(_ context.Context, intent domain.OrderIntent) (execution.SubmitResult, error) {
	executor.intents = append(executor.intents, intent)
	return execution.SubmitResult{Order: domain.Order{ID: "order-1", Intent: intent, Status: domain.OrderStatusAcknowledged}, Created: true}, nil
}

type fixedResultExecutor struct {
	result port.OrderSubmitResult
	err    error
}

func (executor fixedResultExecutor) Submit(context.Context, domain.OrderIntent) (port.OrderSubmitResult, error) {
	return executor.result, executor.err
}

// TestRunBuildsFrozenInputAndExecutesRecordedStrategyOutput 验证 Run Builds Frozen Input And Executes Recorded Strategy Output 场景下的行为。
func TestRunBuildsFrozenInputAndExecutesRecordedStrategyOutput(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	bookSource := &fakeOrderBookSource{books: []domain.OrderBookSnapshot{{
		MarketID:     prediction.MarketID,
		ConditionID:  prediction.ConditionID,
		OutcomeIndex: 0,
		TokenID:      prediction.Outcomes[0].TokenID,
		Status:       domain.OrderBookStatusOK,
		SourceAt:     decisionAt.Add(2 * time.Second),
		ObservedAt:   decisionAt.Add(3 * time.Second),
		DepthLimit:   domain.StrategyOrderBookDepth,
		MinOrderSize: "1",
		Bids:         []domain.PriceLevel{{Price: "0.48", Size: "10"}},
		Asks:         []domain.PriceLevel{{Price: "0.50", Size: "20"}},
	}}}
	midPriceSource := &fakeMidPriceHistorySource{histories: []domain.MidPriceHistory{
		validMidPriceHistory(prediction, 0, decisionAt),
	}}
	strategy := &fakeStrategy{response: domain.StrategyDecisionResponse{
		SchemaVersion: domain.StrategyOutputSchemaVersion,
		DecidedAt:     decisionAt.Add(4 * time.Second),
		Evaluations: []domain.StrategyEvaluation{
			{
				DecisionID:   "pred-1:yes",
				PredictionID: prediction.PredictionID,
				MarketID:     prediction.MarketID,
				ConditionID:  prediction.ConditionID,
				OutcomeIndex: 0,
				TokenID:      prediction.Outcomes[0].TokenID,
				Action:       domain.StrategyActionSubmit,
				ReasonCode:   "ENTRY_SIGNAL",
				Evidence: domain.StrategyEvidence{
					Probability: 0.7,
					Edge:        "0.20",
					Metrics: map[string]string{
						"best_ask": "0.50", "near_logdiff_usd": "1.2", "rel_spread": "0.04",
						"MOM": "0.01", "MACD_SIGNAL": "0.02",
					},
				},
				Order: &domain.StrategyOrderParams{
					Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.50", Size: "20", TimeInForce: domain.TimeInForceFOK,
				},
			},
			{
				DecisionID:   "pred-1:no",
				PredictionID: prediction.PredictionID,
				MarketID:     prediction.MarketID,
				ConditionID:  prediction.ConditionID,
				OutcomeIndex: 1,
				TokenID:      prediction.Outcomes[1].TokenID,
				Action:       domain.StrategyActionSkip,
				ReasonCode:   domain.StrategyReasonInvalidBook,
				Evidence:     domain.StrategyEvidence{Probability: 0.3},
			},
		},
	}}
	recorder := &fakeRecorder{}
	executor := &fakeExecutor{}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion:  domain.PredictionSnapshotSchemaVersion,
			SnapshotID:     "predsnap-1",
			DecisionAt:     decisionAt,
			CompletedAfter: decisionAt.Add(-3 * time.Hour),
			GeneratedAt:    decisionAt.Add(time.Second),
			Predictions:    []domain.Prediction{prediction},
		}},
		PositionSource:  fakePositionSource{},
		OrderBookSource: bookSource,
		MidPriceSource:  midPriceSource,
		Strategy:        strategy,
		Recorder:        recorder,
		Executor:        executor,
		SubmitEnabled:   true,
		Bindings:        []domain.StrategyExecutionBinding{testExecutionBinding()},
		Venue:           "polymarket-paper",
		Now:             func() time.Time { return decisionAt.Add(5 * time.Second) },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, err := service.Run(context.Background(), decisionAt)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(result.Runs) != 1 {
		t.Fatalf("runs = %#v, want one binding run", result.Runs)
	}
	run := result.Runs[0]
	if len(bookSource.targets) != 2 || len(midPriceSource.targets) != 2 || midPriceSource.lookback != 48*time.Hour ||
		len(run.Request.OrderBooks) != 2 || len(run.Request.MidPriceHistories) != 2 {
		t.Fatalf("targets = %#v, books = %#v, want both outcome tokens", bookSource.targets, run.Request.OrderBooks)
	}
	if run.Request.OrderBooks[1].Status != domain.OrderBookStatusMissing {
		t.Fatalf("missing book = %#v, want explicit MISSING status", run.Request.OrderBooks[1])
	}
	if run.Request.MidPriceHistories[0].MidPrices[0].P != "0.49" ||
		run.Request.MidPriceHistories[1].Status != domain.MidPriceHistoryStatusMissing {
		t.Fatalf("mid-price histories = %#v", run.Request.MidPriceHistories)
	}
	if !recorder.inputRecorded || !recorder.outputRecorded || len(executor.intents) != 1 {
		t.Fatalf("recorded input/output = %v/%v, executed = %d", recorder.inputRecorded, recorder.outputRecorded, len(executor.intents))
	}
	if len(run.Intents) != 1 || run.Intents[0].DeliveryStatus != domain.DecisionIntentSubmitted || run.Intents[0].DeliveryAttempt != 1 {
		t.Fatalf("durable intent deliveries = %#v", run.Intents)
	}
	intent := executor.intents[0]
	if intent.ModelID != "test" || intent.StrategyID != domain.StrategyIDMultfactorV2 || intent.ExecutionAccountID != "account-test-v2" ||
		intent.OutcomeIndex == nil || *intent.OutcomeIndex != 0 || intent.OutcomeName != "Yes" ||
		intent.ExpectedNegRisk == nil || *intent.ExpectedNegRisk || intent.MarketSnapshotAt == nil ||
		!intent.MarketSnapshotAt.Equal(bookSource.books[0].SourceAt) || intent.SignalAt == nil ||
		!intent.SignalAt.Equal(strategy.response.DecidedAt) || intent.WorstPrice != "0.50" {
		t.Fatalf("execution market context = %#v", intent)
	}
}

// TestRunRoutesTwoAssignedModelsIntoFourWallets 验证两个互不混合的 Market/Model 集合各自复用到 v1/v2 钱包。
func TestRunRoutesTwoAssignedModelsIntoFourWallets(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	echoPrediction := validPrediction(decisionAt)
	echoPrediction.PredictionID = "pred-echo"
	echoPrediction.Model.Name = "echo-producer-v7"
	echoPrediction.MarketID = "market-echo"
	echoPrediction.ConditionID = "condition-echo"
	echoPrediction.Outcomes[0].TokenID = "echo-yes"
	echoPrediction.Outcomes[1].TokenID = "echo-no"
	maskedPrediction := validPrediction(decisionAt)
	maskedPrediction.PredictionID = "pred-masked"
	maskedPrediction.Model.Name = "gemini-3.6-flash"
	maskedPrediction.MarketID = "market-masked"
	maskedPrediction.ConditionID = "condition-masked"
	maskedPrediction.Outcomes[0].TokenID = "masked-yes"
	maskedPrediction.Outcomes[1].TokenID = "masked-no"
	predictions := []domain.Prediction{echoPrediction, maskedPrediction}
	bindings := fourWalletBindings()
	bookSource := &fakeOrderBookSource{}
	midPriceSource := &fakeMidPriceHistorySource{}
	strategy := &matrixStrategy{}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion,
			SnapshotID:    "predsnap-matrix",
			DecisionAt:    decisionAt,
			Predictions:   predictions,
		}},
		PositionSource:  fakePositionSource{},
		OrderBookSource: bookSource,
		MidPriceSource:  midPriceSource,
		Strategy:        strategy,
		Recorder:        &fakeRecorder{},
		Executor:        &fakeExecutor{},
		SubmitEnabled:   true,
		Bindings:        bindings,
		Venue:           "polymarket-paper",
		Now:             func() time.Time { return decisionAt.Add(2 * time.Second) },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, err := service.Run(context.Background(), decisionAt)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(result.Runs) != 4 || len(strategy.requests) != 4 {
		t.Fatalf("runs/requests = %d/%d, want 4/4", len(result.Runs), len(strategy.requests))
	}
	if len(bookSource.targets) != 4 {
		t.Fatalf("shared orderbook targets = %d, want four assigned outcome tokens", len(bookSource.targets))
	}
	if len(midPriceSource.targets) != 4 {
		t.Fatalf("shared mid-price targets = %d, want four assigned outcome tokens", len(midPriceSource.targets))
	}
	seenAccounts := make(map[string]struct{}, 4)
	for _, request := range strategy.requests {
		if len(request.Predictions) != 1 || request.Predictions[0].Model.Name != request.Context.ModelID {
			t.Fatalf("request mixes model predictions: %#v", request)
		}
		if request.Context.ModelID == "echo" && request.Predictions[0].MarketID != "market-echo" {
			t.Fatalf("echo received another model's market: %#v", request.Predictions)
		}
		if request.Context.ModelID == "gemini_masked" && request.Predictions[0].MarketID != "market-masked" {
			t.Fatalf("gemini_masked received another model's market: %#v", request.Predictions)
		}
		if request.CycleID != request.Context.ExecutionAccountID+":"+decisionAt.Format("20060102T150405Z") {
			t.Fatalf("cycle_id = %q", request.CycleID)
		}
		seenAccounts[request.Context.ExecutionAccountID] = struct{}{}
	}
	if len(seenAccounts) != 4 {
		t.Fatalf("execution accounts = %#v, want four isolated accounts", seenAccounts)
	}
	if predictions[0].Model.Name != "echo-producer-v7" || predictions[1].Model.Name != "gemini-3.6-flash" {
		t.Fatalf("source snapshot was mutated: %#v", predictions)
	}
}

func TestRunStillCallsAllFourWalletsWhenOneModelHasNoMarket(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	prediction.Model.Name = "echo-producer-v7"
	strategy := &matrixStrategy{}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion,
			SnapshotID:    "predsnap-one-model",
			DecisionAt:    decisionAt,
			Predictions:   []domain.Prediction{prediction},
		}},
		PositionSource:  fakePositionSource{},
		OrderBookSource: &fakeOrderBookSource{},
		MidPriceSource:  &fakeMidPriceHistorySource{},
		Strategy:        strategy,
		Recorder:        &fakeRecorder{},
		Bindings:        fourWalletBindings(),
		Venue:           "polymarket-paper",
		Now:             func() time.Time { return decisionAt.Add(2 * time.Second) },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, err := service.Run(context.Background(), decisionAt)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(result.Runs) != 4 || len(strategy.requests) != 4 {
		t.Fatalf("runs/requests = %d/%d, want 4/4", len(result.Runs), len(strategy.requests))
	}
	maskedCalls := 0
	for _, request := range strategy.requests {
		if request.Context.ModelID != "gemini_masked" {
			continue
		}
		maskedCalls++
		if len(request.Predictions) != 0 {
			t.Fatalf("empty masked route received predictions: %#v", request.Predictions)
		}
	}
	if maskedCalls != 2 {
		t.Fatalf("gemini_masked calls = %d, want 2", maskedCalls)
	}
}

func TestRunRejectsMultipleProbabilitiesForSameMarketAndModel(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	first := validPrediction(decisionAt)
	first.Model.Name = "echo-producer-v7"
	second := first
	second.PredictionID = "pred-2"
	second.SourceJobID = "job-2"
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion,
			SnapshotID:    "predsnap-duplicate-market-model",
			DecisionAt:    decisionAt,
			Predictions:   []domain.Prediction{first, second},
		}},
		PositionSource:  fakePositionSource{},
		OrderBookSource: &fakeOrderBookSource{},
		MidPriceSource:  &fakeMidPriceHistorySource{},
		Strategy:        &matrixStrategy{},
		Recorder:        &fakeRecorder{},
		Bindings: []domain.StrategyExecutionBinding{{
			PredictionModelID: "echo-producer-v7", ModelID: "echo",
			StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "main",
		}},
		Venue: "polymarket-paper",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := service.Run(context.Background(), decisionAt); err == nil ||
		!strings.Contains(err.Error(), "multiple probabilities for market") {
		t.Fatalf("Run() error = %v", err)
	}
}

// TestRunFailsClosedWhenInputCannotBeRecorded 验证 Run Fails Closed When Input Cannot Be Recorded 场景下的行为。
func TestRunFailsClosedWhenInputCannotBeRecorded(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	recorder := &fakeRecorder{inputError: errors.New("disk unavailable")}
	strategy := &fakeStrategy{}
	executor := &fakeExecutor{}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion,
			SnapshotID:    "predsnap-1",
			DecisionAt:    decisionAt,
			Predictions:   []domain.Prediction{prediction},
		}},
		PositionSource:  fakePositionSource{},
		OrderBookSource: &fakeOrderBookSource{},
		MidPriceSource:  &fakeMidPriceHistorySource{},
		Strategy:        strategy,
		Recorder:        recorder,
		Executor:        executor,
		SubmitEnabled:   true,
		Bindings:        []domain.StrategyExecutionBinding{testExecutionBinding()},
		Venue:           "polymarket-paper",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := service.Run(context.Background(), decisionAt); err == nil {
		t.Fatal("Run() error = nil, want recorder failure")
	}
	if strategy.request.CycleID != "" || len(executor.intents) != 0 {
		t.Fatal("strategy or executor was called after recorder failure")
	}
}

// TestRunRejectsStrategyResponseThatOmitsAnOutcome 验证 Run Rejects Strategy Response That Omits An Outcome 场景下的行为。
func TestRunRejectsStrategyResponseThatOmitsAnOutcome(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	strategy := &fakeStrategy{response: domain.StrategyDecisionResponse{
		SchemaVersion: domain.StrategyOutputSchemaVersion,
		DecidedAt:     decisionAt.Add(time.Second),
		Evaluations: []domain.StrategyEvaluation{{
			DecisionID:   "pred-1:yes",
			PredictionID: prediction.PredictionID,
			MarketID:     prediction.MarketID,
			ConditionID:  prediction.ConditionID,
			OutcomeIndex: 0,
			TokenID:      prediction.Outcomes[0].TokenID,
			Action:       domain.StrategyActionSkip,
			ReasonCode:   "EDGE_TOO_LOW",
			Evidence:     domain.StrategyEvidence{Probability: 0.7},
		}},
	}}
	recorder := &fakeRecorder{}
	executor := &fakeExecutor{}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion,
			SnapshotID:    "predsnap-1",
			DecisionAt:    decisionAt,
			Predictions:   []domain.Prediction{prediction},
		}},
		PositionSource:  fakePositionSource{},
		OrderBookSource: &fakeOrderBookSource{},
		MidPriceSource:  &fakeMidPriceHistorySource{},
		Strategy:        strategy,
		Recorder:        recorder,
		Executor:        executor,
		SubmitEnabled:   true,
		Bindings:        []domain.StrategyExecutionBinding{testExecutionBinding()},
		Venue:           "polymarket-paper",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := service.Run(context.Background(), decisionAt); !errors.Is(err, ErrInvalidStrategy) {
		t.Fatalf("Run() error = %v, want ErrInvalidStrategy", err)
	}
	if recorder.outputRecorded || len(executor.intents) != 0 {
		t.Fatal("incomplete strategy response was recorded or executed")
	}
}

// TestRunRejectsStrategyResponseThatChangesExecutionAccount 验证 Run Rejects Strategy Response That Changes Execution Account 场景下的行为。
func TestRunRejectsStrategyResponseThatChangesExecutionAccount(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion,
			SnapshotID:    "predsnap-empty",
			DecisionAt:    decisionAt,
			Predictions:   []domain.Prediction{},
		}},
		PositionSource:  fakePositionSource{},
		OrderBookSource: &fakeOrderBookSource{},
		MidPriceSource:  &fakeMidPriceHistorySource{},
		Strategy:        contextHijackStrategy{},
		Recorder:        &fakeRecorder{},
		Executor:        &fakeExecutor{},
		SubmitEnabled:   true,
		Bindings:        []domain.StrategyExecutionBinding{testExecutionBinding()},
		Venue:           "polymarket-paper",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := service.Run(context.Background(), decisionAt); !errors.Is(err, ErrInvalidStrategy) {
		t.Fatalf("Run() error = %v, want ErrInvalidStrategy", err)
	}
}

func TestRunRejectsFutureStrategyDecisionTime(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	strategy := &fakeStrategy{response: domain.StrategyDecisionResponse{
		SchemaVersion: domain.StrategyOutputSchemaVersion,
		DecidedAt:     decisionAt.Add(time.Hour),
		Evaluations:   []domain.StrategyEvaluation{},
		Exits:         []domain.StrategyExit{},
	}}
	recorder := &fakeRecorder{}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion,
			SnapshotID:    "predsnap-empty",
			DecisionAt:    decisionAt,
			Predictions:   []domain.Prediction{},
		}},
		PositionSource:  fakePositionSource{},
		OrderBookSource: &fakeOrderBookSource{},
		MidPriceSource:  &fakeMidPriceHistorySource{},
		Strategy:        strategy,
		Recorder:        recorder,
		Executor:        &fakeExecutor{},
		SubmitEnabled:   true,
		Bindings:        []domain.StrategyExecutionBinding{testExecutionBinding()},
		Venue:           "polymarket-paper",
		Now:             func() time.Time { return decisionAt.Add(2 * time.Second) },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := service.Run(context.Background(), decisionAt); !errors.Is(err, ErrInvalidStrategy) {
		t.Fatalf("Run() error = %v, want ErrInvalidStrategy", err)
	}
	if recorder.outputRecorded {
		t.Fatal("future strategy response was recorded")
	}
}

// TestNewRejectsDuplicateAccountBinding 验证 New Rejects Duplicate Account Binding 场景下的行为。
func TestNewRejectsDuplicateAccountBinding(t *testing.T) {
	first := testExecutionBinding()
	second := first
	second.ModelID = "another-model"
	_, err := New(Params{
		PredictionSource: fakePredictionSource{},
		PositionSource:   fakePositionSource{},
		OrderBookSource:  &fakeOrderBookSource{},
		MidPriceSource:   &fakeMidPriceHistorySource{},
		Strategy:         &fakeStrategy{},
		Recorder:         &fakeRecorder{},
		Executor:         &fakeExecutor{},
		SubmitEnabled:    true,
		Bindings:         []domain.StrategyExecutionBinding{first, second},
		Venue:            "polymarket-paper",
	})
	if err == nil {
		t.Fatal("New() error = nil, want duplicate execution account rejection")
	}
}

func TestNewRejectsPredictionModelRoutedToMultipleLogicalModels(t *testing.T) {
	_, err := New(Params{
		PredictionSource: fakePredictionSource{},
		PositionSource:   fakePositionSource{},
		OrderBookSource:  &fakeOrderBookSource{},
		MidPriceSource:   &fakeMidPriceHistorySource{},
		Strategy:         &fakeStrategy{},
		Recorder:         &fakeRecorder{},
		Bindings: []domain.StrategyExecutionBinding{
			{PredictionModelID: "producer-a", ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "main"},
			{PredictionModelID: "producer-a", ModelID: "gemini_masked", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "wallet-2"},
		},
		Venue: "polymarket-paper",
	})
	if err == nil || !strings.Contains(err.Error(), "multiple logical models") {
		t.Fatalf("New() error = %v", err)
	}
}

// TestRunRejectsNonBoundaryTime 验证 Run Rejects Non Boundary Time 场景下的行为。
func TestRunRejectsNonBoundaryTime(t *testing.T) {
	service, err := New(Params{
		PredictionSource: fakePredictionSource{},
		PositionSource:   fakePositionSource{},
		OrderBookSource:  &fakeOrderBookSource{},
		MidPriceSource:   &fakeMidPriceHistorySource{},
		Strategy:         &fakeStrategy{},
		Recorder:         &fakeRecorder{},
		Executor:         &fakeExecutor{},
		SubmitEnabled:    true,
		Bindings:         []domain.StrategyExecutionBinding{testExecutionBinding()},
		Venue:            "polymarket-paper",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := service.Run(context.Background(), time.Date(2026, 8, 18, 4, 21, 0, 0, time.UTC)); !errors.Is(err, ErrInvalidBoundary) {
		t.Fatalf("Run() error = %v, want ErrInvalidBoundary", err)
	}
}

// TestBuildEntryIntentRequiresUsableMidPriceHistory 验证 Build Entry Intent Requires Usable Mid Price History 场景下的行为。
func TestBuildEntryIntentRequiresUsableMidPriceHistory(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	history := validMidPriceHistory(prediction, 0, decisionAt)
	history.Status = domain.MidPriceHistoryStatusPartial
	request := domain.StrategyDecisionRequest{
		Context:              testBinding(),
		DecisionAt:           decisionAt,
		Predictions:          []domain.Prediction{prediction},
		ExecutionConstraints: domain.DefaultStrategyExecutionConstraints(),
		OrderBooks: []domain.OrderBookSnapshot{{
			MarketID: prediction.MarketID, ConditionID: prediction.ConditionID, OutcomeIndex: 0,
			TokenID: prediction.Outcomes[0].TokenID, Status: domain.OrderBookStatusOK,
			SourceAt: decisionAt, ObservedAt: decisionAt, DepthLimit: domain.StrategyOrderBookDepth,
			Bids: []domain.PriceLevel{{Price: "0.48", Size: "10"}},
			Asks: []domain.PriceLevel{{Price: "0.50", Size: "10"}},
		}},
		MidPriceHistories: []domain.MidPriceHistory{history},
	}
	evaluation := domain.StrategyEvaluation{
		DecisionID: "decision-1", PredictionID: prediction.PredictionID,
		MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
		OutcomeIndex: 0, TokenID: prediction.Outcomes[0].TokenID,
		Order: &domain.StrategyOrderParams{
			Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.50", Size: "10", TimeInForce: domain.TimeInForceFOK,
		},
	}
	if _, err := buildEntryIntent(request, decisionAt.Add(time.Second), evaluation, "polymarket"); err == nil {
		t.Fatal("buildEntryIntent() error = nil, want unusable mid-price history rejection")
	}
}

func TestMultfactorV1EntryDoesNotRequireMidPriceHistory(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	request := domain.StrategyDecisionRequest{
		Context: domain.StrategyExecutionContext{
			ModelID: "test", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "account-v1",
		},
		DecisionAt: decisionAt, Predictions: []domain.Prediction{prediction},
		ExecutionConstraints: domain.DefaultStrategyExecutionConstraints(),
		OrderBooks: []domain.OrderBookSnapshot{{
			MarketID: prediction.MarketID, ConditionID: prediction.ConditionID, OutcomeIndex: 0,
			TokenID: prediction.Outcomes[0].TokenID, Status: domain.OrderBookStatusOK,
			SourceAt: decisionAt, ObservedAt: decisionAt, DepthLimit: domain.StrategyOrderBookDepth,
			Bids: []domain.PriceLevel{{Price: "0.48", Size: "10"}},
			Asks: []domain.PriceLevel{{Price: "0.50", Size: "10"}},
		}},
		MidPriceHistories: []domain.MidPriceHistory{{
			TokenID: prediction.Outcomes[0].TokenID, Status: domain.MidPriceHistoryStatusPartial,
		}},
	}
	evaluation := domain.StrategyEvaluation{
		DecisionID: "decision-v1", PredictionID: prediction.PredictionID,
		MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
		OutcomeIndex: 0, TokenID: prediction.Outcomes[0].TokenID,
		Order: &domain.StrategyOrderParams{
			Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.50", Size: "10", TimeInForce: domain.TimeInForceFOK,
		},
	}
	if _, err := buildEntryIntent(request, decisionAt.Add(time.Second), evaluation, "polymarket"); err != nil {
		t.Fatalf("buildEntryIntent() error = %v, want v1 to ignore hourly history", err)
	}
	if reason := inputFailureReason(request, prediction.Outcomes[0].TokenID); reason != "" {
		t.Fatalf("inputFailureReason() = %q, want no v1 history failure", reason)
	}
}

func TestV1OnlyCycleDoesNotCallMidPriceSource(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	historySource := &fakeMidPriceHistorySource{err: errors.New("history source must not be called")}
	strategy := &matrixStrategy{}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion,
			SnapshotID:    "predsnap-v1", DecisionAt: decisionAt,
			Predictions: []domain.Prediction{prediction},
		}},
		PositionSource: fakePositionSource{}, OrderBookSource: &fakeOrderBookSource{},
		MidPriceSource: historySource, Strategy: strategy, Recorder: &fakeRecorder{},
		Bindings: []domain.StrategyExecutionBinding{{
			ModelID: "test", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "account-v1",
		}},
		Venue: "polymarket", Now: func() time.Time { return decisionAt.Add(time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Run(context.Background(), decisionAt)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if historySource.calls != 0 || len(strategy.requests) != 1 || len(result.Runs) != 1 {
		t.Fatalf("history calls=%d strategy requests=%d runs=%d", historySource.calls, len(strategy.requests), len(result.Runs))
	}
	histories := strategy.requests[0].MidPriceHistories
	if len(histories) != 2 || histories[0].Status != domain.MidPriceHistoryStatusMissing ||
		histories[0].ErrorCode != "NOT_REQUIRED_FOR_MULTFACTOR_V1" {
		t.Fatalf("v1 history placeholders = %#v", histories)
	}
}

func TestMixedCycleHistoryFailureDoesNotBlockV1Binding(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	historySource := &fakeMidPriceHistorySource{err: errors.New("history unavailable")}
	strategy := &matrixStrategy{}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion,
			SnapshotID:    "predsnap-mixed", DecisionAt: decisionAt,
			Predictions: []domain.Prediction{prediction},
		}},
		PositionSource: fakePositionSource{}, OrderBookSource: &fakeOrderBookSource{},
		MidPriceSource: historySource, Strategy: strategy, Recorder: &fakeRecorder{},
		Bindings: []domain.StrategyExecutionBinding{
			{ModelID: "test", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "account-v1"},
			{ModelID: "test", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "account-v2"},
		},
		Venue: "polymarket", Now: func() time.Time { return decisionAt.Add(time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, runErr := service.Run(context.Background(), decisionAt)
	if runErr == nil {
		t.Fatal("Run() error = nil, want v2 history failure")
	}
	if historySource.calls != 1 || len(strategy.requests) != 1 ||
		strategy.requests[0].Context.StrategyID != domain.StrategyIDMultfactorV1 || len(result.Runs) != 2 ||
		result.Runs[0].Error != nil || result.Runs[1].Error == nil || result.Runs[1].PredictionCount != 1 {
		t.Fatalf("mixed result=%#v history calls=%d strategy requests=%#v", result, historySource.calls, strategy.requests)
	}
}

func TestUnknownStrategyFailsClosedBeforeStrategyOutputCanCreateAnIntent(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	request := domain.StrategyDecisionRequest{
		SchemaVersion: domain.StrategyInputSchemaVersion,
		CycleID:       "cycle-unknown",
		InputID:       "input-unknown",
		Context: domain.StrategyExecutionContext{
			ModelID: "test", StrategyID: "multfactor_v3", ExecutionAccountID: "account-1",
		},
		DecisionAt: decisionAt,
	}
	response := domain.StrategyDecisionResponse{
		SchemaVersion: domain.StrategyOutputSchemaVersion,
		CycleID:       request.CycleID,
		InputID:       request.InputID,
		Context:       request.Context,
		DecidedAt:     decisionAt.Add(time.Second),
		Evaluations:   []domain.StrategyEvaluation{},
		Exits:         []domain.StrategyExit{},
	}
	if _, err := validateResponse(request, response, "polymarket"); err == nil || !errors.Is(err, ErrInvalidStrategy) {
		t.Fatalf("validateResponse() error = %v, want unsupported strategy rejection", err)
	}
}

func TestStrategySpecificEvidenceMetricsAndUniverseReason(t *testing.T) {
	request := domain.StrategyDecisionRequest{Context: domain.StrategyExecutionContext{
		ModelID: "test", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "account-v1",
	}, OrderBooks: []domain.OrderBookSnapshot{{
		TokenID: "token", Status: domain.OrderBookStatusOK,
		Asks: []domain.PriceLevel{{Price: "0.50", Size: "10"}},
	}}}
	evidence := domain.StrategyEvidence{Metrics: map[string]string{
		"best_ask": "0.50", "near_logdiff_usd": "2.40", "rel_spread": "0.05",
	}}
	if err := validateEvidenceMetrics(evidence, request, "token", true); err != nil {
		t.Fatalf("v1 evidence error = %v", err)
	}
	evidence.Metrics["MOM"] = "-0.01"
	if err := validateEvidenceMetrics(evidence, request, "token", true); err != nil {
		t.Fatalf("optional v1 hourly evidence error = %v", err)
	}
	if !validSkipReason(domain.StrategyReasonOutsideUniverse) {
		t.Fatal("OUTSIDE_STRATEGY_UNIVERSE is not accepted as an auditable SKIP reason")
	}
}

func TestDecisionIntentCompletionRequiresDurableExecutionOwnership(t *testing.T) {
	if _, complete := decisionIntentCompletion(port.OrderSubmitResult{}, errors.New("database unavailable")); complete {
		t.Fatal("delivery without a durable execution order was marked terminal")
	}
	cases := []struct {
		status domain.OrderStatus
		want   domain.DecisionIntentDeliveryStatus
	}{
		{status: domain.OrderStatusRejected, want: domain.DecisionIntentFailed},
		{status: domain.OrderStatusUnknown, want: domain.DecisionIntentUnknown},
		{status: domain.OrderStatusManualReview, want: domain.DecisionIntentUnknown},
		{status: domain.OrderStatusAcknowledged, want: domain.DecisionIntentSubmitted},
		{status: domain.OrderStatusFilled, want: domain.DecisionIntentSubmitted},
	}
	for _, testCase := range cases {
		completion, complete := decisionIntentCompletion(port.OrderSubmitResult{Order: domain.Order{
			ID: "order-1", Status: testCase.status,
		}}, errors.New("execution returned an audited result"))
		if !complete || completion.Status != testCase.want || completion.OrderID != "order-1" || completion.LastError == "" {
			t.Fatalf("status %s completion = %#v, complete=%v", testCase.status, completion, complete)
		}
	}
}

func TestDeliverPendingSurfacesReplayedRejectedAndUnknownOrders(t *testing.T) {
	for _, status := range []domain.OrderStatus{domain.OrderStatusRejected, domain.OrderStatusUnknown} {
		t.Run(string(status), func(t *testing.T) {
			intent := domain.OrderIntent{ClientOrderID: "client-1"}
			recorder := &fakeRecorder{deliveries: []domain.DecisionIntentDelivery{{
				CycleID: "cycle-1", ClientOrderID: intent.ClientOrderID,
				Intent: intent, Status: domain.DecisionIntentPending,
			}}}
			service := &Service{recorder: recorder, executor: fixedResultExecutor{result: port.OrderSubmitResult{
				Order: domain.Order{ID: "order-1", Status: status},
			}}}
			results, err := service.deliverPending(context.Background(), "cycle-1")
			if err == nil || len(results) != 1 || results[0].Error == nil {
				t.Fatalf("deliverPending() results=%#v error=%v", results, err)
			}
			want := domain.DecisionIntentFailed
			if status == domain.OrderStatusUnknown {
				want = domain.DecisionIntentUnknown
			}
			if results[0].DeliveryStatus != want || recorder.deliveries[0].Status != want {
				t.Fatalf("delivery status=%s stored=%s want=%s", results[0].DeliveryStatus, recorder.deliveries[0].Status, want)
			}
		})
	}
}

func TestDeliverPendingLeavesMissingOrderOwnershipLeasedAndUnhealthy(t *testing.T) {
	intent := domain.OrderIntent{ClientOrderID: "client-1"}
	recorder := &fakeRecorder{deliveries: []domain.DecisionIntentDelivery{{
		CycleID: "cycle-1", ClientOrderID: intent.ClientOrderID,
		Intent: intent, Status: domain.DecisionIntentPending,
	}}}
	service := &Service{recorder: recorder, executor: fixedResultExecutor{}}
	results, err := service.deliverPending(context.Background(), "cycle-1")
	if err == nil || len(results) != 1 || results[0].Error == nil ||
		recorder.deliveries[0].Status != domain.DecisionIntentSubmitting {
		t.Fatalf("deliverPending() results=%#v stored=%#v error=%v", results, recorder.deliveries, err)
	}
}

func TestRecoverStartupDrainsMoreThanOneClaimBatchBeforeNewSchedule(t *testing.T) {
	deliveries := make([]domain.DecisionIntentDelivery, 201)
	for index := range deliveries {
		deliveries[index] = domain.DecisionIntentDelivery{
			CycleID:       "old-cycle",
			ClientOrderID: fmt.Sprintf("client-%03d", index),
			Intent:        domain.OrderIntent{ClientOrderID: fmt.Sprintf("client-%03d", index)},
			Status:        domain.DecisionIntentSubmitting,
			Attempt:       1,
		}
	}
	recorder := &fakeRecorder{deliveries: deliveries}
	executor := &fakeExecutor{}
	service := &Service{
		recorder: recorder, executor: executor, submitEnabled: true,
		now: func() time.Time { return time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC) },
	}
	if err := service.RecoverStartup(context.Background()); err != nil {
		t.Fatalf("RecoverStartup() error = %v", err)
	}
	if recorder.requeueCalls != 3 || len(executor.intents) != 201 {
		t.Fatalf("requeue calls=%d submitted=%d", recorder.requeueCalls, len(executor.intents))
	}
	for _, delivery := range recorder.deliveries {
		if delivery.Status != domain.DecisionIntentSubmitted {
			t.Fatalf("delivery %q status = %s", delivery.ClientOrderID, delivery.Status)
		}
	}
}

// TestValidateResponseBuildsLotAddressedFOKExit 验证 Validate Response Builds Lot Addressed FOK Exit 场景下的行为。
func TestValidateResponseBuildsLotAddressedFOKExit(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	request := domain.StrategyDecisionRequest{
		SchemaVersion: domain.StrategyInputSchemaVersion, CycleID: "cycle-1", InputID: "input-1",
		Context: testBinding(), DecisionAt: decisionAt,
		Positions: []domain.StrategyPositionLot{{
			LotID: "lot-1", MarketID: "market-1", ConditionID: "condition-1",
			OutcomeIndex: 0, OutcomeName: "Yes", TokenID: "yes-token", NegRisk: true,
			EnteredAt: decisionAt.Add(-49 * time.Hour), Shares: "12.50", EntryPrice: "0.40",
		}},
		OrderBooks: []domain.OrderBookSnapshot{{
			MarketID: "market-1", ConditionID: "condition-1", OutcomeIndex: 0, TokenID: "yes-token",
			Status: domain.OrderBookStatusOK, SourceAt: decisionAt, ObservedAt: decisionAt,
			DepthLimit: domain.StrategyOrderBookDepth, MinOrderSize: "1",
			Bids: []domain.PriceLevel{{Price: "0.49", Size: "20"}},
			Asks: []domain.PriceLevel{{Price: "0.50", Size: "20"}},
		}},
		ExecutionConstraints: domain.DefaultStrategyExecutionConstraints(),
	}
	response := domain.StrategyDecisionResponse{
		SchemaVersion: domain.StrategyOutputSchemaVersion, CycleID: request.CycleID, InputID: request.InputID,
		Context: request.Context, DecidedAt: decisionAt.Add(time.Second), Evaluations: []domain.StrategyEvaluation{},
		Exits: []domain.StrategyExit{{
			DecisionID: "exit-1", LotID: "lot-1", TokenID: "yes-token", ReasonCode: domain.StrategyReasonHold48H,
			Order: &domain.StrategyOrderParams{
				Side: domain.SideSell, Type: domain.OrderTypeLimit, WorstPrice: "0.49", Size: "12.50", TimeInForce: domain.TimeInForceFOK,
			},
		}},
	}
	intents, err := validateResponse(request, response, "polymarket")
	if err != nil {
		t.Fatalf("validateResponse() error = %v", err)
	}
	if len(intents) != 1 || intents[0].Side != domain.SideSell || intents[0].TargetLotID != "lot-1" ||
		intents[0].ExpectedNegRisk == nil || !*intents[0].ExpectedNegRisk || intents[0].TimeInForce != domain.TimeInForceFOK {
		t.Fatalf("exit intents = %#v", intents)
	}
}

// validPrediction 构建测试使用的合法输入。
func validPrediction(decisionAt time.Time) domain.Prediction {
	return domain.Prediction{
		PredictionID: "pred-1",
		SourceJobID:  "job-1",
		SandboxID:    "sandbox-1",
		MarketID:     "market-1",
		ConditionID:  "condition-1",
		Question:     "Will it happen?",
		Domains:      []string{"World/Geopolitics"},
		Outcomes: []domain.PredictionOutcome{
			{Index: 0, Name: "Yes", TokenID: "yes-token", Probability: 0.7},
			{Index: 1, Name: "No", TokenID: "no-token", Probability: 0.3},
		},
		PredictionAsOf: decisionAt.Add(-30 * time.Minute),
		CompletedAt:    decisionAt.Add(-10 * time.Minute),
		AvailableAt:    decisionAt.Add(-9 * time.Minute),
		Model:          domain.PredictionModel{Name: "test"},
	}
}

// validMidPriceHistory 构建测试使用的合法输入。
func validMidPriceHistory(prediction domain.Prediction, outcomeIndex int, decisionAt time.Time) domain.MidPriceHistory {
	windowStart := decisionAt.Add(-48 * time.Hour)
	first := windowStart.Add(time.Minute)
	last := decisionAt.Add(-time.Minute)
	return domain.MidPriceHistory{
		MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
		OutcomeIndex: outcomeIndex, TokenID: prediction.Outcomes[outcomeIndex].TokenID,
		Status: domain.MidPriceHistoryStatusOK, WindowStart: windowStart, WindowEnd: decisionAt,
		FidelitySeconds: 60, Sampling: domain.MidPriceSamplingUpstreamRaw,
		MissingValues: domain.MidPriceMissingValuePolicyNoFill, TimestampSemantics: domain.MidPriceTimestampSemanticsIntervalEndUTC,
		FetchedAt: decisionAt.Add(3 * time.Second), CoverageStart: first, CoverageEnd: last,
		MidPrices: []domain.MidPricePoint{{IntervalEndAt: first, P: "0.49"}, {IntervalEndAt: last, P: "0.50"}},
	}
}

// testBinding 实现当前测试场景所需的辅助行为。
func testBinding() domain.StrategyExecutionContext {
	return domain.StrategyExecutionContext{
		ModelID:            "test",
		StrategyID:         "strategy-v2",
		ExecutionAccountID: "account-test-v2",
	}
}

func testExecutionBinding() domain.StrategyExecutionBinding {
	context := testBinding()
	return domain.StrategyExecutionBinding{
		ModelID:            context.ModelID,
		StrategyID:         context.StrategyID,
		ExecutionAccountID: context.ExecutionAccountID,
	}
}

func fourWalletBindings() []domain.StrategyExecutionBinding {
	return []domain.StrategyExecutionBinding{
		{PredictionModelID: "echo-producer-v7", ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "main"},
		{PredictionModelID: "echo-producer-v7", ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet-1"},
		{PredictionModelID: "gemini-3.6-flash", ModelID: "gemini_masked", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "wallet-2"},
		{PredictionModelID: "gemini-3.6-flash", ModelID: "gemini_masked", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet-3"},
	}
}
