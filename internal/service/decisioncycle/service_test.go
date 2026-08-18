package decisioncycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
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
}

// Capture 返回模拟行情快照。
func (source *fakeMidPriceHistorySource) Capture(
	_ context.Context,
	_ time.Time,
	lookback time.Duration,
	targets []domain.BookTarget,
) ([]domain.MidPriceHistory, error) {
	source.targets = targets
	source.lookback = lookback
	return source.histories, nil
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
}

// ClaimInput 模拟幂等认领并保存测试数据。
func (recorder *fakeRecorder) ClaimInput(_ context.Context, request domain.StrategyDecisionRequest) (domain.StrategyDecisionRequest, bool, error) {
	recorder.inputRecorded = true
	return request, true, recorder.inputError
}

// ClaimOutput 模拟幂等认领并保存测试数据。
func (recorder *fakeRecorder) ClaimOutput(_ context.Context, response domain.StrategyDecisionResponse) (domain.StrategyDecisionResponse, bool, error) {
	recorder.outputRecorded = true
	return response, true, nil
}

// fakeExecutor 表示后端使用的 fakeExecutor 类型。
type fakeExecutor struct {
	intents []domain.OrderIntent
}

// Submit 记录模拟订单提交。
func (executor *fakeExecutor) Submit(_ context.Context, intent domain.OrderIntent) (execution.SubmitResult, error) {
	executor.intents = append(executor.intents, intent)
	return execution.SubmitResult{Order: domain.Order{ID: "order-1", Intent: intent}, Created: true}, nil
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
		Bindings:        []domain.StrategyExecutionContext{testBinding()},
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
	intent := executor.intents[0]
	if intent.ModelID != "test" || intent.StrategyID != domain.StrategyIDMultfactorV2 || intent.ExecutionAccountID != "account-test-v2" ||
		intent.OutcomeIndex == nil || *intent.OutcomeIndex != 0 || intent.OutcomeName != "Yes" ||
		intent.ExpectedNegRisk == nil || *intent.ExpectedNegRisk || intent.MarketSnapshotAt == nil ||
		!intent.MarketSnapshotAt.Equal(bookSource.books[0].SourceAt) || intent.SignalAt == nil ||
		!intent.SignalAt.Equal(strategy.response.DecidedAt) || intent.WorstPrice != "0.50" {
		t.Fatalf("execution market context = %#v", intent)
	}
}

// TestRunExpandsThreeModelsAndTwoStrategiesIntoSixAccounts 验证 Run Expands Three Models And Two Strategies Into Six Accounts 场景下的行为。
func TestRunExpandsThreeModelsAndTwoStrategiesIntoSixAccounts(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	models := []string{"model-a", "model-b", "model-c"}
	predictions := make([]domain.Prediction, 0, len(models))
	bindings := make([]domain.StrategyExecutionContext, 0, 6)
	for _, modelID := range models {
		prediction := validPrediction(decisionAt)
		prediction.PredictionID = "pred-" + modelID
		prediction.Model.Name = modelID
		predictions = append(predictions, prediction)
		for _, strategyID := range []string{"strategy-v1", "strategy-v2"} {
			bindings = append(bindings, domain.StrategyExecutionContext{
				ModelID:            modelID,
				StrategyID:         strategyID,
				ExecutionAccountID: "account-" + modelID + "-" + strategyID,
			})
		}
	}
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
	if len(result.Runs) != 6 || len(strategy.requests) != 6 {
		t.Fatalf("runs/requests = %d/%d, want 6/6", len(result.Runs), len(strategy.requests))
	}
	if len(bookSource.targets) != 2 {
		t.Fatalf("shared orderbook targets = %d, want two deduplicated outcome tokens", len(bookSource.targets))
	}
	if len(midPriceSource.targets) != 2 {
		t.Fatalf("shared mid-price targets = %d, want two deduplicated outcome tokens", len(midPriceSource.targets))
	}
	seenAccounts := make(map[string]struct{}, 6)
	for _, request := range strategy.requests {
		if len(request.Predictions) != 1 || request.Predictions[0].Model.Name != request.Context.ModelID {
			t.Fatalf("request mixes model predictions: %#v", request)
		}
		if request.CycleID != request.Context.ExecutionAccountID+":"+decisionAt.Format("20060102T150405Z") {
			t.Fatalf("cycle_id = %q", request.CycleID)
		}
		seenAccounts[request.Context.ExecutionAccountID] = struct{}{}
	}
	if len(seenAccounts) != 6 {
		t.Fatalf("execution accounts = %#v, want six isolated accounts", seenAccounts)
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
		Bindings:        []domain.StrategyExecutionContext{testBinding()},
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
		Bindings:        []domain.StrategyExecutionContext{testBinding()},
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
		Bindings:        []domain.StrategyExecutionContext{testBinding()},
		Venue:           "polymarket-paper",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := service.Run(context.Background(), decisionAt); !errors.Is(err, ErrInvalidStrategy) {
		t.Fatalf("Run() error = %v, want ErrInvalidStrategy", err)
	}
}

// TestNewRejectsDuplicateAccountBinding 验证 New Rejects Duplicate Account Binding 场景下的行为。
func TestNewRejectsDuplicateAccountBinding(t *testing.T) {
	first := testBinding()
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
		Bindings:         []domain.StrategyExecutionContext{first, second},
		Venue:            "polymarket-paper",
	})
	if err == nil {
		t.Fatal("New() error = nil, want duplicate execution account rejection")
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
		Bindings:         []domain.StrategyExecutionContext{testBinding()},
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
