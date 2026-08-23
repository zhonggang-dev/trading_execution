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

type accountPositionSource map[string][]domain.PositionLot

type quarantinePositionSource struct {
	calls           []string
	rejectedAccount string
}

// ListOpenLots 返回模拟数据源中的测试列表。
func (source fakePositionSource) ListOpenLots(context.Context, string) ([]domain.PositionLot, error) {
	return source.lots, nil
}

func (source accountPositionSource) ListOpenLots(_ context.Context, executionAccountID string) ([]domain.PositionLot, error) {
	return source[executionAccountID], nil
}

func (source *quarantinePositionSource) ListOpenLots(_ context.Context, executionAccountID string) ([]domain.PositionLot, error) {
	source.calls = append(source.calls, executionAccountID)
	if executionAccountID == source.rejectedAccount {
		return nil, fmt.Errorf("quarantined account must not be queried")
	}
	return nil, nil
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
	inputRecorded            bool
	outputRecorded           bool
	inputError               error
	deliveries               []domain.DecisionIntentDelivery
	requeueCalls             int
	claimAccountCalls        [][]string
	requeueAccountCalls      [][]string
	claimedOutputs           []domain.StrategyDecisionResponse
	claimedIntents           []domain.OrderIntent
	claimedSubmissionEnabled []bool
	unresolvedCountError     error
}

// ClaimInput 模拟幂等认领并保存测试数据。
func (recorder *fakeRecorder) ClaimInput(_ context.Context, request domain.StrategyDecisionRequest) (domain.StrategyDecisionRequest, bool, error) {
	recorder.inputRecorded = true
	return request, true, recorder.inputError
}

// ClaimOutput 模拟幂等认领并保存测试数据。
func (recorder *fakeRecorder) ClaimOutput(_ context.Context, response domain.StrategyDecisionResponse, intents []domain.OrderIntent, submissionEnabled bool) (domain.StrategyDecisionResponse, bool, error) {
	recorder.outputRecorded = true
	recorder.claimedOutputs = append(recorder.claimedOutputs, response)
	recorder.claimedIntents = append(recorder.claimedIntents, intents...)
	recorder.claimedSubmissionEnabled = append(recorder.claimedSubmissionEnabled, submissionEnabled)
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

func (recorder *fakeRecorder) CountUnresolvedIntentsForAccounts(_ context.Context, executionAccountIDs []string) (int, error) {
	if recorder.unresolvedCountError != nil {
		return 0, recorder.unresolvedCountError
	}
	accounts := make(map[string]struct{}, len(executionAccountIDs))
	for _, accountID := range executionAccountIDs {
		accounts[accountID] = struct{}{}
	}
	count := 0
	for _, delivery := range recorder.deliveries {
		if delivery.Status != domain.DecisionIntentPending && delivery.Status != domain.DecisionIntentSubmitting {
			continue
		}
		if _, disabled := accounts[delivery.Intent.ExecutionAccountID]; disabled {
			count++
		}
	}
	return count, nil
}

type coverageBuyExitStrategy struct {
	requests []domain.StrategyDecisionRequest
}

func (strategy *coverageBuyExitStrategy) Decide(_ context.Context, request domain.StrategyDecisionRequest) (domain.StrategyDecisionResponse, error) {
	strategy.requests = append(strategy.requests, request)
	evaluations := make([]domain.StrategyEvaluation, 0, len(request.Predictions)*2)
	for _, prediction := range request.Predictions {
		for _, outcome := range prediction.Outcomes {
			evaluation := domain.StrategyEvaluation{
				DecisionID:   request.CycleID + ":" + prediction.PredictionID + ":" + outcome.TokenID,
				PredictionID: prediction.PredictionID, MarketID: prediction.MarketID,
				ConditionID: prediction.ConditionID, OutcomeIndex: outcome.Index, TokenID: outcome.TokenID,
				Action: domain.StrategyActionSkip, ReasonCode: domain.StrategyReasonEdgeTooLow,
				Evidence: domain.StrategyEvidence{Probability: outcome.Probability},
			}
			if outcome.Index == 0 {
				evaluation.Action = domain.StrategyActionSubmit
				evaluation.ReasonCode = domain.StrategyReasonEntrySignal
				// Deliberately malformed: the Go coverage gate must audit this
				// proposal but never require or persist a BUY OrderIntent.
				evaluation.Order = nil
			}
			evaluations = append(evaluations, evaluation)
		}
	}
	exits := make([]domain.StrategyExit, 0, len(request.Positions))
	for _, position := range request.Positions {
		exits = append(exits, domain.StrategyExit{
			DecisionID: request.CycleID + ":exit:" + position.LotID,
			LotID:      position.LotID, TokenID: position.TokenID, ReasonCode: domain.StrategyReasonHold48H,
			Order: &domain.StrategyOrderParams{
				Side: domain.SideSell, Type: domain.OrderTypeLimit, WorstPrice: "0.49",
				Size: position.Shares, TimeInForce: domain.TimeInForceFOK,
			},
		})
	}
	return domain.StrategyDecisionResponse{
		SchemaVersion: domain.StrategyOutputSchemaVersion, CycleID: request.CycleID, InputID: request.InputID,
		Context: request.Context, DecidedAt: request.DecisionAt.Add(time.Second),
		Evaluations: evaluations, Exits: exits,
	}, nil
}

func (recorder *fakeRecorder) ClaimPendingIntents(_ context.Context, activeAccountIDs []string, cycleID string, side domain.Side, limit int) ([]domain.DecisionIntentDelivery, error) {
	recorder.claimAccountCalls = append(recorder.claimAccountCalls, append([]string(nil), activeAccountIDs...))
	active := make(map[string]struct{}, len(activeAccountIDs))
	for _, accountID := range activeAccountIDs {
		active[accountID] = struct{}{}
	}
	result := make([]domain.DecisionIntentDelivery, 0)
	for index := range recorder.deliveries {
		delivery := &recorder.deliveries[index]
		if delivery.Status != domain.DecisionIntentPending || (cycleID != "" && delivery.CycleID != cycleID) ||
			(side != "" && delivery.Intent.Side != side) || len(result) >= limit {
			continue
		}
		if _, enabled := active[delivery.Intent.ExecutionAccountID]; !enabled {
			continue
		}
		delivery.Status = domain.DecisionIntentSubmitting
		delivery.Attempt++
		result = append(result, *delivery)
	}
	return result, nil
}

func (recorder *fakeRecorder) RequeueStaleSubmitting(_ context.Context, activeAccountIDs []string, _ time.Time, side domain.Side, limit int) (int, error) {
	recorder.requeueCalls++
	recorder.requeueAccountCalls = append(recorder.requeueAccountCalls, append([]string(nil), activeAccountIDs...))
	active := make(map[string]struct{}, len(activeAccountIDs))
	for _, accountID := range activeAccountIDs {
		active[accountID] = struct{}{}
	}
	requeued := 0
	for index := range recorder.deliveries {
		if requeued >= limit {
			break
		}
		delivery := &recorder.deliveries[index]
		if delivery.Status != domain.DecisionIntentSubmitting || (side != "" && delivery.Intent.Side != side) {
			continue
		}
		if _, enabled := active[delivery.Intent.ExecutionAccountID]; !enabled {
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
			ExpectedPredictions: []domain.PredictionExpectation{
				completedPredictionExpectation(prediction, 1, 1),
			},
		}},
		PositionSource:               fakePositionSource{},
		OrderBookSource:              bookSource,
		MidPriceSource:               midPriceSource,
		Strategy:                     strategy,
		Recorder:                     recorder,
		Executor:                     executor,
		SubmitEnabled:                true,
		RequireCompleteModelCoverage: true,
		Bindings:                     []domain.StrategyExecutionBinding{testExecutionBinding()},
		Venue:                        "polymarket-paper",
		Now:                          func() time.Time { return decisionAt.Add(5 * time.Second) },
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
	if run.Response.EntryPolicy != nil || !run.EntrySubmissionEnabled || run.EntryBlockReason != "" {
		t.Fatalf("healthy entry policy = %#v/%v/%q, want backward-compatible enabled state", run.Response.EntryPolicy, run.EntrySubmissionEnabled, run.EntryBlockReason)
	}
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

func TestRunSkipsQuarantinedBindingWhileOtherBindingSubmits(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	books := make([]domain.OrderBookSnapshot, 0, len(prediction.Outcomes))
	histories := make([]domain.MidPriceHistory, 0, len(prediction.Outcomes))
	for _, outcome := range prediction.Outcomes {
		books = append(books, domain.OrderBookSnapshot{
			MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
			OutcomeIndex: outcome.Index, TokenID: outcome.TokenID, Status: domain.OrderBookStatusOK,
			SourceAt: decisionAt, ObservedAt: decisionAt.Add(time.Second),
			DepthLimit: domain.StrategyOrderBookDepth, MinOrderSize: "1",
			Bids: []domain.PriceLevel{{Price: "0.48", Size: "20"}},
			Asks: []domain.PriceLevel{{Price: "0.50", Size: "20"}},
		})
		histories = append(histories, validMidPriceHistory(prediction, outcome.Index, decisionAt))
	}
	strategy := &fakeStrategy{response: domain.StrategyDecisionResponse{
		SchemaVersion: domain.StrategyOutputSchemaVersion,
		DecidedAt:     decisionAt.Add(2 * time.Second),
		Evaluations: []domain.StrategyEvaluation{
			{
				DecisionID: "decision-submit", PredictionID: prediction.PredictionID,
				MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
				OutcomeIndex: 0, TokenID: prediction.Outcomes[0].TokenID,
				Action: domain.StrategyActionSubmit, ReasonCode: domain.StrategyReasonEntrySignal,
				Evidence: domain.StrategyEvidence{Probability: 0.7, Edge: "0.20", Metrics: map[string]string{
					"best_ask": "0.50", "near_logdiff_usd": "1.2", "rel_spread": "0.04",
					"MOM": "0.01", "MACD_SIGNAL": "0.02",
				}},
				Order: &domain.StrategyOrderParams{
					Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.50",
					Size: "5", TimeInForce: domain.TimeInForceFOK,
				},
			},
			{
				DecisionID: "decision-skip", PredictionID: prediction.PredictionID,
				MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
				OutcomeIndex: 1, TokenID: prediction.Outcomes[1].TokenID,
				Action: domain.StrategyActionSkip, ReasonCode: domain.StrategyReasonInvalidBook,
				Evidence: domain.StrategyEvidence{Probability: 0.3},
			},
		},
	}}
	recorder := &fakeRecorder{}
	executor := &fakeExecutor{}
	positionSource := &quarantinePositionSource{rejectedAccount: "account-quarantined"}
	midPriceSource := &fakeMidPriceHistorySource{histories: histories}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion, SnapshotID: "snapshot-quarantine",
			DecisionAt: decisionAt, GeneratedAt: decisionAt.Add(time.Second),
			Predictions: []domain.Prediction{prediction},
			ExpectedPredictions: []domain.PredictionExpectation{
				completedPredictionExpectation(prediction, 1, 1),
			},
		}},
		PositionSource: positionSource, OrderBookSource: &fakeOrderBookSource{books: books},
		MidPriceSource: midPriceSource, Strategy: strategy,
		Recorder: recorder, Executor: executor, SubmitEnabled: true,
		SubmissionDisabledAccounts:   []string{"account-quarantined"},
		RequireCompleteModelCoverage: true,
		Bindings: []domain.StrategyExecutionBinding{
			{PredictionModelID: prediction.Model.Name, ModelID: "test", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "account-active"},
			{PredictionModelID: prediction.Model.Name, ModelID: "test", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "account-quarantined"},
		},
		Venue: "polymarket-paper", Now: func() time.Time { return decisionAt.Add(5 * time.Second) },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, runErr := service.Run(context.Background(), decisionAt)
	if runErr != nil {
		t.Fatalf("Run() error = %v", runErr)
	}
	if len(result.Runs) != 2 || len(recorder.claimedOutputs) != 1 ||
		len(recorder.claimedSubmissionEnabled) != 1 {
		t.Fatalf("runs/outputs/modes = %d/%d/%#v", len(result.Runs), len(recorder.claimedOutputs), recorder.claimedSubmissionEnabled)
	}
	if !result.Runs[0].OrderSubmissionEnabled || result.Runs[0].AccountSubmissionDisabled ||
		result.Runs[1].OrderSubmissionEnabled || !result.Runs[1].AccountSubmissionDisabled ||
		!recorder.claimedSubmissionEnabled[0] {
		t.Fatalf("binding submission modes = %#v / %#v", result.Runs, recorder.claimedSubmissionEnabled)
	}
	if len(executor.intents) != 1 || executor.intents[0].ExecutionAccountID != "account-active" ||
		len(recorder.deliveries) != 1 || recorder.deliveries[0].Intent.ExecutionAccountID != "account-active" {
		t.Fatalf("executed=%#v deliveries=%#v, want active binding only", executor.intents, recorder.deliveries)
	}
	if len(result.Runs[1].Intents) != 0 || result.Runs[1].Error != nil ||
		result.Runs[1].Request.CycleID != "" || result.Runs[1].Response.CycleID != "" {
		t.Fatalf("quarantined run = %#v, want skipped binding and no error", result.Runs[1])
	}
	if len(positionSource.calls) != 1 || positionSource.calls[0] != "account-active" ||
		strategy.request.Context.ExecutionAccountID != "account-active" || midPriceSource.calls != 0 {
		t.Fatalf("quarantined dependencies were called: positions=%#v strategy=%#v history_calls=%d", positionSource.calls, strategy.request.Context, midPriceSource.calls)
	}
}

// TestRunRoutesIndependentModelMarketsIntoFourWallets verifies that each
// available (Market, source model) result is routed only to that model's two
// strategy wallets. No cross-model Market coverage is required.
func TestRunRoutesIndependentModelMarketsIntoFourWallets(t *testing.T) {
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
	maskedPrediction.SourceJobID = "job-masked"
	maskedPrediction.SandboxID = "sandbox-gemini"
	maskedPrediction.Model.Name = "gemini-3.6-flash"
	maskedPrediction.MarketID = "market-gemini"
	maskedPrediction.ConditionID = "condition-gemini"
	maskedPrediction.Outcomes[0].TokenID = "gemini-yes"
	maskedPrediction.Outcomes[1].TokenID = "gemini-no"
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
		PositionSource:               fakePositionSource{},
		OrderBookSource:              bookSource,
		MidPriceSource:               midPriceSource,
		Strategy:                     strategy,
		Recorder:                     &fakeRecorder{},
		Executor:                     &fakeExecutor{},
		SubmitEnabled:                true,
		RequireCompleteModelCoverage: true,
		Bindings:                     bindings,
		Venue:                        "polymarket-paper",
		Now:                          func() time.Time { return decisionAt.Add(2 * time.Second) },
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
		t.Fatalf("independent orderbook targets = %d, want four outcome tokens", len(bookSource.targets))
	}
	if len(midPriceSource.targets) != 4 {
		t.Fatalf("independent mid-price targets = %d, want four outcome tokens", len(midPriceSource.targets))
	}
	seenAccounts := make(map[string]struct{}, 4)
	for _, request := range strategy.requests {
		if len(request.Predictions) != 1 || request.Predictions[0].Model.Name != request.Context.ModelID {
			t.Fatalf("request mixes model predictions: %#v", request)
		}
		if request.Context.ModelID == "echo" && request.Predictions[0].MarketID != "market-echo" {
			t.Fatalf("echo received another model's market: %#v", request.Predictions)
		}
		if request.Context.ModelID == "gemini_masked" &&
			(request.Predictions[0].MarketID != "market-gemini" ||
				request.Predictions[0].SandboxID != "sandbox-gemini") {
			t.Fatalf("gemini_masked did not receive the Sandbox result: %#v", request.Predictions)
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

func TestRunRoutesAvailableModelWithoutBlockingOnMissingOtherModel(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	prediction.Model.Name = "echo-producer-v7"
	missingModel := prediction
	missingModel.PredictionID = "pred-gemini-pending"
	missingModel.SourceJobID = "job-gemini-pending"
	missingModel.Model.Name = "gemini-3.6-flash"
	expectations := []domain.PredictionExpectation{
		completedPredictionExpectation(prediction, 1, 1),
		pendingPredictionExpectation(missingModel, 1, 1),
	}
	strategy := &matrixStrategy{}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion:       domain.PredictionSnapshotSchemaVersion,
			SnapshotID:          "predsnap-incomplete-model-matrix",
			DecisionAt:          decisionAt,
			Predictions:         []domain.Prediction{prediction},
			ExpectedPredictions: expectations,
		}},
		PositionSource:               fakePositionSource{},
		OrderBookSource:              &fakeOrderBookSource{},
		MidPriceSource:               &fakeMidPriceHistorySource{},
		Strategy:                     strategy,
		Recorder:                     &fakeRecorder{},
		RequireCompleteModelCoverage: true,
		Bindings:                     fourWalletBindings(),
		Venue:                        "polymarket-paper",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, err := service.Run(context.Background(), decisionAt)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(result.Runs) != 4 || len(strategy.requests) != 4 {
		t.Fatalf("runs/strategy requests = %d/%d, want four position-exit-capable calls", len(result.Runs), len(strategy.requests))
	}
	for _, run := range result.Runs {
		switch run.Context.ModelID {
		case "echo":
			if run.PredictionCount != 1 || !run.EntrySubmissionEnabled || run.EntryBlockReason != "" {
				t.Fatalf("echo run = %#v, want one independently usable result", run)
			}
		case "gemini_masked":
			if run.PredictionCount != 0 || run.EntrySubmissionEnabled ||
				run.EntryBlockReason != domain.StrategyEntryBlockIncompleteModelCoverage {
				t.Fatalf("Gemini run = %#v, want only its empty binding blocked", run)
			}
		default:
			t.Fatalf("unexpected run context %#v", run.Context)
		}
	}
}

func TestEmptyModelRouteSubmitsExitButNeverBuy(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	prediction.Model.Name = "echo-producer-v7"
	missingModel := prediction
	missingModel.PredictionID = "pred-gemini-pending"
	missingModel.SourceJobID = "job-gemini-pending"
	missingModel.Model.Name = "gemini-3.6-flash"
	bindings := []domain.StrategyExecutionBinding{
		{PredictionModelID: "gemini-3.6-flash", ModelID: "gemini_masked", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "wallet-2"},
	}
	outcomeIndex := 0
	negRisk := false
	positions := accountPositionSource{
		"wallet-2": {{
			LotID: "lot-wallet-2-1", ExecutionAccountID: "wallet-2", MarketID: prediction.MarketID,
			ConditionID: prediction.ConditionID, TokenID: prediction.Outcomes[0].TokenID,
			OutcomeIndex: &outcomeIndex, OutcomeName: prediction.Outcomes[0].Name, NegRisk: &negRisk,
			ModelID: "gemini_masked", StrategyID: domain.StrategyIDMultfactorV1,
			RemainingShares: "12.50", AverageEntryPrice: "0.40", Status: domain.PositionLotOpen,
			OpenedAt: decisionAt.Add(-49 * time.Hour),
		}},
	}
	books := make([]domain.OrderBookSnapshot, 0, 1)
	for _, outcome := range prediction.Outcomes[:1] {
		books = append(books, domain.OrderBookSnapshot{
			MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
			OutcomeIndex: outcome.Index, TokenID: outcome.TokenID, Status: domain.OrderBookStatusOK,
			SourceAt: decisionAt, ObservedAt: decisionAt.Add(time.Second),
			DepthLimit: domain.StrategyOrderBookDepth, MinOrderSize: "1",
			Bids: []domain.PriceLevel{{Price: "0.49", Size: "20"}},
			Asks: []domain.PriceLevel{{Price: "0.50", Size: "20"}},
		})
	}
	recorder := &fakeRecorder{}
	executor := &fakeExecutor{}
	strategy := &coverageBuyExitStrategy{}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion, SnapshotID: "predsnap-entry-gate",
			DecisionAt: decisionAt, Predictions: []domain.Prediction{prediction},
			ExpectedPredictions: []domain.PredictionExpectation{
				completedPredictionExpectation(prediction, 1, 1),
				pendingPredictionExpectation(missingModel, 1, 1),
			},
		}},
		PositionSource: positions, OrderBookSource: &fakeOrderBookSource{books: books},
		MidPriceSource: &fakeMidPriceHistorySource{}, Strategy: strategy, Recorder: recorder,
		Executor: executor, SubmitEnabled: true, RequireCompleteModelCoverage: true,
		Bindings: bindings, Venue: "polymarket-paper", Now: func() time.Time { return decisionAt.Add(2 * time.Second) },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, runErr := service.Run(context.Background(), decisionAt)
	if runErr != nil {
		t.Fatalf("Run() error = %v", runErr)
	}
	if len(result.Runs) != 1 || len(strategy.requests) != 1 {
		t.Fatalf("runs/requests = %d/%d, want one binding", len(result.Runs), len(strategy.requests))
	}
	if len(executor.intents) != 1 || executor.intents[0].Side != domain.SideSell || executor.intents[0].TargetLotID != "lot-wallet-2-1" {
		t.Fatalf("executed intents = %#v, want only safe SELL exit", executor.intents)
	}
	if len(recorder.claimedIntents) != 1 || recorder.claimedIntents[0].Side != domain.SideSell {
		t.Fatalf("durable intents = %#v, want no BUY", recorder.claimedIntents)
	}
	if len(recorder.claimedOutputs) != 1 {
		t.Fatalf("recorded outputs = %d, want one blocked binding decision", len(recorder.claimedOutputs))
	}
	for index, output := range recorder.claimedOutputs {
		if output.EntryPolicy == nil || output.EntryPolicy.Enabled ||
			output.EntryPolicy.BlockReason != domain.StrategyEntryBlockIncompleteModelCoverage {
			t.Fatalf("output %d entry policy = %#v", index, output.EntryPolicy)
		}
	}
}

func TestOperatorSellOnlyGateSubmitsExitButNeverBuy(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	prediction.Model.Name = "echo-producer-v7"
	outcomeIndex := 0
	negRisk := false
	positions := accountPositionSource{
		"main": {{
			LotID: "lot-main-1", ExecutionAccountID: "main", MarketID: prediction.MarketID,
			ConditionID: prediction.ConditionID, TokenID: prediction.Outcomes[0].TokenID,
			OutcomeIndex: &outcomeIndex, OutcomeName: prediction.Outcomes[0].Name, NegRisk: &negRisk,
			ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV1,
			RemainingShares: "12.50", AverageEntryPrice: "0.40", Status: domain.PositionLotOpen,
			OpenedAt: decisionAt.Add(-49 * time.Hour),
		}},
	}
	books := make([]domain.OrderBookSnapshot, 0, 2)
	for _, outcome := range prediction.Outcomes {
		books = append(books, domain.OrderBookSnapshot{
			MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
			OutcomeIndex: outcome.Index, TokenID: outcome.TokenID, Status: domain.OrderBookStatusOK,
			SourceAt: decisionAt, ObservedAt: decisionAt.Add(time.Second),
			DepthLimit: domain.StrategyOrderBookDepth, MinOrderSize: "1",
			Bids: []domain.PriceLevel{{Price: "0.49", Size: "20"}},
			Asks: []domain.PriceLevel{{Price: "0.50", Size: "20"}},
		})
	}
	recorder := &fakeRecorder{}
	executor := &fakeExecutor{}
	strategy := &coverageBuyExitStrategy{}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion, SnapshotID: "predsnap-sell-only",
			DecisionAt: decisionAt, Predictions: []domain.Prediction{prediction},
			ExpectedPredictions: []domain.PredictionExpectation{
				completedPredictionExpectation(prediction, 1, 1),
			},
		}},
		PositionSource: positions, OrderBookSource: &fakeOrderBookSource{books: books},
		MidPriceSource: &fakeMidPriceHistorySource{}, Strategy: strategy, Recorder: recorder,
		Executor: executor, SubmitEnabled: true, EntrySubmissionDisabled: true,
		RequireCompleteModelCoverage: true,
		Bindings: []domain.StrategyExecutionBinding{{
			PredictionModelID: "echo-producer-v7", ModelID: "echo",
			StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "main",
		}},
		Venue: "polymarket-paper", Now: func() time.Time { return decisionAt.Add(2 * time.Second) },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, runErr := service.Run(context.Background(), decisionAt)
	if runErr != nil {
		t.Fatalf("Run() error = %v", runErr)
	}
	if len(executor.intents) != 1 || executor.intents[0].Side != domain.SideSell ||
		executor.intents[0].TargetLotID != "lot-main-1" {
		t.Fatalf("executed intents = %#v, want only SELL", executor.intents)
	}
	if len(recorder.claimedIntents) != 1 || recorder.claimedIntents[0].Side != domain.SideSell {
		t.Fatalf("durable intents = %#v, want no BUY", recorder.claimedIntents)
	}
	if len(result.Runs) != 1 || result.Runs[0].EntrySubmissionEnabled ||
		result.Runs[0].EntryBlockReason != domain.StrategyEntryBlockSubmissionDisabled {
		t.Fatalf("binding run = %#v, want operator sell-only gate", result.Runs)
	}
	if len(recorder.claimedOutputs) != 1 || recorder.claimedOutputs[0].EntryPolicy == nil ||
		recorder.claimedOutputs[0].EntryPolicy.BlockReason != domain.StrategyEntryBlockSubmissionDisabled {
		t.Fatalf("recorded output policy = %#v", recorder.claimedOutputs)
	}
}

func TestRunSelectsLatestPITProbabilityForSameMarketAndModel(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	first := validPrediction(decisionAt)
	first.Model.Name = "echo-producer-v7"
	second := first
	second.Outcomes = append([]domain.PredictionOutcome(nil), first.Outcomes...)
	second.PredictionID = "pred-2"
	second.SourceJobID = "job-2"
	second.PredictionAsOf = decisionAt.Add(-20 * time.Minute)
	second.CompletedAt = decisionAt.Add(-8 * time.Minute)
	second.AvailableAt = decisionAt.Add(-7 * time.Minute)
	second.Outcomes[0].Probability = 0.8
	second.Outcomes[1].Probability = 0.2
	strategy := &matrixStrategy{}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion,
			SnapshotID:    "predsnap-duplicate-market-model",
			DecisionAt:    decisionAt,
			Predictions:   []domain.Prediction{first, second},
			ExpectedPredictions: []domain.PredictionExpectation{
				completedPredictionExpectation(first, 1, 1),
				completedPredictionExpectation(second, 2, 2),
			},
		}},
		PositionSource:               fakePositionSource{},
		OrderBookSource:              &fakeOrderBookSource{},
		MidPriceSource:               &fakeMidPriceHistorySource{},
		Strategy:                     strategy,
		Recorder:                     &fakeRecorder{},
		RequireCompleteModelCoverage: true,
		Bindings: []domain.StrategyExecutionBinding{{
			PredictionModelID: "echo-producer-v7", ModelID: "echo",
			StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "main",
		}},
		Venue: "polymarket-paper",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := service.Run(context.Background(), decisionAt); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(strategy.requests) != 1 || len(strategy.requests[0].Predictions) != 1 ||
		strategy.requests[0].Predictions[0].PredictionID != "pred-2" ||
		strategy.requests[0].Predictions[0].Outcomes[0].Probability != 0.8 {
		t.Fatalf("effective predictions = %#v, want only newest PIT row", strategy.requests)
	}
}

func TestRunRoutesLatestCompletedResultWithoutManifestDependency(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	expected := validPrediction(decisionAt)
	expected.Model.Name = "echo-producer-v7"
	newerResult := expected
	newerResult.Outcomes = append([]domain.PredictionOutcome(nil), expected.Outcomes...)
	newerResult.PredictionID = "pred-newer-result"
	newerResult.SourceJobID = "job-newer-result"
	newerResult.PredictionAsOf = decisionAt.Add(-5 * time.Minute)
	newerResult.CompletedAt = decisionAt.Add(-4 * time.Minute)
	newerResult.AvailableAt = decisionAt.Add(-3 * time.Minute)
	newerResult.Outcomes[0].Probability = 0.9
	newerResult.Outcomes[1].Probability = 0.1
	strategy := &matrixStrategy{}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion, SnapshotID: "predsnap-manifest-authority",
			DecisionAt: decisionAt, Predictions: []domain.Prediction{newerResult, expected},
			ExpectedPredictions: []domain.PredictionExpectation{completedPredictionExpectation(expected, 1, 1)},
		}},
		PositionSource: fakePositionSource{}, OrderBookSource: &fakeOrderBookSource{},
		MidPriceSource: &fakeMidPriceHistorySource{}, Strategy: strategy, Recorder: &fakeRecorder{},
		RequireCompleteModelCoverage: true,
		Bindings: []domain.StrategyExecutionBinding{{
			PredictionModelID: "echo-producer-v7", ModelID: "echo",
			StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "main",
		}},
		Venue: "polymarket-paper",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := service.Run(context.Background(), decisionAt); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(strategy.requests) != 1 || len(strategy.requests[0].Predictions) != 1 ||
		strategy.requests[0].Predictions[0].PredictionID != newerResult.PredictionID {
		t.Fatalf("routed predictions = %#v, want latest completed result row", strategy.requests)
	}
}

func TestRunNewerSandboxResultReplacesOlderDirectResult(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	direct := validPrediction(decisionAt)
	direct.Model.Name = "echo-producer-v7"
	sandbox := direct
	sandbox.Outcomes = append([]domain.PredictionOutcome(nil), direct.Outcomes...)
	sandbox.PredictionID = "pred-sandbox-newer"
	sandbox.SourceJobID = "job-sandbox-newer"
	sandbox.SandboxID = "sandbox-1"
	sandbox.PredictionAsOf = decisionAt.Add(-5 * time.Minute)
	sandbox.CompletedAt = decisionAt.Add(-4 * time.Minute)
	sandbox.AvailableAt = decisionAt.Add(-3 * time.Minute)
	sandbox.Outcomes[0].Probability = 0.9
	sandbox.Outcomes[1].Probability = 0.1
	strategy := &matrixStrategy{}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion, SnapshotID: "predsnap-ignore-sandbox",
			DecisionAt: decisionAt, Predictions: []domain.Prediction{direct, sandbox},
		}},
		PositionSource: fakePositionSource{}, OrderBookSource: &fakeOrderBookSource{},
		MidPriceSource: &fakeMidPriceHistorySource{}, Strategy: strategy, Recorder: &fakeRecorder{},
		RequireCompleteModelCoverage: true,
		Bindings: []domain.StrategyExecutionBinding{{
			PredictionModelID: "echo-producer-v7", ModelID: "echo",
			StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "main",
		}},
		Venue: "polymarket-paper",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, err := service.Run(context.Background(), decisionAt)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(strategy.requests) != 1 || len(strategy.requests[0].Predictions) != 1 ||
		strategy.requests[0].Predictions[0].PredictionID != sandbox.PredictionID ||
		strategy.requests[0].Predictions[0].Outcomes[0].Probability != 0.9 ||
		len(result.Runs) != 1 || !result.Runs[0].EntrySubmissionEnabled {
		t.Fatalf("newest Sandbox result = %#v, requests = %#v", result, strategy.requests)
	}
}

func TestRunNewerDirectResultReplacesOlderSandboxResult(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	sandbox := validPrediction(decisionAt)
	sandbox.Model.Name = "echo-producer-v7"
	sandbox.SandboxID = "sandbox-1"
	direct := sandbox
	direct.Outcomes = append([]domain.PredictionOutcome(nil), sandbox.Outcomes...)
	direct.PredictionID = "pred-direct-newer"
	direct.SourceJobID = "job-direct-newer"
	direct.SandboxID = ""
	direct.PredictionAsOf = decisionAt.Add(-5 * time.Minute)
	direct.CompletedAt = decisionAt.Add(-4 * time.Minute)
	direct.AvailableAt = decisionAt.Add(-3 * time.Minute)
	direct.Outcomes[0].Probability = 0.85
	direct.Outcomes[1].Probability = 0.15
	strategy := &matrixStrategy{}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion, SnapshotID: "predsnap-direct-newer",
			DecisionAt: decisionAt, Predictions: []domain.Prediction{sandbox, direct},
		}},
		PositionSource: fakePositionSource{}, OrderBookSource: &fakeOrderBookSource{},
		MidPriceSource: &fakeMidPriceHistorySource{}, Strategy: strategy, Recorder: &fakeRecorder{},
		RequireCompleteModelCoverage: true,
		Bindings: []domain.StrategyExecutionBinding{{
			PredictionModelID: "echo-producer-v7", ModelID: "echo",
			StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "main",
		}},
		Venue: "polymarket-paper",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, err := service.Run(context.Background(), decisionAt)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(strategy.requests) != 1 || len(strategy.requests[0].Predictions) != 1 ||
		strategy.requests[0].Predictions[0].PredictionID != direct.PredictionID ||
		strategy.requests[0].Predictions[0].Outcomes[0].Probability != 0.85 ||
		len(result.Runs) != 1 || !result.Runs[0].EntrySubmissionEnabled {
		t.Fatalf("newest Direct result = %#v, requests = %#v", result, strategy.requests)
	}
}

func TestRunSandboxOnlyResultIsRoutedAndAllowsBindingEntry(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	sandbox := validPrediction(decisionAt)
	sandbox.Model.Name = "echo-producer-v7"
	sandbox.SandboxID = "sandbox-1"
	strategy := &matrixStrategy{}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion, SnapshotID: "predsnap-sandbox-only",
			DecisionAt: decisionAt, Predictions: []domain.Prediction{sandbox},
		}},
		PositionSource: fakePositionSource{}, OrderBookSource: &fakeOrderBookSource{},
		MidPriceSource: &fakeMidPriceHistorySource{}, Strategy: strategy, Recorder: &fakeRecorder{},
		RequireCompleteModelCoverage: true,
		Bindings: []domain.StrategyExecutionBinding{{
			PredictionModelID: "echo-producer-v7", ModelID: "echo",
			StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "main",
		}},
		Venue: "polymarket-paper",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, err := service.Run(context.Background(), decisionAt)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(strategy.requests) != 1 || len(strategy.requests[0].Predictions) != 1 ||
		strategy.requests[0].Predictions[0].PredictionID != sandbox.PredictionID ||
		len(result.Runs) != 1 || !result.Runs[0].EntrySubmissionEnabled ||
		result.Runs[0].EntryBlockReason != "" {
		t.Fatalf("sandbox-only result = %#v, requests = %#v", result, strategy.requests)
	}
}

func TestRunRoutesCompletedResultsForDifferentModelsWithoutSyntheticPairs(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	current := validPrediction(decisionAt)
	current.Model.Name = "echo-producer-v7"
	oldUnmanifested := current
	oldUnmanifested.Outcomes = append([]domain.PredictionOutcome(nil), current.Outcomes...)
	oldUnmanifested.PredictionID = "pred-gemini-old-unmanifested"
	oldUnmanifested.SourceJobID = "job-gemini-old-unmanifested"
	oldUnmanifested.Model.Name = "gemini-3.6-flash"
	oldUnmanifested.PredictionAsOf = decisionAt.Add(-2 * time.Hour)
	oldUnmanifested.Outcomes[0].Probability = 0.9
	oldUnmanifested.Outcomes[1].Probability = 0.1
	strategy := &matrixStrategy{}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion, SnapshotID: "predsnap-missing-current-task",
			DecisionAt: decisionAt, Predictions: []domain.Prediction{current, oldUnmanifested},
			ExpectedPredictions: []domain.PredictionExpectation{
				completedPredictionExpectation(current, 2, 2),
			},
		}},
		PositionSource: fakePositionSource{}, OrderBookSource: &fakeOrderBookSource{},
		MidPriceSource: &fakeMidPriceHistorySource{}, Strategy: strategy, Recorder: &fakeRecorder{},
		RequireCompleteModelCoverage: true,
		Bindings: []domain.StrategyExecutionBinding{
			{PredictionModelID: "echo-producer-v7", ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "main"},
			{PredictionModelID: "gemini-3.6-flash", ModelID: "gemini_masked", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "wallet-2"},
		},
		Venue: "polymarket-paper",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, runErr := service.Run(context.Background(), decisionAt)
	if runErr != nil {
		t.Fatalf("Run() error = %v", runErr)
	}
	if len(strategy.requests) != 2 || len(result.Runs) != 2 {
		t.Fatalf("requests/runs = %d/%d, want both exit-capable bindings", len(strategy.requests), len(result.Runs))
	}
	for _, request := range strategy.requests {
		switch request.Context.ModelID {
		case "echo":
			if len(request.Predictions) != 1 || request.Predictions[0].PredictionID != current.PredictionID {
				t.Fatalf("current manifest route = %#v, want only exact echo result", request.Predictions)
			}
		case "gemini_masked":
			if len(request.Predictions) != 1 || request.Predictions[0].PredictionID != oldUnmanifested.PredictionID {
				t.Fatalf("independent Gemini result route = %#v", request.Predictions)
			}
		default:
			t.Fatalf("unexpected strategy context %#v", request.Context)
		}
	}
	for _, run := range result.Runs {
		if !run.EntrySubmissionEnabled || run.EntryBlockReason != "" {
			t.Fatalf("binding run = %#v, want independent available-model entry state", run)
		}
	}
}

func TestRunUsesCompletedResultWhileNewerTaskIsPending(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	old := validPrediction(decisionAt)
	old.Model.Name = "echo-producer-v7"
	newTask := old
	newTask.PredictionID = "pred-current-pending"
	newTask.SourceJobID = "job-current-pending"
	newTask.PredictionAsOf = decisionAt.Add(-5 * time.Minute)
	strategy := &matrixStrategy{}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion, SnapshotID: "predsnap-stale-mask",
			DecisionAt: decisionAt, Predictions: []domain.Prediction{old},
			ExpectedPredictions: []domain.PredictionExpectation{
				completedPredictionExpectation(old, 1, 1),
				pendingPredictionExpectation(newTask, 2, 2),
			},
		}},
		PositionSource: fakePositionSource{}, OrderBookSource: &fakeOrderBookSource{},
		MidPriceSource: &fakeMidPriceHistorySource{}, Strategy: strategy, Recorder: &fakeRecorder{},
		RequireCompleteModelCoverage: true,
		Bindings: []domain.StrategyExecutionBinding{{
			PredictionModelID: "echo-producer-v7", ModelID: "echo",
			StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "main",
		}},
		Venue: "polymarket-paper",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, runErr := service.Run(context.Background(), decisionAt)
	if runErr != nil {
		t.Fatalf("Run() error = %v", runErr)
	}
	if len(strategy.requests) != 1 || len(strategy.requests[0].Predictions) != 1 ||
		strategy.requests[0].Predictions[0].PredictionID != old.PredictionID {
		t.Fatalf("completed result input = %#v", strategy.requests)
	}
	if len(result.Runs) != 1 || !result.Runs[0].EntrySubmissionEnabled || result.Runs[0].EntryBlockReason != "" {
		t.Fatalf("binding run = %#v, want available completed result", result.Runs)
	}
}

func TestRunRoutesIndependentCompletedGenerations(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	current := validPrediction(decisionAt)
	current.Model.Name = "echo-producer-v7"
	oldOther := current
	oldOther.PredictionID = "pred-gemini-old"
	oldOther.SourceJobID = "job-gemini-old"
	oldOther.Model.Name = "gemini-3.6-flash"
	oldOther.PredictionAsOf = decisionAt.Add(-2 * time.Hour)
	strategy := &matrixStrategy{}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion, SnapshotID: "predsnap-mixed-generation",
			DecisionAt: decisionAt, Predictions: []domain.Prediction{current, oldOther},
			ExpectedPredictions: []domain.PredictionExpectation{
				completedPredictionExpectation(current, 2, 2),
				completedPredictionExpectation(oldOther, 1, 1),
			},
		}},
		PositionSource: fakePositionSource{}, OrderBookSource: &fakeOrderBookSource{},
		MidPriceSource: &fakeMidPriceHistorySource{}, Strategy: strategy, Recorder: &fakeRecorder{},
		RequireCompleteModelCoverage: true,
		Bindings: []domain.StrategyExecutionBinding{
			{PredictionModelID: "echo-producer-v7", ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "main"},
			{PredictionModelID: "gemini-3.6-flash", ModelID: "gemini_masked", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "wallet-2"},
		},
		Venue: "polymarket-paper",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, runErr := service.Run(context.Background(), decisionAt)
	if runErr != nil {
		t.Fatalf("Run() error = %v", runErr)
	}
	if len(strategy.requests) != 2 || len(result.Runs) != 2 {
		t.Fatalf("requests/runs = %d/%d, want both exit-capable bindings", len(strategy.requests), len(result.Runs))
	}
	for _, request := range strategy.requests {
		if len(request.Predictions) != 1 {
			t.Fatalf("independent result input = %#v, want one result per model", request.Predictions)
		}
	}
}

func TestRunCompletedTaskOlderThanPredictionLookbackCannotAuthorizeEntry(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	stale := validPrediction(decisionAt)
	stale.Model.Name = "echo-producer-v7"
	stale.SandboxID = "sandbox-stale"
	stale.PredictionAsOf = decisionAt.Add(-4 * time.Hour)
	strategy := &matrixStrategy{}
	service, err := New(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion, SnapshotID: "predsnap-stale-completed",
			DecisionAt: decisionAt, Predictions: []domain.Prediction{stale},
			ExpectedPredictions: []domain.PredictionExpectation{completedPredictionExpectation(stale, 1, 1)},
		}},
		PositionSource: fakePositionSource{}, OrderBookSource: &fakeOrderBookSource{},
		MidPriceSource: &fakeMidPriceHistorySource{}, Strategy: strategy, Recorder: &fakeRecorder{},
		PredictionLookback: 3 * time.Hour, RequireCompleteModelCoverage: true,
		Bindings: []domain.StrategyExecutionBinding{{
			PredictionModelID: "echo-producer-v7", ModelID: "echo",
			StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "main",
		}},
		Venue: "polymarket-paper",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, runErr := service.Run(context.Background(), decisionAt)
	if runErr != nil {
		t.Fatalf("Run() error = %v", runErr)
	}
	if len(strategy.requests) != 1 || len(strategy.requests[0].Predictions) != 0 ||
		len(result.Runs) != 1 || result.Runs[0].EntrySubmissionEnabled {
		t.Fatalf("stale cycle result = %#v, requests = %#v", result, strategy.requests)
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
		PositionSource:               fakePositionSource{},
		OrderBookSource:              &fakeOrderBookSource{},
		MidPriceSource:               &fakeMidPriceHistorySource{},
		Strategy:                     strategy,
		Recorder:                     recorder,
		Executor:                     executor,
		SubmitEnabled:                true,
		RequireCompleteModelCoverage: true,
		Bindings:                     []domain.StrategyExecutionBinding{testExecutionBinding()},
		Venue:                        "polymarket-paper",
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
		PositionSource:               fakePositionSource{},
		OrderBookSource:              &fakeOrderBookSource{},
		MidPriceSource:               &fakeMidPriceHistorySource{},
		Strategy:                     strategy,
		Recorder:                     recorder,
		Executor:                     executor,
		SubmitEnabled:                true,
		RequireCompleteModelCoverage: true,
		Bindings:                     []domain.StrategyExecutionBinding{testExecutionBinding()},
		Venue:                        "polymarket-paper",
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
		PositionSource:               fakePositionSource{},
		OrderBookSource:              &fakeOrderBookSource{},
		MidPriceSource:               &fakeMidPriceHistorySource{},
		Strategy:                     contextHijackStrategy{},
		Recorder:                     &fakeRecorder{},
		Executor:                     &fakeExecutor{},
		SubmitEnabled:                true,
		RequireCompleteModelCoverage: true,
		Bindings:                     []domain.StrategyExecutionBinding{testExecutionBinding()},
		Venue:                        "polymarket-paper",
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
		PositionSource:               fakePositionSource{},
		OrderBookSource:              &fakeOrderBookSource{},
		MidPriceSource:               &fakeMidPriceHistorySource{},
		Strategy:                     strategy,
		Recorder:                     recorder,
		Executor:                     &fakeExecutor{},
		SubmitEnabled:                true,
		RequireCompleteModelCoverage: true,
		Bindings:                     []domain.StrategyExecutionBinding{testExecutionBinding()},
		Venue:                        "polymarket-paper",
		Now:                          func() time.Time { return decisionAt.Add(2 * time.Second) },
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
		PredictionSource:             fakePredictionSource{},
		PositionSource:               fakePositionSource{},
		OrderBookSource:              &fakeOrderBookSource{},
		MidPriceSource:               &fakeMidPriceHistorySource{},
		Strategy:                     &fakeStrategy{},
		Recorder:                     &fakeRecorder{},
		Executor:                     &fakeExecutor{},
		SubmitEnabled:                true,
		RequireCompleteModelCoverage: true,
		Bindings:                     []domain.StrategyExecutionBinding{first, second},
		Venue:                        "polymarket-paper",
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

func TestNewRejectsSubmissionWithoutCompleteModelCoverage(t *testing.T) {
	_, err := New(Params{
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
	if err == nil || !strings.Contains(err.Error(), "complete prediction-model coverage") {
		t.Fatalf("New() error = %v", err)
	}
}

func TestNewRejectsQuarantiningEveryBinding(t *testing.T) {
	binding := testExecutionBinding()
	_, err := New(Params{
		PredictionSource:             fakePredictionSource{},
		PositionSource:               fakePositionSource{},
		OrderBookSource:              &fakeOrderBookSource{},
		MidPriceSource:               &fakeMidPriceHistorySource{},
		Strategy:                     &fakeStrategy{},
		Recorder:                     &fakeRecorder{},
		Executor:                     &fakeExecutor{},
		SubmitEnabled:                true,
		SubmissionDisabledAccounts:   []string{binding.ExecutionAccountID},
		RequireCompleteModelCoverage: true,
		Bindings:                     []domain.StrategyExecutionBinding{binding},
		Venue:                        "polymarket-paper",
	})
	if err == nil || !strings.Contains(err.Error(), "cannot exclude every decision binding") {
		t.Fatalf("New() error = %v", err)
	}
}

// TestRunRejectsNonBoundaryTime 验证 Run Rejects Non Boundary Time 场景下的行为。
func TestRunRejectsNonBoundaryTime(t *testing.T) {
	service, err := New(Params{
		PredictionSource:             fakePredictionSource{},
		PositionSource:               fakePositionSource{},
		OrderBookSource:              &fakeOrderBookSource{},
		MidPriceSource:               &fakeMidPriceHistorySource{},
		Strategy:                     &fakeStrategy{},
		Recorder:                     &fakeRecorder{},
		Executor:                     &fakeExecutor{},
		SubmitEnabled:                true,
		RequireCompleteModelCoverage: true,
		Bindings:                     []domain.StrategyExecutionBinding{testExecutionBinding()},
		Venue:                        "polymarket-paper",
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
	evidence.Metrics["best_bid"] = "0.49"
	evidence.Metrics["expected_avg_price"] = "0.501"
	evidence.Metrics["target_shares"] = "19.96007984031936"
	evidence.Metrics["depth_capped"] = "1"
	if err := validateEvidenceMetrics(evidence, request, "token", true); err != nil {
		t.Fatalf("optional Python audit metrics error = %v", err)
	}
	evidence.Metrics[" "] = "1"
	if err := validateEvidenceMetrics(evidence, request, "token", true); err == nil ||
		!strings.Contains(err.Error(), "key must be non-empty") {
		t.Fatalf("blank metrics key error = %v", err)
	}
	delete(evidence.Metrics, " ")
	evidence.Metrics["expected_avg_price"] = "not-a-decimal"
	if err := validateEvidenceMetrics(evidence, request, "token", true); err == nil ||
		!strings.Contains(err.Error(), "must be a decimal string") {
		t.Fatalf("invalid metrics value error = %v", err)
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
			intent := domain.OrderIntent{ClientOrderID: "client-1", ExecutionAccountID: "account-active"}
			recorder := &fakeRecorder{deliveries: []domain.DecisionIntentDelivery{{
				CycleID: "cycle-1", ClientOrderID: intent.ClientOrderID,
				Intent: intent, Status: domain.DecisionIntentPending,
			}}}
			service := &Service{recorder: recorder, executor: fixedResultExecutor{result: port.OrderSubmitResult{
				Order: domain.Order{ID: "order-1", Status: status},
			}}, activeExecutionAccountIDs: []string{"account-active"}}
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
	intent := domain.OrderIntent{ClientOrderID: "client-1", ExecutionAccountID: "account-active"}
	recorder := &fakeRecorder{deliveries: []domain.DecisionIntentDelivery{{
		CycleID: "cycle-1", ClientOrderID: intent.ClientOrderID,
		Intent: intent, Status: domain.DecisionIntentPending,
	}}}
	service := &Service{recorder: recorder, executor: fixedResultExecutor{}, activeExecutionAccountIDs: []string{"account-active"}}
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
			Intent: domain.OrderIntent{
				ClientOrderID: fmt.Sprintf("client-%03d", index), ExecutionAccountID: "account-active",
			},
			Status:  domain.DecisionIntentSubmitting,
			Attempt: 1,
		}
	}
	recorder := &fakeRecorder{deliveries: deliveries}
	executor := &fakeExecutor{}
	service := &Service{
		recorder: recorder, executor: executor, submitEnabled: true,
		activeExecutionAccountIDs: []string{"account-active"},
		now:                       func() time.Time { return time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC) },
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

func TestRecoverStartupLeavesRetiredDeliveriesUntouched(t *testing.T) {
	recorder := &fakeRecorder{deliveries: []domain.DecisionIntentDelivery{
		{
			CycleID: "old-active", ClientOrderID: "active-submitting",
			Intent: domain.OrderIntent{ClientOrderID: "active-submitting", ExecutionAccountID: "account-active"},
			Status: domain.DecisionIntentSubmitting, Attempt: 1,
		},
		{
			CycleID: "old-retired", ClientOrderID: "retired-submitting",
			Intent: domain.OrderIntent{ClientOrderID: "retired-submitting", ExecutionAccountID: "account-retired"},
			Status: domain.DecisionIntentSubmitting, Attempt: 1,
		},
		{
			CycleID: "old-retired", ClientOrderID: "retired-pending",
			Intent: domain.OrderIntent{ClientOrderID: "retired-pending", ExecutionAccountID: "account-retired"},
			Status: domain.DecisionIntentPending,
		},
	}}
	executor := &fakeExecutor{}
	service := &Service{
		recorder: recorder, executor: executor, submitEnabled: true,
		activeExecutionAccountIDs: []string{"account-active"},
		now:                       func() time.Time { return time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC) },
	}
	if err := service.RecoverStartup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(executor.intents) != 1 || executor.intents[0].ExecutionAccountID != "account-active" {
		t.Fatalf("submitted intents = %#v", executor.intents)
	}
	if recorder.deliveries[0].Status != domain.DecisionIntentSubmitted ||
		recorder.deliveries[1].Status != domain.DecisionIntentSubmitting ||
		recorder.deliveries[2].Status != domain.DecisionIntentPending {
		t.Fatalf("delivery statuses = %s/%s/%s", recorder.deliveries[0].Status, recorder.deliveries[1].Status, recorder.deliveries[2].Status)
	}
	for _, accounts := range append(recorder.requeueAccountCalls, recorder.claimAccountCalls...) {
		if len(accounts) != 1 || accounts[0] != "account-active" {
			t.Fatalf("recovery allowlist = %#v", accounts)
		}
	}
}

func TestRecoverStartupFailsClosedForUnresolvedQuarantinedDelivery(t *testing.T) {
	recorder := &fakeRecorder{deliveries: []domain.DecisionIntentDelivery{
		{
			CycleID: "old-active", ClientOrderID: "active-pending",
			Intent: domain.OrderIntent{ClientOrderID: "active-pending", ExecutionAccountID: "account-active"},
			Status: domain.DecisionIntentPending,
		},
		{
			CycleID: "old-quarantined", ClientOrderID: "quarantined-pending",
			Intent: domain.OrderIntent{ClientOrderID: "quarantined-pending", ExecutionAccountID: "account-quarantined"},
			Status: domain.DecisionIntentPending,
		},
	}}
	executor := &fakeExecutor{}
	service := &Service{
		recorder: recorder, executor: executor, submitEnabled: true,
		activeExecutionAccountIDs:    []string{"account-active"},
		submissionDisabledAccounts:   []string{"account-quarantined"},
		submissionDisabledAccountSet: map[string]struct{}{"account-quarantined": {}},
		now:                          func() time.Time { return time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC) },
	}
	err := service.RecoverStartup(context.Background())
	if err == nil || !strings.Contains(err.Error(), "unresolved intent") {
		t.Fatalf("RecoverStartup() error = %v, want quarantined intent rejection", err)
	}
	if len(executor.intents) != 0 || recorder.requeueCalls != 0 ||
		recorder.deliveries[0].Status != domain.DecisionIntentPending ||
		recorder.deliveries[1].Status != domain.DecisionIntentPending {
		t.Fatalf("fail-closed recovery mutated or submitted work: executor=%#v recorder=%#v", executor.intents, recorder)
	}
}

func TestSellOnlyRecoveryNeverClaimsOrRequeuesBuy(t *testing.T) {
	recorder := &fakeRecorder{deliveries: []domain.DecisionIntentDelivery{
		{
			CycleID: "old-cycle", ClientOrderID: "old-buy-submitting",
			Intent: domain.OrderIntent{ClientOrderID: "old-buy-submitting", ExecutionAccountID: "account-active", Side: domain.SideBuy},
			Status: domain.DecisionIntentSubmitting, Attempt: 1,
		},
		{
			CycleID: "old-cycle", ClientOrderID: "old-buy-pending",
			Intent: domain.OrderIntent{ClientOrderID: "old-buy-pending", ExecutionAccountID: "account-active", Side: domain.SideBuy},
			Status: domain.DecisionIntentPending,
		},
		{
			CycleID: "old-cycle", ClientOrderID: "old-sell-submitting",
			Intent: domain.OrderIntent{ClientOrderID: "old-sell-submitting", ExecutionAccountID: "account-active", Side: domain.SideSell},
			Status: domain.DecisionIntentSubmitting, Attempt: 1,
		},
	}}
	executor := &fakeExecutor{}
	service := &Service{
		recorder: recorder, executor: executor, submitEnabled: true, entrySubmissionDisabled: true,
		activeExecutionAccountIDs: []string{"account-active"},
		now:                       func() time.Time { return time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC) },
	}

	if err := service.RecoverStartup(context.Background()); err != nil {
		t.Fatalf("RecoverStartup() error = %v", err)
	}
	if len(executor.intents) != 1 || executor.intents[0].Side != domain.SideSell {
		t.Fatalf("submitted intents = %#v, want exactly one SELL", executor.intents)
	}
	if recorder.deliveries[0].Status != domain.DecisionIntentSubmitting ||
		recorder.deliveries[1].Status != domain.DecisionIntentPending ||
		recorder.deliveries[2].Status != domain.DecisionIntentSubmitted {
		t.Fatalf("stored delivery statuses = %s/%s/%s", recorder.deliveries[0].Status, recorder.deliveries[1].Status, recorder.deliveries[2].Status)
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

func TestCoverageEntryGateBlocksMalformedBuyButPreservesValidExit(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	request := domain.StrategyDecisionRequest{
		SchemaVersion: domain.StrategyInputSchemaVersion, CycleID: "cycle-coverage", InputID: "input-coverage",
		Context: domain.StrategyExecutionContext{
			ModelID: "test", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "account-test-v1",
		},
		DecisionAt: decisionAt, Predictions: []domain.Prediction{prediction},
		Positions: []domain.StrategyPositionLot{{
			LotID: "lot-1", MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
			OutcomeIndex: 0, OutcomeName: "Yes", TokenID: prediction.Outcomes[0].TokenID,
			EnteredAt: decisionAt.Add(-49 * time.Hour), Shares: "12.50", EntryPrice: "0.40",
		}},
		OrderBooks: []domain.OrderBookSnapshot{
			{
				MarketID: prediction.MarketID, ConditionID: prediction.ConditionID, OutcomeIndex: 0,
				TokenID: prediction.Outcomes[0].TokenID, Status: domain.OrderBookStatusOK,
				SourceAt: decisionAt, ObservedAt: decisionAt, DepthLimit: domain.StrategyOrderBookDepth,
				MinOrderSize: "1", Bids: []domain.PriceLevel{{Price: "0.49", Size: "20"}},
				Asks: []domain.PriceLevel{{Price: "0.50", Size: "20"}},
			},
			{
				MarketID: prediction.MarketID, ConditionID: prediction.ConditionID, OutcomeIndex: 1,
				TokenID: prediction.Outcomes[1].TokenID, Status: domain.OrderBookStatusOK,
				SourceAt: decisionAt, ObservedAt: decisionAt, DepthLimit: domain.StrategyOrderBookDepth,
				MinOrderSize: "1", Bids: []domain.PriceLevel{{Price: "0.49", Size: "20"}},
				Asks: []domain.PriceLevel{{Price: "0.50", Size: "20"}},
			},
		},
		ExecutionConstraints: domain.DefaultStrategyExecutionConstraints(),
	}
	response := domain.StrategyDecisionResponse{
		SchemaVersion: domain.StrategyOutputSchemaVersion, CycleID: request.CycleID, InputID: request.InputID,
		Context: request.Context, DecidedAt: decisionAt.Add(time.Second),
		EntryPolicy: &domain.StrategyEntryPolicy{
			Enabled: false, BlockReason: domain.StrategyEntryBlockIncompleteModelCoverage,
		},
		Evaluations: []domain.StrategyEvaluation{
			{
				DecisionID: "exit-1", PredictionID: prediction.PredictionID,
				MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
				OutcomeIndex: 0, TokenID: prediction.Outcomes[0].TokenID,
				Action: domain.StrategyActionSubmit, ReasonCode: domain.StrategyReasonEntrySignal,
				Evidence: domain.StrategyEvidence{Probability: prediction.Outcomes[0].Probability},
				// Deliberately omit Order: the degraded-cycle gate must not let a
				// non-executable BUY proposal suppress an independent safe exit.
			},
			{
				DecisionID: "skip-no", PredictionID: prediction.PredictionID,
				MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
				OutcomeIndex: 1, TokenID: prediction.Outcomes[1].TokenID,
				Action: domain.StrategyActionSkip, ReasonCode: domain.StrategyReasonEdgeTooLow,
				Evidence: domain.StrategyEvidence{Probability: prediction.Outcomes[1].Probability},
			},
		},
		Exits: []domain.StrategyExit{{
			DecisionID: "exit-1", LotID: "lot-1", TokenID: prediction.Outcomes[0].TokenID,
			ReasonCode: domain.StrategyReasonHold48H,
			Order: &domain.StrategyOrderParams{
				Side: domain.SideSell, Type: domain.OrderTypeLimit, WorstPrice: "0.49",
				Size: "12.50", TimeInForce: domain.TimeInForceFOK,
			},
		}},
	}
	intents, err := validateResponseWithEntryPolicy(
		request, response, "polymarket", false, domain.StrategyEntryBlockIncompleteModelCoverage,
	)
	if err != nil {
		t.Fatalf("validateResponseWithEntryPolicy() error = %v", err)
	}
	if len(intents) != 1 || intents[0].Side != domain.SideSell || intents[0].TargetLotID != "lot-1" {
		t.Fatalf("coverage-gated intents = %#v, want only SELL exit", intents)
	}
	response.EntryPolicy.BlockReason = domain.StrategyEntryBlockSubmissionDisabled
	if _, err := validateResponseWithEntryPolicy(
		request, response, "polymarket", false, domain.StrategyEntryBlockIncompleteModelCoverage,
	); err == nil {
		t.Fatal("coverage-gated validation accepted a mismatched operator block reason")
	}
	response.EntryPolicy = nil
	if _, err := validateResponse(request, response, "polymarket"); err == nil {
		t.Fatal("normal entry validation accepted malformed SUBMIT")
	}
}

// validPrediction 构建测试使用的合法输入。
func validPrediction(decisionAt time.Time) domain.Prediction {
	return domain.Prediction{
		PredictionID: "pred-1",
		SourceJobID:  "job-1",
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

func completedPredictionExpectation(prediction domain.Prediction, selectionID, selectionRunID int64) domain.PredictionExpectation {
	resultAvailableAt := prediction.AvailableAt
	return domain.PredictionExpectation{
		PredictionID: prediction.PredictionID, SourceJobID: prediction.SourceJobID,
		PredictionModelID: prediction.Model.Name, SelectionID: selectionID, SelectionRunID: selectionRunID,
		MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
		Outcomes:       append([]domain.PredictionOutcome(nil), prediction.Outcomes...),
		PredictionAsOf: prediction.PredictionAsOf, TaskAvailableAt: prediction.PredictionAsOf,
		Status: domain.PredictionExpectationCompleted, ResultAvailableAt: &resultAvailableAt,
	}
}

func pendingPredictionExpectation(prediction domain.Prediction, selectionID, selectionRunID int64) domain.PredictionExpectation {
	expectation := completedPredictionExpectation(prediction, selectionID, selectionRunID)
	expectation.Status = domain.PredictionExpectationPending
	expectation.ResultAvailableAt = nil
	return expectation
}

func completedPredictionExpectations(predictions []domain.Prediction, selectionID, selectionRunID int64) []domain.PredictionExpectation {
	result := make([]domain.PredictionExpectation, len(predictions))
	for index, prediction := range predictions {
		result[index] = completedPredictionExpectation(prediction, selectionID, selectionRunID)
	}
	return result
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
