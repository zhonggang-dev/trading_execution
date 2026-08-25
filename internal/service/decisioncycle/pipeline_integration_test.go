package decisioncycle

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/adapter/paper"
	adapterrisk "github.com/UniPat-AI/trading_execution/internal/adapter/risk"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/execution"
)

// replayDecisionRecorder 表示后端使用的 replayDecisionRecorder 类型。
type replayDecisionRecorder struct {
	inputs     map[string]domain.StrategyDecisionRequest
	outputs    map[string]domain.StrategyDecisionResponse
	deliveries map[string][]domain.DecisionIntentDelivery
}

// newReplayDecisionRecorder 创建可验证冻结输入输出重放语义的内存记录器。
func newReplayDecisionRecorder() *replayDecisionRecorder {
	return &replayDecisionRecorder{
		inputs:     make(map[string]domain.StrategyDecisionRequest),
		outputs:    make(map[string]domain.StrategyDecisionResponse),
		deliveries: make(map[string][]domain.DecisionIntentDelivery),
	}
}

// ClaimInput 幂等保存同一决策周期首次冻结的算法输入。
func (recorder *replayDecisionRecorder) ClaimInput(_ context.Context, request domain.StrategyDecisionRequest) (domain.StrategyDecisionRequest, bool, error) {
	if stored, ok := recorder.inputs[request.CycleID]; ok {
		if stored.InputID != request.InputID {
			return domain.StrategyDecisionRequest{}, false, fmt.Errorf("decision input conflict")
		}
		return stored, false, nil
	}
	recorder.inputs[request.CycleID] = request
	return request, true, nil
}

// ClaimOutput 幂等保存同一决策周期首次通过校验的算法输出。
func (recorder *replayDecisionRecorder) ClaimOutput(_ context.Context, response domain.StrategyDecisionResponse, intents []domain.OrderIntent, submissionEnabled bool) (domain.StrategyDecisionResponse, bool, error) {
	if stored, ok := recorder.outputs[response.CycleID]; ok {
		if stored.InputID != response.InputID {
			return domain.StrategyDecisionResponse{}, false, fmt.Errorf("decision output conflict")
		}
		return stored, false, nil
	}
	recorder.outputs[response.CycleID] = response
	if submissionEnabled {
		deliveries := make([]domain.DecisionIntentDelivery, len(intents))
		for index, intent := range intents {
			deliveries[index] = domain.DecisionIntentDelivery{
				CycleID: response.CycleID, ClientOrderID: intent.ClientOrderID,
				Sequence: index, Intent: intent, Status: domain.DecisionIntentPending,
			}
		}
		recorder.deliveries[response.CycleID] = deliveries
	}
	return response, true, nil
}

func (recorder *replayDecisionRecorder) CountUnresolvedIntentsForAccounts(_ context.Context, executionAccountIDs []string) (int, error) {
	accounts := make(map[string]struct{}, len(executionAccountIDs))
	for _, accountID := range executionAccountIDs {
		accounts[accountID] = struct{}{}
	}
	count := 0
	for _, deliveries := range recorder.deliveries {
		for _, delivery := range deliveries {
			if delivery.Status != domain.DecisionIntentPending && delivery.Status != domain.DecisionIntentSubmitting {
				continue
			}
			if _, disabled := accounts[delivery.Intent.ExecutionAccountID]; disabled {
				count++
			}
		}
	}
	return count, nil
}

func (recorder *replayDecisionRecorder) ClaimPendingIntents(_ context.Context, activeAccountIDs []string, cycleID string, side domain.Side, limit int) ([]domain.DecisionIntentDelivery, error) {
	active := make(map[string]struct{}, len(activeAccountIDs))
	for _, accountID := range activeAccountIDs {
		active[accountID] = struct{}{}
	}
	result := make([]domain.DecisionIntentDelivery, 0)
	for key, deliveries := range recorder.deliveries {
		if cycleID != "" && key != cycleID {
			continue
		}
		for index := range deliveries {
			if deliveries[index].Status != domain.DecisionIntentPending ||
				(side != "" && deliveries[index].Intent.Side != side) || len(result) >= limit {
				continue
			}
			if _, enabled := active[deliveries[index].Intent.ExecutionAccountID]; !enabled {
				continue
			}
			deliveries[index].Status = domain.DecisionIntentSubmitting
			deliveries[index].Attempt++
			result = append(result, deliveries[index])
		}
		recorder.deliveries[key] = deliveries
	}
	return result, nil
}

func (recorder *replayDecisionRecorder) RequeueStaleSubmitting(_ context.Context, _ []string, _ time.Time, _ domain.Side, _ int) (int, error) {
	return 0, nil
}

func (recorder *replayDecisionRecorder) CompleteIntent(_ context.Context, clientOrderID string, attempt int, completion domain.DecisionIntentCompletion) error {
	for key, deliveries := range recorder.deliveries {
		for index := range deliveries {
			if deliveries[index].ClientOrderID == clientOrderID && deliveries[index].Attempt == attempt {
				deliveries[index].Status = completion.Status
				deliveries[index].OrderID = completion.OrderID
				deliveries[index].OrderStatus = completion.OrderStatus
				deliveries[index].LastError = completion.LastError
				recorder.deliveries[key] = deliveries
				return nil
			}
		}
	}
	return port.ErrDecisionIntentConflict
}

func (recorder *replayDecisionRecorder) ListIntents(_ context.Context, cycleID string) ([]domain.DecisionIntentDelivery, error) {
	return append([]domain.DecisionIntentDelivery(nil), recorder.deliveries[cycleID]...), nil
}

// replayStrategy 表示后端使用的 replayStrategy 类型。
type replayStrategy struct {
	response domain.StrategyDecisionResponse
	calls    int
}

// Decide 返回当前配置的模拟算法输出并记录调用次数。
func (strategy *replayStrategy) Decide(_ context.Context, request domain.StrategyDecisionRequest) (domain.StrategyDecisionResponse, error) {
	strategy.calls++
	response := strategy.response
	response.CycleID = request.CycleID
	response.InputID = request.InputID
	response.Context = request.Context
	return response, nil
}

// countingVenue 表示后端使用的 countingVenue 类型。
type countingVenue struct {
	delegate   port.Venue
	placeCalls int
}

// Name 返回被包装交易场所的名称。
func (venue *countingVenue) Name() string {
	return venue.delegate.Name()
}

// Place 记录下单次数后调用被包装交易场所。
func (venue *countingVenue) Place(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	venue.placeCalls++
	return venue.delegate.Place(ctx, order)
}

// Cancel 调用被包装交易场所执行撤单。
func (venue *countingVenue) Cancel(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	return venue.delegate.Cancel(ctx, order)
}

// Get 调用被包装交易场所查询订单。
func (venue *countingVenue) Get(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	return venue.delegate.Get(ctx, order)
}

// pipelineFixture 表示后端使用的 pipelineFixture 类型。
type pipelineFixture struct {
	decisionAt   time.Time
	prediction   domain.Prediction
	strategy     *replayStrategy
	recorder     *replayDecisionRecorder
	repository   *memory.OrderRepository
	reservations *paper.ReservationManager
	venue        *countingVenue
	service      *Service
}

// newPipelineFixture 组装算法决策到真实执行服务的完整纸交易测试链路。
func newPipelineFixture(t *testing.T, maxNotional domain.Decimal, submitEnabled ...bool) *pipelineFixture {
	t.Helper()
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	strategy := &replayStrategy{response: validSubmitResponse(decisionAt, prediction)}
	recorder := newReplayDecisionRecorder()
	repository := memory.NewOrderRepository()
	reservations := paper.NewReservationManager()
	venue := &countingVenue{delegate: paper.NewVenue("polymarket-paper")}
	guard, err := adapterrisk.NewStaticGuard(adapterrisk.StaticGuardParams{
		MaxOrderNotional: maxNotional,
	})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: guard,
		MarketValidator: paper.NewMarketValidator(), Reservations: reservations,
		Now:   func() time.Time { return decisionAt.Add(5 * time.Second) },
		NewID: func() string { return "order-pipeline-1" },
	})
	if err != nil {
		t.Fatal(err)
	}
	enabled := true
	if len(submitEnabled) != 0 {
		enabled = submitEnabled[0]
	}
	service, err := newTestService(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion, SnapshotID: "predsnap-pipeline-1",
			DecisionAt: decisionAt, CompletedAfter: decisionAt.Add(-3 * time.Hour),
			GeneratedAt: decisionAt.Add(time.Second), Predictions: []domain.Prediction{prediction},
			ExpectedPredictions: []domain.PredictionExpectation{completedPredictionExpectation(prediction, 1, 1)},
		}},
		PositionSource: fakePositionSource{},
		OrderBookSource: &fakeOrderBookSource{books: []domain.OrderBookSnapshot{{
			MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
			OutcomeIndex: 0, TokenID: prediction.Outcomes[0].TokenID,
			Status: domain.OrderBookStatusOK, SourceAt: decisionAt.Add(2 * time.Second),
			ObservedAt: decisionAt.Add(3 * time.Second), DepthLimit: domain.StrategyOrderBookDepth,
			TickSize: "0.01", MinOrderSize: "1",
			Bids: []domain.PriceLevel{{Price: "0.48", Size: "10"}},
			Asks: []domain.PriceLevel{{Price: "0.50", Size: "20"}},
		}}},
		MidPriceSource: &fakeMidPriceHistorySource{histories: []domain.MidPriceHistory{
			validMidPriceHistory(prediction, 0, decisionAt),
		}},
		Strategy: strategy, Recorder: recorder, Executor: executor, SubmitEnabled: enabled,
		RequireCompleteModelCoverage: enabled,
		Bindings:                     []domain.StrategyExecutionBinding{testExecutionBinding()}, Venue: "polymarket-paper",
		Now: func() time.Time { return decisionAt.Add(5 * time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	return &pipelineFixture{
		decisionAt: decisionAt, prediction: prediction, strategy: strategy,
		recorder: recorder, repository: repository, reservations: reservations,
		venue: venue, service: service,
	}
}

// validSubmitResponse 构造一笔买入和一个跳过结果组成的合法算法输出。
func validSubmitResponse(decisionAt time.Time, prediction domain.Prediction) domain.StrategyDecisionResponse {
	return domain.StrategyDecisionResponse{
		SchemaVersion: domain.StrategyOutputSchemaVersion,
		DecidedAt:     decisionAt.Add(4 * time.Second),
		Evaluations: []domain.StrategyEvaluation{
			{
				DecisionID: "pred-1:yes", PredictionID: prediction.PredictionID,
				MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
				OutcomeIndex: 0, TokenID: prediction.Outcomes[0].TokenID,
				Action: domain.StrategyActionSubmit, ReasonCode: domain.StrategyReasonEntrySignal,
				Evidence: domain.StrategyEvidence{
					Probability: 0.7, Edge: "0.20",
					Metrics: map[string]string{
						"best_ask": "0.50", "near_logdiff_usd": "1.2", "rel_spread": "0.04",
						"MOM": "0.01", "MACD_SIGNAL": "0.02",
					},
				},
				Order: &domain.StrategyOrderParams{
					Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.50",
					Size: "20", TimeInForce: domain.TimeInForceFOK,
				},
			},
			{
				DecisionID: "pred-1:no", PredictionID: prediction.PredictionID,
				MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
				OutcomeIndex: 1, TokenID: prediction.Outcomes[1].TokenID,
				Action: domain.StrategyActionSkip, ReasonCode: domain.StrategyReasonInvalidBook,
				Evidence: domain.StrategyEvidence{Probability: 0.3},
			},
		},
	}
}

// TestAlgorithmToExecutionPaperPipelineAndReplay 验证算法下单可贯通执行链路且周期重放不会重复触达交易所。
func TestAlgorithmToExecutionPaperPipelineAndReplay(t *testing.T) {
	fixture := newPipelineFixture(t, "100")
	result, err := fixture.service.Run(context.Background(), fixture.decisionAt)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(result.Runs) != 1 || len(result.Runs[0].Intents) != 1 {
		t.Fatalf("pipeline result = %#v", result)
	}
	intentResult := result.Runs[0].Intents[0]
	if !intentResult.Result.Created || intentResult.Result.Order.Status != domain.OrderStatusLive {
		t.Fatalf("submitted order = %#v", intentResult.Result)
	}
	order := intentResult.Result.Order
	if order.MarketValidation == nil || order.MarketValidation.Mode != "PAPER_BYPASS" ||
		order.Intent.TimeInForce != domain.TimeInForceFOK || !order.Intent.Price.Equal("0.50") {
		t.Fatalf("validated order = %#v", order)
	}
	events, err := fixture.repository.Events(context.Background(), order.ID)
	if err != nil {
		t.Fatal(err)
	}
	attempts, err := fixture.repository.Attempts(context.Background(), order.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 6 || events[0].ToStatus != domain.OrderStatusReceived ||
		events[len(events)-1].ToStatus != domain.OrderStatusLive {
		t.Fatalf("order events = %#v", events)
	}
	if len(attempts) != 1 || attempts[0].Outcome != domain.AttemptOutcomeSucceeded {
		t.Fatalf("submit attempts = %#v", attempts)
	}
	reservation, ok := fixture.reservations.Get(order.ID)
	if !ok || reservation.Status != domain.ReservationStatusActive {
		t.Fatalf("reservation = %#v, found = %v", reservation, ok)
	}

	changedResponse := validSubmitResponse(fixture.decisionAt, fixture.prediction)
	changedResponse.Evaluations[0].Action = domain.StrategyActionSkip
	changedResponse.Evaluations[0].ReasonCode = domain.StrategyReasonEdgeTooLow
	changedResponse.Evaluations[0].Order = nil
	fixture.strategy.response = changedResponse
	replayed, err := fixture.service.Run(context.Background(), fixture.decisionAt)
	if err != nil {
		t.Fatalf("replayed Run() error = %v", err)
	}
	replayedIntent := replayed.Runs[0].Intents[0]
	if replayedIntent.Result.Created || replayedIntent.Result.Order.ID != order.ID {
		t.Fatalf("replayed submission = %#v", replayedIntent.Result)
	}
	if fixture.strategy.calls != 2 || fixture.venue.placeCalls != 1 {
		t.Fatalf("strategy calls = %d, venue place calls = %d", fixture.strategy.calls, fixture.venue.placeCalls)
	}
	afterEvents, _ := fixture.repository.Events(context.Background(), order.ID)
	afterAttempts, _ := fixture.repository.Attempts(context.Background(), order.ID)
	if len(afterEvents) != len(events) || len(afterAttempts) != len(attempts) {
		t.Fatalf("replay changed audit history: events %d->%d, attempts %d->%d", len(events), len(afterEvents), len(attempts), len(afterAttempts))
	}
}

// TestAlgorithmOrderRejectedByHardRiskBeforeVenue 验证算法合法下单超过硬风控时被拒绝且不会预占或触达交易所。
func TestAlgorithmOrderRejectedByHardRiskBeforeVenue(t *testing.T) {
	fixture := newPipelineFixture(t, "5")
	result, err := fixture.service.Run(context.Background(), fixture.decisionAt)
	var rejection *port.Rejection
	if !errors.As(err, &rejection) || rejection.Code != "ORDER_NOTIONAL_LIMIT" {
		t.Fatalf("Run() error = %v, rejection = %#v", err, rejection)
	}
	if len(result.Runs) != 1 || len(result.Runs[0].Intents) != 1 {
		t.Fatalf("pipeline result = %#v", result)
	}
	order := result.Runs[0].Intents[0].Result.Order
	if order.Status != domain.OrderStatusRejected || order.FailureCode != "ORDER_NOTIONAL_LIMIT" {
		t.Fatalf("rejected order = %#v", order)
	}
	if fixture.venue.placeCalls != 0 {
		t.Fatalf("hard-risk rejection reached venue %d times", fixture.venue.placeCalls)
	}
	if _, ok := fixture.reservations.Get(order.ID); ok {
		t.Fatal("hard-risk rejection unexpectedly created a reservation")
	}
}

// TestDecisionCycleSubmissionGateRecordsOutputWithoutCreatingAnOrder proves
// that the independent production gate still exercises the complete upstream
// decision path while preventing any call into execution.Submit.
func TestDecisionCycleSubmissionGateRecordsOutputWithoutCreatingAnOrder(t *testing.T) {
	fixture := newPipelineFixture(t, "100", false)
	result, err := fixture.service.Run(context.Background(), fixture.decisionAt)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(result.Runs) != 1 || len(result.Runs[0].Intents) != 1 ||
		!result.Runs[0].Intents[0].SubmissionDisabled {
		t.Fatalf("submission-disabled result = %#v", result)
	}
	if fixture.strategy.calls != 1 || len(fixture.recorder.inputs) != 1 || len(fixture.recorder.outputs) != 1 {
		t.Fatalf("strategy/recorder calls = %d/%d/%d", fixture.strategy.calls, len(fixture.recorder.inputs), len(fixture.recorder.outputs))
	}
	if fixture.venue.placeCalls != 0 {
		t.Fatalf("submission-disabled cycle reached venue %d times", fixture.venue.placeCalls)
	}
	if _, err := fixture.repository.GetByClientOrderID(context.Background(), result.Runs[0].Intents[0].Intent.ClientOrderID); !errors.Is(err, port.ErrOrderNotFound) {
		t.Fatalf("submission-disabled cycle created an order: %v", err)
	}
}
