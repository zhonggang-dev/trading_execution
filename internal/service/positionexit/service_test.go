package positionexit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/execution"
)

// fakeTradeSource 表示后端使用的 fakeTradeSource 类型。
type fakeTradeSource struct {
	trades []domain.PositionExitTrade
	calls  int
}

// fakePredictionSource 表示后端使用的 fakePredictionSource 类型。
type fakePredictionSource struct {
	snapshot domain.PredictionSnapshot
	calls    int
}

// Snapshot 返回模拟预测快照。
func (source *fakePredictionSource) Snapshot(context.Context, time.Time, time.Duration) (domain.PredictionSnapshot, error) {
	source.calls++
	return source.snapshot, nil
}

// fakeMarketUniverse 表示后端使用的 fakeMarketUniverse 类型。
type fakeMarketUniverse struct {
	market domain.MarketSnapshot
	calls  int
}

// FindByCondition 实现当前测试场景所需的辅助行为。
func (universe *fakeMarketUniverse) FindByCondition(context.Context, string) (domain.MarketSnapshot, bool, error) {
	universe.calls++
	return universe.market, true, nil
}

// ListOpenPositionExitTrades 返回模拟数据源中的测试列表。
func (source *fakeTradeSource) ListOpenPositionExitTrades(context.Context, string) ([]domain.PositionExitTrade, error) {
	source.calls++
	return append([]domain.PositionExitTrade(nil), source.trades...), nil
}

// fakeBookSource 表示后端使用的 fakeBookSource 类型。
type fakeBookSource struct {
	books   []domain.OrderBookSnapshot
	targets []domain.BookTarget
}

// Capture 返回模拟行情快照。
func (source *fakeBookSource) Capture(_ context.Context, _ time.Time, targets []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	source.targets = append([]domain.BookTarget(nil), targets...)
	return append([]domain.OrderBookSnapshot(nil), source.books...), nil
}

// fakeHistorySource 表示后端使用的 fakeHistorySource 类型。
type fakeHistorySource struct {
	histories []domain.MidPriceHistory
	lookback  time.Duration
}

// Capture 返回模拟行情快照。
func (source *fakeHistorySource) Capture(_ context.Context, _ time.Time, lookback time.Duration, _ []domain.BookTarget) ([]domain.MidPriceHistory, error) {
	source.lookback = lookback
	return append([]domain.MidPriceHistory(nil), source.histories...), nil
}

// fakeStrategy 表示后端使用的 fakeStrategy 类型。
type fakeStrategy struct {
	build func(domain.PositionExitRequest) domain.PositionExitResponse
	calls int
}

// EvaluatePositionExits 实现当前测试场景所需的辅助行为。
func (strategy *fakeStrategy) EvaluatePositionExits(_ context.Context, request domain.PositionExitRequest) (domain.PositionExitResponse, error) {
	strategy.calls++
	return strategy.build(request), nil
}

// memoryRecorder 表示后端使用的 memoryRecorder 类型。
type memoryRecorder struct {
	mu      sync.Mutex
	inputs  map[string]domain.PositionExitRequest
	outputs map[string]domain.PositionExitResponse
}

// newMemoryRecorder 创建测试所需的模拟对象。
func newMemoryRecorder() *memoryRecorder {
	return &memoryRecorder{inputs: make(map[string]domain.PositionExitRequest), outputs: make(map[string]domain.PositionExitResponse)}
}

// GetInput 返回模拟仓储中的测试记录。
func (recorder *memoryRecorder) GetInput(_ context.Context, cycle string) (domain.PositionExitRequest, error) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	value, ok := recorder.inputs[cycle]
	if !ok {
		return domain.PositionExitRequest{}, port.ErrPositionExitRunNotFound
	}
	return value, nil
}

// ClaimInput 模拟幂等认领并保存测试数据。
func (recorder *memoryRecorder) ClaimInput(_ context.Context, request domain.PositionExitRequest) (domain.PositionExitRequest, bool, error) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if stored, ok := recorder.inputs[request.CycleID]; ok {
		if stored.InputID != request.InputID {
			return domain.PositionExitRequest{}, false, port.ErrPositionExitConflict
		}
		return stored, false, nil
	}
	recorder.inputs[request.CycleID] = request
	return request, true, nil
}

// GetOutput 返回模拟仓储中的测试记录。
func (recorder *memoryRecorder) GetOutput(_ context.Context, cycle string) (domain.PositionExitResponse, error) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	value, ok := recorder.outputs[cycle]
	if !ok {
		return domain.PositionExitResponse{}, port.ErrPositionExitRunNotFound
	}
	return value, nil
}

// ClaimOutput 模拟幂等认领并保存测试数据。
func (recorder *memoryRecorder) ClaimOutput(_ context.Context, response domain.PositionExitResponse) (domain.PositionExitResponse, bool, error) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if stored, ok := recorder.outputs[response.CycleID]; ok {
		if stored.InputID != response.InputID {
			return domain.PositionExitResponse{}, false, port.ErrPositionExitConflict
		}
		return stored, false, nil
	}
	recorder.outputs[response.CycleID] = response
	return response, true, nil
}

// fakeExecutor 表示后端使用的 fakeExecutor 类型。
type fakeExecutor struct {
	intents []domain.OrderIntent
}

// Submit 记录模拟订单提交。
func (executor *fakeExecutor) Submit(_ context.Context, intent domain.OrderIntent) (execution.SubmitResult, error) {
	executor.intents = append(executor.intents, intent)
	return execution.SubmitResult{Created: len(executor.intents) == 1}, nil
}

// TestRunSendsPerLotDataAndExecutesPythonSellWithoutGoHoldGate 验证 Run Sends Per Lot Data And Executes Python Sell Without Go Hold Gate 场景下的行为。
func TestRunSendsPerLotDataAndExecutesPythonSellWithoutGoHoldGate(t *testing.T) {
	fixture := newFixture(t)
	service, err := New(fixture.params())
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Run(context.Background(), fixture.decisionAt)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(result.Runs) != 1 || result.Runs[0].Status != "EVALUATED" || len(result.Runs[0].Intents) != 1 {
		t.Fatalf("unexpected run result: %#v", result)
	}
	if fixture.historySource.lookback != 48*time.Hour {
		t.Fatalf("history lookback = %s", fixture.historySource.lookback)
	}
	if len(fixture.bookSource.targets) != 1 || fixture.bookSource.books[0].DepthLimit != 15 {
		t.Fatalf("book targets/books = %#v / %#v", fixture.bookSource.targets, fixture.bookSource.books)
	}
	request := result.Runs[0].Request
	if len(request.Trades) != 1 || request.Trades[0].VenueTradeID != "venue-trade-1" || request.Trades[0].AvailableShares != "5" {
		t.Fatalf("Python trade payload = %#v", request.Trades)
	}
	if request.PredictionSnapshotID != "prediction-snapshot-1" || len(request.Predictions) != 1 ||
		request.Predictions[0].PredictionAsOf != fixture.decisionAt.Add(-time.Minute) {
		t.Fatalf("point-in-time predictions = %#v", request.Predictions)
	}
	marketData := request.MarketData[0]
	if marketData.MarketStatus != domain.PositionExitMarketOpen || marketData.ClosedAt != nil ||
		marketData.OrderBook.BestAsk != "0.46" || marketData.OrderBook.BestBid != "0.45" ||
		marketData.OrderBook.BestAsk != marketData.OrderBook.Asks[0].Price {
		t.Fatalf("market/orderbook semantics = %#v", marketData)
	}
	if marketData.MidPriceHistory.MidPrices[0].P != "0.50" ||
		marketData.MidPriceHistory.MidPrices[0].IntervalEndAt != fixture.decisionAt.Add(-48*time.Hour) {
		t.Fatalf("raw price history = %#v", marketData.MidPriceHistory)
	}
	intent := fixture.executor.intents[0]
	if intent.TargetLotID != "lot-1" || intent.Side != domain.SideSell || intent.TimeInForce != domain.TimeInForceFOK || intent.WorstPrice != "0.45" {
		t.Fatalf("SELL intent = %#v", intent)
	}
	// The lot is only one hour old. Python chose SELL and Go enforced safety,
	// but deliberately did not reinterpret the strategy's holding-period rule.
	if request.DecisionAt.Sub(request.Trades[0].EnteredAt) != time.Hour {
		t.Fatalf("test lot age changed: %s", request.DecisionAt.Sub(request.Trades[0].EnteredAt))
	}
	if _, err := service.Run(context.Background(), fixture.decisionAt); err != nil {
		t.Fatalf("idempotent Run() error = %v", err)
	}
	if fixture.strategy.calls != 1 || fixture.tradeSource.calls != 1 || fixture.predictionSource.calls != 1 || fixture.marketUniverse.calls != 1 {
		t.Fatalf("retry re-fetched frozen input/output: strategy=%d trades=%d predictions=%d markets=%d",
			fixture.strategy.calls, fixture.tradeSource.calls, fixture.predictionSource.calls, fixture.marketUniverse.calls)
	}
	if len(fixture.executor.intents) != 2 || fixture.executor.intents[0].ClientOrderID != fixture.executor.intents[1].ClientOrderID {
		t.Fatalf("retry did not preserve client_order_id: %#v", fixture.executor.intents)
	}
}

// TestRunRejectsSellForResolvedMarket 验证 Run Rejects Sell For Resolved Market 场景下的行为。
func TestRunRejectsSellForResolvedMarket(t *testing.T) {
	fixture := newFixture(t)
	closedAt := fixture.decisionAt.Add(-time.Hour)
	fixture.marketUniverse.market.Resolved = true
	fixture.marketUniverse.market.Closed = true
	fixture.marketUniverse.market.Active = false
	fixture.marketUniverse.market.AcceptingOrders = false
	fixture.marketUniverse.market.ClosedAt = &closedAt
	service, err := New(fixture.params())
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Run(context.Background(), fixture.decisionAt)
	if !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("Run() error = %v, want resolved-market response rejection", err)
	}
	if len(fixture.executor.intents) != 0 {
		t.Fatalf("resolved market SELL reached execution: %#v", fixture.executor.intents)
	}
}

// TestRunRejectsGTCExitBeforeExecution 验证 Run Rejects GTC Exit Before Execution 场景下的行为。
func TestRunRejectsGTCExitBeforeExecution(t *testing.T) {
	fixture := newFixture(t)
	fixture.strategy.build = func(request domain.PositionExitRequest) domain.PositionExitResponse {
		response := sellResponse(request)
		response.Evaluations[0].Order.TimeInForce = domain.TimeInForceGTC
		return response
	}
	service, err := New(fixture.params())
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Run(context.Background(), fixture.decisionAt)
	if !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("Run() error = %v, want ErrInvalidResponse", err)
	}
	if len(fixture.executor.intents) != 0 {
		t.Fatalf("invalid GTC order reached execution: %#v", fixture.executor.intents)
	}
}

// fixture 表示后端使用的 fixture 类型。
type fixture struct {
	decisionAt       time.Time
	predictionSource *fakePredictionSource
	tradeSource      *fakeTradeSource
	marketUniverse   *fakeMarketUniverse
	bookSource       *fakeBookSource
	historySource    *fakeHistorySource
	strategy         *fakeStrategy
	recorder         *memoryRecorder
	executor         *fakeExecutor
}

// newFixture 创建测试所需的模拟对象。
func newFixture(t *testing.T) *fixture {
	t.Helper()
	decisionAt := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	trade := domain.PositionExitTrade{
		LotID: "lot-1", VenueTradeID: "venue-trade-1", OpeningOrderID: "opening-order-1",
		MarketID: "market-1", ConditionID: "condition-1", OutcomeIndex: 0,
		OutcomeName: "YES", TokenID: "token-yes", NegRisk: false,
		EnteredAt: decisionAt.Add(-time.Hour), OriginalShares: "5", RemainingShares: "5",
		AvailableShares: "5", ReservedShares: "0", EntryPrice: "0.5", RemainingCost: "2.5",
		ModelID: "model-a", StrategyID: "multfactor_v1", ExecutionAccountID: "wallet-model-a-v1",
	}
	book := domain.OrderBookSnapshot{
		MarketID: trade.MarketID, ConditionID: trade.ConditionID, OutcomeIndex: trade.OutcomeIndex, TokenID: trade.TokenID,
		Status: domain.OrderBookStatusOK, SourceAt: decisionAt.Add(-time.Second), ObservedAt: decisionAt,
		TickSize: "0.01", MinOrderSize: "1", DepthLimit: 15,
		Bids: []domain.PriceLevel{{Price: "0.45", Size: "100"}},
		Asks: []domain.PriceLevel{{Price: "0.46", Size: "100"}},
	}
	history := domain.MidPriceHistory{
		MarketID: trade.MarketID, ConditionID: trade.ConditionID, OutcomeIndex: trade.OutcomeIndex, TokenID: trade.TokenID,
		Status: domain.MidPriceHistoryStatusOK, WindowStart: decisionAt.Add(-48 * time.Hour), WindowEnd: decisionAt,
		FidelitySeconds: 60, Sampling: domain.MidPriceSamplingUpstreamRaw,
		MissingValues:      domain.MidPriceMissingValuePolicyNoFill,
		TimestampSemantics: domain.MidPriceTimestampSemanticsIntervalEndUTC,
		FetchedAt:          decisionAt, CoverageStart: decisionAt.Add(-48 * time.Hour), CoverageEnd: decisionAt,
		MidPrices: []domain.MidPricePoint{
			{IntervalEndAt: decisionAt.Add(-48 * time.Hour), P: "0.50"},
			{IntervalEndAt: decisionAt, P: "0.455"},
		},
	}
	strategy := &fakeStrategy{build: sellResponse}
	prediction := domain.Prediction{
		PredictionID: "prediction-1", SourceJobID: "job-1", SandboxID: "sandbox-1",
		MarketID: trade.MarketID, ConditionID: trade.ConditionID, Question: "Will it happen?",
		Domains: []string{"example.com"}, NegRisk: false,
		Outcomes: []domain.PredictionOutcome{
			{Index: 0, Name: "YES", TokenID: "token-yes", Probability: 0.55},
			{Index: 1, Name: "NO", TokenID: "token-no", Probability: 0.45},
		},
		PredictionAsOf: decisionAt.Add(-time.Minute), CompletedAt: decisionAt.Add(-50 * time.Second),
		AvailableAt: decisionAt.Add(-40 * time.Second), Model: domain.PredictionModel{Name: "model-a"},
	}
	market := domain.MarketSnapshot{
		MarketID: trade.MarketID, ConditionID: trade.ConditionID, Active: true, AcceptingOrders: true,
		TickSize: "0.01", ObservedAt: decisionAt, Outcomes: []domain.MarketOutcome{
			{Index: 0, Name: "YES", TokenID: "token-yes"}, {Index: 1, Name: "NO", TokenID: "token-no"},
		},
	}
	return &fixture{
		decisionAt: decisionAt,
		predictionSource: &fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion, SnapshotID: "prediction-snapshot-1",
			DecisionAt: decisionAt, GeneratedAt: decisionAt, Predictions: []domain.Prediction{prediction},
		}},
		tradeSource:    &fakeTradeSource{trades: []domain.PositionExitTrade{trade}},
		marketUniverse: &fakeMarketUniverse{market: market},
		bookSource:     &fakeBookSource{books: []domain.OrderBookSnapshot{book}},
		historySource:  &fakeHistorySource{histories: []domain.MidPriceHistory{history}},
		strategy:       strategy, recorder: newMemoryRecorder(), executor: &fakeExecutor{},
	}
}

// params 实现当前测试场景所需的辅助行为。
func (fixture *fixture) params() Params {
	return Params{
		PredictionSource: fixture.predictionSource, TradeSource: fixture.tradeSource,
		MarketUniverse: fixture.marketUniverse, OrderBookSource: fixture.bookSource,
		MidPriceSource: fixture.historySource, Strategy: fixture.strategy,
		Recorder: fixture.recorder, Executor: fixture.executor,
		Bindings: []domain.StrategyExecutionContext{{
			ModelID: "model-a", StrategyID: "multfactor_v1", ExecutionAccountID: "wallet-model-a-v1",
		}},
		Venue: "polymarket", Now: func() time.Time { return fixture.decisionAt.Add(time.Second) },
	}
}

// sellResponse 实现当前测试场景所需的辅助行为。
func sellResponse(request domain.PositionExitRequest) domain.PositionExitResponse {
	trade := request.Trades[0]
	return domain.PositionExitResponse{
		SchemaVersion: domain.PositionExitOutputSchemaVersion,
		CycleID:       request.CycleID, InputID: request.InputID, Context: request.Context,
		DecidedAt: request.DecisionAt.Add(time.Second),
		Evaluations: []domain.PositionExitEvaluation{{
			DecisionID: "exit-decision-lot-1", LotID: trade.LotID,
			Action: domain.PositionExitActionSell, ReasonCode: domain.PositionExitReasonTimeExit48H,
			Evidence: domain.PositionExitEvidence{HeldSeconds: 3600, BestBid: "0.45"},
			Order: &domain.StrategyOrderParams{
				Side: domain.SideSell, Type: domain.OrderTypeLimit, WorstPrice: "0.45",
				Size: "5", TimeInForce: domain.TimeInForceFOK,
			},
		}},
	}
}
