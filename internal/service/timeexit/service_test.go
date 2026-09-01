package timeexit

import (
	"context"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type fakeTrades map[string][]domain.PositionExitTrade

func (source fakeTrades) ListOpenPositionExitTrades(_ context.Context, accountID string) ([]domain.PositionExitTrade, error) {
	return source[accountID], nil
}

type fakeMarkets struct {
	market domain.MarketSnapshot
}

func (source fakeMarkets) FindByCondition(_ context.Context, conditionID string) (domain.MarketSnapshot, bool, error) {
	if source.market.ConditionID != conditionID {
		return domain.MarketSnapshot{}, false, nil
	}
	return source.market, true, nil
}

type fakeBooks struct {
	books   []domain.OrderBookSnapshot
	targets []domain.BookTarget
}

func (source *fakeBooks) Capture(_ context.Context, _ time.Time, targets []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	source.targets = append([]domain.BookTarget(nil), targets...)
	return source.books, nil
}

type fakeExecutor struct {
	intents []domain.OrderIntent
}

func (executor *fakeExecutor) Submit(_ context.Context, intent domain.OrderIntent) (port.OrderSubmitResult, error) {
	executor.intents = append(executor.intents, intent)
	return port.OrderSubmitResult{
		Created: true,
		Order:   domain.Order{ID: "order-1", Intent: intent, Status: domain.OrderStatusSubmitting},
	}, nil
}

func TestRunSellsDueLotThroughLotAddressedFOKIntent(t *testing.T) {
	service, executor, trade, scheduledAt := fixture(t)
	trade.AvailableShares = "25.678"
	trade.RemainingShares = "25.678"
	trade.OriginalShares = "25.678"
	service.trades = fakeTrades{trade.ExecutionAccountID: {trade}}

	result, err := service.Run(context.Background(), scheduledAt)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Scanned != 1 || result.Due != 1 || result.Submitted != 1 || len(executor.intents) != 1 {
		t.Fatalf("run result = %#v, intents = %#v", result, executor.intents)
	}
	intent := executor.intents[0]
	if intent.Side != domain.SideSell || intent.Type != domain.OrderTypeLimit || intent.TimeInForce != domain.TimeInForceFOK ||
		intent.TargetLotID != trade.LotID || !intent.Price.Equal("0.50") || !intent.Size.Equal("7.25") {
		t.Fatalf("time-exit intent = %#v", intent)
	}
	if intent.Metadata["time_exit_reason"] != "HOLD_DURATION_48H" ||
		intent.Metadata["time_exit_held_seconds"] != "172800" {
		t.Fatalf("time-exit metadata = %#v", intent.Metadata)
	}
	if intent.ExpectedNegRisk == nil || *intent.ExpectedNegRisk != trade.NegRisk || intent.OutcomeIndex == nil || *intent.OutcomeIndex != trade.OutcomeIndex {
		t.Fatalf("time-exit market identity = %#v", intent)
	}
}

func TestRunDoesNotSellBeforeExact48HourBoundary(t *testing.T) {
	service, executor, trade, scheduledAt := fixture(t)
	trade.EnteredAt = scheduledAt.Add(-HoldDuration).Add(time.Second)
	service.trades = fakeTrades{trade.ExecutionAccountID: {trade}}

	result, err := service.Run(context.Background(), scheduledAt)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Scanned != 1 || result.Due != 0 || len(executor.intents) != 0 {
		t.Fatalf("run result = %#v, intents = %#v", result, executor.intents)
	}
}

func TestRunSkipsFullyReservedDueLot(t *testing.T) {
	service, executor, trade, scheduledAt := fixture(t)
	trade.AvailableShares = "0"
	trade.ReservedShares = trade.RemainingShares
	service.trades = fakeTrades{trade.ExecutionAccountID: {trade}}

	result, err := service.Run(context.Background(), scheduledAt)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(result.Lots) != 1 || result.Lots[0].Status != StatusFullyReserved || len(executor.intents) != 0 {
		t.Fatalf("run result = %#v, intents = %#v", result, executor.intents)
	}
}

func TestRunSkipsClosedMarket(t *testing.T) {
	service, executor, trade, scheduledAt := fixture(t)
	marketSource := service.markets.(fakeMarkets)
	marketSource.market.Active = false
	marketSource.market.Closed = true
	closedAt := scheduledAt.Add(-time.Minute)
	marketSource.market.ClosedAt = &closedAt
	service.markets = marketSource
	service.trades = fakeTrades{trade.ExecutionAccountID: {trade}}

	result, err := service.Run(context.Background(), scheduledAt)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(result.Lots) != 1 || result.Lots[0].Status != StatusMarketNotTradable || len(executor.intents) != 0 {
		t.Fatalf("run result = %#v, intents = %#v", result, executor.intents)
	}
}

func TestRunUsesStableClientOrderIDWithinSamePoll(t *testing.T) {
	service, executor, trade, scheduledAt := fixture(t)
	service.trades = fakeTrades{trade.ExecutionAccountID: {trade}}
	if _, err := service.Run(context.Background(), scheduledAt); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Run(context.Background(), scheduledAt); err != nil {
		t.Fatal(err)
	}
	if len(executor.intents) != 2 || executor.intents[0].ClientOrderID != executor.intents[1].ClientOrderID {
		t.Fatalf("client order ids = %#v", executor.intents)
	}
}

func fixture(t *testing.T) (*Service, *fakeExecutor, domain.PositionExitTrade, time.Time) {
	t.Helper()
	scheduledAt := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	trade := domain.PositionExitTrade{
		LotID: "lot-1", VenueTradeID: "venue-trade-1", OpeningOrderID: "opening-order-1",
		MarketID: "market-1", ConditionID: "condition-1", OutcomeIndex: 0,
		OutcomeName: "YES", TokenID: "token-yes", NegRisk: false,
		EnteredAt: scheduledAt.Add(-HoldDuration), OriginalShares: "12.50", RemainingShares: "12.50",
		AvailableShares: "12.50", ReservedShares: "0", EntryPrice: "0.40", RemainingCost: "5",
		ModelID: "model-a", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "account-1",
	}
	market := domain.MarketSnapshot{
		MarketID: trade.MarketID, ConditionID: trade.ConditionID, Active: true, AcceptingOrders: true,
		NegRisk: trade.NegRisk, TickSize: "0.01", ObservedAt: scheduledAt,
		Outcomes: []domain.MarketOutcome{
			{Index: 0, Name: "YES", TokenID: "token-yes"},
			{Index: 1, Name: "NO", TokenID: "token-no"},
		},
	}
	book := domain.OrderBookSnapshot{
		MarketID: trade.MarketID, ConditionID: trade.ConditionID, OutcomeIndex: trade.OutcomeIndex,
		TokenID: trade.TokenID, Status: domain.OrderBookStatusOK,
		SourceAt: scheduledAt, ObservedAt: scheduledAt, TickSize: "0.01", MinOrderSize: "1", DepthLimit: 15,
		BestBid: "0.50", BestAsk: "0.51",
		Bids: []domain.PriceLevel{{Price: "0.50", Size: "7.259"}, {Price: "0.49", Size: "100"}},
		Asks: []domain.PriceLevel{{Price: "0.51", Size: "100"}},
	}
	executor := &fakeExecutor{}
	service, err := New(Params{
		Trades: fakeTrades{trade.ExecutionAccountID: {trade}}, Markets: fakeMarkets{market: market},
		OrderBooks: &fakeBooks{books: []domain.OrderBookSnapshot{book}}, Executor: executor,
		Accounts: []string{trade.ExecutionAccountID}, Venue: "polymarket",
	})
	if err != nil {
		t.Fatal(err)
	}
	return service, executor, trade, scheduledAt
}
