package positionexit

import (
	"context"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/adapter/paper"
	adapterrisk "github.com/UniPat-AI/trading_execution/internal/adapter/risk"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/execution"
)

// TestAlgorithmSellToExecutionPaperPipelineAndReplay 验证算法卖出可贯通真实执行服务且重放不会重复提交交易所。
func TestAlgorithmSellToExecutionPaperPipelineAndReplay(t *testing.T) {
	fixture := newFixture(t)
	repository := memory.NewOrderRepository()
	reservations := paper.NewReservationManager()
	guard, err := adapterrisk.NewStaticGuard(adapterrisk.StaticGuardParams{
		MaxOrderSize: "100", MaxOrderNotional: "100",
	})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := execution.New(execution.Params{
		Repository: repository, Venue: paper.NewVenue("polymarket"), Guard: guard,
		MarketValidator: paper.NewMarketValidator(), Reservations: reservations,
		Now:   func() time.Time { return fixture.decisionAt.Add(2 * time.Second) },
		NewID: func() string { return "order-exit-pipeline-1" },
	})
	if err != nil {
		t.Fatal(err)
	}
	params := fixture.params()
	params.Executor = executor
	service, err := New(params)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Run(context.Background(), fixture.decisionAt)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(result.Runs) != 1 || len(result.Runs[0].Intents) != 1 {
		t.Fatalf("exit pipeline result = %#v", result)
	}
	intentResult := result.Runs[0].Intents[0]
	order := intentResult.Result.Order
	if !intentResult.Result.Created || order.Status != domain.OrderStatusLive ||
		order.Intent.Side != domain.SideSell || order.Intent.TargetLotID != "lot-1" ||
		order.Intent.TimeInForce != domain.TimeInForceFOK || !order.Intent.Price.Equal("0.45") {
		t.Fatalf("exit order = %#v", order)
	}
	reservation, ok := reservations.Get(order.ID)
	if !ok || reservation.Status != domain.ReservationStatusActive ||
		!reservation.InitialReservedShares.Equal("5") || !reservation.RemainingReservedShares.Equal("5") {
		t.Fatalf("sell reservation = %#v, found = %v", reservation, ok)
	}
	events, err := repository.Events(context.Background(), order.ID)
	if err != nil {
		t.Fatal(err)
	}
	attempts, err := repository.Attempts(context.Background(), order.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 6 || len(attempts) != 1 || attempts[0].Outcome != domain.AttemptOutcomeSucceeded {
		t.Fatalf("exit audit events/attempts = %#v / %#v", events, attempts)
	}

	replayed, err := service.Run(context.Background(), fixture.decisionAt)
	if err != nil {
		t.Fatalf("replayed Run() error = %v", err)
	}
	replayedOrder := replayed.Runs[0].Intents[0].Result
	if replayedOrder.Created || replayedOrder.Order.ID != order.ID || fixture.strategy.calls != 1 {
		t.Fatalf("replayed exit = %#v, strategy calls = %d", replayedOrder, fixture.strategy.calls)
	}
	afterEvents, _ := repository.Events(context.Background(), order.ID)
	afterAttempts, _ := repository.Attempts(context.Background(), order.ID)
	if len(afterEvents) != len(events) || len(afterAttempts) != len(attempts) {
		t.Fatalf("replay changed exit audit history: events %d->%d, attempts %d->%d", len(events), len(afterEvents), len(attempts), len(afterAttempts))
	}
}
