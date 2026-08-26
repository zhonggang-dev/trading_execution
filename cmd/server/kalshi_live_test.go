package main

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type routeTestExecution struct{ submitted []domain.OrderIntent }

func (execution *routeTestExecution) Submit(_ context.Context, intent domain.OrderIntent) (port.OrderSubmitResult, error) {
	execution.submitted = append(execution.submitted, intent)
	return port.OrderSubmitResult{}, nil
}
func (*routeTestExecution) Resume(context.Context, string) (domain.Order, error) {
	return domain.Order{}, nil
}
func (*routeTestExecution) Get(context.Context, string) (domain.Order, error) {
	return domain.Order{}, nil
}
func (*routeTestExecution) Refresh(context.Context, string) (domain.Order, error) {
	return domain.Order{}, nil
}
func (*routeTestExecution) Cancel(context.Context, string) (domain.Order, error) {
	return domain.Order{}, nil
}
func (*routeTestExecution) Events(context.Context, string) ([]domain.OrderEvent, error) {
	return nil, nil
}
func (*routeTestExecution) Attempts(context.Context, string) ([]domain.OrderAttempt, error) {
	return nil, nil
}

type routeTestPositions struct{ accounts []string }

func (source *routeTestPositions) ListOpenLots(_ context.Context, accountID string) ([]domain.PositionLot, error) {
	source.accounts = append(source.accounts, accountID)
	return nil, nil
}

func TestInternalEntryDisabledAccountsPreservesLogicalGate(t *testing.T) {
	if got := internalEntryDisabledAccounts("main", "kalshi:main", []string{"main", "wallet-1"}); !reflect.DeepEqual(got, []string{"kalshi:main"}) {
		t.Fatalf("main Kalshi gate=%#v", got)
	}
	if got := internalEntryDisabledAccounts("wallet-7", "kalshi:wallet-7", []string{"main", "wallet-1"}); len(got) != 0 {
		t.Fatalf("wallet-7 Kalshi gate=%#v", got)
	}
}

func TestKalshiPreflightFailureLeavesPolymarketRouteOperational(t *testing.T) {
	binding := domain.StrategyExecutionBinding{PredictionModelID: "echo", ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "main"}
	cfg := config.Config{
		Kalshi: config.Kalshi{MarketDataEnabled: true, LiveBindings: []config.KalshiLiveBinding{{
			ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "main",
			APIKeyID: "unavailable-key", PrivateKeyPath: "/tmp/does-not-exist/kalshi.pem",
		}}},
		DecisionCycle: config.DecisionCycle{Bindings: []domain.StrategyExecutionBinding{binding}},
	}
	primary := &routeTestExecution{}
	positions := &routeTestPositions{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	composition, err := composeKalshiExecution(context.Background(), cfg, nil, memory.NewOrderRepository(), nil, nil, primary, positions, logger)
	if err != nil {
		t.Fatalf("composeKalshiExecution() error = %v", err)
	}
	if len(composition.activeAccounts) != 0 {
		t.Fatalf("active Kalshi accounts = %#v, want none after failed preflight", composition.activeAccounts)
	}
	polymarketIntent := domain.OrderIntent{
		ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "main",
		MarketSource: domain.MarketSourcePolymarket, Venue: "polymarket", MarketID: "pm-market", TokenID: "pm-token",
		ClientOrderID: "pm-order", Metadata: map[string]string{"sentinel": "unchanged"},
	}
	if !composition.execution.Enabled(polymarketIntent) {
		t.Fatal("Polymarket route was disabled by Kalshi preflight failure")
	}
	if _, err := composition.execution.Submit(context.Background(), polymarketIntent); err != nil {
		t.Fatalf("Polymarket submit after Kalshi preflight failure: %v", err)
	}
	if len(primary.submitted) != 1 || !reflect.DeepEqual(primary.submitted[0], polymarketIntent) {
		t.Fatalf("Polymarket intent changed or was not submitted: %#v", primary.submitted)
	}
	kalshiIntent := polymarketIntent
	kalshiIntent.MarketSource = domain.MarketSourceKalshi
	if composition.execution.Enabled(kalshiIntent) {
		t.Fatal("failed Kalshi route must remain dry-run")
	}
	if _, err := composition.execution.Submit(context.Background(), kalshiIntent); err == nil {
		t.Fatal("failed Kalshi route submission must fail closed")
	}
	if _, err := composition.positionSource.ListOpenLots(context.Background(), "main"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(positions.accounts, []string{"main"}) {
		t.Fatalf("Polymarket position source changed after Kalshi preflight failure: %#v", positions.accounts)
	}
}
