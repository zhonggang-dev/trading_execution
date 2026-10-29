package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

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
func (*routeTestExecution) FinalizeCancellation(context.Context, string) (domain.Order, error) {
	return domain.Order{}, nil
}
func (*routeTestExecution) Events(context.Context, string) ([]domain.OrderEvent, error) {
	return nil, nil
}
func (*routeTestExecution) Attempts(context.Context, string) ([]domain.OrderAttempt, error) {
	return nil, nil
}

func TestAccountEntryDisabledFollowsTheKalshiAccountMode(t *testing.T) {
	if got := accountEntryDisabled("kalshi:main", []string{"wallet-1", "kalshi:main"}); !reflect.DeepEqual(got, []string{"kalshi:main"}) {
		t.Fatalf("SELL_ONLY/PAUSED Kalshi gate=%#v", got)
	}
	// The Polymarket wallet's mode no longer leaks onto the Kalshi account.
	if got := accountEntryDisabled("kalshi:main", []string{"main", "wallet-1"}); len(got) != 0 {
		t.Fatalf("ACTIVE Kalshi gate=%#v", got)
	}
}

func TestKalshiPreflightFailureLeavesPolymarketRouteOperational(t *testing.T) {
	cfg := config.Config{
		Kalshi: config.Kalshi{MarketDataEnabled: true, LiveAccounts: []config.KalshiLiveAccount{{
			ExecutionAccountID: "kalshi:main", APIKeyID: "unavailable-key", PrivateKeyPath: "/tmp/does-not-exist/kalshi.pem",
		}}},
		DecisionCycle: config.DecisionCycle{PredictionModelID: "echo", Accounts: []config.DecisionAccount{
			{ExecutionAccountID: "main", MarketSource: domain.MarketSourcePolymarket, StrategyID: domain.StrategyIDMultfactorV2, Mode: config.DecisionAccountActive},
			{ExecutionAccountID: "kalshi:main", MarketSource: domain.MarketSourceKalshi, StrategyID: domain.StrategyIDMultfactorV2, Mode: config.DecisionAccountActive},
		}},
	}
	primary := &routeTestExecution{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	composition, err := composeKalshiExecution(context.Background(), cfg, nil, memory.NewOrderRepository(), nil, nil, primary, logger)
	if err != nil {
		t.Fatalf("composeKalshiExecution() error = %v", err)
	}
	if len(composition.activeAccounts) != 0 {
		t.Fatalf("active Kalshi accounts = %#v, want none after failed preflight", composition.activeAccounts)
	}
	if len(composition.settlements) != 0 {
		t.Fatalf("settlement services = %d, want none for a route that failed preflight", len(composition.settlements))
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
	kalshiIntent.ExecutionAccountID = "kalshi:main"
	if composition.execution.Enabled(kalshiIntent) {
		t.Fatal("failed Kalshi route must remain dry-run")
	}
	if _, err := composition.execution.Submit(context.Background(), kalshiIntent); err == nil {
		t.Fatal("failed Kalshi route submission must fail closed")
	}
	if composition.execution.StrategyInputEnabled(domain.StrategyExecutionBinding{
		ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "kalshi:main", MarketSource: domain.MarketSourceKalshi,
	}) {
		t.Fatal("a Kalshi account whose preflight failed must not be sent to the strategy")
	}
}

func TestComposeKalshiRejectsCredentialWithoutKalshiAccount(t *testing.T) {
	cfg := config.Config{
		Kalshi: config.Kalshi{MarketDataEnabled: true, LiveAccounts: []config.KalshiLiveAccount{{
			ExecutionAccountID: "kalshi:wallet-7", APIKeyID: "k", PrivateKeyPath: "/k.pem",
		}}},
		DecisionCycle: config.DecisionCycle{PredictionModelID: "echo", Accounts: []config.DecisionAccount{
			{ExecutionAccountID: "main", MarketSource: domain.MarketSourcePolymarket, StrategyID: "s", Mode: config.DecisionAccountActive},
		}},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, err := composeKalshiExecution(context.Background(), cfg, nil, memory.NewOrderRepository(), nil, nil,
		&routeTestExecution{}, logger); err == nil {
		t.Fatal("a Kalshi credential without a KALSHI account row must fail startup")
	}
}

func rateLimitedError() error {
	return fmt.Errorf("probe Kalshi balance: %w", &port.VenueError{
		Kind: port.VenueErrorUnavailable, Code: "KALSHI_RATE_LIMITED", HTTPStatus: 429,
	})
}

func TestRetryKalshiRateLimitRecoversAfterBackoff(t *testing.T) {
	calls := 0
	var waits []time.Duration
	value, err := retryKalshiRateLimit(context.Background(),
		func(_ context.Context, delay time.Duration) error { waits = append(waits, delay); return nil },
		func(context.Context) (string, error) {
			calls++
			if calls <= 2 {
				return "", rateLimitedError()
			}
			return "ok", nil
		})
	if err != nil || value != "ok" || calls != 3 {
		t.Fatalf("value=%q err=%v calls=%d", value, err, calls)
	}
	if !reflect.DeepEqual(waits, []time.Duration{time.Second, 2 * time.Second}) {
		t.Fatalf("waits=%v", waits)
	}
}

func TestRetryKalshiRateLimitGivesUpAndKeepsError(t *testing.T) {
	calls := 0
	_, err := retryKalshiRateLimit(context.Background(),
		func(context.Context, time.Duration) error { return nil },
		func(context.Context) (string, error) { calls++; return "", rateLimitedError() })
	if !kalshiRateLimited(err) || calls != len(kalshiPreflightRetryDelays)+1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestRetryKalshiRateLimitDoesNotRetryOtherErrors(t *testing.T) {
	calls := 0
	boom := errors.New("auth failed")
	_, err := retryKalshiRateLimit(context.Background(),
		func(context.Context, time.Duration) error { t.Fatal("must not wait"); return nil },
		func(context.Context) (string, error) { calls++; return "", boom })
	if !errors.Is(err, boom) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestRetryKalshiRateLimitStopsWhenContextEnds(t *testing.T) {
	calls := 0
	ctx, cancel := context.WithCancel(context.Background())
	_, err := retryKalshiRateLimit(ctx,
		func(ctx context.Context, _ time.Duration) error { cancel(); return ctx.Err() },
		func(context.Context) (string, error) { calls++; return "", rateLimitedError() })
	if !kalshiRateLimited(err) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}
