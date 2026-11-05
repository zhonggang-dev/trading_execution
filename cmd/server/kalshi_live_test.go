package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/accountmaintenance"
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
	registry := accountmaintenance.New()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	composition, err := composeKalshiExecution(context.Background(), cfg, nil, memory.NewOrderRepository(), nil, nil, primary, registry, logger)
	if err != nil {
		t.Fatalf("composeKalshiExecution() error = %v", err)
	}
	if len(composition.activeAccounts) != 0 {
		t.Fatalf("active Kalshi accounts = %#v, want none after failed preflight", composition.activeAccounts)
	}
	if len(composition.settlements) != 0 {
		t.Fatalf("settlement services = %d, want none for a route that failed preflight", len(composition.settlements))
	}
	if reason, ok := registry.MaintenanceReason("kalshi:main"); !ok || reason != accountmaintenance.ReasonKalshiRouteUnavailable {
		t.Fatalf("failed Kalshi route maintenance = %q, %v", reason, ok)
	}
	if _, ok := registry.MaintenanceReason("main"); ok {
		t.Fatal("Kalshi preflight failure put a Polymarket account in maintenance")
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
	// The Kalshi account keeps its delivery slot although its route failed:
	// claiming its persisted intents must not fail delivery for Polymarket.
	gate, err := newVenueAccountGate(&recordingAccountGate{}, configuredKalshiAccountIDs(cfg.DecisionCycle))
	if err != nil {
		t.Fatal(err)
	}
	release, err := gate.AcquireExecutionAccount(context.Background(), "kalshi:main")
	if err != nil {
		t.Fatalf("configured Kalshi account without a route has no delivery slot: %v", err)
	}
	release()
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
		&routeTestExecution{}, nil, logger); err == nil {
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

// A database without migration 0045 must not stop the process: the running
// sync and position check are switched off and the drift-only check remains.
func TestResolveKalshiLedgerSyncFallsBackWithoutTheMigration(t *testing.T) {
	var logged bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logged, nil))
	cfg := config.Config{Kalshi: config.Kalshi{SettlementEnabled: true, BalanceSyncEnabled: true, PositionCheckEnabled: true}}
	if got := resolveKalshiLedgerSync(context.Background(), cfg, nil, logger); got != (kalshiLedgerSync{}) {
		t.Fatalf("switches without the tables = %#v, want both off", got)
	}
	if !strings.Contains(logged.String(), "migration 0045 are missing") || !strings.Contains(logged.String(), "level=ERROR") {
		t.Fatalf("log = %q, want an error naming the missing migration", logged.String())
	}

	logged.Reset()
	cfg.Kalshi.BalanceSyncEnabled = false
	if got := resolveKalshiLedgerSync(context.Background(), cfg, nil, logger); got != (kalshiLedgerSync{}) || logged.Len() != 0 {
		t.Fatalf("disabled sync = %#v log %q, want off without a table check", got, logged.String())
	}
	cfg.Kalshi.BalanceSyncEnabled, cfg.Kalshi.SettlementEnabled = true, false
	if got := resolveKalshiLedgerSync(context.Background(), cfg, nil, logger); got != (kalshiLedgerSync{balance: true, positions: true}) || logged.Len() != 0 {
		t.Fatalf("settlement disabled = %#v log %q, want the configured switches unused and unchecked", got, logged.String())
	}
}

// TestKalshiKeyFailureWithProtectedAssetsFreezesOnlyKalshi covers acceptance 4:
// a bad local Kalshi key on an account with open reservations no longer
// stops the process. The account is frozen in maintenance (no route, no
// settlement, nothing released) and Polymarket keeps trading.
func TestKalshiKeyFailureWithProtectedAssetsFreezesOnlyKalshi(t *testing.T) {
	original := kalshiProtectedAssets
	t.Cleanup(func() { kalshiProtectedAssets = original })
	kalshiProtectedAssets = func(context.Context, *sql.DB, string) (bool, error) { return true, nil }

	cfg := config.Config{
		Kalshi: config.Kalshi{MarketDataEnabled: true, LiveAccounts: []config.KalshiLiveAccount{{
			ExecutionAccountID: "kalshi:main", APIKeyID: "key", PrivateKeyPath: "/tmp/does-not-exist/kalshi.pem",
		}}},
		DecisionCycle: config.DecisionCycle{PredictionModelID: "echo", Accounts: []config.DecisionAccount{
			{ExecutionAccountID: "main", MarketSource: domain.MarketSourcePolymarket, StrategyID: domain.StrategyIDMultfactorV2, Mode: config.DecisionAccountActive},
			{ExecutionAccountID: "kalshi:main", MarketSource: domain.MarketSourceKalshi, StrategyID: domain.StrategyIDMultfactorV2, Mode: config.DecisionAccountActive},
		}},
	}
	primary := &routeTestExecution{}
	registry := accountmaintenance.New()
	var logs bytes.Buffer
	composition, err := composeKalshiExecution(context.Background(), cfg, nil, memory.NewOrderRepository(), nil, nil,
		primary, registry, slog.New(slog.NewJSONHandler(&logs, nil)))
	if err != nil {
		t.Fatalf("composeKalshiExecution() = %v, want the frozen account to degrade instead of stopping startup", err)
	}
	if len(composition.activeAccounts) != 0 || len(composition.settlements) != 0 {
		t.Fatalf("frozen account composed: active=%#v settlements=%d", composition.activeAccounts, len(composition.settlements))
	}
	entries := registry.Entries("kalshi:main")
	if len(entries) != 1 || entries[0].Reason != accountmaintenance.ReasonKalshiRouteUnavailable ||
		!strings.Contains(entries[0].Detail, "protected assets frozen") {
		t.Fatalf("frozen account maintenance = %#v", entries)
	}
	if !strings.Contains(logs.String(), "frozen in maintenance") {
		t.Fatalf("freeze was not logged at error level:\n%s", logs.String())
	}
	pm := domain.OrderIntent{ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "main",
		MarketSource: domain.MarketSourcePolymarket}
	if !composition.execution.Enabled(pm) {
		t.Fatal("Polymarket disabled by a Kalshi key failure")
	}
	if _, err := composition.execution.Submit(context.Background(), pm); err != nil || len(primary.submitted) != 1 {
		t.Fatalf("Polymarket submit = %v, submitted %d", err, len(primary.submitted))
	}
	kalshiIntent := pm
	kalshiIntent.MarketSource, kalshiIntent.ExecutionAccountID = domain.MarketSourceKalshi, "kalshi:main"
	if composition.execution.Enabled(kalshiIntent) {
		t.Fatal("frozen Kalshi account accepted a delivery")
	}
	if _, err := composition.execution.Submit(context.Background(), kalshiIntent); err == nil {
		t.Fatal("frozen Kalshi account accepted a new order")
	}

	// A failed protected-asset lookup is a database problem and stays fatal.
	kalshiProtectedAssets = func(context.Context, *sql.DB, string) (bool, error) { return false, errors.New("db down") }
	if _, err := composeKalshiExecution(context.Background(), cfg, nil, memory.NewOrderRepository(), nil, nil,
		primary, accountmaintenance.New(), slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("protected-asset lookup failure must stop startup")
	}
}
