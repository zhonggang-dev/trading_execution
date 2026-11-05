package executionrouter

import (
	"context"
	"errors"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/accountmaintenance"
)

type refreshCountingExecution struct {
	fakeExecution
	refreshed []string
	cancelled []string
}

func (execution *refreshCountingExecution) Refresh(_ context.Context, id string) (domain.Order, error) {
	execution.refreshed = append(execution.refreshed, id)
	return domain.Order{ID: id}, nil
}

func (execution *refreshCountingExecution) Cancel(_ context.Context, id string) (domain.Order, error) {
	execution.cancelled = append(execution.cancelled, id)
	return domain.Order{ID: id}, nil
}

func maintenancePolymarketIntent(accountID string) domain.OrderIntent {
	return domain.OrderIntent{ModelID: "v4.1 flash", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: accountID,
		MarketSource: domain.MarketSourcePolymarket, Venue: "polymarket", MarketID: "pm-market", TokenID: "pm-token",
		ClientOrderID: "pm-" + accountID}
}

func TestPolymarketMaintenanceAccountTakesNoNewOrders(t *testing.T) {
	repository := memory.NewOrderRepository()
	primary := &refreshCountingExecution{}
	registry := accountmaintenance.New()
	router, err := NewWithMaintenance(repository, primary, nil, registry)
	if err != nil {
		t.Fatal(err)
	}
	registry.Enter("wallet-7", accountmaintenance.ReasonPreflightCLOBAuth, "auth failed")

	blocked, healthy := maintenancePolymarketIntent("wallet-7"), maintenancePolymarketIntent("main")
	if router.Enabled(blocked) || !router.Enabled(healthy) {
		t.Fatalf("Enabled maintenance=%v healthy=%v", router.Enabled(blocked), router.Enabled(healthy))
	}
	blockedBinding := domain.StrategyExecutionBinding{ModelID: "m", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet-7"}
	healthyBinding := blockedBinding
	healthyBinding.ExecutionAccountID = "main"
	if router.StrategyInputEnabled(blockedBinding) || !router.StrategyInputEnabled(healthyBinding) {
		t.Fatal("maintenance account must not be sent to the strategy while the other account is")
	}
	if _, err := router.Submit(context.Background(), blocked); !errors.Is(err, ErrAccountMaintenance) ||
		!errors.Is(err, ErrMaintenanceOnly) {
		t.Fatalf("Submit(maintenance) error = %v", err)
	}
	if len(primary.submitted) != 0 {
		t.Fatalf("maintenance submit reached the primary execution: %#v", primary.submitted)
	}
	if _, err := router.Submit(context.Background(), healthy); err != nil || len(primary.submitted) != 1 {
		t.Fatalf("healthy submit = %v, submitted=%d", err, len(primary.submitted))
	}

	// Historical orders of the maintenance account keep refresh/cancel, while
	// a pre-venue resume is deferred with an audited self-transition.
	historical := domain.Order{
		ID: "pm-historical", Intent: domain.OrderIntent{ClientOrderID: "pm-historical", ExecutionAccountID: "wallet-7",
			Venue: "polymarket", MarketSource: domain.MarketSourcePolymarket, Size: "1"},
		Status: domain.OrderStatusReserved, FilledSize: "0", FilledNotional: "0", TotalFees: "0", Revision: 1,
	}
	if _, created, createErr := repository.Create(context.Background(), historical); createErr != nil || !created {
		t.Fatalf("create = %t, %v", created, createErr)
	}
	if _, err := router.Refresh(context.Background(), historical.ID); err != nil || len(primary.refreshed) != 1 {
		t.Fatalf("refresh = %v, calls %d", err, len(primary.refreshed))
	}
	if _, err := router.Cancel(context.Background(), historical.ID); err != nil || len(primary.cancelled) != 1 {
		t.Fatalf("cancel = %v, calls %d", err, len(primary.cancelled))
	}
	deferred, err := router.Resume(context.Background(), historical.ID)
	if !errors.Is(err, ErrAccountMaintenance) || len(primary.resumed) != 0 {
		t.Fatalf("resume = %v, resumed %#v", err, primary.resumed)
	}
	if deferred.Status != domain.OrderStatusReserved || deferred.Revision != historical.Revision+1 ||
		deferred.FailureCode != "ACCOUNT_MAINTENANCE" {
		t.Fatalf("maintenance deferral was not durably audited: %#v", deferred)
	}

	// Leaving maintenance restores the normal path.
	registry.Exit("wallet-7", accountmaintenance.ReasonPreflightCLOBAuth)
	if !router.Enabled(blocked) || !router.StrategyInputEnabled(blockedBinding) {
		t.Fatal("account stayed blocked after leaving maintenance")
	}
	if _, err := router.Resume(context.Background(), historical.ID); err != nil || len(primary.resumed) != 1 {
		t.Fatalf("resume after maintenance = %v, resumed %#v", err, primary.resumed)
	}
}

func TestKalshiRouteFollowsMaintenanceRegistry(t *testing.T) {
	repository := memory.NewOrderRepository()
	primary, kalshiExecution := &fakeExecution{}, &fakeExecution{}
	registry := accountmaintenance.New()
	router, err := NewWithMaintenance(repository, primary, kalshiMainRoute(kalshiExecution, false), registry)
	if err != nil {
		t.Fatal(err)
	}
	intent := domain.OrderIntent{ModelID: "v4.1 flash", StrategyID: "brier_mid_edge_once", ExecutionAccountID: "kalshi:main",
		MarketSource: domain.MarketSourceKalshi}
	binding := domain.StrategyExecutionBinding{ModelID: "m", StrategyID: "brier_mid_edge_once", ExecutionAccountID: "kalshi:main",
		MarketSource: domain.MarketSourceKalshi}
	if !router.Enabled(intent) || !router.StrategyInputEnabled(binding) {
		t.Fatal("tradable Kalshi route disabled without maintenance")
	}
	registry.Enter("kalshi:main", accountmaintenance.ReasonKalshiMarketDataUnavailable, "no market data")
	if router.Enabled(intent) || router.KalshiEnabled(intent) || router.StrategyInputEnabled(binding) {
		t.Fatal("Kalshi maintenance account still tradable")
	}
	legacy := intent
	legacy.ExecutionAccountID = "main"
	if router.Enabled(legacy) {
		t.Fatal("legacy Kalshi delivery bypassed the maintenance of its resolved account")
	}
	if _, err := router.Submit(context.Background(), intent); err == nil || len(kalshiExecution.submitted) != 0 {
		t.Fatalf("Kalshi maintenance submit = %v, submitted %d", err, len(kalshiExecution.submitted))
	}
	pending := domain.Order{
		ID: "kalshi-pending", Intent: domain.OrderIntent{ClientOrderID: "kalshi-pending", ExecutionAccountID: "kalshi:main",
			Venue: "kalshi", MarketSource: domain.MarketSourceKalshi, Size: "1"},
		Status: domain.OrderStatusReserved, FilledSize: "0", FilledNotional: "0", TotalFees: "0", Revision: 1,
	}
	if _, _, createErr := repository.Create(context.Background(), pending); createErr != nil {
		t.Fatal(createErr)
	}
	deferred, err := router.Resume(context.Background(), pending.ID)
	if !errors.Is(err, ErrMaintenanceOnly) || len(kalshiExecution.resumed) != 0 || deferred.FailureCode != "KALSHI_ROUTE_MAINTENANCE" {
		t.Fatalf("Kalshi maintenance resume = %#v, %v", deferred, err)
	}
	// Polymarket is untouched by a Kalshi maintenance entry.
	if !router.Enabled(maintenancePolymarketIntent("main")) {
		t.Fatal("Kalshi maintenance disabled a Polymarket account")
	}
}
