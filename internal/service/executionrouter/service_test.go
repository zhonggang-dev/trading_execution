package executionrouter

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type fakeExecution struct {
	submitted []domain.OrderIntent
	resumed   []string
}

func (fake *fakeExecution) Submit(_ context.Context, intent domain.OrderIntent) (port.OrderSubmitResult, error) {
	fake.submitted = append(fake.submitted, intent)
	return port.OrderSubmitResult{}, nil
}

func (fake *fakeExecution) Resume(_ context.Context, orderID string) (domain.Order, error) {
	fake.resumed = append(fake.resumed, orderID)
	return domain.Order{ID: orderID}, nil
}
func (*fakeExecution) Get(context.Context, string) (domain.Order, error) { return domain.Order{}, nil }
func (*fakeExecution) Refresh(context.Context, string) (domain.Order, error) {
	return domain.Order{}, nil
}
func (*fakeExecution) Cancel(context.Context, string) (domain.Order, error) {
	return domain.Order{}, nil
}
func (*fakeExecution) FinalizeCancellation(context.Context, string) (domain.Order, error) {
	return domain.Order{}, nil
}
func (*fakeExecution) Events(context.Context, string) ([]domain.OrderEvent, error) { return nil, nil }
func (*fakeExecution) Attempts(context.Context, string) ([]domain.OrderAttempt, error) {
	return nil, nil
}

func kalshiMainRoute(execution Execution, maintenance bool) []Route {
	return []Route{{AccountID: "kalshi:main", StrategyID: "brier_mid_edge_once", Execution: execution, MaintenanceOnly: maintenance}}
}

func TestPolymarketSubmissionRemainsOnPrimaryWithoutMutation(t *testing.T) {
	primary, kalshiExecution := &fakeExecution{}, &fakeExecution{}
	router, err := New(memory.NewOrderRepository(), primary, kalshiMainRoute(kalshiExecution, false))
	if err != nil {
		t.Fatal(err)
	}
	intent := domain.OrderIntent{ModelID: "v4.1 flash", StrategyID: "brier_mid_edge_once", ExecutionAccountID: "main",
		MarketSource: domain.MarketSourcePolymarket, Venue: "polymarket", MarketID: "pm-market", TokenID: "pm-token",
		ClientOrderID: "pm-order", Metadata: map[string]string{"sentinel": "unchanged"}}
	want := intent
	want.Metadata = map[string]string{"sentinel": "unchanged"}
	if !router.Enabled(intent) {
		t.Fatal("Polymarket must remain enabled")
	}
	if _, err := router.Submit(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	if len(primary.submitted) != 1 || len(kalshiExecution.submitted) != 0 {
		t.Fatalf("wrong venue route: primary=%d kalshi=%d", len(primary.submitted), len(kalshiExecution.submitted))
	}
	if !reflect.DeepEqual(primary.submitted[0], want) {
		t.Fatalf("Polymarket intent mutated\n got: %#v\nwant: %#v", primary.submitted[0], want)
	}
	onKalshiAccount := intent
	onKalshiAccount.ExecutionAccountID = "kalshi:main"
	if router.Enabled(onKalshiAccount) {
		t.Fatal("a Polymarket intent on a Kalshi account must not be deliverable")
	}
	if _, err := router.Submit(context.Background(), onKalshiAccount); err == nil {
		t.Fatal("a Polymarket intent on a Kalshi account must fail closed")
	}
}

func TestKalshiAccountRoutesByAccountWithoutMutation(t *testing.T) {
	primary, kalshiExecution := &fakeExecution{}, &fakeExecution{}
	router, err := New(memory.NewOrderRepository(), primary, kalshiMainRoute(kalshiExecution, false))
	if err != nil {
		t.Fatal(err)
	}
	intent := domain.OrderIntent{ModelID: "v4.1 flash", StrategyID: "brier_mid_edge_once", ExecutionAccountID: "kalshi:main",
		MarketSource: domain.MarketSourceKalshi, Metadata: map[string]string{"sentinel": "unchanged"}}
	want := intent
	want.Metadata = map[string]string{"sentinel": "unchanged"}
	if !router.Enabled(intent) {
		t.Fatal("Kalshi account route must be enabled")
	}
	if _, err := router.Submit(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	if len(primary.submitted) != 0 || len(kalshiExecution.submitted) != 1 {
		t.Fatalf("unexpected route: primary=%d kalshi=%#v", len(primary.submitted), kalshiExecution.submitted)
	}
	if !reflect.DeepEqual(kalshiExecution.submitted[0], want) {
		t.Fatalf("Kalshi intent mutated\n got: %#v\nwant: %#v", kalshiExecution.submitted[0], want)
	}
	// Any model may trade the account: the model is not part of the route.
	otherModel := intent
	otherModel.ModelID = "kimi-k2"
	if !router.Enabled(otherModel) {
		t.Fatal("a model change must not disable the Kalshi account")
	}
	for name, wrong := range map[string]domain.OrderIntent{
		"other strategy": func() domain.OrderIntent {
			value := intent
			value.StrategyID = domain.StrategyIDMultfactorV2
			return value
		}(),
		"unknown account": func() domain.OrderIntent {
			value := intent
			value.ExecutionAccountID = "kalshi:wallet-7"
			return value
		}(),
	} {
		if router.Enabled(wrong) {
			t.Fatalf("%s: must not be deliverable", name)
		}
		if _, err := router.Submit(context.Background(), wrong); err == nil {
			t.Fatalf("%s: submission must fail closed", name)
		}
	}
	if len(kalshiExecution.submitted) != 1 {
		t.Fatalf("rejected intents reached the Kalshi execution: %#v", kalshiExecution.submitted)
	}
}

// TestLegacyKalshiDeliveryUnderPolymarketAccountStillRecovers covers a durable
// delivery persisted before the cutover, when a Kalshi intent carried the
// Polymarket account it rode on. It must reach the same Kalshi account with
// the same identity the old router produced, so execution idempotency matches
// an order that may already exist.
func TestLegacyKalshiDeliveryUnderPolymarketAccountStillRecovers(t *testing.T) {
	primary, kalshiExecution := &fakeExecution{}, &fakeExecution{}
	router, err := New(memory.NewOrderRepository(), primary, kalshiMainRoute(kalshiExecution, false))
	if err != nil {
		t.Fatal(err)
	}
	legacy := domain.OrderIntent{ModelID: "deepseek_masked", StrategyID: "brier_mid_edge_once", ExecutionAccountID: "main",
		MarketSource: domain.MarketSourceKalshi, ClientOrderID: "legacy-kalshi", Metadata: map[string]string{"cycle_id": "c"}}
	if !router.Enabled(legacy) {
		t.Fatal("legacy Kalshi delivery must stay deliverable")
	}
	if _, err := router.Submit(context.Background(), legacy); err != nil {
		t.Fatal(err)
	}
	if len(primary.submitted) != 0 || len(kalshiExecution.submitted) != 1 {
		t.Fatalf("legacy delivery routed to primary=%d kalshi=%d", len(primary.submitted), len(kalshiExecution.submitted))
	}
	got := kalshiExecution.submitted[0]
	if got.ExecutionAccountID != "kalshi:main" || got.Metadata["logical_execution_account_id"] != "main" ||
		got.Metadata["cycle_id"] != "c" || got.ModelID != "deepseek_masked" {
		t.Fatalf("legacy intent identity = %#v", got)
	}
	if _, exists := legacy.Metadata["logical_execution_account_id"]; exists {
		t.Fatal("router mutated the caller's metadata map")
	}
}

func TestMaintenanceRouteRejectsNewIntentButStillRoutesHistoricalOrder(t *testing.T) {
	repository := memory.NewOrderRepository()
	primary, kalshiExecution := &fakeExecution{}, &fakeExecution{}
	router, err := New(repository, primary, kalshiMainRoute(kalshiExecution, true))
	if err != nil {
		t.Fatal(err)
	}
	newIntent := domain.OrderIntent{
		ModelID: "v4.1 flash", StrategyID: "brier_mid_edge_once", ExecutionAccountID: "kalshi:main",
		MarketSource: domain.MarketSourceKalshi,
	}
	if router.Enabled(newIntent) {
		t.Fatal("maintenance-only Kalshi route accepted a new strategy intent")
	}
	if _, err := router.Submit(context.Background(), newIntent); err == nil {
		t.Fatal("maintenance-only Kalshi route submitted a new strategy intent")
	}
	historical := domain.Order{
		ID: "historical-kalshi-order",
		Intent: domain.OrderIntent{
			ClientOrderID: "historical-client", ExecutionAccountID: "kalshi:main",
			Venue: "kalshi", MarketSource: domain.MarketSourceKalshi, Size: "1",
		},
		Status: domain.OrderStatusUnknown, FilledSize: "0", FilledNotional: "0", TotalFees: "0", Revision: 1,
	}
	if _, created, createErr := repository.Create(context.Background(), historical); createErr != nil || !created {
		t.Fatalf("create historical order = %t, %v", created, createErr)
	}
	if _, err := router.Refresh(context.Background(), historical.ID); err != nil {
		t.Fatalf("refresh through maintenance route: %v", err)
	}
	historical.Status = domain.OrderStatusReserved
	historical.Revision++
	if updateErr := repository.Update(context.Background(), historical); updateErr != nil {
		t.Fatalf("mark historical order reserved: %v", updateErr)
	}
	deferred, err := router.Resume(context.Background(), historical.ID)
	if !errors.Is(err, ErrMaintenanceOnly) || len(kalshiExecution.resumed) != 0 {
		t.Fatalf("maintenance route resumed a pre-venue order: resumed=%#v err=%v", kalshiExecution.resumed, err)
	}
	if deferred.Status != domain.OrderStatusReserved || deferred.Revision != historical.Revision+1 ||
		deferred.FailureCode != "KALSHI_ROUTE_MAINTENANCE" || deferred.UpdatedAt.IsZero() {
		t.Fatalf("maintenance deferral was not durably audited: %#v", deferred)
	}
}

func TestStrategyInputFollowsTradableKalshiRoutes(t *testing.T) {
	router, err := New(memory.NewOrderRepository(), &fakeExecution{}, []Route{
		{AccountID: "kalshi:main", StrategyID: "brier_mid_edge_once", Execution: &fakeExecution{}},
		{AccountID: "kalshi:wallet-6", StrategyID: domain.StrategyIDMultfactorV1, Execution: &fakeExecution{}, MaintenanceOnly: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		binding domain.StrategyExecutionBinding
		want    bool
	}{
		{"Polymarket account", domain.StrategyExecutionBinding{ModelID: "m", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet-7"}, true},
		{"tradable Kalshi route", domain.StrategyExecutionBinding{ModelID: "m", StrategyID: "brier_mid_edge_once", ExecutionAccountID: "kalshi:main", MarketSource: domain.MarketSourceKalshi}, true},
		{"maintenance-only route", domain.StrategyExecutionBinding{ModelID: "m", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "kalshi:wallet-6", MarketSource: domain.MarketSourceKalshi}, false},
		{"no route", domain.StrategyExecutionBinding{ModelID: "m", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "kalshi:wallet-7", MarketSource: domain.MarketSourceKalshi}, false},
		{"route for another strategy", domain.StrategyExecutionBinding{ModelID: "m", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "kalshi:main", MarketSource: domain.MarketSourceKalshi}, false},
	} {
		if got := router.StrategyInputEnabled(test.binding); got != test.want {
			t.Errorf("%s: StrategyInputEnabled = %v, want %v", test.name, got, test.want)
		}
	}
}

func TestNewRejectsInvalidRoutes(t *testing.T) {
	for name, routes := range map[string][]Route{
		"missing account":    {{StrategyID: "s", Execution: &fakeExecution{}}},
		"no Kalshi prefix":   {{AccountID: "main", StrategyID: "s", Execution: &fakeExecution{}}},
		"missing strategy":   {{AccountID: "kalshi:main", Execution: &fakeExecution{}}},
		"missing execution":  {{AccountID: "kalshi:main", StrategyID: "s"}},
		"duplicated account": {{AccountID: "kalshi:main", StrategyID: "a", Execution: &fakeExecution{}}, {AccountID: "kalshi:main", StrategyID: "b", Execution: &fakeExecution{}}},
	} {
		if _, err := New(memory.NewOrderRepository(), &fakeExecution{}, routes); err == nil {
			t.Fatalf("%s: New accepted %#v", name, routes)
		}
	}
}
