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

func TestPolymarketSubmissionRemainsOnPrimaryWithoutMutation(t *testing.T) {
	primary, kalshiExecution := &fakeExecution{}, &fakeExecution{}
	router, err := New(memory.NewOrderRepository(), primary, []Route{{ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV2,
		LogicalAccountID: "main", InternalAccountID: "kalshi:main", Execution: kalshiExecution}})
	if err != nil {
		t.Fatal(err)
	}
	intent := domain.OrderIntent{ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "main",
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

func TestExactBindingRoutesKalshiToIsolatedAccount(t *testing.T) {
	primary, kalshiExecution := &fakeExecution{}, &fakeExecution{}
	router, err := New(memory.NewOrderRepository(), primary, []Route{{ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV2,
		LogicalAccountID: "main", InternalAccountID: "kalshi:main", Execution: kalshiExecution}})
	if err != nil {
		t.Fatal(err)
	}
	intent := domain.OrderIntent{ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "main",
		MarketSource: domain.MarketSourceKalshi, Metadata: map[string]string{}}
	if !router.Enabled(intent) {
		t.Fatal("exact Kalshi binding must be enabled")
	}
	if _, err := router.Submit(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	if len(primary.submitted) != 0 || len(kalshiExecution.submitted) != 1 || kalshiExecution.submitted[0].ExecutionAccountID != "kalshi:main" {
		t.Fatalf("unexpected route: primary=%d kalshi=%#v", len(primary.submitted), kalshiExecution.submitted)
	}
	wrong := intent
	wrong.ModelID = "gemini_masked"
	if router.Enabled(wrong) {
		t.Fatal("mismatched model must remain dry-run")
	}
	if _, err := router.Submit(context.Background(), wrong); err == nil {
		t.Fatal("mismatched model submission must fail closed")
	}
}

func TestMaintenanceRouteRejectsNewIntentButStillRoutesHistoricalOrder(t *testing.T) {
	repository := memory.NewOrderRepository()
	primary, kalshiExecution := &fakeExecution{}, &fakeExecution{}
	router, err := New(repository, primary, []Route{{
		ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV2,
		LogicalAccountID: "main", InternalAccountID: "kalshi:main",
		Execution: kalshiExecution, MaintenanceOnly: true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	newIntent := domain.OrderIntent{
		ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "main",
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
