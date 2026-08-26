package executionrouter

import (
	"context"
	"reflect"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type fakeExecution struct{ submitted []domain.OrderIntent }

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
func (*fakeExecution) Resume(context.Context, string) (domain.Order, error) {
	return domain.Order{}, nil
}
func (*fakeExecution) Get(context.Context, string) (domain.Order, error) { return domain.Order{}, nil }
func (*fakeExecution) Refresh(context.Context, string) (domain.Order, error) {
	return domain.Order{}, nil
}
func (*fakeExecution) Cancel(context.Context, string) (domain.Order, error) {
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
