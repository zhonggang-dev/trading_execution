package ordercoordinator_test

import (
	"context"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/ordercoordinator"
)

// TestSweepCancelsExpiredLiveAndRefreshesUnknown 验证 Sweep Cancels Expired Live And Refreshes Unknown 场景下的行为。
func TestSweepCancelsExpiredLiveAndRefreshesUnknown(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 10, 0, time.UTC)
	repository := memory.NewOrderRepository()
	expired := now.Add(-time.Second)
	createOrder(t, repository, domain.Order{ID: "live", Intent: domain.OrderIntent{ClientOrderID: "live", ExpiresAt: &expired}, Status: domain.OrderStatusLive, FilledSize: "0", CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Minute), Revision: 1})
	createOrder(t, repository, domain.Order{ID: "unknown", Intent: domain.OrderIntent{ClientOrderID: "unknown"}, Status: domain.OrderStatusUnknown, FilledSize: "0", CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Minute), Revision: 1})
	execution := &fakeExecution{}
	coordinator, err := ordercoordinator.New(ordercoordinator.Params{
		Repository: repository, Execution: execution, PollInterval: time.Second,
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Sweep(context.Background())
	if result.Selected != 2 || result.Cancelled != 1 || result.Refreshed != 1 || len(result.Errors) != 0 {
		t.Fatalf("Sweep() = %#v", result)
	}
}

func TestSweepDoesNotRefreshRetiredAccount(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 10, 0, time.UTC)
	repository := memory.NewOrderRepository()
	for _, accountID := range []string{"account-active", "account-retired"} {
		createOrder(t, repository, domain.Order{
			ID: "order-" + accountID,
			Intent: domain.OrderIntent{
				ClientOrderID: "client-" + accountID, ExecutionAccountID: accountID,
			},
			Status: domain.OrderStatusUnknown, FilledSize: "0",
			CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Minute), Revision: 1,
		})
	}
	execution := &fakeExecution{}
	coordinator, err := ordercoordinator.New(ordercoordinator.Params{
		Repository: repository, Execution: execution, PollInterval: time.Second,
		Accounts: []string{"account-active"}, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Sweep(context.Background())
	if result.Selected != 1 || result.Refreshed != 1 || len(result.Errors) != 0 ||
		len(execution.refreshed) != 1 || execution.refreshed[0] != "order-account-active" {
		t.Fatalf("Sweep()=%#v refreshed=%#v", result, execution.refreshed)
	}
}

func TestSweepFinalizesCancelledOrderAfterVenueFillGrace(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 10, 0, time.UTC)
	repository := memory.NewOrderRepository()
	createOrder(t, repository, domain.Order{
		ID: "cancelled", Intent: domain.OrderIntent{ClientOrderID: "cancelled"},
		Status: domain.OrderStatusCancelled, FilledSize: "0",
		CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Minute), Revision: 1,
	})
	execution := &fakeExecution{}
	coordinator, err := ordercoordinator.New(ordercoordinator.Params{
		Repository: repository, Execution: execution, PollInterval: time.Second,
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Sweep(context.Background())
	if result.Selected != 1 || result.Finalized != 1 || len(result.Errors) != 0 ||
		len(execution.finalized) != 1 || execution.finalized[0] != "cancelled" {
		t.Fatalf("Sweep()=%#v finalized=%#v", result, execution.finalized)
	}
}

// fakeExecution 表示后端使用的 fakeExecution 类型。
type fakeExecution struct {
	refreshed []string
	cancelled []string
	finalized []string
}

// Refresh 记录模拟订单刷新。
func (execution *fakeExecution) Refresh(_ context.Context, orderID string) (domain.Order, error) {
	execution.refreshed = append(execution.refreshed, orderID)
	return domain.Order{ID: orderID}, nil
}

// Resume 实现当前测试场景所需的辅助行为。
func (execution *fakeExecution) Resume(_ context.Context, orderID string) (domain.Order, error) {
	execution.refreshed = append(execution.refreshed, orderID)
	return domain.Order{ID: orderID}, nil
}

// Cancel 记录模拟订单撤销。
func (execution *fakeExecution) Cancel(_ context.Context, orderID string) (domain.Order, error) {
	execution.cancelled = append(execution.cancelled, orderID)
	return domain.Order{ID: orderID}, nil
}

func (execution *fakeExecution) FinalizeCancellation(_ context.Context, orderID string) (domain.Order, error) {
	execution.finalized = append(execution.finalized, orderID)
	return domain.Order{ID: orderID}, nil
}

// createOrder 模拟创建并返回测试记录。
func createOrder(t *testing.T, repository *memory.OrderRepository, order domain.Order) {
	t.Helper()
	if _, created, err := repository.Create(context.Background(), order); err != nil || !created {
		t.Fatalf("Create(%s) = %v, %v", order.ID, created, err)
	}
}
