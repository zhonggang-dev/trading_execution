package ordercoordinator_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/ordercoordinator"
	"github.com/UniPat-AI/trading_execution/internal/service/orderrecovery"
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
	failures  map[string]error
	resolved  map[string]domain.OrderStatus
}

// Refresh 记录模拟订单刷新。
func (execution *fakeExecution) Refresh(_ context.Context, orderID string) (domain.Order, error) {
	execution.refreshed = append(execution.refreshed, orderID)
	if err := execution.failures[orderID]; err != nil {
		return domain.Order{ID: orderID}, err
	}
	status := domain.OrderStatusUnknown
	if resolved, ok := execution.resolved[orderID]; ok {
		status = resolved
	}
	return domain.Order{ID: orderID, Status: status}, nil
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

// TestSweepSerializesRecoveryOrdersThroughTheLease 验证恢复状态的订单经过订单级租约：
// 被其他恢复者持有的订单被跳过而不是重复刷新，失败后按退避推迟，成功脱离恢复状态后释放租约。
func TestSweepSerializesRecoveryOrdersThroughTheLease(t *testing.T) {
	now := time.Date(2026, 9, 10, 8, 0, 10, 0, time.UTC)
	clock := now
	repository := memory.NewOrderRepository()
	createOrder(t, repository, domain.Order{ID: "held", Intent: domain.OrderIntent{ClientOrderID: "held", ExecutionAccountID: "acct"}, Status: domain.OrderStatusUnknown, FilledSize: "0", CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Minute), Revision: 1})
	createOrder(t, repository, domain.Order{ID: "failing", Intent: domain.OrderIntent{ClientOrderID: "failing", ExecutionAccountID: "acct"}, Status: domain.OrderStatusUnknown, FilledSize: "0", CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Minute), Revision: 1})
	createOrder(t, repository, domain.Order{ID: "live", Intent: domain.OrderIntent{ClientOrderID: "live", ExecutionAccountID: "acct"}, Status: domain.OrderStatusLive, FilledSize: "0", CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Minute), Revision: 1})
	leases := memory.NewOrderRecoveryLeaseStore()
	if _, err := leases.AcquireOrderRecoveryLease(context.Background(), domain.OrderRecoveryLeaseRequest{
		OrderID: "held", ExecutionAccountID: "acct", Holder: "reconciliation:run-1", OrderRevision: 1, Now: now, TTL: 5 * time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	guard, err := orderrecovery.NewGuard(orderrecovery.GuardParams{
		Store: leases, Policy: orderrecovery.Policy{BaseBackoff: 30 * time.Second}, Now: func() time.Time { return clock },
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	execution := &fakeExecution{failures: map[string]error{"failing": errors.New("clob 503")}}
	coordinator, err := ordercoordinator.New(ordercoordinator.Params{
		Repository: repository, Execution: execution, PollInterval: time.Second,
		Now: func() time.Time { return clock }, Recovery: guard, RecoveryHolder: "ordercoordinator:test",
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Sweep(context.Background())
	if result.Selected != 3 || result.Deferred != 1 || result.Refreshed != 1 || len(result.Errors) != 1 {
		t.Fatalf("Sweep() = %#v", result)
	}
	for _, refreshed := range execution.refreshed {
		if refreshed == "held" {
			t.Fatalf("held order was refreshed by the coordinator: %#v", execution.refreshed)
		}
	}
	lease, exists := leases.Lease("failing")
	if !exists || lease.Attempts != 1 || lease.Holder != "" || lease.NextRetryAt == nil || !lease.NextRetryAt.Equal(now.Add(30*time.Second)) {
		t.Fatalf("failing lease = %#v exists=%v, want one failed attempt with 30s backoff", lease, exists)
	}
	if _, exists := leases.Lease("live"); exists {
		t.Fatal("LIVE order must not use the recovery lease")
	}

	// Inside the backoff window the failing order is deferred, not retried.
	clock = now.Add(10 * time.Second)
	refreshedBefore := len(execution.refreshed)
	result = coordinator.Sweep(context.Background())
	if result.Deferred != 2 || len(result.Errors) != 0 || len(execution.refreshed) != refreshedBefore+1 {
		t.Fatalf("Sweep() inside backoff = %#v refreshed=%#v", result, execution.refreshed)
	}

	// After the backoff the order recovers and leaves the lease behind.
	clock = now.Add(time.Minute)
	delete(execution.failures, "failing")
	execution.resolved = map[string]domain.OrderStatus{"failing": domain.OrderStatusFilled}
	result = coordinator.Sweep(context.Background())
	if result.Refreshed != 2 || len(result.Errors) != 0 {
		t.Fatalf("Sweep() after backoff = %#v", result)
	}
	if _, exists := leases.Lease("failing"); exists {
		t.Fatal("resolved order still holds a recovery lease")
	}
}
