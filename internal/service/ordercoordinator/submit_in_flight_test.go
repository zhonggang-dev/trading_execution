package ordercoordinator_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/ordercoordinator"
	"github.com/UniPat-AI/trading_execution/internal/service/orderrecovery"
)

// TestSweepWaitsForInFlightSubmit: an in-flight SUBMITTING order is a wait,
// not a recovery failure, and never escalates its lease.
func TestSweepWaitsForInFlightSubmit(t *testing.T) {
	now := time.Date(2026, 11, 5, 9, 0, 10, 0, time.UTC)
	repository := memory.NewOrderRepository()
	createOrder(t, repository, domain.Order{ID: "submitting", Intent: domain.OrderIntent{ClientOrderID: "submitting", ExecutionAccountID: "acct"}, Status: domain.OrderStatusSubmitting, FilledSize: "0", CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(-3 * time.Second), Revision: 1})
	leases := memory.NewOrderRecoveryLeaseStore()
	guard, err := orderrecovery.NewGuard(orderrecovery.GuardParams{
		Store: leases, Now: func() time.Time { return now }, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	execution := &fakeExecution{failures: map[string]error{"submitting": port.ErrSubmitInFlight}}
	coordinator, err := ordercoordinator.New(ordercoordinator.Params{
		Repository: repository, Execution: execution, PollInterval: time.Second,
		Now: func() time.Time { return now }, Recovery: guard,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Sweep(context.Background())
	if len(result.Errors) != 0 || result.Refreshed != 1 {
		t.Fatalf("Sweep() = %#v; want a clean wait", result)
	}
	lease, exists := leases.Lease("submitting")
	if !exists || lease.Attempts != 0 || lease.LastError != "" {
		t.Fatalf("lease = %#v exists=%v; want a WAITING lease without failure", lease, exists)
	}

	unguarded, err := ordercoordinator.New(ordercoordinator.Params{
		Repository: repository, Execution: execution, PollInterval: time.Second, Now: func() time.Time { return now.Add(time.Minute) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if result := unguarded.Sweep(context.Background()); len(result.Errors) != 0 || result.Deferred != 1 {
		t.Fatalf("unguarded Sweep() = %#v; want a deferral", result)
	}
}

// TestMemoryRepositoryKeepsInFlightSubmitOutOfSelection mirrors the
// PostgreSQL selection filter.
func TestMemoryRepositoryKeepsInFlightSubmitOutOfSelection(t *testing.T) {
	now := time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)
	repository := memory.NewOrderRepository()
	repository.SetSubmitInFlightGrace(90 * time.Second)
	createOrder(t, repository, domain.Order{ID: "young", Intent: domain.OrderIntent{ClientOrderID: "young", ExecutionAccountID: "acct"}, Status: domain.OrderStatusSubmitting, FilledSize: "0", CreatedAt: now.Add(-4 * time.Second), UpdatedAt: now.Add(-4 * time.Second), Revision: 1})
	createOrder(t, repository, domain.Order{ID: "stale", Intent: domain.OrderIntent{ClientOrderID: "stale", ExecutionAccountID: "acct"}, Status: domain.OrderStatusSubmitting, FilledSize: "0", CreatedAt: now.Add(-2 * time.Minute), UpdatedAt: now.Add(-2 * time.Minute), Revision: 1})
	orders, err := repository.ListPendingForAccounts(context.Background(), []string{"acct"}, now.Add(-2*time.Second), 10)
	if err != nil || len(orders) != 1 || orders[0].ID != "stale" {
		t.Fatalf("ListPendingForAccounts() = %#v, %v; want only the stale submission", orders, err)
	}
}
