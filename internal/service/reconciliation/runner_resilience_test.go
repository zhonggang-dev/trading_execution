package reconciliation

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

type accountReconcilerFunc func(context.Context, RunAccountParams) (Result, error)

func (f accountReconcilerFunc) RunAccount(ctx context.Context, p RunAccountParams) (Result, error) {
	return f(ctx, p)
}

func TestRunnerSlowAccountDoesNotBlockOtherAccountOrTriggerStorm(t *testing.T) {
	slowStarted := make(chan struct{}, 4)
	healthyDone := make(chan struct{}, 4)
	var active, peak atomic.Int32
	service := accountReconcilerFunc(func(ctx context.Context, p RunAccountParams) (Result, error) {
		if p.ExecutionAccountID == "slow" {
			current := active.Add(1)
			defer active.Add(-1)
			if current > peak.Load() {
				peak.Store(current)
			}
			select {
			case slowStarted <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return Result{}, ctx.Err()
		}
		now := time.Now().UTC()
		select {
		case healthyDone <- struct{}{}:
		default:
		}
		return Result{Run: domain.ReconciliationRun{ExecutionAccountID: p.ExecutionAccountID, Status: domain.ReconciliationRunCompleted, CompletedAt: &now}}, nil
	})
	runner, err := NewRunner(RunnerParams{Service: service, Accounts: []string{"slow", "healthy"}, Interval: time.Second, MaxResultAge: 10 * time.Second, AccountTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- runner.RunAfterStartupReady(ctx, ready) }()
	<-ready
	runner.Trigger("slow", domain.ReconciliationTriggerOrderUnknown, "bad")
	select {
	case <-slowStarted:
	case <-time.After(time.Second):
		t.Fatal("slow account did not start")
	}
	for i := 0; i < 3000; i++ {
		runner.Trigger("slow", domain.ReconciliationTriggerOrderUnknown, "bad")
	}
	// A scheduled sweep is independently enqueued even if a burst filled the
	// incoming focus queue; the healthy account must complete while slow is active.
	select {
	case <-healthyDone:
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("slow account blocked scheduled reconciliation of healthy account")
	}
	if active.Load() != 1 || peak.Load() != 1 {
		t.Fatalf("active=%d peak=%d", active.Load(), peak.Load())
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("shutdown=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runner did not drain workers")
	}
	if peak.Load() != 1 {
		t.Fatalf("one account ran concurrently: peak=%d", peak.Load())
	}
}

func TestStartupSweepPublishesHealthyAccountBeforeSlowAccountTimesOut(t *testing.T) {
	entered := make(chan struct{})
	healthy := make(chan struct{})
	service := accountReconcilerFunc(func(ctx context.Context, p RunAccountParams) (Result, error) {
		if p.ExecutionAccountID == "slow" {
			close(entered)
			<-ctx.Done()
			return Result{}, ctx.Err()
		}
		<-entered
		now := time.Now().UTC()
		close(healthy)
		return Result{Run: domain.ReconciliationRun{ExecutionAccountID: p.ExecutionAccountID, Status: domain.ReconciliationRunCompleted, CompletedAt: &now}}, nil
	})
	runner, err := NewRunner(RunnerParams{Service: service, Accounts: []string{"slow", "healthy"}, Interval: time.Second, MaxResultAge: 10 * time.Second, AccountTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan SweepResult, 1)
	go func() { done <- runner.Sweep(context.Background(), domain.ReconciliationTriggerStartup) }()
	select {
	case <-healthy:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("startup accounts ran serially")
	}
	result := <-done
	if len(result.Errors) != 1 || result.Runs[0].Run.Status != domain.ReconciliationRunFailed || result.Runs[1].Run.Status != domain.ReconciliationRunCompleted {
		t.Fatalf("sweep=%#v", result)
	}
}
