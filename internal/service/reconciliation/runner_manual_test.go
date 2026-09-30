package reconciliation

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestRunnerRunAccountRejectsQuarantinedAndUnconfiguredAccounts(t *testing.T) {
	service := &fakeAccountReconciler{}
	runner, err := NewRunner(RunnerParams{
		Service: service, Accounts: []string{"wallet-6"}, QuarantinedAccounts: []string{"wallet-7"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for account, want := range map[string]string{"wallet-7": "quarantined", "wallet-9": "not active", " ": "not active"} {
		_, err := runner.RunAccount(context.Background(), RunAccountParams{
			ExecutionAccountID: account, Trigger: domain.ReconciliationTriggerAssetDrift,
		})
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("RunAccount(%q) error = %v, want %q", account, err, want)
		}
	}
	if len(service.calls) != 0 {
		t.Fatalf("rejected accounts reached the service: %#v", service.calls)
	}
	if _, ok := runner.LastResult("wallet-7"); ok {
		t.Fatal("rejected account got a remembered result")
	}
}

func TestRunnerRunAccountWaitsForDecisionDeliveryAndRemembersResult(t *testing.T) {
	var running, peak atomic.Int32
	calls := make(chan RunAccountParams, 1)
	service := accountReconcilerFunc(func(_ context.Context, params RunAccountParams) (Result, error) {
		current := running.Add(1)
		defer running.Add(-1)
		if current > peak.Load() {
			peak.Store(current)
		}
		calls <- params
		completedAt := time.Now().UTC()
		return Result{Run: domain.ReconciliationRun{
			RunID: "manual", ExecutionAccountID: params.ExecutionAccountID, Trigger: params.Trigger,
			Status: domain.ReconciliationRunCompleted, CompletedAt: &completedAt,
		}}, nil
	})
	runner, err := NewRunner(RunnerParams{Service: service, Accounts: []string{"wallet-6"}})
	if err != nil {
		t.Fatal(err)
	}
	// Decision delivery holds the wallet lock while it submits its batch.
	release, err := runner.AcquireExecutionAccount(context.Background(), "wallet-6")
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		result Result
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := runner.RunAccount(context.Background(), RunAccountParams{
			ExecutionAccountID: " wallet-6 ", Trigger: domain.ReconciliationTriggerAssetDrift, FocusOrderID: " order-1 ",
		})
		done <- outcome{result, err}
	}()
	select {
	case params := <-calls:
		t.Fatalf("manual reconciliation ran while decision delivery held the lock: %#v", params)
	case <-time.After(50 * time.Millisecond):
	}
	release()
	got := <-done
	if got.err != nil || got.result.Run.RunID != "manual" {
		t.Fatalf("RunAccount() = %#v/%v", got.result, got.err)
	}
	params := <-calls
	if params.ExecutionAccountID != "wallet-6" || params.FocusOrderID != "order-1" ||
		params.Trigger != domain.ReconciliationTriggerAssetDrift {
		t.Fatalf("service params = %#v, want normalized account and focus order", params)
	}
	if peak.Load() != 1 {
		t.Fatalf("peak concurrent runs = %d, want 1", peak.Load())
	}
	if last, ok := runner.LastResult("wallet-6"); !ok || last.Run.RunID != "manual" {
		t.Fatalf("LastResult() = %#v/%v, want the manual run", last, ok)
	}
}

func TestRunnerRunAccountLockWaitTimeoutKeepsPreviousResult(t *testing.T) {
	var calls atomic.Int32
	service := accountReconcilerFunc(func(_ context.Context, params RunAccountParams) (Result, error) {
		calls.Add(1)
		completedAt := time.Now().UTC()
		return Result{Run: domain.ReconciliationRun{
			RunID: "healthy", ExecutionAccountID: params.ExecutionAccountID,
			Status: domain.ReconciliationRunCompleted, CompletedAt: &completedAt,
		}}, nil
	})
	runner, err := NewRunner(RunnerParams{Service: service, Accounts: []string{"wallet-6"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.RunAccount(context.Background(), RunAccountParams{
		ExecutionAccountID: "wallet-6", Trigger: domain.ReconciliationTriggerScheduled,
	}); err != nil {
		t.Fatal(err)
	}
	release, err := runner.AcquireExecutionAccount(context.Background(), "wallet-6")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result, err := runner.RunAccount(ctx, RunAccountParams{
		ExecutionAccountID: "wallet-6", Trigger: domain.ReconciliationTriggerAssetDrift,
	})
	if err == nil || !errors.Is(err, context.DeadlineExceeded) ||
		!strings.Contains(err.Error(), "waiting for the execution account lock") || !strings.Contains(err.Error(), "did not run") {
		t.Fatalf("lock wait error = %v, want a deadline explaining nothing ran", err)
	}
	if result.Run.RunID != "" || calls.Load() != 1 {
		t.Fatalf("lock wait timeout result/calls = %#v/%d, want nothing executed", result, calls.Load())
	}
	if last, ok := runner.LastResult("wallet-6"); !ok || last.Run.RunID != "healthy" ||
		last.Run.Status != domain.ReconciliationRunCompleted {
		t.Fatalf("LastResult() = %#v/%v, want the previous healthy run kept", last, ok)
	}
}

func TestRunnerRunAccountExecutionTimeoutIsRememberedAsFailed(t *testing.T) {
	service := accountReconcilerFunc(func(ctx context.Context, params RunAccountParams) (Result, error) {
		<-ctx.Done()
		completedAt := time.Now().UTC()
		return Result{Run: domain.ReconciliationRun{
			RunID: "slow", ExecutionAccountID: params.ExecutionAccountID,
			Status: domain.ReconciliationRunAttentionRequired, CompletedAt: &completedAt,
		}}, ctx.Err()
	})
	runner, err := NewRunner(RunnerParams{Service: service, Accounts: []string{"wallet-6"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result, err := runner.RunAccount(ctx, RunAccountParams{
		ExecutionAccountID: "wallet-6", Trigger: domain.ReconciliationTriggerAssetDrift,
	})
	if err == nil || !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "did not run") {
		t.Fatalf("execution timeout error = %v, want a deadline from the started run", err)
	}
	if result.Run.RunID != "slow" || result.Run.Status != domain.ReconciliationRunFailed {
		t.Fatalf("execution timeout result = %#v, want the started run marked FAILED", result.Run)
	}
	last, ok := runner.LastResult("wallet-6")
	if !ok || last.Run.RunID != "slow" || last.Run.Status != domain.ReconciliationRunFailed {
		t.Fatalf("LastResult() = %#v/%v, want the FAILED run remembered", last, ok)
	}
	if err := runner.CheckAccount(context.Background(), "wallet-6"); err == nil {
		t.Fatal("failed manual reconciliation left the account ready for placement")
	}
}
