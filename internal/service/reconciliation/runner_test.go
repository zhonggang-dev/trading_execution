package reconciliation

import (
	"context"
	"fmt"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// fakeAccountReconciler 表示后端使用的 fakeAccountReconciler 类型。
type fakeAccountReconciler struct {
	calls []request
}

// RunAccount 执行测试模拟流程。
func (reconciler *fakeAccountReconciler) RunAccount(
	_ context.Context,
	accountID string,
	trigger domain.ReconciliationTrigger,
	orderID string,
) (Result, error) {
	reconciler.calls = append(reconciler.calls, request{accountID: accountID, trigger: trigger, orderID: orderID})
	return Result{Run: domain.ReconciliationRun{
		RunID: fmt.Sprintf("run-%d", len(reconciler.calls)), ExecutionAccountID: accountID,
		Trigger: trigger, Status: domain.ReconciliationRunCompleted,
	}}, nil
}

// TestRunnerStartupSweepCoversEveryConfiguredAccount 验证 Runner Startup Sweep Covers Every Configured Account 场景下的行为。
func TestRunnerStartupSweepCoversEveryConfiguredAccount(t *testing.T) {
	service := &fakeAccountReconciler{}
	runner, err := NewRunner(RunnerParams{Service: service, Accounts: []string{"account-1", "account-2", "account-1"}})
	if err != nil {
		t.Fatal(err)
	}
	result := runner.Sweep(context.Background(), domain.ReconciliationTriggerStartup)
	if len(result.Errors) != 0 || len(result.Runs) != 2 || len(service.calls) != 2 {
		t.Fatalf("sweep = %#v, calls = %#v", result, service.calls)
	}
	if service.calls[0].accountID != "account-1" || service.calls[1].accountID != "account-2" {
		t.Fatalf("accounts = %#v", service.calls)
	}
}

// TestRunnerTriggerDoesNotBlockOrderPathWhenQueueIsFull 验证 Runner Trigger Does Not Block Order Path When Queue Is Full 场景下的行为。
func TestRunnerTriggerDoesNotBlockOrderPathWhenQueueIsFull(t *testing.T) {
	runner, err := NewRunner(RunnerParams{Service: &fakeAccountReconciler{}, Accounts: []string{"account-1"}})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < cap(runner.requests)+100; index++ {
		runner.Trigger("account-1", domain.ReconciliationTriggerOrderUnknown, fmt.Sprintf("order-%d", index))
	}
	if len(runner.requests) != cap(runner.requests) {
		t.Fatalf("queue length = %d, want %d", len(runner.requests), cap(runner.requests))
	}
}
