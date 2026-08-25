package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// fakeAccountReconciler 表示后端使用的 fakeAccountReconciler 类型。
type fakeAccountReconciler struct {
	calls           []request
	now             func() time.Time
	omitCompletedAt bool
}

// RunAccount 执行测试模拟流程。
func (reconciler *fakeAccountReconciler) RunAccount(_ context.Context, params RunAccountParams) (Result, error) {
	reconciler.calls = append(reconciler.calls, request{accountID: params.ExecutionAccountID, trigger: params.Trigger, orderID: params.FocusOrderID})
	completedAt := time.Now().UTC()
	if reconciler.now != nil {
		completedAt = reconciler.now().UTC()
	}
	var completedAtPointer *time.Time
	if !reconciler.omitCompletedAt {
		completedAtPointer = &completedAt
	}
	return Result{Run: domain.ReconciliationRun{
		RunID: fmt.Sprintf("run-%d", len(reconciler.calls)), ExecutionAccountID: params.ExecutionAccountID,
		Trigger: params.Trigger, Status: domain.ReconciliationRunCompleted, CompletedAt: completedAtPointer,
	}}, nil
}

type runnerTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *runnerTestClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *runnerTestClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(duration)
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

func TestRunnerQuarantineExcludesStartupAndSuppressesAutomaticTrigger(t *testing.T) {
	service := &fakeAccountReconciler{}
	runner, err := NewRunner(RunnerParams{
		Service:             service,
		Accounts:            []string{"main", "wallet-1", "wallet-6"},
		QuarantinedAccounts: []string{"wallet-7"},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := runner.Sweep(context.Background(), domain.ReconciliationTriggerStartup)
	if len(result.Errors) != 0 || len(result.Runs) != 3 || len(service.calls) != 3 {
		t.Fatalf("startup sweep = %#v, calls = %#v", result, service.calls)
	}
	for _, call := range service.calls {
		if call.accountID == "wallet-7" {
			t.Fatalf("quarantined account was reconciled: %#v", service.calls)
		}
	}
	scheduled := runner.Sweep(context.Background(), domain.ReconciliationTriggerScheduled)
	if len(scheduled.Errors) != 0 || len(scheduled.Runs) != 3 || len(service.calls) != 6 {
		t.Fatalf("scheduled sweep = %#v, calls = %#v", scheduled, service.calls)
	}
	for _, call := range service.calls {
		if call.accountID == "wallet-7" {
			t.Fatalf("quarantined account was reconciled: %#v", service.calls)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- runner.RunAfterStartupReady(ctx, ready) }()
	select {
	case <-ready:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("active-account reconciliation loop did not become ready")
	}
	if err := runner.Check(context.Background()); err != nil {
		cancel()
		t.Fatalf("active-account readiness check: %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunAfterStartupReady() error = %v, want context canceled", err)
	}

	runner.Trigger("wallet-7", domain.ReconciliationTriggerOrderUnknown, "order-quarantined")
	if got := runner.SuppressedTriggerCount("wallet-7"); got != 1 {
		t.Fatalf("suppressed trigger count = %d, want 1", got)
	}
	if len(runner.requests) != 0 {
		t.Fatalf("quarantined trigger queue length = %d, want 0", len(runner.requests))
	}
}

func TestRunnerRejectsAccountConfiguredAsActiveAndQuarantined(t *testing.T) {
	_, err := NewRunner(RunnerParams{
		Service:             &fakeAccountReconciler{},
		Accounts:            []string{"wallet-1", "wallet-7"},
		QuarantinedAccounts: []string{"wallet-7"},
	})
	if err == nil || !strings.Contains(err.Error(), "both active and quarantined") {
		t.Fatalf("NewRunner() error = %v, want overlap rejection", err)
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

func TestRunnerCheckRejectsStaleCompletedResult(t *testing.T) {
	clock := &runnerTestClock{now: time.Date(2026, time.August, 19, 8, 0, 0, 0, time.UTC)}
	runner := newReadinessTestRunner(t, clock, false)
	runner.Sweep(context.Background(), domain.ReconciliationTriggerStartup)
	cancel, done := startReadinessTestLoop(t, runner)
	defer stopReadinessTestLoop(t, cancel, done)

	clock.Advance(2*time.Hour + time.Nanosecond)
	if err := runner.Check(context.Background()); err == nil || !strings.Contains(err.Error(), "reconciliation is stale") {
		t.Fatalf("Check() error = %v, want stale reconciliation", err)
	}
}

func TestRunnerCheckRejectsCompletedResultWithoutCompletedAt(t *testing.T) {
	clock := &runnerTestClock{now: time.Date(2026, time.August, 19, 8, 0, 0, 0, time.UTC)}
	runner := newReadinessTestRunner(t, clock, true)
	runner.Sweep(context.Background(), domain.ReconciliationTriggerStartup)
	cancel, done := startReadinessTestLoop(t, runner)
	defer stopReadinessTestLoop(t, cancel, done)

	if err := runner.Check(context.Background()); err == nil || !strings.Contains(err.Error(), "has no completed_at") {
		t.Fatalf("Check() error = %v, want missing completed_at", err)
	}
}

func TestRunnerCheckRejectsStartupSweepBeforeLoopStarts(t *testing.T) {
	clock := &runnerTestClock{now: time.Date(2026, time.August, 19, 8, 0, 0, 0, time.UTC)}
	runner := newReadinessTestRunner(t, clock, false)
	runner.Sweep(context.Background(), domain.ReconciliationTriggerStartup)

	if err := runner.Check(context.Background()); err == nil || !strings.Contains(err.Error(), "background loop has not started") {
		t.Fatalf("Check() error = %v, want loop-not-started failure", err)
	}
}

func TestRunnerCheckRejectsStoppedLoop(t *testing.T) {
	clock := &runnerTestClock{now: time.Date(2026, time.August, 19, 8, 0, 0, 0, time.UTC)}
	runner := newReadinessTestRunner(t, clock, false)
	runner.Sweep(context.Background(), domain.ReconciliationTriggerStartup)
	cancel, done := startReadinessTestLoop(t, runner)
	stopReadinessTestLoop(t, cancel, done)

	if err := runner.Check(context.Background()); err == nil || !strings.Contains(err.Error(), "background loop stopped") {
		t.Fatalf("Check() error = %v, want stopped-loop failure", err)
	}
}

func TestRunnerCheckRejectsInactiveLoop(t *testing.T) {
	clock := &runnerTestClock{now: time.Date(2026, time.August, 19, 8, 0, 0, 0, time.UTC)}
	runner := newReadinessTestRunner(t, clock, false)
	runner.Sweep(context.Background(), domain.ReconciliationTriggerStartup)
	cancel, done := startReadinessTestLoop(t, runner)
	defer stopReadinessTestLoop(t, cancel, done)

	clock.Advance(2*time.Hour + time.Nanosecond)
	completedAt := clock.Now()
	runner.remember("account-1", Result{Run: domain.ReconciliationRun{
		ExecutionAccountID: "account-1", Status: domain.ReconciliationRunCompleted, CompletedAt: &completedAt,
	}})
	if err := runner.Check(context.Background()); err == nil || !strings.Contains(err.Error(), "background loop is inactive") {
		t.Fatalf("Check() error = %v, want inactive-loop failure", err)
	}
}

func TestRunnerCheckAcceptsFreshResultsWhileLoopRuns(t *testing.T) {
	clock := &runnerTestClock{now: time.Date(2026, time.August, 19, 8, 0, 0, 0, time.UTC)}
	runner := newReadinessTestRunner(t, clock, false)
	runner.Sweep(context.Background(), domain.ReconciliationTriggerStartup)
	cancel, done := startReadinessTestLoop(t, runner)
	defer stopReadinessTestLoop(t, cancel, done)

	if err := runner.Check(context.Background()); err != nil {
		t.Fatalf("Check() error = %v, want healthy runner", err)
	}
}

func TestRunnerCheckAccountDoesNotLetUnrelatedWalletBlockPlacement(t *testing.T) {
	clock := &runnerTestClock{now: time.Date(2026, time.August, 25, 3, 20, 0, 0, time.UTC)}
	service := &fakeAccountReconciler{now: clock.Now}
	runner, err := NewRunner(RunnerParams{
		Service: service, Accounts: []string{"main", "wallet-7"}, Interval: time.Hour,
		Now: clock.Now, MaxResultAge: 2 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	runner.Sweep(context.Background(), domain.ReconciliationTriggerStartup)
	cancel, done := startReadinessTestLoop(t, runner)
	defer stopReadinessTestLoop(t, cancel, done)

	completedAt := clock.Now()
	runner.remember("main", Result{Run: domain.ReconciliationRun{
		ExecutionAccountID: "main", Status: domain.ReconciliationRunAttentionRequired, CompletedAt: &completedAt,
	}})
	if err := runner.Check(context.Background()); err == nil || !strings.Contains(err.Error(), "main") {
		t.Fatalf("global Check() error = %v, want main reconciliation failure", err)
	}
	if err := runner.CheckAccount(context.Background(), "wallet-7"); err != nil {
		t.Fatalf("wallet-7 CheckAccount() error = %v, want healthy account placement", err)
	}
	if err := runner.CheckAccount(context.Background(), "main"); err == nil || !strings.Contains(err.Error(), "ATTENTION_REQUIRED") {
		t.Fatalf("main CheckAccount() error = %v, want account-local failure", err)
	}
	if err := runner.CheckAccount(context.Background(), "wallet-8"); err == nil || !strings.Contains(err.Error(), "not active") {
		t.Fatalf("unknown CheckAccount() error = %v, want inactive account failure", err)
	}
}

func TestRunAfterStartupReadySignalsOnlyAfterLoopIsRunning(t *testing.T) {
	clock := &runnerTestClock{now: time.Date(2026, time.August, 19, 8, 0, 0, 0, time.UTC)}
	runner := newReadinessTestRunner(t, clock, false)
	runner.Sweep(context.Background(), domain.ReconciliationTriggerStartup)
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- runner.RunAfterStartupReady(ctx, ready) }()
	select {
	case <-ready:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("reconciliation loop did not signal readiness")
	}
	if err := runner.Check(context.Background()); err != nil {
		cancel()
		t.Fatalf("Check() immediately after readiness = %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunAfterStartupReady() error = %v, want context canceled", err)
	}
}

func TestNewRunnerBoundsResultFreshnessWindow(t *testing.T) {
	service := &fakeAccountReconciler{}
	if _, err := NewRunner(RunnerParams{
		Service: service, Accounts: []string{"account-1"}, Interval: time.Hour, MaxResultAge: 24*time.Hour + time.Nanosecond,
	}); err == nil || !strings.Contains(err.Error(), "must not exceed") {
		t.Fatalf("NewRunner() error = %v, want maximum-age rejection", err)
	}
	if _, err := NewRunner(RunnerParams{
		Service: service, Accounts: []string{"account-1"}, Interval: time.Hour, MaxResultAge: time.Hour,
	}); err == nil || !strings.Contains(err.Error(), "greater than interval") {
		t.Fatalf("NewRunner() error = %v, want interval relationship rejection", err)
	}
}

func newReadinessTestRunner(t *testing.T, clock *runnerTestClock, omitCompletedAt bool) *Runner {
	t.Helper()
	service := &fakeAccountReconciler{now: clock.Now, omitCompletedAt: omitCompletedAt}
	runner, err := NewRunner(RunnerParams{
		Service: service, Accounts: []string{"account-1"}, Interval: time.Hour,
		Now: clock.Now, MaxResultAge: 2 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func startReadinessTestLoop(t *testing.T, runner *Runner) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runner.RunAfterStartup(ctx)
	}()

	deadline := time.Now().Add(time.Second)
	for {
		runner.mu.Lock()
		running := runner.loopRunning
		runner.mu.Unlock()
		if running {
			return cancel, done
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("reconciliation loop did not start")
		}
		time.Sleep(time.Millisecond)
	}
}

func stopReadinessTestLoop(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RunAfterStartup() error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reconciliation loop did not stop")
	}
}
