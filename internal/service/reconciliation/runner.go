package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// AccountReconciler 表示后端使用的 AccountReconciler 类型。
type AccountReconciler interface {
	RunAccount(context.Context, RunAccountParams) (Result, error)
}

// RunnerParams 表示后端使用的 RunnerParams 类型。
type RunnerParams struct {
	Service             AccountReconciler
	Accounts            []string
	QuarantinedAccounts []string
	Interval            time.Duration
	Now                 func() time.Time
	MaxResultAge        time.Duration
	Logger              *slog.Logger
}

// Runner 表示后端使用的 Runner 类型。
type Runner struct {
	service     AccountReconciler
	accounts    []string
	active      map[string]struct{}
	quarantined map[string]struct{}
	interval    time.Duration
	now         func() time.Time
	maxAge      time.Duration
	logger      *slog.Logger
	requests    chan request

	mu               sync.Mutex
	lastResults      map[string]Result
	loopStarted      bool
	loopRunning      bool
	loopLastActivity time.Time
	loopStoppedAt    time.Time
	suppressed       map[string]uint64
}

// request 表示后端使用的 request 类型。
type request struct {
	accountID string
	trigger   domain.ReconciliationTrigger
	orderID   string
}

// SweepResult 表示后端使用的 SweepResult 类型。
type SweepResult struct {
	Trigger domain.ReconciliationTrigger `json:"trigger"`
	Runs    []Result                     `json:"runs"`
	Errors  []error                      `json:"-"`
}

var _ port.ReconciliationTriggerer = (*Runner)(nil)

const (
	defaultRunnerInterval = 5 * time.Minute
	maximumRunnerAge      = 24 * time.Hour
)

// NewRunner 校验账户和周期配置后创建对账运行器。
func NewRunner(params RunnerParams) (*Runner, error) {
	if params.Service == nil {
		return nil, fmt.Errorf("reconciliation service is required")
	}
	if params.Interval == 0 {
		params.Interval = defaultRunnerInterval
	}
	if params.Interval < time.Second {
		return nil, fmt.Errorf("reconciliation interval must be at least one second")
	}
	if params.Interval >= maximumRunnerAge {
		return nil, fmt.Errorf("reconciliation interval must be less than %s", maximumRunnerAge)
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	if params.MaxResultAge == 0 {
		params.MaxResultAge = params.Interval * 3
		if params.MaxResultAge > maximumRunnerAge {
			params.MaxResultAge = maximumRunnerAge
		}
	}
	if params.MaxResultAge <= params.Interval {
		return nil, fmt.Errorf("reconciliation max result age must be greater than interval")
	}
	if params.MaxResultAge > maximumRunnerAge {
		return nil, fmt.Errorf("reconciliation max result age must not exceed %s", maximumRunnerAge)
	}
	accounts, active, err := normalizeRunnerAccounts(params.Accounts, nil)
	if err != nil {
		return nil, err
	}
	if len(accounts) == 0 {
		return nil, fmt.Errorf("at least one reconciliation account is required")
	}
	_, quarantined, err := normalizeRunnerAccounts(params.QuarantinedAccounts, active)
	if err != nil {
		return nil, err
	}
	if params.Logger == nil {
		params.Logger = slog.Default()
	}
	return &Runner{
		service: params.Service, accounts: accounts, active: active, quarantined: quarantined,
		interval: params.Interval, now: params.Now, maxAge: params.MaxResultAge, logger: params.Logger,
		requests: make(chan request, 1024), lastResults: make(map[string]Result),
		suppressed: make(map[string]uint64),
	}, nil
}

func normalizeRunnerAccounts(rawAccounts []string, disallowed map[string]struct{}) ([]string, map[string]struct{}, error) {
	seen := make(map[string]struct{}, len(rawAccounts))
	accounts := make([]string, 0, len(rawAccounts))
	for _, raw := range rawAccounts {
		account := strings.TrimSpace(raw)
		if account == "" {
			return nil, nil, fmt.Errorf("reconciliation account id is empty")
		}
		if _, rejected := disallowed[account]; rejected {
			return nil, nil, fmt.Errorf("execution account %q cannot be both active and quarantined for reconciliation", account)
		}
		if _, duplicate := seen[account]; duplicate {
			continue
		}
		seen[account] = struct{}{}
		accounts = append(accounts, account)
	}
	return accounts, seen, nil
}

// Trigger 以非阻塞方式提交一次账户对账请求。
func (runner *Runner) Trigger(accountID string, trigger domain.ReconciliationTrigger, orderID string) {
	request := request{accountID: strings.TrimSpace(accountID), trigger: trigger, orderID: strings.TrimSpace(orderID)}
	if request.accountID == "" {
		return
	}
	if _, quarantined := runner.quarantined[request.accountID]; quarantined {
		runner.mu.Lock()
		runner.suppressed[request.accountID]++
		runner.mu.Unlock()
		runner.logger.Warn("automatic reconciliation trigger suppressed for quarantined execution account",
			"execution_account_id", request.accountID,
			"trigger", request.trigger,
			"focus_order_id", request.orderID,
		)
		return
	}
	if _, active := runner.active[request.accountID]; !active {
		runner.logger.Warn("automatic reconciliation trigger ignored for unconfigured execution account",
			"execution_account_id", request.accountID,
			"trigger", request.trigger,
			"focus_order_id", request.orderID,
		)
		return
	}
	select {
	case runner.requests <- request:
	default:
	}
}

// SuppressedTriggerCount exposes the in-process audit counter for automatic
// triggers rejected by account quarantine. Durable order/issue evidence is
// deliberately left untouched.
func (runner *Runner) SuppressedTriggerCount(accountID string) uint64 {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.suppressed[strings.TrimSpace(accountID)]
}

// Run 先执行启动对账再持续处理周期和即时对账请求。
func (runner *Runner) Run(ctx context.Context) error {
	startup := runner.Sweep(ctx, domain.ReconciliationTriggerStartup)
	if err := ctx.Err(); err != nil {
		return err
	}
	return runner.runLoop(ctx, startup.Errors, nil)
}

// RunAfterStartup starts only the scheduled/immediate loop. Production uses
// this after a synchronous startup Sweep has passed before opening HTTP.
func (runner *Runner) RunAfterStartup(ctx context.Context) error {
	return runner.runLoop(ctx, nil, nil)
}

// RunAfterStartupReady is the production startup handshake. It closes ready
// only after the loop has acquired its single-runner state, allowing callers
// to start crash-recovery coordinators without racing the placement gate.
func (runner *Runner) RunAfterStartupReady(ctx context.Context, ready chan<- struct{}) error {
	if ready == nil {
		return fmt.Errorf("reconciliation loop readiness channel is required")
	}
	return runner.runLoop(ctx, nil, ready)
}

func (runner *Runner) runLoop(ctx context.Context, initialErrors []error, ready chan<- struct{}) error {
	if err := runner.beginLoop(); err != nil {
		return err
	}
	defer runner.endLoop()
	if ready != nil {
		close(ready)
	}

	ticker := time.NewTicker(runner.interval)
	defer ticker.Stop()
	var accumulated []error
	accumulated = append(accumulated, initialErrors...)
	for {
		select {
		case <-ctx.Done():
			return errors.Join(append(accumulated, ctx.Err())...)
		case <-ticker.C:
			runner.recordLoopActivity()
			result := runner.Sweep(ctx, domain.ReconciliationTriggerScheduled)
			accumulated = appendBounded(accumulated, result.Errors...)
			runner.recordLoopActivity()
		case requested := <-runner.requests:
			runner.recordLoopActivity()
			params := RunAccountParams{ExecutionAccountID: requested.accountID, Trigger: requested.trigger, FocusOrderID: requested.orderID}
			result, err := runner.service.RunAccount(ctx, params)
			runner.remember(requested.accountID, result)
			if err != nil {
				accumulated = appendBounded(accumulated, err)
			}
			runner.recordLoopActivity()
		}
	}
}

// Check implements live readiness. Every configured account must have a fresh,
// completed reconciliation and the background loop must be running and active.
func (runner *Runner) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := runner.now().UTC()
	runner.mu.Lock()
	defer runner.mu.Unlock()
	for _, accountID := range runner.accounts {
		result, exists := runner.lastResults[accountID]
		if !exists {
			return fmt.Errorf("execution account %q has not completed reconciliation", accountID)
		}
		if result.Run.Status != domain.ReconciliationRunCompleted {
			return fmt.Errorf("execution account %q reconciliation status is %s", accountID, result.Run.Status)
		}
		if result.Run.CompletedAt == nil || result.Run.CompletedAt.IsZero() {
			return fmt.Errorf("execution account %q completed reconciliation has no completed_at", accountID)
		}
		completedAt := result.Run.CompletedAt.UTC()
		if completedAt.After(now) {
			return fmt.Errorf("execution account %q reconciliation completed_at is in the future", accountID)
		}
		if age := now.Sub(completedAt); age > runner.maxAge {
			return fmt.Errorf("execution account %q reconciliation is stale (age %s, maximum %s)", accountID, age, runner.maxAge)
		}
	}
	if !runner.loopStarted {
		return fmt.Errorf("reconciliation background loop has not started")
	}
	if !runner.loopRunning {
		return fmt.Errorf("reconciliation background loop stopped at %s", runner.loopStoppedAt.UTC().Format(time.RFC3339Nano))
	}
	if runner.loopLastActivity.IsZero() {
		return fmt.Errorf("reconciliation background loop has no activity timestamp")
	}
	if runner.loopLastActivity.After(now) {
		return fmt.Errorf("reconciliation background loop activity is in the future")
	}
	if age := now.Sub(runner.loopLastActivity); age > runner.maxAge {
		return fmt.Errorf("reconciliation background loop is inactive (age %s, maximum %s)", age, runner.maxAge)
	}
	return nil
}

func (runner *Runner) beginLoop() error {
	now := runner.now().UTC()
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if runner.loopRunning {
		return fmt.Errorf("reconciliation background loop is already running")
	}
	runner.loopStarted = true
	runner.loopRunning = true
	runner.loopLastActivity = now
	runner.loopStoppedAt = time.Time{}
	return nil
}

func (runner *Runner) endLoop() {
	now := runner.now().UTC()
	runner.mu.Lock()
	defer runner.mu.Unlock()
	runner.loopRunning = false
	runner.loopStoppedAt = now
}

func (runner *Runner) recordLoopActivity() {
	now := runner.now().UTC()
	runner.mu.Lock()
	defer runner.mu.Unlock()
	runner.loopLastActivity = now
}

// Sweep 执行一次有界扫描并处理选中的记录。
func (runner *Runner) Sweep(ctx context.Context, trigger domain.ReconciliationTrigger) SweepResult {
	result := SweepResult{Trigger: trigger}
	for _, accountID := range runner.accounts {
		if err := ctx.Err(); err != nil {
			result.Errors = append(result.Errors, err)
			break
		}
		params := RunAccountParams{ExecutionAccountID: accountID, Trigger: trigger}
		run, err := runner.service.RunAccount(ctx, params)
		result.Runs = append(result.Runs, run)
		runner.remember(accountID, run)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("reconcile %s: %w", accountID, err))
		}
	}
	return result
}

// LastResult 返回指定账户最近一次对账结果。
func (runner *Runner) LastResult(accountID string) (Result, bool) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	result, ok := runner.lastResults[strings.TrimSpace(accountID)]
	return result, ok
}

// remember 并发安全地保存账户最近一次对账结果。
func (runner *Runner) remember(accountID string, result Result) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	runner.lastResults[accountID] = result
}

// appendBounded 追加并限制 Bounded。
func appendBounded(existing []error, values ...error) []error {
	for _, value := range values {
		if value == nil {
			continue
		}
		if len(existing) == 100 {
			copy(existing, existing[1:])
			existing = existing[:99]
		}
		existing = append(existing, value)
	}
	return existing
}
