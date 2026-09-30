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
	AccountTimeout      time.Duration
	// ScheduleOffset pins SCHEDULED runs to wall-clock instants congruent to
	// the offset modulo Interval (for example x3:30 and x8:30 with a 5-minute
	// interval and a 3m30s offset). Nil keeps the start-relative ticker.
	ScheduleOffset *time.Duration
	Logger         *slog.Logger
}

// DecisionActivity reports whether a decision cycle is running or due within
// lead. The decision runner implements it; reconciliation only reads it so the
// dependency points from the decision side into this package.
type DecisionActivity interface {
	DecisionBusy(now time.Time, lead time.Duration) bool
}

// afterFunc returns a channel that fires after the duration. It is injectable
// so schedule and deferral tests never sleep on the wall clock.
type afterFunc func(time.Duration) <-chan time.Time

// Runner 表示后端使用的 Runner 类型。
type Runner struct {
	service        AccountReconciler
	accounts       []string
	active         map[string]struct{}
	quarantined    map[string]struct{}
	interval       time.Duration
	now            func() time.Time
	maxAge         time.Duration
	accountTimeout time.Duration
	accountSlots   map[string]chan struct{}
	logger         *slog.Logger
	requests       chan request
	scheduleOffset *time.Duration
	scheduleAfter  afterFunc
	deferAfter     afterFunc

	mu               sync.Mutex
	lastResults      map[string]Result
	loopStarted      bool
	loopRunning      bool
	loopLastActivity time.Time
	loopStoppedAt    time.Time
	suppressed       map[string]uint64
	decisionActivity DecisionActivity
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
var _ port.ExecutionAccountGate = (*Runner)(nil)

const (
	defaultRunnerInterval = 5 * time.Minute
	maximumRunnerAge      = 24 * time.Hour

	// decisionLead must cover one reconciliation run (about 51s worst case
	// observed for wallet-6) so a deferred run cannot still hold the account
	// lock when the decision cycle wants to submit its batch.
	decisionLead = 60 * time.Second
	// maxDecisionDefer bounds how long SCHEDULED reconciliation yields, so a
	// stuck decision cycle cannot starve risk-state freshness. Interval plus
	// this cap plus one run stays below the 600s placement max_state_age.
	maxDecisionDefer   = 3 * time.Minute
	decisionPollPeriod = 2 * time.Second
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
	if params.AccountTimeout == 0 {
		params.AccountTimeout = min(2*params.Interval, 2*time.Minute)
	}
	if params.AccountTimeout < time.Second || params.AccountTimeout >= params.MaxResultAge {
		return nil, fmt.Errorf("reconciliation account timeout must be at least one second and below max result age")
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
	if params.ScheduleOffset != nil {
		if offset := *params.ScheduleOffset; offset < 0 || offset >= params.Interval {
			return nil, fmt.Errorf("reconciliation schedule offset must be in [0, interval)")
		}
		offset := *params.ScheduleOffset
		params.ScheduleOffset = &offset
	}
	if params.Logger == nil {
		params.Logger = slog.Default()
	}
	slots := make(map[string]chan struct{}, len(accounts))
	for _, account := range accounts {
		slots[account] = make(chan struct{}, 1)
	}
	return &Runner{
		accountTimeout: params.AccountTimeout, accountSlots: slots,
		service: params.Service, accounts: accounts, active: active, quarantined: quarantined,
		interval: params.Interval, now: params.Now, maxAge: params.MaxResultAge, logger: params.Logger,
		requests: make(chan request, 1024), lastResults: make(map[string]Result),
		suppressed:     make(map[string]uint64),
		scheduleOffset: params.ScheduleOffset, scheduleAfter: time.After, deferAfter: time.After,
	}, nil
}

// BindDecisionActivity installs the decision runner after construction; live
// composition creates reconciliation first because decision delivery uses this
// runner as its account gate. Unbound runners never defer.
func (runner *Runner) BindDecisionActivity(activity DecisionActivity) error {
	if activity == nil {
		return fmt.Errorf("decision activity is required")
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if runner.decisionActivity != nil {
		return fmt.Errorf("reconciliation runner decision activity is already bound")
	}
	runner.decisionActivity = activity
	return nil
}

func (runner *Runner) boundDecisionActivity() DecisionActivity {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.decisionActivity
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

	// One bounded queue and one worker per configured account. A stuck account
	// cannot consume another account's worker, queue, or heartbeat.
	workCtx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	queues := make(map[string]chan request, len(runner.accounts))
	failures := make(chan error, len(runner.accounts))
	for _, accountID := range runner.accounts {
		queue := make(chan request, 1)
		queues[accountID] = queue
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-workCtx.Done():
					return
				case requested := <-queue:
					requested, err := runner.yieldToDecisionCycle(workCtx, requested, queue)
					if err != nil {
						return
					}
					_, err = runner.runAccount(workCtx, RunAccountParams{ExecutionAccountID: requested.accountID,
						Trigger: requested.trigger, FocusOrderID: requested.orderID})
					if err != nil {
						select {
						case failures <- err:
						case <-workCtx.Done():
							return
						}
					}
				}
			}
		}()
	}
	enqueue := func(requested request) {
		// Coalesce a burst into at most one pending account sweep. Every sweep
		// scans all pending orders, so dropping duplicate focus hints loses no work.
		if queue := queues[requested.accountID]; queue != nil {
			select {
			case queue <- requested:
			default:
			}
		}
	}
	var scheduled <-chan time.Time
	rearm := func() {}
	if runner.scheduleOffset == nil {
		ticker := time.NewTicker(runner.interval)
		defer ticker.Stop()
		scheduled = ticker.C
	} else {
		// Aligned mode recomputes the next wall-clock instant after every tick,
		// so neither restarts nor slow iterations shift the phase.
		var lastTick time.Time
		rearm = func() {
			now := runner.now().UTC()
			next := nextAlignedTick(now, runner.interval, *runner.scheduleOffset)
			if !lastTick.IsZero() && !next.After(lastTick) {
				next = lastTick.Add(runner.interval)
			}
			lastTick = next
			scheduled = runner.scheduleAfter(next.Sub(now))
		}
		rearm()
	}
	accumulated := append([]error(nil), initialErrors...)
	for {
		select {
		case <-ctx.Done():
			return errors.Join(append(accumulated, ctx.Err())...)
		case <-scheduled:
			rearm()
			runner.recordLoopActivity()
			for _, accountID := range runner.accounts {
				enqueue(request{accountID: accountID, trigger: domain.ReconciliationTriggerScheduled})
			}
		case requested := <-runner.requests:
			runner.recordLoopActivity()
			enqueue(requested)
		case err := <-failures:
			accumulated = appendBounded(accumulated, err)
		}
	}
}

// nextAlignedTick returns the first instant strictly after now that is
// congruent to offset modulo interval (on absolute UTC time).
func nextAlignedTick(now time.Time, interval, offset time.Duration) time.Time {
	now = now.UTC()
	next := now.Truncate(interval).Add(offset)
	if !next.After(now) {
		next = next.Add(interval)
	}
	return next
}

// yieldToDecisionCycle delays a SCHEDULED run while a decision cycle is running
// or due within decisionLead. The account lock has no priority, so a scheduled
// run that grabs it first would make the decision batch wait until its signals
// are stale. Waiting happens before AcquireExecutionAccount and never holds the
// lock. Every other trigger runs immediately; if one arrives while deferring it
// replaces the pending scheduled run, because each run scans the whole account.
func (runner *Runner) yieldToDecisionCycle(ctx context.Context, requested request, queue <-chan request) (request, error) {
	if requested.trigger != domain.ReconciliationTriggerScheduled {
		return requested, nil
	}
	activity := runner.boundDecisionActivity()
	if activity == nil {
		return requested, nil
	}
	startedAt := runner.now().UTC()
	if !activity.DecisionBusy(startedAt, decisionLead) {
		return requested, nil
	}
	deadline := startedAt.Add(maxDecisionDefer)
	const reason = "decision cycle running or due within lead"
	runner.logger.Info("scheduled reconciliation deferred for decision cycle",
		"execution_account_id", requested.accountID, "reason", reason,
		"lead", decisionLead, "max_defer", maxDecisionDefer)
	for {
		select {
		case <-ctx.Done():
			return request{}, ctx.Err()
		case next := <-queue:
			if next.trigger != domain.ReconciliationTriggerScheduled {
				runner.logger.Info("deferred scheduled reconciliation superseded by immediate trigger",
					"execution_account_id", requested.accountID, "trigger", next.trigger,
					"waited", runner.now().UTC().Sub(startedAt))
				return next, nil
			}
			// A later tick coalesces into the run already being deferred.
		case <-runner.deferAfter(decisionPollPeriod):
		}
		now := runner.now().UTC()
		waited := now.Sub(startedAt)
		if !activity.DecisionBusy(now, decisionLead) {
			runner.logger.Info("scheduled reconciliation resumed after decision cycle",
				"execution_account_id", requested.accountID, "reason", reason, "waited", waited)
			return requested, nil
		}
		if !now.Before(deadline) {
			runner.logger.Warn("scheduled reconciliation deferral limit exceeded; running during decision window",
				"execution_account_id", requested.accountID, "reason", reason, "waited", waited,
				"max_defer", maxDecisionDefer)
			return requested, nil
		}
	}
}

// Check implements live readiness. Every configured account must have a fresh
// finished scan without account-wide issues, and the background loop must be
// running and active. Scoped issues (one order, one token) degrade only the
// affected placements through CheckPlacement, never the whole process.
func (runner *Runner) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := runner.now().UTC()
	runner.mu.Lock()
	defer runner.mu.Unlock()
	for _, accountID := range runner.accounts {
		if err := runner.checkAccountResultLocked(accountID, now); err != nil {
			return err
		}
	}
	return runner.checkLoopLocked(now)
}

// CheckAccount gates a placement only on the account that will own the order.
// A reconciliation issue in an unrelated wallet still degrades global
// readiness through Check, but cannot suppress a healthy wallet's strategy.
func (runner *Runner) CheckAccount(ctx context.Context, accountID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return fmt.Errorf("execution account id is required for reconciliation readiness")
	}
	now := runner.now().UTC()
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if _, active := runner.active[accountID]; !active {
		return fmt.Errorf("execution account %q is not active for reconciliation", accountID)
	}
	if err := runner.checkAccountResultLocked(accountID, now); err != nil {
		return err
	}
	return runner.checkLoopLocked(now)
}

// CheckPlacement gates one concrete placement. Beyond the account freshness
// and account-wide checks it refuses only intents whose token, condition, or
// market is named by an OPEN scoped issue of the latest finished scan.
func (runner *Runner) CheckPlacement(ctx context.Context, order domain.Order) error {
	if err := runner.CheckAccount(ctx, order.Intent.ExecutionAccountID); err != nil {
		return err
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	result := runner.lastResults[strings.TrimSpace(order.Intent.ExecutionAccountID)]
	for _, issue := range result.Issues {
		if issue.Status != domain.ReconciliationIssueOpen || !issue.BlocksIntent(order.Intent) {
			continue
		}
		return fmt.Errorf(
			"execution account %q has an open %s issue (%s) on market %s / token %s (order %s); only that market is gated",
			order.Intent.ExecutionAccountID, issue.Type, issue.Resolution, firstNonEmpty(issue.MarketID, issue.ConditionID),
			issue.TokenID, issue.OrderID,
		)
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (runner *Runner) checkAccountResultLocked(accountID string, now time.Time) error {
	result, exists := runner.lastResults[accountID]
	if !exists {
		return fmt.Errorf("execution account %q has not completed reconciliation", accountID)
	}
	switch result.Run.Status {
	case domain.ReconciliationRunCompleted:
	case domain.ReconciliationRunAttentionRequired:
		// The scan finished. Only account-wide issues block every placement;
		// scoped issues are enforced per intent by CheckPlacement.
		if result.Impact.AccountWide {
			return fmt.Errorf("execution account %q reconciliation has account-wide open issues: %s",
				accountID, strings.Join(result.Impact.Reasons, ", "))
		}
	default:
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
	return nil
}

func (runner *Runner) checkLoopLocked(now time.Time) error {
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
	result := SweepResult{Trigger: trigger, Runs: make([]Result, len(runner.accounts))}
	failures := make([]error, len(runner.accounts))
	var workers sync.WaitGroup
	for index, accountID := range runner.accounts {
		workers.Add(1)
		go func() {
			defer workers.Done()
			result.Runs[index], failures[index] = runner.runAccount(ctx, RunAccountParams{ExecutionAccountID: accountID, Trigger: trigger})
		}()
	}
	workers.Wait()
	for _, err := range failures {
		if err != nil {
			result.Errors = append(result.Errors, err)
		}
	}
	return result
}

func (runner *Runner) runAccount(ctx context.Context, params RunAccountParams) (Result, error) {
	runCtx, cancel := context.WithTimeout(ctx, runner.accountTimeout)
	defer cancel()
	release, err := runner.AcquireExecutionAccount(runCtx, params.ExecutionAccountID)
	if err != nil {
		return Result{}, err
	}
	defer release()
	result, err := runner.service.RunAccount(runCtx, params)
	if runCtx.Err() != nil {
		err = errors.Join(err, runCtx.Err())
		result.Run.Status = domain.ReconciliationRunFailed
		result.Run.Error = err.Error()
	}
	runner.remember(params.ExecutionAccountID, result)
	if err != nil {
		return result, fmt.Errorf("reconcile %s: %w", params.ExecutionAccountID, err)
	}
	return result, nil
}

// AcquireExecutionAccount shares the runner's existing per-account slot with
// decision delivery. Reconciliation therefore waits until an account's whole
// durable order batch has finished submitting, while unrelated accounts keep
// running independently.
func (runner *Runner) AcquireExecutionAccount(ctx context.Context, executionAccountID string) (func(), error) {
	accountID := strings.TrimSpace(executionAccountID)
	slot, exists := runner.accountSlots[accountID]
	if !exists {
		return nil, fmt.Errorf("execution account %q is not active for reconciliation", accountID)
	}
	select {
	case slot <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() { <-slot })
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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
