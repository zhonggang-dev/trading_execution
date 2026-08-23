// Package decisionrunner schedules the production decision cycle on exact UTC
// boundaries. It deliberately skips missed boundaries instead of replaying
// stale market decisions and serializes all cycle executions in one process.
package decisionrunner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/decisioncycle"
)

const RequiredInterval = 10 * time.Minute

const defaultMaxStartLateness = 30 * time.Second

var ErrCycleInProgress = errors.New("decision cycle is already in progress")

type Cycle interface {
	Run(context.Context, time.Time) (decisioncycle.RunResult, error)
}

type startupRecoverer interface {
	RecoverStartup(context.Context) error
}

// WaitFunc is injectable so scheduling semantics can be tested without using
// wall-clock sleeps. Production leaves it nil.
type WaitFunc func(context.Context, time.Time) error

type Params struct {
	Cycle        Cycle
	Interval     time.Duration
	StartupDelay time.Duration
	// MaxStartLateness bounds how long after the configured due time a market
	// decision may still start. It is intentionally much shorter than the
	// cadence so a paused VM cannot place an order from an old boundary.
	MaxStartLateness time.Duration
	Timeout          time.Duration
	Now              func() time.Time
	Wait             WaitFunc
	Logger           *slog.Logger
}

type Runner struct {
	cycle            Cycle
	interval         time.Duration
	startupDelay     time.Duration
	maxStartLateness time.Duration
	timeout          time.Duration
	now              func() time.Time
	wait             WaitFunc
	logger           *slog.Logger

	mu                    sync.Mutex
	loopStarted           bool
	loopRunning           bool
	loopStoppedAt         time.Time
	nextBoundary          time.Time
	nextDueAt             time.Time
	lastScheduledBoundary time.Time
	inFlight              bool
	lastStartedAt         time.Time
	lastCompletedAt       time.Time
	lastSucceededAt       time.Time
	lastError             error
	skippedBoundaries     uint64
}

// Status is a credential-free snapshot for tests and operational diagnostics.
type Status struct {
	LoopStarted       bool
	LoopRunning       bool
	LoopStoppedAt     time.Time
	NextBoundary      time.Time
	NextDueAt         time.Time
	InFlight          bool
	LastStartedAt     time.Time
	LastCompletedAt   time.Time
	LastSucceededAt   time.Time
	LastError         string
	SkippedBoundaries uint64
}

func New(params Params) (*Runner, error) {
	if params.Cycle == nil {
		return nil, fmt.Errorf("decision cycle service is required")
	}
	if params.Interval == 0 {
		params.Interval = RequiredInterval
	}
	if params.Interval != RequiredInterval {
		return nil, fmt.Errorf("decision cycle interval must be exactly %s", RequiredInterval)
	}
	if params.StartupDelay < 0 || params.StartupDelay >= params.Interval {
		return nil, fmt.Errorf("decision cycle startup delay must be non-negative and less than the interval")
	}
	if params.MaxStartLateness == 0 {
		params.MaxStartLateness = defaultMaxStartLateness
	}
	if params.MaxStartLateness <= 0 || params.MaxStartLateness >= params.Interval {
		return nil, fmt.Errorf("decision cycle maximum start lateness must be positive and less than the interval")
	}
	if params.Timeout <= 0 || params.Timeout >= params.Interval {
		return nil, fmt.Errorf("decision cycle timeout must be positive and less than the interval")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	if params.Wait == nil {
		params.Wait = waitUntil
	}
	if params.Logger == nil {
		params.Logger = slog.Default()
	}
	return &Runner{
		cycle: params.Cycle, interval: params.Interval, startupDelay: params.StartupDelay,
		maxStartLateness: params.MaxStartLateness,
		timeout:          params.Timeout, now: params.Now, wait: params.Wait, logger: params.Logger,
	}, nil
}

func waitUntil(ctx context.Context, at time.Time) error {
	delay := time.Until(at)
	if delay <= 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Run starts the loop without a startup handshake.
func (runner *Runner) Run(ctx context.Context) error {
	return runner.runLoop(ctx, nil)
}

// RunReady closes ready only after the loop is running and its first future
// boundary has been published to readiness state.
func (runner *Runner) RunReady(ctx context.Context, ready chan<- struct{}) error {
	if ready == nil {
		return fmt.Errorf("decision runner readiness channel is required")
	}
	return runner.runLoop(ctx, ready)
}

func (runner *Runner) runLoop(ctx context.Context, ready chan<- struct{}) error {
	if err := runner.beginLoop(); err != nil {
		return err
	}
	defer runner.endLoop()
	if recoverer, ok := runner.cycle.(startupRecoverer); ok {
		recoveryContext, cancel := context.WithTimeout(ctx, runner.timeout)
		recoveryErr := recoverer.RecoverStartup(recoveryContext)
		cancel()
		if recoveryErr != nil {
			runner.recordRecoveryError(recoveryErr)
			return fmt.Errorf("startup strategy intent recovery: %w", recoveryErr)
		}
	}
	readySent := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		now := runner.now().UTC()
		boundary, dueAt := nextSchedule(now, runner.lastBoundary(), runner.interval, runner.startupDelay)
		runner.setSchedule(boundary, dueAt)
		if ready != nil && !readySent {
			close(ready)
			readySent = true
		}
		if err := runner.wait(ctx, dueAt); err != nil {
			return err
		}
		wokeAt := runner.now().UTC()
		if wokeAt.After(dueAt.Add(runner.maxStartLateness)) {
			runner.recordSkipped(boundary, wokeAt)
			continue
		}
		_, _ = runner.runBoundary(ctx, boundary)
	}
}

// nextSchedule returns the current boundary only while it is still inside its
// configured delay window. Once that due time is missed, it advances directly
// to the next future boundary and never backfills intermediate cycles.
func nextSchedule(now, lastBoundary time.Time, interval, delay time.Duration) (time.Time, time.Time) {
	now = now.UTC()
	boundary := now.Truncate(interval)
	dueAt := boundary.Add(delay)
	if dueAt.Before(now) {
		boundary = boundary.Add(interval)
		dueAt = boundary.Add(delay)
	}
	if !lastBoundary.IsZero() && !boundary.After(lastBoundary) {
		boundary = lastBoundary.UTC().Add(interval)
		dueAt = boundary.Add(delay)
	}
	return boundary, dueAt
}

// runBoundary is protected independently of the scheduler loop so future
// manual triggers cannot overlap a scheduled run.
func (runner *Runner) runBoundary(ctx context.Context, boundary time.Time) (decisioncycle.RunResult, error) {
	startedAt := runner.now().UTC()
	if !runner.beginCycle(startedAt) {
		return decisioncycle.RunResult{}, ErrCycleInProgress
	}
	runContext, cancel := context.WithTimeout(ctx, runner.timeout)
	result, err := runner.cycle.Run(runContext, boundary.UTC())
	cancel()
	completedAt := runner.now().UTC()
	runner.completeCycle(completedAt, err)
	runner.logResult(boundary.UTC(), result, err)
	return result, err
}

func (runner *Runner) logResult(boundary time.Time, result decisioncycle.RunResult, runErr error) {
	bindings, intents, submitted, disabled, failed := summarize(result)
	attributes := []any{
		"decision_at", boundary,
		"snapshot_id", result.PredictionSnapshotID,
		"bindings", bindings,
		"intents", intents,
		"submitted", submitted,
		"submission_disabled", disabled,
		"failed_intents", failed,
		"binding_runs", bindingRunSummaries(result),
	}
	if runErr != nil {
		runner.logger.Error("decision cycle completed with errors", append(attributes, "error", runErr)...)
		return
	}
	runner.logger.Info("decision cycle completed", attributes...)
}

type bindingRunSummary struct {
	ModelID                   string `json:"model_id"`
	PredictionModelID         string `json:"prediction_model_id"`
	StrategyID                string `json:"strategy_id"`
	ExecutionAccountID        string `json:"execution_account_id"`
	Predictions               int    `json:"predictions"`
	Positions                 int    `json:"positions"`
	Intents                   int    `json:"intents"`
	OrderSubmissionEnabled    bool   `json:"order_submission_enabled"`
	AccountSubmissionDisabled bool   `json:"account_submission_disabled"`
	EntrySubmissionEnabled    bool   `json:"entry_submission_enabled"`
	EntryBlockReason          string `json:"entry_block_reason,omitempty"`
	Failed                    bool   `json:"failed"`
}

func bindingRunSummaries(result decisioncycle.RunResult) []bindingRunSummary {
	summaries := make([]bindingRunSummary, 0, len(result.Runs))
	for _, run := range result.Runs {
		summaries = append(summaries, bindingRunSummary{
			ModelID:                   run.Context.ModelID,
			PredictionModelID:         run.PredictionModelID,
			StrategyID:                run.Context.StrategyID,
			ExecutionAccountID:        run.Context.ExecutionAccountID,
			Predictions:               run.PredictionCount,
			Positions:                 run.PositionCount,
			Intents:                   len(run.Intents),
			OrderSubmissionEnabled:    run.OrderSubmissionEnabled,
			AccountSubmissionDisabled: run.AccountSubmissionDisabled,
			EntrySubmissionEnabled:    run.EntrySubmissionEnabled,
			EntryBlockReason:          run.EntryBlockReason,
			Failed:                    run.Error != nil || run.EntryBlockReason != "",
		})
	}
	return summaries
}

func summarize(result decisioncycle.RunResult) (bindings, intents, submitted, disabled, failed int) {
	bindings = len(result.Runs)
	for _, run := range result.Runs {
		for _, intent := range run.Intents {
			intents++
			switch {
			case intent.SubmissionDisabled:
				disabled++
			case intent.Error != nil || (intent.DeliveryStatus != "" && intent.DeliveryStatus != domain.DecisionIntentSubmitted):
				failed++
			default:
				submitted++
			}
		}
	}
	return bindings, intents, submitted, disabled, failed
}

// Check fails readiness when the scheduler stopped, its current run exceeded
// the configured timeout, the loop fell more than one cadence behind, or the
// most recent cycle failed. The loop itself continues and can recover on the
// next boundary.
func (runner *Runner) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := runner.now().UTC()
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if !runner.loopStarted {
		return fmt.Errorf("decision cycle loop has not started")
	}
	if !runner.loopRunning {
		return fmt.Errorf("decision cycle loop stopped at %s", runner.loopStoppedAt.UTC().Format(time.RFC3339Nano))
	}
	if runner.inFlight && now.After(runner.lastStartedAt.Add(runner.timeout)) {
		return fmt.Errorf("decision cycle exceeded timeout %s", runner.timeout)
	}
	if !runner.inFlight && !runner.nextDueAt.IsZero() && now.After(runner.nextDueAt.Add(runner.maxStartLateness)) {
		return fmt.Errorf("decision cycle scheduler is behind its next boundary")
	}
	if runner.lastError != nil {
		return fmt.Errorf("most recent decision cycle failed: %w", runner.lastError)
	}
	if runner.lastSucceededAt.IsZero() {
		return fmt.Errorf("decision cycle has not completed its first successful boundary")
	}
	return nil
}

func (runner *Runner) Snapshot() Status {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	status := Status{
		LoopStarted: runner.loopStarted, LoopRunning: runner.loopRunning,
		LoopStoppedAt: runner.loopStoppedAt, NextBoundary: runner.nextBoundary,
		NextDueAt: runner.nextDueAt, InFlight: runner.inFlight,
		LastStartedAt: runner.lastStartedAt, LastCompletedAt: runner.lastCompletedAt,
		LastSucceededAt: runner.lastSucceededAt, SkippedBoundaries: runner.skippedBoundaries,
	}
	if runner.lastError != nil {
		status.LastError = runner.lastError.Error()
	}
	return status
}

func (runner *Runner) beginLoop() error {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if runner.loopRunning {
		return fmt.Errorf("decision cycle loop is already running")
	}
	runner.loopStarted = true
	runner.loopRunning = true
	runner.loopStoppedAt = time.Time{}
	return nil
}

func (runner *Runner) endLoop() {
	stoppedAt := runner.now().UTC()
	runner.mu.Lock()
	defer runner.mu.Unlock()
	runner.loopRunning = false
	runner.loopStoppedAt = stoppedAt
}

func (runner *Runner) lastBoundary() time.Time {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.lastScheduledBoundary
}

func (runner *Runner) setSchedule(boundary, dueAt time.Time) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	runner.nextBoundary = boundary.UTC()
	runner.nextDueAt = dueAt.UTC()
	runner.lastScheduledBoundary = boundary.UTC()
}

func (runner *Runner) beginCycle(startedAt time.Time) bool {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if runner.inFlight {
		return false
	}
	runner.inFlight = true
	runner.lastStartedAt = startedAt.UTC()
	return true
}

func (runner *Runner) completeCycle(completedAt time.Time, err error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	runner.inFlight = false
	runner.lastCompletedAt = completedAt.UTC()
	runner.lastError = err
	if err == nil {
		runner.lastSucceededAt = completedAt.UTC()
	}
}

func (runner *Runner) recordSkipped(boundary, wokeAt time.Time) {
	runner.mu.Lock()
	runner.skippedBoundaries++
	runner.lastCompletedAt = wokeAt.UTC()
	runner.lastError = fmt.Errorf("decision boundary %s was skipped after exceeding maximum start lateness", boundary.UTC().Format(time.RFC3339Nano))
	runner.mu.Unlock()
	runner.logger.Warn("decision cycle boundary skipped because scheduler woke too late",
		"decision_at", boundary.UTC(), "woke_at", wokeAt.UTC())
}

func (runner *Runner) recordRecoveryError(err error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	runner.lastCompletedAt = runner.now().UTC()
	runner.lastError = fmt.Errorf("pending strategy intent recovery: %w", err)
}
