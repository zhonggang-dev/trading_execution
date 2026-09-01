package timeexit

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const (
	defaultInterval = time.Minute
	defaultTimeout  = 45 * time.Second
)

type Sweeper interface {
	Run(context.Context, time.Time) (RunResult, error)
}

type RunnerParams struct {
	Service  Sweeper
	Interval time.Duration
	Timeout  time.Duration
	Now      func() time.Time
	Logger   *slog.Logger
}

type Runner struct {
	service  Sweeper
	interval time.Duration
	timeout  time.Duration
	now      func() time.Time
	logger   *slog.Logger

	mu              sync.Mutex
	loopStarted     bool
	loopRunning     bool
	inFlight        bool
	lastStartedAt   time.Time
	lastCompletedAt time.Time
	lastSucceededAt time.Time
	lastError       error
}

func NewRunner(params RunnerParams) (*Runner, error) {
	if params.Service == nil {
		return nil, fmt.Errorf("time exit service is required")
	}
	if params.Interval == 0 {
		params.Interval = defaultInterval
	}
	if params.Timeout == 0 {
		params.Timeout = defaultTimeout
	}
	if params.Interval < 10*time.Second || params.Interval > 10*time.Minute {
		return nil, fmt.Errorf("time exit interval must be between 10s and 10m")
	}
	if params.Timeout <= 0 || params.Timeout >= params.Interval {
		return nil, fmt.Errorf("time exit timeout must be positive and less than its interval")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	if params.Logger == nil {
		params.Logger = slog.Default()
	}
	return &Runner{
		service: params.Service, interval: params.Interval, timeout: params.Timeout,
		now: params.Now, logger: params.Logger,
	}, nil
}

func (runner *Runner) Run(ctx context.Context) error {
	return runner.runLoop(ctx, nil)
}

// RunReady performs the startup sweep before publishing readiness, so an
// overdue lot is handled immediately after the heartbeat gate is active.
func (runner *Runner) RunReady(ctx context.Context, ready chan<- struct{}) error {
	if ready == nil {
		return fmt.Errorf("time exit readiness channel is required")
	}
	return runner.runLoop(ctx, ready)
}

func (runner *Runner) runLoop(ctx context.Context, ready chan<- struct{}) error {
	if !runner.beginLoop() {
		return fmt.Errorf("time exit loop is already running")
	}
	defer runner.endLoop()
	if _, err := runner.runOnce(ctx); err != nil {
		return fmt.Errorf("initial time exit sweep: %w", err)
	}
	if ready != nil {
		close(ready)
	}
	ticker := time.NewTicker(runner.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_, _ = runner.runOnce(ctx)
		}
	}
}

func (runner *Runner) runOnce(ctx context.Context) (RunResult, error) {
	startedAt := runner.now().UTC()
	runner.markStarted(startedAt)
	runContext, cancel := context.WithTimeout(ctx, runner.timeout)
	result, err := runner.service.Run(runContext, startedAt.Truncate(runner.interval))
	cancel()
	completedAt := runner.now().UTC()
	runner.markCompleted(completedAt, err)
	attributes := []any{
		"scheduled_at", result.ScheduledAt, "scanned", result.Scanned, "due", result.Due,
		"submitted", result.Submitted, "skipped", result.Skipped, "failed", result.Failed,
	}
	if err != nil {
		runner.logger.Error("48-hour position exit sweep completed with errors", append(attributes, "error", err)...)
	} else if result.Due != 0 || result.Failed != 0 {
		runner.logger.Info("48-hour position exit sweep completed", attributes...)
	}
	return result, err
}

// Check exposes scheduler health without treating ordinary no-liquidity or
// non-tradable skips as an infrastructure outage.
func (runner *Runner) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := runner.now().UTC()
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if !runner.loopStarted || !runner.loopRunning {
		return fmt.Errorf("time exit loop is not running")
	}
	if runner.inFlight && now.After(runner.lastStartedAt.Add(runner.timeout)) {
		return fmt.Errorf("time exit sweep exceeded timeout %s", runner.timeout)
	}
	if runner.lastError != nil {
		return fmt.Errorf("most recent time exit sweep failed: %w", runner.lastError)
	}
	if runner.lastSucceededAt.IsZero() || now.After(runner.lastSucceededAt.Add(3*runner.interval)) {
		return fmt.Errorf("time exit sweep is stale")
	}
	return nil
}

func (runner *Runner) beginLoop() bool {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if runner.loopRunning {
		return false
	}
	runner.loopStarted = true
	runner.loopRunning = true
	return true
}

func (runner *Runner) endLoop() {
	runner.mu.Lock()
	runner.loopRunning = false
	runner.inFlight = false
	runner.mu.Unlock()
}

func (runner *Runner) markStarted(at time.Time) {
	runner.mu.Lock()
	runner.inFlight = true
	runner.lastStartedAt = at
	runner.mu.Unlock()
}

func (runner *Runner) markCompleted(at time.Time, err error) {
	runner.mu.Lock()
	runner.inFlight = false
	runner.lastCompletedAt = at
	runner.lastError = err
	if err == nil {
		runner.lastSucceededAt = at
	}
	runner.mu.Unlock()
}
