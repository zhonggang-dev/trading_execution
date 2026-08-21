package decisionrunner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/decisioncycle"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *fakeClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *fakeClock) Set(value time.Time) {
	clock.mu.Lock()
	clock.now = value
	clock.mu.Unlock()
}

type advancingCycle struct {
	mu         sync.Mutex
	clock      *fakeClock
	cancel     context.CancelFunc
	boundaries []time.Time
	active     int
	maxActive  int
}

func (cycle *advancingCycle) Run(_ context.Context, boundary time.Time) (decisioncycle.RunResult, error) {
	cycle.mu.Lock()
	cycle.active++
	if cycle.active > cycle.maxActive {
		cycle.maxActive = cycle.active
	}
	cycle.boundaries = append(cycle.boundaries, boundary)
	call := len(cycle.boundaries)
	cycle.mu.Unlock()

	var err error
	if call == 1 {
		cycle.clock.Set(boundary.Add(25*time.Minute + 6*time.Second))
		err = errors.New("temporary strategy failure")
	} else {
		cycle.clock.Set(boundary.Add(6 * time.Second))
		cycle.cancel()
	}

	cycle.mu.Lock()
	cycle.active--
	cycle.mu.Unlock()
	return decisioncycle.RunResult{DecisionAt: boundary}, err
}

type blockingCycle struct {
	started chan struct{}
	release chan struct{}
}

type recoveringCycle struct {
	recoveryErr error
	recovered   bool
}

type countingCycle struct {
	mu    sync.Mutex
	calls int
}

func (cycle *countingCycle) Run(_ context.Context, boundary time.Time) (decisioncycle.RunResult, error) {
	cycle.mu.Lock()
	cycle.calls++
	cycle.mu.Unlock()
	return decisioncycle.RunResult{DecisionAt: boundary}, nil
}

func (cycle *recoveringCycle) RecoverStartup(context.Context) error {
	cycle.recovered = true
	return cycle.recoveryErr
}

func (cycle *recoveringCycle) Run(_ context.Context, boundary time.Time) (decisioncycle.RunResult, error) {
	return decisioncycle.RunResult{DecisionAt: boundary}, nil
}

func (cycle *blockingCycle) Run(ctx context.Context, boundary time.Time) (decisioncycle.RunResult, error) {
	close(cycle.started)
	select {
	case <-ctx.Done():
		return decisioncycle.RunResult{}, ctx.Err()
	case <-cycle.release:
		return decisioncycle.RunResult{DecisionAt: boundary}, nil
	}
}

func TestNextScheduleUsesUTCBoundaryAndSkipsExpiredWindow(t *testing.T) {
	boundary := time.Date(2026, 8, 19, 4, 20, 0, 0, time.UTC)
	gotBoundary, dueAt := nextSchedule(boundary.Add(3*time.Second), time.Time{}, RequiredInterval, 15*time.Second)
	if !gotBoundary.Equal(boundary) || !dueAt.Equal(boundary.Add(15*time.Second)) {
		t.Fatalf("current schedule = %s/%s", gotBoundary, dueAt)
	}
	gotBoundary, dueAt = nextSchedule(boundary.Add(16*time.Second), time.Time{}, RequiredInterval, 15*time.Second)
	if !gotBoundary.Equal(boundary.Add(RequiredInterval)) || !dueAt.Equal(boundary.Add(RequiredInterval+15*time.Second)) {
		t.Fatalf("advanced schedule = %s/%s", gotBoundary, dueAt)
	}
	gotBoundary, _ = nextSchedule(boundary, boundary, RequiredInterval, 0)
	if !gotBoundary.Equal(boundary.Add(RequiredInterval)) {
		t.Fatalf("already scheduled boundary replayed as %s", gotBoundary)
	}
}

func TestBindingRunSummariesExposePredictionRoutingAndCounts(t *testing.T) {
	summaries := bindingRunSummaries(decisioncycle.RunResult{Runs: []decisioncycle.BindingRunResult{{
		Context: domain.StrategyExecutionContext{
			ModelID: "gemini_masked", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet-3",
		},
		PredictionModelID: "gemini-3.6-flash",
		PredictionCount:   2,
		PositionCount:     1,
		Request: domain.StrategyDecisionRequest{
			Predictions: []domain.Prediction{{}, {}},
			Positions:   []domain.StrategyPositionLot{{}},
		},
		Intents: []decisioncycle.IntentResult{{}},
		Error:   errors.New("strategy unavailable"),
	}}})
	if len(summaries) != 1 || summaries[0].PredictionModelID != "gemini-3.6-flash" ||
		summaries[0].ModelID != "gemini_masked" || summaries[0].ExecutionAccountID != "wallet-3" ||
		summaries[0].Predictions != 2 || summaries[0].Positions != 1 || summaries[0].Intents != 1 || !summaries[0].Failed {
		t.Fatalf("binding summaries = %#v", summaries)
	}
}

func TestRunnerNeverExecutesAStaleBoundaryAfterLateWake(t *testing.T) {
	for _, lateness := range []time.Duration{time.Minute, 9 * time.Minute} {
		t.Run(lateness.String(), func(t *testing.T) {
			start := time.Date(2026, 8, 19, 4, 20, 1, 0, time.UTC)
			clock := &fakeClock{now: start}
			cycle := &countingCycle{}
			ctx, cancel := context.WithCancel(context.Background())
			waits := 0
			runner, err := New(Params{
				Cycle: cycle, Interval: RequiredInterval, StartupDelay: 15 * time.Second,
				MaxStartLateness: 30 * time.Second, Timeout: time.Minute, Now: clock.Now,
				Wait: func(ctx context.Context, dueAt time.Time) error {
					waits++
					if waits == 1 {
						clock.Set(dueAt.Add(lateness))
						return nil
					}
					cancel()
					return ctx.Err()
				},
				Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := runner.Run(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("Run() error = %v", err)
			}
			cycle.mu.Lock()
			calls := cycle.calls
			cycle.mu.Unlock()
			if calls != 0 || runner.Snapshot().SkippedBoundaries != 1 {
				t.Fatalf("stale cycle calls=%d status=%#v", calls, runner.Snapshot())
			}
		})
	}
}

func TestRunnerContinuesAfterErrorAndDoesNotBackfillMissedBoundaries(t *testing.T) {
	start := time.Date(2026, 8, 19, 4, 20, 1, 0, time.UTC)
	clock := &fakeClock{now: start}
	ctx, cancel := context.WithCancel(context.Background())
	cycle := &advancingCycle{clock: clock, cancel: cancel}
	runner, err := New(Params{
		Cycle: cycle, Interval: RequiredInterval, StartupDelay: 5 * time.Second,
		Timeout: 9 * time.Minute, Now: clock.Now,
		Wait: func(ctx context.Context, until time.Time) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			clock.Set(until)
			return nil
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v", err)
	}
	cycle.mu.Lock()
	defer cycle.mu.Unlock()
	wantFirst := start.Truncate(RequiredInterval)
	wantSecond := wantFirst.Add(3 * RequiredInterval)
	if len(cycle.boundaries) != 2 || !cycle.boundaries[0].Equal(wantFirst) || !cycle.boundaries[1].Equal(wantSecond) {
		t.Fatalf("boundaries = %#v, want %s then %s", cycle.boundaries, wantFirst, wantSecond)
	}
	if cycle.maxActive != 1 {
		t.Fatalf("maximum concurrent cycles = %d", cycle.maxActive)
	}
	status := runner.Snapshot()
	if !status.LastSucceededAt.Equal(wantSecond.Add(6*time.Second)) || status.LastError != "" {
		t.Fatalf("runner status = %#v", status)
	}
}

func TestRunBoundaryRejectsOverlap(t *testing.T) {
	cycle := &blockingCycle{started: make(chan struct{}), release: make(chan struct{})}
	runner, err := New(Params{
		Cycle: cycle, Interval: RequiredInterval, Timeout: time.Minute,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	boundary := time.Now().UTC().Truncate(RequiredInterval)
	firstDone := make(chan error, 1)
	go func() {
		_, runErr := runner.runBoundary(context.Background(), boundary)
		firstDone <- runErr
	}()
	<-cycle.started
	if _, err := runner.runBoundary(context.Background(), boundary); !errors.Is(err, ErrCycleInProgress) {
		t.Fatalf("overlapping run error = %v", err)
	}
	close(cycle.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first run error = %v", err)
	}
}

func TestReadinessReportsLatestCycleFailure(t *testing.T) {
	now := time.Date(2026, 8, 19, 4, 20, 5, 0, time.UTC)
	clock := &fakeClock{now: now}
	runner, err := New(Params{
		Cycle:    &blockingCycle{started: make(chan struct{}), release: make(chan struct{})},
		Interval: RequiredInterval, Timeout: time.Minute, Now: clock.Now,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.beginLoop(); err != nil {
		t.Fatal(err)
	}
	runner.setSchedule(now.Truncate(RequiredInterval), now.Add(time.Minute))
	runner.completeCycle(now, errors.New("strategy unavailable"))
	if err := runner.Check(context.Background()); err == nil {
		t.Fatal("Check() error = nil, want latest-cycle failure")
	}
}

func TestReadinessRequiresFirstSuccessfulBoundary(t *testing.T) {
	now := time.Date(2026, 8, 19, 4, 20, 5, 0, time.UTC)
	clock := &fakeClock{now: now}
	runner, err := New(Params{
		Cycle: &countingCycle{}, Interval: RequiredInterval, Timeout: time.Minute, Now: clock.Now,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.beginLoop(); err != nil {
		t.Fatal(err)
	}
	runner.setSchedule(now.Truncate(RequiredInterval), now.Add(time.Minute))
	if err := runner.Check(context.Background()); err == nil {
		t.Fatal("Check() error = nil before first successful boundary")
	}
	runner.completeCycle(now, nil)
	if err := runner.Check(context.Background()); err != nil {
		t.Fatalf("Check() error after success = %v", err)
	}
}

func TestRunReadyRecoversDurableIntentsBeforePublishingSchedule(t *testing.T) {
	now := time.Date(2026, 8, 19, 4, 20, 5, 0, time.UTC)
	clock := &fakeClock{now: now}
	cycle := &recoveringCycle{recoveryErr: errors.New("postgres temporarily unavailable")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner, err := New(Params{
		Cycle: cycle, Interval: RequiredInterval, Timeout: time.Minute, Now: clock.Now,
		Wait:   func(ctx context.Context, _ time.Time) error { <-ctx.Done(); return ctx.Err() },
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- runner.RunReady(ctx, ready) }()
	select {
	case <-ready:
		t.Fatal("RunReady published schedule after startup recovery failed")
	case runErr := <-done:
		if runErr == nil || !strings.Contains(runErr.Error(), "startup strategy intent recovery") {
			t.Fatalf("RunReady() error = %v", runErr)
		}
	case <-time.After(time.Second):
		t.Fatal("RunReady did not fail after startup recovery error")
	}
	if !cycle.recovered {
		t.Fatal("RunReady did not attempt pending intent recovery")
	}
}
