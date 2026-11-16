package decisionrunner

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/decisioncycle"
)

type boundaryRecordingCycle struct {
	mu        sync.Mutex
	clock     *fakeClock
	cancel    context.CancelFunc
	boundary  time.Time
	ranAt     time.Time
	runsCount int
}

func (cycle *boundaryRecordingCycle) Run(_ context.Context, boundary time.Time) (decisioncycle.RunResult, error) {
	cycle.mu.Lock()
	defer cycle.mu.Unlock()
	cycle.boundary, cycle.ranAt = boundary, cycle.clock.Now()
	cycle.runsCount++
	cycle.cancel()
	return decisioncycle.RunResult{DecisionAt: boundary}, nil
}

// TestDelayedLaneRunsBoundaryTAtItsOwnDelay covers AC2 at the scheduler: a
// runner configured with the delayed lane's 5m15s delay runs decision_at T
// (the same boundary the primary lane ran at T+15s) at T+5m15s.
func TestDelayedLaneRunsBoundaryTAtItsOwnDelay(t *testing.T) {
	boundary := time.Date(2026, 11, 16, 8, 20, 0, 0, time.UTC)
	delay := 5*time.Minute + 15*time.Second
	clock := &fakeClock{now: boundary.Add(20 * time.Second)} // primary already ran
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cycle := &boundaryRecordingCycle{clock: clock, cancel: cancel}
	var waitedUntil time.Time
	runner, err := New(Params{
		Cycle: cycle, StartupDelay: delay, Timeout: 4*time.Minute + 30*time.Second,
		Now: clock.Now,
		Wait: func(_ context.Context, until time.Time) error {
			waitedUntil = until
			clock.Set(until.Add(200 * time.Millisecond))
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Run(ctx); err == nil {
		t.Fatal("Run() returned nil after cancellation")
	}
	cycle.mu.Lock()
	defer cycle.mu.Unlock()
	if cycle.runsCount != 1 || !cycle.boundary.Equal(boundary) || !waitedUntil.Equal(boundary.Add(delay)) ||
		cycle.ranAt.Sub(boundary) < delay {
		t.Fatalf("runs=%d decision_at=%s waited_until=%s ran_at=%s", cycle.runsCount, cycle.boundary, waitedUntil, cycle.ranAt)
	}
}
