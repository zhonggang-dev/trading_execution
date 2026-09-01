package timeexit

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

type runnerSweeper struct {
	calls chan time.Time
	err   error
}

func (sweeper runnerSweeper) Run(_ context.Context, scheduledAt time.Time) (RunResult, error) {
	sweeper.calls <- scheduledAt
	return RunResult{ScheduledAt: scheduledAt}, sweeper.err
}

func TestRunnerCompletesStartupSweepBeforeReadiness(t *testing.T) {
	now := time.Date(2026, 9, 1, 12, 0, 37, 0, time.UTC)
	sweeper := runnerSweeper{calls: make(chan time.Time, 1)}
	runner, err := NewRunner(RunnerParams{
		Service: sweeper, Interval: time.Minute, Timeout: 45 * time.Second,
		Now:    func() time.Time { return now },
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	errorsChannel := make(chan error, 1)
	go func() { errorsChannel <- runner.RunReady(ctx, ready) }()

	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("runner did not publish readiness after its startup sweep")
	}
	if scheduledAt := <-sweeper.calls; !scheduledAt.Equal(now.Truncate(time.Minute)) {
		t.Fatalf("scheduled_at = %s", scheduledAt)
	}
	if err := runner.Check(context.Background()); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	cancel()
	if err := <-errorsChannel; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunReady() error = %v", err)
	}
}

func TestRunnerFailsStartupWithoutPublishingReadiness(t *testing.T) {
	wantErr := errors.New("database unavailable")
	sweeper := runnerSweeper{calls: make(chan time.Time, 1), err: wantErr}
	runner, err := NewRunner(RunnerParams{
		Service: sweeper, Interval: time.Minute, Timeout: 45 * time.Second,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	if err := runner.RunReady(context.Background(), ready); !errors.Is(err, wantErr) {
		t.Fatalf("RunReady() error = %v", err)
	}
	select {
	case <-ready:
		t.Fatal("runner published readiness after a failed startup sweep")
	default:
	}
}
