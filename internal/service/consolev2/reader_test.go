package consolev2

import (
	"context"
	"testing"
	"time"
)

func TestRuntimeUsesRunnerSnapshotAndSafeConfig(t *testing.T) {
	now := time.Date(2026, 10, 16, 8, 0, 0, 0, time.UTC)
	reader := New(nil, RuntimeConfig{
		StartedAt: now.Add(-time.Minute), Now: func() time.Time { return now },
		RunnerEnabled: true, RunnerInterval: 10 * time.Minute,
		Bindings:               []Binding{{PredictionModelID: "source", ModelID: "logical", StrategyID: "multfactor_v2", ExecutionAccountID: "wallet-1", SourceMode: "DIRECT"}},
		OrderSubmissionEnabled: true,
		RunnerSnapshot: func() RunnerSnapshot {
			return RunnerSnapshot{LoopStarted: true, LoopRunning: true, InFlight: true, NextDueAt: now.Add(10 * time.Minute), LastError: ""}
		},
	})
	runtime, err := reader.Runtime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !runtime.Runner.Enabled || !runtime.Runner.Running || runtime.Runner.State != "running" ||
		runtime.Runner.IntervalSeconds != 600 || !runtime.Runner.InFlight ||
		len(runtime.Bindings) != 1 || runtime.Bindings[0].SourceMode != "DIRECT" ||
		!runtime.Submission.OrderSubmissionEnabled {
		t.Fatalf("runtime projection = %#v", runtime)
	}
}

func TestRuntimeMarksUnconfiguredEnabledRunnerUnknown(t *testing.T) {
	reader := New(nil, RuntimeConfig{RunnerEnabled: true})
	runtime, err := reader.Runtime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Runner.State != "unknown" || runtime.Runner.Running {
		t.Fatalf("runner state = %#v", runtime.Runner)
	}
}

func TestRuntimeRedactsRunnerErrorText(t *testing.T) {
	now := time.Date(2026, 10, 16, 8, 0, 0, 0, time.UTC)
	reader := New(nil, RuntimeConfig{
		Now:           func() time.Time { return now },
		RunnerEnabled: true,
		RunnerSnapshot: func() RunnerSnapshot {
			return RunnerSnapshot{
				LoopStarted: true,
				LoopRunning: true,
				LastError:   "load prediction snapshot: https://internal.example/token=secret",
			}
		},
	})
	runtime, err := reader.Runtime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !runtime.Runner.HasLastError || runtime.Runner.LastErrorCode != "CYCLE_FAILED" {
		t.Fatalf("runner error projection = %#v", runtime.Runner)
	}
}
