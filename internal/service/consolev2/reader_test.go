package consolev2

import (
	"context"
	"database/sql"
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

func TestRuntimeKeepsAccountProjectionExplicitlyUnavailableWithoutDatabase(t *testing.T) {
	reader := New(nil, RuntimeConfig{
		Bindings: []Binding{{ModelID: "echo", StrategyID: "multfactor_v2", ExecutionAccountID: "wallet-1"}},
	})
	runtime, err := reader.Runtime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if runtime.AccountDataState != "unavailable" || len(runtime.Accounts) != 0 || len(runtime.Bindings) != 1 {
		t.Fatalf("runtime account projection = %#v", runtime)
	}
}

func TestFinishPageUsesExactTotalForNavigation(t *testing.T) {
	now := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	page := finishPage([]string{"c"}, ListFilter{Limit: 2, Offset: 2}, 3, now, nil)
	if len(page.Items) != 1 || page.Total != 3 || page.HasMore {
		t.Fatalf("last page = %#v", page)
	}
	empty := finishPage([]string{}, ListFilter{Limit: 20, Offset: 60}, 41, now, nil)
	if len(empty.Items) != 0 || empty.Total != 41 || empty.HasMore {
		t.Fatalf("out-of-range page = %#v", empty)
	}
	zero := finishPage([]string(nil), ListFilter{Limit: 20, Offset: 0}, 0, now, nil)
	if len(zero.Items) != 0 || zero.Total != 0 || zero.HasMore {
		t.Fatalf("empty page = %#v", zero)
	}
}

func TestRuntimeEntryGateStateDoesNotRewriteHistoricalCycle(t *testing.T) {
	historicalCycle := sql.NullTime{Valid: true, Time: time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)}
	entryEnabled := sql.NullBool{Valid: true, Bool: true}
	blocked := sql.NullString{Valid: true, String: "ENTRY_SUBMISSION_DISABLED"}

	if got := runtimeEntryGateState(historicalCycle, sql.NullBool{}, sql.NullString{}); got != "unknown" {
		t.Fatalf("historical cycle without entry policy = %q, want unknown", got)
	}
	if got := runtimeEntryGateState(historicalCycle, entryEnabled, blocked); got != "sell_only" {
		t.Fatalf("historical blocked cycle = %q, want sell_only", got)
	}
	if got := runtimeEntryGateState(historicalCycle, entryEnabled, sql.NullString{}); got != "enabled" {
		t.Fatalf("historical enabled cycle = %q, want enabled", got)
	}
	if got := runtimeEntryGateState(sql.NullTime{}, sql.NullBool{}, sql.NullString{}); got != "unknown" {
		t.Fatalf("current config without cycle = %q, want unknown", got)
	}
}
