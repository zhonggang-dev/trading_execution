package decisionrunner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/decisioncycle"
)

type partialCycle struct {
	result decisioncycle.RunResult
	err    error
}

func (cycle partialCycle) Run(context.Context, time.Time) (decisioncycle.RunResult, error) {
	return cycle.result, cycle.err
}

func runLoggedBoundary(t *testing.T, cycle partialCycle) (map[string]any, *Runner) {
	t.Helper()
	var output bytes.Buffer
	now := time.Date(2026, 11, 5, 3, 10, 15, 0, time.UTC)
	runner, err := New(Params{
		Cycle: cycle, Interval: RequiredInterval, Timeout: time.Minute,
		Now: func() time.Time { return now }, Logger: slog.New(slog.NewJSONHandler(&output, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.beginLoop(); err != nil {
		t.Fatal(err)
	}
	runner.setSchedule(now.Truncate(RequiredInterval), now.Add(time.Minute))
	_, _ = runner.runBoundary(context.Background(), now.Truncate(RequiredInterval))
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatalf("log output %q: %v", output.String(), err)
	}
	return record, runner
}

// TestPartiallyFailedCycleIsLoggedAsErrorWithAccountCounts keeps a partly
// failed cycle visible: it logs at ERROR with the per-account outcome, and
// readiness reports the failure, while healthy accounts are counted as ok.
func TestPartiallyFailedCycleIsLoggedAsErrorWithAccountCounts(t *testing.T) {
	accountErr := errors.New("account input unavailable (positions)")
	record, runner := runLoggedBoundary(t, partialCycle{
		result: decisioncycle.RunResult{
			PredictionSnapshotID:        "predsnap-unavailable-20261105T031000Z",
			PredictionSourceUnavailable: true,
			DegradedVenues:              []domain.MarketSource{domain.MarketSourceKalshi},
			Recovery:                    decisioncycle.RecoveryResult{Rejected: 1, Unknown: 2},
			Runs: []decisioncycle.BindingRunResult{
				{Context: domain.StrategyExecutionContext{ExecutionAccountID: "main"}},
				{Context: domain.StrategyExecutionContext{ExecutionAccountID: "wallet-6"}, InputUnavailable: true,
					FailureStage: decisioncycle.FailureStagePositions, Error: accountErr},
				{Context: domain.StrategyExecutionContext{ExecutionAccountID: "wallet-7"}, NotRun: true,
					FailureStage: decisioncycle.FailureStageCycleDeadline, Error: context.DeadlineExceeded},
				{Context: domain.StrategyExecutionContext{ExecutionAccountID: "kalshi:main"}, VenueBooksUnavailable: true,
					FailureStage: decisioncycle.FailureStageOrderBooks, Error: errors.New("KALSHI orderbooks unavailable")},
				{Context: domain.StrategyExecutionContext{ExecutionAccountID: "wallet-1"}, RouteUnavailable: true},
			},
		},
		err: accountErr,
	})
	if record["level"] != "ERROR" {
		t.Fatalf("level = %v, want ERROR", record["level"])
	}
	for key, want := range map[string]any{
		"prediction_source_unavailable": true, "recovered_rejected": float64(1), "recovered_unknown": float64(2),
		"accounts_ok": float64(1), "accounts_failed": float64(3), "accounts_skipped": float64(1),
	} {
		if record[key] != want {
			t.Fatalf("%s = %#v, want %#v", key, record[key], want)
		}
	}
	if venues, _ := record["degraded_venues"].([]any); len(venues) != 1 || venues[0] != "KALSHI" {
		t.Fatalf("degraded_venues = %#v", record["degraded_venues"])
	}
	runs, _ := record["binding_runs"].([]any)
	if len(runs) != 5 {
		t.Fatalf("binding_runs = %#v", record["binding_runs"])
	}
	wallet6 := runs[1].(map[string]any)
	if wallet6["input_unavailable"] != true || wallet6["failure_stage"] != "positions" || wallet6["failed"] != true {
		t.Fatalf("wallet-6 summary = %#v", wallet6)
	}
	if wallet7 := runs[2].(map[string]any); wallet7["not_run"] != true {
		t.Fatalf("wallet-7 summary = %#v", wallet7)
	}
	if kalshi := runs[3].(map[string]any); kalshi["venue_books_unavailable"] != true {
		t.Fatalf("Kalshi summary = %#v", kalshi)
	}
	if err := runner.Check(context.Background()); err == nil {
		t.Fatal("Check() error = nil, want the partly failed cycle reported")
	}
}

func TestHealthyCycleIsLoggedAsInfo(t *testing.T) {
	record, runner := runLoggedBoundary(t, partialCycle{result: decisioncycle.RunResult{
		PredictionSnapshotID: "predsnap-1",
		Runs:                 []decisioncycle.BindingRunResult{{Context: domain.StrategyExecutionContext{ExecutionAccountID: "main"}}},
	}})
	if record["level"] != "INFO" || record["accounts_ok"] != float64(1) || record["accounts_failed"] != float64(0) {
		t.Fatalf("record = %#v", record)
	}
	if err := runner.Check(context.Background()); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
}
