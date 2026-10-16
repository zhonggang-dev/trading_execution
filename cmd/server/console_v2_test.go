package main

import (
	"context"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
	"github.com/UniPat-AI/trading_execution/internal/service/decisioncycle"
	"github.com/UniPat-AI/trading_execution/internal/service/decisionrunner"
)

type consoleV2TestCycle struct{}

func (consoleV2TestCycle) Run(_ context.Context, boundary time.Time) (decisioncycle.RunResult, error) {
	return decisioncycle.RunResult{DecisionAt: boundary}, nil
}

func TestConsoleV2RuntimeConfigProjectsBindingsAndRunnerSnapshot(t *testing.T) {
	runner, err := decisionrunner.New(decisionrunner.Params{
		Cycle: consoleV2TestCycle{}, Interval: decisionrunner.RequiredInterval,
		StartupDelay: time.Second, MaxStartLateness: time.Second,
		Timeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 16, 8, 0, 0, 0, time.UTC)
	cfg := config.Config{DecisionCycle: config.DecisionCycle{
		Enabled: true, Interval: decisionrunner.RequiredInterval,
		OrderSubmissionEnabled: true,
		Bindings: []domain.StrategyExecutionBinding{{
			PredictionModelID: "source-model", ModelID: "logical-model",
			StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet-1",
		}},
		PredictionSourceModes: map[string]domain.PredictionSourceMode{
			"source-model": domain.PredictionSourceModeSandbox,
		},
	}}
	live := &liveRuntime{decisionRunner: runner}
	reader := consolev2.New(nil, consoleV2RuntimeConfig(cfg, live, now))
	value, err := reader.Runtime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !value.Runner.Enabled || value.Runner.State != "not_started" || value.Runner.IntervalSeconds != 600 ||
		len(value.Bindings) != 1 || value.Bindings[0].PredictionModelID != "source-model" ||
		value.Bindings[0].ModelID != "logical-model" || value.Bindings[0].SourceMode != "SANDBOX" ||
		!value.Submission.OrderSubmissionEnabled {
		t.Fatalf("runtime projection = %#v", value)
	}
}
