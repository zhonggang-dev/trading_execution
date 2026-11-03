package main

import (
	"context"
	"strings"
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
		PredictionModelID:      "source-model", PredictionSourceMode: domain.PredictionSourceModeSandbox,
		Accounts: []config.DecisionAccount{
			{ExecutionAccountID: "wallet-1", MarketSource: domain.MarketSourcePolymarket,
				StrategyID: domain.StrategyIDMultfactorV2, Mode: config.DecisionAccountPaused},
			{ExecutionAccountID: "kalshi:main", MarketSource: domain.MarketSourceKalshi,
				StrategyID: "brier_mid_edge_once", Mode: config.DecisionAccountActive},
		},
	}}
	live := &liveRuntime{decisionRunner: runner}
	reader := consolev2.New(nil, consoleV2RuntimeConfig(cfg, live, now))
	value, err := reader.Runtime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !value.Runner.Enabled || value.Runner.State != "not_started" || value.Runner.IntervalSeconds != 600 ||
		len(value.Bindings) != 2 || value.Bindings[0].PredictionModelID != "source-model" ||
		value.Bindings[0].ModelID != "source-model" || value.Bindings[0].SourceMode != "SANDBOX" ||
		value.Bindings[0].MarketSource != "POLYMARKET" || value.Bindings[0].Mode != "PAUSED" ||
		value.Bindings[1].ExecutionAccountID != "kalshi:main" || value.Bindings[1].MarketSource != "KALSHI" ||
		value.Bindings[1].Mode != "ACTIVE" || !value.Submission.OrderSubmissionEnabled {
		t.Fatalf("runtime projection = %#v", value)
	}
	// The retired switches keep their neutral values in the console contract.
	submission := value.Submission
	if len(submission.SubmissionDisabledAccounts) != 0 || !submission.KalshiStrategyInputEnabled ||
		strings.Join(submission.EntryDisabledAccounts, ",") != "wallet-1" ||
		strings.Join(submission.StrategyDisabledAccounts, ",") != "wallet-1" {
		t.Fatalf("submission projection = %#v", submission)
	}
}

func TestConsoleV2AccountsUseValidatedVenueMembership(t *testing.T) {
	cfg := config.Config{Kalshi: config.Kalshi{LiveBindings: []config.KalshiLiveBinding{{ExecutionAccountID: "main", APIKeyID: "never-export-key"}, {ExecutionAccountID: "unregistered-route"}}}}
	live := &liveRuntime{polymarketAccounts: []string{"main"}, kalshiAccounts: []string{"kalshi:main"}, activeAccounts: []string{"main", "kalshi:main"}}
	projected := consoleV2RuntimeConfig(cfg, live, time.Now())
	if len(projected.ManagedAccounts) != 2 || projected.ManagedAccounts[0].MarketSource != "POLYMARKET" || projected.ManagedAccounts[1].MarketSource != "KALSHI" || projected.ManagedAccounts[1].ExecutionAccountID != "kalshi:main" {
		t.Fatalf("venue membership %#v", projected.ManagedAccounts)
	}
	if empty := consoleV2RuntimeConfig(cfg, nil, time.Now()); len(empty.ManagedAccounts) != 0 {
		t.Fatal("unvalidated config exposed as account directory")
	}
}
