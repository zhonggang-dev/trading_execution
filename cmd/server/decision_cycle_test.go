package main

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type decisionRunnerTestPositionSource struct{}

func (decisionRunnerTestPositionSource) ListOpenLots(context.Context, string) ([]domain.PositionLot, error) {
	return nil, nil
}

type decisionRunnerTestOrderBookSource struct{}

func (decisionRunnerTestOrderBookSource) Capture(context.Context, time.Time, []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	return nil, nil
}

type decisionRunnerTestExecutor struct{}

func (decisionRunnerTestExecutor) Submit(context.Context, domain.OrderIntent) (port.OrderSubmitResult, error) {
	return port.OrderSubmitResult{}, nil
}

func TestBuildDecisionRunnerWiresSnapshotRecorder(t *testing.T) {
	database, err := sql.Open("pgx", "postgres://unused")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	binding := domain.StrategyExecutionBinding{
		PredictionModelID: "producer", ModelID: "model", StrategyID: domain.StrategyIDMultfactorV1,
		ExecutionAccountID: "account",
	}
	runner, err := buildDecisionRunner(buildDecisionRunnerParams{
		cfg: config.Config{
			Execution: config.Execution{Venue: "polymarket"},
			DecisionCycle: config.DecisionCycle{
				Enabled: true, PredictionInfraBaseURL: "https://prediction.example",
				PredictionInfraToken: "prediction-token", StrategyBaseURL: "https://strategy.example",
				StrategyToken: "strategy-token", Interval: 10 * time.Minute, Timeout: time.Minute,
				PredictionLookback: 3 * time.Hour, Bindings: []domain.StrategyExecutionBinding{binding},
				PredictionSourceModes: map[string]domain.PredictionSourceMode{
					"producer": domain.PredictionSourceModeDirect,
				},
			},
		},
		database: database, positionSource: decisionRunnerTestPositionSource{},
		orderBooks: decisionRunnerTestOrderBookSource{}, executor: decisionRunnerTestExecutor{},
		accountIDs: []string{"account"},
	})
	if err != nil || runner == nil {
		t.Fatalf("buildDecisionRunner() = %#v, error %v", runner, err)
	}
}

func TestValidateDecisionAccountsRejectsBindingOutsideWalletFile(t *testing.T) {
	cycleConfig := config.DecisionCycle{Bindings: []domain.StrategyExecutionBinding{{
		ModelID: "model-a", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "account-missing",
	}}}
	if err := validateDecisionAccounts(cycleConfig, []string{"account-present"}); err == nil || !strings.Contains(err.Error(), "absent from the wallet file") {
		t.Fatalf("validateDecisionAccounts() error = %v", err)
	}
}

func TestValidateDecisionAccountsAcceptsFourConfiguredWallets(t *testing.T) {
	cycleConfig := config.DecisionCycle{Bindings: []domain.StrategyExecutionBinding{
		{ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "main"},
		{ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "wallet-1"},
		{ModelID: "gemini_masked", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "wallet-6"},
		{ModelID: "gemini_masked", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet-7"},
	}}
	if err := validateDecisionAccounts(cycleConfig, []string{"main", "wallet-1", "wallet-6", "wallet-7"}); err != nil {
		t.Fatalf("validateDecisionAccounts() error = %v", err)
	}
}

func TestValidateDecisionAccountsRejectsExtraRetiredWallet(t *testing.T) {
	cycleConfig := config.DecisionCycle{Bindings: []domain.StrategyExecutionBinding{
		{ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "main"},
		{ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "wallet-1"},
		{ModelID: "gemini_masked", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "wallet-6"},
		{ModelID: "gemini_masked", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet-7"},
	}}
	err := validateDecisionAccounts(
		cycleConfig,
		[]string{"main", "wallet-1", "wallet-2", "wallet-6", "wallet-7"},
	)
	if err == nil || !strings.Contains(err.Error(), `wallet file execution account "wallet-2" has no decision-cycle binding`) {
		t.Fatalf("validateDecisionAccounts() error = %v, want extra retired-wallet rejection", err)
	}
}
