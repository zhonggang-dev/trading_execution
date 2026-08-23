package main

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/adapter/predictioninfra"
	"github.com/UniPat-AI/trading_execution/internal/adapter/strategyhttp"
	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/decisioncycle"
	"github.com/UniPat-AI/trading_execution/internal/service/decisionrunner"
)

type buildDecisionRunnerParams struct {
	cfg            config.Config
	database       *sql.DB
	positionSource port.StrategyPositionSource
	orderBooks     port.OrderBookSource
	executor       port.OrderExecutor
	accountIDs     []string
	logger         *slog.Logger
}

// buildDecisionRunner composes the pull-based production workflow without
// starting it. All configuration and durable dependencies are validated before
// the CLOB heartbeat is activated.
func buildDecisionRunner(params buildDecisionRunnerParams) (*decisionrunner.Runner, error) {
	cycleConfig := params.cfg.DecisionCycle
	if !cycleConfig.Enabled {
		return nil, nil
	}
	if params.database == nil || params.positionSource == nil || params.orderBooks == nil || params.executor == nil {
		return nil, fmt.Errorf("decision cycle requires postgres, position, orderbook, and execution dependencies")
	}
	if err := validateDecisionAccounts(cycleConfig, params.accountIDs); err != nil {
		return nil, err
	}
	predictionClient, err := predictioninfra.New(predictioninfra.Params{
		BaseURL: cycleConfig.PredictionInfraBaseURL, BearerToken: cycleConfig.PredictionInfraToken,
		HTTPClient: noRedirectHTTPClient(cycleConfig.Timeout),
	})
	if err != nil {
		return nil, fmt.Errorf("build prediction_infra decision client: %w", err)
	}
	strategyClient, err := strategyhttp.New(strategyhttp.Params{
		BaseURL: cycleConfig.StrategyBaseURL, BearerToken: cycleConfig.StrategyToken,
		HTTPClient: noRedirectHTTPClient(cycleConfig.Timeout),
	})
	if err != nil {
		return nil, fmt.Errorf("build strategy decision client: %w", err)
	}
	midPrices, err := polymarket.NewMidPriceHistorySource(polymarket.MidPriceHistoryParams{
		BaseURL:    params.cfg.Polymarket.CLOBURL,
		HTTPClient: noRedirectHTTPClient(cycleConfig.Timeout),
	})
	if err != nil {
		return nil, fmt.Errorf("build decision mid-price source: %w", err)
	}
	recorder, err := postgresadapter.NewDecisionRecorder(params.database, nil)
	if err != nil {
		return nil, fmt.Errorf("build strategy decision recorder: %w", err)
	}
	cycle, err := decisioncycle.New(decisioncycle.Params{
		PredictionSource:             predictionClient,
		PositionSource:               params.positionSource,
		OrderBookSource:              params.orderBooks,
		MidPriceSource:               midPrices,
		Strategy:                     strategyClient,
		Recorder:                     recorder,
		Executor:                     params.executor,
		SubmitEnabled:                cycleConfig.OrderSubmissionEnabled,
		SubmissionDisabledAccounts:   cycleConfig.SubmissionDisabledAccounts,
		EntrySubmissionDisabled:      cycleConfig.EntrySubmissionDisabled,
		RequireCompleteModelCoverage: cycleConfig.RequireCompleteModelCoverage,
		Bindings:                     cycleConfig.Bindings,
		Venue:                        params.cfg.Execution.Venue,
		PredictionLookback:           cycleConfig.PredictionLookback,
		MidPriceLookback:             cycleConfig.MidPriceLookback,
		DeliveryStaleAge:             cycleConfig.Timeout,
	})
	if err != nil {
		return nil, fmt.Errorf("build decision cycle: %w", err)
	}
	runner, err := decisionrunner.New(decisionrunner.Params{
		Cycle: cycle, Interval: cycleConfig.Interval, StartupDelay: cycleConfig.StartupDelay,
		MaxStartLateness: cycleConfig.MaxStartLateness,
		Timeout:          cycleConfig.Timeout, Logger: params.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("build decision cycle runner: %w", err)
	}
	return runner, nil
}

func validateDecisionAccounts(cycleConfig config.DecisionCycle, accountIDs []string) error {
	available := make(map[string]struct{}, len(accountIDs))
	for _, accountID := range accountIDs {
		available[accountID] = struct{}{}
	}
	for _, binding := range cycleConfig.Bindings {
		binding = binding.Normalize()
		if _, exists := available[binding.ExecutionAccountID]; !exists {
			return fmt.Errorf("decision-cycle binding references execution account %q which is absent from the wallet file", binding.ExecutionAccountID)
		}
	}
	return nil
}
