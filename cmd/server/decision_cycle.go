package main

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/adapter/kalshi"
	"github.com/UniPat-AI/trading_execution/internal/adapter/marketbooks"
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
	cfg              config.Config
	database         *sql.DB
	positionSource   port.StrategyPositionSource
	orderBooks       port.OrderBookSource
	executor         port.OrderExecutor
	accountIDs       []string
	logger           *slog.Logger
	submissionPolicy decisioncycle.IntentSubmissionPolicy
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
	var kalshiClient *kalshi.Client
	var kalshiOrderBooks port.OrderBookSource
	if params.cfg.Kalshi.MarketDataEnabled {
		var buildErr error
		kalshiClient, buildErr = kalshi.NewClient(kalshi.ClientParams{
			BaseURL: params.cfg.Kalshi.APIURL, APIKeyID: params.cfg.Kalshi.APIKeyID,
			PrivateKeyPath: params.cfg.Kalshi.PrivateKeyPath,
			HTTPClient:     noRedirectHTTPClient(params.cfg.Kalshi.RequestTimeout),
		})
		if buildErr != nil {
			return nil, fmt.Errorf("build Kalshi authenticated client: %w", buildErr)
		}
		kalshiOrderBooks, buildErr = kalshi.NewOrderBookSource(kalshi.OrderBookParams{Client: kalshiClient})
		if buildErr != nil {
			return nil, fmt.Errorf("build Kalshi orderbook source: %w", buildErr)
		}
	}
	orderBooks, err := marketbooks.New(marketbooks.Params{
		Polymarket: params.orderBooks,
		Kalshi:     kalshiOrderBooks,
	})
	if err != nil {
		return nil, fmt.Errorf("build venue orderbook router: %w", err)
	}
	polymarketMidPrices, err := polymarket.NewMidPriceHistorySource(polymarket.MidPriceHistoryParams{
		BaseURL:    params.cfg.Polymarket.CLOBURL,
		HTTPClient: noRedirectHTTPClient(cycleConfig.Timeout),
	})
	if err != nil {
		return nil, fmt.Errorf("build decision mid-price source: %w", err)
	}
	var kalshiMidPrices port.MidPriceHistorySource
	if kalshiClient != nil {
		kalshiMidPrices, err = kalshi.NewMidPriceHistorySource(kalshi.MidPriceHistoryParams{Client: kalshiClient})
		if err != nil {
			return nil, fmt.Errorf("build Kalshi mid-price history source: %w", err)
		}
	}
	midPrices, err := marketbooks.NewHistorySource(marketbooks.HistoryParams{
		Polymarket: polymarketMidPrices,
		Kalshi:     kalshiMidPrices,
	})
	if err != nil {
		return nil, fmt.Errorf("build venue mid-price history router: %w", err)
	}
	recorder, err := postgresadapter.NewDecisionRecorder(params.database, nil)
	if err != nil {
		return nil, fmt.Errorf("build strategy decision recorder: %w", err)
	}
	cycle, err := decisioncycle.New(decisioncycle.Params{
		PredictionSource:             predictionClient,
		PositionSource:               params.positionSource,
		OrderBookSource:              orderBooks,
		MidPriceSource:               midPrices,
		Strategy:                     strategyClient,
		Recorder:                     recorder,
		Executor:                     params.executor,
		SubmissionPolicy:             params.submissionPolicy,
		SubmitEnabled:                cycleConfig.OrderSubmissionEnabled,
		SubmissionDisabledAccounts:   cycleConfig.SubmissionDisabledAccounts,
		EntrySubmissionDisabled:      cycleConfig.EntrySubmissionDisabled,
		EntryDisabledAccounts:        cycleConfig.EntryDisabledAccounts,
		RequireCompleteModelCoverage: cycleConfig.RequireCompleteModelCoverage,
		Bindings:                     cycleConfig.Bindings,
		PredictionSourceModes:        cycleConfig.PredictionSourceModes,
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
	for index, raw := range accountIDs {
		accountID := strings.TrimSpace(raw)
		if accountID == "" {
			return fmt.Errorf("wallet file execution account %d is empty", index)
		}
		if _, duplicate := available[accountID]; duplicate {
			return fmt.Errorf("wallet file contains duplicate execution account %q", accountID)
		}
		available[accountID] = struct{}{}
	}
	if len(available) == 0 {
		return fmt.Errorf("decision cycle requires at least one wallet-file execution account")
	}
	bound := make(map[string]struct{}, len(cycleConfig.Bindings))
	for _, binding := range cycleConfig.Bindings {
		binding = binding.Normalize()
		if binding.ExecutionAccountID == "" {
			return fmt.Errorf("decision-cycle binding execution account is empty")
		}
		if _, duplicate := bound[binding.ExecutionAccountID]; duplicate {
			return fmt.Errorf("decision-cycle binding repeats execution account %q", binding.ExecutionAccountID)
		}
		bound[binding.ExecutionAccountID] = struct{}{}
		if _, exists := available[binding.ExecutionAccountID]; !exists {
			return fmt.Errorf("decision-cycle binding references execution account %q which is absent from the wallet file", binding.ExecutionAccountID)
		}
	}
	for accountID := range available {
		if _, exists := bound[accountID]; !exists {
			return fmt.Errorf("wallet file execution account %q has no decision-cycle binding", accountID)
		}
	}
	return nil
}
