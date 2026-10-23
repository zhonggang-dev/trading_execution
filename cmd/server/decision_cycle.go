package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/adapter/kalshi"
	"github.com/UniPat-AI/trading_execution/internal/adapter/marketbooks"
	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/adapter/predictioninfra"
	"github.com/UniPat-AI/trading_execution/internal/adapter/strategyhttp"
	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/decisioncycle"
	"github.com/UniPat-AI/trading_execution/internal/service/decisionrunner"
	"github.com/UniPat-AI/trading_execution/internal/service/strategyreport"
)

type buildDecisionRunnerParams struct {
	cfg              config.Config
	database         *sql.DB
	positionSource   port.StrategyPositionSource
	orderBooks       port.OrderBookSource
	executor         port.OrderExecutor
	accountGate      port.ExecutionAccountGate
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
	if cycleConfig.OrderSubmissionEnabled && params.accountGate == nil {
		return nil, fmt.Errorf("live decision-cycle submission requires a shared execution account gate")
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
	recorder, err := postgresadapter.NewDecisionRecorder(params.database, nil)
	if err != nil {
		return nil, fmt.Errorf("build strategy decision recorder: %w", err)
	}
	snapshotRecorder, err := postgresadapter.NewOrderBookSnapshotRecorder(params.database, nil)
	if err != nil {
		return nil, fmt.Errorf("build orderbook snapshot recorder: %w", err)
	}
	var orderReporter *strategyreport.Reporter
	if len(cycleConfig.OrderReportStrategyIDs) > 0 {
		orderRepository, repositoryErr := postgresadapter.NewOrderRepository(params.database)
		if repositoryErr != nil {
			return nil, fmt.Errorf("build order repository for strategy order reports: %w", repositoryErr)
		}
		orderReporter, err = buildOrderReporter(cycleConfig, orderRepository, strategyClient, params.logger)
		if err != nil {
			return nil, err
		}
	}
	var notSubmittedReporter decisioncycle.NotSubmittedReporter
	if orderReporter != nil {
		notSubmittedReporter = orderReporter
	}
	cycle, err := decisioncycle.New(decisioncycle.Params{
		PredictionSource:             predictionClient,
		PositionSource:               params.positionSource,
		OrderBookSource:              orderBooks,
		SnapshotRecorder:             snapshotRecorder,
		Strategy:                     strategyClient,
		Recorder:                     recorder,
		Executor:                     params.executor,
		AccountGate:                  params.accountGate,
		SubmissionPolicy:             params.submissionPolicy,
		SubmitEnabled:                cycleConfig.OrderSubmissionEnabled,
		SubmissionDisabledAccounts:   cycleConfig.SubmissionDisabledAccounts,
		EntrySubmissionDisabled:      cycleConfig.EntrySubmissionDisabled,
		EntryDisabledAccounts:        cycleConfig.EntryDisabledAccounts,
		StrategyDisabledAccounts:     cycleConfig.StrategyDisabledAccounts,
		KalshiStrategyInputDisabled:  !cycleConfig.KalshiStrategyInputEnabled,
		RequireCompleteModelCoverage: cycleConfig.RequireCompleteModelCoverage,
		Bindings:                     cycleConfig.Bindings,
		PredictionSourceModes:        cycleConfig.PredictionSourceModes,
		Venue:                        params.cfg.Execution.Venue,
		PredictionLookback:           cycleConfig.PredictionLookback,
		DeliveryStaleAge:             cycleConfig.Timeout,
		DeliveryConcurrency:          cycleConfig.DeliveryConcurrency,
		NotSubmittedReporter:         notSubmittedReporter,
		Logger:                       params.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("build decision cycle: %w", err)
	}
	runner, err := decisionrunner.New(decisionrunner.Params{
		Cycle: cycle, Interval: cycleConfig.Interval, StartupDelay: cycleConfig.StartupDelay,
		MaxStartLateness: cycleConfig.MaxStartLateness,
		Timeout:          cycleConfig.Timeout, Logger: params.logger,
		AfterCycle: orderReportHook(orderReporter),
	})
	if err != nil {
		return nil, fmt.Errorf("build decision cycle runner: %w", err)
	}
	return runner, nil
}

// buildOrderReporter wires strategy order reporting to the accounts bound to
// the configured strategies. A configured strategy with no binding is a typo.
func buildOrderReporter(
	cycleConfig config.DecisionCycle,
	orders strategyreport.OrderSource,
	sink strategyreport.Sink,
	logger *slog.Logger,
) (*strategyreport.Reporter, error) {
	configured := make(map[string]struct{}, len(cycleConfig.OrderReportStrategyIDs))
	for _, id := range cycleConfig.OrderReportStrategyIDs {
		configured[id] = struct{}{}
	}
	bound := make(map[string]struct{}, len(configured))
	accounts := make([]string, 0)
	seenAccounts := make(map[string]struct{})
	for _, binding := range cycleConfig.Bindings {
		if _, reported := configured[binding.StrategyID]; !reported {
			continue
		}
		bound[binding.StrategyID] = struct{}{}
		if _, seen := seenAccounts[binding.ExecutionAccountID]; !seen {
			seenAccounts[binding.ExecutionAccountID] = struct{}{}
			accounts = append(accounts, binding.ExecutionAccountID)
		}
	}
	for id := range configured {
		if _, ok := bound[id]; !ok {
			return nil, fmt.Errorf("DECISION_CYCLE_ORDER_REPORT_STRATEGY_IDS_JSON strategy %q has no decision binding", id)
		}
	}
	reporter, err := strategyreport.New(strategyreport.Params{
		Orders: orders, Sink: sink, StrategyIDs: cycleConfig.OrderReportStrategyIDs, AccountIDs: accounts,
		Lookback: cycleConfig.OrderReportLookback, Logger: logger,
	})
	if err != nil {
		return nil, fmt.Errorf("build strategy order reporter: %w", err)
	}
	return reporter, nil
}

// orderReportHook returns the post-cycle hook, or nil when reporting is off
// (a nil func keeps the runner free of any reporting path).
func orderReportHook(reporter *strategyreport.Reporter) func(context.Context) error {
	if reporter == nil {
		return nil
	}
	return reporter.Sync
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
