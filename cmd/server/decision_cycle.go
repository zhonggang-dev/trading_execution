package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/kalshi"
	"github.com/UniPat-AI/trading_execution/internal/adapter/marketbooks"
	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/adapter/predictioninfra"
	"github.com/UniPat-AI/trading_execution/internal/adapter/strategyhttp"
	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/accountmaintenance"
	"github.com/UniPat-AI/trading_execution/internal/service/decisioncycle"
	"github.com/UniPat-AI/trading_execution/internal/service/decisionrunner"
	"github.com/UniPat-AI/trading_execution/internal/service/reconciliation"
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
	// routeInputPolicy skips the strategy call for accounts whose venue route
	// cannot place orders (a Kalshi account in maintenance); nil routes all.
	routeInputPolicy decisioncycle.RouteInputPolicy
	// maintenance receives the Kalshi accounts that cannot trade because the
	// Kalshi market-data client could not be built; optional.
	maintenance *accountmaintenance.Registry
}

// decisionLanes holds the runners of the decision cycle. delayed is nil unless
// DECISION_CYCLE_DELAYED_ACCOUNTS_JSON lists at least one account.
type decisionLanes struct {
	primary *decisionrunner.Runner
	delayed *decisionrunner.Runner
}

// delayedLaneLogAttribute marks every log line of the delayed lane. The
// primary lane's logger is left untouched so its lines stay byte-identical.
const delayedLaneLogAttribute = "delayed"

// buildDecisionRunner returns the primary lane's runner. Live composition uses
// buildDecisionLanes, which also builds the optional delayed lane.
func buildDecisionRunner(params buildDecisionRunnerParams) (*decisionrunner.Runner, error) {
	lanes, err := buildDecisionLanes(params)
	if err != nil {
		return nil, err
	}
	return lanes.primary, nil
}

// decisionLaneShared are the dependencies built once and used by both lanes.
type decisionLaneShared struct {
	params          buildDecisionRunnerParams
	prediction      *predictioninfra.Client
	strategy        *strategyhttp.Client
	orderBooks      port.OrderBookSource
	recorder        *postgresadapter.DecisionRecorder
	orderRepository strategyreport.OrderSource
}

// decisionLaneSpec is what differs between the two lanes.
type decisionLaneSpec struct {
	// cycle is the lane's view of the configuration: its accounts and its
	// StartupDelay/Timeout.
	cycle            config.DecisionCycle
	snapshotRecorder port.OrderBookSnapshotRecorder
	// allowAllStrategyDisabled is set only for the delayed lane, whose
	// accounts may all be PAUSED.
	allowAllStrategyDisabled bool
	logger                   *slog.Logger
}

// buildDecisionLanes composes the pull-based production workflow without
// starting it. All configuration and durable dependencies are validated before
// the CLOB heartbeat is activated. Without a delayed account list it builds
// exactly the one runner it always built. With one, a second runner runs only
// the delayed accounts at T+DECISION_CYCLE_DELAYED_STARTUP_DELAY for the same
// decision_at T; the lanes share every dependency except their snapshot tables
// and order reporters, and their account sets are disjoint.
func buildDecisionLanes(params buildDecisionRunnerParams) (decisionLanes, error) {
	cycleConfig := params.cfg.DecisionCycle
	if !cycleConfig.Enabled {
		return decisionLanes{}, nil
	}
	if params.database == nil || params.positionSource == nil || params.orderBooks == nil || params.executor == nil {
		return decisionLanes{}, fmt.Errorf("decision cycle requires postgres, position, orderbook, and execution dependencies")
	}
	if cycleConfig.OrderSubmissionEnabled && params.accountGate == nil {
		return decisionLanes{}, fmt.Errorf("live decision-cycle submission requires a shared execution account gate")
	}
	if err := validateDecisionAccounts(cycleConfig, params.accountIDs); err != nil {
		return decisionLanes{}, err
	}
	predictionClient, err := predictioninfra.New(predictioninfra.Params{
		BaseURL: cycleConfig.PredictionInfraBaseURL, BearerToken: cycleConfig.PredictionInfraToken,
		HTTPClient: noRedirectHTTPClient(cycleConfig.Timeout),
	})
	if err != nil {
		return decisionLanes{}, fmt.Errorf("build prediction_infra decision client: %w", err)
	}
	strategyClient, err := strategyhttp.New(strategyhttp.Params{
		BaseURL: cycleConfig.StrategyBaseURL, BearerToken: cycleConfig.StrategyToken,
		HTTPClient: noRedirectHTTPClient(cycleConfig.Timeout),
	})
	if err != nil {
		return decisionLanes{}, fmt.Errorf("build strategy decision client: %w", err)
	}
	var kalshiClient *kalshi.Client
	var kalshiOrderBooks port.OrderBookSource
	if params.cfg.Kalshi.MarketDataEnabled {
		var buildErr error
		kalshiClient, buildErr = kalshi.NewClient(kalshi.ClientParams{
			BaseURL: params.cfg.Kalshi.APIURL, ReadBaseURLs: params.cfg.Kalshi.ReadAPIURLs,
			APIKeyID: params.cfg.Kalshi.APIKeyID, PrivateKeyPath: params.cfg.Kalshi.PrivateKeyPath,
			HTTPClient: noRedirectHTTPClient(params.cfg.Kalshi.RequestTimeout), Logger: params.logger,
		})
		if buildErr != nil {
			buildErr = fmt.Errorf("build Kalshi authenticated client: %w", buildErr)
		} else if kalshiOrderBooks, buildErr = kalshi.NewOrderBookSource(kalshi.OrderBookParams{
			Client: kalshiClient, Logger: params.logger,
		}); buildErr != nil {
			buildErr = fmt.Errorf("build Kalshi orderbook source: %w", buildErr)
		}
		if buildErr != nil {
			// A local Kalshi key/client problem must not stop Polymarket. Without
			// Kalshi books every Kalshi account is held in maintenance (no strategy
			// call, no delivery); marketbooks reports Kalshi targets as not
			// configured. A restart is needed after the key is repaired.
			kalshiOrderBooks = nil
			logger := params.logger
			if logger == nil {
				logger = slog.Default()
			}
			logger.Error("Kalshi market data unavailable; Kalshi accounts held in maintenance", "error", buildErr)
			for _, accountID := range configuredKalshiAccountIDs(cycleConfig) {
				enterAccountMaintenance(logger, params.maintenance, accountID,
					accountmaintenance.ReasonKalshiMarketDataUnavailable, buildErr.Error())
			}
		}
	}
	// Each venue's capture has its own budget (marketbooks.DefaultVenueTimeout);
	// a venue that fails as a whole is marked ERROR without failing the other.
	orderBooks, err := marketbooks.New(marketbooks.Params{
		Polymarket: params.orderBooks,
		Kalshi:     kalshiOrderBooks,
		Logger:     params.logger,
	})
	if err != nil {
		return decisionLanes{}, fmt.Errorf("build venue orderbook router: %w", err)
	}
	recorder, err := postgresadapter.NewDecisionRecorder(params.database, nil)
	if err != nil {
		return decisionLanes{}, fmt.Errorf("build strategy decision recorder: %w", err)
	}
	snapshotRecorder, err := postgresadapter.NewOrderBookSnapshotRecorder(params.database, nil)
	if err != nil {
		return decisionLanes{}, fmt.Errorf("build orderbook snapshot recorder: %w", err)
	}
	shared := decisionLaneShared{
		params: params, prediction: predictionClient, strategy: strategyClient,
		orderBooks: orderBooks, recorder: recorder,
	}
	if len(cycleConfig.OrderReportStrategyIDs) > 0 {
		orderRepository, repositoryErr := postgresadapter.NewOrderRepository(params.database)
		if repositoryErr != nil {
			return decisionLanes{}, fmt.Errorf("build order repository for strategy order reports: %w", repositoryErr)
		}
		shared.orderRepository = orderRepository
	}
	primarySpec, delayedSpec := planDecisionLanes(cycleConfig, params.logger)
	primarySpec.snapshotRecorder = snapshotRecorder
	primary, err := buildDecisionLane(shared, primarySpec)
	if err != nil {
		return decisionLanes{}, err
	}
	if delayedSpec == nil {
		return decisionLanes{primary: primary}, nil
	}
	delayedRecorder, err := postgresadapter.NewDelayedOrderBookSnapshotRecorder(params.database, nil)
	if err != nil {
		return decisionLanes{}, fmt.Errorf("build delayed orderbook snapshot recorder: %w", err)
	}
	delayedSpec.snapshotRecorder = delayedRecorder
	delayed, err := buildDecisionLane(shared, *delayedSpec)
	if err != nil {
		return decisionLanes{}, fmt.Errorf("delayed decision lane: %w", err)
	}
	return decisionLanes{primary: primary, delayed: delayed}, nil
}

// planDecisionLanes splits the configuration into the primary lane and, when
// DECISION_CYCLE_DELAYED_ACCOUNTS_JSON is set, the delayed lane. Snapshot
// recorders are attached by the caller. Without a delayed list the primary
// lane is the unchanged configuration with the unchanged logger.
func planDecisionLanes(cycleConfig config.DecisionCycle, logger *slog.Logger) (decisionLaneSpec, *decisionLaneSpec) {
	primary := decisionLaneSpec{cycle: cycleConfig.PrimaryLane(), logger: logger}
	if !cycleConfig.HasDelayedLane() {
		return primary, nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	return primary, &decisionLaneSpec{
		cycle: cycleConfig.DelayedLane(), allowAllStrategyDisabled: true,
		logger: logger.With("lane", delayedLaneLogAttribute),
	}
}

// buildDecisionLane builds one lane's decision service and runner.
func buildDecisionLane(shared decisionLaneShared, lane decisionLaneSpec) (*decisionrunner.Runner, error) {
	params, cycleConfig := shared.params, lane.cycle
	var orderReporter *strategyreport.Reporter
	if shared.orderRepository != nil {
		var err error
		orderReporter, err = buildOrderReporter(params.cfg.DecisionCycle, cycleConfig.Bindings(),
			shared.orderRepository, shared.strategy, lane.logger)
		if err != nil {
			return nil, err
		}
	}
	var notSubmittedReporter decisioncycle.NotSubmittedReporter
	if orderReporter != nil {
		notSubmittedReporter = orderReporter
	}
	cycle, err := decisioncycle.New(decisionCycleParams(shared, lane, notSubmittedReporter))
	if err != nil {
		return nil, fmt.Errorf("build decision cycle: %w", err)
	}
	runner, err := decisionrunner.New(decisionRunnerParams(cycle, lane, orderReporter))
	if err != nil {
		return nil, fmt.Errorf("build decision cycle runner: %w", err)
	}
	return runner, nil
}

// decisionCycleParams derives one lane's decision-service parameters. For the
// primary lane without a delayed list they are the parameters the single
// decision cycle always had.
func decisionCycleParams(
	shared decisionLaneShared,
	lane decisionLaneSpec,
	notSubmittedReporter decisioncycle.NotSubmittedReporter,
) decisioncycle.Params {
	params, cycleConfig := shared.params, lane.cycle
	return decisioncycle.Params{
		PredictionSource:             shared.prediction,
		PositionSource:               params.positionSource,
		OrderBookSource:              shared.orderBooks,
		SnapshotRecorder:             lane.snapshotRecorder,
		Strategy:                     shared.strategy,
		Recorder:                     shared.recorder,
		Executor:                     params.executor,
		AccountGate:                  params.accountGate,
		SubmissionPolicy:             params.submissionPolicy,
		SubmitEnabled:                cycleConfig.OrderSubmissionEnabled,
		EntrySubmissionDisabled:      cycleConfig.EntrySubmissionDisabled,
		EntryDisabledAccounts:        cycleConfig.EntryDisabledAccounts(),
		StrategyDisabledAccounts:     cycleConfig.StrategyDisabledAccounts(),
		RouteInputPolicy:             params.routeInputPolicy,
		RequireCompleteModelCoverage: cycleConfig.RequireCompleteModelCoverage,
		Bindings:                     cycleConfig.Bindings(),
		PredictionSourceModes:        cycleConfig.PredictionSourceModes(),
		Venue:                        params.cfg.Execution.Venue,
		PredictionLookback:           cycleConfig.PredictionLookback,
		DeliveryStaleAge:             cycleConfig.Timeout,
		DeliveryConcurrency:          cycleConfig.DeliveryConcurrency,
		NotSubmittedReporter:         notSubmittedReporter,
		StrategyTimeout:              cycleConfig.StrategyTimeout,
		PredictionTimeout:            cycleConfig.PredictionTimeout,
		Logger:                       lane.logger,
		AllowAllStrategyDisabled:     lane.allowAllStrategyDisabled,
	}
}

// decisionRunnerParams derives one lane's scheduler parameters.
func decisionRunnerParams(
	cycle decisionrunner.Cycle,
	lane decisionLaneSpec,
	orderReporter *strategyreport.Reporter,
) decisionrunner.Params {
	cycleConfig := lane.cycle
	return decisionrunner.Params{
		Cycle: cycle, Interval: cycleConfig.Interval, StartupDelay: cycleConfig.StartupDelay,
		MaxStartLateness: cycleConfig.MaxStartLateness,
		Timeout:          cycleConfig.Timeout, Logger: lane.logger,
		AfterCycle: orderReportHook(orderReporter),
	}
}

// buildOrderReporter wires strategy order reporting to the accounts of one lane
// bound to the configured strategies. A configured strategy with no binding in
// the whole configuration is a typo; a lane without any reported account gets
// no reporter (nil).
func buildOrderReporter(
	cycleConfig config.DecisionCycle,
	laneBindings []domain.StrategyExecutionBinding,
	orders strategyreport.OrderSource,
	sink strategyreport.Sink,
	logger *slog.Logger,
) (*strategyreport.Reporter, error) {
	configured := make(map[string]struct{}, len(cycleConfig.OrderReportStrategyIDs))
	for _, id := range cycleConfig.OrderReportStrategyIDs {
		configured[id] = struct{}{}
	}
	bound := make(map[string]struct{}, len(configured))
	for _, binding := range cycleConfig.Bindings() {
		if _, reported := configured[binding.StrategyID]; reported {
			bound[binding.StrategyID] = struct{}{}
		}
	}
	for id := range configured {
		if _, ok := bound[id]; !ok {
			return nil, fmt.Errorf("DECISION_CYCLE_ORDER_REPORT_STRATEGY_IDS_JSON strategy %q has no decision binding", id)
		}
	}
	accounts := make([]string, 0)
	seenAccounts := make(map[string]struct{})
	for _, binding := range laneBindings {
		if _, reported := configured[binding.StrategyID]; !reported {
			continue
		}
		if _, seen := seenAccounts[binding.ExecutionAccountID]; !seen {
			seenAccounts[binding.ExecutionAccountID] = struct{}{}
			accounts = append(accounts, binding.ExecutionAccountID)
		}
	}
	if len(accounts) == 0 {
		return nil, nil
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

// laneDecisionActivity tells scheduled reconciliation that a decision cycle is
// running or due when either lane is.
type laneDecisionActivity struct {
	lanes []reconciliation.DecisionActivity
}

func (activity laneDecisionActivity) DecisionBusy(now time.Time, lead time.Duration) bool {
	for _, lane := range activity.lanes {
		if lane.DecisionBusy(now, lead) {
			return true
		}
	}
	return false
}

// decisionActivity returns what reconciliation defers to. Without a delayed
// lane it is the primary runner itself, exactly as before.
func (lanes decisionLanes) decisionActivity() reconciliation.DecisionActivity {
	if lanes.delayed == nil {
		return lanes.primary
	}
	return laneDecisionActivity{lanes: []reconciliation.DecisionActivity{lanes.primary, lanes.delayed}}
}

// delayedLaneRestartDelay is how long the delayed lane waits before it starts
// again after its loop stopped (for example a failed startup recovery).
const delayedLaneRestartDelay = time.Minute

// runDelayedDecisionLane keeps the delayed lane's loop running until ctx ends.
// It never returns an error: a delayed-lane failure is logged and retried and
// never stops the process or the primary lane.
func runDelayedDecisionLane(
	ctx context.Context,
	run func(context.Context) error,
	logger *slog.Logger,
	restartAfter func(time.Duration) <-chan time.Time,
) {
	for {
		err := run(ctx)
		if ctx.Err() != nil {
			return
		}
		logger.Error("delayed decision lane loop stopped; restarting", "error", err,
			"restart_after", delayedLaneRestartDelay)
		select {
		case <-ctx.Done():
			return
		case <-restartAfter(delayedLaneRestartDelay):
		}
	}
}

// validateDecisionAccounts proves that the Polymarket wallet file holds
// exactly the configured Polymarket accounts. Kalshi accounts have no wallet
// and are checked against their credentials by the Kalshi composition.
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
		return fmt.Errorf("live execution requires at least one wallet-file execution account")
	}
	bound := make(map[string]struct{}, len(cycleConfig.Accounts))
	for _, account := range cycleConfig.AccountsForSource(domain.MarketSourcePolymarket) {
		accountID := strings.TrimSpace(account.ExecutionAccountID)
		if accountID == "" {
			return fmt.Errorf("decision-cycle account execution account is empty")
		}
		if _, duplicate := bound[accountID]; duplicate {
			return fmt.Errorf("decision-cycle accounts repeat execution account %q", accountID)
		}
		bound[accountID] = struct{}{}
		if _, exists := available[accountID]; !exists {
			return fmt.Errorf("Polymarket account %q is absent from the wallet file", accountID)
		}
	}
	for accountID := range available {
		if _, exists := bound[accountID]; !exists {
			return fmt.Errorf("wallet file execution account %q is not a POLYMARKET account in DECISION_CYCLE_ACCOUNTS_JSON", accountID)
		}
	}
	return nil
}

// venueAccountGate serializes intent delivery per execution account. A
// Polymarket account is held through the reconciliation runner so delivery
// never overlaps its reconciliation; a Kalshi account has no Polymarket
// reconciliation and uses its own in-process slot.
type venueAccountGate struct {
	polymarket port.ExecutionAccountGate
	kalshi     map[string]chan struct{}
}

func newVenueAccountGate(polymarketGate port.ExecutionAccountGate, kalshiAccounts []string) (*venueAccountGate, error) {
	if polymarketGate == nil {
		return nil, fmt.Errorf("Polymarket execution account gate is required")
	}
	gate := &venueAccountGate{polymarket: polymarketGate, kalshi: make(map[string]chan struct{}, len(kalshiAccounts))}
	for _, raw := range kalshiAccounts {
		accountID := strings.TrimSpace(raw)
		if !domain.IsKalshiExecutionAccount(accountID) {
			return nil, fmt.Errorf("Kalshi execution account gate %q must start with %q", accountID, domain.KalshiExecutionAccountPrefix)
		}
		if _, duplicate := gate.kalshi[accountID]; duplicate {
			return nil, fmt.Errorf("Kalshi execution account gate %q is duplicated", accountID)
		}
		gate.kalshi[accountID] = make(chan struct{}, 1)
	}
	return gate, nil
}

func (gate *venueAccountGate) AcquireExecutionAccount(ctx context.Context, executionAccountID string) (func(), error) {
	accountID := strings.TrimSpace(executionAccountID)
	if !domain.IsKalshiExecutionAccount(accountID) {
		return gate.polymarket.AcquireExecutionAccount(ctx, accountID)
	}
	slot, exists := gate.kalshi[accountID]
	if !exists {
		return nil, fmt.Errorf("Kalshi execution account %q is not configured", accountID)
	}
	select {
	case slot <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-slot }) }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
