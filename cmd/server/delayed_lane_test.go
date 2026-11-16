package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/adapter/predictioninfra"
	"github.com/UniPat-AI/trading_execution/internal/adapter/strategyhttp"
	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/decisioncycle"
	"github.com/UniPat-AI/trading_execution/internal/service/decisionrunner"
	"github.com/UniPat-AI/trading_execution/internal/service/reconciliation"
	"github.com/UniPat-AI/trading_execution/internal/service/strategyreport"
	"github.com/UniPat-AI/trading_execution/internal/service/tradingmonitor"
)

func laneTestDatabase(t *testing.T) *sql.DB {
	t.Helper()
	database, err := sql.Open("pgx", "postgres://unused")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

// laneTestCycle is a production-like topology. delayed lists the accounts of
// the delayed lane (empty = no delayed lane).
func laneTestCycle(delayed ...string) config.DecisionCycle {
	return config.DecisionCycle{
		Enabled: true, PredictionInfraBaseURL: "https://prediction.example",
		PredictionInfraToken: "prediction-token", StrategyBaseURL: "https://strategy.example",
		StrategyToken: "strategy-token", Interval: 10 * time.Minute, StartupDelay: 15 * time.Second,
		MaxStartLateness: 30 * time.Second, Timeout: 8 * time.Minute,
		StrategyTimeout: 90 * time.Second, PredictionTimeout: 30 * time.Second,
		OrderSubmissionEnabled: true, RequireCompleteModelCoverage: true,
		PredictionLookback: 3 * time.Hour, DeliveryConcurrency: 2,
		PredictionModelID: "producer", PredictionSourceMode: domain.PredictionSourceModeSandbox,
		Accounts: []config.DecisionAccount{
			{ExecutionAccountID: "main", MarketSource: domain.MarketSourcePolymarket, StrategyID: "brier_mid_edge_once", Mode: config.DecisionAccountActive},
			{ExecutionAccountID: "wallet-1", MarketSource: domain.MarketSourcePolymarket, StrategyID: "delayed_lane_pending", Mode: config.DecisionAccountActive},
			{ExecutionAccountID: "wallet-6", MarketSource: domain.MarketSourcePolymarket, StrategyID: domain.StrategyIDMultfactorV1, Mode: config.DecisionAccountPaused},
			{ExecutionAccountID: "wallet-7", MarketSource: domain.MarketSourcePolymarket, StrategyID: domain.StrategyIDMultfactorV2, Mode: config.DecisionAccountSellOnly},
			{ExecutionAccountID: "kalshi:main", MarketSource: domain.MarketSourceKalshi, StrategyID: "brier_mid_edge_once", Mode: config.DecisionAccountActive},
			{ExecutionAccountID: "kalshi:delayed", MarketSource: domain.MarketSourceKalshi, StrategyID: "delayed_lane_pending", Mode: config.DecisionAccountActive},
		},
		OrderReportStrategyIDs: []string{"brier_mid_edge_once"}, OrderReportLookback: 24 * time.Hour,
		DelayedAccountIDs:   delayed,
		DelayedStartupDelay: 5*time.Minute + 15*time.Second, DelayedTimeout: 4*time.Minute + 30*time.Second,
	}
}

func laneTestParams(t *testing.T, cycle config.DecisionCycle) buildDecisionRunnerParams {
	t.Helper()
	return buildDecisionRunnerParams{
		cfg:      config.Config{Execution: config.Execution{Venue: "polymarket"}, DecisionCycle: cycle},
		database: laneTestDatabase(t), positionSource: decisionRunnerTestPositionSource{},
		orderBooks: decisionRunnerTestOrderBookSource{}, executor: decisionRunnerTestExecutor{},
		accountGate: decisionRunnerTestAccountGate{},
		accountIDs:  []string{"main", "wallet-1", "wallet-6", "wallet-7"},
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func laneTestShared(t *testing.T, params buildDecisionRunnerParams) decisionLaneShared {
	t.Helper()
	prediction, err := predictioninfra.New(predictioninfra.Params{BaseURL: "https://prediction.example", BearerToken: "prediction-token"})
	if err != nil {
		t.Fatal(err)
	}
	strategy, err := strategyhttp.New(strategyhttp.Params{BaseURL: "https://strategy.example", BearerToken: "strategy-token"})
	if err != nil {
		t.Fatal(err)
	}
	recorder, err := postgresadapter.NewDecisionRecorder(params.database, nil)
	if err != nil {
		t.Fatal(err)
	}
	return decisionLaneShared{
		params: params, prediction: prediction, strategy: strategy,
		orderBooks: decisionRunnerTestOrderBookSource{}, recorder: recorder,
	}
}

func bindingAccounts(bindings []domain.StrategyExecutionBinding) string {
	ids := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		ids = append(ids, binding.ExecutionAccountID)
	}
	return strings.Join(ids, ",")
}

// TestDecisionLanesWithoutDelayedListMatchTheSingleCycle is AC1 for the
// composition: with an empty delayed list the primary lane receives exactly
// the decision-service and runner parameters the single cycle was built with
// before the delayed lane existed, and reconciliation binds the runner itself.
func TestDecisionLanesWithoutDelayedListMatchTheSingleCycle(t *testing.T) {
	cycleConfig := laneTestCycle()
	params := laneTestParams(t, cycleConfig)
	primary, delayed := planDecisionLanes(cycleConfig, params.logger)
	if delayed != nil {
		t.Fatalf("delayed lane = %#v, want none", delayed)
	}
	if !reflect.DeepEqual(primary.cycle, cycleConfig) || primary.logger != params.logger || primary.allowAllStrategyDisabled {
		t.Fatalf("primary lane = %#v", primary)
	}
	shared := laneTestShared(t, params)
	snapshotRecorder, err := postgresadapter.NewOrderBookSnapshotRecorder(params.database, nil)
	if err != nil {
		t.Fatal(err)
	}
	primary.snapshotRecorder = snapshotRecorder
	var notSubmitted decisioncycle.NotSubmittedReporter = &strategyreport.Reporter{}
	// The parameter literal of buildDecisionRunner before the delayed lane.
	want := decisioncycle.Params{
		PredictionSource:             shared.prediction,
		PositionSource:               params.positionSource,
		OrderBookSource:              shared.orderBooks,
		SnapshotRecorder:             snapshotRecorder,
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
		NotSubmittedReporter:         notSubmitted,
		StrategyTimeout:              cycleConfig.StrategyTimeout,
		PredictionTimeout:            cycleConfig.PredictionTimeout,
		Logger:                       params.logger,
	}
	if got := decisionCycleParams(shared, primary, notSubmitted); !reflect.DeepEqual(got, want) {
		t.Fatalf("primary decision-cycle params changed:\n%#v\nwant\n%#v", got, want)
	}
	runnerParams := decisionRunnerParams(nil, primary, nil)
	if runnerParams.Interval != cycleConfig.Interval || runnerParams.StartupDelay != cycleConfig.StartupDelay ||
		runnerParams.MaxStartLateness != cycleConfig.MaxStartLateness || runnerParams.Timeout != cycleConfig.Timeout ||
		runnerParams.Logger != params.logger || runnerParams.AfterCycle != nil || runnerParams.Now != nil || runnerParams.Wait != nil {
		t.Fatalf("primary runner params changed: %#v", runnerParams)
	}

	lanes, err := buildDecisionLanes(params)
	if err != nil || lanes.primary == nil || lanes.delayed != nil {
		t.Fatalf("buildDecisionLanes() = %#v, %v", lanes, err)
	}
	if activity, ok := lanes.decisionActivity().(*decisionrunner.Runner); !ok || activity != lanes.primary {
		t.Fatalf("decision activity = %#v, want the primary runner itself", lanes.decisionActivity())
	}
}

func TestDecisionLanesSplitDelayedAccounts(t *testing.T) {
	cycleConfig := laneTestCycle("wallet-1", "kalshi:delayed")
	params := laneTestParams(t, cycleConfig)
	primary, delayed := planDecisionLanes(cycleConfig, params.logger)
	if delayed == nil {
		t.Fatal("no delayed lane planned")
	}
	if got := bindingAccounts(primary.cycle.Bindings()); got != "main,wallet-6,wallet-7,kalshi:main" {
		t.Fatalf("primary accounts = %s", got)
	}
	if got := bindingAccounts(delayed.cycle.Bindings()); got != "wallet-1,kalshi:delayed" {
		t.Fatalf("delayed accounts = %s", got)
	}
	if primary.allowAllStrategyDisabled || !delayed.allowAllStrategyDisabled || primary.logger != params.logger || delayed.logger == params.logger {
		t.Fatalf("lane flags = primary %#v delayed %#v", primary, delayed)
	}
	shared := laneTestShared(t, params)
	primaryCycle, delayedCycle := decisionCycleParams(shared, primary, nil), decisionCycleParams(shared, *delayed, nil)
	seen := map[string]string{}
	for lane, bindings := range map[string][]domain.StrategyExecutionBinding{"primary": primaryCycle.Bindings, "delayed": delayedCycle.Bindings} {
		for _, binding := range bindings {
			if other, dup := seen[binding.ExecutionAccountID]; dup {
				t.Fatalf("account %s is in both %s and %s", binding.ExecutionAccountID, other, lane)
			}
			seen[binding.ExecutionAccountID] = lane
		}
	}
	if len(seen) != len(cycleConfig.Accounts) {
		t.Fatalf("lanes cover %d of %d accounts", len(seen), len(cycleConfig.Accounts))
	}
	if delayedCycle.DeliveryStaleAge != 4*time.Minute+30*time.Second || primaryCycle.DeliveryStaleAge != 8*time.Minute {
		t.Fatalf("delivery stale ages = %s/%s", primaryCycle.DeliveryStaleAge, delayedCycle.DeliveryStaleAge)
	}
	primaryRunner, delayedRunner := decisionRunnerParams(nil, primary, nil), decisionRunnerParams(nil, *delayed, nil)
	if primaryRunner.StartupDelay != 15*time.Second || primaryRunner.Timeout != 8*time.Minute ||
		delayedRunner.StartupDelay != 5*time.Minute+15*time.Second || delayedRunner.Timeout != 4*time.Minute+30*time.Second ||
		delayedRunner.Interval != primaryRunner.Interval || delayedRunner.MaxStartLateness != primaryRunner.MaxStartLateness {
		t.Fatalf("runner timing = primary %#v delayed %#v", primaryRunner, delayedRunner)
	}

	lanes, err := buildDecisionLanes(params)
	if err != nil || lanes.primary == nil || lanes.delayed == nil {
		t.Fatalf("buildDecisionLanes() = %#v, %v", lanes, err)
	}
	if _, composite := lanes.decisionActivity().(laneDecisionActivity); !composite {
		t.Fatalf("decision activity = %T, want the two-lane composite", lanes.decisionActivity())
	}
}

// TestDecisionLanesBuildWithEveryDelayedAccountPaused covers R4: a delayed
// lane whose accounts are all PAUSED still builds, while the primary lane
// keeps its all-paused rejection.
func TestDecisionLanesBuildWithEveryDelayedAccountPaused(t *testing.T) {
	cycleConfig := laneTestCycle("wallet-1", "kalshi:delayed")
	cycleConfig.Accounts[1].Mode = config.DecisionAccountPaused
	cycleConfig.Accounts[5].Mode = config.DecisionAccountPaused
	lanes, err := buildDecisionLanes(laneTestParams(t, cycleConfig))
	if err != nil || lanes.primary == nil || lanes.delayed == nil {
		t.Fatalf("buildDecisionLanes() = %#v, %v", lanes, err)
	}
	// Without the delayed list the same PAUSED accounts are ordinary primary
	// accounts and the primary lane still has active ones.
	cycleConfig.DelayedAccountIDs = nil
	if lanes, err := buildDecisionLanes(laneTestParams(t, cycleConfig)); err != nil || lanes.delayed != nil {
		t.Fatalf("buildDecisionLanes() without list = %#v, %v", lanes, err)
	}
}

type recordingOrderSource struct {
	mu       sync.Mutex
	accounts [][]string
}

func (source *recordingOrderSource) ListUpdatedSince(_ context.Context, accounts []string, _ time.Time, _ int) ([]domain.Order, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.accounts = append(source.accounts, append([]string(nil), accounts...))
	return nil, nil
}

type noopReportSink struct{}

func (noopReportSink) ReportOrders(context.Context, []domain.StrategyOrderReport) ([]domain.StrategyOrderReportResult, error) {
	return nil, nil
}

// TestOrderReporterCoversOnlyItsLane covers R7: each lane reports only its
// own accounts, and a lane without a reported strategy gets no reporter.
func TestOrderReporterCoversOnlyItsLane(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reportedAccounts := func(cycleConfig config.DecisionCycle, lane []domain.StrategyExecutionBinding) string {
		source := &recordingOrderSource{}
		reporter, err := buildOrderReporter(cycleConfig, lane, source, noopReportSink{}, logger)
		if err != nil {
			t.Fatalf("buildOrderReporter() error = %v", err)
		}
		if reporter == nil {
			return "<none>"
		}
		if err := reporter.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
		return strings.Join(source.accounts[0], ",")
	}
	empty := laneTestCycle()
	if got := reportedAccounts(empty, empty.PrimaryBindings()); got != "main,kalshi:main" {
		t.Fatalf("single-lane reported accounts = %s", got)
	}
	split := laneTestCycle("wallet-1", "kalshi:delayed")
	split.OrderReportStrategyIDs = []string{"brier_mid_edge_once", "delayed_lane_pending"}
	if got := reportedAccounts(split, split.PrimaryBindings()); got != "main,kalshi:main" {
		t.Fatalf("primary reported accounts = %s", got)
	}
	if got := reportedAccounts(split, split.DelayedBindings()); got != "wallet-1,kalshi:delayed" {
		t.Fatalf("delayed reported accounts = %s", got)
	}
	split.OrderReportStrategyIDs = []string{"brier_mid_edge_once"}
	if got := reportedAccounts(split, split.DelayedBindings()); got != "<none>" {
		t.Fatalf("delayed lane without a reported strategy = %s", got)
	}
	// A reported strategy bound only in the other lane is not a typo.
	split.OrderReportStrategyIDs = []string{"delayed_lane_pending"}
	if got := reportedAccounts(split, split.PrimaryBindings()); got != "<none>" {
		t.Fatalf("primary lane without a reported strategy = %s", got)
	}
	split.OrderReportStrategyIDs = []string{"strategy_typo"}
	if _, err := buildOrderReporter(split, split.DelayedBindings(), &recordingOrderSource{}, noopReportSink{}, logger); err == nil ||
		!strings.Contains(err.Error(), `"strategy_typo" has no decision binding`) {
		t.Fatalf("unbound reported strategy error = %v", err)
	}
}

type fixedActivity bool

func (activity fixedActivity) DecisionBusy(time.Time, time.Duration) bool { return bool(activity) }

// TestLaneDecisionActivityIsBusyWhenEitherLaneIs covers R6.
func TestLaneDecisionActivityIsBusyWhenEitherLaneIs(t *testing.T) {
	for _, test := range []struct {
		primary, delayed, want bool
	}{
		{false, false, false}, {true, false, true}, {false, true, true}, {true, true, true},
	} {
		activity := laneDecisionActivity{lanes: []reconciliation.DecisionActivity{fixedActivity(test.primary), fixedActivity(test.delayed)}}
		if got := activity.DecisionBusy(time.Now(), time.Minute); got != test.want {
			t.Fatalf("primary=%t delayed=%t busy=%t, want %t", test.primary, test.delayed, got, test.want)
		}
	}
}

// TestLaneDecisionActivityFollowsRealRunners checks the composite with two
// runners whose schedules are published: the delayed runner is due at
// T+5m15s and makes reconciliation yield around it.
func TestLaneDecisionActivityFollowsRealRunners(t *testing.T) {
	boundary := time.Date(2026, 11, 16, 8, 20, 0, 0, time.UTC)
	newRunner := func(delay time.Duration, now *time.Time, published chan<- struct{}) *decisionrunner.Runner {
		runner, err := decisionrunner.New(decisionrunner.Params{
			Cycle: idleCycle{}, StartupDelay: delay, Timeout: 4 * time.Minute,
			Now: func() time.Time { return *now },
			Wait: func(ctx context.Context, _ time.Time) error {
				published <- struct{}{}
				<-ctx.Done()
				return ctx.Err()
			},
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		if err != nil {
			t.Fatal(err)
		}
		return runner
	}
	now := boundary.Add(20 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	published := make(chan struct{}, 2)
	primary := newRunner(15*time.Second, &now, published)
	delayed := newRunner(5*time.Minute+15*time.Second, &now, published)
	go func() { _ = primary.Run(ctx) }()
	go func() { _ = delayed.Run(ctx) }()
	<-published
	<-published
	if got := delayed.Snapshot(); !got.NextBoundary.Equal(boundary) || !got.NextDueAt.Equal(boundary.Add(5*time.Minute+15*time.Second)) {
		t.Fatalf("delayed schedule = %s due %s, want boundary %s due T+5m15s", got.NextBoundary, got.NextDueAt, boundary)
	}
	activity := decisionLanes{primary: primary, delayed: delayed}.decisionActivity()
	for _, test := range []struct {
		at   time.Duration
		want bool
	}{
		{at: 3*time.Minute + 30*time.Second, want: false}, // :x3:30 reconciliation runs
		{at: 4*time.Minute + 30*time.Second, want: true},  // within 60s of the delayed lane
		{at: 5*time.Minute + 30*time.Second, want: true},  // inside its start window
		{at: 8*time.Minute + 30*time.Second, want: false}, // :x8:30 reconciliation runs
	} {
		if got := activity.DecisionBusy(boundary.Add(test.at), time.Minute); got != test.want {
			t.Fatalf("busy at T+%s = %t, want %t", test.at, got, test.want)
		}
		if primary.DecisionBusy(boundary.Add(test.at), time.Minute) {
			t.Fatalf("primary runner alone was busy at T+%s", test.at)
		}
	}
}

type idleCycle struct{}

func (idleCycle) Run(context.Context, time.Time) (decisioncycle.RunResult, error) {
	return decisioncycle.RunResult{}, nil
}

// TestRunDelayedDecisionLaneRestartsAndNeverFails covers R5 for the loop: a
// failed delayed lane is logged and restarted, never returned.
func TestRunDelayedDecisionLaneRestartsAndNeverFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var output strings.Builder
	var outputMu sync.Mutex
	logger := slog.New(slog.NewTextHandler(&lockedWriter{w: &output, mu: &outputMu}, nil))
	calls := 0
	run := func(runCtx context.Context) error {
		calls++
		if calls < 3 {
			return fmt.Errorf("startup strategy intent recovery: failure %d", calls)
		}
		cancel()
		<-runCtx.Done()
		return runCtx.Err()
	}
	restarts := 0
	restartAfter := func(delay time.Duration) <-chan time.Time {
		if delay != delayedLaneRestartDelay {
			t.Fatalf("restart delay = %s", delay)
		}
		restarts++
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}
	done := make(chan struct{})
	go func() {
		runDelayedDecisionLane(ctx, run, logger, restartAfter)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runDelayedDecisionLane did not return after cancellation")
	}
	outputMu.Lock()
	defer outputMu.Unlock()
	if calls != 3 || restarts != 2 || strings.Count(output.String(), "delayed decision lane loop stopped; restarting") != 2 {
		t.Fatalf("calls=%d restarts=%d log=%s", calls, restarts, output.String())
	}
}

type lockedWriter struct {
	w  io.Writer
	mu *sync.Mutex
}

func (writer *lockedWriter) Write(p []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.w.Write(p)
}

// TestPipelineMonitorObservesOnlyThePrimaryLane covers the monitor half of
// R3: without a delayed list the observed bindings are the ones it always
// built; with one, delayed accounts are left out.
func TestPipelineMonitorObservesOnlyThePrimaryLane(t *testing.T) {
	legacy := func(cycle config.DecisionCycle) []tradingmonitor.Binding {
		var bindings []tradingmonitor.Binding
		for _, raw := range cycle.Bindings() {
			b := raw.Normalize()
			bindings = append(bindings, tradingmonitor.Binding{StrategyExecutionBinding: b,
				EntryDisabled:    cycle.EntrySubmissionDisabled || containsMonitorAccount(cycle.EntryDisabledAccounts(), b.ExecutionAccountID),
				StrategyDisabled: containsMonitorAccount(cycle.StrategyDisabledAccounts(), b.ExecutionAccountID),
			})
		}
		return bindings
	}
	single := laneTestCycle()
	if got, want := pipelineMonitorBindings(single), legacy(single); !reflect.DeepEqual(got, want) {
		t.Fatalf("monitor bindings changed:\n%#v\nwant\n%#v", got, want)
	}
	split := laneTestCycle("wallet-1", "kalshi:delayed")
	var ids []string
	for _, binding := range pipelineMonitorBindings(split) {
		ids = append(ids, binding.ExecutionAccountID)
	}
	if strings.Join(ids, ",") != "main,wallet-6,wallet-7,kalshi:main" {
		t.Fatalf("monitored accounts = %v", ids)
	}
}
