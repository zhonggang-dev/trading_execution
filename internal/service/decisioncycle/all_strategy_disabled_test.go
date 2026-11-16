package decisioncycle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// The delayed decision lane may hold only PAUSED accounts. These fakes fail
// loudly if such an instance touches anything beyond durable recovery.

type countingPredictionSource struct{ calls int }

func (source *countingPredictionSource) Snapshot(context.Context, time.Time, time.Duration) (domain.PredictionSnapshot, error) {
	source.calls++
	return domain.PredictionSnapshot{}, errors.New("prediction source must not be called")
}

type countingOrderBookSource struct{ calls int }

func (source *countingOrderBookSource) Capture(context.Context, time.Time, []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	source.calls++
	return nil, errors.New("orderbook source must not be called")
}

type countingStrategy struct{ calls int }

func (strategy *countingStrategy) Decide(context.Context, domain.StrategyDecisionRequest) (domain.StrategyDecisionResponse, error) {
	strategy.calls++
	return domain.StrategyDecisionResponse{}, errors.New("strategy must not be called")
}

type countingPositionSource struct{ calls int }

func (source *countingPositionSource) ListOpenLots(context.Context, domain.StrategyExecutionContext) ([]domain.PositionLot, error) {
	source.calls++
	return nil, errors.New("position source must not be called")
}

func pausedLaneBindings() []domain.StrategyExecutionBinding {
	return []domain.StrategyExecutionBinding{
		{PredictionModelID: "producer", ModelID: "producer", StrategyID: "delayed_lane_pending",
			ExecutionAccountID: "wallet-1", MarketSource: domain.MarketSourcePolymarket},
		{PredictionModelID: "producer", ModelID: "producer", StrategyID: "delayed_lane_pending",
			ExecutionAccountID: "kalshi:delayed", MarketSource: domain.MarketSourceKalshi},
	}
}

func pausedLaneParams(allow bool) Params {
	return Params{
		PredictionSource: &countingPredictionSource{}, PositionSource: &countingPositionSource{},
		OrderBookSource: &countingOrderBookSource{}, SnapshotRecorder: &fakeOrderBookSnapshotRecorder{err: errors.New("snapshot recorder must not be called")},
		Strategy: &countingStrategy{}, Recorder: &fakeRecorder{}, Executor: &fakeExecutor{},
		SubmitEnabled: true, RequireCompleteModelCoverage: true,
		EntryDisabledAccounts:    []string{"wallet-1", "kalshi:delayed"},
		StrategyDisabledAccounts: []string{"wallet-1", "kalshi:delayed"},
		Bindings:                 pausedLaneBindings(),
		PredictionSourceModes:    map[string]domain.PredictionSourceMode{"producer": domain.PredictionSourceModeDirect},
		Venue:                    "polymarket",
		AllowAllStrategyDisabled: allow,
	}
}

// TestNewDefaultStillRejectsEveryAccountStrategyDisabled pins the unchanged
// default: an instance whose every account is PAUSED is refused.
func TestNewDefaultStillRejectsEveryAccountStrategyDisabled(t *testing.T) {
	_, err := New(pausedLaneParams(false))
	if err == nil || !strings.Contains(err.Error(), "strategy-disabled accounts cannot exclude every active decision binding") {
		t.Fatalf("New() error = %v, want the all-paused rejection", err)
	}
}

// TestAllStrategyDisabledLaneOnlyRecovers covers the delayed lane with every
// account PAUSED: it builds, delivers the persisted exit, and never fetches
// predictions, reads lots, captures books, stores a snapshot or calls the
// strategy.
func TestAllStrategyDisabledLaneOnlyRecovers(t *testing.T) {
	decisionAt := time.Date(2026, 11, 16, 8, 20, 0, 0, time.UTC)
	params := pausedLaneParams(true)
	recorder := &fakeRecorder{deliveries: []domain.DecisionIntentDelivery{
		{
			CycleID: "wallet-1:old", ClientOrderID: "persisted-exit",
			Intent: domain.OrderIntent{ClientOrderID: "persisted-exit", ExecutionAccountID: "wallet-1", Side: domain.SideSell},
			Status: domain.DecisionIntentPending,
		},
		{
			CycleID: "wallet-1:old", ClientOrderID: "persisted-entry",
			Intent: domain.OrderIntent{ClientOrderID: "persisted-entry", ExecutionAccountID: "wallet-1", Side: domain.SideBuy},
			Status: domain.DecisionIntentPending,
		},
	}}
	executor := &fakeExecutor{}
	params.Recorder, params.Executor = recorder, executor
	params.Now = func() time.Time { return decisionAt.Add(5*time.Minute + 15*time.Second) }
	service, err := New(params)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, err := service.Run(context.Background(), decisionAt)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	predictions := params.PredictionSource.(*countingPredictionSource)
	books := params.OrderBookSource.(*countingOrderBookSource)
	strategy := params.Strategy.(*countingStrategy)
	positions := params.PositionSource.(*countingPositionSource)
	snapshots := params.SnapshotRecorder.(*fakeOrderBookSnapshotRecorder)
	if predictions.calls != 0 || books.calls != 0 || strategy.calls != 0 || positions.calls != 0 || len(snapshots.batches) != 0 {
		t.Fatalf("all-paused lane touched new-cycle work: predictions=%d books=%d strategy=%d positions=%d snapshots=%d",
			predictions.calls, books.calls, strategy.calls, positions.calls, len(snapshots.batches))
	}
	// PAUSED is entry-disabled: like any paused account, only its persisted
	// exit is delivered; the persisted BUY stays pending.
	if len(executor.intents) != 1 || executor.intents[0].ClientOrderID != "persisted-exit" {
		t.Fatalf("delivered intents = %#v, want the persisted exit only", executor.intents)
	}
	if recorder.deliveries[0].Status != domain.DecisionIntentSubmitted || recorder.deliveries[1].Status != domain.DecisionIntentPending {
		t.Fatalf("delivery statuses = %s/%s", recorder.deliveries[0].Status, recorder.deliveries[1].Status)
	}
	if len(result.Recovery.Intents) != 1 || !result.DecisionAt.Equal(decisionAt) || result.PredictionSnapshotID != "" {
		t.Fatalf("result = %#v", result)
	}
	if len(result.Runs) != 2 {
		t.Fatalf("runs = %#v, want one skipped run per account", result.Runs)
	}
	for _, run := range result.Runs {
		if !run.AccountStrategyDisabled || run.Error != nil || run.OrderSubmissionEnabled || len(run.Intents) != 0 {
			t.Fatalf("run = %#v, want a strategy-disabled skip", run)
		}
	}
	if err := service.RecoverStartup(context.Background()); err != nil {
		t.Fatalf("RecoverStartup() error = %v", err)
	}
}

// TestAllowAllStrategyDisabledKeepsRunningAnActiveAccount proves the flag
// changes nothing while at least one account is sent to the strategy.
func TestAllowAllStrategyDisabledKeepsRunningAnActiveAccount(t *testing.T) {
	params := pausedLaneParams(true)
	params.EntryDisabledAccounts = []string{"kalshi:delayed"}
	params.StrategyDisabledAccounts = []string{"kalshi:delayed"}
	service, err := New(params)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if len(service.strategyBindings) != 1 || service.strategyBindings[0].ExecutionAccountID != "wallet-1" {
		t.Fatalf("strategy bindings = %#v", service.strategyBindings)
	}
	decisionAt := time.Date(2026, 11, 16, 8, 20, 0, 0, time.UTC)
	if _, err := service.Run(context.Background(), decisionAt); err == nil {
		t.Fatal("Run() succeeded, want the normal path to reach the (failing) prediction source")
	}
	if params.PredictionSource.(*countingPredictionSource).calls != 1 {
		t.Fatal("an active account did not take the normal cycle path")
	}
}
