package decisioncycle

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

var runQueueDecisionAt = time.Date(2026, 11, 20, 4, 10, 0, 0, time.UTC)

// queuePrediction is one Sandbox prediction of a model run. Every model and run
// of one market shares the market identity, like the producer.
func queuePrediction(model string, run int, sandbox string, market int, yes float64) domain.Prediction {
	return domain.Prediction{
		PredictionID:  fmt.Sprintf("pred-%s-%d-%s", strings.ReplaceAll(model, " ", "_"), run, sandbox),
		SourceJobID:   fmt.Sprintf("job-%s-%d-%s", strings.ReplaceAll(model, " ", "_"), run, sandbox),
		SandboxID:     sandbox,
		PredictionRun: run,
		MarketID:      fmt.Sprintf("market-%d", market),
		ConditionID:   fmt.Sprintf("condition-%d", market),
		Question:      "Will it happen?",
		Domains:       []string{"World"},
		Outcomes: []domain.PredictionOutcome{
			{Index: 0, Name: "Yes", TokenID: fmt.Sprintf("yes-%d", market), Probability: yes},
			{Index: 1, Name: "No", TokenID: fmt.Sprintf("no-%d", market), Probability: 1 - yes},
		},
		PredictionAsOf: runQueueDecisionAt.Add(-30 * time.Minute),
		CompletedAt:    runQueueDecisionAt.Add(-10 * time.Minute),
		AvailableAt:    runQueueDecisionAt.Add(-9 * time.Minute),
		Model:          domain.PredictionModel{Name: model},
	}
}

func queueBooks(markets ...int) []domain.OrderBookSnapshot {
	books := make([]domain.OrderBookSnapshot, 0, 2*len(markets))
	for _, market := range markets {
		books = append(books, polymarketBooks(runQueueDecisionAt, queuePrediction("any", 1, "s", market, 0.5))...)
	}
	return books
}

type queueFixture struct {
	params    Params
	strategy  *matrixStrategy
	books     *fakeOrderBookSource
	snapshots *fakeOrderBookSnapshotRecorder
	recorder  *fakeRecorder
}

func newQueueFixture(bindings []domain.StrategyExecutionBinding, predictions []domain.Prediction, markets ...int) *queueFixture {
	fixture := &queueFixture{
		strategy:  &matrixStrategy{},
		books:     &fakeOrderBookSource{books: queueBooks(markets...)},
		snapshots: &fakeOrderBookSnapshotRecorder{},
		recorder:  &fakeRecorder{},
	}
	modes := make(map[string]domain.PredictionSourceMode)
	for _, binding := range bindings {
		modes[binding.Normalize().PredictionModelID] = domain.PredictionSourceModeSandbox
	}
	fixture.params = Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion, SnapshotID: "predsnap-queue",
			DecisionAt: runQueueDecisionAt, CompletedAfter: runQueueDecisionAt.Add(-3 * time.Hour),
			GeneratedAt: runQueueDecisionAt.Add(time.Second), Predictions: predictions,
		}},
		PositionSource:               accountPositionSource{},
		OrderBookSource:              fixture.books,
		SnapshotRecorder:             fixture.snapshots,
		Strategy:                     fixture.strategy,
		Recorder:                     fixture.recorder,
		Executor:                     &fakeExecutor{},
		SubmitEnabled:                true,
		RequireCompleteModelCoverage: true,
		Bindings:                     bindings,
		PredictionSourceModes:        modes,
		Venue:                        "polymarket-paper",
		Now:                          func() time.Time { return runQueueDecisionAt.Add(5 * time.Second) },
	}
	return fixture
}

func (fixture *queueFixture) run(t *testing.T) RunResult {
	t.Helper()
	service, err := New(fixture.params)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, err := service.Run(context.Background(), runQueueDecisionAt)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	return result
}

func predictionIDs(request domain.StrategyDecisionRequest) string {
	ids := make([]string, 0, len(request.Predictions))
	for _, prediction := range request.Predictions {
		ids = append(ids, prediction.PredictionID)
	}
	return strings.Join(ids, ",")
}

func brierBinding(account, model string, run int) domain.StrategyExecutionBinding {
	return domain.StrategyExecutionBinding{
		PredictionModelID: model, PredictionRun: run, ModelID: model,
		StrategyID: "brier_mid_edge_once", ExecutionAccountID: account,
	}
}

// TestRunGivesEachAccountItsOwnPredictionRun: three runs of one model for one
// Sandbox reach three accounts, each only its own run.
func TestRunGivesEachAccountItsOwnPredictionRun(t *testing.T) {
	predictions := []domain.Prediction{
		queuePrediction("v4.1 flash", 1, "sandbox-1", 1, 0.61),
		queuePrediction("v4.1 flash", 2, "sandbox-1", 1, 0.62),
		queuePrediction("v4.1 flash", 3, "sandbox-1", 1, 0.63),
	}
	fixture := newQueueFixture([]domain.StrategyExecutionBinding{
		brierBinding("main", "v4.1 flash", 1),
		brierBinding("wallet-2", "v4.1 flash", 2),
		brierBinding("wallet-4", "v4.1 flash", 3),
	}, predictions, 1)
	result := fixture.run(t)
	requests := requestsByAccount(fixture.strategy)
	for account, want := range map[string]domain.Prediction{
		"main": predictions[0], "wallet-2": predictions[1], "wallet-4": predictions[2],
	} {
		request := requests[account]
		if predictionIDs(request) != want.PredictionID || request.Predictions[0].Outcomes[0].Probability != want.Outcomes[0].Probability {
			t.Fatalf("%s predictions = %s, want only %s", account, predictionIDs(request), want.PredictionID)
		}
		payload, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(payload), "prediction_run") {
			t.Fatalf("strategy request of %s exposes prediction_run", account)
		}
	}
	for _, run := range result.Runs {
		if run.Error != nil || !run.EntrySubmissionEnabled || run.LeaderGated != 0 {
			t.Fatalf("run %s = %#v", run.Context.ExecutionAccountID, run)
		}
	}
}

// TestRunNeverSubstitutesAnotherPredictionRun: an account whose run is missing
// gets no prediction for that market (and is coverage-blocked) instead of
// another run's result; a legacy record without a run is run 1.
func TestRunNeverSubstitutesAnotherPredictionRun(t *testing.T) {
	legacy := queuePrediction("v4.1 flash", 0, "sandbox-1", 1, 0.6)
	second := queuePrediction("v4.1 flash", 2, "sandbox-2", 2, 0.7)
	fixture := newQueueFixture([]domain.StrategyExecutionBinding{
		brierBinding("main", "v4.1 flash", 0),
		brierBinding("wallet-2", "v4.1 flash", 2),
		brierBinding("wallet-4", "v4.1 flash", 3),
	}, []domain.Prediction{legacy, second}, 1, 2)
	result := fixture.run(t)
	requests := requestsByAccount(fixture.strategy)
	if got := predictionIDs(requests["main"]); got != legacy.PredictionID {
		t.Fatalf("main predictions = %s, want the legacy run-1 record", got)
	}
	// wallet-2 follows main; main has not predicted sandbox-2, so wallet-2's
	// run-2 prediction is held, never replaced by main's run 1.
	if got := predictionIDs(requests["wallet-2"]); got != "" {
		t.Fatalf("wallet-2 predictions = %s, want none", got)
	}
	if got := predictionIDs(requests["wallet-4"]); got != "" {
		t.Fatalf("wallet-4 predictions = %s, want none: run 3 is missing", got)
	}
	for _, run := range result.Runs {
		switch run.Context.ExecutionAccountID {
		case "wallet-4":
			if run.LeaderGated != 0 || run.EntryBlockReason != domain.StrategyEntryBlockIncompleteModelCoverage {
				t.Fatalf("wallet-4 run = %#v, want coverage-blocked without a substitute", run)
			}
		case "wallet-2":
			if run.LeaderGated != 1 {
				t.Fatalf("wallet-2 run = %#v, want its run-2 prediction held by the leader gate", run)
			}
			// Waiting for the leader is not a coverage incident: the model did
			// deliver, so entries stay enabled with no block reason.
			if !run.EntrySubmissionEnabled || run.EntryBlockReason != "" {
				t.Fatalf("wallet-2 run = %#v, want entries enabled while only the leader gate holds", run)
			}
		}
	}
}

// TestRunLeaderGateHoldsFollowerUntilLeaderArrives: a follower's prediction is
// released in the same cycle as the leader's prediction of that Sandbox and is
// held (neither sent nor captured) while the leader's has not arrived.
func TestRunLeaderGateHoldsFollowerUntilLeaderArrives(t *testing.T) {
	fixture := newQueueFixture([]domain.StrategyExecutionBinding{
		brierBinding("main", "v4.1 flash", 1),
		brierBinding("wallet-1", "qwen3.8-27b", 1),
	}, []domain.Prediction{
		queuePrediction("v4.1 flash", 1, "sandbox-1", 1, 0.6),
		queuePrediction("qwen3.8-27b", 1, "sandbox-1", 1, 0.55),
		queuePrediction("qwen3.8-27b", 1, "sandbox-2", 2, 0.65),
	}, 1, 2)
	result := fixture.run(t)
	requests := requestsByAccount(fixture.strategy)
	if got := predictionIDs(requests["wallet-1"]); got != "pred-qwen3.8-27b-1-sandbox-1" {
		t.Fatalf("wallet-1 predictions = %s, want only sandbox-1", got)
	}
	if run := runFor(t, result, "wallet-1"); run.LeaderGated != 1 || run.PredictionCount != 1 || !run.EntrySubmissionEnabled {
		t.Fatalf("wallet-1 run = %#v", run)
	}
	if run := runFor(t, result, "main"); run.LeaderGated != 0 {
		t.Fatalf("leader run = %#v, a leader is never gated", run)
	}
	for _, batch := range fixture.snapshots.batches {
		for _, snapshot := range batch.Snapshots {
			if batch.ExecutionAccountID == "wallet-1" && snapshot.MarketID == "market-2" {
				t.Fatal("a held prediction's books were captured")
			}
		}
	}
	// The leader's prediction of sandbox-2 arrives: released in that cycle.
	arrived := newQueueFixture(fixture.params.Bindings, []domain.Prediction{
		queuePrediction("v4.1 flash", 1, "sandbox-1", 1, 0.6),
		queuePrediction("v4.1 flash", 1, "sandbox-2", 2, 0.6),
		queuePrediction("qwen3.8-27b", 1, "sandbox-1", 1, 0.55),
		queuePrediction("qwen3.8-27b", 1, "sandbox-2", 2, 0.65),
	}, 1, 2)
	result = arrived.run(t)
	if got := predictionIDs(requestsByAccount(arrived.strategy)["wallet-1"]); got != "pred-qwen3.8-27b-1-sandbox-1,pred-qwen3.8-27b-1-sandbox-2" {
		t.Fatalf("wallet-1 predictions = %s, want both Sandboxes", got)
	}
	if run := runFor(t, result, "wallet-1"); run.LeaderGated != 0 {
		t.Fatalf("wallet-1 run = %#v", run)
	}
}

// TestRunLeaderGateUsesSellOnlyOrPausedLeader: whether the leader may buy, or
// is sent to the strategy at all, does not matter; its prediction still gates
// the followers, even when no buying account uses the leader's model.
func TestRunLeaderGateUsesSellOnlyOrPausedLeader(t *testing.T) {
	for name, configure := range map[string]func(*Params){
		"sell-only leader": func(params *Params) { params.EntryDisabledAccounts = []string{"main"} },
		"paused leader": func(params *Params) {
			params.EntryDisabledAccounts = []string{"main"}
			params.StrategyDisabledAccounts = []string{"main"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newQueueFixture([]domain.StrategyExecutionBinding{
				brierBinding("main", "v4.1 flash", 1),
				brierBinding("wallet-1", "qwen3.8-27b", 1),
			}, []domain.Prediction{
				queuePrediction("v4.1 flash", 1, "sandbox-1", 1, 0.6),
				queuePrediction("qwen3.8-27b", 1, "sandbox-1", 1, 0.55),
				queuePrediction("qwen3.8-27b", 1, "sandbox-2", 2, 0.65),
			}, 1, 2)
			configure(&fixture.params)
			result := fixture.run(t)
			if got := predictionIDs(requestsByAccount(fixture.strategy)["wallet-1"]); got != "pred-qwen3.8-27b-1-sandbox-1" {
				t.Fatalf("wallet-1 predictions = %s, want only the Sandbox the leader predicted", got)
			}
			if run := runFor(t, result, "wallet-1"); run.LeaderGated != 1 {
				t.Fatalf("wallet-1 run = %#v", run)
			}
		})
	}
}

// TestRunLeaderGateOnlyWaitsForTheGroupLeader: followers do not wait for each
// other, and another strategy group has its own leader.
func TestRunLeaderGateOnlyWaitsForTheGroupLeader(t *testing.T) {
	bindings := []domain.StrategyExecutionBinding{
		brierBinding("main", "v4.1 flash", 1),
		{PredictionModelID: "v4.1 flash", ModelID: "v4.1 flash", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet-7"},
		brierBinding("wallet-1", "qwen3.8-27b", 1),
		brierBinding("wallet-2", "v4.1 flash", 2),
		{PredictionModelID: "qwen3.8-27b", ModelID: "qwen3.8-27b", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet-6"},
	}
	fixture := newQueueFixture(bindings, []domain.Prediction{
		// main predicted both Sandboxes; wallet-1 (an earlier follower) only
		// sandbox-2. wallet-2 still gets sandbox-1.
		queuePrediction("v4.1 flash", 1, "sandbox-1", 1, 0.6),
		queuePrediction("v4.1 flash", 1, "sandbox-2", 2, 0.6),
		queuePrediction("qwen3.8-27b", 1, "sandbox-2", 2, 0.55),
		queuePrediction("v4.1 flash", 2, "sandbox-1", 1, 0.62),
	}, 1, 2)
	result := fixture.run(t)
	requests := requestsByAccount(fixture.strategy)
	if got := predictionIDs(requests["wallet-2"]); got != "pred-v4.1_flash-2-sandbox-1" {
		t.Fatalf("wallet-2 predictions = %s, want sandbox-1 without waiting for wallet-1", got)
	}
	if got := predictionIDs(requests["wallet-1"]); got != "pred-qwen3.8-27b-1-sandbox-2" {
		t.Fatalf("wallet-1 predictions = %s", got)
	}
	// wallet-6 follows wallet-7 (multfactor_v2), which predicted both, so
	// nothing is held even though wallet-6 has no prediction of its own for
	// sandbox-1.
	if got := predictionIDs(requests["wallet-6"]); got != "pred-qwen3.8-27b-1-sandbox-2" {
		t.Fatalf("wallet-6 predictions = %s", got)
	}
	for _, run := range result.Runs {
		if run.LeaderGated != 0 {
			t.Fatalf("run %s held %d predictions, want none", run.Context.ExecutionAccountID, run.LeaderGated)
		}
	}
	// The PM lane ran in configuration order.
	order := make([]string, 0, len(fixture.strategy.requests))
	for _, request := range fixture.strategy.requests {
		order = append(order, request.Context.ExecutionAccountID)
	}
	if got := strings.Join(order, ","); got != "main,wallet-7,wallet-1,wallet-2,wallet-6" {
		t.Fatalf("execution order = %s, want the configuration order", got)
	}
}

// TestRunLeaderGateWithoutGroupChangesNothing: a group of one account and
// accounts with different strategies never gate anything.
func TestRunLeaderGateWithoutGroupChangesNothing(t *testing.T) {
	leaders := groupLeaders([]domain.StrategyExecutionBinding{
		brierBinding("main", "v4.1 flash", 1),
		{ModelID: "m", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet-7"},
		{ModelID: "m", StrategyID: "brier_mid_edge_once", ExecutionAccountID: "kalshi:main", MarketSource: domain.MarketSourceKalshi},
		brierBinding("wallet-1", "qwen3.8-27b", 1),
	})
	if len(leaders) != 1 || leaders["wallet-1"].ExecutionAccountID != "main" {
		t.Fatalf("leaders = %#v, want only wallet-1 following main (Kalshi is its own group)", leaders)
	}
}

// TestRunReplayReusesStoredBooks: a replay of the boundary reuses the input
// frozen by the first attempt, including its books, and captures nothing.
func TestRunReplayReusesStoredBooks(t *testing.T) {
	fixture := newQueueFixture([]domain.StrategyExecutionBinding{
		brierBinding("main", "v4.1 flash", 1),
	}, []domain.Prediction{queuePrediction("v4.1 flash", 1, "sandbox-1", 1, 0.6)}, 1)
	first := fixture.run(t)
	captures := len(fixture.books.calls)
	// The venue now shows different books; the replay must not see them.
	fixture.books.books[0].Bids = []domain.PriceLevel{{Price: "0.30", Size: "1"}}
	second := fixture.run(t)
	if len(fixture.books.calls) != captures || len(fixture.snapshots.batches) != 1 {
		t.Fatalf("replay captured books: captures %d->%d, batches %d", captures, len(fixture.books.calls), len(fixture.snapshots.batches))
	}
	if second.Runs[0].Request.InputID != first.Runs[0].Request.InputID ||
		second.Runs[0].Request.OrderBooks[0].Bids[0].Price != "0.48" {
		t.Fatalf("replayed input = %#v, want the stored input and books", second.Runs[0].Request)
	}
}

// TestRunProductionTopologyWithoutRunsKeepsEveryPrediction: the current five
// accounts without prediction_run, in the new configuration order, get every
// prediction they got before (each leader predicted every Sandbox), and each
// account captures only its own books.
func TestRunProductionTopologyWithoutRunsKeepsEveryPrediction(t *testing.T) {
	bindings := []domain.StrategyExecutionBinding{
		{PredictionModelID: "v4.1 flash", ModelID: "v4.1 flash", StrategyID: "brier_mid_edge_once", ExecutionAccountID: "main"},
		{PredictionModelID: "v4.1 flash", ModelID: "v4.1 flash", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet-7"},
		{PredictionModelID: "qwen3.8-27b", ModelID: "qwen3.8-27b", StrategyID: "brier_mid_edge_once", ExecutionAccountID: "wallet-1"},
		{PredictionModelID: "qwen3.8-27b", ModelID: "qwen3.8-27b", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet-6"},
	}
	predictions := []domain.Prediction{
		queuePrediction("v4.1 flash", 0, "sandbox-1", 1, 0.6),
		queuePrediction("v4.1 flash", 0, "sandbox-2", 2, 0.6),
		queuePrediction("qwen3.8-27b", 0, "sandbox-1", 1, 0.55),
		queuePrediction("qwen3.8-27b", 0, "sandbox-2", 2, 0.65),
	}
	fixture := newQueueFixture(bindings, predictions, 1, 2)
	result := fixture.run(t)
	requests := requestsByAccount(fixture.strategy)
	for _, binding := range bindings {
		want := predictionsForBinding(predictions, binding.Normalize())
		request := requests[binding.ExecutionAccountID]
		if len(request.Predictions) != 2 || len(want) != 2 {
			t.Fatalf("%s predictions = %s, want both Sandboxes", binding.ExecutionAccountID, predictionIDs(request))
		}
		if len(request.OrderBooks) != 4 {
			t.Fatalf("%s books = %d, want its four tokens", binding.ExecutionAccountID, len(request.OrderBooks))
		}
	}
	for _, run := range result.Runs {
		if run.Error != nil || run.LeaderGated != 0 || !run.EntrySubmissionEnabled {
			t.Fatalf("run %s = %#v", run.Context.ExecutionAccountID, run)
		}
	}
	if len(fixture.snapshots.batches) != len(bindings) {
		t.Fatalf("batches = %d, want one per account", len(fixture.snapshots.batches))
	}
}

// TestOrderBookSnapshotBatchIdentityIncludesAccount: two accounts that
// captured identical books still get distinct snapshot_set_id values, so their
// batches never collide on the unique set id.
func TestOrderBookSnapshotBatchIdentityIncludesAccount(t *testing.T) {
	books, err := alignBooks(
		[]domain.BookTarget{{MarketID: "market-1", ConditionID: "condition-1", OutcomeIndex: 0, TokenID: "yes-1"},
			{MarketID: "market-1", ConditionID: "condition-1", OutcomeIndex: 1, TokenID: "no-1"}},
		queueBooks(1), runQueueDecisionAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	first, err := domain.NewOrderBookSnapshotBatch(runQueueDecisionAt, "main", "predsnap-1", len(books), books)
	if err != nil {
		t.Fatal(err)
	}
	second, err := domain.NewOrderBookSnapshotBatch(runQueueDecisionAt, "wallet-1", "predsnap-1", len(books), books)
	if err != nil {
		t.Fatal(err)
	}
	if first.SnapshotSetID == second.SnapshotSetID {
		t.Fatalf("identical books of two accounts share snapshot_set_id %s", first.SnapshotSetID)
	}
	again, err := domain.NewOrderBookSnapshotBatch(runQueueDecisionAt, "main", "predsnap-1", len(books), books)
	if err != nil || again.SnapshotSetID != first.SnapshotSetID {
		t.Fatalf("snapshot_set_id is not deterministic: %v", err)
	}
}
