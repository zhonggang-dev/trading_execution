package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/decisioncycle"
)

// These tests run the decision cycle against the real decision and snapshot
// recorders, so first-writer idempotency and the stored audit trail are the
// PostgreSQL ones. Prediction, positions, books, strategy and execution are
// fakes.

var isolationPGDecisionAt = time.Date(2026, 11, 5, 3, 10, 0, 0, time.UTC)

const isolationPGKalshiAccount = "kalshi:main"

func isolationPGBindings() []domain.StrategyExecutionBinding {
	return []domain.StrategyExecutionBinding{
		{ModelID: "model-a", StrategyID: "brier_mid_edge_once", ExecutionAccountID: "main"},
		{ModelID: "model-a", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "wallet-6"},
		{ModelID: "model-a", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet-7"},
		{ModelID: "model-a", StrategyID: "brier_mid_edge_once", ExecutionAccountID: isolationPGKalshiAccount, MarketSource: domain.MarketSourceKalshi},
	}
}

func isolationPGPrediction() domain.Prediction {
	return domain.Prediction{
		PredictionID: "pred-1", SourceJobID: "job-1", MarketID: "market-1", ConditionID: "condition-1",
		Question: "Will it happen?", Domains: []string{"World/Geopolitics"},
		Outcomes: []domain.PredictionOutcome{
			{Index: 0, Name: "Yes", TokenID: "yes-token", Probability: 0.7},
			{Index: 1, Name: "No", TokenID: "no-token", Probability: 0.3},
		},
		PredictionAsOf: isolationPGDecisionAt.Add(-30 * time.Minute),
		CompletedAt:    isolationPGDecisionAt.Add(-10 * time.Minute),
		AvailableAt:    isolationPGDecisionAt.Add(-9 * time.Minute),
		Model:          domain.PredictionModel{Name: "model-a"},
	}
}

func isolationPGSnapshot() domain.PredictionSnapshot {
	prediction := isolationPGPrediction()
	available := prediction.AvailableAt
	return domain.PredictionSnapshot{
		SchemaVersion: domain.PredictionSnapshotSchemaVersion, SnapshotID: "predsnap-0123456789abcdef",
		DecisionAt: isolationPGDecisionAt, CompletedAfter: isolationPGDecisionAt.Add(-3 * time.Hour),
		GeneratedAt: isolationPGDecisionAt.Add(time.Second),
		Predictions: []domain.Prediction{prediction},
		ExpectedPredictions: []domain.PredictionExpectation{{
			PredictionID: prediction.PredictionID, SourceJobID: prediction.SourceJobID,
			PredictionModelID: prediction.Model.Name, SelectionID: 1, SelectionRunID: 1,
			MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
			Outcomes:       append([]domain.PredictionOutcome(nil), prediction.Outcomes...),
			PredictionAsOf: prediction.PredictionAsOf, TaskAvailableAt: prediction.PredictionAsOf,
			Status: domain.PredictionExpectationCompleted, ResultAvailableAt: &available,
		}},
	}
}

func isolationPGLot(account, lotID, marketID, conditionID, tokenID string, outcomeIndex int) domain.PositionLot {
	negRisk := false
	return domain.PositionLot{
		LotID: lotID, ExecutionAccountID: account, MarketID: marketID, ConditionID: conditionID,
		TokenID: tokenID, OutcomeIndex: &outcomeIndex, OutcomeName: "Yes", NegRisk: &negRisk,
		ModelID: "model-a", StrategyID: "opened-by-another-strategy",
		RemainingShares: "10", AverageEntryPrice: "0.40", Status: domain.PositionLotOpen,
		OpenedAt: isolationPGDecisionAt.Add(-49 * time.Hour),
	}
}

type isolationPGPositions map[string][]domain.PositionLot

func (positions isolationPGPositions) ListOpenLots(_ context.Context, binding domain.StrategyExecutionContext) ([]domain.PositionLot, error) {
	return positions[binding.ExecutionAccountID], nil
}

func newIsolationPGPositions() isolationPGPositions {
	return isolationPGPositions{
		"main":     {isolationPGLot("main", "lot-main", "market-1", "condition-1", "yes-token", 0)},
		"wallet-6": {isolationPGLot("wallet-6", "lot-w6", "market-1", "condition-1", "no-token", 1)},
		"wallet-7": {isolationPGLot("wallet-7", "lot-w7", "market-1", "condition-1", "yes-token", 0)},
		isolationPGKalshiAccount: {
			isolationPGLot(isolationPGKalshiAccount, "lot-kalshi", "KXLOT", "kalshi:KXLOT", "kalshi:KXLOT:YES", 0),
		},
	}
}

type isolationPGPredictions struct {
	snapshot domain.PredictionSnapshot
	err      error
}

func (source *isolationPGPredictions) Snapshot(context.Context, time.Time, time.Duration) (domain.PredictionSnapshot, error) {
	return source.snapshot, source.err
}

// isolationPGBooks returns an OK book for every Polymarket target; Kalshi
// targets are left to the cycle (MISSING).
type isolationPGBooks struct{}

func (isolationPGBooks) Capture(_ context.Context, decisionAt time.Time, targets []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	books := make([]domain.OrderBookSnapshot, 0, len(targets))
	for _, target := range targets {
		if target.MarketSource.Normalize() != domain.MarketSourcePolymarket {
			continue
		}
		books = append(books, domain.OrderBookSnapshot{
			MarketID: target.MarketID, ConditionID: target.ConditionID, OutcomeIndex: target.OutcomeIndex,
			TokenID: target.TokenID, Status: domain.OrderBookStatusOK,
			SourceAt: decisionAt, ObservedAt: decisionAt.Add(time.Second), DepthLimit: domain.StrategyOrderBookDepth,
			TickSize: "0.01", MinOrderSize: "1",
			Bids: []domain.PriceLevel{{Price: "0.48", Size: "20"}}, Asks: []domain.PriceLevel{{Price: "0.50", Size: "20"}},
		})
	}
	return books, nil
}

// isolationPGStrategy skips every outcome and exits every lot.
type isolationPGStrategy struct {
	calls []string
}

func (strategy *isolationPGStrategy) Decide(_ context.Context, request domain.StrategyDecisionRequest) (domain.StrategyDecisionResponse, error) {
	strategy.calls = append(strategy.calls, request.Context.ExecutionAccountID)
	evaluations := make([]domain.StrategyEvaluation, 0)
	for _, prediction := range request.Predictions {
		for _, outcome := range prediction.Outcomes {
			evaluations = append(evaluations, domain.StrategyEvaluation{
				DecisionID:   request.CycleID + ":" + outcome.TokenID,
				PredictionID: prediction.PredictionID, MarketID: prediction.MarketID,
				ConditionID: prediction.ConditionID, OutcomeIndex: outcome.Index, TokenID: outcome.TokenID,
				Action: domain.StrategyActionSkip, ReasonCode: domain.StrategyReasonEdgeTooLow,
				Evidence: domain.StrategyEvidence{Probability: outcome.Probability},
			})
		}
	}
	exits := make([]domain.StrategyExit, 0, len(request.Positions))
	for _, position := range request.Positions {
		exits = append(exits, domain.StrategyExit{
			DecisionID: request.CycleID + ":exit:" + position.LotID, LotID: position.LotID, TokenID: position.TokenID,
			ReasonCode: domain.StrategyReasonHold48H,
			Order: &domain.StrategyOrderParams{
				Side: domain.SideSell, Type: domain.OrderTypeLimit, WorstPrice: "0.40",
				Size: position.Shares, TimeInForce: domain.TimeInForceFOK,
			},
		})
	}
	return domain.StrategyDecisionResponse{
		SchemaVersion: domain.StrategyOutputSchemaVersion, CycleID: request.CycleID, InputID: request.InputID,
		Context: request.Context, DecidedAt: request.DecisionAt.Add(time.Second),
		Evaluations: evaluations, Exits: exits,
	}, nil
}

type isolationPGExecutor struct {
	rejected map[string]bool
}

func (executor isolationPGExecutor) Submit(_ context.Context, intent domain.OrderIntent) (port.OrderSubmitResult, error) {
	status := domain.OrderStatusAcknowledged
	if executor.rejected[intent.ClientOrderID] {
		status = domain.OrderStatusRejected
	}
	return port.OrderSubmitResult{Order: domain.Order{ID: "order-" + intent.ClientOrderID, Intent: intent, Status: status}}, nil
}

type isolationPGFixture struct {
	db          *sql.DB
	predictions *isolationPGPredictions
	positions   isolationPGPositions
	strategy    *isolationPGStrategy
	executor    isolationPGExecutor
	recorder    *DecisionRecorder
}

func newIsolationPGFixture(t *testing.T) *isolationPGFixture {
	t.Helper()
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	recorder, err := NewDecisionRecorder(db, func() time.Time { return isolationPGDecisionAt.Add(30 * time.Second) })
	if err != nil {
		t.Fatal(err)
	}
	return &isolationPGFixture{
		db: db, predictions: &isolationPGPredictions{snapshot: isolationPGSnapshot()},
		positions: newIsolationPGPositions(), strategy: &isolationPGStrategy{},
		executor: isolationPGExecutor{rejected: map[string]bool{}}, recorder: recorder,
	}
}

func (fixture *isolationPGFixture) run(t *testing.T) (decisioncycle.RunResult, error) {
	t.Helper()
	snapshots, err := NewOrderBookSnapshotRecorder(fixture.db, nil)
	if err != nil {
		t.Fatal(err)
	}
	service, err := decisioncycle.New(decisioncycle.Params{
		PredictionSource: fixture.predictions, PositionSource: fixture.positions,
		OrderBookSource: isolationPGBooks{}, SnapshotRecorder: snapshots,
		Strategy: fixture.strategy, Recorder: fixture.recorder, Executor: fixture.executor,
		SubmitEnabled: true, RequireCompleteModelCoverage: true,
		Bindings:              isolationPGBindings(),
		PredictionSourceModes: map[string]domain.PredictionSourceMode{"model-a": domain.PredictionSourceModeDirect},
		Venue:                 "polymarket",
		Now:                   func() time.Time { return isolationPGDecisionAt.Add(20 * time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	return service.Run(context.Background(), isolationPGDecisionAt)
}

func isolationPGRunAccounts(t *testing.T, db *sql.DB) map[string]bool {
	t.Helper()
	rows, err := db.Query(`SELECT execution_account_id, output_payload IS NOT NULL FROM strategy_decision_runs WHERE decision_at=$1`, isolationPGDecisionAt)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	accounts := make(map[string]bool)
	for rows.Next() {
		var account string
		var hasOutput bool
		if err := rows.Scan(&account, &hasOutput); err != nil {
			t.Fatal(err)
		}
		accounts[account] = hasOutput
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return accounts
}

func TestDecisionCycleIsolationBrokenLotPostgresIntegration(t *testing.T) {
	fixture := newIsolationPGFixture(t)
	fixture.positions[isolationPGKalshiAccount][0].TokenID = "kalshi:OTHER:YES"
	result, err := fixture.run(t)
	if err == nil {
		t.Fatal("Run() error = nil, want the excluded Kalshi account reported")
	}
	accounts := isolationPGRunAccounts(t, fixture.db)
	for _, account := range []string{"main", "wallet-6", "wallet-7"} {
		if hasOutput, exists := accounts[account]; !exists || !hasOutput {
			t.Fatalf("decision runs = %#v, want input and output for %s", accounts, account)
		}
	}
	if _, exists := accounts[isolationPGKalshiAccount]; exists {
		t.Fatalf("decision runs = %#v, the excluded Kalshi account must not have an input", accounts)
	}
	var batches int
	if err := fixture.db.QueryRow(`SELECT count(*) FROM strategy_orderbook_snapshot_batches WHERE decision_at=$1`, isolationPGDecisionAt).Scan(&batches); err != nil || batches != 1 {
		t.Fatalf("snapshot batches = %d, error %v", batches, err)
	}
	for _, run := range result.Runs {
		if run.Context.ExecutionAccountID == isolationPGKalshiAccount && (!run.InputUnavailable || run.FailureStage != decisioncycle.FailureStageTargets) {
			t.Fatalf("Kalshi run = %#v", run)
		}
	}
}

func TestDecisionCycleIsolationExitOnlyReplayPostgresIntegration(t *testing.T) {
	fixture := newIsolationPGFixture(t)
	fixture.predictions.err = fmt.Errorf("%w: prediction snapshot HTTP 503: UNAVAILABLE: down", port.ErrPredictionSourceUnavailable)
	if _, err := fixture.run(t); !errors.Is(err, port.ErrPredictionSourceUnavailable) {
		t.Fatalf("exit-only Run() error = %v", err)
	}
	wantSnapshotID := "predsnap-unavailable-20261105T031000Z"
	var batchSnapshotID string
	if err := fixture.db.QueryRow(`SELECT prediction_snapshot_id FROM strategy_orderbook_snapshot_batches WHERE decision_at=$1`, isolationPGDecisionAt).Scan(&batchSnapshotID); err != nil || batchSnapshotID != wantSnapshotID {
		t.Fatalf("batch prediction_snapshot_id = %q, error %v", batchSnapshotID, err)
	}
	var blocked, inputs int
	if err := fixture.db.QueryRow(`
		SELECT count(*) FILTER (WHERE output_payload->'entry_policy'->>'block_reason' = $2
		                          AND input_payload->>'prediction_snapshot_id' = $3),
		       count(*)
		FROM strategy_decision_runs WHERE decision_at=$1`,
		isolationPGDecisionAt, domain.StrategyEntryBlockPredictionSourceUnavailable, wantSnapshotID,
	).Scan(&blocked, &inputs); err != nil || inputs != 4 || blocked != 4 {
		t.Fatalf("decision runs blocked=%d inputs=%d error=%v, want all four exit-only", blocked, inputs, err)
	}
	countDeliveries := func(side domain.Side) int {
		var count int
		if err := fixture.db.QueryRow(`
			SELECT count(*) FROM strategy_order_intent_deliveries d
			JOIN strategy_decision_runs r ON r.cycle_id = d.cycle_id
			WHERE r.decision_at=$1 AND d.intent_payload->>'side' = $2`, isolationPGDecisionAt, string(side)).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	if sells, buys := countDeliveries(domain.SideSell), countDeliveries(domain.SideBuy); sells != 3 || buys != 0 {
		t.Fatalf("deliveries SELL=%d BUY=%d, want three Polymarket exits and no BUY", sells, buys)
	}

	// The same boundary replayed with predictions available again.
	calls := len(fixture.strategy.calls)
	fixture.predictions.err = nil
	if _, err := fixture.run(t); !errors.Is(err, port.ErrOrderBookSnapshotConflict) {
		t.Fatalf("replay Run() error = %v, want the snapshot batch conflict", err)
	}
	if len(fixture.strategy.calls) != calls || countDeliveries(domain.SideBuy) != 0 {
		t.Fatalf("replay called the strategy %d more times or created BUY deliveries", len(fixture.strategy.calls)-calls)
	}
	// Even past the batch claim, the account input with predictions conflicts.
	request, err := (domain.StrategyDecisionRequestParams{
		CycleID: "wallet-6:20261105T031000Z", DecisionAt: isolationPGDecisionAt,
		GeneratedAt: isolationPGDecisionAt.Add(5 * time.Second),
		Context: domain.StrategyExecutionContext{
			ModelID: "model-a", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "wallet-6",
		},
		PredictionSnapshotID: isolationPGSnapshot().SnapshotID,
		Predictions:          []domain.Prediction{isolationPGPrediction()},
		Positions:            []domain.StrategyPositionLot{},
		OrderBooks:           []domain.OrderBookSnapshot{},
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := fixture.recorder.ClaimInput(context.Background(), request); !errors.Is(err, port.ErrDecisionConflict) {
		t.Fatalf("ClaimInput() with predictions error = %v, want the first-writer conflict", err)
	}
}

func TestDecisionCycleIsolationRecoveryRejectionPostgresIntegration(t *testing.T) {
	fixture := newIsolationPGFixture(t)
	earlier := isolationPGDecisionAt.Add(-10 * time.Minute)
	request := integrationDecisionRequestForAccount(t, earlier, "predsnap-earlier", earlier.Add(time.Second), "wallet-6")
	if _, _, err := fixture.recorder.ClaimInput(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	intent := integrationDecisionIntentWithID(t, request, earlier, "old-decision", "old-client")
	response := domain.StrategyDecisionResponse{
		SchemaVersion: domain.StrategyOutputSchemaVersion, CycleID: request.CycleID, InputID: request.InputID,
		Context: request.Context, DecidedAt: earlier.Add(2 * time.Second),
		Evaluations: []domain.StrategyEvaluation{}, Exits: []domain.StrategyExit{},
	}
	if _, _, err := fixture.recorder.ClaimOutput(context.Background(), response, []domain.OrderIntent{intent}, true); err != nil {
		t.Fatal(err)
	}
	fixture.executor.rejected["old-client"] = true
	result, err := fixture.run(t)
	if err == nil || result.Recovery.Rejected != 1 {
		t.Fatalf("Run() recovery = %#v, error %v, want the rejected delivery reported", result.Recovery, err)
	}
	var status string
	if err := fixture.db.QueryRow(`SELECT status FROM strategy_order_intent_deliveries WHERE client_order_id='old-client'`).Scan(&status); err != nil || status != string(domain.DecisionIntentFailed) {
		t.Fatalf("recovered delivery status = %q, error %v", status, err)
	}
	accounts := isolationPGRunAccounts(t, fixture.db)
	for _, account := range []string{"main", "wallet-6", "wallet-7", isolationPGKalshiAccount} {
		if hasOutput, exists := accounts[account]; !exists || !hasOutput {
			t.Fatalf("decision runs = %#v, want input and output for %s", accounts, account)
		}
	}
	for _, run := range result.Runs {
		if run.Error != nil {
			t.Fatalf("run %s failed after a rejected recovery: %v", run.Context.ExecutionAccountID, run.Error)
		}
	}
}
