package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

func TestDecisionRecorderPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	decisionAt := time.Date(2026, 8, 19, 9, 20, 0, 0, time.UTC)
	recorder, err := NewDecisionRecorder(db, func() time.Time { return decisionAt.Add(time.Minute) })
	if err != nil {
		t.Fatal(err)
	}
	request := integrationDecisionRequest(t, decisionAt, "snapshot-a", decisionAt.Add(time.Second))

	stored, created, err := recorder.ClaimInput(context.Background(), request)
	if err != nil || !created || stored.InputID != request.InputID {
		t.Fatalf("first ClaimInput() = created %v, input %q, error %v", created, stored.InputID, err)
	}
	retry := integrationDecisionRequest(t, decisionAt, "snapshot-a", decisionAt.Add(2*time.Second))
	stored, created, err = recorder.ClaimInput(context.Background(), retry)
	if err != nil || created || !stored.GeneratedAt.Equal(request.GeneratedAt) {
		t.Fatalf("replayed ClaimInput() = created %v, generated_at %s, error %v", created, stored.GeneratedAt, err)
	}
	conflicting := integrationDecisionRequest(t, decisionAt, "snapshot-b", decisionAt.Add(3*time.Second))
	if _, _, err := recorder.ClaimInput(context.Background(), conflicting); !errors.Is(err, port.ErrDecisionConflict) {
		t.Fatalf("conflicting ClaimInput() error = %v", err)
	}

	response := domain.StrategyDecisionResponse{
		SchemaVersion: domain.StrategyOutputSchemaVersion,
		CycleID:       request.CycleID,
		InputID:       request.InputID,
		Context:       request.Context,
		DecidedAt:     decisionAt.Add(4 * time.Second),
		Evaluations:   []domain.StrategyEvaluation{},
		Exits:         []domain.StrategyExit{},
	}
	output, created, err := recorder.ClaimOutput(context.Background(), response, nil, false)
	if err != nil || !created || output.CycleID != request.CycleID {
		t.Fatalf("first ClaimOutput() = created %v, output %#v, error %v", created, output, err)
	}
	output, created, err = recorder.ClaimOutput(context.Background(), response, nil, false)
	if err != nil || created || !output.DecidedAt.Equal(response.DecidedAt) {
		t.Fatalf("replayed ClaimOutput() = created %v, output %#v, error %v", created, output, err)
	}
	changed := response
	changed.DecidedAt = changed.DecidedAt.Add(time.Second)
	if _, _, err := recorder.ClaimOutput(context.Background(), changed, nil, false); !errors.Is(err, port.ErrDecisionConflict) {
		t.Fatalf("conflicting ClaimOutput() error = %v", err)
	}
	future := response
	future.CycleID = "account-a:20260819T093000Z"
	future.InputID = "strategy-input-future"
	future.DecidedAt = decisionAt.Add(2 * time.Minute)
	if _, _, err := recorder.ClaimOutput(context.Background(), future, nil, false); err == nil {
		t.Fatal("future ClaimOutput() error = nil, want rejection")
	}

	var inputID string
	var hasOutput bool
	if err := db.QueryRow(`
		SELECT input_id, output_payload IS NOT NULL
		FROM strategy_decision_runs WHERE cycle_id=$1`, request.CycleID).Scan(&inputID, &hasOutput); err != nil {
		t.Fatal(err)
	}
	if inputID != request.InputID || !hasOutput {
		t.Fatalf("persisted strategy decision = input %q, output %v", inputID, hasOutput)
	}
	var submissionEnabled bool
	var deliveryCount int
	if err := db.QueryRow(`SELECT order_submission_enabled FROM strategy_decision_runs WHERE cycle_id=$1`, request.CycleID).Scan(&submissionEnabled); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM strategy_order_intent_deliveries WHERE cycle_id=$1`, request.CycleID).Scan(&deliveryCount); err != nil {
		t.Fatal(err)
	}
	if submissionEnabled || deliveryCount != 0 {
		t.Fatalf("dry-run output unexpectedly enabled submission=%v deliveries=%d", submissionEnabled, deliveryCount)
	}
}

func TestDecisionRecorderRejectsOutputWithoutInputPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	recorder, err := NewDecisionRecorder(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	response := domain.StrategyDecisionResponse{
		SchemaVersion: domain.StrategyOutputSchemaVersion,
		CycleID:       "account-a:20260819T092000Z",
		InputID:       "strategy-input-missing",
		Context: domain.StrategyExecutionContext{
			ModelID: "model-a", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "account-a",
		},
		DecidedAt: time.Date(2026, 8, 19, 9, 20, 1, 0, time.UTC),
	}
	if _, _, err := recorder.ClaimOutput(context.Background(), response, nil, false); !errors.Is(err, port.ErrDecisionRunNotFound) {
		t.Fatalf("ClaimOutput() error = %v", err)
	}
}

func TestDecisionRecorderDurableIntentDeliveryPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	decisionAt := time.Date(2026, 8, 19, 10, 20, 0, 0, time.UTC)
	now := decisionAt.Add(time.Minute)
	recorder, err := NewDecisionRecorder(db, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	request := integrationDecisionRequest(t, decisionAt, "snapshot-delivery", decisionAt.Add(time.Second))
	if _, _, err := recorder.ClaimInput(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	response := domain.StrategyDecisionResponse{
		SchemaVersion: domain.StrategyOutputSchemaVersion, CycleID: request.CycleID,
		InputID: request.InputID, Context: request.Context, DecidedAt: decisionAt.Add(2 * time.Second),
		Evaluations: []domain.StrategyEvaluation{}, Exits: []domain.StrategyExit{},
	}
	firstIntent := integrationDecisionIntentWithID(t, request, decisionAt, "decision-z", "z-client-order")
	secondIntent := integrationDecisionIntentWithID(t, request, decisionAt, "decision-a", "a-client-order")
	if _, created, err := recorder.ClaimOutput(context.Background(), response, []domain.OrderIntent{firstIntent, secondIntent}, true); err != nil || !created {
		t.Fatalf("ClaimOutput() = created %v, error %v", created, err)
	}
	if _, _, err := recorder.ClaimOutput(context.Background(), response, []domain.OrderIntent{firstIntent, secondIntent}, false); !errors.Is(err, port.ErrDecisionConflict) {
		t.Fatalf("submission-mode replay error = %v", err)
	}

	claimed, err := recorder.ClaimPendingIntents(context.Background(), request.CycleID, 10)
	if err != nil || len(claimed) != 2 || claimed[0].ClientOrderID != firstIntent.ClientOrderID ||
		claimed[1].ClientOrderID != secondIntent.ClientOrderID || claimed[0].Attempt != 1 ||
		claimed[0].Status != domain.DecisionIntentSubmitting {
		t.Fatalf("claimed deliveries = %#v, error %v", claimed, err)
	}
	if err := recorder.CompleteIntent(context.Background(), firstIntent.ClientOrderID, 2, domain.DecisionIntentCompletion{
		Status: domain.DecisionIntentSubmitted, OrderID: "order-delivery", OrderStatus: domain.OrderStatusAcknowledged,
	}); !errors.Is(err, port.ErrDecisionIntentConflict) {
		t.Fatalf("stale completion error = %v", err)
	}
	if err := recorder.CompleteIntent(context.Background(), firstIntent.ClientOrderID, 1, domain.DecisionIntentCompletion{
		Status: domain.DecisionIntentSubmitted, OrderID: "order-delivery", OrderStatus: domain.OrderStatusAcknowledged,
	}); err != nil {
		t.Fatal(err)
	}
	if err := recorder.CompleteIntent(context.Background(), secondIntent.ClientOrderID, 1, domain.DecisionIntentCompletion{
		Status: domain.DecisionIntentFailed, OrderID: "order-rejected", OrderStatus: domain.OrderStatusRejected,
	}); err != nil {
		t.Fatal(err)
	}
	deliveries, err := recorder.ListIntents(context.Background(), request.CycleID)
	if err != nil || len(deliveries) != 2 || deliveries[0].Status != domain.DecisionIntentSubmitted ||
		deliveries[0].OrderID != "order-delivery" || deliveries[0].Attempt != 1 {
		t.Fatalf("stored deliveries = %#v, error %v", deliveries, err)
	}
	if claimed, err := recorder.ClaimPendingIntents(context.Background(), "", 10); err != nil || len(claimed) != 0 {
		t.Fatalf("terminal delivery reclaimed = %#v, error %v", claimed, err)
	}
}

func integrationDecisionIntent(t *testing.T, request domain.StrategyDecisionRequest, at time.Time) domain.OrderIntent {
	return integrationDecisionIntentWithID(t, request, at, "decision-delivery", "decision-client-delivery")
}

func integrationDecisionIntentWithID(t *testing.T, request domain.StrategyDecisionRequest, at time.Time, signalID, clientOrderID string) domain.OrderIntent {
	t.Helper()
	outcomeIndex := 0
	negRisk := false
	intent, err := (domain.OrderIntentParams{
		ModelID: request.Context.ModelID, StrategyID: request.Context.StrategyID,
		ExecutionAccountID: request.Context.ExecutionAccountID,
		SignalID:           signalID, ClientOrderID: clientOrderID,
		Venue: "polymarket", MarketID: "market-delivery", ConditionID: "condition-delivery",
		OutcomeIndex: &outcomeIndex, OutcomeName: "Yes", TokenID: "token-delivery",
		ExpectedNegRisk: &negRisk, MarketSnapshotAt: &at, SignalAt: &at,
		Side: domain.SideBuy, Type: domain.OrderTypeLimit, Price: "0.50", WorstPrice: "0.50",
		Size: "2", TimeInForce: domain.TimeInForceFOK,
		Metadata: map[string]string{"cycle_id": request.CycleID, "input_id": request.InputID},
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func integrationDecisionRequest(t *testing.T, decisionAt time.Time, snapshotID string, generatedAt time.Time) domain.StrategyDecisionRequest {
	t.Helper()
	request, err := (domain.StrategyDecisionRequestParams{
		CycleID:     "account-a:" + decisionAt.Format("20060102T150405Z"),
		DecisionAt:  decisionAt,
		GeneratedAt: generatedAt,
		Context: domain.StrategyExecutionContext{
			ModelID: "model-a", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "account-a",
		},
		PredictionSnapshotID: snapshotID,
		Predictions:          []domain.Prediction{},
		Positions:            []domain.StrategyPositionLot{},
		OrderBooks:           []domain.OrderBookSnapshot{},
		MidPriceHistories:    []domain.MidPriceHistory{},
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	return request
}
