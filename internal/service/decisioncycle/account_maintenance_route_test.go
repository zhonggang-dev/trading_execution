package decisioncycle

import (
	"context"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/accountmaintenance"
	"github.com/UniPat-AI/trading_execution/internal/service/executionrouter"
)

// unusedPrimaryExecution satisfies the router; these tests only use the
// router's policies, never its submission path.
type unusedPrimaryExecution struct{}

func (unusedPrimaryExecution) Submit(context.Context, domain.OrderIntent) (port.OrderSubmitResult, error) {
	return port.OrderSubmitResult{}, nil
}
func (unusedPrimaryExecution) Resume(context.Context, string) (domain.Order, error) {
	return domain.Order{}, nil
}
func (unusedPrimaryExecution) Get(context.Context, string) (domain.Order, error) {
	return domain.Order{}, nil
}
func (unusedPrimaryExecution) Refresh(context.Context, string) (domain.Order, error) {
	return domain.Order{}, nil
}
func (unusedPrimaryExecution) Cancel(context.Context, string) (domain.Order, error) {
	return domain.Order{}, nil
}
func (unusedPrimaryExecution) FinalizeCancellation(context.Context, string) (domain.Order, error) {
	return domain.Order{}, nil
}
func (unusedPrimaryExecution) Events(context.Context, string) ([]domain.OrderEvent, error) {
	return nil, nil
}
func (unusedPrimaryExecution) Attempts(context.Context, string) ([]domain.OrderAttempt, error) {
	return nil, nil
}

func maintenanceRouter(t *testing.T, registry *accountmaintenance.Registry) *executionrouter.Service {
	t.Helper()
	router, err := executionrouter.NewWithMaintenance(memory.NewOrderRepository(), unusedPrimaryExecution{}, nil, registry)
	if err != nil {
		t.Fatal(err)
	}
	return router
}

// TestRunSkipsPolymarketAccountInMaintenanceWhileOtherAccountTrades is the
// decision half of C-05 acceptance 1: one Polymarket account in maintenance
// is not sent to the strategy and creates no intent, while the other account
// decides and submits normally, and the cycle reports no error.
func TestRunSkipsPolymarketAccountInMaintenanceWhileOtherAccountTrades(t *testing.T) {
	decisionAt := time.Date(2026, 11, 5, 4, 20, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	strategy := &fakeStrategy{response: domain.StrategyDecisionResponse{
		SchemaVersion: domain.StrategyOutputSchemaVersion,
		DecidedAt:     decisionAt.Add(2 * time.Second),
		Evaluations: []domain.StrategyEvaluation{{
			DecisionID: "decision-submit", PredictionID: prediction.PredictionID,
			MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
			OutcomeIndex: 0, TokenID: prediction.Outcomes[0].TokenID,
			Action: domain.StrategyActionSubmit, ReasonCode: domain.StrategyReasonEntrySignal,
			Evidence: domain.StrategyEvidence{Probability: 0.7, Edge: "0.20", Metrics: map[string]string{
				"best_ask": "0.50", "near_logdiff_usd": "1.2", "rel_spread": "0.04",
				"MOM": "0.01", "MACD_SIGNAL": "0.02",
			}},
			Order: &domain.StrategyOrderParams{
				Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.50",
				Size: "5", TimeInForce: domain.TimeInForceFOK,
			},
		}, {
			DecisionID: "decision-skip", PredictionID: prediction.PredictionID,
			MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
			OutcomeIndex: 1, TokenID: prediction.Outcomes[1].TokenID,
			Action: domain.StrategyActionSkip, ReasonCode: domain.StrategyReasonInvalidBook,
			Evidence: domain.StrategyEvidence{Probability: 0.3},
		}},
	}}
	registry := accountmaintenance.New()
	registry.Enter("account-strategy-off", accountmaintenance.ReasonPreflightCLOBAuth, "401")
	router := maintenanceRouter(t, registry)
	recorder := &fakeRecorder{}
	executor := &fakeExecutor{}
	service, err := newTestService(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion, SnapshotID: "snapshot-maintenance",
			DecisionAt: decisionAt, GeneratedAt: decisionAt.Add(time.Second),
			Predictions:         []domain.Prediction{prediction},
			ExpectedPredictions: []domain.PredictionExpectation{completedPredictionExpectation(prediction, 1, 1)},
		}},
		PositionSource:  accountPositionSource{},
		OrderBookSource: &fakeOrderBookSource{books: polymarketBooks(decisionAt, prediction)},
		Strategy:        strategy, Recorder: recorder, Executor: executor, SubmitEnabled: true,
		SubmissionPolicy: router, RouteInputPolicy: router,
		RequireCompleteModelCoverage: true,
		Bindings:                     strategySwitchTestBindings(prediction),
		Venue:                        "polymarket-paper", Now: func() time.Time { return decisionAt.Add(5 * time.Second) },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, runErr := service.Run(context.Background(), decisionAt)
	if runErr != nil {
		t.Fatalf("Run() error = %v, want a maintenance account to be skipped, not failed", runErr)
	}
	var active, maintenance *BindingRunResult
	for index := range result.Runs {
		switch result.Runs[index].Context.ExecutionAccountID {
		case "account-active":
			active = &result.Runs[index]
		case "account-strategy-off":
			maintenance = &result.Runs[index]
		}
	}
	if active == nil || maintenance == nil {
		t.Fatalf("runs = %#v", result.Runs)
	}
	if !maintenance.RouteUnavailable || maintenance.Error != nil || len(maintenance.Intents) != 0 ||
		maintenance.Request.CycleID != "" {
		t.Fatalf("maintenance run = %#v, want skipped without strategy request or intent", maintenance)
	}
	if active.Error != nil || active.RouteUnavailable || strategy.request.Context.ExecutionAccountID != "account-active" {
		t.Fatalf("active run = %#v, strategy context %#v", active, strategy.request.Context)
	}
	if len(executor.intents) != 1 || executor.intents[0].ExecutionAccountID != "account-active" {
		t.Fatalf("submitted intents = %#v, want only the healthy account", executor.intents)
	}
}

// TestRecoverStartupHoldsMaintenanceAccountDeliveries shows that a frozen
// intent of a maintenance account is held (not submitted, not failed) during
// startup recovery, while the other account's delivery is submitted.
func TestRecoverStartupHoldsMaintenanceAccountDeliveries(t *testing.T) {
	registry := accountmaintenance.New()
	registry.Enter("account-maintenance", accountmaintenance.ReasonStartupReconciliationFailed, "RUNNING")
	router := maintenanceRouter(t, registry)
	held := domain.OrderIntent{ClientOrderID: "client-held", ExecutionAccountID: "account-maintenance",
		Venue: "polymarket", MarketSource: domain.MarketSourcePolymarket}
	healthy := domain.OrderIntent{ClientOrderID: "client-healthy", ExecutionAccountID: "account-active",
		Venue: "polymarket", MarketSource: domain.MarketSourcePolymarket}
	recorder := &fakeRecorder{deliveries: []domain.DecisionIntentDelivery{
		{CycleID: "old-cycle", ClientOrderID: held.ClientOrderID, Intent: held, Status: domain.DecisionIntentSubmitting, Attempt: 1},
		{CycleID: "old-cycle", ClientOrderID: healthy.ClientOrderID, Intent: healthy, Status: domain.DecisionIntentSubmitting, Attempt: 1},
	}}
	executor := &fakeExecutor{}
	service := &Service{
		recorder: recorder, executor: executor, submissionPolicy: router, submitEnabled: true,
		activeExecutionAccountIDs:       []string{"account-active", "account-maintenance"},
		entryEnabledExecutionAccountIDs: []string{"account-active", "account-maintenance"},
		now:                             func() time.Time { return time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC) },
	}
	if err := service.RecoverStartup(context.Background()); err != nil {
		t.Fatalf("RecoverStartup() error = %v", err)
	}
	if len(executor.intents) != 1 || executor.intents[0].ClientOrderID != "client-healthy" {
		t.Fatalf("recovered submissions = %#v, want only the healthy account", executor.intents)
	}
	for _, delivery := range recorder.deliveries {
		if delivery.ClientOrderID == "client-held" && delivery.Status != domain.DecisionIntentSubmitting {
			t.Fatalf("held delivery was completed: %#v", delivery)
		}
	}
}
