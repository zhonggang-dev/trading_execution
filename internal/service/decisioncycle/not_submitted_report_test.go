package decisioncycle

import (
	"context"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

type notSubmittedCall struct {
	execution domain.StrategyExecutionContext
	decisions []domain.StrategyUndeliveredDecision
	code      string
}

type fakeNotSubmittedReporter struct{ calls []notSubmittedCall }

func (reporter *fakeNotSubmittedReporter) ReportNotSubmitted(
	_ context.Context, execution domain.StrategyExecutionContext,
	decisions []domain.StrategyUndeliveredDecision, failureCode, _ string,
) {
	reporter.calls = append(reporter.calls, notSubmittedCall{execution, decisions, failureCode})
}

// runSingleSubmit drives one cycle in which the strategy answers with one
// SUBMIT (probability is configurable so the response can be made invalid).
func runSingleSubmit(t *testing.T, submitEnabled bool, probability float64) (*fakeNotSubmittedReporter, *fakeExecutor, error) {
	t.Helper()
	decisionAt := time.Date(2026, 9, 2, 4, 20, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	books := make([]domain.OrderBookSnapshot, 0, len(prediction.Outcomes))
	for _, outcome := range prediction.Outcomes {
		books = append(books, domain.OrderBookSnapshot{
			MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
			OutcomeIndex: outcome.Index, TokenID: outcome.TokenID, Status: domain.OrderBookStatusOK,
			SourceAt: decisionAt, ObservedAt: decisionAt.Add(time.Second),
			DepthLimit: domain.StrategyOrderBookDepth, TickSize: "0.01", MinOrderSize: "1",
			Bids: []domain.PriceLevel{{Price: "0.48", Size: "20"}},
			Asks: []domain.PriceLevel{{Price: "0.50", Size: "20"}},
		})
	}
	strategy := &fakeStrategy{response: domain.StrategyDecisionResponse{
		SchemaVersion: domain.StrategyOutputSchemaVersion,
		DecidedAt:     decisionAt.Add(2 * time.Second),
		Evaluations: []domain.StrategyEvaluation{
			{
				DecisionID: "decision-submit", PredictionID: prediction.PredictionID,
				MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
				OutcomeIndex: 0, TokenID: prediction.Outcomes[0].TokenID,
				Action: domain.StrategyActionSubmit, ReasonCode: domain.StrategyReasonEntrySignal,
				Evidence: domain.StrategyEvidence{Probability: probability, Edge: "0.20", Metrics: map[string]string{
					"best_ask": "0.50", "near_logdiff_usd": "1.2", "rel_spread": "0.04",
					"MOM": "0.01", "MACD_SIGNAL": "0.02",
				}},
				Order: &domain.StrategyOrderParams{
					Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.50",
					Size: "5", TimeInForce: domain.TimeInForceFOK,
				},
			},
			{
				DecisionID: "decision-skip", PredictionID: prediction.PredictionID,
				MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
				OutcomeIndex: 1, TokenID: prediction.Outcomes[1].TokenID,
				Action: domain.StrategyActionSkip, ReasonCode: domain.StrategyReasonInvalidBook,
				Evidence: domain.StrategyEvidence{Probability: 0.3},
			},
		},
	}}
	reporter := &fakeNotSubmittedReporter{}
	executor := &fakeExecutor{}
	service, err := newTestService(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion, SnapshotID: "snapshot-report",
			DecisionAt: decisionAt, GeneratedAt: decisionAt.Add(time.Second),
			Predictions:         []domain.Prediction{prediction},
			ExpectedPredictions: []domain.PredictionExpectation{completedPredictionExpectation(prediction, 1, 1)},
		}},
		PositionSource: fakePositionSource{}, OrderBookSource: &fakeOrderBookSource{books: books},
		Strategy: strategy, Recorder: &fakeRecorder{}, Executor: executor, SubmitEnabled: submitEnabled,
		RequireCompleteModelCoverage: true, NotSubmittedReporter: reporter,
		Bindings: []domain.StrategyExecutionBinding{{
			PredictionModelID: prediction.Model.Name, ModelID: "test",
			StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "account-active",
		}},
		Venue: "polymarket-paper", Now: func() time.Time { return decisionAt.Add(5 * time.Second) },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, runErr := service.Run(context.Background(), decisionAt)
	return reporter, executor, runErr
}

func TestRunDoesNotReportNotSubmittedWhenOrderIsDelivered(t *testing.T) {
	reporter, executor, err := runSingleSubmit(t, true, 0.7)
	if err != nil || len(executor.intents) != 1 {
		t.Fatalf("Run() error = %v, executed = %d", err, len(executor.intents))
	}
	if len(reporter.calls) != 0 {
		t.Fatalf("NOT_SUBMITTED reports = %#v, want none for a delivered order", reporter.calls)
	}
}

func TestRunReportsEverySubmitWhenResponseIsRejected(t *testing.T) {
	reporter, executor, err := runSingleSubmit(t, true, 0.99)
	if err == nil || len(executor.intents) != 0 {
		t.Fatalf("Run() error = %v, executed = %d, want a rejected response", err, len(executor.intents))
	}
	if len(reporter.calls) != 1 || reporter.calls[0].code != "STRATEGY_RESPONSE_REJECTED" ||
		len(reporter.calls[0].decisions) != 1 || reporter.calls[0].decisions[0].DecisionID != "decision-submit" ||
		reporter.calls[0].execution.ExecutionAccountID != "account-active" {
		t.Fatalf("NOT_SUBMITTED reports = %#v", reporter.calls)
	}
}

func TestRunReportsSubmitWhenOrderSubmissionIsDisabled(t *testing.T) {
	reporter, executor, err := runSingleSubmit(t, false, 0.7)
	if err != nil || len(executor.intents) != 0 {
		t.Fatalf("Run() error = %v, executed = %d", err, len(executor.intents))
	}
	if len(reporter.calls) != 1 || reporter.calls[0].code != "ORDER_SUBMISSION_DISABLED" ||
		len(reporter.calls[0].decisions) != 1 || reporter.calls[0].decisions[0].DecisionID != "decision-submit" {
		t.Fatalf("NOT_SUBMITTED reports = %#v", reporter.calls)
	}
}
