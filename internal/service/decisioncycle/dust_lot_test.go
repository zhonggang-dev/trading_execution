package decisioncycle

import (
	"context"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// TestRunExcludesDustLotsFromStrategyPositions 验证低于 0.01 股的零头批次不进入策略输入 positions，
// 也不会被当作可卖批次生成 SELL；它留在账本中等待结算赎回。
func TestRunExcludesDustLotsFromStrategyPositions(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	prediction.Model.Name = "echo-producer-v7"
	outcomeIndex := 0
	negRisk := false
	sellable := domain.PositionLot{
		LotID: "lot-main-1", ExecutionAccountID: "main", MarketID: prediction.MarketID,
		ConditionID: prediction.ConditionID, TokenID: prediction.Outcomes[0].TokenID,
		OutcomeIndex: &outcomeIndex, OutcomeName: prediction.Outcomes[0].Name, NegRisk: &negRisk,
		ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV1,
		RemainingShares: "12.50", AverageEntryPrice: "0.40", Status: domain.PositionLotOpen,
		OpenedAt: decisionAt.Add(-49 * time.Hour),
	}
	dust := sellable
	dust.LotID = "lot-main-dust"
	dust.RemainingShares = "0.007057"
	dust.AverageEntryPrice = "0.338341"
	positions := accountPositionSource{"main": {sellable, dust.WithDerivedDust()}}
	books := []domain.OrderBookSnapshot{{
		MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
		OutcomeIndex: 0, TokenID: prediction.Outcomes[0].TokenID, Status: domain.OrderBookStatusOK,
		SourceAt: decisionAt, ObservedAt: decisionAt.Add(time.Second),
		DepthLimit: domain.StrategyOrderBookDepth, TickSize: "0.01", MinOrderSize: "1",
		Bids: []domain.PriceLevel{{Price: "0.49", Size: "20"}},
		Asks: []domain.PriceLevel{{Price: "0.50", Size: "20"}},
	}}
	recorder := &fakeRecorder{}
	executor := &fakeExecutor{}
	strategy := &coverageBuyExitStrategy{}
	service, err := newTestService(Params{
		PredictionSource: fakePredictionSource{snapshot: domain.PredictionSnapshot{
			SchemaVersion: domain.PredictionSnapshotSchemaVersion, SnapshotID: "predsnap-dust",
			DecisionAt: decisionAt, Predictions: []domain.Prediction{prediction},
			ExpectedPredictions: []domain.PredictionExpectation{
				completedPredictionExpectation(prediction, 1, 1),
			},
		}},
		PositionSource: positions, OrderBookSource: &fakeOrderBookSource{books: books},
		Strategy: strategy, Recorder: recorder,
		Executor: executor, SubmitEnabled: true, EntryDisabledAccounts: []string{"main"},
		RequireCompleteModelCoverage: true,
		Bindings: []domain.StrategyExecutionBinding{{
			PredictionModelID: "echo-producer-v7", ModelID: "echo",
			StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "main",
		}},
		Venue: "polymarket-paper", Now: func() time.Time { return decisionAt.Add(2 * time.Second) },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := service.Run(context.Background(), decisionAt); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(strategy.requests) != 1 || len(strategy.requests[0].Positions) != 1 ||
		strategy.requests[0].Positions[0].LotID != "lot-main-1" {
		t.Fatalf("strategy positions = %#v, want only the sellable lot", strategy.requests)
	}
	if len(executor.intents) != 1 || executor.intents[0].TargetLotID != "lot-main-1" {
		t.Fatalf("executed intents = %#v, want one SELL for the sellable lot", executor.intents)
	}
}
