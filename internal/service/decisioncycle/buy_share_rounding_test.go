package decisioncycle

import (
	"context"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// TestRoundBuySharesUsesHalfUpRounding 验证 BUY shares 按四舍五入变为整数。
func TestRoundBuySharesUsesHalfUpRounding(t *testing.T) {
	tests := []struct {
		input domain.Decimal
		want  domain.Decimal
	}{
		{input: "19.49", want: "19"},
		{input: "19.50", want: "20"},
		{input: "19.96", want: "20"},
		{input: "20", want: "20"},
	}
	for _, test := range tests {
		t.Run(test.input.String(), func(t *testing.T) {
			got, err := roundBuyShares(test.input)
			if err != nil {
				t.Fatalf("roundBuyShares() error = %v", err)
			}
			if !got.Equal(test.want) {
				t.Fatalf("roundBuyShares(%s) = %s, want %s", test.input, got, test.want)
			}
		})
	}
}

// TestBuildEntryIntentRoundsBuySharesBeforeNotionalValidation 验证 BUY 在金额精度校验和构建订单前先四舍五入 shares。
func TestBuildEntryIntentRoundsBuySharesBeforeNotionalValidation(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	request := domain.StrategyDecisionRequest{
		CycleID: "cycle-round-buy",
		Context: domain.StrategyExecutionContext{
			ModelID: "model-1", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "account-1",
		},
		DecisionAt:  decisionAt,
		Predictions: []domain.Prediction{prediction},
		OrderBooks: []domain.OrderBookSnapshot{{
			MarketID: prediction.MarketID, ConditionID: prediction.ConditionID, OutcomeIndex: 0,
			TokenID: prediction.Outcomes[0].TokenID, Status: domain.OrderBookStatusOK,
			SourceAt: decisionAt, MinOrderSize: "5",
			Asks: []domain.PriceLevel{{Price: "0.501", Size: "100"}},
		}},
		ExecutionConstraints: domain.DefaultStrategyExecutionConstraints(),
	}
	evaluation := domain.StrategyEvaluation{
		DecisionID: "decision-round-buy", PredictionID: prediction.PredictionID,
		MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
		OutcomeIndex: 0, TokenID: prediction.Outcomes[0].TokenID,
		Order: &domain.StrategyOrderParams{
			Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.501", Size: "19.96", TimeInForce: domain.TimeInForceFOK,
		},
	}

	intent, err := buildEntryIntent(request, decisionAt.Add(time.Second), evaluation, "polymarket")
	if err != nil {
		t.Fatalf("buildEntryIntent() error = %v", err)
	}
	if !intent.Size.Equal("20") {
		t.Fatalf("intent.Size = %s, want 20", intent.Size)
	}
	if intent.Metadata["strategy_requested_size"] != "19.96" {
		t.Fatalf("strategy_requested_size = %q, want 19.96", intent.Metadata["strategy_requested_size"])
	}
}

func TestKalshiPredictionBuildsVenueIntentButStaysOutOfPolymarketDelivery(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	prediction.MarketSource = domain.MarketSourceKalshi
	prediction.MarketID = "TEST-MARKET"
	prediction.ConditionID = "kalshi:TEST-MARKET"
	prediction.Outcomes[0].OutcomeID = "YES"
	prediction.Outcomes[0].TokenID = "kalshi:TEST-MARKET:YES"
	prediction.Outcomes[1].OutcomeID = "NO"
	prediction.Outcomes[1].TokenID = "kalshi:TEST-MARKET:NO"
	request := domain.StrategyDecisionRequest{
		CycleID: "cycle-kalshi", InputID: "input-kalshi",
		Context: domain.StrategyExecutionContext{
			ModelID: "model-1", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "account-1",
		},
		DecisionAt: decisionAt, Predictions: []domain.Prediction{prediction},
		OrderBooks: []domain.OrderBookSnapshot{{
			MarketSource: domain.MarketSourceKalshi,
			MarketID:     prediction.MarketID, ConditionID: prediction.ConditionID, OutcomeIndex: 0,
			OutcomeID: "YES", TokenID: prediction.Outcomes[0].TokenID, Status: domain.OrderBookStatusOK,
			SourceAt: decisionAt, MinOrderSize: "1", TickSize: "0.01",
			Bids: []domain.PriceLevel{{Price: "0.59", Size: "100"}},
			Asks: []domain.PriceLevel{{Price: "0.60", Size: "100"}},
		}},
		ExecutionConstraints: domain.DefaultStrategyExecutionConstraints(),
	}
	evaluation := domain.StrategyEvaluation{
		DecisionID: "decision-kalshi", PredictionID: prediction.PredictionID,
		MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
		OutcomeIndex: 0, TokenID: prediction.Outcomes[0].TokenID,
		Evidence: domain.StrategyEvidence{Probability: 0.7},
		Order: &domain.StrategyOrderParams{
			Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.60", Size: "2", TimeInForce: domain.TimeInForceFOK,
		},
	}
	intent, err := buildEntryIntent(request, decisionAt.Add(time.Second), evaluation, "polymarket")
	if err != nil {
		t.Fatalf("buildEntryIntent() error = %v", err)
	}
	if intent.Venue != "kalshi" || intent.MarketSource != domain.MarketSourceKalshi || intent.OutcomeID != "YES" {
		t.Fatalf("intent = %#v", intent)
	}
	deliverable, dryRun := submissionIntents([]domain.OrderIntent{intent})
	if len(deliverable) != 0 || len(dryRun) != 1 || dryRun[0].ClientOrderID != intent.ClientOrderID {
		t.Fatalf("submission partition: deliverable=%#v dry-run=%#v", deliverable, dryRun)
	}
}

// TestRoundedBuyUsesEffectiveSizeForExecution 验证持久化和执行链路使用整数化后的 BUY shares。
func TestRoundedBuyUsesEffectiveSizeForExecution(t *testing.T) {
	fixture := newPipelineFixture(t)
	fixture.strategy.response.Evaluations[0].Order.Size = "19.50"

	result, err := fixture.service.Run(context.Background(), fixture.decisionAt)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(result.Runs) != 1 || len(result.Runs[0].Intents) != 1 {
		t.Fatalf("pipeline result = %#v", result)
	}
	order := result.Runs[0].Intents[0].Result.Order
	if !order.Intent.Size.Equal("20") || order.Intent.Metadata["strategy_requested_size"] != "19.50" {
		t.Fatalf("rounded order intent = %#v", order.Intent)
	}
	if order.Status != domain.OrderStatusLive || order.FailureCode != "" {
		t.Fatalf("rounded order = %#v", order)
	}
	if fixture.venue.placeCalls != 1 {
		t.Fatalf("rounded strategy order reached venue %d times, want 1", fixture.venue.placeCalls)
	}
}

// TestValidateStrategyOrderRejectsBuyThatRoundsToZero 验证不会提交四舍五入后为零的 BUY。
func TestValidateStrategyOrderRejectsBuyThatRoundsToZero(t *testing.T) {
	order := domain.StrategyOrderParams{
		Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.50", Size: "0.49", TimeInForce: domain.TimeInForceFOK,
	}
	err := validateStrategyOrder(order, domain.SideBuy, "0.50", domain.OrderBookSnapshot{}, domain.DefaultStrategyExecutionConstraints())
	if err == nil || err.Error() != "BUY order.size rounds below one whole share" {
		t.Fatalf("validateStrategyOrder() error = %v", err)
	}
}

// TestValidateStrategyOrderRejectsRoundedBuyBeyondBestAskLiquidity 验证向上取整不会超过保护价的可见卖盘数量。
func TestValidateStrategyOrderRejectsRoundedBuyBeyondBestAskLiquidity(t *testing.T) {
	order := domain.StrategyOrderParams{
		Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.501", Size: "19.96", TimeInForce: domain.TimeInForceFOK,
	}
	book := domain.OrderBookSnapshot{
		Asks: []domain.PriceLevel{{Price: "0.501", Size: "19.99"}},
	}
	err := validateStrategyOrder(order, domain.SideBuy, "0.501", book, domain.DefaultStrategyExecutionConstraints())
	if err == nil || err.Error() != "rounded BUY order.size exceeds best-ask liquidity" {
		t.Fatalf("validateStrategyOrder() error = %v", err)
	}
}

// TestValidateStrategyOrderSumsDuplicateBestAskLevels 验证同价卖盘档会汇总后再校验整数化数量。
func TestValidateStrategyOrderSumsDuplicateBestAskLevels(t *testing.T) {
	order := domain.StrategyOrderParams{
		Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.50", Size: "19.96", TimeInForce: domain.TimeInForceFOK,
	}
	book := domain.OrderBookSnapshot{
		Asks: []domain.PriceLevel{
			{Price: "0.50", Size: "10"},
			{Price: "0.50", Size: "10"},
			{Price: "0.51", Size: "100"},
		},
	}
	if err := validateStrategyOrder(order, domain.SideBuy, "0.50", book, domain.DefaultStrategyExecutionConstraints()); err != nil {
		t.Fatalf("validateStrategyOrder() error = %v", err)
	}
}

// TestValidateStrategyOrderAllowsFourDecimalBuyNotional 验证 BUY 整数化后允许四位小数的金额。
func TestValidateStrategyOrderAllowsFourDecimalBuyNotional(t *testing.T) {
	order := domain.StrategyOrderParams{
		Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.5001", Size: "18.60", TimeInForce: domain.TimeInForceFOK,
	}
	book := domain.OrderBookSnapshot{
		Asks: []domain.PriceLevel{{Price: "0.5001", Size: "100"}},
	}
	if err := validateStrategyOrder(order, domain.SideBuy, "0.5001", book, domain.DefaultStrategyExecutionConstraints()); err != nil {
		t.Fatalf("validateStrategyOrder() error = %v", err)
	}
}

// TestValidateStrategyOrderRejectsBuyNotionalBeyondFourDecimals 验证 BUY 金额仍拒绝超过四位小数。
func TestValidateStrategyOrderRejectsBuyNotionalBeyondFourDecimals(t *testing.T) {
	order := domain.StrategyOrderParams{
		Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.50001", Size: "18.60", TimeInForce: domain.TimeInForceFOK,
	}
	book := domain.OrderBookSnapshot{
		Asks: []domain.PriceLevel{{Price: "0.50001", Size: "100"}},
	}
	err := validateStrategyOrder(order, domain.SideBuy, "0.50001", book, domain.DefaultStrategyExecutionConstraints())
	if err == nil || err.Error() != "BUY price*size exceeds 4 decimal places" {
		t.Fatalf("validateStrategyOrder() error = %v", err)
	}
}
