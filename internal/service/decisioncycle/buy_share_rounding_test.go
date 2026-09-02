package decisioncycle

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

type testIntentSubmissionPolicy func(domain.OrderIntent) bool

func (policy testIntentSubmissionPolicy) Enabled(intent domain.OrderIntent) bool {
	return policy(intent)
}

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
			SourceAt: decisionAt, MinOrderSize: "5", TickSize: "0.001",
			Asks: []domain.PriceLevel{
				{Price: "0.500", Size: "5"},
				{Price: "0.501", Size: "95"},
			},
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
	if !intent.Price.Equal("0.501") || !intent.WorstPrice.Equal("0.501") ||
		intent.Metadata["strategy_reference_price"] != "0.500" || intent.Metadata["strategy_worst_price"] != "0.501" {
		t.Fatalf("depth-aware intent price context = %#v", intent)
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
			Bids: []domain.PriceLevel{{Price: "0.37", Size: "100"}},
			Asks: []domain.PriceLevel{
				{Price: "0.39", Size: "7.73"},
				{Price: "0.42", Size: "15"},
				{Price: "0.43", Size: "50"},
			},
		}},
		ExecutionConstraints: domain.DefaultStrategyExecutionConstraints(),
	}
	evaluation := domain.StrategyEvaluation{
		DecisionID: "decision-kalshi", PredictionID: prediction.PredictionID,
		MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
		OutcomeIndex: 0, TokenID: prediction.Outcomes[0].TokenID,
		Evidence: domain.StrategyEvidence{Probability: 0.7},
		Order: &domain.StrategyOrderParams{
			Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.42", Size: "12", TimeInForce: domain.TimeInForceFOK,
		},
	}
	intent, err := buildEntryIntent(request, decisionAt.Add(time.Second), evaluation, "polymarket")
	if err != nil {
		t.Fatalf("buildEntryIntent() error = %v", err)
	}
	if intent.Venue != "kalshi" || intent.MarketSource != domain.MarketSourceKalshi || intent.OutcomeID != "YES" {
		t.Fatalf("intent = %#v", intent)
	}
	if intent.TimeInForce != domain.TimeInForceIOC || intent.Metadata["strategy_time_in_force"] != "FOK" ||
		intent.Metadata["execution_time_in_force"] != "IOC" {
		t.Fatalf("Kalshi execution time-in-force = %#v", intent)
	}
	if !intent.Size.Equal("12") || !intent.WorstPrice.Equal("0.42") || intent.Metadata["strategy_reference_price"] != "0.39" {
		t.Fatalf("sparse-depth intent = %#v", intent)
	}
	deliverable, dryRun := (&Service{}).submissionIntents([]domain.OrderIntent{intent})
	if len(deliverable) != 0 || len(dryRun) != 1 || dryRun[0].ClientOrderID != intent.ClientOrderID {
		t.Fatalf("submission partition: deliverable=%#v dry-run=%#v", deliverable, dryRun)
	}
	liveService := &Service{submissionPolicy: testIntentSubmissionPolicy(func(candidate domain.OrderIntent) bool {
		return candidate.MarketSource.Normalize() == domain.MarketSourceKalshi && candidate.ModelID == "model-1" && candidate.StrategyID == domain.StrategyIDMultfactorV1 && candidate.ExecutionAccountID == "account-1"
	})}
	deliverable, dryRun = liveService.submissionIntents([]domain.OrderIntent{intent})
	if len(deliverable) != 1 || len(dryRun) != 0 {
		t.Fatalf("enabled Kalshi partition: deliverable=%#v dry-run=%#v", deliverable, dryRun)
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
	err := validateStrategyOrder(order, domain.SideBuy, domain.OrderBookSnapshot{}, domain.DefaultStrategyExecutionConstraints())
	if err == nil || err.Error() != "BUY order.size rounds below one whole share" {
		t.Fatalf("validateStrategyOrder() error = %v", err)
	}
}

// TestValidateStrategyOrderRejectsRoundedBuyBeyondBestAskLiquidity 验证向上取整不会超过保护价的可见卖盘数量。
func TestValidateStrategyOrderAllowsRoundedBuyBeyondBestAskLiquidityAsIOC(t *testing.T) {
	order := domain.StrategyOrderParams{
		Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.501", Size: "19.96", TimeInForce: domain.TimeInForceFOK,
	}
	book := domain.OrderBookSnapshot{
		TickSize: "0.001",
		Asks:     []domain.PriceLevel{{Price: "0.501", Size: "19.99"}},
	}
	// Polymarket BUY executes as IOC: the visible 19.99 shares fill and the
	// remainder is cancelled, so a snapshot shortfall is not a rejection.
	if err := validateStrategyOrderForMarket(
		order, domain.SideBuy, book, domain.DefaultStrategyExecutionConstraints(), domain.MarketSourcePolymarket,
	); err != nil {
		t.Fatalf("validateStrategyOrder() error = %v", err)
	}
	book.Asks = []domain.PriceLevel{{Price: "0.502", Size: "19.99"}}
	order.WorstPrice = "0.502"
	book.Asks[0].Price = "0.503"
	err := validateStrategyOrderForMarket(
		order, domain.SideBuy, book, domain.DefaultStrategyExecutionConstraints(), domain.MarketSourcePolymarket,
	)
	if err == nil || !strings.Contains(err.Error(), "best ask") {
		t.Fatalf("unmarketable BUY error = %v", err)
	}
}

// TestValidateStrategyOrderSumsDuplicateBestAskLevels 验证同价卖盘档会汇总后再校验整数化数量。
func TestValidateStrategyOrderSumsDuplicateBestAskLevels(t *testing.T) {
	order := domain.StrategyOrderParams{
		Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.50", Size: "19.96", TimeInForce: domain.TimeInForceFOK,
	}
	book := domain.OrderBookSnapshot{
		TickSize: "0.01",
		Asks: []domain.PriceLevel{
			{Price: "0.50", Size: "10"},
			{Price: "0.50", Size: "10"},
			{Price: "0.51", Size: "100"},
		},
	}
	if err := validateStrategyOrder(order, domain.SideBuy, book, domain.DefaultStrategyExecutionConstraints()); err != nil {
		t.Fatalf("validateStrategyOrder() error = %v", err)
	}
}

// TestValidateStrategyOrderAllowsFourDecimalBuyNotional 验证 BUY 整数化后允许四位小数的金额。
func TestValidateStrategyOrderAllowsFourDecimalBuyNotional(t *testing.T) {
	order := domain.StrategyOrderParams{
		Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.5001", Size: "18.60", TimeInForce: domain.TimeInForceFOK,
	}
	book := domain.OrderBookSnapshot{
		TickSize: "0.0001",
		Asks:     []domain.PriceLevel{{Price: "0.5001", Size: "100"}},
	}
	if err := validateStrategyOrder(order, domain.SideBuy, book, domain.DefaultStrategyExecutionConstraints()); err != nil {
		t.Fatalf("validateStrategyOrder() error = %v", err)
	}
}

// TestValidateStrategyOrderRejectsBuyNotionalBeyondFourDecimals 验证 BUY 金额仍拒绝超过四位小数。
func TestValidateStrategyOrderRejectsBuyNotionalBeyondFourDecimals(t *testing.T) {
	order := domain.StrategyOrderParams{
		Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.50001", Size: "18.60", TimeInForce: domain.TimeInForceFOK,
	}
	book := domain.OrderBookSnapshot{
		TickSize: "0.00001",
		Asks:     []domain.PriceLevel{{Price: "0.50001", Size: "100"}},
	}
	err := validateStrategyOrder(order, domain.SideBuy, book, domain.DefaultStrategyExecutionConstraints())
	if err == nil || err.Error() != "BUY price*size exceeds 4 decimal places" {
		t.Fatalf("validateStrategyOrder() error = %v", err)
	}
}

func TestDefaultStrategyExecutionConstraintsUseDepthAwareLimit(t *testing.T) {
	constraints := domain.DefaultStrategyExecutionConstraints()
	if constraints.PriceProtectionPolicy != domain.StrategyPriceProtectionDepthAwareLimit ||
		constraints.MaxPriceSlippageTicks != 2 || len(constraints.AllowedTimeInForce) != 2 ||
		constraints.AllowedTimeInForce[0] != domain.TimeInForceIOC || constraints.AllowedTimeInForce[1] != domain.TimeInForceFOK {
		t.Fatalf("default execution constraints = %#v", constraints)
	}
	if err := validateExecutionConstraints(constraints); err != nil {
		t.Fatalf("validateExecutionConstraints() error = %v", err)
	}
}

// TestValidateStrategyOrderAllowsDepthAwareBuyWithinTwoTicks verifies that a
// protected BUY can consume cumulative visible ask depth through two ticks.
func TestValidateStrategyOrderAllowsDepthAwareBuyWithinTwoTicks(t *testing.T) {
	order := domain.StrategyOrderParams{
		Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.52", Size: "20", TimeInForce: domain.TimeInForceFOK,
	}
	book := domain.OrderBookSnapshot{
		TickSize: "0.01",
		Asks: []domain.PriceLevel{
			{Price: "0.50", Size: "4"},
			{Price: "0.51", Size: "6"},
			{Price: "0.52", Size: "10"},
			{Price: "0.53", Size: "100"},
		},
	}
	if err := validateStrategyOrder(order, domain.SideBuy, book, domain.DefaultStrategyExecutionConstraints()); err != nil {
		t.Fatalf("validateStrategyOrder() error = %v", err)
	}
}

func TestValidateStrategyOrderAllowsBuyBeyondTwoTicksWithProtectedDepth(t *testing.T) {
	order := domain.StrategyOrderParams{
		Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.53", Size: "10", TimeInForce: domain.TimeInForceFOK,
	}
	book := domain.OrderBookSnapshot{
		TickSize: "0.01",
		Asks:     []domain.PriceLevel{{Price: "0.50", Size: "10"}, {Price: "0.53", Size: "100"}},
	}
	if err := validateStrategyOrder(order, domain.SideBuy, book, domain.DefaultStrategyExecutionConstraints()); err != nil {
		t.Fatalf("validateStrategyOrder() error = %v", err)
	}
}

func TestValidateStrategyOrderStillRejectsBuyBelowBestAsk(t *testing.T) {
	order := domain.StrategyOrderParams{
		Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.49", Size: "10", TimeInForce: domain.TimeInForceFOK,
	}
	book := domain.OrderBookSnapshot{
		TickSize: "0.01",
		Asks:     []domain.PriceLevel{{Price: "0.50", Size: "10"}},
	}
	err := validateStrategyOrder(order, domain.SideBuy, book, domain.DefaultStrategyExecutionConstraints())
	if err == nil || !strings.Contains(err.Error(), "must be at or above the strategy snapshot best ask") {
		t.Fatalf("validateStrategyOrder() error = %v", err)
	}
}

func TestValidateStrategyOrderAllowsKalshiSparseDepthBeyondTwoTicks(t *testing.T) {
	order := domain.StrategyOrderParams{
		Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.42", Size: "12", TimeInForce: domain.TimeInForceFOK,
	}
	book := domain.OrderBookSnapshot{
		MarketSource: domain.MarketSourceKalshi,
		TickSize:     "0.01",
		MinOrderSize: "1",
		Asks: []domain.PriceLevel{
			{Price: "0.39", Size: "7.73"},
			{Price: "0.42", Size: "15"},
			{Price: "0.43", Size: "50"},
		},
	}
	if err := validateStrategyOrderForMarket(
		order, domain.SideBuy, book, domain.DefaultStrategyExecutionConstraints(), domain.MarketSourceKalshi,
	); err != nil {
		t.Fatalf("validateStrategyOrderForMarket() error = %v", err)
	}
}

func TestValidateStrategyOrderAllowsKalshiPartialIOCDepth(t *testing.T) {
	order := domain.StrategyOrderParams{
		Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.40", Size: "30", TimeInForce: domain.TimeInForceFOK,
	}
	book := domain.OrderBookSnapshot{
		MarketSource: domain.MarketSourceKalshi,
		TickSize:     "0.01",
		MinOrderSize: "1",
		Asks: []domain.PriceLevel{
			{Price: "0.39", Size: "7.73"},
			{Price: "0.40", Size: "15"},
			{Price: "0.41", Size: "50"},
		},
	}
	if err := validateStrategyOrderForMarket(
		order, domain.SideBuy, book, domain.DefaultStrategyExecutionConstraints(), domain.MarketSourceKalshi,
	); err != nil {
		t.Fatalf("validateStrategyOrderForMarket() error = %v", err)
	}
	// Polymarket BUY entries are IOC too, so partial protected depth is enough.
	if err := validateStrategyOrderForMarket(
		order, domain.SideBuy, book, domain.DefaultStrategyExecutionConstraints(), domain.MarketSourcePolymarket,
	); err != nil {
		t.Fatalf("Polymarket partial-depth BUY validation error = %v", err)
	}
	// A Polymarket SELL keeps the strategy FOK and therefore still needs the
	// frozen bids to cover the whole quantity.
	sell := domain.StrategyOrderParams{
		Side: domain.SideSell, Type: domain.OrderTypeLimit, WorstPrice: "0.38", Size: "30", TimeInForce: domain.TimeInForceFOK,
	}
	sellBook := domain.OrderBookSnapshot{
		TickSize: "0.01", MinOrderSize: "1",
		Bids: []domain.PriceLevel{{Price: "0.39", Size: "7.73"}, {Price: "0.38", Size: "15"}, {Price: "0.37", Size: "50"}},
	}
	if err := validateStrategyOrderForMarket(
		sell, domain.SideSell, sellBook, domain.DefaultStrategyExecutionConstraints(), domain.MarketSourcePolymarket,
	); err == nil || !strings.Contains(err.Error(), "protected-price liquidity") {
		t.Fatalf("Polymarket SELL FOK full-depth validation error = %v", err)
	}
}

func TestValidateStrategyOrderAllowsKalshiSparseSellDepthBeyondTwoTicks(t *testing.T) {
	order := domain.StrategyOrderParams{
		Side: domain.SideSell, Type: domain.OrderTypeLimit, WorstPrice: "0.58", Size: "12", TimeInForce: domain.TimeInForceFOK,
	}
	book := domain.OrderBookSnapshot{
		MarketSource: domain.MarketSourceKalshi,
		TickSize:     "0.01",
		MinOrderSize: "1",
		Bids: []domain.PriceLevel{
			{Price: "0.61", Size: "7.73"},
			{Price: "0.58", Size: "15"},
			{Price: "0.57", Size: "50"},
		},
	}
	if err := validateStrategyOrderForMarket(
		order, domain.SideSell, book, domain.DefaultStrategyExecutionConstraints(), domain.MarketSourceKalshi,
	); err != nil {
		t.Fatalf("validateStrategyOrderForMarket() error = %v", err)
	}
}

func TestValidateStrategyOrderRejectsUnmarketableOrOffTickBuy(t *testing.T) {
	book := domain.OrderBookSnapshot{
		TickSize: "0.01",
		Asks:     []domain.PriceLevel{{Price: "0.50", Size: "100"}},
	}
	for _, test := range []struct {
		name       string
		worstPrice domain.Decimal
		want       string
	}{
		{name: "below best ask", worstPrice: "0.49", want: "at or above"},
		{name: "off tick", worstPrice: "0.505", want: "exact multiple"},
	} {
		t.Run(test.name, func(t *testing.T) {
			order := domain.StrategyOrderParams{
				Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: test.worstPrice, Size: "10", TimeInForce: domain.TimeInForceFOK,
			}
			err := validateStrategyOrder(order, domain.SideBuy, book, domain.DefaultStrategyExecutionConstraints())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateStrategyOrder() error = %v", err)
			}
		})
	}
}

func TestValidateStrategyOrderAllowsBuyBeyondProtectedDepthAsIOCPartial(t *testing.T) {
	order := domain.StrategyOrderParams{
		Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.52", Size: "11", TimeInForce: domain.TimeInForceFOK,
	}
	book := domain.OrderBookSnapshot{
		TickSize: "0.01",
		Asks: []domain.PriceLevel{
			{Price: "0.50", Size: "4"},
			{Price: "0.51", Size: "6"},
			{Price: "0.53", Size: "100"},
		},
	}
	// 10 of 11 shares are inside worst_price; IOC takes them and cancels the rest.
	if err := validateStrategyOrder(order, domain.SideBuy, book, domain.DefaultStrategyExecutionConstraints()); err != nil {
		t.Fatalf("validateStrategyOrder() error = %v", err)
	}
}

// TestValidateStrategyOrderAcceptsExplicitIOCFromStrategy 验证白名单加入 IOC 后策略可直接返回 IOC。
func TestValidateStrategyOrderAcceptsExplicitIOCFromStrategy(t *testing.T) {
	order := domain.StrategyOrderParams{
		Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.52", Size: "11", TimeInForce: domain.TimeInForceIOC,
	}
	book := domain.OrderBookSnapshot{TickSize: "0.01", Asks: []domain.PriceLevel{{Price: "0.50", Size: "4"}}}
	if err := validateStrategyOrder(order, domain.SideBuy, book, domain.DefaultStrategyExecutionConstraints()); err != nil {
		t.Fatalf("IOC validateStrategyOrder() error = %v", err)
	}
	legacy := domain.DefaultStrategyExecutionConstraints()
	legacy.AllowedTimeInForce = []domain.TimeInForce{domain.TimeInForceFOK}
	if err := validateStrategyOrder(order, domain.SideBuy, book, legacy); err == nil || !strings.Contains(err.Error(), "allowed_time_in_force") {
		t.Fatalf("IOC against a FOK-only whitelist error = %v", err)
	}
	order.TimeInForce = domain.TimeInForceFAK
	if err := validateStrategyOrder(order, domain.SideBuy, book, domain.DefaultStrategyExecutionConstraints()); err == nil {
		t.Fatal("FAK strategy order was accepted")
	}
}

// TestValidateExecutionConstraintsAcceptsRolloutWhitelists 验证冻结的 ["FOK"] 与当前 ["IOC","FOK"] 都合法。
func TestValidateExecutionConstraintsAcceptsRolloutWhitelists(t *testing.T) {
	for _, allowed := range [][]domain.TimeInForce{
		{domain.TimeInForceFOK},
		{domain.TimeInForceIOC, domain.TimeInForceFOK},
		{domain.TimeInForceIOC},
	} {
		constraints := domain.DefaultStrategyExecutionConstraints()
		constraints.AllowedTimeInForce = allowed
		if err := validateExecutionConstraints(constraints); err != nil {
			t.Fatalf("validateExecutionConstraints(%v) error = %v", allowed, err)
		}
	}
	for _, allowed := range [][]domain.TimeInForce{
		{},
		{domain.TimeInForceFAK},
		{domain.TimeInForceFOK, domain.TimeInForceFOK},
		{domain.TimeInForceIOC, domain.TimeInForceFOK, domain.TimeInForceGTC},
	} {
		constraints := domain.DefaultStrategyExecutionConstraints()
		constraints.AllowedTimeInForce = allowed
		if err := validateExecutionConstraints(constraints); err == nil {
			t.Fatalf("validateExecutionConstraints(%v) accepted an invalid whitelist", allowed)
		}
	}
}

func TestValidateStrategyOrderAllowsDepthAwareSellWithinTwoTicks(t *testing.T) {
	order := domain.StrategyOrderParams{
		Side: domain.SideSell, Type: domain.OrderTypeLimit, WorstPrice: "0.53", Size: "12", TimeInForce: domain.TimeInForceFOK,
	}
	book := domain.OrderBookSnapshot{
		TickSize: "0.01",
		Bids: []domain.PriceLevel{
			{Price: "0.55", Size: "4"},
			{Price: "0.54", Size: "4"},
			{Price: "0.53", Size: "4"},
			{Price: "0.52", Size: "100"},
		},
	}
	if err := validateStrategyOrder(order, domain.SideSell, book, domain.DefaultStrategyExecutionConstraints()); err != nil {
		t.Fatalf("validateStrategyOrder() error = %v", err)
	}
}

func TestValidateStrategyOrderAllowsSellBeyondTwoTicksWithProtectedDepth(t *testing.T) {
	book := domain.OrderBookSnapshot{
		TickSize: "0.01",
		Bids: []domain.PriceLevel{
			{Price: "0.55", Size: "4"},
			{Price: "0.54", Size: "4"},
			{Price: "0.53", Size: "4"},
			{Price: "0.52", Size: "100"},
		},
	}
	order := domain.StrategyOrderParams{
		Side: domain.SideSell, Type: domain.OrderTypeLimit, WorstPrice: "0.52", Size: "12", TimeInForce: domain.TimeInForceFOK,
	}
	if err := validateStrategyOrder(order, domain.SideSell, book, domain.DefaultStrategyExecutionConstraints()); err != nil {
		t.Fatalf("validateStrategyOrder() error = %v", err)
	}
}

func TestValidateStrategyOrderStillRejectsSellAboveBestBid(t *testing.T) {
	order := domain.StrategyOrderParams{
		Side: domain.SideSell, Type: domain.OrderTypeLimit, WorstPrice: "0.56", Size: "10", TimeInForce: domain.TimeInForceFOK,
	}
	book := domain.OrderBookSnapshot{
		TickSize: "0.01",
		Bids:     []domain.PriceLevel{{Price: "0.55", Size: "10"}},
	}
	err := validateStrategyOrder(order, domain.SideSell, book, domain.DefaultStrategyExecutionConstraints())
	if err == nil || !strings.Contains(err.Error(), "must be at or below the strategy snapshot best bid") {
		t.Fatalf("validateStrategyOrder() error = %v", err)
	}
}

func TestValidateStrategyOrderRejectsSellBeyondProtectedDepth(t *testing.T) {
	order := domain.StrategyOrderParams{
		Side: domain.SideSell, Type: domain.OrderTypeLimit, WorstPrice: "0.53", Size: "13", TimeInForce: domain.TimeInForceFOK,
	}
	book := domain.OrderBookSnapshot{
		TickSize: "0.01",
		Bids: []domain.PriceLevel{
			{Price: "0.55", Size: "4"},
			{Price: "0.54", Size: "4"},
			{Price: "0.53", Size: "4"},
			{Price: "0.52", Size: "100"},
		},
	}
	err := validateStrategyOrder(order, domain.SideSell, book, domain.DefaultStrategyExecutionConstraints())
	if err == nil || !strings.Contains(err.Error(), "SELL order.size exceeds protected-price liquidity") {
		t.Fatalf("validateStrategyOrder() error = %v", err)
	}
}

// TestBuildEntryIntentExecutesPolymarketBuyAsIOC 验证 Polymarket BUY 的执行 intent 为 IOC，
// 并记录协议 FOK 与执行 IOC 的映射。
func TestBuildEntryIntentExecutesPolymarketBuyAsIOC(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	request := domain.StrategyDecisionRequest{
		CycleID: "cycle-ioc-buy",
		Context: domain.StrategyExecutionContext{
			ModelID: "model-1", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "account-1",
		},
		DecisionAt:  decisionAt,
		Predictions: []domain.Prediction{prediction},
		OrderBooks: []domain.OrderBookSnapshot{{
			MarketID: prediction.MarketID, ConditionID: prediction.ConditionID, OutcomeIndex: 0,
			TokenID: prediction.Outcomes[0].TokenID, Status: domain.OrderBookStatusOK,
			SourceAt: decisionAt, MinOrderSize: "5", TickSize: "0.01",
			// Only 30 of the 42 requested shares are inside worst_price: IOC
			// takes them and cancels the remainder instead of rejecting.
			Asks: []domain.PriceLevel{{Price: "0.24", Size: "30"}, {Price: "0.30", Size: "500"}},
		}},
		ExecutionConstraints: domain.DefaultStrategyExecutionConstraints(),
	}
	evaluation := domain.StrategyEvaluation{
		DecisionID: "decision-ioc-buy", PredictionID: prediction.PredictionID,
		MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
		OutcomeIndex: 0, TokenID: prediction.Outcomes[0].TokenID,
		Order: &domain.StrategyOrderParams{
			Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: "0.29", Size: "42", TimeInForce: domain.TimeInForceFOK,
		},
	}

	intent, err := buildEntryIntent(request, decisionAt.Add(time.Second), evaluation, "polymarket")
	if err != nil {
		t.Fatalf("buildEntryIntent() error = %v", err)
	}
	if intent.TimeInForce != domain.TimeInForceIOC || intent.Size != "42" || intent.Price != "0.29" || intent.WorstPrice != "0.29" {
		t.Fatalf("intent = %#v, want IOC for 42 shares limited at 0.29", intent)
	}
	if intent.Metadata["strategy_time_in_force"] != "FOK" || intent.Metadata["execution_time_in_force"] != "IOC" {
		t.Fatalf("intent metadata = %#v", intent.Metadata)
	}

	evaluation.Order.TimeInForce = domain.TimeInForceIOC
	intent, err = buildEntryIntent(request, decisionAt.Add(time.Second), evaluation, "polymarket")
	if err != nil || intent.TimeInForce != domain.TimeInForceIOC || intent.Metadata["strategy_time_in_force"] != "" {
		t.Fatalf("explicit IOC intent = %#v, err = %v, want pass-through without mapping metadata", intent, err)
	}
}
