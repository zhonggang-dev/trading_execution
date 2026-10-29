package decisioncycle

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// decisionQuotesRequest 构造只含指定盘口的买入请求，用于验证买入单记录的决策盘口。
func decisionQuotesRequest(decisionAt time.Time, prediction domain.Prediction, books ...domain.OrderBookSnapshot) domain.StrategyDecisionRequest {
	return domain.StrategyDecisionRequest{
		CycleID: "cycle-quotes", InputID: "input-quotes",
		Context: domain.StrategyExecutionContext{
			ModelID: "model-1", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "account-1",
		},
		DecisionAt: decisionAt, Predictions: []domain.Prediction{prediction}, OrderBooks: books,
		ExecutionConstraints: domain.DefaultStrategyExecutionConstraints(),
	}
}

func quotesBook(prediction domain.Prediction, outcome int, decisionAt time.Time, bid, ask domain.Decimal) domain.OrderBookSnapshot {
	book := domain.OrderBookSnapshot{
		MarketSource: prediction.MarketSource, MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
		OutcomeIndex: outcome, TokenID: prediction.Outcomes[outcome].TokenID, OutcomeID: prediction.Outcomes[outcome].OutcomeID,
		Status: domain.OrderBookStatusOK, SourceAt: decisionAt, MinOrderSize: "1", TickSize: "0.01",
		BestBid: bid, BestAsk: ask,
	}
	if !bid.IsEmpty() {
		book.Bids = []domain.PriceLevel{{Price: bid, Size: "100"}}
	}
	if !ask.IsEmpty() {
		book.Asks = []domain.PriceLevel{{Price: ask, Size: "100"}}
	}
	if bid.IsEmpty() && ask.IsEmpty() {
		book.Status = domain.OrderBookStatusEmpty
	}
	return book
}

func quotesBuyEvaluation(prediction domain.Prediction, outcome int, worstPrice domain.Decimal, probability float64) domain.StrategyEvaluation {
	return domain.StrategyEvaluation{
		DecisionID: "decision-quotes", PredictionID: prediction.PredictionID,
		MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
		OutcomeIndex: outcome, TokenID: prediction.Outcomes[outcome].TokenID,
		Evidence: domain.StrategyEvidence{Probability: probability},
		Order: &domain.StrategyOrderParams{
			Side: domain.SideBuy, Type: domain.OrderTypeLimit, WorstPrice: worstPrice, Size: "10", TimeInForce: domain.TimeInForceFOK,
		},
	}
}

func kalshiQuotesPrediction(decisionAt time.Time) domain.Prediction {
	prediction := validPrediction(decisionAt)
	prediction.MarketSource = domain.MarketSourceKalshi
	prediction.MarketID = "TEST-MARKET"
	prediction.ConditionID = "kalshi:TEST-MARKET"
	prediction.Outcomes[0].OutcomeID = "YES"
	prediction.Outcomes[0].TokenID = "kalshi:TEST-MARKET:YES"
	prediction.Outcomes[1].OutcomeID = "NO"
	prediction.Outcomes[1].TokenID = "kalshi:TEST-MARKET:NO"
	return prediction
}

func assertQuotes(t *testing.T, got *domain.OrderDecisionQuotes, want domain.OrderDecisionQuotes) {
	t.Helper()
	if got == nil {
		t.Fatalf("decision quotes = nil, want %#v", want)
	}
	if *got != want {
		t.Fatalf("decision quotes = %#v, want %#v", *got, want)
	}
}

// TestBuildEntryIntentRecordsPolymarketYesAndNoQuotes 验证 Polymarket 买入单记录两侧买一卖一和 YES 概率。
func TestBuildEntryIntentRecordsPolymarketYesAndNoQuotes(t *testing.T) {
	decisionAt := time.Date(2026, 10, 28, 8, 30, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	request := decisionQuotesRequest(decisionAt, prediction,
		quotesBook(prediction, 0, decisionAt, "0.48", "0.50"),
		quotesBook(prediction, 1, decisionAt, "0.49", "0.52"),
	)
	intent, err := buildEntryIntent(request, decisionAt, quotesBuyEvaluation(prediction, 0, "0.50", 0.7), "polymarket")
	if err != nil {
		t.Fatalf("buildEntryIntent() error = %v", err)
	}
	assertQuotes(t, intent.DecisionQuotes, domain.OrderDecisionQuotes{
		YesBid: "0.48", YesAsk: "0.50", NoBid: "0.49", NoAsk: "0.52", YesProbability: "0.7",
	})
}

// TestBuildEntryIntentRecordsYesProbabilityWhenBuyingNo 验证买 NO 时记录的仍是 YES 概率，原有的所买一侧概率不变。
func TestBuildEntryIntentRecordsYesProbabilityWhenBuyingNo(t *testing.T) {
	decisionAt := time.Date(2026, 10, 28, 8, 30, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	request := decisionQuotesRequest(decisionAt, prediction,
		quotesBook(prediction, 0, decisionAt, "0.68", "0.70"),
		quotesBook(prediction, 1, decisionAt, "0.30", "0.32"),
	)
	intent, err := buildEntryIntent(request, decisionAt, quotesBuyEvaluation(prediction, 1, "0.32", 0.3), "polymarket")
	if err != nil {
		t.Fatalf("buildEntryIntent() error = %v", err)
	}
	assertQuotes(t, intent.DecisionQuotes, domain.OrderDecisionQuotes{
		YesBid: "0.68", YesAsk: "0.70", NoBid: "0.30", NoAsk: "0.32", YesProbability: "0.7",
	})
	if intent.Metadata["predicted_probability"] != "0.3" {
		t.Fatalf("predicted_probability = %q, want bought-side 0.3", intent.Metadata["predicted_probability"])
	}
}

// TestBuildEntryIntentLeavesMissingSideEmpty 验证另一侧没有书时对应值为空，下单不受影响。
func TestBuildEntryIntentLeavesMissingSideEmpty(t *testing.T) {
	decisionAt := time.Date(2026, 10, 28, 8, 30, 0, 0, time.UTC)
	prediction := kalshiQuotesPrediction(decisionAt)
	request := decisionQuotesRequest(decisionAt, prediction, quotesBook(prediction, 0, decisionAt, "0.37", "0.39"))
	intent, err := buildEntryIntent(request, decisionAt, quotesBuyEvaluation(prediction, 0, "0.39", 0.7), "polymarket")
	if err != nil {
		t.Fatalf("buildEntryIntent() error = %v", err)
	}
	assertQuotes(t, intent.DecisionQuotes, domain.OrderDecisionQuotes{YesBid: "0.37", YesAsk: "0.39", YesProbability: "0.7"})
}

// TestBuildEntryIntentHandlesOneSidedAndFailedBooks 验证只有一侧报价、MISSING/ERROR 书时记录为空而不报错。
func TestBuildEntryIntentHandlesOneSidedAndFailedBooks(t *testing.T) {
	decisionAt := time.Date(2026, 10, 28, 8, 30, 0, 0, time.UTC)
	tests := []struct {
		name   string
		noBook domain.OrderBookSnapshot
		noBid  domain.Decimal
		noAsk  domain.Decimal
	}{
		{name: "ask only", noBook: domain.OrderBookSnapshot{Status: domain.OrderBookStatusEmpty, Asks: []domain.PriceLevel{{Price: "0.65", Size: "3"}}, BestAsk: "0.65"}, noAsk: "0.65"},
		{name: "bid only", noBook: domain.OrderBookSnapshot{Status: domain.OrderBookStatusEmpty, Bids: []domain.PriceLevel{{Price: "0.61", Size: "3"}}, BestBid: "0.61"}, noBid: "0.61"},
		{name: "missing", noBook: domain.OrderBookSnapshot{Status: domain.OrderBookStatusMissing, BestBid: "0.61", BestAsk: "0.65"}},
		{name: "error", noBook: domain.OrderBookSnapshot{Status: domain.OrderBookStatusError, ErrorCode: "TIMEOUT"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			prediction := kalshiQuotesPrediction(decisionAt)
			noBook := test.noBook
			noBook.MarketSource, noBook.MarketID, noBook.ConditionID = prediction.MarketSource, prediction.MarketID, prediction.ConditionID
			noBook.OutcomeIndex, noBook.TokenID, noBook.OutcomeID = 1, prediction.Outcomes[1].TokenID, "NO"
			request := decisionQuotesRequest(decisionAt, prediction, quotesBook(prediction, 0, decisionAt, "0.35", "0.39"), noBook)
			intent, err := buildEntryIntent(request, decisionAt, quotesBuyEvaluation(prediction, 0, "0.39", 0.7), "polymarket")
			if err != nil {
				t.Fatalf("buildEntryIntent() error = %v", err)
			}
			assertQuotes(t, intent.DecisionQuotes, domain.OrderDecisionQuotes{
				YesBid: "0.35", YesAsk: "0.39", NoBid: test.noBid, NoAsk: test.noAsk, YesProbability: "0.7",
			})
		})
	}
}

// TestBuildEntryIntentFallsBackToTopLevelsWhenBestIsUnset 验证 best 字段未填时取首档价格。
func TestBuildEntryIntentFallsBackToTopLevelsWhenBestIsUnset(t *testing.T) {
	decisionAt := time.Date(2026, 10, 28, 8, 30, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	yes := quotesBook(prediction, 0, decisionAt, "0.48", "0.50")
	yes.BestBid, yes.BestAsk = "", ""
	no := quotesBook(prediction, 1, decisionAt, "0.49", "0.52")
	no.BestBid, no.BestAsk = "", ""
	intent, err := buildEntryIntent(decisionQuotesRequest(decisionAt, prediction, yes, no), decisionAt, quotesBuyEvaluation(prediction, 0, "0.50", 0.7), "polymarket")
	if err != nil {
		t.Fatalf("buildEntryIntent() error = %v", err)
	}
	assertQuotes(t, intent.DecisionQuotes, domain.OrderDecisionQuotes{
		YesBid: "0.48", YesAsk: "0.50", NoBid: "0.49", NoAsk: "0.52", YesProbability: "0.7",
	})
}

// TestBuildEntryIntentMapsKalshiSidesByOutcomeID 验证 Kalshi 按 outcome_id 而不是数组顺序区分 YES/NO。
func TestBuildEntryIntentMapsKalshiSidesByOutcomeID(t *testing.T) {
	decisionAt := time.Date(2026, 10, 28, 8, 30, 0, 0, time.UTC)
	prediction := kalshiQuotesPrediction(decisionAt)
	prediction.Outcomes[0], prediction.Outcomes[1] = prediction.Outcomes[1], prediction.Outcomes[0]
	prediction.Outcomes[0].Index, prediction.Outcomes[1].Index = 0, 1
	// 交换后 outcome 0 是 NO（概率 0.3），outcome 1 是 YES（概率 0.7）。
	request := decisionQuotesRequest(decisionAt, prediction,
		quotesBook(prediction, 0, decisionAt, "0.29", "0.31"),
		quotesBook(prediction, 1, decisionAt, "0.69", "0.71"),
	)
	intent, err := buildEntryIntent(request, decisionAt, quotesBuyEvaluation(prediction, 0, "0.31", 0.3), "polymarket")
	if err != nil {
		t.Fatalf("buildEntryIntent() error = %v", err)
	}
	assertQuotes(t, intent.DecisionQuotes, domain.OrderDecisionQuotes{
		YesBid: "0.69", YesAsk: "0.71", NoBid: "0.29", NoAsk: "0.31", YesProbability: "0.7",
	})
}

// TestBuildEntryIntentDropsQuotesOutsideUnitRange 验证越界或无法解析的盘口值记为空，
// 不能让买入单因订单表的范围约束或 JSON 编码失败而无法创建。
func TestBuildEntryIntentDropsQuotesOutsideUnitRange(t *testing.T) {
	decisionAt := time.Date(2026, 10, 28, 8, 30, 0, 0, time.UTC)
	prediction := validPrediction(decisionAt)
	no := quotesBook(prediction, 1, decisionAt, "0.49", "0.52")
	no.BestBid, no.BestAsk = "not-a-number", "1.5"
	request := decisionQuotesRequest(decisionAt, prediction, quotesBook(prediction, 0, decisionAt, "0.48", "0.50"), no)
	intent, err := buildEntryIntent(request, decisionAt, quotesBuyEvaluation(prediction, 0, "0.50", 0.7), "polymarket")
	if err != nil {
		t.Fatalf("buildEntryIntent() error = %v", err)
	}
	assertQuotes(t, intent.DecisionQuotes, domain.OrderDecisionQuotes{YesBid: "0.48", YesAsk: "0.50", YesProbability: "0.7"})
	if _, err := json.Marshal(intent); err != nil {
		t.Fatalf("json.Marshal(intent) error = %v", err)
	}
}

// TestBuildEntryIntentRecordsMarketEndAndAllDomains 验证买入单记录预测里的到期日（UTC）和完整 domain 列表，
// Polymarket 与 Kalshi 一致。
func TestBuildEntryIntentRecordsMarketEndAndAllDomains(t *testing.T) {
	decisionAt := time.Date(2026, 10, 28, 8, 30, 0, 0, time.UTC)
	local := time.FixedZone("CST", 8*3600)
	endAt := time.Date(2026, 11, 6, 16, 0, 0, 0, local)
	for name, prediction := range map[string]domain.Prediction{
		"polymarket": validPrediction(decisionAt),
		"kalshi":     kalshiQuotesPrediction(decisionAt),
	} {
		t.Run(name, func(t *testing.T) {
			prediction.EndAt = &endAt
			prediction.Domains = []string{"Politics", " Economics ", ""}
			request := decisionQuotesRequest(decisionAt, prediction,
				quotesBook(prediction, 0, decisionAt, "0.48", "0.50"), quotesBook(prediction, 1, decisionAt, "0.49", "0.52"))
			intent, err := buildEntryIntent(request, decisionAt, quotesBuyEvaluation(prediction, 0, "0.50", 0.7), "polymarket")
			if err != nil {
				t.Fatalf("buildEntryIntent() error = %v", err)
			}
			meta := intent.DecisionMeta
			if meta == nil || meta.MarketEndAt == nil || meta.MarketEndAt.Location() != time.UTC || !meta.MarketEndAt.Equal(endAt) {
				t.Fatalf("decision meta = %#v, want the end date as the same instant in UTC", meta)
			}
			if len(meta.Domains) != 2 || meta.Domains[0] != "Politics" || meta.Domains[1] != "Economics" {
				t.Fatalf("domains = %#v, want every non-empty domain in order", meta.Domains)
			}
		})
	}
}

// TestBuildEntryIntentLeavesDecisionMetaOutWhenPredictionHasNone 验证预测没有到期日和 domain 时不带复盘信息，
// 只有一项时只记录这一项，都不影响下单。
func TestBuildEntryIntentLeavesDecisionMetaOutWhenPredictionHasNone(t *testing.T) {
	decisionAt := time.Date(2026, 10, 28, 8, 30, 0, 0, time.UTC)
	endAt := time.Date(2026, 11, 6, 8, 0, 0, 0, time.UTC)
	zero := time.Time{}
	for _, test := range []struct {
		name        string
		end         *time.Time
		domains     []string
		wantNil     bool
		wantEnd     bool
		wantDomains int
	}{
		{name: "neither", wantNil: true},
		{name: "zero end and blank domains", end: &zero, domains: []string{" ", ""}, wantNil: true},
		{name: "end only", end: &endAt, wantEnd: true},
		{name: "domains only", domains: []string{"Finance"}, wantDomains: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			prediction := validPrediction(decisionAt)
			prediction.EndAt, prediction.Domains = test.end, test.domains
			request := decisionQuotesRequest(decisionAt, prediction,
				quotesBook(prediction, 0, decisionAt, "0.48", "0.50"), quotesBook(prediction, 1, decisionAt, "0.49", "0.52"))
			intent, err := buildEntryIntent(request, decisionAt, quotesBuyEvaluation(prediction, 0, "0.50", 0.7), "polymarket")
			if err != nil {
				t.Fatalf("buildEntryIntent() error = %v", err)
			}
			if test.wantNil {
				if intent.DecisionMeta != nil {
					t.Fatalf("decision meta = %#v, want nil", intent.DecisionMeta)
				}
				return
			}
			meta := intent.DecisionMeta
			if meta == nil || (meta.MarketEndAt != nil) != test.wantEnd || len(meta.Domains) != test.wantDomains {
				t.Fatalf("decision meta = %#v, want end=%v domains=%d", meta, test.wantEnd, test.wantDomains)
			}
		})
	}
}
