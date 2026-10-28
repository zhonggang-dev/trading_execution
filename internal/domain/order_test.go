package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestDecimalJSONRequiresString 验证 Decimal JSON Requires String 场景下的行为。
func TestDecimalJSONRequiresString(t *testing.T) {
	var value struct {
		Price Decimal `json:"price"`
	}
	if err := json.Unmarshal([]byte(`{"price":0.51}`), &value); err == nil {
		t.Fatal("numeric JSON decimal was accepted")
	}
	if err := json.Unmarshal([]byte(`{"price":"0.51"}`), &value); err != nil {
		t.Fatalf("string JSON decimal error = %v", err)
	}
	if value.Price != Decimal("0.51") {
		t.Fatalf("price = %q, want 0.51", value.Price)
	}
}

// TestOrderIntentNormalizeAndValidate 验证 Order Intent Normalize And Validate 场景下的行为。
func TestOrderIntentNormalizeAndValidate(t *testing.T) {
	expiresAt := time.Date(2026, 8, 18, 16, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))
	intent := OrderIntent{
		ModelID:            " model-1 ",
		StrategyID:         " strategy-1 ",
		ExecutionAccountID: " account-1 ",
		SignalID:           " signal-1 ",
		ClientOrderID:      " client-1 ",
		Venue:              " POLYMARKET-PAPER ",
		MarketID:           " market-1 ",
		TokenID:            " token-1 ",
		Side:               " buy ",
		Type:               " limit ",
		Price:              " 0.50 ",
		Size:               " 10 ",
		TimeInForce:        " gtc ",
		ExpiresAt:          &expiresAt,
	}
	normalized := intent.Normalize()
	if err := normalized.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if normalized.Venue != "polymarket-paper" || normalized.Side != SideBuy || normalized.Type != OrderTypeLimit {
		t.Fatalf("normalized intent = %#v", normalized)
	}
	if normalized.ExpiresAt.Location() != time.UTC {
		t.Fatalf("expires_at location = %v, want UTC", normalized.ExpiresAt.Location())
	}
}

// TestOrderIntentEquivalentUsesDecimalValue 验证 Order Intent Equivalent Uses Decimal Value 场景下的行为。
func TestOrderIntentEquivalentUsesDecimalValue(t *testing.T) {
	left := validIntent()
	right := validIntent()
	left.Price = "0.50"
	right.Price = "0.5"
	left.Size = "10.00"
	right.Size = "10"
	if !left.Equivalent(right) {
		t.Fatal("numerically equivalent intents were treated as different")
	}

	left.Type = OrderTypeMarket
	right.Type = OrderTypeMarket
	left.Price = ""
	right.Price = ""
	left.TimeInForce = TimeInForceIOC
	right.TimeInForce = TimeInForceIOC
	if !left.Equivalent(right) {
		t.Fatal("equivalent market intents were treated as different")
	}
	right.ExecutionAccountID = "another-account"
	if left.Equivalent(right) {
		t.Fatal("intents for different execution accounts were treated as equivalent")
	}
}

// TestOrderIntentRejectsInvalidCombinations 验证 Order Intent Rejects Invalid Combinations 场景下的行为。
func TestOrderIntentRejectsInvalidCombinations(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*OrderIntent)
	}{
		{name: "zero size", mutate: func(intent *OrderIntent) { intent.Size = "0" }},
		{name: "limit without price", mutate: func(intent *OrderIntent) { intent.Price = "" }},
		{name: "market with price", mutate: func(intent *OrderIntent) {
			intent.Type = OrderTypeMarket
			intent.TimeInForce = TimeInForceIOC
		}},
		{name: "market GTC", mutate: func(intent *OrderIntent) {
			intent.Type = OrderTypeMarket
			intent.Price = ""
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			intent := validIntent()
			test.mutate(&intent)
			if err := intent.Validate(); err == nil {
				t.Fatalf("Validate() error = nil for %#v", intent)
			}
		})
	}
}

// TestOrderStatusCannotRegress 验证 Order Status Cannot Regress 场景下的行为。
func TestOrderStatusCannotRegress(t *testing.T) {
	order := Order{Status: OrderStatusPartiallyFilled}
	if order.CanApplyVenueStatus(OrderStatusOpen) {
		t.Fatal("PARTIALLY_FILLED order was allowed to regress to OPEN")
	}
	if !order.CanTransitionTo(OrderStatusFilled) || !order.CanTransitionTo(OrderStatusCancelPending) {
		t.Fatal("PARTIALLY_FILLED order rejected fill or cancel-pending transition")
	}
	order.Status = OrderStatusFilled
	if order.CanTransitionTo(OrderStatusCancelled) {
		t.Fatal("terminal FILLED order was allowed to transition")
	}
}

// TestTerminalAuditStatesAcceptLateConfirmedFillCorrections 验证 Terminal Audit States Accept Late Confirmed Fill Corrections 场景下的行为。
func TestTerminalAuditStatesAcceptLateConfirmedFillCorrections(t *testing.T) {
	cancelled := Order{Status: OrderStatusCancelled}
	if !cancelled.CanTransitionTo(OrderStatusCancelled) || !cancelled.CanTransitionTo(OrderStatusFilled) {
		t.Fatal("CANCELLED did not accept a late partial/full fill correction")
	}
	manual := Order{Status: OrderStatusManualReview}
	if !manual.CanTransitionTo(OrderStatusManualReview) || !manual.CanTransitionTo(OrderStatusFilled) ||
		manual.CanTransitionTo(OrderStatusLive) {
		t.Fatal("MANUAL_REVIEW late-fill transition policy is incorrect")
	}
}

// validIntent 构建测试使用的合法输入。
func validIntent() OrderIntent {
	return OrderIntent{
		ModelID:            "model-1",
		StrategyID:         "strategy-1",
		ExecutionAccountID: "account-1",
		SignalID:           "signal-1",
		ClientOrderID:      "client-1",
		Venue:              "polymarket-paper",
		MarketID:           "market-1",
		TokenID:            "token-1",
		Side:               SideBuy,
		Type:               OrderTypeLimit,
		Price:              "0.5",
		Size:               "10",
		TimeInForce:        TimeInForceGTC,
	}
}

// TestOrderIntentEquivalentIgnoresDecisionQuotes 验证决策盘口只作审计附带信息，不改变意图身份：
// 上线前冻结、上线后重放的同一意图不带它，不能被判为冲突。
func TestOrderIntentEquivalentIgnoresDecisionQuotes(t *testing.T) {
	base := OrderIntent{ClientOrderID: "client-1", Side: SideBuy, Size: "10", Metadata: map[string]string{"cycle_id": "c-1"}}
	withQuotes := base
	withQuotes.DecisionQuotes = &OrderDecisionQuotes{YesBid: "0.48", YesAsk: "0.50", NoBid: "0.49", NoAsk: "0.52", YesProbability: "0.7"}
	if !base.Equivalent(withQuotes) || !withQuotes.Equivalent(base) {
		t.Fatal("intents differing only by decision quotes must stay equivalent")
	}
	other := withQuotes
	other.DecisionQuotes = &OrderDecisionQuotes{YesBid: "0.10"}
	if !withQuotes.Equivalent(other) {
		t.Fatal("different decision quotes must not change intent identity")
	}
}

// TestOrderIntentNormalizeCopiesDecisionQuotes 验证规范化会深复制并清理决策盘口。
func TestOrderIntentNormalizeCopiesDecisionQuotes(t *testing.T) {
	quotes := &OrderDecisionQuotes{YesBid: " 0.48 ", YesAsk: "0.50", YesProbability: "0.7 "}
	intent := OrderIntent{DecisionQuotes: quotes}.Normalize()
	if intent.DecisionQuotes == quotes {
		t.Fatal("Normalize must copy decision quotes")
	}
	if intent.DecisionQuotes.YesBid != "0.48" || intent.DecisionQuotes.YesProbability != "0.7" || intent.DecisionQuotes.NoBid != "" {
		t.Fatalf("normalized quotes = %#v", intent.DecisionQuotes)
	}
	quotes.YesAsk = "0.99"
	if intent.DecisionQuotes.YesAsk != "0.50" {
		t.Fatal("normalized intent must not share the caller's quotes")
	}
	if (OrderIntent{}).Normalize().DecisionQuotes != nil {
		t.Fatal("intent without decision quotes must stay nil")
	}
}

// TestOrderIntentJSONCarriesDecisionQuotesOnlyWhenPresent 验证 intent JSON 只在有值时带 decision_quotes，旧 JSON 解码后为空。
func TestOrderIntentJSONCarriesDecisionQuotesOnlyWhenPresent(t *testing.T) {
	plain, err := json.Marshal(OrderIntent{ClientOrderID: "client-1"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(plain), "decision_quotes") {
		t.Fatalf("intent without quotes JSON = %s", plain)
	}
	encoded, err := json.Marshal(OrderIntent{ClientOrderID: "client-1", DecisionQuotes: &OrderDecisionQuotes{YesBid: "0.48", NoAsk: "0.52"}})
	if err != nil {
		t.Fatal(err)
	}
	var decoded OrderIntent
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.DecisionQuotes == nil || decoded.DecisionQuotes.YesBid != "0.48" || decoded.DecisionQuotes.NoAsk != "0.52" || decoded.DecisionQuotes.YesAsk != "" {
		t.Fatalf("decoded quotes = %#v from %s", decoded.DecisionQuotes, encoded)
	}
}
