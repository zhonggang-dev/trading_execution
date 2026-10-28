package postgres

import (
	"encoding/json"
	"maps"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestDecisionIntentReplayEquivalentGrandfathersOnlyFrozenKalshiFOK(t *testing.T) {
	legacy := domain.OrderIntent{
		ModelID: "model", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet",
		SignalID: "decision", ClientOrderID: "client-order", Venue: "kalshi",
		MarketSource: domain.MarketSourceKalshi, MarketID: "KXTEST", ConditionID: "kalshi:KXTEST",
		OutcomeID: "YES", TokenID: "kalshi:KXTEST:YES", Side: domain.SideBuy,
		Type: domain.OrderTypeLimit, Price: "0.42", WorstPrice: "0.42", Size: "12",
		TimeInForce: domain.TimeInForceFOK,
		Metadata:    map[string]string{"cycle_id": "cycle", "input_id": "input"},
	}
	current := legacy
	current.TimeInForce = domain.TimeInForceIOC
	current.Metadata = maps.Clone(legacy.Metadata)
	current.Metadata["strategy_time_in_force"] = "FOK"
	current.Metadata["execution_time_in_force"] = "IOC"
	if !decisionIntentReplayEquivalent(legacy, current) {
		t.Fatal("legacy Kalshi FOK delivery did not replay against its IOC successor")
	}
	if decisionIntentReplayEquivalent(current, legacy) {
		t.Fatal("IOC-to-FOK replay was accepted in the unsafe reverse direction")
	}

	for name, mutate := range map[string]func(*domain.OrderIntent){
		"price change":        func(intent *domain.OrderIntent) { intent.WorstPrice = "0.43" },
		"missing marker":      func(intent *domain.OrderIntent) { delete(intent.Metadata, "execution_time_in_force") },
		"different venue":     func(intent *domain.OrderIntent) { intent.MarketSource = domain.MarketSourcePolymarket },
		"unexpected metadata": func(intent *domain.OrderIntent) { intent.Metadata["new_semantics"] = "true" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := current
			candidate.Metadata = maps.Clone(current.Metadata)
			mutate(&candidate)
			if decisionIntentReplayEquivalent(legacy, candidate) {
				t.Fatalf("unsafe replay difference was accepted: %#v", candidate)
			}
		})
	}
}

// TestDecisionIntentReplayIgnoresDecisionQuotesAfterJSONRoundTrip 验证投递表里上线前冻结、
// 不带 decision_quotes 的意图，与上线后重建出的带盘口意图经 JSON 往返后仍判为同一意图；
// 带盘口的意图经 JSON 往返后字段保留。
func TestDecisionIntentReplayIgnoresDecisionQuotesAfterJSONRoundTrip(t *testing.T) {
	legacy := domain.OrderIntent{
		ModelID: "model", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet",
		SignalID: "decision", ClientOrderID: "client-order", Venue: "polymarket",
		MarketSource: domain.MarketSourcePolymarket, MarketID: "market", ConditionID: "condition",
		TokenID: "token-yes", Side: domain.SideBuy, Type: domain.OrderTypeLimit,
		Price: "0.50", WorstPrice: "0.50", Size: "12", TimeInForce: domain.TimeInForceGTC,
		Metadata: map[string]string{"cycle_id": "cycle", "input_id": "input"},
	}
	current := legacy
	current.DecisionQuotes = &domain.OrderDecisionQuotes{YesBid: "0.48", YesAsk: "0.50", NoBid: "0.49", NoAsk: "0.52", YesProbability: "0.7"}
	roundTrip := func(intent domain.OrderIntent) domain.OrderIntent {
		t.Helper()
		payload, err := json.Marshal(intent.Normalize())
		if err != nil {
			t.Fatal(err)
		}
		var decoded domain.OrderIntent
		if err := json.Unmarshal(payload, &decoded); err != nil {
			t.Fatal(err)
		}
		return decoded
	}
	stored, proposed := roundTrip(legacy), roundTrip(current)
	if !decisionIntentReplayEquivalent(stored, proposed) || !decisionIntentReplayEquivalent(proposed, stored) {
		t.Fatal("frozen intent without decision quotes conflicted with its rebuilt successor")
	}
	if proposed.DecisionQuotes == nil || *proposed.DecisionQuotes != *current.DecisionQuotes {
		t.Fatalf("decision quotes after JSON round trip = %#v", proposed.DecisionQuotes)
	}
	changed := proposed
	changed.Size = "11"
	if decisionIntentReplayEquivalent(stored, changed) {
		t.Fatal("a real intent change was accepted alongside decision quotes")
	}
}
