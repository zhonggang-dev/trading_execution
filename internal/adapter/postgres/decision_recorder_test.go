package postgres

import (
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
