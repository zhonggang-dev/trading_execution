package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestMarketValidationEmptyBookEvidenceAndLegacyCompatibility(t *testing.T) {
	now := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	base := MarketValidationParams{
		Mode: "LIVE_CHECK", TokenID: "token-yes", OutcomeName: "Yes",
		ValidatedAt: now, MarketObservedAt: now, StrategySnapshotAt: now,
		LatestBookSourceAt: now, LatestBookObservedAt: now,
		TickSize: "0.01", WorstPrice: "0.50",
	}
	for _, test := range []struct {
		name   string
		status OrderBookStatus
		bid    Decimal
		ask    Decimal
		valid  bool
	}{
		{"legacy full", "", "0.49", "0.51", true},
		{"legacy one side", "", "0.49", "", false},
		{"OK full", OrderBookStatusOK, "0.49", "0.51", true},
		{"OK missing side", OrderBookStatusOK, "", "0.51", false},
		{"EMPTY bid only", OrderBookStatusEmpty, "0.49", "", true},
		{"EMPTY ask only", OrderBookStatusEmpty, "", "0.51", true},
		{"EMPTY no quotes", OrderBookStatusEmpty, "", "", true},
		{"EMPTY full book", OrderBookStatusEmpty, "0.49", "0.51", false},
		{"EMPTY invalid quote", OrderBookStatusEmpty, "-1", "", false},
		{"ERROR", OrderBookStatusError, "0.49", "0.51", false},
		{"MISSING", OrderBookStatusMissing, "0.49", "0.51", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			params := base
			params.BookStatus, params.BestBid, params.BestAsk = test.status, test.bid, test.ask
			validation, err := params.Build()
			if (err == nil) != test.valid {
				t.Fatalf("Build() error = %v, want valid=%v", err, test.valid)
			}
			if !test.valid {
				return
			}
			payload, err := json.Marshal(validation)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(payload, &fields); err != nil {
				t.Fatal(err)
			}
			for name, present := range map[string]bool{"book_status": test.status != "", "best_bid": test.bid != "", "best_ask": test.ask != ""} {
				if _, ok := fields[name]; ok != present {
					t.Fatalf("%s presence = %v, want %v in %s", name, ok, present, payload)
				}
			}
			var restored MarketValidationParams
			if err := json.Unmarshal(payload, &restored); err != nil {
				t.Fatal(err)
			}
			if _, err := restored.Build(); err != nil {
				t.Fatalf("persisted evidence rejected after JSON round trip: %v", err)
			}
		})
	}
}
