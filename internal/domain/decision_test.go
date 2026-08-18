package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestMidPricePointWireFormatIsUnambiguousRawP 验证 Mid Price Point Wire Format Is Unambiguous Raw P 场景下的行为。
func TestMidPricePointWireFormatIsUnambiguousRawP(t *testing.T) {
	point := MidPricePoint{IntervalEndAt: time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC), P: "0.41"}
	payload, err := json.Marshal(point)
	if err != nil {
		t.Fatal(err)
	}
	text := string(payload)
	if !strings.Contains(text, `"interval_end_at":"2026-08-18T04:20:00Z"`) || !strings.Contains(text, `"p":"0.41"`) ||
		strings.Contains(text, "observed_at") || strings.Contains(text, "mid_price") {
		t.Fatalf("mid-price point JSON = %s", text)
	}
}

// TestComputeStrategyInputIDIgnoresTransportGenerationTime 验证 Compute Strategy Input ID Ignores Transport Generation Time 场景下的行为。
func TestComputeStrategyInputIDIgnoresTransportGenerationTime(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	request := StrategyDecisionRequest{
		SchemaVersion: StrategyInputSchemaVersion,
		CycleID:       "account-1:20260818T042000Z",
		Context: StrategyExecutionContext{
			ModelID: "model-1", StrategyID: "strategy", ExecutionAccountID: "account-1",
		},
		DecisionAt:           decisionAt,
		GeneratedAt:          decisionAt.Add(time.Second),
		PredictionSnapshotID: "predsnap-1",
		Predictions:          []Prediction{},
		OrderBooks:           []OrderBookSnapshot{},
		MidPriceHistories:    []MidPriceHistory{},
	}
	first, err := ComputeStrategyInputID(request)
	if err != nil {
		t.Fatalf("ComputeStrategyInputID() error = %v", err)
	}
	request.GeneratedAt = request.GeneratedAt.Add(time.Minute)
	second, err := ComputeStrategyInputID(request)
	if err != nil {
		t.Fatalf("ComputeStrategyInputID() replay error = %v", err)
	}
	if first == "" || first != second {
		t.Fatalf("input ids = %q / %q, want stable identity", first, second)
	}
	request.Context.ExecutionAccountID = "account-2"
	third, err := ComputeStrategyInputID(request)
	if err != nil {
		t.Fatalf("ComputeStrategyInputID() context error = %v", err)
	}
	if third == first {
		t.Fatal("different execution accounts produced the same input id")
	}
	request.Context.ExecutionAccountID = "account-1"
	request.MidPriceHistories = []MidPriceHistory{{TokenID: "token-1"}}
	fourth, err := ComputeStrategyInputID(request)
	if err != nil {
		t.Fatalf("ComputeStrategyInputID() mid-price history error = %v", err)
	}
	if fourth == first {
		t.Fatal("different mid-price histories produced the same input id")
	}
}

// TestMidPriceHistoryValidate 验证 Mid Price History Validate 场景下的行为。
func TestMidPriceHistoryValidate(t *testing.T) {
	windowEnd := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	windowStart := windowEnd.Add(-48 * time.Hour)
	first := windowStart.Add(time.Minute)
	last := windowEnd.Add(-time.Minute)
	history := MidPriceHistory{
		MarketID: "market-1", ConditionID: "condition-1", OutcomeIndex: 0, TokenID: "token-1",
		Status: MidPriceHistoryStatusOK, WindowStart: windowStart, WindowEnd: windowEnd,
		FidelitySeconds: 60, Sampling: MidPriceSamplingUpstreamRaw,
		MissingValues: MidPriceMissingValuePolicyNoFill, TimestampSemantics: MidPriceTimestampSemanticsIntervalEndUTC,
		FetchedAt: windowEnd.Add(time.Second), CoverageStart: first, CoverageEnd: last,
		MidPrices: []MidPricePoint{{IntervalEndAt: first, P: "0.41"}, {IntervalEndAt: last, P: "0.42"}},
	}
	if err := history.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	history.MidPrices[1].P = "1.01"
	if err := history.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want out-of-range midpoint rejection")
	}
}
