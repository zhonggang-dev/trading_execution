package polymarket

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// TestMidPriceHistoryCaptureMapsPDirectlyAndDeduplicatesTimestamps 验证 Mid Price History Capture Maps P Directly And Deduplicates Timestamps 场景下的行为。
func TestMidPriceHistoryCaptureMapsPDirectlyAndDeduplicatesTimestamps(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	windowStart := decisionAt.Add(-48 * time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/batch-prices-history" {
			t.Fatalf("request = %s %s", request.Method, request.URL.Path)
		}
		var body struct {
			Markets  []string `json:"markets"`
			StartTS  int64    `json:"start_ts"`
			EndTS    int64    `json:"end_ts"`
			Fidelity int      `json:"fidelity"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if len(body.Markets) != 2 || body.StartTS != windowStart.Unix() || body.EndTS != decisionAt.Unix() || body.Fidelity != 1 {
			t.Fatalf("request body = %#v", body)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"history":{
			"yes-token":[
				{"t":1786854060,"p":0.41},
				{"t":1787026740,"p":0.42},
				{"t":1787026740,"p":0.43}
			],
			"no-token":[
				{"t":1786854060,"p":0.59},
				{"t":1787026740,"p":0.57}
			]
		}}`))
	}))
	defer server.Close()
	source, err := NewMidPriceHistorySource(MidPriceHistoryParams{
		BaseURL: server.URL, HTTPClient: server.Client(), Workers: 1, RequestsPerSecond: 100,
		Now: func() time.Time { return decisionAt.Add(time.Second) },
	})
	if err != nil {
		t.Fatalf("NewMidPriceHistorySource() error = %v", err)
	}
	targets := []domain.BookTarget{
		{MarketID: "market-1", ConditionID: "condition-1", OutcomeIndex: 0, TokenID: "yes-token"},
		{MarketID: "market-1", ConditionID: "condition-1", OutcomeIndex: 1, TokenID: "no-token"},
	}
	histories, err := source.Capture(t.Context(), decisionAt, 48*time.Hour, targets)
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if len(histories) != 2 || histories[0].Status != domain.MidPriceHistoryStatusOK || len(histories[0].MidPrices) != 2 {
		t.Fatalf("histories = %#v", histories)
	}
	if histories[0].MidPrices[0].P != "0.41" || histories[0].MidPrices[1].P != "0.43" {
		t.Fatalf("YES mid prices = %#v, want p mapped directly and duplicate timestamp kept once", histories[0].MidPrices)
	}
	if histories[0].MidPrices[0].IntervalEndAt.Second() != 0 || histories[0].MidPrices[0].IntervalEndAt.Nanosecond() != 0 {
		t.Fatalf("interval end was not normalized to a UTC minute: %s", histories[0].MidPrices[0].IntervalEndAt)
	}
	for index, history := range histories {
		if err := history.Validate(); err != nil {
			t.Fatalf("history %d Validate() error = %v", index, err)
		}
	}
}

// TestMidPriceHistoryCaptureRepresentsHTTPFailureInBand 验证 Mid Price History Capture Represents HTTP Failure In Band 场景下的行为。
func TestMidPriceHistoryCaptureRepresentsHTTPFailureInBand(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	source, err := NewMidPriceHistorySource(MidPriceHistoryParams{
		BaseURL: server.URL, HTTPClient: server.Client(), Workers: 1, RequestsPerSecond: 100, MaxAttempts: 1,
	})
	if err != nil {
		t.Fatalf("NewMidPriceHistorySource() error = %v", err)
	}
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	histories, err := source.Capture(t.Context(), decisionAt, 48*time.Hour, []domain.BookTarget{{
		MarketID: "market-1", ConditionID: "condition-1", OutcomeIndex: 0, TokenID: "token-1",
	}})
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if len(histories) != 1 || histories[0].Status != domain.MidPriceHistoryStatusError || histories[0].ErrorCode != "CLOB_HTTP_429" {
		t.Fatalf("histories = %#v, want explicit HTTP 429 error", histories)
	}
}

// TestNormalizeMidPricePointsUsesMinuteIntervalEndAndKeepsRawP 验证 Normalize Mid Price Points Uses Minute Interval End And Keeps Raw P 场景下的行为。
func TestNormalizeMidPricePointsUsesMinuteIntervalEndAndKeepsRawP(t *testing.T) {
	windowStart := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	windowEnd := windowStart.Add(2 * time.Minute)
	points, err := normalizeMidPricePoints([]clobMidPricePoint{
		{Timestamp: windowStart.Add(55 * time.Second).Unix(), Price: "0.4100"},
	}, windowStart, windowEnd)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 1 || points[0].IntervalEndAt != windowStart.Add(time.Minute) || points[0].P != "0.4100" {
		t.Fatalf("normalized points = %#v", points)
	}
}

// TestMidPriceHistoryCaptureUsesBatchesOfAtMostTwenty 验证 Mid Price History Capture Uses Batches Of At Most Twenty 场景下的行为。
func TestMidPriceHistoryCaptureUsesBatchesOfAtMostTwenty(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		var body struct {
			Markets []string `json:"markets"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if len(body.Markets) > 20 {
			t.Fatalf("batch contains %d markets", len(body.Markets))
		}
		response := map[string]any{"history": map[string]any{}}
		_ = json.NewEncoder(writer).Encode(response)
	}))
	defer server.Close()
	source, err := NewMidPriceHistorySource(MidPriceHistoryParams{
		BaseURL: server.URL, HTTPClient: server.Client(), Workers: 2, RequestsPerSecond: 100,
	})
	if err != nil {
		t.Fatalf("NewMidPriceHistorySource() error = %v", err)
	}
	targets := make([]domain.BookTarget, 21)
	for index := range targets {
		targets[index] = domain.BookTarget{
			MarketID: "market", ConditionID: "condition", OutcomeIndex: index % 2, TokenID: "token-" + string(rune('a'+index)),
		}
	}
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	if _, err := source.Capture(t.Context(), decisionAt, 48*time.Hour, targets); err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2", calls.Load())
	}
}
