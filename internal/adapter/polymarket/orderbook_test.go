package polymarket

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// TestNormalizeLevelsKeepsExactlyNearestFifteenWhenAvailable 验证 Normalize Levels Keeps Exactly Nearest Fifteen When Available 场景下的行为。
func TestNormalizeLevelsKeepsExactlyNearestFifteenWhenAvailable(t *testing.T) {
	asks := make([]clobLevel, 20)
	for index := range asks {
		asks[index] = clobLevel{Price: fmt.Sprintf("0.%02d", 70-index), Size: "1"}
	}
	levels, err := normalizeLevels(asks, false, domain.StrategyOrderBookDepth)
	if err != nil {
		t.Fatal(err)
	}
	if len(levels) != 15 || levels[0].Price != "0.51" || levels[14].Price != "0.65" {
		t.Fatalf("top-15 asks = %#v", levels)
	}
}

// TestCaptureNormalizesAndTrimsBooks 验证 Capture Normalizes And Trims Books 场景下的行为。
func TestCaptureNormalizesAndTrimsBooks(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("token_id") != "token-1" {
			t.Fatalf("token_id = %q", request.URL.Query().Get("token_id"))
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{
			"timestamp":"1787026800123",
			"bids":[{"price":"0.40","size":"3"},{"price":"0.45","size":"2"},{"price":"0.42","size":"1"}],
			"asks":[{"price":"0.60","size":"3"},{"price":"0.52","size":"2"},{"price":"0.55","size":"1"}]
		}`))
	}))
	defer server.Close()
	now := time.Date(2026, 8, 18, 4, 20, 1, 0, time.UTC)
	source, err := NewOrderBookSource(OrderBookParams{
		BaseURL: server.URL, HTTPClient: server.Client(), Depth: 2, Workers: 1,
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewOrderBookSource() error = %v", err)
	}
	books, err := source.Capture(t.Context(), now, []domain.BookTarget{{
		MarketID: "market-1", ConditionID: "condition-1", OutcomeIndex: 0, TokenID: "token-1",
	}})
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if len(books) != 1 || books[0].Status != domain.OrderBookStatusOK || len(books[0].Bids) != 2 || len(books[0].Asks) != 2 {
		t.Fatalf("books = %#v, want one top-2 book", books)
	}
	if books[0].Bids[0].Price != "0.45" || books[0].Asks[0].Price != "0.52" ||
		books[0].BestBid != "0.45" || books[0].BestAsk != "0.52" {
		t.Fatalf("best levels = %#v / %#v, want sorted best bid/ask", books[0].Bids, books[0].Asks)
	}
}

// TestCaptureRepresentsPerTokenFailureInBand 验证 Capture Represents Per Token Failure In Band 场景下的行为。
func TestCaptureRepresentsPerTokenFailureInBand(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	source, err := NewOrderBookSource(OrderBookParams{BaseURL: server.URL, HTTPClient: server.Client(), Workers: 1})
	if err != nil {
		t.Fatalf("NewOrderBookSource() error = %v", err)
	}
	books, err := source.Capture(t.Context(), time.Now(), []domain.BookTarget{{TokenID: "token-1"}})
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if len(books) != 1 || books[0].Status != domain.OrderBookStatusError || books[0].ErrorCode != "CLOB_HTTP_429" {
		t.Fatalf("books = %#v, want explicit per-token error", books)
	}
}
