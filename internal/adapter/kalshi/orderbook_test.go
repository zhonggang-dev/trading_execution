package kalshi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestOrderBookSourceNormalizesYesAndNoBooks(t *testing.T) {
	privateKey := testPrivateKey(t)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		verifySignature(t, request, "key", &privateKey.PublicKey)
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/trade-api/v2/markets/TEST-MARKET":
			_, _ = writer.Write([]byte(`{"market":{"ticker":"TEST-MARKET","market_type":"binary","status":"active","result":"","fractional_trading_enabled":false,"price_ranges":[{"step":"0.01"}]}}`))
		case "/trade-api/v2/markets/TEST-MARKET/orderbook":
			if request.URL.Query().Get("depth") != "15" {
				t.Errorf("depth = %q", request.URL.Query().Get("depth"))
			}
			_, _ = writer.Write([]byte(`{"orderbook_fp":{"yes_dollars":[["0.50","9.00"],["0.55","4.00"]],"no_dollars":[["0.35","8.00"],["0.40","3.00"]]}}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)
	client := testClient(t, server.URL, "key", privateKey, false)
	observedAt := time.Date(2026, 8, 26, 1, 2, 3, 0, time.UTC)
	source, err := NewOrderBookSource(OrderBookParams{Client: client, Now: func() time.Time { return observedAt }})
	if err != nil {
		t.Fatalf("NewOrderBookSource() error = %v", err)
	}
	targets := []domain.BookTarget{
		{MarketSource: domain.MarketSourceKalshi, MarketID: "TEST-MARKET", ConditionID: "kalshi:TEST-MARKET", OutcomeIndex: 0, OutcomeID: "YES", TokenID: "kalshi:TEST-MARKET:YES"},
		{MarketSource: domain.MarketSourceKalshi, MarketID: "TEST-MARKET", ConditionID: "kalshi:TEST-MARKET", OutcomeIndex: 1, OutcomeID: "NO", TokenID: "kalshi:TEST-MARKET:NO"},
	}
	books, err := source.Capture(context.Background(), observedAt, targets)
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if len(books) != 2 {
		t.Fatalf("books = %d", len(books))
	}
	for index := range books {
		if err := books[index].Validate(); err != nil {
			t.Fatalf("book %d Validate() error = %v; book = %#v", index, err, books[index])
		}
	}
	if books[0].BestBid != "0.55" || books[0].BestAsk != "0.6" || books[0].Bids[0].Size != "4.00" || books[0].Asks[0].Size != "3.00" {
		t.Fatalf("YES book = %#v", books[0])
	}
	if books[1].BestBid != "0.40" || books[1].BestAsk != "0.45" || books[1].Bids[0].Size != "3.00" || books[1].Asks[0].Size != "4.00" {
		t.Fatalf("NO book = %#v", books[1])
	}
}

func TestOrderBookSourceFailsClosedForUntradableMarket(t *testing.T) {
	privateKey := testPrivateKey(t)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		verifySignature(t, request, "key", &privateKey.PublicKey)
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"market":{"ticker":"TEST-MARKET","market_type":"binary","status":"closed","result":"yes"}}`))
	}))
	t.Cleanup(server.Close)
	source, err := NewOrderBookSource(OrderBookParams{Client: testClient(t, server.URL, "key", privateKey, false)})
	if err != nil {
		t.Fatalf("NewOrderBookSource() error = %v", err)
	}
	books, err := source.Capture(context.Background(), time.Now(), []domain.BookTarget{{
		MarketSource: domain.MarketSourceKalshi, MarketID: "TEST-MARKET", ConditionID: "kalshi:TEST-MARKET",
		OutcomeIndex: 0, OutcomeID: "YES", TokenID: "kalshi:TEST-MARKET:YES",
	}})
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if books[0].Status != domain.OrderBookStatusError || books[0].ErrorCode != "KALSHI_MARKET_NOT_TRADABLE" {
		t.Fatalf("book = %#v", books[0])
	}
}
