package kalshi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestMidPriceHistorySourceBuildsYesAndNoMinuteSeries(t *testing.T) {
	privateKey := testPrivateKey(t)
	decisionAt := time.Date(2026, 8, 26, 4, 20, 0, 0, time.UTC)
	windowStart := decisionAt.Add(-3 * time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		verifySignature(t, request, "key", &privateKey.PublicKey)
		if request.URL.Path != "/trade-api/v2/markets/candlesticks" ||
			request.URL.Query().Get("market_tickers") != "TEST-MARKET" || request.URL.Query().Get("period_interval") != "1" {
			t.Errorf("request URL = %s", request.URL.String())
		}
		candles := make([]map[string]any, 0, 181)
		for pointAt := windowStart; !pointAt.After(decisionAt); pointAt = pointAt.Add(time.Minute) {
			candles = append(candles, map[string]any{
				"end_period_ts": pointAt.Unix(),
				"yes_bid":       map[string]string{"close_dollars": "0.2000"},
				"yes_ask":       map[string]string{"close_dollars": "0.4000"},
			})
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{"markets": []any{map[string]any{
			"market_ticker": "TEST-MARKET", "candlesticks": candles,
		}}})
	}))
	t.Cleanup(server.Close)
	source, err := NewMidPriceHistorySource(MidPriceHistoryParams{
		Client: testClient(t, server.URL, "key", privateKey, false), Now: func() time.Time { return decisionAt.Add(time.Second) },
	})
	if err != nil {
		t.Fatalf("NewMidPriceHistorySource() error = %v", err)
	}
	targets := []domain.BookTarget{
		{MarketSource: domain.MarketSourceKalshi, MarketID: "TEST-MARKET", ConditionID: "kalshi:TEST-MARKET", OutcomeIndex: 0, OutcomeID: "YES", TokenID: "kalshi:TEST-MARKET:YES"},
		{MarketSource: domain.MarketSourceKalshi, MarketID: "TEST-MARKET", ConditionID: "kalshi:TEST-MARKET", OutcomeIndex: 1, OutcomeID: "NO", TokenID: "kalshi:TEST-MARKET:NO"},
	}
	histories, err := source.Capture(context.Background(), decisionAt, 3*time.Hour, targets)
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	for index := range histories {
		if err := histories[index].Validate(); err != nil {
			t.Fatalf("history %d Validate() error = %v", index, err)
		}
		if histories[index].Status != domain.MidPriceHistoryStatusOK || len(histories[index].MidPrices) != 181 {
			t.Fatalf("history %d = %#v", index, histories[index])
		}
	}
	if histories[0].MidPrices[0].P != "0.3" || histories[1].MidPrices[0].P != "0.7" {
		t.Fatalf("YES/NO points = %s/%s", histories[0].MidPrices[0].P, histories[1].MidPrices[0].P)
	}
}
