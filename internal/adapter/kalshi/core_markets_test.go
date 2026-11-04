package kalshi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCoreMarketsSignedReadBatchAndNoTradingGate(t *testing.T) {
	key := testPrivateKey(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		verifySignature(t, r, "key", &key.PublicKey)
		if r.Method != "GET" || r.URL.Path != "/trade-api/v2/markets" || r.URL.Query().Get("tickers") != "A,B" || r.URL.Query().Has("status") {
			t.Errorf("invalid read request %s", r.URL)
		}
		w.Write([]byte(`{"markets":[{"ticker":"A","market_type":"binary","status":"finalized","result":"yes","settlement_value_dollars":"1","settlement_ts":"2026-10-01T00:00:00Z"}],"cursor":""}`))
	}))
	defer server.Close()
	client := testClient(t, server.URL, "key", key, false)
	got, e := client.CoreMarkets(context.Background(), []string{"A", "B"})
	if e != nil || len(got) != 1 || got[0].Status != "finalized" {
		t.Fatal(got, e)
	}
	for _, invalid := range [][]string{nil, {"A", "A"}, {"A,B"}} {
		if _, e = client.CoreMarkets(context.Background(), invalid); e == nil {
			t.Fatal("invalid ticker accepted")
		}
	}
}
