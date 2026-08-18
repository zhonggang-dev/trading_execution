package marketuniverse

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestFindByCondition 验证 Find By Condition 场景下的行为。
func TestFindByCondition(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/markets/by-condition/0xabc" {
			t.Fatalf("path = %q", request.URL.Path)
		}
		if request.Header.Get("Authorization") != "Bearer universe-secret" {
			t.Fatalf("Authorization = %q", request.Header.Get("Authorization"))
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"data":{"market_id":"market-1","condition_id":"0xabc","active":true,"closed":false,"resolved":false,"paused":false,"accepting_orders":true,"neg_risk":false,"tick_size":"0.01","outcomes":[{"index":0,"name":"Yes","token_id":"token-yes"},{"index":1,"name":"No","token_id":"token-no"}],"observed_at":"2026-08-18T08:00:00Z"}}`))
	}))
	defer server.Close()
	client, err := New(Params{BaseURL: server.URL, BearerToken: "universe-secret"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	market, found, err := client.FindByCondition(t.Context(), "0xabc")
	if err != nil || !found || market.MarketID != "market-1" || len(market.Outcomes) != 2 {
		t.Fatalf("FindByCondition() = %#v, %v, %v", market, found, err)
	}
}

// TestFindByConditionReturnsNotFound 验证 Find By Condition Returns Not Found 场景下的行为。
func TestFindByConditionReturnsNotFound(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	client, err := New(Params{BaseURL: server.URL, BearerToken: "universe-secret"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, found, err := client.FindByCondition(t.Context(), "0xmissing")
	if err != nil || found {
		t.Fatalf("FindByCondition() found = %v, error = %v", found, err)
	}
}

// TestFindByConditionCarriesClosedAtForResolvedMarket 验证 Find By Condition Carries Closed At For Resolved Market 场景下的行为。
func TestFindByConditionCarriesClosedAtForResolvedMarket(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"data":{"market_id":"market-1","condition_id":"0xabc","active":false,"closed":true,"resolved":true,"closed_at":"2026-08-18T07:59:00+00:00","paused":false,"accepting_orders":false,"neg_risk":false,"tick_size":"0.01","outcomes":[{"index":0,"name":"Yes","token_id":"token-yes"},{"index":1,"name":"No","token_id":"token-no"}],"observed_at":"2026-08-18T08:00:00Z"}}`))
	}))
	defer server.Close()
	client, err := New(Params{BaseURL: server.URL, BearerToken: "universe-secret"})
	if err != nil {
		t.Fatal(err)
	}
	market, found, err := client.FindByCondition(t.Context(), "0xabc")
	if err != nil || !found || market.ClosedAt == nil || market.ClosedAt.Format(time.RFC3339) != "2026-08-18T07:59:00Z" {
		t.Fatalf("resolved market = %#v, found=%v, err=%v", market, found, err)
	}
}
