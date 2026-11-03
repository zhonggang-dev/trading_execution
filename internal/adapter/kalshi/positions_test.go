package kalshi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetMarketPositionReadsTheSignedNetContracts(t *testing.T) {
	var method, path, ticker string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path, ticker = r.Method, r.URL.Path, r.URL.Query().Get("ticker")
		_, _ = w.Write([]byte(`{"cursor":"","event_positions":[],"market_positions":[
			{"ticker":"KXRT-GOD-92","position_fp":"-1.00","total_traded_dollars":"19.000000"}]}`))
	}))
	defer server.Close()
	client := testClient(t, server.URL, "key", testPrivateKey(t), false)
	position, err := client.GetMarketPosition(context.Background(), " KXRT-GOD-92 ")
	if err != nil || position != "-1.00" {
		t.Fatalf("GetMarketPosition() = %q, %v, want -1.00", position, err)
	}
	if method != http.MethodGet || path != "/trade-api/v2/portfolio/positions" || ticker != "KXRT-GOD-92" {
		t.Fatalf("request = %s %s ticker=%q", method, path, ticker)
	}
}

func TestGetMarketPositionTreatsAMissingRowAsZeroAndFollowsTheCursor(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			_, _ = w.Write([]byte(`{"cursor":"next","market_positions":[]}`))
			return
		}
		if r.URL.Query().Get("cursor") != "next" {
			t.Errorf("second page cursor = %q", r.URL.Query().Get("cursor"))
		}
		_, _ = w.Write([]byte(`{"cursor":"","market_positions":[]}`))
	}))
	defer server.Close()
	client := testClient(t, server.URL, "key", testPrivateKey(t), false)
	position, err := client.GetMarketPosition(context.Background(), "KX-A")
	if err != nil || position != "0" || calls != 2 {
		t.Fatalf("GetMarketPosition() = %q, %v after %d calls", position, err, calls)
	}
}

func TestGetMarketPositionRejectsAmbiguousResponses(t *testing.T) {
	for name, body := range map[string]string{
		"missing collection": `{"cursor":""}`,
		"another market":     `{"cursor":"","market_positions":[{"ticker":"KX-B","position_fp":"1.00"}]}`,
		"missing position":   `{"cursor":"","market_positions":[{"ticker":"KX-A"}]}`,
		"numeric position":   `{"cursor":"","market_positions":[{"ticker":"KX-A","position_fp":1}]}`,
		"duplicate market":   `{"cursor":"","market_positions":[{"ticker":"KX-A","position_fp":"1.00"},{"ticker":"KX-A","position_fp":"1.00"}]}`,
		"stuck cursor":       `{"cursor":"same","market_positions":[]}`,
		"not json":           `nope`,
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
		client := testClient(t, server.URL, "key", testPrivateKey(t), false)
		if _, err := client.GetMarketPosition(context.Background(), "KX-A"); err == nil {
			t.Errorf("%s: expected an error", name)
		}
		server.Close()
	}
	client := testClient(t, "https://example.test", "key", testPrivateKey(t), false)
	if _, err := client.GetMarketPosition(context.Background(), " "); err == nil {
		t.Error("an empty ticker must be rejected")
	}
}
