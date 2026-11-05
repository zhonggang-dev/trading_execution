package kalshi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestListPositionsReadsEveryNonZeroMarketAcrossPages(t *testing.T) {
	var queries []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/trade-api/v2/portfolio/positions" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		queries = append(queries, r.URL.RawQuery)
		if r.URL.Query().Get("cursor") == "" {
			_, _ = w.Write([]byte(`{"cursor":"p2","market_positions":[
				{"ticker":"KX-A","position_fp":"3.00"},{"ticker":"KX-ZERO","position_fp":"0.00"}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"cursor":"","market_positions":[{"ticker":"KX-B","position_fp":"-2.00"}]}`))
	}))
	defer server.Close()
	client := testClient(t, server.URL, "key", testPrivateKey(t), false)
	positions, err := client.ListPositions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(positions) != 2 || positions[0].Ticker != "KX-A" || positions[0].PositionFP != "3.00" ||
		positions[1].Ticker != "KX-B" || positions[1].PositionFP != "-2.00" || len(positions[1].Raw) == 0 {
		t.Fatalf("positions = %#v, want KX-A 3 and KX-B -2 without the zero row", positions)
	}
	for _, query := range queries {
		if strings.Contains(query, "count_filter") || strings.Contains(query, "subaccount") || strings.Contains(query, "ticker=") {
			t.Fatalf("query %q must not filter on the server", query)
		}
	}
}

func TestListPositionsRejectsAmbiguousResponses(t *testing.T) {
	for name, body := range map[string]string{
		"missing collection": `{"cursor":""}`,
		"empty ticker":       `{"cursor":"","market_positions":[{"ticker":"","position_fp":"1.00"}]}`,
		"missing position":   `{"cursor":"","market_positions":[{"ticker":"KX-A"}]}`,
		"duplicate market":   `{"cursor":"","market_positions":[{"ticker":"KX-A","position_fp":"1.00"},{"ticker":"KX-A","position_fp":"0.00"}]}`,
		"stuck cursor":       `{"cursor":"same","market_positions":[]}`,
		"not json":           `nope`,
	} {
		calls := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls++
			if name == "stuck cursor" && calls == 1 {
				_, _ = w.Write([]byte(`{"cursor":"same","market_positions":[]}`))
				return
			}
			_, _ = w.Write([]byte(body))
		}))
		client := testClient(t, server.URL, "key", testPrivateKey(t), false)
		if _, err := client.ListPositions(context.Background()); err == nil {
			t.Errorf("%s: expected an error", name)
		}
		server.Close()
	}
	pages := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		pages++
		_, _ = w.Write([]byte(fmt.Sprintf(`{"cursor":"c%d","market_positions":[]}`, pages)))
	}))
	defer server.Close()
	client := testClient(t, server.URL, "key", testPrivateKey(t), false)
	if _, err := client.ListPositions(context.Background()); err == nil || pages != 20 {
		t.Fatalf("endless pagination = %v after %d pages, want an error after 20", err, pages)
	}
}

func TestListMarketFillsFiltersByTickerAndChecksIdentity(t *testing.T) {
	var tickers []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/trade-api/v2/portfolio/fills" || r.URL.Query().Get("order_id") != "" {
			t.Errorf("request = %s ?%s", r.URL.Path, r.URL.RawQuery)
		}
		tickers = append(tickers, r.URL.Query().Get("ticker"))
		if r.URL.Query().Get("cursor") == "" {
			_, _ = w.Write([]byte(`{"cursor":"n","fills":[{"fill_id":"f1","ticker":"KX-A","count_fp":"2.00"}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"cursor":"","fills":[{"fill_id":"f2","market_ticker":"KX-A","count_fp":"1.00"}]}`))
	}))
	defer server.Close()
	client := testClient(t, server.URL, "key", testPrivateKey(t), false)
	fills, err := client.ListMarketFills(context.Background(), " KX-A ")
	if err != nil || len(fills) != 2 || fills[0].FillID != "f1" || fills[1].FillID != "f2" {
		t.Fatalf("ListMarketFills() = %#v, %v", fills, err)
	}
	if strings.Join(tickers, ",") != "KX-A,KX-A" {
		t.Fatalf("ticker filters = %v", tickers)
	}
	for name, body := range map[string]string{
		"another market": `{"cursor":"","fills":[{"fill_id":"f1","ticker":"KX-B","count_fp":"1.00"}]}`,
		"missing id":     `{"cursor":"","fills":[{"ticker":"KX-A","count_fp":"1.00"}]}`,
		"missing count":  `{"cursor":"","fills":[{"fill_id":"f1","ticker":"KX-A"}]}`,
		"missing cursor": `{"fills":[]}`,
	} {
		bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
		if _, err := testClient(t, bad.URL, "key", testPrivateKey(t), false).ListMarketFills(context.Background(), "KX-A"); err == nil {
			t.Errorf("%s: expected an error", name)
		}
		bad.Close()
	}
	if _, err := client.ListMarketFills(context.Background(), "kalshi:KX-A:YES"); err == nil {
		t.Error("a token id must be rejected as a ticker")
	}
}
