package polymarketdata

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPnLClientParsesSortsFiltersNullAndDeduplicates(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v2/user-pnl" || request.URL.Query().Get("user") == "" {
			t.Fatalf("unexpected request: %s", request.URL.String())
		}
		writer.Header().Set("content-type", "application/json")
		_, _ = writer.Write([]byte(`{"data":{"points":[{"timestamp":20,"trade_pnl":2.5},{"timestamp":10,"trade_pnl":"1.25"},{"timestamp":20,"trade_pnl":2.50},{"timestamp":30,"trade_pnl":null}]}}`))
	}))
	defer server.Close()
	client, err := NewPnLClient(PnLClientParams{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	series, err := client.Series(context.Background(), "0x1111111111111111111111111111111111111111", "1w", "3h")
	if err != nil {
		t.Fatal(err)
	}
	if len(series.Points) != 2 || series.Points[0].Timestamp != 10 || series.Points[1].Timestamp != 20 {
		t.Fatalf("points = %#v", series.Points)
	}
	if series.Points[0].TradePnL.String() != "1.25" || series.Points[1].TradePnL.String() != "2.5" {
		t.Fatalf("values = %#v", series.Points)
	}
}

func TestPnLClientRejectsConflictingDuplicate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"data":{"points":[{"timestamp":10,"trade_pnl":1},{"timestamp":10,"trade_pnl":2}]}}`))
	}))
	defer server.Close()
	client, err := NewPnLClient(PnLClientParams{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Series(context.Background(), "0x1111111111111111111111111111111111111111", "1w", "3h"); err == nil {
		t.Fatal("expected conflicting duplicate error")
	}
}

func TestPnLClientRejectsMissingStructureAndTrailingJSON(t *testing.T) {
	tests := []string{
		`{}`,
		`{"data":{}}`,
		`{"data":{"points":[{"timestamp":0,"trade_pnl":1}]}}`,
		`{"data":{"points":[{"timestamp":1}]}}`,
		`{"data":{"points":[]}} {"data":{"points":[]}}`,
	}
	for _, body := range tests {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { _, _ = writer.Write([]byte(body)) }))
		client, err := NewPnLClient(PnLClientParams{BaseURL: server.URL, HTTPClient: server.Client()})
		if err != nil {
			server.Close()
			t.Fatal(err)
		}
		_, err = client.Series(context.Background(), "0x1111111111111111111111111111111111111111", "1w", "3h")
		server.Close()
		if err == nil {
			t.Fatalf("body %q was accepted", body)
		}
	}
}

func TestPnLClientNormalizesScientificDecimalExactly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"data":{"points":[
			{"timestamp":1,"trade_pnl":1e-7},
			{"timestamp":1,"trade_pnl":"0.0000001"},
			{"timestamp":2,"trade_pnl":"-2.5e-3"},
			{"timestamp":3,"trade_pnl":"1.20e2"}
		]}}`))
	}))
	defer server.Close()
	client, err := NewPnLClient(PnLClientParams{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	series, err := client.Series(context.Background(), "0x1111111111111111111111111111111111111111", "1w", "3h")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"0.0000001", "-0.0025", "120"}
	if len(series.Points) != len(want) {
		t.Fatalf("point count = %d, want %d", len(series.Points), len(want))
	}
	for index, value := range want {
		if got := series.Points[index].TradePnL.String(); got != value {
			t.Fatalf("point %d = %q, want %q", index, got, value)
		}
	}
}

func TestPnLClientRejectsNonDecimalOrHugeScientificValues(t *testing.T) {
	for _, value := range []string{`"0x10"`, `"1/2"`, `"NaN"`, `"Inf"`, `"1e9999"`, `"1e-9999"`} {
		body := `{"data":{"points":[{"timestamp":1,"trade_pnl":` + value + `}]}}`
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { _, _ = writer.Write([]byte(body)) }))
		client, err := NewPnLClient(PnLClientParams{BaseURL: server.URL, HTTPClient: server.Client()})
		if err != nil {
			server.Close()
			t.Fatal(err)
		}
		_, err = client.Series(context.Background(), "0x1111111111111111111111111111111111111111", "1w", "3h")
		server.Close()
		if err == nil {
			t.Fatalf("value %s was accepted", value)
		}
	}
}
