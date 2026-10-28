package kalshi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// settlementPage is shaped like a live GET /portfolio/settlements response.
const settlementPage = `{"cursor":%q,"settlements":[{"event_ticker":"KXTESTMATCH-26OCT231730INANEW","exchange_index":0,
"fee_cost":"0.039700","market_result":"no","no_count_fp":"0.00","no_total_cost_dollars":"0.000000","revenue":0,
"settled_time":"2026-10-27T07:25:04.999607Z","ticker":"KXTESTMATCH-26OCT231730INANEW-INA","value":0,
"yes_count_fp":"2.34","yes_total_cost_dollars":"0.959400"}]}`

func TestListSettlementsParsesLiveRecordAndKeepsTheRawRow(t *testing.T) {
	var query string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/trade-api/v2/portfolio/settlements" || r.Method != http.MethodGet {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		query = r.URL.RawQuery
		_, _ = w.Write([]byte(strings.Replace(settlementPage, `%q`, `""`, 1)))
	}))
	defer server.Close()
	client := testClient(t, server.URL, "key", testPrivateKey(t), false)
	records, err := client.ListSettlements(context.Background(), "KXTESTMATCH-26OCT231730INANEW-INA")
	if err != nil || len(records) != 1 {
		t.Fatalf("ListSettlements() = %v, %v", records, err)
	}
	for _, want := range []string{"ticker=KXTESTMATCH-26OCT231730INANEW-INA", "subaccount=0"} {
		if !strings.Contains(query, want) {
			t.Errorf("query %q lacks %q", query, want)
		}
	}
	record := records[0]
	if record.MarketResult != "no" || record.YesCount != "2.34" || record.NoCount != "0.00" ||
		record.Revenue != "0.00" || record.PayoutPerYes != "0.00" || record.FeeCost != "0.039700" {
		t.Fatalf("record = %#v", record)
	}
	if !record.SettledAt.Equal(time.Date(2026, 10, 27, 7, 25, 4, 999607000, time.UTC)) {
		t.Fatalf("settled at = %v", record.SettledAt)
	}
	if !strings.Contains(string(record.Raw), `"yes_count_fp":"2.34"`) {
		t.Fatalf("raw row not preserved: %s", record.Raw)
	}
}

func TestListSettlementsConvertsCentsAndFollowsCursor(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			_, _ = w.Write([]byte(`{"cursor":"next","settlements":[{"ticker":"KX-A","market_result":"yes","yes_count_fp":"12.34","no_count_fp":"0.00","revenue":1234,"value":100,"settled_time":"2026-10-27T07:25:04Z"}]}`))
			return
		}
		if r.URL.Query().Get("cursor") != "next" {
			t.Errorf("second page cursor = %q", r.URL.Query().Get("cursor"))
		}
		_, _ = w.Write([]byte(`{"cursor":"","settlements":[{"ticker":"KX-A","market_result":"yes","yes_count_fp":"1.00","no_count_fp":"0.00","revenue":100,"value":100}]}`))
	}))
	defer server.Close()
	client := testClient(t, server.URL, "key", testPrivateKey(t), false)
	records, err := client.ListSettlements(context.Background(), "KX-A")
	if err != nil || len(records) != 2 {
		t.Fatalf("ListSettlements() = %v, %v", records, err)
	}
	if records[0].Revenue != "12.34" || records[0].PayoutPerYes != "1.00" || records[1].Revenue != "1.00" {
		t.Fatalf("cents were not converted to dollars: %#v", records)
	}
}

func TestListSettlementsRejectsMalformedResponses(t *testing.T) {
	for name, body := range map[string]string{
		"missing collection": `{"cursor":""}`,
		"bad time":           `{"cursor":"","settlements":[{"ticker":"KX","settled_time":"yesterday"}]}`,
		"not json":           `nope`,
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
		client := testClient(t, server.URL, "key", testPrivateKey(t), false)
		if _, err := client.ListSettlements(context.Background(), "KX"); err == nil {
			t.Errorf("%s: expected an error", name)
		}
		server.Close()
	}
	client := testClient(t, "https://example.test", "key", testPrivateKey(t), false)
	if _, err := client.ListSettlements(context.Background(), " "); err == nil {
		t.Error("an empty ticker must be rejected")
	}
}

func TestAvailableBalanceReturnsDollars(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"balance":9802,"portfolio_value":0}`))
	}))
	defer server.Close()
	client := testClient(t, server.URL, "key", testPrivateKey(t), false)
	balance, err := client.AvailableBalance(context.Background())
	if err != nil || balance != "98.02" {
		t.Fatalf("AvailableBalance() = %q, %v, want 98.02", balance, err)
	}
}
