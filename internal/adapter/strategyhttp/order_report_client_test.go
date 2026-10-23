package strategyhttp

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func testReports() []domain.StrategyOrderReport {
	execution := domain.StrategyExecutionContext{ModelID: "deepseek_masked", StrategyID: "brier_mid_edge_once", ExecutionAccountID: "main"}
	return []domain.StrategyOrderReport{
		{ReportSeq: 2, Context: execution, DecisionID: "entry-1", Status: domain.StrategyReportFilled,
			CumulativeFilledShares: "18", AverageFillPrice: "0.53", FeesUSD: "0", EventAt: time.Date(2026, 10, 23, 1, 0, 0, 0, time.UTC)},
		{ReportSeq: 1, Context: execution, DecisionID: "entry-2", Status: domain.StrategyReportNotSubmitted,
			CumulativeFilledShares: "0", FailureCode: "INSUFFICIENT_WALLET_BALANCE", EventAt: time.Date(2026, 10, 23, 1, 0, 0, 0, time.UTC)},
	}
}

func TestReportOrdersPostsContractEnvelopeAndMapsResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/api/v1/strategy-order-reports" ||
			request.Header.Get("Authorization") != "Bearer token" {
			t.Fatalf("request = %s %s auth=%q", request.Method, request.URL.Path, request.Header.Get("Authorization"))
		}
		body, _ := io.ReadAll(request.Body)
		var payload struct {
			SchemaVersion string           `json:"schema_version"`
			Reports       []map[string]any `json:"reports"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if payload.SchemaVersion != "trading.strategy_order_report.v1" || len(payload.Reports) != 2 {
			t.Fatalf("payload = %s", body)
		}
		first, second := payload.Reports[0], payload.Reports[1]
		if first["status"] != "FILLED" || first["average_fill_price"] != "0.53" || first["cumulative_filled_shares"] != "18" || first["report_seq"] != float64(2) {
			t.Fatalf("first report = %#v", first)
		}
		if _, present := second["average_fill_price"]; present || second["failure_code"] != "INSUFFICIENT_WALLET_BALANCE" {
			t.Fatalf("second report = %#v", second)
		}
		_, _ = writer.Write([]byte(`{"data":{"schema_version":"trading.strategy_order_report_ack.v1","extra":1,"results":[
			{"index":1,"decision_id":"entry-2","report_seq":1,"outcome":"error","error":{"code":"UNKNOWN_DECISION","message":"unknown","retryable":false}},
			{"index":0,"decision_id":"entry-1","report_seq":2,"outcome":"applied,market_traded"}]}}`))
	}))
	defer server.Close()
	client, err := New(Params{BaseURL: server.URL, BearerToken: "token", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	results, err := client.ReportOrders(t.Context(), testReports())
	if err != nil {
		t.Fatalf("ReportOrders() error = %v", err)
	}
	if len(results) != 2 || results[0].DecisionID != "entry-1" || results[0].Outcome != "applied,market_traded" || results[0].ErrorCode != "" ||
		results[1].DecisionID != "entry-2" || results[1].ErrorCode != "UNKNOWN_DECISION" {
		t.Fatalf("results = %#v", results)
	}
}

func TestReportOrdersFailsWholeCallOnHTTPErrorOrShortAck(t *testing.T) {
	status, body := http.StatusServiceUnavailable, `{"error":{"code":"BUSY"}}`
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(status)
		_, _ = writer.Write([]byte(body))
	}))
	defer server.Close()
	client, err := New(Params{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ReportOrders(t.Context(), testReports()); err == nil || !strings.Contains(err.Error(), "HTTP 503") {
		t.Fatalf("503 error = %v", err)
	}
	status, body = http.StatusOK, `{"data":{"results":[{"index":0,"decision_id":"entry-1","report_seq":2,"outcome":"applied"}]}}`
	if _, err := client.ReportOrders(t.Context(), testReports()); err == nil || !strings.Contains(err.Error(), "1 results for 2 reports") {
		t.Fatalf("short ack error = %v", err)
	}
	if _, err := client.ReportOrders(t.Context(), nil); err == nil {
		t.Fatal("empty batch was accepted")
	}
}
