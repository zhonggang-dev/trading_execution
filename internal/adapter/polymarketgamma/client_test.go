package polymarketgamma

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testConditionID      = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	otherTestConditionID = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// TestFindByConditionMapsAuthoritativeOpenMarket 验证公开 Gamma 字段被精确映射并同时查询 open/closed 分区。
func TestFindByConditionMapsAuthoritativeOpenMarket(t *testing.T) {
	observedAt := time.Date(2026, 8, 19, 12, 30, 0, 0, time.FixedZone("UTC+8", 8*60*60))
	requestedConditionID := "0x" + strings.Repeat("A", 64)
	seen := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/gamma/markets" {
			t.Fatalf("path = %q", request.URL.Path)
		}
		query := request.URL.Query()
		if query.Get("condition_ids") != testConditionID || query.Get("limit") != "2" {
			t.Fatalf("query = %v", query)
		}
		if request.Header.Get("Accept") != "application/json" || request.Header.Get("User-Agent") != "trading-execution-test" {
			t.Fatalf("headers = %v", request.Header)
		}
		closed := query.Get("closed")
		seen[closed]++
		if closed == "false" {
			writeJSON(t, writer, []any{validGammaMarket()})
			return
		}
		writeJSON(t, writer, []any{})
	}))
	defer server.Close()

	client, err := New(Params{
		BaseURL: server.URL + "/gamma", HTTPClient: server.Client(), MaxAttempts: 1,
		UserAgent: "trading-execution-test", Now: func() time.Time { return observedAt },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	market, found, err := client.FindByCondition(t.Context(), requestedConditionID)
	if err != nil || !found {
		t.Fatalf("FindByCondition() found=%v error=%v", found, err)
	}
	if market.MarketID != "559651" || market.ConditionID != requestedConditionID || !market.Active || market.Closed ||
		market.Resolved || market.Paused || !market.AcceptingOrders || market.NegRisk || market.TickSize != "0.001" {
		t.Fatalf("market state = %#v", market)
	}
	if !market.ObservedAt.Equal(time.Date(2026, 8, 19, 4, 30, 0, 0, time.UTC)) || market.ClosedAt != nil {
		t.Fatalf("market times = observed %v closed %v", market.ObservedAt, market.ClosedAt)
	}
	if len(market.Outcomes) != 2 || market.Outcomes[0].Index != 0 || market.Outcomes[0].Name != "Yes" ||
		market.Outcomes[0].TokenID != "123456789" || market.Outcomes[1].Index != 1 ||
		market.Outcomes[1].Name != "No" || market.Outcomes[1].TokenID != "987654321" {
		t.Fatalf("outcomes = %#v", market.Outcomes)
	}
	if seen["false"] != 1 || seen["true"] != 1 {
		t.Fatalf("closed partitions seen = %v", seen)
	}
}

// TestFindByConditionMapsClosedResolvedMarket 验证 closedTime、resolved 和直接数组兼容映射。
func TestFindByConditionMapsClosedResolvedMarket(t *testing.T) {
	closed := validGammaMarket()
	closed["active"] = false
	closed["closed"] = true
	closed["acceptingOrders"] = false
	closed["closedTime"] = "2026-08-18 07:59:00+00"
	closed["umaResolutionStatus"] = "resolved"
	closed["orderPriceMinTickSize"] = "0.01"
	closed["outcomes"] = []string{"Up", "Down"}
	closed["clobTokenIds"] = []string{"123456789", "987654321"}

	server := partitionServer(t, nil, []any{closed})
	defer server.Close()
	client, err := New(Params{
		BaseURL: server.URL, HTTPClient: server.Client(), MaxAttempts: 1,
		Now: func() time.Time { return time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	market, found, err := client.FindByCondition(t.Context(), testConditionID)
	if err != nil || !found {
		t.Fatalf("FindByCondition() found=%v error=%v", found, err)
	}
	if !market.Closed || !market.Resolved || market.Active || market.AcceptingOrders || market.TickSize != "0.01" {
		t.Fatalf("closed market = %#v", market)
	}
	if market.ClosedAt == nil || market.ClosedAt.Format(time.RFC3339) != "2026-08-18T07:59:00Z" {
		t.Fatalf("closed_at = %v", market.ClosedAt)
	}
}

// TestFindByConditionReturnsNotFoundOnlyAfterBothPartitions 验证两个生命周期分区均为空时返回 found=false。
func TestFindByConditionReturnsNotFoundOnlyAfterBothPartitions(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writeJSON(t, writer, []any{})
	}))
	defer server.Close()
	client, err := New(Params{BaseURL: server.URL, HTTPClient: server.Client(), MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, found, err := client.FindByCondition(t.Context(), testConditionID)
	if err != nil || found || requests.Load() != 2 {
		t.Fatalf("FindByCondition() found=%v requests=%d error=%v", found, requests.Load(), err)
	}
}

// TestFindByConditionRejectsAmbiguousOrMismatchedResults 验证服务端过滤失效和重复结果会 fail closed。
func TestFindByConditionRejectsAmbiguousOrMismatchedResults(t *testing.T) {
	tests := []struct {
		name      string
		open      []any
		closed    []any
		wantError string
	}{
		{
			name:      "duplicate in one partition",
			open:      []any{validGammaMarket(), validGammaMarket()},
			wantError: "multiple open markets",
		},
		{
			name: "same condition appears in both partitions",
			open: []any{validGammaMarket()}, closed: []any{marketWith("closed", true)},
			wantError: "returned 2 markets",
		},
		{
			name:      "server ignores condition filter",
			open:      []any{marketWith("conditionId", otherTestConditionID)},
			wantError: "returned condition_id",
		},
		{
			name:      "server contradicts lifecycle filter",
			closed:    []any{validGammaMarket()},
			wantError: "contradicts its closed filter",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := partitionServer(t, test.open, test.closed)
			defer server.Close()
			client, err := New(Params{BaseURL: server.URL, HTTPClient: server.Client(), MaxAttempts: 1})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = client.FindByCondition(t.Context(), testConditionID)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("FindByCondition() error = %v, want containing %q", err, test.wantError)
			}
		})
	}
}

// TestFindByConditionRejectsMissingOrInvalidTrustedFields 验证所有下单相关 Gamma 字段都必须存在且自洽。
func TestFindByConditionRejectsMissingOrInvalidTrustedFields(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(map[string]any)
		wantError string
	}{
		{name: "missing active", mutate: func(value map[string]any) { delete(value, "active") }, wantError: "missing required state fields"},
		{name: "missing accepting orders", mutate: func(value map[string]any) { delete(value, "acceptingOrders") }, wantError: "missing required state fields"},
		{name: "missing order book flag", mutate: func(value map[string]any) { delete(value, "enableOrderBook") }, wantError: "missing required state fields"},
		{name: "missing neg risk", mutate: func(value map[string]any) { delete(value, "negRisk") }, wantError: "missing required state fields"},
		{name: "zero tick", mutate: func(value map[string]any) { value["orderPriceMinTickSize"] = 0 }, wantError: "value must be positive"},
		{name: "exponent tick", mutate: func(value map[string]any) { value["orderPriceMinTickSize"] = "1e-3" }, wantError: "invalid base-10 decimal"},
		{name: "too few outcomes", mutate: func(value map[string]any) { value["outcomes"] = `["Yes"]` }, wantError: "exactly two outcomes"},
		{name: "duplicate token", mutate: func(value map[string]any) { value["clobTokenIds"] = `["123456789","123456789"]` }, wantError: "token ids are duplicated"},
		{name: "non-uint256 token", mutate: func(value map[string]any) { value["clobTokenIds"] = `["-1","987654321"]` }, wantError: "unsigned decimal integer"},
		{name: "open with closed time", mutate: func(value map[string]any) { value["closedTime"] = "2026-08-18T07:59:00Z" }, wantError: "open market cannot contain closedTime"},
		{name: "inactive accepting orders", mutate: func(value map[string]any) { value["active"] = false }, wantError: "accepting orders must be active"},
		{name: "disabled book accepting orders", mutate: func(value map[string]any) { value["enableOrderBook"] = false }, wantError: "accepting orders must be active"},
		{name: "resolved but open", mutate: func(value map[string]any) { value["umaResolutionStatus"] = "RESOLVED" }, wantError: "resolved market must be closed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			market := validGammaMarket()
			test.mutate(market)
			server := partitionServer(t, []any{market}, nil)
			defer server.Close()
			client, err := New(Params{BaseURL: server.URL, HTTPClient: server.Client(), MaxAttempts: 1})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = client.FindByCondition(t.Context(), testConditionID)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("FindByCondition() error = %v, want containing %q", err, test.wantError)
			}
		})
	}
}

// TestFindByConditionRetriesTransientHTTPError 验证 5xx 有界重试，而 4xx 不重试。
func TestFindByConditionRetriesTransientHTTPError(t *testing.T) {
	var openAttempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("closed") == "false" {
			if openAttempts.Add(1) == 1 {
				http.Error(writer, "temporary", http.StatusServiceUnavailable)
				return
			}
			writeJSON(t, writer, []any{validGammaMarket()})
			return
		}
		writeJSON(t, writer, []any{})
	}))
	defer server.Close()
	client, err := New(Params{BaseURL: server.URL, HTTPClient: server.Client(), MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	_, found, err := client.FindByCondition(t.Context(), testConditionID)
	if err != nil || !found || openAttempts.Load() != 2 {
		t.Fatalf("FindByCondition() found=%v attempts=%d error=%v", found, openAttempts.Load(), err)
	}
}

// TestNewAndFindByConditionRejectInvalidInput 验证配置和 condition_id 在发请求前校验。
func TestNewAndFindByConditionRejectInvalidInput(t *testing.T) {
	for _, baseURL := range []string{"", "ftp://gamma.example", "https://user:pass@gamma.example", "https://gamma.example?x=1"} {
		if _, err := New(Params{BaseURL: baseURL}); err == nil {
			t.Fatalf("New(%q) expected error", baseURL)
		}
	}
	if _, err := New(Params{BaseURL: "https://gamma.example", MaxAttempts: 7}); err == nil {
		t.Fatal("New() expected MaxAttempts error")
	}
	client, err := New(Params{BaseURL: "https://gamma.example", MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.FindByCondition(t.Context(), "0xabc"); err == nil || !strings.Contains(err.Error(), "32-byte") {
		t.Fatalf("FindByCondition() error = %v", err)
	}
}

func validGammaMarket() map[string]any {
	return map[string]any{
		"id": "559651", "conditionId": testConditionID,
		"active": true, "closed": false, "acceptingOrders": true,
		"enableOrderBook": true, "negRisk": false,
		"orderPriceMinTickSize": 0.001,
		"outcomes":              `["Yes", "No"]`,
		"clobTokenIds":          `["123456789", "987654321"]`,
		"umaResolutionStatus":   "",
	}
}

func marketWith(key string, value any) map[string]any {
	market := validGammaMarket()
	market[key] = value
	return market
}

func partitionServer(t *testing.T, open, closed []any) *httptest.Server {
	t.Helper()
	if open == nil {
		open = []any{}
	}
	if closed == nil {
		closed = []any{}
	}
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Query().Get("closed") {
		case "false":
			writeJSON(t, writer, open)
		case "true":
			writeJSON(t, writer, closed)
		default:
			t.Fatalf("closed query = %q", request.URL.Query().Get("closed"))
		}
	}))
}

func writeJSON(t *testing.T, writer http.ResponseWriter, value any) {
	t.Helper()
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
