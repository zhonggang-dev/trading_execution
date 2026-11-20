package polymarketgamma

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// listedMarket 在合法执行字段上叠加列表字段。
func listedMarket(id string, extra map[string]any) map[string]any {
	market := validGammaMarket()
	market["id"] = id
	market["question"] = "Will " + id + "?"
	market["slug"] = "will-" + id
	market["endDate"] = "2026-11-03T12:00:00Z"
	market["volumeNum"] = 9899.565704999999
	market["liquidityNum"] = "5546.03841"
	for key, value := range extra {
		market[key] = value
	}
	return market
}

// TestListMarketsSendsServerSideFiltersAndMapsFields 验证请求参数、数值映射与 Fetched 计数。
func TestListMarketsSendsServerSideFiltersAndMapsFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		query := request.URL.Query()
		want := map[string]string{
			"closed": "false", "active": "true", "enable_order_book": "true", "order": "liquidityNum",
			"ascending": "true", "limit": "100", "offset": "200", "volume_num_max": "150000", "liquidity_num_max": "30000", "liquidity_num_min": "0.01",
		}
		for key, value := range want {
			if query.Get(key) != value {
				t.Errorf("query[%s] = %q, want %q", key, query.Get(key), value)
			}
		}
		writeJSON(t, writer, []any{
			listedMarket("1", nil),
			listedMarket("2", map[string]any{"volumeNum": nil, "liquidityNum": 0.00001}),
		})
	}))
	defer server.Close()
	client, err := New(Params{BaseURL: server.URL, HTTPClient: server.Client(), MaxAttempts: 1,
		Now: func() time.Time { return time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC) }})
	if err != nil {
		t.Fatal(err)
	}
	page, err := client.ListMarkets(t.Context(), domain.MarketListFilter{MaxVolume: "150000", MaxLiquidity: "30000", MinLiquidity: "0.01", Offset: 200, Limit: 100})
	if err != nil {
		t.Fatalf("ListMarkets() error = %v", err)
	}
	if page.Fetched != 2 || len(page.Markets) != 2 || page.LastLiquidity != "0.00001" {
		t.Fatalf("page = %+v", page)
	}
	first, second := page.Markets[0], page.Markets[1]
	if first.MarketID != "1" || first.Question != "Will 1?" || first.Slug != "will-1" || first.Volume != "9899.565705" || first.Liquidity != "5546.03841" ||
		first.EndDate == nil || !first.EndDate.Equal(time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)) ||
		len(first.Outcomes) != 2 || first.Outcomes[0].TokenID != "123456789" {
		t.Fatalf("first = %+v", first)
	}
	if second.Volume != "0" || second.Liquidity != "0.00001" {
		t.Fatalf("second volume/liquidity = %s / %s", second.Volume, second.Liquidity)
	}
}

// TestListMarketsSkipsUntradableAndMalformedMarkets 验证不可交易与脏数据被跳过但仍计入 Fetched。
func TestListMarketsSkipsUntradableAndMalformedMarkets(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(t, writer, []any{
			listedMarket("ok", nil),
			listedMarket("not-accepting", map[string]any{"acceptingOrders": false}),
			listedMarket("accepting-null", map[string]any{"acceptingOrders": nil}),
			listedMarket("three-outcomes", map[string]any{"outcomes": `["A","B","C"]`, "clobTokenIds": `["1","2","3"]`}),
			listedMarket("bad-tick", map[string]any{"orderPriceMinTickSize": nil}),
		})
	}))
	defer server.Close()
	client, _ := New(Params{BaseURL: server.URL, HTTPClient: server.Client(), MaxAttempts: 1})
	page, err := client.ListMarkets(t.Context(), domain.MarketListFilter{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if page.Fetched != 5 || len(page.Markets) != 1 || page.Markets[0].MarketID != "ok" {
		t.Fatalf("page = %+v", page)
	}
}

// TestListMarketsRejectsOutOfRangePaging 验证页大小与 offset 越界在发请求前被拒绝。
func TestListMarketsRejectsOutOfRangePaging(t *testing.T) {
	client, _ := New(Params{BaseURL: "https://gamma.example", MaxAttempts: 1})
	for name, filter := range map[string]domain.MarketListFilter{
		"limit 为 0":      {Limit: 0},
		"limit 超过 100":   {Limit: 101},
		"offset 超过 2000": {Limit: 100, Offset: 2001},
		"offset 为负":      {Limit: 100, Offset: -1},
	} {
		if _, err := client.ListMarkets(t.Context(), filter); err == nil {
			t.Errorf("%s: error = nil", name)
		}
	}
}
