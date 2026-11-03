package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

func TestConsoleV2PortfolioRoutesAndIdentity(t *testing.T) {
	server := testConsoleV2Server(t, &fakeConsoleV2Service{})
	raw := `["main","POLYMARKET","m","c","t",0,"Yes"]`
	path := "/api/v1/console/v2/portfolio-holdings/lots?holding_identity=" + url.QueryEscape(raw) + "&limit=1&offset=2"
	identity, filter, err := parseConsoleV2PortfolioHoldingLotsQuery(httptest.NewRequest(http.MethodGet, path, nil))
	if err != nil || identity.ExecutionAccountID != "main" || identity.MarketSource != "POLYMARKET" || identity.OutcomeIndex == nil || *identity.OutcomeIndex != 0 || filter.Limit != 1 || filter.Offset != 2 {
		t.Fatalf("identity=%#v filter=%#v err=%v", identity, filter, err)
	}
	for _, target := range []string{"/api/v1/console/v2/portfolio-holdings", path} {
		for _, token := range []string{"", "execution-secret", "readonly-secret"} {
			response := performRequest(t, server, http.MethodGet, target, "", token)
			expected := http.StatusUnauthorized
			if token == "execution-secret" {
				expected = http.StatusForbidden
			}
			if token == "readonly-secret" {
				expected = http.StatusOK
			}
			if response.Code != expected {
				t.Fatalf("route %s auth %q status=%d body=%s", target, token, response.Code, response.Body.String())
			}
		}
	}
	invalid := []string{
		"/api/v1/console/v2/portfolio-holdings?model_id=old",
		"/api/v1/console/v2/portfolio-holdings?strategy_id=old",
		"/api/v1/console/v2/portfolio-holdings?from=2026-01-01T00:00:00Z",
		"/api/v1/console/v2/portfolio-holdings?market_source=GENERATED",
		"/api/v1/console/v2/portfolio-holdings?limit=0",
		"/api/v1/console/v2/portfolio-holdings?offset=-1",
		"/api/v1/console/v2/portfolio-holdings?status=CLOSED",
		"/api/v1/console/v2/portfolio-holdings/lots",
		path + "&q=m", path + "&execution_account_id=other", path + "&holding_identity=" + url.QueryEscape(raw),
	}
	for _, value := range []string{`["a"]`, `["a","POLYMARKET","m","c","t",2,"Yes"]`, `[null,"POLYMARKET","m","c","t",0,"Yes"]`, `["a","","m","c","t",0,"Yes"]`, `["a","KALSHI","","c","t",0,"Yes"]`, `["a","KALSHI","m","c","t",0,42]`, `["a","KALSHI","m","c","t",0,"bad\nname"]`, strings.Repeat("x", 2049)} {
		invalid = append(invalid, "/api/v1/console/v2/portfolio-holdings/lots?holding_identity="+url.QueryEscape(value))
	}
	for _, target := range invalid {
		response := performRequest(t, server, http.MethodGet, target, "", "readonly-secret")
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "INVALID_CONSOLE_V2_FILTER") {
			t.Fatalf("accepted invalid target %s: %d %s", target, response.Code, response.Body.String())
		}
	}
	fake := &fakeConsoleV2Service{readErr: consolev2.ErrUnavailable}
	unavailable := testConsoleV2Server(t, fake)
	for _, target := range []string{"/api/v1/console/v2/portfolio-holdings", path} {
		response := performRequest(t, unavailable, http.MethodGet, target, "", "readonly-secret")
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("failure became empty success: %d %s", response.Code, response.Body.String())
		}
	}
}

func (fake *deadlineConsoleV2Service) PortfolioHoldings(ctx context.Context, _ consolev2.ListFilter) (consolev2.Page[consolev2.PortfolioHolding], error) {
	_, err := fake.Orders(ctx, consolev2.ListFilter{})
	return consolev2.Page[consolev2.PortfolioHolding]{}, err
}
func (fake *deadlineConsoleV2Service) PortfolioHoldingLots(ctx context.Context, _ consolev2.PortfolioHoldingIdentity, _ consolev2.ListFilter) (consolev2.Page[consolev2.PositionLot], error) {
	_, err := fake.Orders(ctx, consolev2.ListFilter{})
	return consolev2.Page[consolev2.PositionLot]{}, err
}
func TestConsoleV2PortfolioDeadlineAndCancellation(t *testing.T) {
	for _, target := range []string{"/api/v1/console/v2/portfolio-holdings", "/api/v1/console/v2/portfolio-holdings/lots?holding_identity=" + url.QueryEscape(`["main","POLYMARKET","m","c","t",null,null]`)} {
		fake := &deadlineConsoleV2Service{deadline: make(chan time.Time, 1)}
		server := testConsoleV2Server(t, fake)
		started := time.Now()
		response := performRequest(t, server, http.MethodGet, target, "", "readonly-secret")
		deadline := <-fake.deadline
		if response.Code != http.StatusServiceUnavailable || deadline.Before(started.Add(4*time.Second)) || deadline.After(started.Add(consoleV2ReadTimeout+250*time.Millisecond)) {
			t.Fatalf("unbounded route %s: %d deadline=%v", target, response.Code, deadline)
		}
		fake.waitForCancel = true
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		req := httptest.NewRequest(http.MethodGet, target, nil).WithContext(ctx)
		req.Header.Set("Authorization", "Bearer readonly-secret")
		out := httptest.NewRecorder()
		server.Handler().ServeHTTP(out, req)
		actual := <-fake.deadline
		expected, _ := ctx.Deadline()
		cancel()
		if out.Code != http.StatusServiceUnavailable || !actual.Equal(expected) {
			t.Fatalf("parent cancellation not preserved: %d %v vs %v", out.Code, actual, expected)
		}
	}
}
