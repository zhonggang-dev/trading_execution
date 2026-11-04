package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

type performanceConsoleV2Service struct {
	fakeConsoleV2Service
	account  string
	calls    int
	deadline time.Time
	wait     bool
}

func (fake *performanceConsoleV2Service) AccountPerformance(ctx context.Context, account string) (consolev2.AccountPerformanceResponse, error) {
	fake.account = account
	fake.calls++
	fake.deadline, _ = ctx.Deadline()
	if fake.wait {
		<-ctx.Done()
		return consolev2.AccountPerformanceResponse{}, ctx.Err()
	}
	return consolev2.AccountPerformanceResponse{Items: []consolev2.AccountPerformance{}}, fake.readErr
}
func TestConsoleV2AccountPerformanceNarrowReadOnlyContract(t *testing.T) {
	fake := &performanceConsoleV2Service{}
	server := testConsoleV2Server(t, fake)
	target := "/api/v1/console/v2/account-performance"
	for token, want := range map[string]int{"": http.StatusUnauthorized, "execution-secret": http.StatusForbidden} {
		got := performRequest(t, server, http.MethodGet, target, "", token)
		if got.Code != want {
			t.Fatalf("token=%q status=%d", token, got.Code)
		}
	}
	if got := performRequest(t, server, http.MethodPost, target, "{}", "readonly-secret"); got.Code != http.StatusMethodNotAllowed {
		t.Fatal(got.Code)
	}
	for _, query := range []string{"?limit=1", "?offset=0", "?q=x", "?account=main", "?model_id=m", "?from=2026-11-01", "?market_source=KALSHI", "?execution_account_id=", "?execution_account_id=main&execution_account_id=main", "?execution_account_id=%09main", "?execution_account_id=main%0A", "?execution_account_id=%20main", "?execution_account_id=" + strings.Repeat("a", 129), "?x=%zz", "?x=1;y=2"} {
		got := performRequest(t, server, http.MethodGet, target+query, "", "readonly-secret")
		if got.Code != http.StatusBadRequest || !strings.Contains(got.Body.String(), "INVALID_CONSOLE_V2_FILTER") {
			t.Fatalf("%s: %d %s", query, got.Code, got.Body)
		}
	}
	if fake.calls != 0 {
		t.Fatalf("invalid/auth requests reached reader: %d", fake.calls)
	}
	started := time.Now()
	got := performRequest(t, server, http.MethodGet, target+"?execution_account_id=kalshi%3Amain", "", "readonly-secret")
	if got.Code != http.StatusOK || fake.account != "kalshi:main" || fake.deadline.Before(started.Add(4*time.Second)) || fake.deadline.After(started.Add(6*time.Second)) {
		t.Fatalf("contract: %d account=%q deadline=%v", got.Code, fake.account, fake.deadline)
	}
	fake.readErr = consolev2.ErrInvalidPerformanceAccount
	got = performRequest(t, server, http.MethodGet, target+"?execution_account_id=unknown", "", "readonly-secret")
	if got.Code != http.StatusBadRequest {
		t.Fatalf("unknown account: %d %s", got.Code, got.Body)
	}
	fake.readErr = errors.New("SQL secret wallet-address database details")
	got = performRequest(t, server, http.MethodGet, target, "", "readonly-secret")
	if got.Code != http.StatusServiceUnavailable || strings.Contains(got.Body.String(), "secret") || strings.Contains(got.Body.String(), "SQL") {
		t.Fatalf("unsafe failure %d %s", got.Code, got.Body)
	}
}
func TestConsoleV2AccountPerformancePreservesParentCancellation(t *testing.T) {
	fake := &performanceConsoleV2Service{wait: true}
	server := testConsoleV2Server(t, fake)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/console/v2/account-performance", nil).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer readonly-secret")
	response := httptest.NewRecorder()
	started := time.Now()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || time.Since(started) > time.Second {
		t.Fatalf("parent cancellation: %d elapsed=%v", response.Code, time.Since(started))
	}
	deadline, _ := ctx.Deadline()
	if fake.deadline.After(deadline) {
		t.Fatalf("parent deadline was widened: %v > %v", fake.deadline, deadline)
	}
}
