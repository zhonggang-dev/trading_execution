package httpapi

import (
	"context"
	"errors"
	core "github.com/UniPat-AI/trading_execution/internal/service/accountcoremetrics"
	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
	"net/http"
	"strings"
	"testing"
	"time"
)

type fakeCoreMetrics struct {
	calls    int
	account  string
	deadline time.Time
	err      error
}

func (f *fakeCoreMetrics) Query(ctx context.Context, account string) (core.Response, error) {
	f.calls++
	f.account = account
	f.deadline, _ = ctx.Deadline()
	return core.Response{Items: []core.Metrics{}}, f.err
}
func TestCoreMetricsNarrowAuthorizedReadOnlyHTTP(t *testing.T) {
	f := &fakeCoreMetrics{}
	server := testConsoleV2Server(t, &fakeConsoleV2Service{})
	server.accountCoreMetrics = f
	path := "/api/v1/console/v2/account-core-metrics"
	for token, want := range map[string]int{"": 401, "execution-secret": 403} {
		if got := performRequest(t, server, "GET", path, "", token); got.Code != want {
			t.Fatal(got.Code)
		}
	}
	if got := performRequest(t, server, "POST", path, "{}", "readonly-secret"); got.Code != 405 {
		t.Fatal(got.Code)
	}
	for _, query := range []string{"?limit=20", "?from=x", "?account=x", "?execution_account_id=", "?execution_account_id=x&execution_account_id=x", "?execution_account_id=%20x", "?execution_account_id=x%0A", "?x=%zz", "?x=1;y=2"} {
		got := performRequest(t, server, "GET", path+query, "", "readonly-secret")
		if got.Code != 400 {
			t.Fatal(query, got.Code)
		}
	}
	if f.calls != 0 {
		t.Fatal("invalid reached service")
	}
	started := time.Now()
	got := performRequest(t, server, "GET", path+"?execution_account_id=kalshi%3Amain", "", "readonly-secret")
	if got.Code != 200 || f.account != "kalshi:main" || f.deadline.Before(started.Add(4*time.Second)) || f.deadline.After(started.Add(6*time.Second)) {
		t.Fatal("deadline/scope", got.Code, f.account, f.deadline)
	}
	f.err = consolev2.ErrInvalidPerformanceAccount
	if got = performRequest(t, server, "GET", path+"?execution_account_id=unknown", "", "readonly-secret"); got.Code != 400 {
		t.Fatal(got.Code)
	}
	f.err = errors.New("secret wallet SQL credentials")
	got = performRequest(t, server, http.MethodGet, path, "", "readonly-secret")
	if got.Code != 503 || strings.Contains(got.Body.String(), "secret") {
		t.Fatal("unsafe error", got.Body)
	}
}
