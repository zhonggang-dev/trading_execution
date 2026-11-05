package httpapi

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	core "github.com/UniPat-AI/trading_execution/internal/service/accountcoremetrics"
	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

type fakeCohortMetrics struct {
	fakeCoreMetrics
	filter      consolev2.CohortFilter
	cohortCalls int
}

func (f *fakeCohortMetrics) QueryCohort(ctx context.Context, filter consolev2.CohortFilter) (core.CohortResponse, error) {
	f.filter = filter
	f.cohortCalls++
	f.deadline, _ = ctx.Deadline()
	return core.CohortResponse{Items: []core.CohortMetrics{}}, f.err
}
func TestCohortMetricsAuthorizedStrictReadOnlyHTTP(t *testing.T) {
	f := &fakeCohortMetrics{}
	server := testConsoleV2Server(t, &fakeConsoleV2Service{})
	server.accountCoreMetrics = f
	path := "/api/v1/console/v2/account-cohort-metrics"
	for token, want := range map[string]int{"": 401, "execution-secret": 403} {
		if got := performRequest(t, server, "GET", path, "", token); got.Code != want {
			t.Fatal(got.Code)
		}
	}
	if got := performRequest(t, server, "POST", path, "{}", "readonly-secret"); got.Code != 405 {
		t.Fatal(got.Code)
	}
	for _, q := range []string{"?limit=20", "?from=2026-11-01T00:00:00Z", "?execution_account_id=", "?execution_account_id=%20x", "?account_scope=", "?account_scope=paused", "?account_scope=active&account_scope=all", "?execution_account_id=pm&account_scope=active", "?opening_from=2026-11-01T00:00:00Z", "?opening_to=2026-11-01T00:00:00Z", "?opening_from=x&opening_to=y", "?opening_from=2026-11-01T00:00:00Z&opening_to=2026-11-01T00:00:00Z", "?opening_from=2026-11-02T00:00:00Z&opening_to=2026-11-01T00:00:00Z", "?opening_from=1969-01-01T00:00:00Z&opening_to=2026-11-01T00:00:00Z", "?opening_from=2026-11-01T00:00:00Z&opening_from=2026-11-01T00:00:00Z&opening_to=2026-11-02T00:00:00Z", "?x=%zz", "?x=1;y=2"} {
		if got := performRequest(t, server, "GET", path+q, "", "readonly-secret"); got.Code != 400 {
			t.Fatal(q, got.Code)
		}
	}
	if f.cohortCalls != 0 {
		t.Fatal("invalid request reached service")
	}
	start := time.Now()
	got := performRequest(t, server, "GET", path+"?account_scope=active&opening_from=2026-11-01T00:00:00%2B08:00&opening_to=2026-11-02T00:00:00%2B08:00", "", "readonly-secret")
	if got.Code != 200 || f.filter.AccountScope != "active" || f.filter.OpeningFrom.Hour() != 16 || f.filter.OpeningFrom.Location() != time.UTC || f.deadline.Before(start.Add(4*time.Second)) || f.deadline.After(start.Add(6*time.Second)) {
		t.Fatal(got.Code, f.filter, f.deadline)
	}
	if got = performRequest(t, server, "GET", path+"?execution_account_id=kalshi%3Amain", "", "readonly-secret"); got.Code != 200 || f.filter.ExecutionAccountID != "kalshi:main" {
		t.Fatal(got.Code)
	}
	f.err = consolev2.ErrInvalidPerformanceAccount
	if got = performRequest(t, server, "GET", path+"?execution_account_id=unknown", "", "readonly-secret"); got.Code != 400 {
		t.Fatal(got.Code)
	}
	f.err = errors.New("secret wallet SQL credential")
	got = performRequest(t, server, "GET", path, "", "readonly-secret")
	if got.Code != 503 || strings.Contains(got.Body.String(), "secret") {
		t.Fatal(got.Code, got.Body)
	}
}
