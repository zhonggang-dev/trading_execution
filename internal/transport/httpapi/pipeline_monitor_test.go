package httpapi

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	pm "github.com/UniPat-AI/trading_execution/internal/service/pipelinemonitor"
)

type pipelineFixture struct {
	snapshot pm.Snapshot
	calls    int
}

func (f *pipelineFixture) Snapshot(context.Context) (pm.Snapshot, error) {
	f.calls++
	return f.snapshot, nil
}

func TestPipelineMonitorRequiresDedicatedReadOnlyToken(t *testing.T) {
	f := &pipelineFixture{snapshot: pm.Snapshot{SchemaVersion: "trading.pipeline_monitor.v1", Component: "trading-execution", SampledAt: time.Now(), Checks: []pm.Check{{ID: "account.reconciliation", Scope: "account-a", Status: pm.Critical, Summary: "对账过期"}}}}
	s, err := New(Params{Service: baseExecutionService(t), APIToken: "execute", ReadOnlyToken: "read", PipelineMonitor: f})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		token  string
		status int
	}{{"", http.StatusUnauthorized}, {"execute", http.StatusForbidden}, {"read", http.StatusOK}} {
		r := performRequest(t, s, http.MethodGet, "/api/v1/pipeline-monitor", "", tc.token)
		if r.Code != tc.status {
			t.Fatalf("status=%d body=%s", r.Code, r.Body.String())
		}
		if tc.status == http.StatusOK && (!strings.Contains(r.Body.String(), "account.reconciliation") || r.Header().Get("Cache-Control") != "no-store") {
			t.Fatalf("bad readonly diagnostic: %s", r.Body.String())
		}
	}
	if f.calls != 1 {
		t.Fatalf("unauthorized request reached monitor: %d", f.calls)
	}
}

func TestPipelineMonitorStaleAndDisabledAreUnavailable(t *testing.T) {
	for _, fixture := range []*pipelineFixture{nil, {snapshot: pm.Snapshot{Stale: true, SampledAt: time.Now()}}, {snapshot: pm.Snapshot{}}} {
		params := Params{Service: baseExecutionService(t), ReadOnlyToken: "read"}
		if fixture != nil {
			params.PipelineMonitor = fixture
		}
		s, err := New(params)
		if err != nil {
			t.Fatal(err)
		}
		r := performRequest(t, s, http.MethodGet, "/api/v1/pipeline-monitor", "", "read")
		if r.Code != http.StatusServiceUnavailable {
			t.Fatalf("status=%d body=%s", r.Code, r.Body.String())
		}
	}
}
