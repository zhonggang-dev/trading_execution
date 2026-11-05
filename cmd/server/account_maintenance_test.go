package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/accountmaintenance"
	"github.com/UniPat-AI/trading_execution/internal/service/reconciliation"
)

type fakeReconciliationResults struct {
	mu      sync.Mutex
	results map[string]reconciliation.Result
}

func (source *fakeReconciliationResults) LastResult(accountID string) (reconciliation.Result, bool) {
	source.mu.Lock()
	defer source.mu.Unlock()
	result, ok := source.results[accountID]
	return result, ok
}

func (source *fakeReconciliationResults) set(accountID string, status domain.ReconciliationRunStatus, completed bool) {
	source.mu.Lock()
	defer source.mu.Unlock()
	run := domain.ReconciliationRun{ExecutionAccountID: accountID, Status: status}
	if completed {
		at := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
		run.CompletedAt = &at
	}
	source.results[accountID] = reconciliation.Result{Run: run}
}

// scriptedPreflight answers the probe's read-only preflight from a mutable
// per-account outcome.
type scriptedPreflight struct {
	mu       sync.Mutex
	outcomes map[string]error
	calls    []string
}

func (preflight *scriptedPreflight) run(ctx context.Context, accountID string) (liveAccountPreflightResult, error) {
	preflight.mu.Lock()
	defer preflight.mu.Unlock()
	preflight.calls = append(preflight.calls, accountID)
	if _, ok := ctx.Deadline(); !ok {
		return liveAccountPreflightResult{}, errors.New("probe preflight must run with a per-call timeout")
	}
	return liveAccountPreflightResult{}, preflight.outcomes[accountID]
}

func (preflight *scriptedPreflight) set(accountID string, err error) {
	preflight.mu.Lock()
	defer preflight.mu.Unlock()
	preflight.outcomes[accountID] = err
}

func (preflight *scriptedPreflight) callCount() int {
	preflight.mu.Lock()
	defer preflight.mu.Unlock()
	return len(preflight.calls)
}

func newTestMaintenanceProbe(t *testing.T, registry *accountmaintenance.Registry, preflight *scriptedPreflight,
	results *fakeReconciliationResults, logs *bytes.Buffer, interval time.Duration) *accountMaintenanceProbe {
	t.Helper()
	probe, err := newAccountMaintenanceProbe(accountMaintenanceProbeParams{
		registry: registry, preflight: preflight.run, reconciliation: results, interval: interval,
		logger: slog.New(slog.NewJSONHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return probe
}

func TestAccountMaintenanceProbeDefaultsToOneMinute(t *testing.T) {
	probe, err := newAccountMaintenanceProbe(accountMaintenanceProbeParams{
		registry: accountmaintenance.New(), preflight: (&scriptedPreflight{}).run,
		reconciliation: &fakeReconciliationResults{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if probe.interval != time.Minute || probe.callTimeout != 30*time.Second {
		t.Fatalf("probe timing = %s / %s, want 1m / 30s", probe.interval, probe.callTimeout)
	}
	if _, err := newAccountMaintenanceProbe(accountMaintenanceProbeParams{}); err == nil {
		t.Fatal("probe without dependencies accepted")
	}
}

func TestAccountMaintenanceProbeClearsRecoveredPreflight(t *testing.T) {
	registry := accountmaintenance.New()
	registry.Enter("wallet-7", accountmaintenance.ReasonPreflightCLOBAuth, "401")
	preflight := &scriptedPreflight{outcomes: map[string]error{
		"wallet-7": accountPreflightFailure(accountmaintenance.ReasonPreflightCLOBAuth, "still 401"),
	}}
	var logs bytes.Buffer
	probe := newTestMaintenanceProbe(t, registry, preflight, &fakeReconciliationResults{results: map[string]reconciliation.Result{}}, &logs, time.Minute)

	probe.RunOnce(context.Background())
	if reason, ok := registry.MaintenanceReason("wallet-7"); !ok || reason != accountmaintenance.ReasonPreflightCLOBAuth {
		t.Fatalf("still failing account left maintenance: %q %v", reason, ok)
	}
	if strings.Contains(logs.String(), "entered maintenance") {
		t.Fatalf("an unchanged reason was logged again:\n%s", logs.String())
	}

	// The key is repaired but an allowance is still missing: the reason moves
	// without the account ever leaving maintenance.
	preflight.set("wallet-7", accountPreflightFailure(accountmaintenance.ReasonPreflightAllowanceMissing, "allowance 0"))
	probe.RunOnce(context.Background())
	if reason, ok := registry.MaintenanceReason("wallet-7"); !ok || reason != accountmaintenance.ReasonPreflightAllowanceMissing {
		t.Fatalf("moved reason = %q %v", reason, ok)
	}
	if strings.Contains(logs.String(), "left maintenance") {
		t.Fatal("account briefly left maintenance while its reason moved")
	}

	preflight.set("wallet-7", nil)
	probe.RunOnce(context.Background())
	if _, ok := registry.MaintenanceReason("wallet-7"); ok {
		t.Fatal("recovered account stayed in maintenance")
	}
	if !strings.Contains(logs.String(), "execution account left maintenance") {
		t.Fatalf("exit was not logged:\n%s", logs.String())
	}
}

// TestAccountMaintenanceProbeRunClearsWithinOneInterval checks the background
// loop: a recovered account leaves maintenance at the next tick (60s in
// production, shortened here).
func TestAccountMaintenanceProbeRunClearsWithinOneInterval(t *testing.T) {
	registry := accountmaintenance.New()
	registry.Enter("wallet-7", accountmaintenance.ReasonPreflightFundingUnavailable, "502")
	preflight := &scriptedPreflight{outcomes: map[string]error{}}
	var logs bytes.Buffer
	probe := newTestMaintenanceProbe(t, registry, preflight, &fakeReconciliationResults{results: map[string]reconciliation.Result{}}, &logs, 20*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { probe.Run(ctx); close(done) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := registry.MaintenanceReason("wallet-7"); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background probe did not clear the recovered account")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
}

func TestAccountMaintenanceProbeClearsReconciliationOnlyAfterFinishedRun(t *testing.T) {
	registry := accountmaintenance.New()
	registry.Enter("wallet-6", accountmaintenance.ReasonStartupReconciliationFailed, "status RUNNING")
	results := &fakeReconciliationResults{results: map[string]reconciliation.Result{}}
	results.set("wallet-6", domain.ReconciliationRunRunning, false)
	preflight := &scriptedPreflight{outcomes: map[string]error{}}
	var logs bytes.Buffer
	probe := newTestMaintenanceProbe(t, registry, preflight, results, &logs, time.Minute)

	probe.RunOnce(context.Background())
	results.set("wallet-6", domain.ReconciliationRunFailed, true)
	probe.RunOnce(context.Background())
	if _, ok := registry.MaintenanceReason("wallet-6"); !ok {
		t.Fatal("account left maintenance before a reconciliation finished")
	}
	if preflight.callCount() != 0 {
		t.Fatal("a reconciliation-only maintenance ran the venue preflight")
	}
	// Once the stale lease expired, a scheduled run finished.
	results.set("wallet-6", domain.ReconciliationRunAttentionRequired, true)
	probe.RunOnce(context.Background())
	if _, ok := registry.MaintenanceReason("wallet-6"); ok {
		t.Fatal("finished reconciliation did not clear maintenance")
	}
}

func TestAccountMaintenanceProbeKeepsKalshiAndCombinedReasons(t *testing.T) {
	registry := accountmaintenance.New()
	registry.Enter("kalshi:main", accountmaintenance.ReasonKalshiRouteUnavailable, "bad key")
	registry.Enter("wallet-1", accountmaintenance.ReasonPreflightCLOBAuth, "401")
	registry.Enter("wallet-1", accountmaintenance.ReasonStartupReconciliationFailed, "FAILED")
	results := &fakeReconciliationResults{results: map[string]reconciliation.Result{}}
	results.set("wallet-1", domain.ReconciliationRunFailed, true)
	preflight := &scriptedPreflight{outcomes: map[string]error{}}
	var logs bytes.Buffer
	probe := newTestMaintenanceProbe(t, registry, preflight, results, &logs, time.Minute)

	probe.RunOnce(context.Background())
	if reason, ok := registry.MaintenanceReason("wallet-1"); !ok || reason != accountmaintenance.ReasonStartupReconciliationFailed {
		t.Fatalf("wallet-1 maintenance = %q %v, want only the reconciliation reason left", reason, ok)
	}
	if _, ok := registry.MaintenanceReason("kalshi:main"); !ok {
		t.Fatal("Kalshi maintenance must only be cleared by a restart")
	}
	if preflight.calls[0] != "wallet-1" || len(preflight.calls) != 1 {
		t.Fatalf("preflight calls = %#v, want only wallet-1", preflight.calls)
	}
	results.set("wallet-1", domain.ReconciliationRunCompleted, true)
	probe.RunOnce(context.Background())
	if _, ok := registry.MaintenanceReason("wallet-1"); ok {
		t.Fatal("wallet-1 stayed in maintenance after both reasons recovered")
	}
}

// TestAccountMaintenanceProbeUsesReadOnlyPreflight wires the real preflight
// with fakes that only implement the read-only probe methods, proving the
// recheck needs nothing that can write to the venue.
func TestAccountMaintenanceProbeUsesReadOnlyPreflight(t *testing.T) {
	client := &accountKeyedProbeClient{failAccount: map[string]error{}}
	eligibility := &livePreflightEligibility{}
	registry := accountmaintenance.New()
	registry.Enter("wallet-7", accountmaintenance.ReasonPreflightNotEligible, "closed_only")
	probe, err := newAccountMaintenanceProbe(accountMaintenanceProbeParams{
		registry: registry,
		preflight: func(ctx context.Context, accountID string) (liveAccountPreflightResult, error) {
			return preflightLiveAccount(ctx, accountID, false, client, eligibility)
		},
		reconciliation: &fakeReconciliationResults{results: map[string]reconciliation.Result{}},
		logger:         slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	eligibility.result = polymarket.GeographicEligibility{Blocked: true, Reason: "closed_only"}
	probe.RunOnce(context.Background())
	if _, ok := registry.MaintenanceReason("wallet-7"); !ok {
		t.Fatal("still blocked account left maintenance")
	}
	eligibility.result = polymarket.GeographicEligibility{}
	probe.RunOnce(context.Background())
	if _, ok := registry.MaintenanceReason("wallet-7"); ok {
		t.Fatal("eligible account stayed in maintenance")
	}
	if len(client.calls) != 2 || len(eligibility.calls) != 2 {
		t.Fatalf("probe calls account=%#v eligibility=%#v", client.calls, eligibility.calls)
	}
}
