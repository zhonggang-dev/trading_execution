package tradingmonitor

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/decisionrunner"
	pm "github.com/UniPat-AI/trading_execution/internal/service/pipelinemonitor"
)

type predictionFixture struct {
	snapshot domain.PredictionSnapshot
	err      error
}

func (f predictionFixture) Snapshot(context.Context, time.Time, time.Duration) (domain.PredictionSnapshot, error) {
	return f.snapshot, f.err
}

type evidenceFixture struct {
	checks []pm.Check
	err    error
	calls  *int
}

func (f evidenceFixture) Observe(context.Context, time.Time, []Binding) ([]pm.Check, error) {
	if f.calls != nil {
		*f.calls++
	}
	return f.checks, f.err
}

var testNow = time.Date(2026, 10, 30, 0, 30, 0, 0, time.UTC)

func fixtureParams() Params {
	return Params{Enabled: true, SubmissionEnabled: true, StartedAt: testNow.Add(-time.Hour), Interval: 10 * time.Minute, MaxStartLateness: 30 * time.Second, CycleTimeout: 8 * time.Minute, PredictionLookback: 3 * time.Hour,
		Bindings:    []Binding{{StrategyExecutionBinding: domain.StrategyExecutionBinding{PredictionModelID: "source-model", ModelID: "logical-model", StrategyID: "strategy-a", ExecutionAccountID: "account-a"}, KalshiEnabled: true}},
		SourceModes: map[string]domain.PredictionSourceMode{"source-model": domain.PredictionSourceModeSandbox},
		RunnerSnapshot: func() decisionrunner.Status {
			return decisionrunner.Status{LoopStarted: true, LoopRunning: true, NextDueAt: testNow.Add(10 * time.Minute), LastStartedAt: testNow.Add(-time.Minute), LastCompletedAt: testNow.Add(-30 * time.Second), LastSucceededAt: testNow.Add(-30 * time.Second)}
		},
		HeartbeatCheck: func(context.Context, string) error { return nil }, ReconciliationCheck: func(context.Context, string) error { return nil }, Evidence: evidenceFixture{},
		Predictions: predictionFixture{snapshot: domain.PredictionSnapshot{SnapshotID: "snapshot-a", Predictions: []domain.Prediction{validPrediction("market-a", domain.MarketSourcePolymarket, testNow.Add(-time.Hour))}}},
	}
}

func validPrediction(market string, source domain.MarketSource, at time.Time) domain.Prediction {
	return domain.Prediction{PredictionID: "pred-" + market, SandboxID: "sandbox-" + market, MarketID: market, ConditionID: "condition-" + market, MarketSource: source, Model: domain.PredictionModel{Name: "source-model"}, PredictionAsOf: at, CompletedAt: at.Add(time.Minute), AvailableAt: at.Add(2 * time.Minute), Outcomes: []domain.PredictionOutcome{{Index: 0, Name: "Yes", TokenID: "yes-" + market, Probability: .6}, {Index: 1, Name: "No", TokenID: "no-" + market, Probability: .4}}}
}

func collect(t *testing.T, p Params) []pm.Check {
	t.Helper()
	c, e := New(p)
	if e != nil {
		t.Fatal(e)
	}
	checks, e := c.Collect(context.Background(), testNow)
	if e != nil {
		t.Fatal(e)
	}
	return checks
}
func find(t *testing.T, checks []pm.Check, id, scope string) pm.Check {
	t.Helper()
	for _, c := range checks {
		if c.ID == id && (scope == "" || c.Scope == scope) {
			return c
		}
	}
	t.Fatalf("missing %s %s", id, scope)
	return pm.Check{}
}

func TestNoOrderAndNoDirectManifestAreNotBusinessFailure(t *testing.T) {
	p := fixtureParams()
	checks := collect(t, p)
	for _, c := range checks {
		if c.Status == pm.Warning || c.Status == pm.Critical || c.Status == pm.Unknown {
			t.Fatalf("unexpected alert: %+v", c)
		}
	}
	check := find(t, checks, "prediction.coverage", p.Bindings[0].Scope())
	if check.Evidence["effective_markets"] != "1" {
		t.Fatal(check)
	}
}

func TestPredictionCoverageUsesProductionSourceAndFreshness(t *testing.T) {
	for _, tc := range []struct {
		name string
		p    domain.Prediction
	}{
		{"wrong-source", func() domain.Prediction {
			p := validPrediction("market-a", domain.MarketSourcePolymarket, testNow.Add(-time.Hour))
			p.SandboxID = ""
			return p
		}()},
		{"expired-as-of", validPrediction("market-a", domain.MarketSourcePolymarket, testNow.Add(-4*time.Hour))},
		{"ended", func() domain.Prediction {
			p := validPrediction("market-a", domain.MarketSourcePolymarket, testNow.Add(-time.Hour))
			at := testNow
			p.EndAt = &at
			return p
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := fixtureParams()
			p.Predictions = predictionFixture{snapshot: domain.PredictionSnapshot{Predictions: []domain.Prediction{tc.p}}}
			check := find(t, collect(t, p), "prediction.coverage", p.Bindings[0].Scope())
			if check.Status != pm.Critical || check.Evidence["effective_markets"] != "0" {
				t.Fatal(check)
			}
		})
	}
}

func TestUpcomingInputExpiryWarnsBeforeEntryBlock(t *testing.T) {
	p := fixtureParams()
	p.Predictions = predictionFixture{snapshot: domain.PredictionSnapshot{Predictions: []domain.Prediction{validPrediction("market-a", domain.MarketSourcePolymarket, testNow.Add(-170*time.Minute))}}}
	check := find(t, collect(t, p), "prediction.coverage", p.Bindings[0].Scope())
	if check.Status != pm.Warning {
		t.Fatal(check)
	}
}

func TestExpectedMissingVenueIsNotHiddenByOtherVenue(t *testing.T) {
	p := fixtureParams()
	p.SourceModes["source-model"] = domain.PredictionSourceModeDirect
	poly := validPrediction("market-a", domain.MarketSourcePolymarket, testNow.Add(-time.Hour))
	poly.SandboxID = ""
	pending := domain.PredictionExpectation{PredictionID: "direct-kalshi", PredictionModelID: "source-model", MarketID: "kalshi-a", MarketSource: domain.MarketSourceKalshi, Status: domain.PredictionExpectationPending, TaskAvailableAt: testNow.Add(-time.Hour), PredictionAsOf: testNow.Add(-time.Hour)}
	p.Predictions = predictionFixture{snapshot: domain.PredictionSnapshot{Predictions: []domain.Prediction{poly}, ExpectedPredictions: []domain.PredictionExpectation{pending}}}
	checks := collect(t, p)
	if check := find(t, checks, "prediction.coverage", p.Bindings[0].Scope()); check.Status != pm.Healthy {
		t.Fatal(check)
	}
	if check := find(t, checks, "prediction.venue", "KALSHI/"+p.Bindings[0].Scope()); check.Status != pm.Critical || check.Evidence["pending_direct_tasks"] != "1" {
		t.Fatal(check)
	}
	pending.TaskAvailableAt = testNow.Add(-time.Minute)
	p.Predictions = predictionFixture{snapshot: domain.PredictionSnapshot{Predictions: []domain.Prediction{poly}, ExpectedPredictions: []domain.PredictionExpectation{pending}}}
	if check := find(t, collect(t, p), "prediction.venue", "KALSHI/"+p.Bindings[0].Scope()); check.Status != pm.Suppressed {
		t.Fatal(check)
	}
}

func TestKalshiRequiresActualBindingRoute(t *testing.T) {
	p := fixtureParams()
	p.Predictions = predictionFixture{snapshot: domain.PredictionSnapshot{Predictions: []domain.Prediction{validPrediction("kalshi-a", domain.MarketSourceKalshi, testNow.Add(-time.Hour))}}}
	p.Bindings[0].KalshiEnabled = false
	if c := find(t, collect(t, p), "prediction.coverage", p.Bindings[0].Scope()); c.Status != pm.Critical {
		t.Fatal(c)
	}
	p.Bindings[0].KalshiEnabled = true
	if c := find(t, collect(t, p), "prediction.coverage", p.Bindings[0].Scope()); c.Status != pm.Healthy || c.Evidence["kalshi_markets"] != "1" {
		t.Fatal(c)
	}
}

func TestIntentionalDisabledBindingDoesNotRequirePredictions(t *testing.T) {
	p := fixtureParams()
	p.Bindings[0].StrategyDisabled = true
	p.Predictions = predictionFixture{err: errors.New("must not be read")}
	calls := 0
	p.Evidence = evidenceFixture{calls: &calls}
	checks := collect(t, p)
	if calls != 0 {
		t.Fatal("read evidence for disabled binding")
	}
	for _, c := range checks {
		if c.Status == pm.Critical || c.Status == pm.Warning {
			t.Fatal(c)
		}
	}
	p = fixtureParams()
	p.Bindings[0].EntryDisabled = true
	p.Predictions = predictionFixture{err: errors.New("sell-only route must not request prediction source")}
	if c := find(t, collect(t, p), "prediction.coverage", p.Bindings[0].Scope()); c.Status != pm.Suppressed {
		t.Fatal(c)
	}
}

func TestAccountLocalReadinessDoesNotBlockOtherAccounts(t *testing.T) {
	p := fixtureParams()
	b := p.Bindings[0]
	b.ExecutionAccountID = "account-b"
	p.Bindings = append(p.Bindings, b)
	p.ReconciliationCheck = func(_ context.Context, id string) error {
		if id == "account-a" {
			return errors.New("account-wide issue")
		}
		return nil
	}
	checks := collect(t, p)
	if c := find(t, checks, "account.reconciliation", "POLYMARKET/"+p.Bindings[0].Scope()); c.Status != pm.Critical {
		t.Fatal(c)
	}
	if c := find(t, checks, "account.reconciliation", "POLYMARKET/"+p.Bindings[1].Scope()); c.Status != pm.Healthy {
		t.Fatal(c)
	}
}

func TestObservationFailureDoesNotPretendEmptySuccessfulInput(t *testing.T) {
	p := fixtureParams()
	p.Predictions = predictionFixture{err: errors.New("Authorization: Bearer secret https://secret.example")}
	p.Evidence = evidenceFixture{err: errors.New("database failed")}
	checks := collect(t, p)
	if c := find(t, checks, "prediction.observation", ""); c.Status != pm.Unknown {
		t.Fatal(c)
	}
	if c := find(t, checks, "trading.database", ""); c.Status != pm.Unknown {
		t.Fatal(c)
	}
	for _, c := range checks {
		if strings.Contains(c.Details, "secret") || c.ID == "prediction.coverage" {
			t.Fatal(c)
		}
	}
}

func TestSchedulerChecksMissedWindowAndTimeout(t *testing.T) {
	for _, tc := range []struct {
		name string
		s    decisionrunner.Status
	}{
		{"stopped", decisionrunner.Status{}},
		{"missed", decisionrunner.Status{LoopStarted: true, LoopRunning: true, NextDueAt: testNow.Add(-time.Minute)}},
		{"timeout", decisionrunner.Status{LoopStarted: true, LoopRunning: true, InFlight: true, LastStartedAt: testNow.Add(-9 * time.Minute)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := fixtureParams()
			p.RunnerSnapshot = func() decisionrunner.Status { return tc.s }
			if c := find(t, collect(t, p), "decision.schedule", ""); c.Status != pm.Critical {
				t.Fatal(c)
			}
		})
	}
}

func TestSchedulerInitialGraceAndActionableFailuresNotifyCorrectly(t *testing.T) {
	for _, tc := range []struct {
		name          string
		status        decisionrunner.Status
		want          string
		notifications int64
	}{
		{name: "waiting-first-future-boundary", status: decisionrunner.Status{LoopStarted: true, LoopRunning: true, NextDueAt: testNow.Add(time.Minute)}, want: pm.Suppressed},
		{name: "within-first-start-lateness-grace", status: decisionrunner.Status{LoopStarted: true, LoopRunning: true, NextDueAt: testNow.Add(-10 * time.Second)}, want: pm.Suppressed},
		{name: "first-inflight-within-budget", status: decisionrunner.Status{LoopStarted: true, LoopRunning: true, InFlight: true, LastStartedAt: testNow.Add(-time.Minute)}, want: pm.Suppressed},
		{name: "stopped-with-future-boundary", status: decisionrunner.Status{LoopStarted: true, NextDueAt: testNow.Add(time.Minute)}, want: pm.Critical, notifications: 1},
		{name: "first-start-overdue", status: decisionrunner.Status{LoopStarted: true, LoopRunning: true, NextDueAt: testNow.Add(-time.Minute)}, want: pm.Critical, notifications: 1},
		{name: "first-inflight-over-budget", status: decisionrunner.Status{LoopStarted: true, LoopRunning: true, InFlight: true, LastStartedAt: testNow.Add(-9 * time.Minute)}, want: pm.Critical, notifications: 1},
		{name: "first-cycle-error", status: decisionrunner.Status{LoopStarted: true, LoopRunning: true, NextDueAt: testNow.Add(time.Minute), LastError: "cycle failed"}, want: pm.Warning, notifications: 1},
		{name: "no-first-schedule-evidence", status: decisionrunner.Status{LoopStarted: true, LoopRunning: true}, want: pm.Unknown, notifications: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := fixtureParams()
			p.RunnerSnapshot = func() decisionrunner.Status { return tc.status }
			c, err := New(p)
			if err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); io.WriteString(w, `{"code":0}`) }))
			defer server.Close()
			cfg, err := pm.LoadConfig(func(string) string { return "" })
			if err != nil {
				t.Fatal(err)
			}
			cfg.Enabled = true
			cfg.WebhookURL = server.URL
			cfg.StatePath = filepath.Join(t.TempDir(), "alerts.json")
			monitor, err := pm.Build(pm.BuildParams{Config: cfg, Component: "trading-execution", Now: func() time.Time { return testNow }, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Collector: pm.CollectFunc(func(_ context.Context, now time.Time) ([]pm.Check, error) { return c.schedule(now), nil })})
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := monitor.Refresh(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if check := find(t, snapshot.Checks, "decision.schedule", "engine"); check.Status != tc.want {
				t.Fatalf("check=%+v want=%s", check, tc.want)
			}
			if calls.Load() != tc.notifications {
				t.Fatalf("notifications=%d want=%d", calls.Load(), tc.notifications)
			}
		})
	}
}
