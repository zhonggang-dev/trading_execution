package postgres

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/executionrouter"
	pm "github.com/UniPat-AI/trading_execution/internal/service/pipelinemonitor"
	"github.com/UniPat-AI/trading_execution/internal/service/tradingmonitor"
)

func TestPipelineMonitorReadOnlyBusinessEvidencePostgresIntegration(t *testing.T) {
	url := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, url)
	now := time.Now().UTC().Truncate(time.Second)
	insertAccount(t, db, "account-a", "0xmonitor-account", "100", "100", "0")
	provisionLiveRisk(t, db, liveRiskFixture{accountID: "account-a", now: now, binding: true, maxOrder: "100", maxMarket: "100", maxStrategy: "100", maxWallet: "100", maxDaily: "100"})
	if _, err := db.Exec("UPDATE execution_risk_global_control SET kill_switch=FALSE,version=version+1"); err != nil {
		t.Fatal(err)
	}
	b := tradingmonitor.Binding{StrategyExecutionBinding: domain.StrategyExecutionBinding{ModelID: "model-a", PredictionModelID: "source-model", StrategyID: "multfactor_v1", ExecutionAccountID: "account-a"}}
	source, err := NewPipelineMonitorSource(db, 8*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	checks, err := source.Observe(context.Background(), now, []tradingmonitor.Binding{b})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"account.authorization", "account.cash", "account.risk_state", "intent.delivery", "order.recovery", "fill.application"} {
		if check := pipelineCheck(t, checks, id); check.Status != pm.Healthy {
			t.Fatal(check)
		}
	}

	// A durable input with no output is an actual missing stage; the monitor
	// observes it without retrying or invoking the strategy.
	decisionAt := now.Add(-15 * time.Minute)
	recorder, err := NewDecisionRecorder(db, func() time.Time { return decisionAt.Add(time.Minute) })
	if err != nil {
		t.Fatal(err)
	}
	req := integrationDecisionRequest(t, decisionAt, "monitor-snapshot", decisionAt.Add(time.Second))
	if _, _, err := recorder.ClaimInput(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	checks, err = source.Observe(context.Background(), now, []tradingmonitor.Binding{b})
	if err != nil {
		t.Fatal(err)
	}
	if check := pipelineCheck(t, checks, "strategy.progress"); check.Status != pm.Critical {
		t.Fatal(check)
	}
	var outputs int
	if err := db.QueryRow("SELECT count(*) FROM strategy_decision_runs WHERE output_payload IS NOT NULL").Scan(&outputs); err != nil || outputs != 0 {
		t.Fatalf("monitor changed output %d: %v", outputs, err)
	}

	// An accepted strategy response with zero intents is healthy, not a
	// missing-order alert. No venue operation is involved in this test.
	response := domain.StrategyDecisionResponse{SchemaVersion: domain.StrategyOutputSchemaVersion, CycleID: req.CycleID, InputID: req.InputID, Context: req.Context, DecidedAt: decisionAt.Add(2 * time.Second), Evaluations: []domain.StrategyEvaluation{}, Exits: []domain.StrategyExit{}}
	if _, _, err := recorder.ClaimOutput(context.Background(), response, nil, true); err != nil {
		t.Fatal(err)
	}
	checks, err = source.Observe(context.Background(), now, []tradingmonitor.Binding{b})
	if err != nil {
		t.Fatal(err)
	}
	if check := pipelineCheck(t, checks, "strategy.progress"); check.Status != pm.Healthy {
		t.Fatal(check)
	}
	if check := pipelineCheck(t, checks, "intent.delivery"); check.Status != pm.Healthy || check.Evidence["sampled_rows"] != "0" {
		t.Fatal(check)
	}
	if _, err := db.Exec(`INSERT INTO reconciliation_issues(issue_id,run_id,fingerprint,execution_account_id,issue_type,resolution,status,impact_scope,condition_id,token_id,observed_at)
 VALUES('monitor-scoped','risk-run-account-a','monitor-scoped','account-a','POSITION_DRIFT','MANUAL_REVIEW','OPEN','TOKEN','condition-a','token-a',$1)`, now); err != nil {
		t.Fatal(err)
	}
	checks, err = source.Observe(context.Background(), now, []tradingmonitor.Binding{b})
	if err != nil {
		t.Fatal(err)
	}
	if check := pipelineCheck(t, checks, "market.reconciliation_issues"); check.Status != pm.Warning || check.Evidence["sample_token_id"] != "token-a" {
		t.Fatal(check)
	}
	if check := pipelineCheck(t, checks, "account.reconciliation_issues"); check.Status != pm.Healthy {
		t.Fatal("scoped issue incorrectly blocked entire account", check)
	}
	repository, err := NewOrderRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	order := integrationOrder("monitor-auth", "account-a", "token-a", domain.SideBuy, "2", "0.5")
	order.Status = domain.OrderStatusRejected
	order.FailureCode = "CLOB_AUTH_FAILED"
	order.CreatedAt = now
	order.UpdatedAt = now
	if _, _, err := repository.Create(context.Background(), order); err != nil {
		t.Fatal(err)
	}
	checks, err = source.Observe(context.Background(), now, []tradingmonitor.Binding{b})
	if err != nil {
		t.Fatal(err)
	}
	if check := pipelineCheck(t, checks, "venue.execution_errors"); check.Status != pm.Critical || check.Evidence["sample_failure_code"] != "CLOB_AUTH_FAILED" {
		t.Fatal(check)
	}

	if _, err := db.Exec("UPDATE execution_risk_controls SET paused=TRUE,version=version+1 WHERE execution_account_id='account-a' AND control_scope='ACCOUNT'"); err != nil {
		t.Fatal(err)
	}
	checks, err = source.Observe(context.Background(), now, []tradingmonitor.Binding{b})
	if err != nil {
		t.Fatal(err)
	}
	if check := pipelineCheck(t, checks, "account.authorization"); check.Status != pm.Critical {
		t.Fatal(check)
	}
	var paused bool
	if err := db.QueryRow("SELECT paused FROM execution_risk_controls WHERE execution_account_id='account-a' AND control_scope='ACCOUNT'").Scan(&paused); err != nil || !paused {
		t.Fatalf("monitor changed account control: %v", err)
	}
}

// The delivery records the initial submit result and is not rewritten after
// order reconciliation. Match the authoritative order through the real router's
// stable client identity and exact account projection, not Order.Terminal()
// (which also includes MANUAL_REVIEW).
func TestPipelineMonitorUnknownDeliveryResolutionPostgresIntegration(t *testing.T) {
	url := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	cases := []struct {
		name            string
		source          domain.MarketSource
		orderAccount    string
		orderStatus     domain.OrderStatus
		differentClient bool
		wantStalled     bool
	}{
		{name: "logical-filled", orderAccount: "account-a", orderStatus: domain.OrderStatusFilled},
		{name: "logical-cancelled", orderAccount: "account-a", orderStatus: domain.OrderStatusCancelled},
		{name: "logical-rejected", orderAccount: "account-a", orderStatus: domain.OrderStatusRejected},
		{name: "native-filled", source: domain.MarketSourceKalshi, orderAccount: "kalshi:account-a", orderStatus: domain.OrderStatusFilled},
		{name: "native-cancelled", source: domain.MarketSourceKalshi, orderAccount: "kalshi:account-a", orderStatus: domain.OrderStatusCancelled},
		{name: "missing-order", wantStalled: true},
		{name: "actual-unknown", orderAccount: "account-a", orderStatus: domain.OrderStatusUnknown, wantStalled: true},
		{name: "actual-manual-review", orderAccount: "account-a", orderStatus: domain.OrderStatusManualReview, wantStalled: true},
		{name: "actual-live", orderAccount: "account-a", orderStatus: domain.OrderStatusLive, wantStalled: true},
		{name: "foreign-logical-account", orderAccount: "account-b", orderStatus: domain.OrderStatusFilled, wantStalled: true},
		{name: "foreign-native-account", source: domain.MarketSourceKalshi, orderAccount: "kalshi:account-b", orderStatus: domain.OrderStatusFilled, wantStalled: true},
		{name: "polymarket-cannot-use-native-account", orderAccount: "kalshi:account-a", orderStatus: domain.OrderStatusFilled, wantStalled: true},
		{name: "kalshi-cannot-use-logical-account", source: domain.MarketSourceKalshi, orderAccount: "account-a", orderStatus: domain.OrderStatusFilled, wantStalled: true},
		{name: "different-client-order-id", orderAccount: "account-a", orderStatus: domain.OrderStatusFilled, differentClient: true, wantStalled: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := newIntegrationDatabase(t, url)
			for _, id := range []string{"account-a", "account-b", "kalshi:account-a", "kalshi:account-b"} {
				insertAccount(t, db, id, "monitor-"+id, "100", "100", "0")
			}
			now := time.Now().UTC().Truncate(time.Second)
			at := now.Add(-time.Hour)
			recorder, err := NewDecisionRecorder(db, func() time.Time { return at.Add(time.Minute) })
			if err != nil {
				t.Fatal(err)
			}
			request := integrationDecisionRequest(t, at, "monitor-resolution", at.Add(time.Second))
			if _, _, err := recorder.ClaimInput(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			intent := integrationDecisionIntentWithID(t, request, at, "monitor-signal", "monitor-client")
			if tc.source == domain.MarketSourceKalshi {
				intent.MarketSource = tc.source
				intent.Venue = "kalshi"
				intent.MarketID = "KXMONITOR"
				intent.ConditionID = "kalshi:KXMONITOR"
				intent.OutcomeID = "YES"
				intent.TokenID = "kalshi:KXMONITOR:YES"
			}
			response := domain.StrategyDecisionResponse{SchemaVersion: domain.StrategyOutputSchemaVersion, CycleID: request.CycleID, InputID: request.InputID, Context: request.Context, DecidedAt: at.Add(2 * time.Second), Evaluations: []domain.StrategyEvaluation{}, Exits: []domain.StrategyExit{}}
			if _, _, err := recorder.ClaimOutput(context.Background(), response, []domain.OrderIntent{intent}, true); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE strategy_order_intent_deliveries SET status='UNKNOWN',attempt_count=1,claimed_at=$2,completed_at=$2,order_id='monitor-order',order_status='UNKNOWN',updated_at=$2 WHERE client_order_id=$1`, intent.ClientOrderID, at.Add(2*time.Minute)); err != nil {
				t.Fatal(err)
			}

			repository, err := NewOrderRepository(db)
			if err != nil {
				t.Fatal(err)
			}
			primary, native := &monitorRouteExecution{}, &monitorRouteExecution{}
			router, err := executionrouter.New(repository, primary, []executionrouter.Route{{ModelID: intent.ModelID, StrategyID: intent.StrategyID, LogicalAccountID: "account-a", InternalAccountID: "kalshi:account-a", Execution: native}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := router.Submit(context.Background(), intent); err != nil {
				t.Fatal(err)
			}
			routed := primary.submitted
			wantRoutedAccount := "account-a"
			if tc.source == domain.MarketSourceKalshi {
				routed = native.submitted
				wantRoutedAccount = "kalshi:account-a"
			}
			if routed == nil || routed.ClientOrderID != intent.ClientOrderID || routed.ExecutionAccountID != wantRoutedAccount {
				t.Fatalf("router changed client identity or projected wrong account: %+v", routed)
			}
			if tc.orderStatus != "" {
				order := integrationOrder("monitor-resolution", tc.orderAccount, routed.TokenID, domain.SideBuy, "2", "0.5")
				order.ID = "monitor-order"
				order.Intent = *routed
				order.Intent.ExecutionAccountID = tc.orderAccount
				order.Status = tc.orderStatus
				order.CreatedAt = at
				order.UpdatedAt = now
				if tc.differentClient {
					order.Intent.ClientOrderID = "other-client"
				}
				if tc.orderStatus == domain.OrderStatusFilled {
					order.FilledSize = "2"
					order.FilledNotional = "1"
					order.AverageFillPrice = "0.5"
				}
				if _, _, err := repository.Create(context.Background(), order); err != nil {
					t.Fatal(err)
				}
			}
			source, err := NewPipelineMonitorSource(db, 8*time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			checks, err := source.progress(context.Background(), tx, now, "account-a")
			if err != nil {
				t.Fatal(err)
			}
			check := pipelineCheck(t, checks, "intent.delivery")
			wantRows, wantStatus := "0", pm.Healthy
			if tc.wantStalled {
				wantRows = "1"
				wantStatus = pm.Critical
			}
			if check.Status != wantStatus || check.Evidence["sampled_rows"] != wantRows || check.Evidence["stalled_sample_rows"] != wantRows {
				t.Fatalf("check %+v; want %s stalled rows %s", check, wantStatus, wantRows)
			}
			var deliveryStatus string
			if err := tx.QueryRow("SELECT status FROM strategy_order_intent_deliveries WHERE client_order_id=$1", intent.ClientOrderID).Scan(&deliveryStatus); err != nil || deliveryStatus != "UNKNOWN" {
				t.Fatalf("monitor rewrote historical delivery: %q %v", deliveryStatus, err)
			}
			if tc.orderStatus != "" {
				var state string
				if err := tx.QueryRow("SELECT status FROM execution_orders WHERE order_id='monitor-order'").Scan(&state); err != nil || state != string(tc.orderStatus) {
					t.Fatalf("monitor rewrote authoritative order: %q %v", state, err)
				}
			}
		})
	}
}

type monitorRouteExecution struct {
	executionrouter.Execution
	submitted *domain.OrderIntent
}

func (f *monitorRouteExecution) Submit(_ context.Context, intent domain.OrderIntent) (port.OrderSubmitResult, error) {
	f.submitted = &intent
	return port.OrderSubmitResult{}, nil
}

func pipelineCheck(t *testing.T, checks []pm.Check, id string) pm.Check {
	t.Helper()
	for _, c := range checks {
		if c.ID == id {
			return c
		}
	}
	t.Fatalf("missing %s", id)
	return pm.Check{}
}

func TestPipelineMonitorKalshiDoesNotRequirePolymarketRiskPostgresIntegration(t *testing.T) {
	url := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, url)
	now := time.Now().UTC().Truncate(time.Second)
	insertAccount(t, db, "account-a", "0xmonitor-account", "100", "100", "0")
	provisionLiveRisk(t, db, liveRiskFixture{accountID: "account-a", now: now, binding: true, maxOrder: "100", maxMarket: "100", maxStrategy: "100", maxWallet: "100", maxDaily: "100"})
	if _, err := db.Exec("UPDATE execution_risk_global_control SET kill_switch=FALSE,version=version+1"); err != nil {
		t.Fatal(err)
	}
	b := tradingmonitor.Binding{StrategyExecutionBinding: domain.StrategyExecutionBinding{ModelID: "model-a", PredictionModelID: "source-model", StrategyID: "multfactor_v1", ExecutionAccountID: "account-a"}, KalshiEnabled: true}
	source, err := NewPipelineMonitorSource(db, 8*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	observe := func() []pm.Check {
		checks, err := source.Observe(context.Background(), now, []tradingmonitor.Binding{b})
		if err != nil {
			t.Fatal(err)
		}
		return checks
	}
	checks := observe()
	if c := pipelineScopedCheck(t, checks, "account.authorization", "kalshi:account-a"); c.Status != pm.Critical {
		t.Fatalf("missing native ledger must still be reported: %+v", c)
	}
	insertAccount(t, db, "kalshi:account-a", "kalshi:monitor-account", "100", "100", "0")
	checks = observe()
	for _, id := range []string{"account.authorization", "account.risk_state", "account.reconciliation_issues", "market.reconciliation_issues"} {
		if c := pipelineScopedCheck(t, checks, id, "kalshi:account-a"); c.Status != pm.Suppressed {
			t.Fatalf("Kalshi incorrectly requires a Polymarket gate: %+v", c)
		}
	}
	if c := pipelineScopedCheck(t, checks, "account.authorization", "account-a"); c.Status != pm.Healthy {
		t.Fatalf("Polymarket authorization changed: %+v", c)
	}
	if c := pipelineScopedCheck(t, checks, "account.cash", "kalshi:account-a"); c.Status != pm.Healthy {
		t.Fatal(c)
	}
	var riskRecords int
	if err := db.QueryRow("SELECT count(*) FROM execution_risk_policies WHERE execution_account_id='kalshi:account-a'").Scan(&riskRecords); err != nil || riskRecords != 0 {
		t.Fatalf("unexpected native PM risk records: %d, %v", riskRecords, err)
	}
	if _, err := db.Exec("UPDATE execution_accounts SET total_balance=0,available_balance=0 WHERE execution_account_id='kalshi:account-a'"); err != nil {
		t.Fatal(err)
	}
	if c := pipelineScopedCheck(t, observe(), "account.cash", "kalshi:account-a"); c.Status != pm.Warning {
		t.Fatalf("native funding issue was hidden: %+v", c)
	}
	repository, err := NewOrderRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	order := integrationOrder("monitor-kalshi-auth", "kalshi:account-a", "kalshi:KXMONITOR:YES", domain.SideBuy, "2", "0.5")
	order.Intent.Venue = "kalshi"
	order.Intent.MarketSource = domain.MarketSourceKalshi
	order.Status = domain.OrderStatusRejected
	order.FailureCode = "KALSHI_AUTH_FAILED"
	order.CreatedAt = now
	order.UpdatedAt = now
	if _, _, err := repository.Create(context.Background(), order); err != nil {
		t.Fatal(err)
	}
	if c := pipelineScopedCheck(t, observe(), "venue.execution_errors", "kalshi:account-a"); c.Status != pm.Critical || c.Evidence["sample_failure_code"] != "KALSHI_AUTH_FAILED" {
		t.Fatalf("real native API failure was hidden: %+v", c)
	}
}

func TestPipelineMonitorHistoricalBaselineOnlyIsDiagnosticPostgresIntegration(t *testing.T) {
	url := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, url)
	now := time.Now().UTC().Truncate(time.Second)
	insertAccount(t, db, "account-a", "0xmonitor-account", "100", "100", "0")
	provisionLiveRisk(t, db, liveRiskFixture{accountID: "account-a", now: now, binding: true, maxOrder: "100", maxMarket: "100", maxStrategy: "100", maxWallet: "100", maxDaily: "100"})
	source, err := NewPipelineMonitorSource(db, 8*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	addIssue := func(id, kind, scope string, at time.Time) {
		_, err := db.Exec(`INSERT INTO reconciliation_issues(issue_id,run_id,fingerprint,execution_account_id,issue_type,resolution,status,impact_scope,condition_id,token_id,observed_at)
 VALUES($1,'risk-run-account-a',$1,'account-a',$2,'MANUAL_REVIEW','OPEN',$3,'condition-'||$1,'token-'||$1,$4)`, id, kind, scope, at)
		if err != nil {
			t.Fatal(err)
		}
	}
	observe := func() []pm.Check {
		tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		checks, err := source.issues(context.Background(), tx, "account-a")
		if err != nil {
			t.Fatal(err)
		}
		return checks
	}
	addIssue("baseline-old", "EXTERNAL_POSITION_BASELINE_DRIFT", "TOKEN", now.Add(-time.Hour))
	addIssue("baseline-new", "EXTERNAL_POSITION_BASELINE_DRIFT", "TOKEN", now)
	checks := observe()
	if c := pipelineCheck(t, checks, "market.reconciliation_issues"); c.Status != pm.Suppressed || c.Evidence["sampled_scoped_issues"] != "2" || c.Evidence["sampled_actionable_scoped_issues"] != "0" {
		t.Fatalf("baseline-only scope should retain evidence without notifying: %+v", c)
	}
	addIssue("actual-drift", "POSITION_DRIFT", "TOKEN", now.Add(time.Minute))
	checks = observe()
	if c := pipelineCheck(t, checks, "market.reconciliation_issues"); c.Status != pm.Warning || c.Evidence["sampled_scoped_issues"] != "3" || c.Evidence["sampled_baseline_issues"] != "2" || c.Evidence["sampled_actionable_scoped_issues"] != "1" || c.Evidence["sample_issue_id"] != "actual-drift" {
		t.Fatalf("baseline sample hid an actionable market issue: %+v", c)
	}
	addIssue("account-drift", "EXTERNAL_POSITION_BASELINE_DRIFT", "ACCOUNT", now)
	if c := pipelineCheck(t, observe(), "account.reconciliation_issues"); c.Status != pm.Critical {
		t.Fatalf("account-wide issue was incorrectly suppressed: %+v", c)
	}
	var open int
	if err := db.QueryRow("SELECT count(*) FROM reconciliation_issues WHERE status='OPEN'").Scan(&open); err != nil || open != 4 {
		t.Fatalf("monitor changed real reconciliation issues: %d, %v", open, err)
	}
}

func pipelineScopedCheck(t *testing.T, checks []pm.Check, id, scope string) pm.Check {
	t.Helper()
	for _, c := range checks {
		if c.ID == id && c.Scope == scope {
			return c
		}
	}
	t.Fatalf("missing %s %s", id, scope)
	return pm.Check{}
}
