package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
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
