package consolev2

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// This test is intentionally opt-in. The console-v2 test database is created
// by the parent task; ordinary unit runs must not require PostgreSQL.
func TestReaderAgainstMigratedTradingSchema(t *testing.T) {
	url := os.Getenv("TRADING_EXECUTION_CONSOLE_V2_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TRADING_EXECUTION_CONSOLE_V2_TEST_DATABASE_URL is not configured")
	}
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	const (
		accountID = "console-v2-test-account"
		orderID   = "console-v2-test-order"
		cycleID   = "console-v2-test-cycle"
		runID     = "console-v2-test-recon-run"
		issueID   = "console-v2-test-open-issue"
		condition = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	)
	cleanup := func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM reconciliation_issues WHERE issue_id=$1`, issueID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM reconciliation_runs WHERE run_id=$1`, runID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM strategy_order_intent_deliveries WHERE cycle_id=$1`, cycleID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM strategy_decision_runs WHERE cycle_id=$1`, cycleID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM execution_orders WHERE order_id=$1`, orderID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM polymarket_redemptions WHERE execution_account_id=$1 AND condition_id=$2`, accountID, condition)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM execution_accounts WHERE execution_account_id=$1`, accountID)
	}
	cleanup()
	defer cleanup()

	decisionAt := time.Now().UTC()
	snapshotSetID := "set-" + decisionAt.Format("20060102150405.999999")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO execution_accounts (execution_account_id, wallet_address, collateral_asset)
		VALUES ($1, '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'pUSD')`, accountID); err != nil {
		t.Fatal(err)
	}
	intent := `{"model_id":"echo","strategy_id":"multfactor_v2","execution_account_id":"console-v2-test-account","client_order_id":"console-v2-client","market_source":"POLYMARKET","condition_id":"` + condition + `","side":"BUY","price":"0.4","size":"2"}`
	if _, err := db.ExecContext(ctx, `
		INSERT INTO execution_orders (
			order_id, client_order_id, execution_account_id, venue, market_id, token_id,
			intent, status, revision, created_at, updated_at
		) VALUES ($1, 'console-v2-client', $2, 'polymarket', 'market-1', 'token-1', $3::jsonb,
		          'LIVE', 1, $4, $4)`, orderID, accountID, intent, decisionAt); err != nil {
		t.Fatal(err)
	}
	input := `{"prediction_snapshot_id":"snapshot-1","predictions":[{"prediction_id":"prediction-1","source_job_id":"job-1","market_source":"POLYMARKET","market_id":"market-1","condition_id":"` + condition + `","token_id":"token-1","model":{"name":"echo"}}],"positions":[]}`
	if _, err := db.ExecContext(ctx, `
		INSERT INTO strategy_decision_runs (
			cycle_id, input_id, decision_at, model_id, strategy_id, execution_account_id,
			input_payload, output_payload, decided_at, order_submission_enabled, created_at, updated_at
		) VALUES ($1, 'input-1', $2, 'echo', 'multfactor_v2', $3, $4::jsonb, '{}'::jsonb, $2, FALSE, $2, $2)`, cycleID, decisionAt, accountID, input); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO strategy_order_intent_deliveries (
			client_order_id, cycle_id, sequence_no, intent_payload, status,
			attempt_count, claimed_at, completed_at, order_id, order_status,
			created_at, updated_at
		) VALUES ($1, $2, 0, '{"side":"BUY"}'::jsonb, 'SUBMITTED', 1,
		          $3, $3, $4, 'LIVE', $3, $3)`, "console-v2-client", cycleID, decisionAt, orderID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO strategy_orderbook_snapshot_batches (
			decision_at, prediction_snapshot_id, snapshot_set_id, target_count,
			snapshot_count, ok_count, empty_count, missing_count, error_count
		) VALUES ($1, 'snapshot-1', $2, 0, 0, 0, 0, 0, 0)`, decisionAt, snapshotSetID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO reconciliation_runs (
			run_id, execution_account_id, trigger, status, started_at, completed_at
		) VALUES ($1, $2, 'SCHEDULED', 'ATTENTION_REQUIRED', $3, $3)`, runID, accountID, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO reconciliation_issues (
			issue_id, run_id, fingerprint, execution_account_id, issue_type,
			resolution, status, observed_at
		) VALUES ($1, $2, 'console-v2-fingerprint', $3, 'BALANCE_DRIFT',
		          'MANUAL_REVIEW', 'OPEN', $4)`, issueID, runID, accountID, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO polymarket_redemptions (
			execution_account_id, condition_id, wallet_address, neg_risk, status
		) VALUES ($1, $2, '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', FALSE, 'READY')`, accountID, condition); err != nil {
		t.Fatal(err)
	}

	now := decisionAt.Add(1 * time.Hour)
	reader := New(db, RuntimeConfig{StartedAt: decisionAt, Now: func() time.Time { return now }})
	cycles, err := reader.DecisionCycles(ctx, ListFilter{Limit: 5})
	if err != nil || len(cycles.Items) != 1 || cycles.Items[0].PredictionSnapshotID != "snapshot-1" ||
		cycles.Items[0].PredictionModelID != nil || cycles.Items[0].OrderSubmissionEnabled == nil ||
		*cycles.Items[0].OrderSubmissionEnabled || cycles.Items[0].EntrySubmissionEnabled != nil ||
		cycles.Items[0].EntryBlockReason != nil ||
		cycles.Items[0].IntentCount != 1 || cycles.Items[0].Status != "OUTPUT_RECORDED" {
		t.Fatalf("cycles=%#v err=%v", cycles, err)
	}
	detail, err := reader.DecisionCycle(ctx, cycleID)
	if err != nil || len(detail.Predictions) != 1 || detail.Predictions[0].PredictionID != "prediction-1" {
		t.Fatalf("cycle detail=%#v err=%v", detail, err)
	}
	orders, err := reader.Orders(ctx, ListFilter{Limit: 5})
	if err != nil || len(orders.Items) != 1 || orders.Items[0].OrderID != orderID || orders.Items[0].Shares != "2" {
		t.Fatalf("orders=%#v err=%v", orders, err)
	}
	trace, err := reader.OrderTrace(ctx, orderID)
	if err != nil || trace.Order.OrderID != orderID || len(trace.Events) != 0 {
		t.Fatalf("trace=%#v err=%v", trace, err)
	}
	lots, err := reader.PositionLots(ctx, ListFilter{Limit: 5})
	if err != nil || len(lots.Items) != 0 {
		t.Fatalf("lots=%#v err=%v", lots, err)
	}
	runs, err := reader.ReconciliationRuns(ctx, ListFilter{Limit: 5})
	if err != nil || len(runs.Items) != 1 || runs.Items[0].RunID != runID {
		t.Fatalf("runs=%#v err=%v", runs, err)
	}
	issues, err := reader.ReconciliationIssues(ctx, ListFilter{Limit: 5})
	if err != nil || len(issues.Items) != 1 || issues.Items[0].IssueID != issueID || issues.Items[0].ReasonCode != nil {
		t.Fatalf("issues=%#v err=%v", issues, err)
	}
	redemptions, err := reader.Redemptions(ctx, ListFilter{Limit: 5})
	if err != nil || len(redemptions.Items) != 1 || redemptions.Items[0].Status != "READY" {
		t.Fatalf("redemptions=%#v err=%v", redemptions, err)
	}
	metrics, err := reader.ServiceMetrics(ctx)
	if err != nil || metrics.UptimeSeconds != 3600 || metrics.Availability["request_rate"] != "unavailable" {
		t.Fatalf("metrics=%#v err=%v", metrics, err)
	}
}
