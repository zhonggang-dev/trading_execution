package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// The remote snapshot can describe an intermediate state: local 60, remote 30,
// followed by three finalized SELL applications producing 45, 30, then 15.
// Exact continuity and final independent verification are both required.
func TestReconciliationRecorderRecoversIntermediatePositionSnapshot(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	recorder, err := NewReconciliationRecorder(db)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	cases := []struct {
		name     string
		mutation string
		verified bool
		failed   bool
		repeated bool
		resolved bool
	}{
		{name: "continuous finalized path", verified: true, resolved: true},
		{name: "not independently verified"},
		{name: "failed sweep", verified: true, failed: true},
		{name: "reproduced token problem", verified: true, repeated: true},
		{name: "missing middle event", verified: true, mutation: `DELETE FROM position_events WHERE execution_account_id=$1 AND shares_after=30`},
		{name: "broken continuity", verified: true, mutation: `UPDATE position_events SET shares_after=46 WHERE execution_account_id=$1 AND shares_after=45`},
		{name: "old remote never reached", verified: true, mutation: `UPDATE reconciliation_issues SET remote_value=29 WHERE execution_account_id=$1`},
		{name: "current position mismatch", verified: true, mutation: `UPDATE execution_positions SET total_shares=16,available_shares=16 WHERE execution_account_id=$1`},
		{name: "unconfirmed fill", verified: true, mutation: `UPDATE execution_fills SET status='MINED',applied_at=NULL,confirmed_at=NULL WHERE execution_account_id=$1`},
		{name: "wrong event order identity", verified: true, mutation: `UPDATE position_events SET order_id='other-order' WHERE execution_account_id=$1 AND shares_after=30`},
		{name: "wrong fill condition", verified: true, mutation: `UPDATE execution_orders SET intent=jsonb_set(intent,'{condition_id}','"other-condition"') WHERE execution_account_id=$1`},
		{name: "wrong event market", verified: true, mutation: `UPDATE position_events SET market_id='other-market' WHERE execution_account_id=$1 AND shares_after=30`},
		{name: "foreign event account", verified: true, mutation: `UPDATE position_events SET execution_account_id='trajectory-foreign' WHERE execution_account_id=$1 AND shares_after=30`},
		{name: "event after sweep began", verified: true, mutation: `UPDATE position_events SET occurred_at=occurred_at+interval '1 hour' WHERE execution_account_id=$1 AND shares_after=15`},
		{name: "fill after sweep began", verified: true, mutation: `UPDATE execution_fills SET applied_at=applied_at+interval '1 hour' WHERE execution_account_id=$1`},
		{name: "unattributed adjustment", verified: true, mutation: `UPDATE position_events SET event_type='REDEEMED',fill_key='' WHERE execution_account_id=$1 AND shares_after=30`},
	}
	insertAccount(t, db, "trajectory-foreign", "0xtrajectoryforeign", "10", "10", "0")
	for n, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			account := fmt.Sprintf("trajectory-%d", n)
			token := account + "-token"
			insertAccount(t, db, account, "0x"+account, "10", "10", "0")
			insertRecoveredSellPosition(t, db, account, "market", "condition", token, "15")
			old := startReconciliationFixtureRun(t, recorder, account, account+"-old", base)
			issue := domain.ReconciliationIssue{
				IssueID: account + "-issue", RunID: old.RunID, Fingerprint: account + "-fingerprint",
				ExecutionAccountID: account, Type: domain.ReconciliationIssuePositionDrift,
				Resolution: domain.ReconciliationResolutionManual, Status: domain.ReconciliationIssueOpen,
				MarketID: "market", ConditionID: "condition", TokenID: token,
				LocalValue: "60", RemoteValue: "30", Source: "POLYMARKET_DATA_API", ObservedAt: base,
			}
			if err := recorder.RecordIssue(context.Background(), issue); err != nil {
				t.Fatal(err)
			}
			completeReconciliationFixtureRun(t, recorder, old, domain.ReconciliationRunAttentionRequired, base.Add(time.Second))
			for step := 0; step < 3; step++ {
				id := n*10 + step + 10000
				at := base.Add(time.Duration(step+2) * time.Second)
				insertRecoveredSellFill(t, db, account, "market", "condition", token, "15", "polymarket", at, id)
				_, err := db.Exec(`INSERT INTO position_events (
 position_event_id,event_type,execution_account_id,market_id,token_id,order_id,fill_key,
 shares_delta,cash_delta,cost_basis_delta,realized_pnl_delta,shares_after,cost_basis_after,
 average_cost_after,realized_pnl_after,unrealized_pnl_after,occurred_at)
 VALUES ($1,'SOLD',$2,'market',$3,$4,$5,-15,1.5,-1.5,0,$6,0,0,0,0,$7)`,
					fmt.Sprintf("trajectory-event-%d", id), account, token,
					fmt.Sprintf("order-sell-recovery-%d", id), fmt.Sprintf("fill-sell-recovery-%d", id), 45-step*15, at)
				if err != nil {
					t.Fatal(err)
				}
			}
			if tc.mutation != "" {
				if _, err := db.Exec(tc.mutation, account); err != nil {
					t.Fatal(err)
				}
			}
			run := startReconciliationFixtureRun(t, recorder, account, account+"-new", base.Add(5*time.Second))
			run.Status = domain.ReconciliationRunAttentionRequired // unrelated scopes may still be unhealthy
			if tc.failed {
				run.Status = domain.ReconciliationRunFailed
			}
			if tc.verified {
				run.VerifyReconciliation("position", token)
			}
			if tc.repeated {
				issue.RunID = run.RunID
				issue.ObservedAt = run.StartedAt
				if err := recorder.RecordIssue(context.Background(), issue); err != nil {
					t.Fatal(err)
				}
			}
			end := base.Add(6 * time.Second)
			run.CompletedAt = &end
			finished, open, err := recorder.CompleteWithState(context.Background(), run)
			if err != nil {
				t.Fatal(err)
			}
			wantOpen := 1
			if tc.resolved {
				wantOpen = 0
			}
			if len(open) != wantOpen || finished.Summary["impact_scoped_tokens"] != wantOpen {
				t.Fatalf("open=%d scoped=%d, want %d", len(open), finished.Summary["impact_scoped_tokens"], wantOpen)
			}
			if tc.resolved {
				if finished.Summary["issues_resolved"] != 1 {
					t.Fatalf("resolved summary = %v", finished.Summary)
				}
				third := startReconciliationFixtureRun(t, recorder, account, account+"-idempotent", base.Add(7*time.Second))
				completeReconciliationFixtureRun(t, recorder, third, domain.ReconciliationRunCompleted, base.Add(8*time.Second))
				var position, fills, events int
				if err := db.QueryRow(`SELECT total_shares,
 (SELECT count(*) FROM execution_fills WHERE execution_account_id=$1),
 (SELECT count(*) FROM position_events WHERE execution_account_id=$1)
 FROM execution_positions WHERE execution_account_id=$1`, account).Scan(&position, &fills, &events); err != nil {
					t.Fatal(err)
				}
				if position != 15 || fills != 3 || events != 3 {
					t.Fatalf("resolver changed ledger: position=%d fills=%d events=%d", position, fills, events)
				}
			}
		})
	}
}
