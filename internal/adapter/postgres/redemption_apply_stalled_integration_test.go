package postgres

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestRedemptionApplyStalledIssueClosesOnlyAfterApplyPostgresIntegration(t *testing.T) {
	dsn := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, dsn)
	recorder, err := NewReconciliationRecorder(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	account := "wallet-stalled"
	wallet := "0x" + strings.Repeat("6a", 20)
	insertAccount(t, db, account, wallet, "100", "100", "0")
	applied := "0x" + strings.Repeat("a1", 32)
	stillStalled := "0x" + strings.Repeat("b2", 32)
	notReproduced := "0x" + strings.Repeat("c3", 32)
	confirmedAt := now.Add(-time.Hour)
	for index, condition := range []string{applied, stillStalled, notReproduced} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO polymarket_redemptions (
				execution_account_id, condition_id, wallet_address, neg_risk, status,
				submission_provider, submission_reference, transaction_hash, event_type,
				payout_base_units, receipt_block_number, receipt_block_hash, confirmations,
				submitting_at, submitted_at, confirmed_at
			) VALUES ($1,$2,$3,FALSE,'CONFIRMED','relayer',$4,$5,'POSITIONS_REDEEMED',
			          3350000,100,$6,64,$7,$7,$7)`,
			account, condition, wallet, "ref-"+condition[2:6], "0x"+strings.Repeat(string(rune('d'+index)), 64),
			"0x"+strings.Repeat("ef", 32), confirmedAt); err != nil {
			t.Fatal(err)
		}
	}

	reader, err := NewRedemptionProgressReader(db)
	if err != nil {
		t.Fatal(err)
	}
	inFlight, err := reader.ListInFlightRedemptions(ctx, account)
	if err != nil {
		t.Fatal(err)
	}
	if len(inFlight) != 3 {
		t.Fatalf("in-flight redemptions = %#v, want three CONFIRMED rows", inFlight)
	}
	for _, redemption := range inFlight {
		if redemption.ConfirmedAt == nil || !redemption.ConfirmedAt.Equal(confirmedAt) {
			t.Fatalf("in-flight redemption %s confirmed_at = %v, want %s", redemption.ConditionID, redemption.ConfirmedAt, confirmedAt)
		}
	}

	stalledIssue := func(runID, condition string, observedAt time.Time) domain.ReconciliationIssue {
		return domain.ReconciliationIssue{
			IssueID: "stalled-" + runID + "-" + condition[2:6], RunID: runID, Fingerprint: "stalled-" + condition,
			ExecutionAccountID: account, Type: domain.ReconciliationIssueRedemptionApplyStalled,
			Resolution: domain.ReconciliationResolutionObserved, Status: domain.ReconciliationIssueOpen,
			ConditionID: condition, Source: "POSTGRES_REDEMPTIONS",
			Details: "redemption confirmed on chain is still not applied", ObservedAt: observedAt,
		}
	}
	prior := startReconciliationFixtureRun(t, recorder, account, "prior-stalled", now.Add(-10*time.Minute))
	for _, condition := range []string{applied, stillStalled, notReproduced} {
		if err := recorder.RecordIssue(ctx, stalledIssue(prior.RunID, condition, prior.StartedAt)); err != nil {
			t.Fatal(err)
		}
	}
	completeReconciliationFixtureRun(t, recorder, prior, domain.ReconciliationRunAttentionRequired, prior.StartedAt.Add(time.Second))

	// Auto redeem applied the first redemption; the other two stay CONFIRMED.
	if _, err := db.ExecContext(ctx, `
		UPDATE polymarket_redemptions SET status='APPLIED', applied_at=$3
		WHERE execution_account_id=$1 AND condition_id=$2`, account, applied, now.Add(-5*time.Minute)); err != nil {
		t.Fatal(err)
	}

	// A failed run never closes anything.
	failed := startReconciliationFixtureRun(t, recorder, account, "failed-stalled", now.Add(-2*time.Minute))
	failed.Status = domain.ReconciliationRunFailed
	failedAt := failed.StartedAt.Add(time.Second)
	failed.CompletedAt = &failedAt
	if _, _, err := recorder.CompleteWithState(ctx, failed); err != nil {
		t.Fatal(err)
	}
	assertStalledIssueStatus(t, db, account, applied, "OPEN")

	current := startReconciliationFixtureRun(t, recorder, account, "current-stalled", now)
	if err := recorder.RecordIssue(ctx, stalledIssue(current.RunID, stillStalled, now)); err != nil {
		t.Fatal(err)
	}
	current.Status = domain.ReconciliationRunAttentionRequired
	completedAt := now.Add(time.Second)
	current.CompletedAt = &completedAt
	persisted, open, err := recorder.CompleteWithState(ctx, current)
	if err != nil {
		t.Fatal(err)
	}
	assertStalledIssueStatus(t, db, account, applied, "RESOLVED")
	assertStalledIssueStatus(t, db, account, stillStalled, "OPEN")
	// Still CONFIRMED: disappearing from one run is not evidence of the apply.
	assertStalledIssueStatus(t, db, account, notReproduced, "OPEN")
	if persisted.Summary["issues_resolved"] != 1 {
		t.Fatalf("summary = %#v, want exactly one automatic resolution", persisted.Summary)
	}
	if impact := domain.SummarizeReconciliationImpact(open); impact.AccountWide || len(impact.TokenIDs) != 0 || len(impact.OrderIDs) != 0 {
		t.Fatalf("stalled redemption observations block trading: %#v", impact)
	}
	var scope, resolution, details string
	if err := db.QueryRowContext(ctx, `
		SELECT impact_scope, resolution, details FROM reconciliation_issues
		WHERE execution_account_id=$1 AND condition_id=$2`, account, applied).Scan(&scope, &resolution, &details); err != nil {
		t.Fatal(err)
	}
	if scope != "NONE" || resolution != "AUTOMATIC" || !strings.Contains(details, "applied to the ledger") {
		t.Fatalf("resolved stalled issue scope/resolution/details = %s/%s/%s", scope, resolution, details)
	}
}

func assertStalledIssueStatus(t *testing.T, db *sql.DB, account, condition, want string) {
	t.Helper()
	var status string
	if err := db.QueryRow(`
		SELECT status FROM reconciliation_issues
		WHERE execution_account_id=$1 AND condition_id=$2 AND issue_type='REDEMPTION_APPLY_STALLED'`,
		account, condition).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != want {
		t.Fatalf("REDEMPTION_APPLY_STALLED %s status = %s, want %s", condition, status, want)
	}
}
