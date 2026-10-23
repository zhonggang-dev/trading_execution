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

const amendmentEvidenceTemplate = `{"wallet_address":"0xabc","block_number":100,"chain_shares":"%s"}`

func insertAmendmentRow(db *sql.DB, id, accountID, token, condition, kind, shares, chainShares string) error {
	_, err := db.Exec(`
		INSERT INTO execution_external_position_baseline_amendments (
			amendment_id, execution_account_id, token_id, condition_id, outcome_index,
			outcome_name, neg_risk, kind, shares, observed_at, evidence, actor, reason
		) VALUES ($1,$2,$3,$4,0,'YES',FALSE,$5,$6::numeric,clock_timestamp(),$7::jsonb,'integration-test','amendment test')`,
		id, accountID, token, condition, kind, shares,
		strings.Replace(amendmentEvidenceTemplate, "%s", chainShares, 1))
	return err
}

func sealBaselineWithItem(t *testing.T, db *sql.DB, accountID, baselineID, token, condition string, shares int) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`
		INSERT INTO execution_external_position_baseline_items (
			baseline_id, execution_account_id, token_id, condition_id, outcome_index, outcome_name, neg_risk, shares
		) VALUES ($1,$2,$3,$4,0,'YES',FALSE,$5)`, baselineID, accountID, token, condition, shares); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`
		INSERT INTO execution_external_position_baselines (
			baseline_id, execution_account_id, source, observed_at, evidence, actor, reason
		) VALUES ($1,$2,'POLYMARKET_DATA_API',clock_timestamp() - interval '1 hour',
		          '{"snapshot_sha256":"amendment-test"}'::jsonb,'integration-test','sealed baseline')`, baselineID, accountID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func remainingFor(t *testing.T, db *sql.DB, accountID, token string) string {
	t.Helper()
	var remaining string
	if err := db.QueryRow(`
		SELECT COALESCE((SELECT sum(shares) FROM execution_external_position_baseline_remaining
		                  WHERE execution_account_id=$1 AND token_id=$2),0)::text`, accountID, token).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	return remaining
}

func TestBaselineAmendmentsPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	const (
		accountID  = "account-baseline-amendments"
		baselineID = "baseline-amendments"
		tokenA     = "amend-token-a"
		tokenB     = "amend-token-b"
		condA      = "amend-condition-a"
		condB      = "amend-condition-b"
	)
	insertAccount(t, db, accountID, "0xbaselineamendments", "20", "20", "0")
	sealBaselineWithItem(t, db, accountID, baselineID, tokenA, condA, 5)
	repository, err := NewExternalPositionBaselineRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	list := func() map[string]domain.ExternalPositionBaseline {
		t.Helper()
		baselines, err := repository.ListExternalPositionBaselines(context.Background(), accountID)
		if err != nil {
			t.Fatal(err)
		}
		byToken := map[string]domain.ExternalPositionBaseline{}
		for _, baseline := range baselines {
			byToken[baseline.TokenID] = baseline
		}
		return byToken
	}

	if got := list(); len(got) != 1 || got[tokenA].Shares.String() != "5" || got[tokenA].BaselineID != baselineID {
		t.Fatalf("sealed baseline only = %#v", got)
	}

	// A residual on a baselined token adds to its unmanaged shares.
	if err := insertAmendmentRow(db, "amend-add-a", accountID, tokenA, condA, "ADD_RESIDUAL", "0.5", "5.5"); err != nil {
		t.Fatalf("residual on baselined token: %v", err)
	}
	if got := list(); len(got) != 1 || !got[tokenA].Shares.Equal("5.5") || got[tokenA].BaselineID != baselineID {
		t.Fatalf("baselined token after residual = %#v", got)
	}

	// A residual on a token the baseline never captured becomes its own entry,
	// and the merged set shares one identity as every consumer requires.
	if err := insertAmendmentRow(db, "amend-add-b", accountID, tokenB, condB, "ADD_RESIDUAL", "0.025174", "5.025174"); err != nil {
		t.Fatalf("residual on uncaptured token: %v", err)
	}
	got := list()
	if len(got) != 2 || !got[tokenB].Shares.Equal("0.025174") || got[tokenB].Source != "BASELINE_AMENDMENT" ||
		got[tokenA].BaselineID != "effective-baseline:"+accountID || got[tokenB].BaselineID != got[tokenA].BaselineID {
		t.Fatalf("merged baselines = %#v", got)
	}
	if remainingFor(t, db, accountID, tokenA) != "5.5" || remainingFor(t, db, accountID, tokenB) != "0.025174" {
		t.Fatalf("view remaining = %s / %s", remainingFor(t, db, accountID, tokenA), remainingFor(t, db, accountID, tokenB))
	}

	// Guards: identity, evidence shape, and exactness of a retirement.
	if err := insertAmendmentRow(db, "amend-bad-identity", accountID, tokenA, "other-condition", "ADD_RESIDUAL", "0.1", "5.6"); err == nil {
		t.Fatal("amendment with a different condition than the baseline item succeeded")
	}
	if _, err := db.Exec(`
		INSERT INTO execution_external_position_baseline_amendments (
			amendment_id, execution_account_id, token_id, condition_id, outcome_index,
			outcome_name, neg_risk, kind, shares, observed_at, evidence, actor, reason
		) VALUES ('amend-no-evidence',$1,$2,$3,0,'YES',FALSE,'ADD_RESIDUAL',0.1,clock_timestamp(),
		          '{"note":"missing keys"}'::jsonb,'integration-test','no evidence')`, accountID, tokenB, condB); err == nil {
		t.Fatal("amendment without chain evidence keys succeeded")
	}
	if err := insertAmendmentRow(db, "amend-zero", accountID, tokenB, condB, "ADD_RESIDUAL", "0", "5"); err == nil {
		t.Fatal("zero-share amendment succeeded")
	}
	if err := insertAmendmentRow(db, "amend-retire-partial", accountID, tokenA, condA, "RETIRE_CONSUMED", "5", "0"); err == nil {
		t.Fatal("partial retirement succeeded")
	}
	if err := insertAmendmentRow(db, "amend-retire-held", accountID, tokenA, condA, "RETIRE_CONSUMED", "5.5", "1"); err == nil {
		t.Fatal("retirement with evidence that the wallet still holds shares succeeded")
	}
	if err := insertAmendmentRow(db, "amend-retire-unknown", accountID, "never-baselined", "c", "RETIRE_CONSUMED", "1", "0"); err == nil {
		t.Fatal("retirement of a token without baseline succeeded")
	}

	// An exact retirement removes the token from the effective baseline.
	if err := insertAmendmentRow(db, "amend-retire-a", accountID, tokenA, condA, "RETIRE_CONSUMED", "5.5", "0"); err != nil {
		t.Fatalf("exact retirement: %v", err)
	}
	got = list()
	if len(got) != 1 || !got[tokenB].Shares.Equal("0.025174") || remainingFor(t, db, accountID, tokenA) != "0" {
		t.Fatalf("after retirement = %#v", got)
	}
	// A token once fully retired cannot be retired again.
	if err := insertAmendmentRow(db, "amend-retire-a-again", accountID, tokenA, condA, "RETIRE_CONSUMED", "5.5", "0"); err == nil {
		t.Fatal("second retirement of an already retired token succeeded")
	}

	// Append-only, including TRUNCATE.
	if _, err := db.Exec(`UPDATE execution_external_position_baseline_amendments SET reason='mutated'`); err == nil {
		t.Fatal("append-only amendment UPDATE succeeded")
	}
	if _, err := db.Exec(`DELETE FROM execution_external_position_baseline_amendments`); err == nil {
		t.Fatal("append-only amendment DELETE succeeded")
	}
	if _, err := db.Exec(`TRUNCATE execution_external_position_baseline_amendments`); err == nil {
		t.Fatal("append-only amendment TRUNCATE succeeded")
	}
}

func TestBaselineAmendmentStoreAppendIsIdempotent(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	const accountID = "account-baseline-amendment-store"
	insertAccount(t, db, accountID, "0xbaselineamendmentstore", "20", "20", "0")
	store, err := NewBaselineAmendmentStore(db)
	if err != nil {
		t.Fatal(err)
	}
	index := 1
	amendment := domain.BaselineAmendment{
		AmendmentID: "store-amendment-1", ExecutionAccountID: accountID, TokenID: "store-token",
		ConditionID: "store-condition", OutcomeIndex: &index, OutcomeName: "NO", NegRisk: true,
		Kind: domain.BaselineAmendmentAddResidual, Shares: "0.02", ObservedAt: time.Now().UTC(),
		Evidence: []byte(`{"wallet_address":"0xabc","block_number":7,"chain_shares":"16.02"}`),
		Actor:    "integration-test", Reason: "store test",
	}
	inserted, err := store.Append(context.Background(), amendment)
	if err != nil || !inserted {
		t.Fatalf("first append inserted=%v err=%v", inserted, err)
	}
	inserted, err = store.Append(context.Background(), amendment)
	if err != nil || inserted {
		t.Fatalf("repeat append inserted=%v err=%v, want a no-op", inserted, err)
	}
	facts, err := store.Facts(context.Background(), accountID, "store-token")
	if err != nil || facts.BaselineRemaining.String() != "0.02" || facts.WalletAddress != "0xbaselineamendmentstore" {
		t.Fatalf("facts = %#v err=%v", facts, err)
	}
}

func TestResolveAmendedBaselineIssues(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	const (
		accountID  = "account-amended-issues"
		baselineID = "baseline-amended-issues"
	)
	insertAccount(t, db, accountID, "0xamendedissues", "20", "20", "0")
	sealBaselineWithItem(t, db, accountID, baselineID, "retired-token", "retired-condition", 3)
	if err := insertAmendmentRow(db, "amended-retire", accountID, "retired-token", "retired-condition", "RETIRE_CONSUMED", "3", "0"); err != nil {
		t.Fatal(err)
	}
	if err := insertAmendmentRow(db, "amended-dust", accountID, "dust-token", "dust-condition", "ADD_RESIDUAL", "0.02", "16.02"); err != nil {
		t.Fatal(err)
	}
	observed := time.Now().UTC().Add(-time.Hour)
	for _, run := range []string{"amended-old-run", "amended-current-run"} {
		if _, err := db.Exec(`
			INSERT INTO reconciliation_runs (run_id,execution_account_id,trigger,status,summary,error,started_at,completed_at)
			VALUES ($1,$2,'SCHEDULED','ATTENTION_REQUIRED','{}'::jsonb,'',$3,$3)`, run, accountID, observed); err != nil {
			t.Fatal(err)
		}
	}
	for _, issue := range []struct{ id, kind, token string }{
		{"issue-retired", "EXTERNAL_POSITION_BASELINE_DRIFT", "retired-token"},
		{"issue-dust", "POSITION_DRIFT", "dust-token"},
		{"issue-unamended", "POSITION_DRIFT", "unamended-token"},
	} {
		if _, err := db.Exec(`
			INSERT INTO reconciliation_issues (issue_id,run_id,fingerprint,execution_account_id,issue_type,resolution,status,token_id,source,details,observed_at)
			VALUES ($1,'amended-old-run',$1,$2,$3,'MANUAL_REVIEW','OPEN',$4,'POLYMARKET_DATA_API','stale',$5)`,
			issue.id, accountID, issue.kind, issue.token, observed); err != nil {
			t.Fatal(err)
		}
	}
	statusOf := func(id string) string {
		var status string
		if err := db.QueryRow(`SELECT status FROM reconciliation_issues WHERE issue_id=$1`, id).Scan(&status); err != nil {
			t.Fatal(err)
		}
		return status
	}
	resolve := func(summary map[string]int) int {
		t.Helper()
		completedAt := time.Now().UTC()
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		count, err := resolveAmendedBaselineIssues(context.Background(), tx, domain.ReconciliationRun{
			RunID: "amended-current-run", ExecutionAccountID: accountID,
			Status: domain.ReconciliationRunCompleted, CompletedAt: &completedAt, Summary: summary,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		return count
	}

	// Without independent verification nothing closes, amendment or not.
	if count := resolve(map[string]int{}); count != 0 {
		t.Fatalf("resolved %d issues without verification", count)
	}
	// A token that raised a new issue in this run never closes its old one.
	if _, err := db.Exec(`
		INSERT INTO reconciliation_issues (issue_id,run_id,fingerprint,execution_account_id,issue_type,resolution,status,token_id,source,details,observed_at)
		VALUES ('issue-reproduced','amended-current-run','reproduced',$1,'PHANTOM_POSITION','MANUAL_REVIEW','OPEN','retired-token','POLYMARKET_DATA_API','still held',$2)`,
		accountID, observed); err != nil {
		t.Fatal(err)
	}
	if count := resolve(map[string]int{"verified_source:EXTERNAL_POSITIONS": 1}); count != 0 || statusOf("issue-retired") != "OPEN" {
		t.Fatalf("closed a retired token that still raised an issue (count=%d)", count)
	}
	if _, err := db.Exec(`UPDATE reconciliation_issues SET status='RESOLVED', resolved_at=clock_timestamp() WHERE issue_id='issue-reproduced'`); err != nil {
		t.Fatal(err)
	}

	count := resolve(map[string]int{
		"verified_source:EXTERNAL_POSITIONS": 1,
		"verified_position:dust-token":       1,
	})
	if count != 2 || statusOf("issue-retired") != "RESOLVED" || statusOf("issue-dust") != "RESOLVED" ||
		statusOf("issue-unamended") != "OPEN" {
		t.Fatalf("resolved=%d retired=%s dust=%s unamended=%s", count,
			statusOf("issue-retired"), statusOf("issue-dust"), statusOf("issue-unamended"))
	}
}

// TestBaselineAmendmentFactsReadManagedPositionIdentity runs the position and
// lot identity queries against the real schema: a residual takes its identity
// from the managed position, so these must resolve (and tolerate a position
// without lots, whose neg_risk is then reported as unknown rather than guessed).
func TestBaselineAmendmentFactsReadManagedPositionIdentity(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	const accountID = "account-baseline-amendment-facts"
	insertAccount(t, db, accountID, "0xBaselineAmendmentFacts", "20", "20", "0")
	if _, err := db.Exec(`
		INSERT INTO execution_positions (
			execution_account_id, market_id, condition_id, token_id, outcome_index, outcome_name,
			total_shares, available_shares, reserved_shares, cost_basis
		) VALUES ($1,'market-facts','condition-facts','token-facts',1,'No',5,5,0,3.4)`, accountID); err != nil {
		t.Fatal(err)
	}
	store, err := NewBaselineAmendmentStore(db)
	if err != nil {
		t.Fatal(err)
	}
	facts, err := store.Facts(context.Background(), accountID, "token-facts")
	if err != nil {
		t.Fatalf("facts: %v", err)
	}
	if facts.WalletAddress != "0xbaselineamendmentfacts" || facts.ManagedShares.String() != "5" ||
		facts.Position == nil || facts.Position.ConditionID != "condition-facts" ||
		facts.Position.OutcomeIndex == nil || *facts.Position.OutcomeIndex != 1 ||
		facts.Position.OutcomeName != "No" || facts.Position.NegRisk != nil || facts.Baseline != nil {
		t.Fatalf("facts = %#v position=%#v", facts, facts.Position)
	}
}
