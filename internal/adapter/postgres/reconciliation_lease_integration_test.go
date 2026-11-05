package postgres

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// TestReconciliationRunLeasePostgresIntegration pins the lease rule that
// startup degradation (C-05) relies on: a RUNNING row older than the 30-minute
// lease is closed by the next Start, which then takes over; a fresh RUNNING
// row is never closed and the new Start fails on the unique index, so the
// account only stays in maintenance until the lease expires.
func TestReconciliationRunLeasePostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	recorder, err := NewReconciliationRecorder(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	insertAccount(t, db, "stale-lease", "0x1000000000000000000000000000000000000001", "0", "0", "0")
	insertAccount(t, db, "fresh-lease", "0x1000000000000000000000000000000000000002", "0", "0", "0")
	now := time.Now().UTC().Truncate(time.Microsecond)
	start := func(runID, accountID string, startedAt time.Time) error {
		run, buildErr := (domain.ReconciliationRunParams{
			RunID: runID, ExecutionAccountID: accountID,
			Trigger: domain.ReconciliationTriggerStartup, StartedAt: startedAt,
		}).Build()
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		return recorder.Start(ctx, run)
	}
	status := func(runID string) (string, string, bool) {
		var runStatus, runError string
		var completed bool
		if err := db.QueryRowContext(ctx, `
			SELECT status, error, completed_at IS NOT NULL FROM reconciliation_runs WHERE run_id=$1`, runID,
		).Scan(&runStatus, &runError, &completed); err != nil {
			t.Fatalf("read run %s: %v", runID, err)
		}
		return runStatus, runError, completed
	}

	// Expired lease: a process killed 31 minutes ago left RUNNING behind.
	if err := start("abandoned", "stale-lease", now.Add(-31*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := start("takeover", "stale-lease", now); err != nil {
		t.Fatalf("Start() after an expired lease = %v, want takeover", err)
	}
	if runStatus, runError, completed := status("abandoned"); runStatus != "FAILED" ||
		runError != "reconciliation worker lease expired" || !completed {
		t.Fatalf("abandoned run = %s %q completed=%v", runStatus, runError, completed)
	}
	if runStatus, _, _ := status("takeover"); runStatus != "RUNNING" {
		t.Fatalf("takeover run = %s, want RUNNING", runStatus)
	}

	// Unexpired lease: a SIGKILL 5 minutes ago. The row must not be closed.
	if err := start("recent", "fresh-lease", now.Add(-5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	err = start("blocked", "fresh-lease", now)
	if err == nil || !strings.Contains(err.Error(), "start reconciliation run") {
		t.Fatalf("Start() under a fresh lease = %v, want unique-index rejection", err)
	}
	if runStatus, _, completed := status("recent"); runStatus != "RUNNING" || completed {
		t.Fatalf("fresh RUNNING row was closed: %s completed=%v", runStatus, completed)
	}
	var blockedRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM reconciliation_runs WHERE run_id='blocked'`).Scan(&blockedRows); err != nil {
		t.Fatal(err)
	}
	if blockedRows != 0 {
		t.Fatal("a rejected Start left a row behind")
	}
	// Exactly at the boundary the lease is still owned (strictly older only).
	if err := start("boundary", "fresh-lease", now.Add(-5*time.Minute).Add(30*time.Minute)); err == nil {
		t.Fatal("a row exactly at the lease boundary was taken over")
	}
	if err := start("after-expiry", "fresh-lease", now.Add(-5*time.Minute).Add(30*time.Minute+time.Microsecond)); err != nil {
		t.Fatalf("Start() just after the lease = %v", err)
	}
	if runStatus, _, _ := status("recent"); runStatus != "FAILED" {
		t.Fatalf("expired row = %s, want FAILED", runStatus)
	}
}
