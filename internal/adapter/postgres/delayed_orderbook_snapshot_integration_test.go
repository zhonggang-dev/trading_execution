package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// TestDelayedOrderBookSnapshotRecorderPostgresIntegration proves the delayed
// lane keeps its own first-write-wins batch for the same decision_at as the
// primary lane, without touching the primary tables.
func TestDelayedOrderBookSnapshotRecorderPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	ctx := context.Background()
	decisionAt := time.Date(2026, 11, 16, 8, 20, 0, 0, time.UTC)
	if err := CheckDelayedOrderBookSnapshotMigration(ctx, db); err != nil {
		t.Fatalf("CheckDelayedOrderBookSnapshotMigration() error = %v", err)
	}
	primary, err := NewOrderBookSnapshotRecorder(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	delayed, err := NewDelayedOrderBookSnapshotRecorder(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	primaryBatch := integrationOrderBookBatch(t, decisionAt, "predsnap-shared")
	if _, created, err := primary.ClaimBatch(ctx, primaryBatch); err != nil || !created {
		t.Fatalf("primary ClaimBatch() = created %t, error %v", created, err)
	}
	// The delayed lane captures the same tokens five minutes later: a
	// different batch for the same decision_at.
	delayedBatch := shiftedOrderBookBatch(t, primaryBatch, 5*time.Minute+15*time.Second, "0.47")
	stored, created, err := delayed.ClaimBatch(ctx, delayedBatch)
	if err != nil || !created || stored.SnapshotSetID != delayedBatch.SnapshotSetID {
		t.Fatalf("delayed ClaimBatch() = created %t, stored %#v, error %v", created, stored, err)
	}
	if _, created, err := delayed.ClaimBatch(ctx, delayedBatch); err != nil || created {
		t.Fatalf("equal delayed replay = created %t, error %v", created, err)
	}
	conflict := shiftedOrderBookBatch(t, primaryBatch, 5*time.Minute+16*time.Second, "0.47")
	if _, _, err := delayed.ClaimBatch(ctx, conflict); !errors.Is(err, port.ErrOrderBookSnapshotConflict) {
		t.Fatalf("conflicting delayed ClaimBatch() error = %v", err)
	}
	// The primary batch is still exactly the first observation, and the
	// primary recorder never sees the delayed rows.
	if _, created, err := primary.ClaimBatch(ctx, primaryBatch); err != nil || created {
		t.Fatalf("primary replay after delayed write = created %t, error %v", created, err)
	}
	if _, _, err := primary.ClaimBatch(ctx, delayedBatch); !errors.Is(err, port.ErrOrderBookSnapshotConflict) {
		t.Fatalf("delayed batch into primary tables error = %v", err)
	}
	for table, want := range map[string]string{
		"strategy_orderbook_snapshots": "0.49",
		"delayed_orderbook_snapshots":  "0.47",
	} {
		var rows int
		var bestBid string
		if err := db.QueryRow(`SELECT count(*), max(best_bid)::text FROM `+table+` WHERE decision_at=$1`, decisionAt).
			Scan(&rows, &bestBid); err != nil {
			t.Fatal(err)
		}
		if rows != 2 || bestBid != want {
			t.Fatalf("%s rows=%d best_bid=%s, want 2/%s", table, rows, bestBid, want)
		}
	}
	var batches int
	if err := db.QueryRow(`SELECT count(*) FROM delayed_orderbook_snapshot_batches WHERE decision_at=$1 AND snapshot_set_id=$2`,
		decisionAt, delayedBatch.SnapshotSetID).Scan(&batches); err != nil || batches != 1 {
		t.Fatalf("delayed batch rows = %d, error %v", batches, err)
	}

	// Append-only and complete-batch triggers hold on the delayed pair.
	for _, statement := range []string{
		`UPDATE delayed_orderbook_snapshots SET error_code='mutated'`,
		`DELETE FROM delayed_orderbook_snapshots`,
		`UPDATE delayed_orderbook_snapshot_batches SET ok_count=ok_count`,
		`DELETE FROM delayed_orderbook_snapshot_batches`,
		`TRUNCATE delayed_orderbook_snapshots`,
	} {
		if _, err := db.Exec(statement); err == nil || !strings.Contains(err.Error(), "delayed orderbook snapshots are append-only") {
			t.Fatalf("%s error = %v", statement, err)
		}
	}
	if _, err := db.Exec(`
		INSERT INTO delayed_orderbook_snapshot_batches (
			decision_at,prediction_snapshot_id,snapshot_set_id,target_count,snapshot_count,
			ok_count,empty_count,missing_count,error_count
		) VALUES ($1,'predsnap-incomplete','set-incomplete',1,1,1,0,0,0)`, decisionAt.Add(10*time.Minute)); err == nil ||
		!strings.Contains(err.Error(), "delayed orderbook snapshot batch is incomplete") {
		t.Fatalf("incomplete delayed batch error = %v", err)
	}
}

// TestDelayedOrderBookSnapshotSchemaMirrorsPrimaryPostgresIntegration checks
// that migration 0048 reproduces every constraint, index and trigger of 0026,
// and that the startup check fails closed without it.
func TestDelayedOrderBookSnapshotSchemaMirrorsPrimaryPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	ctx := context.Background()
	rename := strings.NewReplacer(
		"strategy_orderbook_snapshot_batches", "delayed_orderbook_snapshot_batches",
		"strategy_orderbook_snapshots", "delayed_orderbook_snapshots",
		"execution_require_complete_strategy_orderbook_snapshot_batch", "execution_require_complete_delayed_orderbook_snapshot_batch",
		"execution_reject_strategy_orderbook_snapshot_mutation", "execution_reject_delayed_orderbook_snapshot_mutation",
	)
	for name, query := range map[string]string{
		"constraints": `
			SELECT c.conname || ' ' || pg_get_constraintdef(c.oid)
			FROM pg_constraint c
			WHERE c.conrelid = to_regclass($1)
			ORDER BY c.conname`,
		"indexes": `
			SELECT indexname || ' ' || regexp_replace(indexdef, ' ON [^ ]+\.', ' ON ')
			FROM pg_indexes
			WHERE schemaname = current_schema() AND tablename = $1
			ORDER BY indexname`,
		"triggers": `
			SELECT t.tgname || ' ' || regexp_replace(pg_get_triggerdef(t.oid), ' ON [^ ]+\.', ' ON ')
			FROM pg_trigger t
			WHERE t.tgrelid = to_regclass($1) AND NOT t.tgisinternal
			ORDER BY t.tgname`,
		"columns": `
			SELECT column_name || ' ' || data_type || ' ' || is_nullable || ' ' || COALESCE(column_default, '')
			FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = $1
			ORDER BY ordinal_position`,
	} {
		for _, table := range []string{"strategy_orderbook_snapshot_batches", "strategy_orderbook_snapshots"} {
			want := schemaLines(t, ctx, db, query, table)
			for index := range want {
				want[index] = rename.Replace(want[index])
			}
			got := schemaLines(t, ctx, db, query, rename.Replace(table))
			if len(want) == 0 || strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Fatalf("%s of %s differ:\n%s\nwant\n%s", name, rename.Replace(table), strings.Join(got, "\n"), strings.Join(want, "\n"))
			}
		}
	}
	if _, err := db.Exec(`DROP TABLE delayed_orderbook_snapshots, delayed_orderbook_snapshot_batches`); err != nil {
		t.Fatal(err)
	}
	if err := CheckDelayedOrderBookSnapshotMigration(ctx, db); err == nil || !strings.Contains(err.Error(), "0048_delayed_orderbook_snapshots.sql is not applied") {
		t.Fatalf("CheckDelayedOrderBookSnapshotMigration() without 0048 error = %v", err)
	}
}

// TestDelayedOrderBookSnapshotPrivilegesPostgresIntegration proves the startup
// check fails closed when 0048 is applied but the application role was not
// granted SELECT, INSERT on both delayed tables.
func TestDelayedOrderBookSnapshotPrivilegesPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	ctx := context.Background()
	var schema string
	if err := db.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	role := schema + "_app"
	if _, err := db.ExecContext(ctx, `CREATE ROLE `+role+` NOLOGIN`); err != nil {
		t.Fatalf("create application role (the test database user needs CREATEROLE): %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`REVOKE ALL ON ALL TABLES IN SCHEMA ` + schema + ` FROM ` + role)
		_, _ = db.Exec(`REVOKE ALL ON SCHEMA ` + schema + ` FROM ` + role)
		_, _ = db.Exec(`DROP ROLE ` + role)
	})
	if _, err := db.ExecContext(ctx, `GRANT USAGE ON SCHEMA `+schema+` TO `+role); err != nil {
		t.Fatal(err)
	}
	checkAsRole := func() error {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, err := conn.ExecContext(ctx, `SET ROLE `+role); err != nil {
			t.Fatal(err)
		}
		defer func() { _, _ = conn.ExecContext(ctx, `RESET ROLE`) }()
		return checkDelayedOrderBookSnapshotMigration(ctx, conn)
	}
	if err := checkAsRole(); err == nil || !strings.Contains(err.Error(), "lacks 4 SELECT/INSERT privilege(s)") {
		t.Fatalf("check without grants error = %v", err)
	}
	if _, err := db.ExecContext(ctx, `GRANT SELECT, INSERT ON delayed_orderbook_snapshot_batches TO `+role); err != nil {
		t.Fatal(err)
	}
	if err := checkAsRole(); err == nil || !strings.Contains(err.Error(), "lacks 2 SELECT/INSERT privilege(s)") {
		t.Fatalf("check with one table granted error = %v", err)
	}
	if _, err := db.ExecContext(ctx, `GRANT SELECT, INSERT ON delayed_orderbook_snapshots TO `+role); err != nil {
		t.Fatal(err)
	}
	if err := checkAsRole(); err != nil {
		t.Fatalf("check with grants error = %v", err)
	}
}

func schemaLines(t *testing.T, ctx context.Context, db *sql.DB, query, table string) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, query, table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	lines := make([]string, 0)
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return lines
}

// shiftedOrderBookBatch is a later capture of the same tokens for the same
// decision_at, with another best bid on the OK book.
func shiftedOrderBookBatch(t *testing.T, base domain.OrderBookSnapshotBatch, after time.Duration, bestBid domain.Decimal) domain.OrderBookSnapshotBatch {
	t.Helper()
	books := make([]domain.OrderBookSnapshot, 0, len(base.Snapshots))
	for _, book := range base.Snapshots {
		book.ObservedAt = base.DecisionAt.Add(after)
		if book.Status == domain.OrderBookStatusOK {
			book.SourceAt = base.DecisionAt.Add(after - time.Second)
			book.BestBid = bestBid
			book.Bids = []domain.PriceLevel{{Price: bestBid, Size: "10"}}
		}
		books = append(books, book)
	}
	batch, err := domain.NewOrderBookSnapshotBatch(base.DecisionAt, base.PredictionSnapshotID, len(books), books)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}
