package postgres

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// preDelayedLaneStatements are the four SQL texts of the recorder before the
// delayed decision lane existed (master 71845b4), copied verbatim.
var preDelayedLaneStatements = orderBookSnapshotStatements{
	insertBatch: `
		INSERT INTO strategy_orderbook_snapshot_batches (
			decision_at, prediction_snapshot_id, snapshot_set_id,
			target_count, snapshot_count, ok_count, empty_count,
			missing_count, error_count, created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (decision_at) DO NOTHING`,
	insertSnapshot: `
			INSERT INTO strategy_orderbook_snapshots (
				decision_at, market_source, market_id, condition_id, token_id,
				outcome_index, outcome_id, status, error_code, source_at, observed_at,
				tick_size, min_order_size, best_bid, best_ask, depth_limit,
				bids, asks, created_at
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
				$12::numeric,$13::numeric,$14::numeric,$15::numeric,$16,
				$17::jsonb,$18::jsonb,$19
			)`,
	selectSnapshots: `
		SELECT market_source, market_id, condition_id, token_id, outcome_index,
			outcome_id, status, error_code, source_at, observed_at,
			tick_size::text, min_order_size::text, best_bid::text, best_ask::text,
			depth_limit, bids, asks
		FROM strategy_orderbook_snapshots
		WHERE decision_at=$1
		ORDER BY market_source, condition_id, outcome_index, token_id`,
	selectBatch: `
		SELECT decision_at, prediction_snapshot_id, snapshot_set_id,
			target_count, snapshot_count, ok_count, empty_count,
			missing_count, error_count, created_at
		FROM strategy_orderbook_snapshot_batches
		WHERE decision_at=$1 FOR UPDATE`,
}

// TestOrderBookSnapshotRecorderDefaultSQLIsUnchanged pins the primary
// decision cycle's statements byte for byte.
func TestOrderBookSnapshotRecorderDefaultSQLIsUnchanged(t *testing.T) {
	recorder, err := NewOrderBookSnapshotRecorder(dbPlaceholder(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if recorder.statements != preDelayedLaneStatements {
		t.Fatalf("default recorder SQL changed:\n%#v\nwant\n%#v", recorder.statements, preDelayedLaneStatements)
	}
}

// TestDelayedOrderBookSnapshotRecorderUsesOnlyDelayedTables proves the delayed
// recorder writes and reads only its own pair, with otherwise identical SQL.
func TestDelayedOrderBookSnapshotRecorderUsesOnlyDelayedTables(t *testing.T) {
	recorder, err := NewDelayedOrderBookSnapshotRecorder(dbPlaceholder(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	got := recorder.statements
	for name, pair := range map[string][2]string{
		"insertBatch":     {got.insertBatch, preDelayedLaneStatements.insertBatch},
		"insertSnapshot":  {got.insertSnapshot, preDelayedLaneStatements.insertSnapshot},
		"selectSnapshots": {got.selectSnapshots, preDelayedLaneStatements.selectSnapshots},
		"selectBatch":     {got.selectBatch, preDelayedLaneStatements.selectBatch},
	} {
		delayed, original := pair[0], pair[1]
		if strings.Contains(delayed, "strategy_orderbook") || !strings.Contains(delayed, "delayed_orderbook_snapshot") {
			t.Fatalf("%s targets the wrong tables: %s", name, delayed)
		}
		back := strings.NewReplacer("delayed_orderbook_snapshot_batches", "strategy_orderbook_snapshot_batches",
			"delayed_orderbook_snapshots", "strategy_orderbook_snapshots").Replace(delayed)
		if back != original {
			t.Fatalf("%s differs beyond the table names:\n%s\nwant\n%s", name, back, original)
		}
	}
	if _, err := NewDelayedOrderBookSnapshotRecorder(nil, nil); err == nil {
		t.Fatal("delayed recorder accepted a nil database")
	}
}

// dbPlaceholder returns a handle that is never connected: the recorder only
// stores it at construction.
func dbPlaceholder(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", "postgres://unused")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
