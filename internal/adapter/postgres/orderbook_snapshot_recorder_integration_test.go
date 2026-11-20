package postgres

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

func TestOrderBookSnapshotRecorderPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	decisionAt := time.Date(2026, 9, 9, 5, 20, 0, 0, time.UTC)
	createdAt := decisionAt.Add(time.Minute)
	recorder, err := NewOrderBookSnapshotRecorder(db, func() time.Time { return createdAt })
	if err != nil {
		t.Fatal(err)
	}
	batch := integrationOrderBookBatch(t, decisionAt, "predsnap-storage")

	stored, created, err := recorder.ClaimBatch(context.Background(), batch)
	if err != nil || !created || stored.SnapshotSetID != batch.SnapshotSetID || !stored.CreatedAt.Equal(createdAt) {
		t.Fatalf("first ClaimBatch() = created %t, stored %#v, error %v", created, stored, err)
	}
	stored, created, err = recorder.ClaimBatch(context.Background(), batch)
	if err != nil || created || stored.SnapshotSetID != batch.SnapshotSetID || !stored.CreatedAt.Equal(createdAt) {
		t.Fatalf("equal ClaimBatch() = created %t, stored %#v, error %v", created, stored, err)
	}

	var rows, okCount, missingCount int
	if err := db.QueryRow(`
		SELECT count(*), count(*) FILTER (WHERE status='OK'), count(*) FILTER (WHERE status='MISSING')
		FROM strategy_orderbook_snapshots WHERE decision_at=$1`, decisionAt).
		Scan(&rows, &okCount, &missingCount); err != nil {
		t.Fatal(err)
	}
	if rows != 2 || okCount != 1 || missingCount != 1 {
		t.Fatalf("stored snapshot counts = %d/%d/%d", rows, okCount, missingCount)
	}
	var bids, asks string
	var bestBid, bestAsk, tickSize, minOrderSize string
	if err := db.QueryRow(`
		SELECT bids::text, asks::text, best_bid::text, best_ask::text,
		       tick_size::text, min_order_size::text
		FROM strategy_orderbook_snapshots
		WHERE decision_at=$1 AND token_id='token-ok'`, decisionAt).
		Scan(&bids, &asks, &bestBid, &bestAsk, &tickSize, &minOrderSize); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(bids, `"price": "0.49"`) || !strings.Contains(asks, `"price": "0.51"`) ||
		bestBid != "0.49" || bestAsk != "0.51" || tickSize != "0.01" || minOrderSize != "1" {
		t.Fatalf("stored OK book = bids %s asks %s bbo %s/%s metadata %s/%s", bids, asks, bestBid, bestAsk, tickSize, minOrderSize)
	}

	changedBooks := append([]domain.OrderBookSnapshot(nil), batch.Snapshots...)
	changedBooks[0].ObservedAt = changedBooks[0].ObservedAt.Add(time.Second)
	conflict, err := domain.NewOrderBookSnapshotBatch(decisionAt, batch.ExecutionAccountID, batch.PredictionSnapshotID, len(changedBooks), changedBooks)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := recorder.ClaimBatch(context.Background(), conflict); !errors.Is(err, port.ErrOrderBookSnapshotConflict) {
		t.Fatalf("conflicting ClaimBatch() error = %v", err)
	}
	if _, err := db.Exec(`ALTER TABLE strategy_orderbook_snapshots DISABLE TRIGGER strategy_orderbook_snapshots_append_only_trigger`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE strategy_orderbook_snapshots SET error_code='corrupt' WHERE decision_at=$1 AND token_id='token-missing'`, decisionAt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE strategy_orderbook_snapshots ENABLE TRIGGER strategy_orderbook_snapshots_append_only_trigger`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := recorder.ClaimBatch(context.Background(), batch); !errors.Is(err, port.ErrOrderBookSnapshotConflict) {
		t.Fatalf("corrupt child replay error = %v", err)
	}
	if _, err := db.Exec(`UPDATE strategy_orderbook_snapshots SET error_code='mutated' WHERE decision_at=$1`, decisionAt); err == nil {
		t.Fatal("append-only snapshot UPDATE succeeded")
	}
}

func TestOrderBookSnapshotRecorderRollsBackPartialBatchPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	if _, err := db.Exec(`
		CREATE FUNCTION reject_test_snapshot() RETURNS TRIGGER LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.token_id='token-missing' THEN RAISE EXCEPTION 'injected snapshot failure'; END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER reject_test_snapshot_trigger
		BEFORE INSERT ON strategy_orderbook_snapshots
		FOR EACH ROW EXECUTE FUNCTION reject_test_snapshot()`); err != nil {
		t.Fatal(err)
	}
	decisionAt := time.Date(2026, 9, 9, 5, 30, 0, 0, time.UTC)
	recorder, err := NewOrderBookSnapshotRecorder(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := recorder.ClaimBatch(context.Background(), integrationOrderBookBatch(t, decisionAt, "predsnap-rollback")); err == nil || !strings.Contains(err.Error(), "injected snapshot failure") {
		t.Fatalf("ClaimBatch() error = %v", err)
	}
	var batches, snapshots int
	if err := db.QueryRow(`SELECT count(*) FROM strategy_orderbook_snapshot_batches WHERE decision_at=$1`, decisionAt).Scan(&batches); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM strategy_orderbook_snapshots WHERE decision_at=$1`, decisionAt).Scan(&snapshots); err != nil {
		t.Fatal(err)
	}
	if batches != 0 || snapshots != 0 {
		t.Fatalf("partial batch remained after rollback: batches=%d snapshots=%d", batches, snapshots)
	}
}

func TestOrderBookSnapshotMigrationConstraintsAndIndexesPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	decisionAt := time.Date(2026, 9, 9, 5, 40, 0, 0, time.UTC)
	if _, err := db.Exec(`
		INSERT INTO strategy_orderbook_snapshot_batches (
			decision_at,prediction_snapshot_id,snapshot_set_id,target_count,snapshot_count,
			ok_count,empty_count,missing_count,error_count
		) VALUES ($1,'predsnap-incomplete','set-incomplete',1,1,1,0,0,0)`, decisionAt.Add(-10*time.Minute)); err == nil || !strings.Contains(err.Error(), "batch is incomplete") {
		t.Fatalf("incomplete batch error = %v", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`
		INSERT INTO strategy_orderbook_snapshot_batches (
			decision_at,prediction_snapshot_id,snapshot_set_id,target_count,snapshot_count,
			ok_count,empty_count,missing_count,error_count
		) VALUES ($1,'predsnap-constraints','set-constraints',1,1,1,0,0,0)`, decisionAt); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`
		INSERT INTO strategy_orderbook_snapshots (
			decision_at,market_source,market_id,condition_id,token_id,outcome_index,status,
			observed_at,depth_limit,bids,asks
		) VALUES ($1,'POLYMARKET','market','condition','bad-token',0,'OK',$2,15,'[]','[]')`,
		decisionAt, decisionAt.Add(time.Second)); err == nil {
		t.Fatal("OK snapshot without BBO and both sides satisfied database constraints")
	}
	_ = tx.Rollback()

	batchAt := decisionAt.Add(10 * time.Minute)
	recorder, err := NewOrderBookSnapshotRecorder(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := recorder.ClaimBatch(context.Background(), integrationOrderBookBatch(t, batchAt, "predsnap-index")); err != nil {
		t.Fatal(err)
	}
	connection, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(context.Background(), `SET enable_seqscan=off`); err != nil {
		t.Fatal(err)
	}
	for _, query := range []struct {
		sql       string
		argument  string
		indexName string
	}{
		{`SELECT token_id FROM strategy_orderbook_snapshots WHERE token_id=$1 AND decision_at >= $2`, "token-ok", "strategy_orderbook_snapshots_token_time_idx"},
		{`SELECT token_id FROM strategy_orderbook_snapshots WHERE condition_id=$1 AND decision_at >= $2`, "condition-1", "strategy_orderbook_snapshots_condition_time_idx"},
	} {
		var plan string
		rows, err := connection.QueryContext(context.Background(), `EXPLAIN (COSTS OFF, FORMAT TEXT) `+query.sql, query.argument, decisionAt)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			plan += line + "\n"
		}
		rows.Close()
		if !strings.Contains(plan, query.indexName) {
			t.Fatalf("query plan did not use %s:\n%s", query.indexName, plan)
		}
	}
}

func integrationOrderBookBatch(t *testing.T, decisionAt time.Time, predictionSnapshotID string) domain.OrderBookSnapshotBatch {
	t.Helper()
	return integrationAccountOrderBookBatch(t, decisionAt, "main", predictionSnapshotID)
}

func integrationAccountOrderBookBatch(
	t *testing.T, decisionAt time.Time, executionAccountID, predictionSnapshotID string,
) domain.OrderBookSnapshotBatch {
	t.Helper()
	books := []domain.OrderBookSnapshot{
		{
			MarketID: "market-1", ConditionID: "condition-1", OutcomeIndex: 0, TokenID: "token-ok",
			Status: domain.OrderBookStatusOK, SourceAt: decisionAt, ObservedAt: decisionAt.Add(time.Second),
			TickSize: "0.01", MinOrderSize: "1", BestBid: "0.49", BestAsk: "0.51",
			DepthLimit: domain.StrategyOrderBookDepth,
			Bids:       []domain.PriceLevel{{Price: "0.49", Size: "10"}},
			Asks:       []domain.PriceLevel{{Price: "0.51", Size: "11"}},
		},
		{
			MarketID: "market-1", ConditionID: "condition-1", OutcomeIndex: 1, TokenID: "token-missing",
			Status: domain.OrderBookStatusMissing, ErrorCode: "SOURCE_DID_NOT_RETURN_TOKEN",
			ObservedAt: decisionAt.Add(time.Second), DepthLimit: domain.StrategyOrderBookDepth,
			Bids: []domain.PriceLevel{}, Asks: []domain.PriceLevel{},
		},
	}
	batch, err := domain.NewOrderBookSnapshotBatch(decisionAt, executionAccountID, predictionSnapshotID, len(books), books)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

// TestOrderBookSnapshotRecorderKeepsOneBatchPerAccountPostgresIntegration:
// accounts of one boundary each own a batch, identical books included, next
// to a legacy shared batch; first write wins per account and the deferred
// completeness check counts only that account's rows.
func TestOrderBookSnapshotRecorderKeepsOneBatchPerAccountPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	decisionAt := time.Date(2026, 11, 20, 4, 10, 0, 0, time.UTC)
	recorder, err := NewOrderBookSnapshotRecorder(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	legacy := integrationAccountOrderBookBatch(t, decisionAt, "", "predsnap-accounts")
	main := integrationAccountOrderBookBatch(t, decisionAt, "main", "predsnap-accounts")
	wallet := integrationAccountOrderBookBatch(t, decisionAt, "wallet-1", "predsnap-accounts")
	for _, batch := range []domain.OrderBookSnapshotBatch{legacy, main, wallet} {
		stored, created, err := recorder.ClaimBatch(context.Background(), batch)
		if err != nil || !created || stored.ExecutionAccountID != batch.ExecutionAccountID {
			t.Fatalf("ClaimBatch(%q) = created %t, stored %#v, error %v", batch.ExecutionAccountID, created, stored, err)
		}
	}
	if main.SnapshotSetID == wallet.SnapshotSetID || main.SnapshotSetID == legacy.SnapshotSetID {
		t.Fatal("identical books of two accounts share a snapshot_set_id")
	}
	stored, created, err := recorder.ClaimBatch(context.Background(), wallet)
	if err != nil || created || stored.ExecutionAccountID != "wallet-1" || len(stored.Snapshots) != 2 {
		t.Fatalf("equal replay = created %t, stored %#v, error %v", created, stored, err)
	}
	changed := append([]domain.OrderBookSnapshot(nil), wallet.Snapshots...)
	changed[0].ObservedAt = changed[0].ObservedAt.Add(time.Second)
	conflict, err := domain.NewOrderBookSnapshotBatch(decisionAt, "wallet-1", "predsnap-accounts", len(changed), changed)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := recorder.ClaimBatch(context.Background(), conflict); !errors.Is(err, port.ErrOrderBookSnapshotConflict) {
		t.Fatalf("conflicting account replay error = %v", err)
	}
	var batches, rows int
	if err := db.QueryRow(`SELECT count(*) FROM strategy_orderbook_snapshot_batches WHERE decision_at=$1`, decisionAt).Scan(&batches); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM strategy_orderbook_snapshots WHERE decision_at=$1`, decisionAt).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if batches != 3 || rows != 6 {
		t.Fatalf("stored batches/rows = %d/%d, want 3/6", batches, rows)
	}
	// A header whose own rows are missing is still incomplete, even though
	// other accounts of the boundary have rows.
	if _, err := db.Exec(`
		INSERT INTO strategy_orderbook_snapshot_batches (
			decision_at,execution_account_id,prediction_snapshot_id,snapshot_set_id,target_count,snapshot_count,
			ok_count,empty_count,missing_count,error_count
		) VALUES ($1,'wallet-2','predsnap-accounts','set-wallet-2-incomplete',2,2,1,0,1,0)`, decisionAt); err == nil ||
		!strings.Contains(err.Error(), "batch is incomplete") {
		t.Fatalf("incomplete account batch error = %v", err)
	}
	// Child rows must belong to a header of the same account.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`
		INSERT INTO strategy_orderbook_snapshots (
			decision_at,execution_account_id,market_source,market_id,condition_id,token_id,outcome_index,status,
			error_code,observed_at,depth_limit,bids,asks
		) VALUES ($1,'wallet-9','POLYMARKET','market','condition','token',0,'MISSING','X',$2,15,'[]','[]')`,
		decisionAt, decisionAt.Add(time.Second)); err == nil {
		t.Fatal("snapshot row without its account header satisfied the foreign key")
	}
	_ = tx.Rollback()
}

// TestOrderBookSnapshotRollbackScriptPostgresIntegration runs the deploy
// rollback of 0053: per-account batches are removed, legacy ones kept, the
// one-batch-per-boundary key, completeness function and binding index are
// back, readiness of the new release fails, and 0053 can be applied again.
func TestOrderBookSnapshotRollbackScriptPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	decisionAt := time.Date(2026, 11, 20, 4, 20, 0, 0, time.UTC)
	recorder, err := NewOrderBookSnapshotRecorder(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, account := range []string{"", "main", "wallet-1"} {
		at := decisionAt
		if account == "" {
			at = decisionAt.Add(-10 * time.Minute)
		}
		if _, _, err := recorder.ClaimBatch(context.Background(), integrationAccountOrderBookBatch(t, at, account, "predsnap-rollback")); err != nil {
			t.Fatal(err)
		}
	}
	script, err := os.ReadFile("../../../deploy/rollback_0053_account_orderbook_snapshots.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(script)); err != nil {
		t.Fatalf("rollback script: %v", err)
	}
	var batches, rows int
	if err := db.QueryRow(`SELECT count(*) FROM strategy_orderbook_snapshot_batches`).Scan(&batches); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM strategy_orderbook_snapshots`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if batches != 1 || rows != 2 {
		t.Fatalf("after rollback batches/rows = %d/%d, want only the legacy batch", batches, rows)
	}
	checker, err := NewHealthChecker(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := checker.Check(context.Background()); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("readiness after rollback = %v, want the new release to refuse the old schema", err)
	}
	var indexes int
	if err := db.QueryRow(`SELECT count(*) FROM pg_indexes WHERE schemaname=current_schema()
		AND indexname='execution_strategy_bindings_enabled_model_strategy_uidx'`).Scan(&indexes); err != nil {
		t.Fatal(err)
	}
	if indexes != 1 {
		t.Fatal("rollback did not restore the enabled binding unique index")
	}
	migration, err := os.ReadFile("../../../migrations/0053_account_orderbook_snapshots.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(migration)); err != nil {
		t.Fatalf("re-apply 0053: %v", err)
	}
	if err := checker.Check(context.Background()); err != nil {
		t.Fatalf("readiness after re-applying 0053: %v", err)
	}
}
