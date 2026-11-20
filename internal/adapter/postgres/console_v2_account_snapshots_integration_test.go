package postgres

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

// insertConsoleSnapshotBatch seals one batch of OK books for the given tokens
// at one mid price; account "" is a legacy shared batch.
func insertConsoleSnapshotBatch(
	t *testing.T, db *sql.DB, decisionAt time.Time, account, setID, bid, ask string, tokens ...string,
) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`
		INSERT INTO strategy_orderbook_snapshot_batches (
			decision_at,execution_account_id,prediction_snapshot_id,snapshot_set_id,target_count,snapshot_count,
			ok_count,empty_count,missing_count,error_count,created_at
		) VALUES ($1,$2,'predsnap-console',$3,$4,$4,$4,0,0,0,$1)`, decisionAt, account, setID, len(tokens)); err != nil {
		t.Fatal(err)
	}
	for _, token := range tokens {
		if _, err := tx.Exec(`
			INSERT INTO strategy_orderbook_snapshots (
				decision_at,execution_account_id,market_source,market_id,condition_id,token_id,outcome_index,status,
				source_at,observed_at,best_bid,best_ask,depth_limit,bids,asks
			) VALUES ($1,$2,'POLYMARKET','market-1','condition-1',$3,0,'OK',$1,$1,$4::numeric,$5::numeric,1,
			          jsonb_build_array(jsonb_build_object('price',$4::text,'size','1')),
			          jsonb_build_array(jsonb_build_object('price',$5::text,'size','1')))`,
			decisionAt, account, token, bid, ask); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// TestPostgresIntegrationConsoleV2CyclesUseAccountSnapshotBatch: with one batch
// per account and a legacy shared batch at the same boundary, every cycle is
// listed once with its own batch (falling back to the shared one), and its
// detail lists only that batch's books.
func TestPostgresIntegrationConsoleV2CyclesUseAccountSnapshotBatch(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	now := time.Now().UTC().Truncate(time.Second)
	decisionAt := now.Add(-time.Hour).Truncate(10 * time.Minute)
	insertConsoleSnapshotBatch(t, db, decisionAt, "", "set-legacy", "0.40", "0.60", "token-legacy-a", "token-legacy-b")
	insertConsoleSnapshotBatch(t, db, decisionAt, "main", "set-main", "0.40", "0.60", "token-main")
	insertConsoleSnapshotBatch(t, db, decisionAt, "wallet-1", "set-wallet-1", "0.40", "0.60", "token-w1-a", "token-w1-b", "token-w1-c")
	for _, account := range []string{"main", "wallet-1", "wallet-6"} {
		if _, err := db.Exec(`
			INSERT INTO strategy_decision_runs (
				cycle_id,input_id,decision_at,model_id,strategy_id,execution_account_id,input_payload,created_at,updated_at
			) VALUES ($1,$2,$3,'model','brier_mid_edge_once',$4,'{"prediction_snapshot_id":"predsnap-console","predictions":[],"positions":[]}'::jsonb,$3,$3)`,
			account+":cycle", "input-"+account, decisionAt, account); err != nil {
			t.Fatal(err)
		}
	}
	reader := consolev2.New(db, consolev2.RuntimeConfig{Now: func() time.Time { return now }})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cycles, err := reader.DecisionCycles(ctx, consolev2.ListFilter{Limit: 20})
	if err != nil || cycles.Total != 3 || len(cycles.Items) != 3 {
		t.Fatalf("cycles = %#v, err %v, want each cycle exactly once", cycles, err)
	}
	want := map[string]string{"main": "set-main", "wallet-1": "set-wallet-1", "wallet-6": "set-legacy"}
	wantBooks := map[string]int{"main": 1, "wallet-1": 3, "wallet-6": 2}
	for _, item := range cycles.Items {
		if item.OrderbookSnapshotSetID != want[item.ExecutionAccountID] {
			t.Fatalf("cycle %s snapshot set = %q, want %q", item.CycleID, item.OrderbookSnapshotSetID, want[item.ExecutionAccountID])
		}
		detail, err := reader.DecisionCycle(ctx, item.CycleID)
		if err != nil {
			t.Fatal(err)
		}
		if detail.OrderbookSnapshotSetID != want[item.ExecutionAccountID] || len(detail.OrderbookSnapshots) != wantBooks[item.ExecutionAccountID] {
			t.Fatalf("detail of %s = set %q with %d books, want %q with %d",
				item.ExecutionAccountID, detail.OrderbookSnapshotSetID, len(detail.OrderbookSnapshots),
				want[item.ExecutionAccountID], wantBooks[item.ExecutionAccountID])
		}
	}
}

// TestPostgresIntegrationConsoleV2PerformanceUsesAccountSnapshotBatch: account
// valuation reads only the account's own latest batch, never another
// account's batch of the same boundary, so no quote is counted twice; an
// account without its own batch falls back to the legacy shared one.
func TestPostgresIntegrationConsoleV2PerformanceUsesAccountSnapshotBatch(t *testing.T) {
	f := newKalshiNettingFixture(t, "kalshi:performance-accounts", "1000")
	f.buy("NET", "YES", "10", "0.3", "0.1")
	f.buy("PLAIN", "YES", "2", "0.6", "0.1")
	read := func() consolev2.AccountPerformance {
		t.Helper()
		r := consolev2.New(f.database, consolev2.RuntimeConfig{Now: func() time.Time { return f.clock }, ManagedAccounts: []consolev2.ManagedAccount{{ExecutionAccountID: f.account, MarketSource: "KALSHI"}}})
		result, err := r.AccountPerformance(context.Background(), f.account)
		if err != nil || len(result.Items) != 1 {
			t.Fatalf("performance=%#v err=%v", result, err)
		}
		return result.Items[0]
	}
	seal := func(decisionAt time.Time, account, setID, bid, ask string) {
		t.Helper()
		tx, err := f.database.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		var count int
		if err := tx.QueryRow(`SELECT count(DISTINCT token_id) FROM position_lots WHERE execution_account_id=$1 AND remaining_shares>0 AND status='OPEN'`, f.account).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(`INSERT INTO strategy_orderbook_snapshot_batches(decision_at,execution_account_id,prediction_snapshot_id,snapshot_set_id,target_count,snapshot_count,ok_count,empty_count,missing_count,error_count,created_at) VALUES($1,$2,'performance-prediction',$3,$4,$4,$4,0,0,0,$1)`, decisionAt, account, setID, count); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(`INSERT INTO strategy_orderbook_snapshots(decision_at,execution_account_id,market_source,market_id,condition_id,token_id,outcome_index,status,source_at,observed_at,best_bid,best_ask,depth_limit,bids,asks)
		 SELECT DISTINCT $1::timestamptz,$2,'KALSHI',market_id,condition_id,token_id,outcome_index,'OK',$1::timestamptz,$1::timestamptz,$3::numeric,$4::numeric,1,
		 jsonb_build_array(jsonb_build_object('price',$3::text,'size','1')),jsonb_build_array(jsonb_build_object('price',$4::text,'size','1'))
		 FROM position_lots WHERE execution_account_id=$5 AND remaining_shares>0 AND status='OPEN'`, decisionAt, account, bid, ask, f.account); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	decisionAt := f.clock.Add(-time.Minute)
	// Only a legacy shared batch: it is used (mid 0.5, 12 shares).
	seal(decisionAt.Add(-10*time.Minute), "", "legacy-set", "0.4", "0.6")
	item := read()
	if item.ValuationState != "complete" || item.MarketValue == nil || item.SnapshotDecisionAt == nil ||
		!item.SnapshotDecisionAt.Equal(decisionAt.Add(-10*time.Minute)) {
		t.Fatalf("legacy valuation = %#v", item)
	}
	assertConsoleAmount(t, *item.MarketValue, "6")
	// The account's own batch and another account's batch of a later boundary:
	// only the own batch counts (mid 0.7), never both.
	seal(decisionAt, f.account, "own-set", "0.6", "0.8")
	seal(decisionAt, "kalshi:someone-else", "other-set", "0.1", "0.3")
	item = read()
	if item.ValuationState != "complete" || item.PricedAssetCount != item.AssetCount || item.MarketValue == nil ||
		item.SnapshotDecisionAt == nil || !item.SnapshotDecisionAt.Equal(decisionAt) {
		t.Fatalf("account valuation = %#v", item)
	}
	assertConsoleAmount(t, *item.MarketValue, "8.4")
}
