package consolev2

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// Connection-local copies of the migrated columns let malformed and zero
// quotes be exercised without weakening production's append-only snapshots.
// Separate adapter tests use real FillLedger/SettlementStore/adoption writes.
type performanceFixture struct {
	db     *sql.DB
	reader *Reader
	now    time.Time
}

func newPerformanceFixture(t *testing.T) performanceFixture {
	t.Helper()
	dsn := os.Getenv("TRADING_EXECUTION_CONSOLE_V2_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TRADING_EXECUTION_CONSOLE_V2_TEST_DATABASE_URL is not configured")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	now := time.Date(2026, 11, 4, 6, 0, 0, 0, time.UTC)
	f := performanceFixture{db: db, now: now}
	for _, table := range []string{"execution_accounts", "execution_orders", "position_lots", "position_events", "strategy_orderbook_snapshot_batches", "strategy_orderbook_snapshots"} {
		f.exec(t, `CREATE TEMP TABLE `+table+` AS SELECT * FROM `+table+` WITH NO DATA`)
	}
	f.exec(t, `INSERT INTO execution_accounts(execution_account_id) VALUES('pm'),('unprefixed-k'),('flat'),('unmanaged')`)
	for _, a := range []struct{ id, source, token, market, condition, shares, cost string }{
		{"pm", "POLYMARKET", "token-p", "market-p", "condition-p", "2", "0.4"},
		{"pm", "POLYMARKET", "token-p", "market-p", "condition-p", "3", "0.9"},
		{"unprefixed-k", "KALSHI", "token-k", "market-k", "condition-k", "4", "1.2"},
	} {
		var seq int
		if err := db.QueryRow(`SELECT count(*) FROM position_lots`).Scan(&seq); err != nil {
			t.Fatal(err)
		}
		order := fmt.Sprintf("order-%d", seq)
		f.exec(t, `INSERT INTO execution_orders(order_id,execution_account_id,venue,market_id,token_id,intent) VALUES($1,$2,$3,$4,$5,jsonb_build_object('market_source',$3::text,'condition_id',$6::text))`, order, a.id, a.source, a.market, a.token, a.condition)
		f.exec(t, `INSERT INTO position_lots(lot_id,execution_account_id,market_id,condition_id,token_id,model_id,strategy_id,opening_order_id,outcome_index,outcome_name,status,remaining_shares,remaining_cost,opened_at) VALUES($1,$2,$3,$4,$5,$6,$7,$1,0,'Yes','OPEN',$8,$9,$10)`, order, a.id, a.market, a.condition, a.token, "historical-model-"+order, "historical-strategy-"+order, a.shares, a.cost, now.Add(-800*24*time.Hour))
	}
	from := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	for _, e := range []struct {
		id, kind, cost, pnl string
		at                  time.Time
	}{
		{"a", "BOUGHT", "10", "0", now.Add(-400 * 24 * time.Hour)},
		{"b", "BOUGHT", "5", "0", now.Add(-400 * 24 * time.Hour)},
		{"c", "SOLD", "-10", "100", from.Add(-time.Microsecond)},
		{"d", "SOLD", "-1", "3", from},
		{"e", "SOLD", "-1", "-5", from.Add(24 * time.Hour)},
		{"f", "SOLD", "-2", "4", from.Add(48 * time.Hour)},
		{"g", "ADOPTED", "0.3", "0", now},
		{"future", "BOUGHT", "999", "999", now.Add(time.Microsecond)},
	} {
		f.event(t, "pm", e.id, e.kind, e.cost, e.pnl, e.at)
	}
	f.event(t, "unprefixed-k", "k-buy", "BOUGHT", "1.2", "0", now.Add(-time.Hour))
	f.reader = New(db, RuntimeConfig{Now: func() time.Time { return now }, ManagedAccounts: []ManagedAccount{
		{ExecutionAccountID: "pm", MarketSource: "POLYMARKET"},
		{ExecutionAccountID: "unprefixed-k", MarketSource: "KALSHI"},
		{ExecutionAccountID: "flat", MarketSource: "POLYMARKET"},
		{ExecutionAccountID: "not-registered", MarketSource: "POLYMARKET"},
	}})
	return f
}
func (f performanceFixture) exec(t *testing.T, q string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := f.db.ExecContext(ctx, q, args...); err != nil {
		t.Fatalf("fixture query: %v", err)
	}
}
func (f performanceFixture) event(t *testing.T, account, id, kind, cost, pnl string, at time.Time) {
	t.Helper()
	f.exec(t, `INSERT INTO position_events(position_event_id,event_type,execution_account_id,market_id,token_id,model_id,strategy_id,cost_basis_delta,realized_pnl_delta,occurred_at) VALUES($1,$2,$3,'market','token','old-model','old-strategy',$4,$5,$6)`, id, kind, account, cost, pnl, at)
}
func (f performanceFixture) read(t *testing.T, account string) AccountPerformanceResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := f.reader.AccountPerformance(ctx, account)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func performanceAmount(t *testing.T, actual, expected string) {
	t.Helper()
	if !domain.Decimal(actual).Equal(domain.Decimal(expected)) {
		t.Fatalf("amount %q want %q", actual, expected)
	}
}
func (f performanceFixture) batch(t *testing.T) {
	t.Helper()
	f.exec(t, `INSERT INTO strategy_orderbook_snapshot_batches(decision_at,prediction_snapshot_id,snapshot_set_id,target_count,snapshot_count,ok_count,empty_count,missing_count,error_count,created_at) VALUES($1,'p','current',2,2,2,0,0,0,$2)`, f.now.Add(-5*time.Minute), f.now.Add(-4*time.Minute))
	// source_at is old, but observed_at is fresh. Price freshness must use the
	// actual local capture time rather than the last venue book change time.
	f.exec(t, `INSERT INTO strategy_orderbook_snapshots(decision_at,market_source,market_id,condition_id,token_id,outcome_index,status,best_bid,best_ask,observed_at,source_at) VALUES($1,'POLYMARKET','market-p','condition-p','token-p',0,'OK',0.6,0.8,$2,$3),($1,'KALSHI','market-k','condition-k','token-k',0,'OK',0,0,$2,$3)`, f.now.Add(-5*time.Minute), f.now.Add(-2*time.Minute), f.now.Add(-7*24*time.Hour))
}
func TestAccountPerformanceLedgerAndCompleteValuation(t *testing.T) {
	f := newPerformanceFixture(t)
	noBatch := f.read(t, "")
	if len(noBatch.Items) != 3 || noBatch.Items[0].ExecutionAccountID != "flat" || noBatch.Items[1].ExecutionAccountID != "pm" || noBatch.Items[2].ExecutionAccountID != "unprefixed-k" {
		t.Fatalf("managed/registered sorted scope %#v", noBatch)
	}
	flat := noBatch.Items[0]
	if flat.AssetCount != 0 || flat.ValuationState != "complete" || flat.MarketValue == nil || flat.TotalPnL == nil || flat.ReturnRate != nil {
		t.Fatalf("zero asset semantics %#v", flat)
	}
	performanceAmount(t, *flat.MarketValue, "0")
	pm := noBatch.Items[1]
	if pm.AssetCount != 1 || pm.PricedAssetCount != 0 || strings.Join(pm.ValuationReasons, ",") != "missing_batch" || pm.MarketValue != nil || pm.TotalPnL != nil {
		t.Fatalf("no batch %#v", pm)
	}
	performanceAmount(t, pm.CurrentCost, "1.3")
	performanceAmount(t, pm.PeakHoldingCost, "15")
	performanceAmount(t, pm.CumulativeInvestedCost, "15.3")
	performanceAmount(t, pm.RealizedPnL, "102")
	performanceAmount(t, pm.RealizedDrawdown14D, "5")
	if !pm.DrawdownFrom.Equal(time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)) || !pm.DrawdownTo.Equal(f.now) {
		t.Fatalf("UTC window %#v", pm)
	}
	f.batch(t)
	complete := f.read(t, "pm").Items[0]
	if complete.ValuationState != "complete" || complete.AssetCount != 1 || complete.PricedAssetCount != 1 || len(complete.ValuationReasons) != 0 || complete.ReturnRate == nil {
		t.Fatalf("complete %#v", complete)
	}
	performanceAmount(t, *complete.MarketValue, "3.5")
	performanceAmount(t, *complete.UnrealizedPnL, "2.2")
	performanceAmount(t, *complete.TotalPnL, "104.2")
	if complete.SnapshotObservedAt == nil || !complete.SnapshotObservedAt.Equal(f.now.Add(-2*time.Minute)) || complete.SnapshotDecisionAt == nil || !complete.SnapshotDecisionAt.Equal(f.now.Add(-5*time.Minute)) {
		t.Fatalf("actual sample metadata %#v", complete)
	}
	kalshi := f.read(t, "unprefixed-k").Items[0]
	if kalshi.MarketSource != "KALSHI" || kalshi.MarketValue == nil || kalshi.ValuationState != "complete" {
		t.Fatalf("valid zero quote %#v", kalshi)
	}
	performanceAmount(t, *kalshi.MarketValue, "0")
	performanceAmount(t, *kalshi.UnrealizedPnL, "-1.2")
	performanceAmount(t, *kalshi.ReturnRate, "-1")
	for _, id := range []string{"unmanaged", "not-registered", "unknown", " pm", "pm\t", strings.Repeat("x", 129)} {
		if _, err := f.reader.AccountPerformance(context.Background(), id); !errors.Is(err, ErrInvalidPerformanceAccount) {
			t.Fatalf("invalid account %q: %v", id, err)
		}
	}
	// Query scopes cannot borrow another platform's record even if the token
	// and timestamps match. All five identity fields are required.
	f.exec(t, `UPDATE strategy_orderbook_snapshots SET condition_id='different' WHERE market_source='POLYMARKET'`)
	pm = f.read(t, "pm").Items[0]
	if pm.ValuationState != "incomplete" || strings.Join(pm.ValuationReasons, ",") != "missing_quote" {
		t.Fatalf("wrong identity matched %#v", pm)
	}
}
func TestAccountPerformanceQuoteErrorsAndPartialCoverage(t *testing.T) {
	f := newPerformanceFixture(t)
	f.batch(t)
	// An older valid record must never fill a missing/error member in the
	// latest sealed batch. Headers that claim a future decision are excluded.
	f.exec(t, `INSERT INTO strategy_orderbook_snapshot_batches(decision_at,created_at) VALUES($1,$1),($2,$2)`, f.now.Add(-10*time.Minute), f.now.Add(time.Hour))
	f.exec(t, `INSERT INTO strategy_orderbook_snapshots SELECT $1,market_source,market_id,condition_id,token_id,outcome_index,outcome_id,status,error_code,source_at,observed_at,tick_size,min_order_size,best_bid,best_ask,depth_limit,bids,asks,created_at FROM strategy_orderbook_snapshots WHERE market_source='POLYMARKET'`, f.now.Add(-10*time.Minute))
	for _, test := range []struct{ name, sql, reason string }{
		{"single sided", "best_ask=NULL", "invalid_quote"},
		{"crossed", "best_bid=0.9,best_ask=0.8", "invalid_quote"},
		{"out of range", "best_bid=0.6,best_ask=1.1", "invalid_quote"},
		{"not a number", "best_bid='NaN'::numeric", "invalid_quote"},
		{"infinity", "best_ask='Infinity'::numeric", "invalid_quote"},
		{"error", "status='ERROR'", "invalid_quote"},
		{"empty", "status='EMPTY'", "invalid_quote"},
		{"missing status", "status='MISSING'", "invalid_quote"},
		{"stale", "observed_at='2026-11-04T05:44:59.999999Z'", "stale_quote"},
		{"future", "observed_at='2026-11-04T06:00:00.000001Z'", "future_quote"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f.exec(t, `UPDATE strategy_orderbook_snapshots SET status='OK',best_bid=0.6,best_ask=0.8,observed_at=$1 WHERE decision_at=$2 AND market_source='POLYMARKET'`, f.now.Add(-2*time.Minute), f.now.Add(-5*time.Minute))
			f.exec(t, `UPDATE strategy_orderbook_snapshots SET `+test.sql+` WHERE decision_at=$1 AND market_source='POLYMARKET'`, f.now.Add(-5*time.Minute))
			pm := f.read(t, "pm").Items[0]
			if pm.ValuationState != "incomplete" || pm.PricedAssetCount != 0 || pm.MarketValue != nil || pm.UnrealizedPnL != nil || pm.TotalPnL != nil || pm.ReturnRate != nil || strings.Join(pm.ValuationReasons, ",") != test.reason {
				t.Fatalf("%s %#v", test.name, pm)
			}
			performanceAmount(t, pm.RealizedPnL, "102")
		})
	}
	f.exec(t, `UPDATE strategy_orderbook_snapshots SET status='OK',best_bid=0.6,best_ask=0.8,observed_at=$1 WHERE decision_at=$2 AND market_source='POLYMARKET'`, f.now.Add(-15*time.Minute), f.now.Add(-5*time.Minute))
	if got := f.read(t, "pm").Items[0]; got.ValuationState != "complete" {
		t.Fatalf("exact freshness boundary %#v", got)
	}
	f.exec(t, `INSERT INTO position_lots(lot_id,execution_account_id,market_id,condition_id,token_id,model_id,strategy_id,outcome_index,outcome_name,status,remaining_shares,remaining_cost,opened_at) VALUES('missing-quote-lot','pm','market-other','condition-other','token-other','retired-model','retired-strategy',0,'Yes','SETTLED_PENDING_REDEEM',2,0.7,$1)`, f.now.Add(-600*24*time.Hour))
	got := f.read(t, "pm").Items[0]
	if got.AssetCount != 2 || got.PricedAssetCount != 1 || got.MarketValue != nil || got.UnrealizedPnL != nil || got.TotalPnL != nil || got.ReturnRate != nil || strings.Join(got.ValuationReasons, ",") != "missing_quote" {
		t.Fatalf("partial value masqueraded as total %#v", got)
	}
	performanceAmount(t, got.CurrentCost, "2")
	// A current asset with contradictory venue evidence is not silently hidden.
	f.exec(t, `UPDATE execution_orders SET venue='KALSHI' WHERE execution_account_id='pm'`)
	if _, err := f.reader.AccountPerformance(context.Background(), "pm"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("portfolio identity conflict accepted %v", err)
	}
}
func TestAccountPerformanceHistoryBudgetAndCancellation(t *testing.T) {
	f := newPerformanceFixture(t)
	f.batch(t)
	f.exec(t, `INSERT INTO position_lots(lot_id,execution_account_id,remaining_shares,remaining_cost,status) SELECT 'closed-'||n,'pm',0,0,'CLOSED' FROM generate_series(1,100000) n`)
	f.exec(t, `INSERT INTO position_events(position_event_id,event_type,execution_account_id,market_id,token_id,cost_basis_delta,realized_pnl_delta,occurred_at) SELECT 'history-'||n,'BOUGHT','pm','old-market','old-token',0.000001,0,$1 FROM generate_series(1,100000) n`, f.now.Add(-1000*24*time.Hour))
	started := time.Now()
	got := f.read(t, "pm")
	elapsed := time.Since(started)
	if len(got.Items) != 1 || got.Items[0].AssetCount != 1 || elapsed >= 5*time.Second {
		t.Fatalf("history budget %#v elapsed=%s", got, elapsed)
	}
	t.Logf("100000 historical cost events + 100000 closed lots: %s", elapsed)
	connection, err := f.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	waiting, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, err = f.reader.AccountPerformance(waiting, "pm")
	cancel()
	if closeErr := connection.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pool deadline %v", err)
	}
	if r := f.read(t, "pm"); len(r.Items) != 1 || f.db.Stats().InUse != 0 {
		t.Fatalf("cancel held connection %#v stats=%#v", r, f.db.Stats())
	}
	// Cancel an executing financial query as well, without leaving a pooled
	// transaction open. This uses only a local shadow view, never production.
	f.exec(t, `ALTER TABLE position_events RENAME TO performance_slow_events`)
	f.exec(t, `CREATE TEMP VIEW position_events AS SELECT e.* FROM performance_slow_events e CROSS JOIN LATERAL (SELECT pg_sleep(1) WHERE e.position_event_id<>'') delay`)
	running, stop := context.WithTimeout(context.Background(), 50*time.Millisecond)
	started = time.Now()
	_, err = f.reader.AccountPerformance(running, "pm")
	stop()
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
		t.Fatalf("executing cancellation %v elapsed=%s", err, time.Since(started))
	}
	ctx, end := context.WithTimeout(context.Background(), time.Second)
	defer end()
	var n int
	if err = f.db.QueryRowContext(ctx, `SELECT 1`).Scan(&n); err != nil || n != 1 || f.db.Stats().InUse != 0 {
		t.Fatalf("cancelled query retained transaction: %v stats=%#v", err, f.db.Stats())
	}
}
