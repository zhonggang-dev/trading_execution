package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	h "github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
)

func historyStoreDatabase(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL required")
	}
	db := newIntegrationDatabase(t, dsn)
	applyHistoryMigration(t, db)
	return db
}
func applyHistoryMigration(t *testing.T, db *sql.DB) {
	t.Helper()
	var schema string
	if e := db.QueryRow(`SELECT current_schema()`).Scan(&schema); e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "0044_account_price_history.sql"))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = db.Exec(strings.ReplaceAll(string(raw), "search_path=public,pg_temp", "search_path="+schema+",pg_temp")); e != nil {
		t.Fatal(e)
	}
}
func historyTestAsset() h.Asset {
	i := 0
	n := "Yes"
	return h.Asset{MarketSource: "POLYMARKET", MarketID: "1", ConditionID: "condition", TokenID: "token", OutcomeIndex: &i, OutcomeName: &n}
}
func TestPostgresIntegrationHistoryAppendOnlyRevisionAndBoundedProjection(t *testing.T) {
	db := historyStoreDatabase(t)
	store := NewAccountPriceHistoryStore(db)
	ctx := context.Background()
	at := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	a := historyTestAsset()
	w := h.Window{Asset: a, From: at, To: at.Add(time.Hour)}
	r := h.Result{Window: w, Series: h.Series{Points: []h.Point{{At: at, Price: "0.2", ResolutionSeconds: 3600, Source: "PM_HISTORY", FetchedAt: at}}, Coverage: []h.Coverage{{From: w.From, To: w.To, State: "complete", FetchedAt: at}}}}
	for range 2 {
		if e := store.Save(ctx, r); e != nil {
			t.Fatal(e)
		}
	}
	if n := scalar(t, db, `SELECT COUNT(*)::text FROM account_price_history_windows`); n != "1" {
		t.Fatal("duplicate changed evidence count", n)
	}
	r.Series.Points[0].Price = "0.4"
	r.Series.Points[0].FetchedAt = at.Add(time.Minute)
	r.Series.Coverage[0].FetchedAt = at.Add(time.Minute)
	if e := store.Save(ctx, r); e != nil {
		t.Fatal(e)
	}
	data, e := store.Load(ctx, []h.Window{w})
	if e != nil {
		t.Fatal(e)
	}
	p, ok := h.PriceAt(data.Series[a.Key()], at)
	if !ok || p.Price != "0.4" || data.Version == "0" {
		t.Fatal(data, p, ok)
	}
	if n := scalar(t, db, `SELECT COUNT(*)::text FROM account_price_history_windows`); n != "2" {
		t.Fatal("revision not retained", n)
	}
	if n := scalar(t, db, `SELECT COUNT(*)::text FROM account_price_history_points`); n != "1" {
		t.Fatal("read projection not unique", n)
	}
	if _, e = db.Exec(`UPDATE account_price_history_windows SET recorded_at=recorded_at`); e == nil {
		t.Fatal("audit mutation allowed")
	}
	if _, e = db.Exec(`DELETE FROM account_price_history_windows`); e == nil {
		t.Fatal("audit delete allowed")
	}
	// Simulate years of immutable runs. Effective inputs stay constant, so the
	// API must not reject or deserialize 25,001 historical audit windows.
	rawAsset, _ := json.Marshal(a)
	rawSeries, _ := json.Marshal(r.Series)
	if _, e = db.Exec(`INSERT INTO account_price_history_windows(asset_key,asset,window_from,window_to,evidence_sha256,series) SELECT $1,$2::jsonb,$3,$4,md5(i::text)||md5('audit'||i::text),$5::jsonb FROM generate_series(1,25001)i`, a.Key(), string(rawAsset), w.From, w.To, string(rawSeries)); e != nil {
		t.Fatal(e)
	}
	started := time.Now()
	data, e = store.Load(ctx, []h.Window{w})
	if e != nil {
		t.Fatal(e)
	}
	if len(data.Series[a.Key()].Points) != 1 {
		t.Fatal("raw audits leaked into input budget", len(data.Series[a.Key()].Points))
	}
	t.Log("load with 25003 audit windows", time.Since(started))
}
func TestPostgresIntegrationHistorySettlementCrossWindowCoverageMergeAndLock(t *testing.T) {
	db := historyStoreDatabase(t)
	store := NewAccountPriceHistoryStore(db)
	ctx := context.Background()
	at := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	a := historyTestAsset()
	for i := range 24 {
		from := at.Add(time.Duration(i) * time.Hour)
		r := h.Result{Window: h.Window{Asset: a, From: from, To: from.Add(time.Hour)}, Series: h.Series{Coverage: []h.Coverage{{From: from, To: from.Add(time.Hour), State: "complete", FetchedAt: from}}}}
		if i == 0 {
			r.Series.Settlements = []h.Settlement{{EffectiveAt: at, Price: "0", Source: "OFFICIAL_SETTLEMENT", SourceReference: "tx:1", FetchedAt: at}}
		}
		if e := store.Save(ctx, r); e != nil {
			t.Fatal(e)
		}
	}
	if n := scalar(t, db, `SELECT COUNT(*)::text FROM account_price_history_coverage WHERE state='complete'`); n != "1" {
		t.Fatal("coverage fragments grew per run", n)
	}
	later := h.Window{Asset: a, From: at.Add(48 * time.Hour), To: at.Add(49 * time.Hour)}
	data, e := store.Load(ctx, []h.Window{later})
	if e != nil {
		t.Fatal(e)
	}
	if p, ok := h.PriceAt(data.Series[a.Key()], later.From); !ok || p.Price != "0" {
		t.Fatal("older finalization lost across request windows", data, p, ok)
	}
	unlock, ok, e := store.Lock(ctx)
	if e != nil || !ok {
		t.Fatal(ok, e)
	}
	otherUnlock, otherOK, e := store.Lock(ctx)
	if e != nil || otherOK {
		t.Fatal("overlapping sync permitted", otherOK, e)
	}
	otherUnlock()
	unlock()
	unlock2, ok, e := store.Lock(ctx)
	if e != nil || !ok {
		t.Fatal("lock not released", ok, e)
	}
	unlock2()
}
func TestPostgresIntegrationHistoryTargetsReadActualLifecycleAndDoNotMutateLedger(t *testing.T) {
	f := newKalshiNettingFixture(t, "kalshi:history-target", "100")
	f.buy("HISTORY", "YES", "3", "0.4", "0.03")
	buy := f.buy("CLOSED", "NO", "2", "0.3", "0.02")
	f.sell("CLOSED", "NO", "lot:"+buy.Fill.Key, "2", "0.5", "0.01")
	before := scalar(t, f.database, `SELECT COUNT(*)::text FROM position_events`)
	store := NewAccountPriceHistoryStore(f.database)
	targets, e := store.Targets(context.Background(), f.clock.Add(time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	if len(targets) != 2 {
		t.Fatal(targets)
	}
	for _, target := range targets {
		if target.Asset.MarketSource != "KALSHI" {
			t.Fatal(target)
		}
		if target.Asset.MarketID == "CLOSED" && target.To.Equal(f.clock.Add(time.Hour)) {
			t.Fatal("closed asset priced after exit", target)
		}
	}
	if after := scalar(t, f.database, `SELECT COUNT(*)::text FROM position_events`); after != before {
		t.Fatal("target discovery changed ledger")
	}
}

func TestPostgresIntegrationHistoryVersionChangesWhenOldEvidenceReturns(t *testing.T) {
	db := historyStoreDatabase(t)
	store := NewAccountPriceHistoryStore(db)
	ctx := context.Background()
	at := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	a := historyTestAsset()
	b := a
	b.TokenID = "other-token"
	step := 0
	save := func(asset h.Asset, price string) {
		step++
		fetched := at.Add(time.Duration(step) * time.Minute)
		r := h.Result{Window: h.Window{Asset: asset, From: at, To: at.Add(time.Hour)}, Series: h.Series{Points: []h.Point{{At: at, Price: price, Source: "PM_HISTORY", ResolutionSeconds: 3600, FetchedAt: fetched}}, Coverage: []h.Coverage{{From: at, To: at.Add(time.Hour), State: "complete", FetchedAt: fetched}}}}
		if e := store.Save(ctx, r); e != nil {
			t.Fatal(e)
		}
	}
	save(a, "0.2")
	save(a, "0.4")
	save(b, "0.3")
	windows := []h.Window{{Asset: a, From: at, To: at.Add(time.Hour)}, {Asset: b, From: at, To: at.Add(time.Hour)}}
	before, e := store.Load(ctx, windows)
	if e != nil {
		t.Fatal(e)
	}
	// Returns to canonical first evidence: no new raw history_id, but the
	// effective price changes while the other asset still has the highest ID.
	save(a, "0.2")
	after, e := store.Load(ctx, windows)
	if e != nil {
		t.Fatal(e)
	}
	if before.Version == after.Version {
		t.Fatal("semantic revision reused stale cache version")
	}
	if p, ok := h.PriceAt(after.Series[a.Key()], at); !ok || p.Price != "0.2" {
		t.Fatal(p, ok)
	}
	if n := scalar(t, db, `SELECT COUNT(*)::text FROM account_price_history_windows`); n != "3" {
		t.Fatal("canonical audit fact not idempotent", n)
	}
}

func TestPostgresIntegrationHistoryEffectiveInputBudgetAtRealScale(t *testing.T) {
	db := historyStoreDatabase(t)
	store := NewAccountPriceHistoryStore(db)
	a := historyTestAsset()
	at := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	asset, _ := json.Marshal(a)
	var id int64
	if e := db.QueryRow(`INSERT INTO account_price_history_windows(asset_key,asset,window_from,window_to,evidence_sha256,series) VALUES($1,$2::jsonb,$3,$4,repeat('a',64),'{}') RETURNING history_id`, a.Key(), string(asset), at, at.Add(112871*time.Minute)).Scan(&id); e != nil {
		t.Fatal(e)
	}
	if _, e := db.Exec(`INSERT INTO account_price_history_points(asset_key,point_at,fetched_at,history_id,point)
 SELECT $1,$2::timestamptz+i*interval '1 minute',$2,$3,jsonb_build_object('at',$2::timestamptz+i*interval '1 minute','price','0.4','source_at',NULL,'fetched_at',$2::timestamptz,'resolution_seconds',3600,'source','PM_HISTORY','valid_until',NULL) FROM generate_series(0,112870)i`, a.Key(), at, id); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	started := time.Now()
	data, e := store.Load(ctx, []h.Window{{Asset: a, From: at, To: at.Add(112871 * time.Minute)}})
	if e != nil {
		t.Fatal(e)
	}
	if len(data.Series[a.Key()].Points) != 112871 {
		t.Fatal("effective inputs lost", len(data.Series[a.Key()].Points))
	}
	t.Log("112871 effective inputs", time.Since(started))
}
