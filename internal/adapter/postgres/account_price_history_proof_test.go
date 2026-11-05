package postgres

import (
	"context"
	"testing"
	"time"

	h "github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
)

func TestPostgresIntegrationHistoryAnchorProofUnionAndNoResurrection(t *testing.T) {
	db := historyStoreDatabase(t)
	store := NewAccountPriceHistoryStore(db)
	ctx := context.Background()
	index := 1
	name := "No"
	a := h.Asset{MarketSource: "KALSHI", MarketID: "KXUSPSELECTION-26", ConditionID: "kalshi:KXUSPSELECTION-26", TokenID: "kalshi:KXUSPSELECTION-26:NO", OutcomeIndex: &index, OutcomeName: &name}
	at := time.Date(2026, 10, 30, 15, 0, 0, 0, time.UTC)
	target := time.Date(2026, 10, 30, 16, 50, 19, 25092000, time.UTC)
	fetched := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	save := func(price, source string, until *time.Time, observed time.Time) {
		t.Helper()
		windowEnd := at.Add(time.Hour)
		if until != nil {
			windowEnd = *until
		}
		point := h.Point{At: at, Price: price, Source: source, FetchedAt: observed, ResolutionSeconds: 3600, ValidUntil: until}
		if e := store.Save(ctx, h.Result{Window: h.Window{Asset: a, From: windowEnd, To: windowEnd}, Series: h.Series{Points: []h.Point{point}}}); e != nil {
			t.Fatal(e)
		}
	}
	load := func() h.Series {
		t.Helper()
		d, e := store.Load(ctx, []h.Window{{Asset: a, From: at, To: target.Add(time.Hour)}})
		if e != nil {
			t.Fatal(e)
		}
		return d.Series[a.Key()]
	}
	save("0.9", "KALSHI_PREVIOUS_TRADE", &target, fetched)
	narrow := at.Add(time.Hour)
	save("0.9000", "KALSHI_PREVIOUS_TRADE", &narrow, fetched.Add(time.Minute))
	if p, ok := h.PriceAt(load(), target); !ok || p.ValidUntil == nil || !p.ValidUntil.Equal(target) {
		t.Fatal("new narrower observation erased verified proof", p, ok)
	}
	later := target.Add(time.Second)
	save("0.9", "KALSHI_PREVIOUS_TRADE", &later, fetched.Add(-time.Minute))
	if p, ok := h.PriceAt(load(), later); !ok || p.ValidUntil == nil || !p.ValidUntil.Equal(later) {
		t.Fatal("older equivalent proof did not union", p, ok)
	}
	save("0.5", "KALSHI_PREVIOUS_TRADE", nil, fetched.Add(2*time.Minute))
	if _, ok := h.PriceAt(load(), target); ok {
		t.Fatal("different price resurrected old proof")
	}
	save("0.9", "KALSHI_PREVIOUS_TRADE", &target, fetched.Add(3*time.Minute))
	save("0.9", "KALSHI_BOOK_MID", nil, fetched.Add(4*time.Minute))
	if _, ok := h.PriceAt(load(), target); ok {
		t.Fatal("different source inherited old no-trade proof")
	}
}

func TestPostgresIntegrationHistoryPersistsLocalTerminalWithNoPriceJobs(t *testing.T) {
	db := historyStoreDatabase(t)
	store := NewAccountPriceHistoryStore(db)
	ctx := context.Background()
	a := historyTestAsset()
	at := time.Date(2026, 9, 29, 12, 23, 35, 362096000, time.UTC)
	seedLocalHistorySettlement(t, db, "known-no-fetch", a, at, "1")
	w := h.Window{Asset: a, From: at.Add(-time.Hour), To: at.Add(time.Hour)}
	if e := store.Save(ctx, h.Result{Window: w, Series: h.Series{Points: []h.Point{{At: w.From, Price: "0.4", Source: "PM_HISTORY", FetchedAt: at, ResolutionSeconds: 3600}}, Coverage: []h.Coverage{{From: w.From, To: w.To, State: "complete", FetchedAt: at}}}}); e != nil {
		t.Fatal(e)
	}
	data, e := store.Load(ctx, []h.Window{w})
	if e != nil {
		t.Fatal(e)
	}
	if len(h.Plan([]h.Window{w}, data, at.Add(30*24*time.Hour))) != 0 {
		t.Fatal("fixture must need no network Fetch")
	}
	if scalar(t, db, `SELECT COUNT(*)::text FROM account_price_history_settlements`) != "0" {
		t.Fatal("fixture already had durable terminal")
	}
	for range 2 {
		if e = h.PersistKnownSettlements(ctx, store, []h.Window{w}, data); e != nil {
			t.Fatal(e)
		}
	}
	if scalar(t, db, `SELECT COUNT(*)::text FROM account_price_history_settlements`) != "1" {
		t.Fatal("known terminal not durably stored")
	}
	if scalar(t, db, `SELECT COUNT(*)::text FROM account_price_history_windows`) != "2" {
		t.Fatal("repeated persistence not idempotent")
	}
	// Read the new projection directly, as the still-running old HTTP reader
	// does, rather than relying on the newly added virtual local facts.
	if scalar(t, db, `SELECT settlement->>'effective_at' FROM account_price_history_settlements`) != "2026-09-29T12:23:35.362096Z" {
		t.Fatal("confirmation time replaced by current import time")
	}
}

func TestPostgresIntegrationHistoryNarrowLoadKeepsExpiredPredecessor(t *testing.T) {
	db := historyStoreDatabase(t)
	store := NewAccountPriceHistoryStore(db)
	ctx := context.Background()
	a := historyTestAsset()
	at := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	until := at.Add(10 * time.Hour)
	r := h.Result{Window: h.Window{Asset: a, From: at, To: until}, Series: h.Series{Points: []h.Point{{At: at, Price: "0.1", Source: "KALSHI_PREVIOUS_TRADE", FetchedAt: at, ResolutionSeconds: 3600, ValidUntil: &until}, {At: at.Add(2 * time.Hour), Price: "0.9", Source: "KALSHI_TRADE_CLOSE", FetchedAt: at, ResolutionSeconds: 3600}}}}
	if e := store.Save(ctx, r); e != nil {
		t.Fatal(e)
	}
	full, e := store.Load(ctx, []h.Window{{Asset: a, From: at, To: until}})
	if e != nil {
		t.Fatal(e)
	}
	if _, ok := h.PriceAt(full.Series[a.Key()], at.Add(9*time.Hour)); ok {
		t.Fatal("full series wrongly revived old proof")
	}
	narrow, e := store.Load(ctx, []h.Window{{Asset: a, From: at.Add(9 * time.Hour), To: at.Add(9 * time.Hour)}})
	if e != nil {
		t.Fatal(e)
	}
	if len(narrow.Series[a.Key()].Points) != 2 {
		t.Fatal("expired latest predecessor was cropped", narrow.Series[a.Key()].Points)
	}
	if p, ok := h.PriceAt(narrow.Series[a.Key()], at.Add(9*time.Hour)); ok {
		t.Fatal("narrow read revived superseded old price", p)
	}
}

func TestPostgresIntegrationHistoryHourlyRefreshMarkerSurvivesWarmMerge(t *testing.T) {
	db := historyStoreDatabase(t)
	store := NewAccountPriceHistoryStore(db)
	ctx := context.Background()
	a := historyTestAsset()
	hour := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	save := func(w h.Window, coverage h.Coverage) {
		t.Helper()
		if e := store.Save(ctx, h.Result{Window: w, Series: h.Series{Coverage: []h.Coverage{coverage}}}); e != nil {
			t.Fatal(e)
		}
	}
	initial := h.Window{Asset: a, From: hour.Add(-2 * time.Hour), To: hour.Add(time.Hour)}
	save(initial, h.Coverage{From: initial.From, To: initial.To, State: "complete", FetchedAt: hour})
	refresh := h.Window{Asset: a, From: hour, To: hour, RefreshAt: &hour}
	marker := h.Coverage{From: hour, To: hour, State: "complete", Reason: "hourly_refresh", FetchedAt: hour.Add(17 * time.Minute)}
	save(refresh, marker)
	save(refresh, marker)
	warm := h.Window{Asset: a, From: hour.Add(-time.Hour), To: hour.Add(2 * time.Hour)}
	save(warm, h.Coverage{From: warm.From, To: warm.To, State: "complete", FetchedAt: hour.Add(20 * time.Minute)})
	if n := scalar(t, db, `SELECT COUNT(*)::text FROM account_price_history_coverage WHERE state='complete'`); n != "2" {
		t.Fatal("marker merged into ordinary coverage", n)
	}
	loaded, e := store.Load(ctx, []h.Window{{Asset: a, From: hour, To: hour}})
	if e != nil {
		t.Fatal(e)
	}
	count := 0
	for _, c := range loaded.Series[a.Key()].Coverage {
		if c.Reason == "hourly_refresh" {
			count++
			if !c.From.Equal(hour) || !c.To.Equal(hour) || !c.FetchedAt.Equal(marker.FetchedAt) {
				t.Fatal("refresh evidence time changed", c)
			}
		}
	}
	if count != 1 {
		t.Fatal("missing or duplicate hourly marker", loaded)
	}
}

func TestPostgresIntegrationHistoryRefreshProjectionStaysBoundedAndMonotonic(t *testing.T) {
	db := historyStoreDatabase(t)
	store := NewAccountPriceHistoryStore(db)
	ctx := context.Background()
	a := historyTestAsset()
	latest := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	save := func(hour, observed time.Time) {
		t.Helper()
		if e := store.Save(ctx, h.Result{Window: h.Window{Asset: a, From: hour, To: hour, RefreshAt: &hour}, Series: h.Series{Coverage: []h.Coverage{{From: hour, To: hour, State: "complete", Reason: "hourly_refresh", FetchedAt: observed}}}}); e != nil {
			t.Fatal(e)
		}
	}
	for i := 23; i >= 0; i-- {
		hour := latest.Add(-time.Duration(i) * time.Hour)
		save(hour, hour.Add(17*time.Minute))
	}
	save(latest.Add(-time.Hour), latest.Add(2*time.Hour))
	save(latest, latest.Add(17*time.Minute))
	if n := scalar(t, db, `SELECT COUNT(*)::text FROM account_price_history_coverage WHERE reason='hourly_refresh'`); n != "1" {
		t.Fatal("refresh projection grew with run count", n)
	}
	var actual time.Time
	if e := db.QueryRow(`SELECT window_from FROM account_price_history_coverage WHERE coverage_key='hourly_refresh'`).Scan(&actual); e != nil || !actual.Equal(latest) {
		t.Fatal("out-of-order old hour replaced latest marker", actual, e)
	}
	// Preserve >20,000 earlier immutable hourly observations, while the
	// checkpoint projection and Load input remain one current marker.
	if _, e := db.Exec(`INSERT INTO account_price_history_windows(asset_key,asset,window_from,window_to,evidence_sha256,series)
 SELECT asset_key,asset,$1::timestamptz-i*interval '1 hour',$1::timestamptz-i*interval '1 hour',md5(i::text)||md5('refresh-audit'||i::text),series
 FROM account_price_history_windows CROSS JOIN generate_series(25,20050)i WHERE history_id=(SELECT MIN(history_id) FROM account_price_history_windows)`, latest); e != nil {
		t.Fatal(e)
	}
	data, e := store.Load(ctx, []h.Window{{Asset: a, From: latest.Add(-21000 * time.Hour), To: latest}})
	if e != nil {
		t.Fatal("raw observation growth exhausted reader", e)
	}
	if len(data.Series[a.Key()].Coverage) != 1 || !data.Series[a.Key()].Coverage[0].From.Equal(latest) {
		t.Fatal("Load read historical checkpoint observations", data.Series[a.Key()].Coverage)
	}
	if n := scalar(t, db, `SELECT COUNT(*)::text FROM account_price_history_windows`); n != "20050" {
		t.Fatal("raw hourly audit evidence was lost", n)
	}
}
