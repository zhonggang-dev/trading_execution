package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/accountprices"
	h "github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
)

func seedLocalHistorySettlement(t *testing.T, db *sql.DB, account string, a h.Asset, at time.Time, price string) {
	t.Helper()
	insertAccount(t, db, account, "wallet-"+account, "0", "0", "0")
	if _, e := db.Exec(`INSERT INTO execution_positions(execution_account_id,market_id,condition_id,token_id,outcome_index,outcome_name,lifecycle_status,settlement_price,settlement_source,settled_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,'CLOSED',$7::numeric,$8,$9,$9)`, account, a.MarketID, a.ConditionID, a.TokenID, a.OutcomeIndex, a.OutcomeName, price, "POLYMARKET_DATA_API:"+a.ConditionID, at); e != nil {
		t.Fatal(e)
	}
}
func TestPostgresIntegrationHistoryLocalSettlementRealClosedCases(t *testing.T) {
	db := historyStoreDatabase(t)
	store := NewAccountPriceHistoryStore(db)
	parse := func(s string) time.Time {
		v, e := time.Parse(time.RFC3339Nano, s)
		if e != nil {
			t.Fatal(e)
		}
		return v
	}
	cases := []struct {
		market, condition, token, name, price string
		index                                 int
		confirmed, redeemed                   string
	}{
		{"4908363", "0x1d4e142bad636742b7422c1637a085c5f4c253910bf0863e0f414bbe8e568900", "75935870907256494620150531746523553886277518592037994821997520000948292428590", "G2 Esports", "0", 0, "2026-09-29T12:23:35.362096Z", "2026-09-29T13:23:01.311128Z"},
		{"4908363", "0x1d4e142bad636742b7422c1637a085c5f4c253910bf0863e0f414bbe8e568900", "22896490946510458050474707526173184189658564014943270144730376614268675831557", "Paper Rex", "1", 1, "2026-09-29T12:23:35.362096Z", "2026-09-29T13:23:01.311128Z"},
		{"637004", "0xae76ef049f09b109cfb0a9bfa2335d7c9fde5bd262c64548b43308c810c314ec", "28096161697269491739201577018265114286225290564353204301878588204336630210171", "No", "1", 1, "2026-10-09T12:13:31.735869Z", "2026-10-19T04:29:24.160397Z"},
	}
	for i, c := range cases {
		a := h.Asset{MarketSource: "POLYMARKET", MarketID: c.market, ConditionID: c.condition, TokenID: c.token, OutcomeIndex: &c.index, OutcomeName: &c.name}
		at, to := parse(c.confirmed), parse(c.redeemed)
		seedLocalHistorySettlement(t, db, "local-real-"+c.name, a, at, c.price)
		window := h.Window{Asset: a, From: at.Add(-2 * time.Hour), To: to}
		before, e := store.Load(context.Background(), []h.Window{window})
		if e != nil {
			t.Fatal(e)
		}
		series := before.Series[a.Key()]
		if _, ok := h.PriceAt(series, at.Add(-time.Nanosecond)); ok {
			t.Fatal("future local result used before actual confirmation", i)
		}
		if p, ok := h.PriceAt(series, to); !ok || p.Price != c.price || !p.At.Equal(at) {
			t.Fatal("closed local evidence lost", p, ok)
		}
		if len(series.Settlements) != 1 || !series.Settlements[0].FetchedAt.Equal(at) {
			t.Fatal("read time replaced original observation", series)
		}
		second, e := store.Load(context.Background(), []h.Window{window})
		if e != nil || second.Version != before.Version {
			t.Fatal("unchanged read invalidated cache", before.Version, second.Version, e)
		}
		// Persistence of the already-known result remains idempotent and cannot
		// conflict with the same local evidence on the next Load.
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/v2/prices-history" || r.URL.Query().Get("as_of") != "" {
				t.Error("trusted local evidence should avoid missing-tx endpoint", r.URL.Path)
				http.Error(w, "unexpected endpoint", 400)
				return
			}
			points := []map[string]any{}
			for pointAt := window.From.Truncate(time.Hour); pointAt.Before(at); pointAt = pointAt.Add(time.Hour) {
				points = append(points, map[string]any{"timestamp": pointAt.Unix(), "price": 0.4, "resolution_seconds": 3600})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": points, "pagination": map[string]any{"has_more": false}})
		}))
		t.Cleanup(server.Close)
		source, makeErr := accountprices.New(accountprices.Params{Client: server.Client(), PMBaseURL: server.URL, GammaBaseURL: server.URL, KalshiBaseURL: server.URL, RequestInterval: time.Nanosecond})
		if makeErr != nil {
			t.Fatal(makeErr)
		}
		result, fetchErr := source.FetchWithSettlements(context.Background(), window, series.Settlements)
		if fetchErr != nil {
			t.Fatal(fetchErr)
		}
		if e = store.Save(context.Background(), result); e != nil {
			t.Fatal(e)
		}
		if e = store.Save(context.Background(), result); e != nil {
			t.Fatal(e)
		}
		stored, e := store.Load(context.Background(), []h.Window{window})
		if e != nil || len(stored.Series[a.Key()].Settlements) != 1 {
			t.Fatal("stored+local evidence duplicated", stored, e)
		}
		if p, ok := h.PriceAt(stored.Series[a.Key()], at.Add(-time.Nanosecond)); !ok || p.Price != "0.4" {
			t.Fatal("real pre-settlement observation was lost during Save", p, ok)
		}
		if p, ok := h.PriceAt(stored.Series[a.Key()], to); !ok || p.Price != c.price {
			t.Fatal("persisted local terminal tail missing", p, ok)
		}
	}
}
func TestPostgresIntegrationHistoryRejectsUntrustedLocalEvidence(t *testing.T) {
	db := historyStoreDatabase(t)
	store := NewAccountPriceHistoryStore(db)
	a := historyTestAsset()
	at := time.Date(2026, 9, 29, 12, 23, 35, 362096000, time.UTC)
	seedLocalHistorySettlement(t, db, "local-invalid", a, at, "1")
	// Explicitly model a corrupt legacy row in this isolated schema. Production
	// constraints are never changed by the loader or migration.
	if _, e := db.Exec(`ALTER TABLE execution_positions DROP CONSTRAINT execution_positions_settlement_price_check`); e != nil {
		t.Fatal(e)
	}
	tests := []struct {
		name, sql string
		args      []any
		requested h.Asset
	}{
		{"market", `UPDATE execution_positions SET market_id='other'`, nil, a},
		{"condition", `UPDATE execution_positions SET condition_id='other',settlement_source='POLYMARKET_DATA_API:other'`, nil, a},
		{"token", `UPDATE execution_positions SET token_id='other-token'`, nil, a},
		{"index", `UPDATE execution_positions SET outcome_index=1`, nil, a},
		{"outcome", `UPDATE execution_positions SET outcome_name='other'`, nil, a},
		{"source", `UPDATE execution_positions SET settlement_source='UNTRUSTED:condition'`, nil, a},
		{"nonbinary", `UPDATE execution_positions SET settlement_price=0.5`, nil, a},
		{"future", `UPDATE execution_positions SET settled_at=$1,updated_at=$1`, []any{time.Now().UTC().Add(time.Hour)}, a},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, e := db.Exec(`UPDATE execution_positions SET market_id=$1,condition_id=$2,token_id=$3,outcome_index=$4,outcome_name=$5,settlement_price=1,settlement_source=$6,settled_at=$7,updated_at=$7`, a.MarketID, a.ConditionID, a.TokenID, a.OutcomeIndex, a.OutcomeName, "POLYMARKET_DATA_API:"+a.ConditionID, at); e != nil {
				t.Fatal(e)
			}
			if _, e := db.Exec(tc.sql, tc.args...); e != nil {
				t.Fatal(e)
			}
			data, e := store.Load(context.Background(), []h.Window{{Asset: tc.requested, From: at.Add(-time.Hour), To: time.Now().UTC().Add(2 * time.Hour)}})
			if e != nil {
				t.Fatal(e)
			}
			if len(data.Series[a.Key()].Settlements) != 0 {
				t.Fatal("untrusted or wrong-identity settlement reused", tc.name)
			}
		})
	}
}
func TestPostgresIntegrationHistoryLocalConflictAndVersionChange(t *testing.T) {
	db := historyStoreDatabase(t)
	store := NewAccountPriceHistoryStore(db)
	a := historyTestAsset()
	at := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	seedLocalHistorySettlement(t, db, "local-first", a, at, "1")
	window := h.Window{Asset: a, From: at.Add(-time.Hour), To: at.Add(5 * time.Hour)}
	first, e := store.Load(context.Background(), []h.Window{window})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = db.Exec(`UPDATE execution_positions SET settled_at=$1,updated_at=$1`, at.Add(time.Minute)); e != nil {
		t.Fatal(e)
	}
	second, e := store.Load(context.Background(), []h.Window{window})
	if e != nil || first.Version == second.Version {
		t.Fatal("changed local evidence reused cache", e)
	}
	seedLocalHistorySettlement(t, db, "local-conflict", a, at, "0")
	if _, e = store.Load(context.Background(), []h.Window{window}); e == nil {
		t.Fatal("contradictory local payouts accepted")
	}
}
