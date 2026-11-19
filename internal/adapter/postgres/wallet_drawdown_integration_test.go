package postgres

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/walletdrawdown"
)

func drawdownStoreDatabase(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL required")
	}
	db := newIntegrationDatabase(t, dsn)
	var schema string
	if err := db.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "0048_wallet_drawdown_samples.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(strings.ReplaceAll(string(raw), "search_path=public,pg_temp", "search_path="+schema+",pg_temp")); err != nil {
		t.Fatal(err)
	}
	return db
}

func drawdownSample(slot time.Time, equity string, drawdown *string, exceeded bool) walletdrawdown.Sample {
	s := walletdrawdown.Sample{
		AccountID: "main", SlotAt: slot, SampledAt: slot.Add(20 * time.Second), CashPUSD: "10.000000", CashBlock: 9,
		PositionsValue: "0.000000", Equity: equity, WindowMinutes: 60, Threshold: "50.00", Exceeded: exceeded,
		Positions: []walletdrawdown.PositionValue{{TokenID: "t1", Title: "A", Outcome: "No", Shares: "1", Price: "0.5", Value: "0.500000"}},
	}
	if drawdown != nil {
		peak, peakAt := "200.000000", slot.Add(-10*time.Minute)
		s.Drawdown, s.PeakEquity, s.PeakAt = drawdown, &peak, &peakAt
	}
	return s
}

func TestPostgresIntegrationWalletDrawdownStoreRoundTripAndEpisode(t *testing.T) {
	db := drawdownStoreDatabase(t)
	store, err := NewWalletDrawdownStore(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	base := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	d60, d10, d65 := "60.000000", "10.000000", "65.000000"
	rows := []walletdrawdown.Sample{
		drawdownSample(base, "200.000000", nil, false),
		drawdownSample(base.Add(10*time.Minute), "140.000000", &d60, true),
		drawdownSample(base.Add(20*time.Minute), "190.000000", &d10, false),
		drawdownSample(base.Add(30*time.Minute), "135.000000", &d65, true),
	}
	for _, row := range rows {
		inserted, err := store.InsertSample(ctx, row)
		if err != nil || !inserted {
			t.Fatalf("insert %v: inserted=%v err=%v", row.SlotAt, inserted, err)
		}
	}
	if inserted, err := store.InsertSample(ctx, rows[0]); err != nil || inserted {
		t.Fatalf("duplicate slot inserted=%v err=%v", inserted, err)
	}
	if err := store.MarkNotified(ctx, "main", rows[1].SlotAt, true, ""); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := store.LoadSample(ctx, "main", rows[1].SlotAt)
	if err != nil || !found || !loaded.Notified || *loaded.Drawdown != "60.000000" || loaded.PeakAt == nil || len(loaded.Positions) != 1 || loaded.Positions[0].Title != "A" {
		t.Fatalf("loaded=%#v found=%v err=%v", loaded, found, err)
	}
	// Episode of rows[1] ended at rows[2]; rows[3] starts a new one.
	if last, err := store.LastNotifiedDrawdown(ctx, "main", rows[2].SlotAt); err != nil || last == nil || last.FloatString(0) != "60" {
		t.Fatalf("last before rows[2] = %v err=%v", last, err)
	}
	if last, err := store.LastNotifiedDrawdown(ctx, "main", rows[3].SlotAt.Add(10*time.Minute)); err != nil || last != nil {
		t.Fatalf("last in new episode = %v err=%v", last, err)
	}
	window, err := store.WindowSamples(ctx, "main", base.Add(5*time.Minute), rows[3].SlotAt)
	if err != nil || len(window) != 2 || !window[0].SlotAt.Equal(rows[1].SlotAt) {
		t.Fatalf("window=%d err=%v", len(window), err)
	}
	if err := store.MarkNotified(ctx, "main", base.Add(time.Hour), true, ""); err == nil {
		t.Fatal("marking a missing slot must fail")
	}
}
