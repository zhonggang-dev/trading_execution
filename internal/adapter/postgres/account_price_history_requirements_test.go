package postgres

import (
	"context"
	"testing"
	"time"

	h "github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
)

func containsRequired(w h.Window, at time.Time) bool {
	for _, value := range w.RequiredAt {
		if value.Equal(at) {
			return true
		}
	}
	return false
}

func TestPostgresIntegrationHistoryRequirementsUseWholePortfolioEventClock(t *testing.T) {
	f := newKalshiNettingFixture(t, "unprefixed-history-events", "1000")
	a := f.buy("A", "YES", "3", "0.4", "0.01")
	b := f.buy("B", "NO", "2", "0.3", "0.02")
	add := f.buy("A", "YES", "1", "0.4", "0.01")
	partial := f.sell("A", "YES", "lot:"+a.Fill.Key, "1", "0.5", "0.01")
	now := f.clock.Add(10 * time.Minute)
	before := scalar(t, f.database, `SELECT COUNT(*)::text FROM position_events`)
	store := NewAccountPriceHistoryStore(f.database)
	windows, err := store.Targets(context.Background(), now)
	if err != nil || len(windows) != 2 {
		t.Fatal(windows, err)
	}
	for _, window := range windows {
		if window.Asset.MarketSource != "KALSHI" || window.RequiredAt == nil {
			t.Fatal("explicit source or real sampling set missing", window)
		}
		for _, required := range window.RequiredAt {
			if !required.Before(now) {
				t.Fatal("current quote endpoint was frozen as historical price", required)
			}
		}
		if window.Asset.MarketID == "A" {
			for _, at := range []time.Time{b.Fill.MatchedAt, add.Fill.MatchedAt, partial.Fill.MatchedAt} {
				if !containsRequired(window, at) {
					t.Fatal("other asset or own internal cash event omitted", window, at)
				}
			}
		}
		if window.Asset.MarketID == "B" && !containsRequired(window, partial.Fill.MatchedAt) {
			t.Fatal("portfolio liquidation event omitted for peer holding", window)
		}
	}
	if got := scalar(t, f.database, `SELECT COUNT(*)::text FROM position_events`); got != before {
		t.Fatal("requirements discovery mutated the ledger")
	}
}

func TestPostgresIntegrationHistoryRequirementsDoNotQuoteAtomicFlatLegs(t *testing.T) {
	f := newKalshiNettingFixture(t, "kalshi:requirements-netting", "100")
	f.buy("PAIR", "YES", "1", "0.4", "0.01")
	f.buy("PAIR", "NO", "1", "0.5", "0.01")
	store := NewAccountPriceHistoryStore(f.database)
	windows, err := store.Targets(context.Background(), f.clock.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(windows) != 1 || windows[0].Asset.OutcomeName == nil || *windows[0].Asset.OutcomeName != "Yes" {
		t.Fatal("immediately paired NO leg became a historical holding", windows)
	}
}
