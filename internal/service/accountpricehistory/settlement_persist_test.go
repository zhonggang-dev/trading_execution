package accountpricehistory

import (
	"context"
	"testing"
	"time"
)

func TestPersistLocalSettlementsEvenWithoutFetchUsesOriginalTime(t *testing.T) {
	at := time.Date(2026, 9, 29, 12, 23, 35, 362096000, time.UTC)
	now := at.Add(30 * 24 * time.Hour)
	a := testAsset()
	known := Settlement{EffectiveAt: at, Price: "0", Source: "OFFICIAL_SETTLEMENT", SourceReference: "LOCAL_POSITION:original", FetchedAt: at}
	store := &memoryStore{}
	data := Dataset{ObservedAt: now, Series: map[string]Series{a.Key(): {Settlements: []Settlement{known}, Coverage: []Coverage{{From: at.Add(-time.Hour), To: at.Add(time.Hour), State: "complete"}}}}}
	targets := []Window{{Asset: a, From: at.Add(-time.Hour), To: at.Add(time.Hour)}}
	if len(Plan(targets, data, now)) != 0 {
		t.Fatal("fixture must require no network windows")
	}
	if err := PersistKnownSettlements(context.Background(), store, targets, data); err != nil {
		t.Fatal(err)
	}
	if len(store.saves) != 1 {
		t.Fatal(store.saves)
	}
	r := store.saves[0]
	if !r.Window.From.Equal(at) || !r.Window.To.Equal(at) || r.Window.Asset.Key() != a.Key() || !r.Series.Settlements[0].FetchedAt.Equal(at) || len(r.Series.Points) != 0 {
		t.Fatal("persistence invented a price or read-time event", r)
	}
}
