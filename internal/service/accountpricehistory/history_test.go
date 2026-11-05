package accountpricehistory

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func testAsset() Asset {
	return Asset{MarketSource: "POLYMARKET", MarketID: "1", ConditionID: "condition", TokenID: "token"}
}
func TestPriceAtRejectsLookaheadAndAllowsOnlyProvenExtensions(t *testing.T) {
	at := time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)
	until := at.Add(5 * time.Hour)
	s := Series{Points: []Point{{At: at, Price: "0.4", ResolutionSeconds: 3600}, {At: at.Add(2 * time.Hour), Price: "0.9", ResolutionSeconds: 3600}}}
	SortSeries(&s)
	if p, ok := PriceAt(s, at.Add(time.Hour)); !ok || p.Price != "0.4" {
		t.Fatal(p, ok)
	}
	if _, ok := PriceAt(s, at.Add(90*time.Minute)); ok {
		t.Fatal("unproven gap filled")
	}
	s.Points[0].ValidUntil = &until
	if p, ok := PriceAt(s, at.Add(90*time.Minute)); !ok || p.Price != "0.4" {
		t.Fatal("bounded proof ignored", p, ok)
	}
	if p, ok := PriceAt(s, at.Add(2*time.Hour)); !ok || p.Price != "0.9" {
		t.Fatal("later actual observation must supersede proof", p, ok)
	}
	if _, ok := PriceAt(s, at.Add(6*time.Hour)); ok {
		t.Fatal("proof extended indefinitely")
	}
	s.Settlements = []Settlement{{EffectiveAt: at.Add(3 * time.Hour), Price: "0", SourceReference: "confirmed"}}
	if p, ok := PriceAt(s, at.Add(10*time.Hour)); !ok || p.Price != "0" {
		t.Fatal("zero payout is valid", p, ok)
	}
	if p, ok := PriceAt(s, at.Add(2*time.Hour)); !ok || p.Price != "0.9" {
		t.Fatal("future final price leaked", p, ok)
	}
}
func TestValidPriceStrictDecimal(t *testing.T) {
	for _, v := range []string{"1/2", "1e-1", "NaN", "-0.01", "1.001", " 0.5", "", "Inf"} {
		if ValidPrice(v) {
			t.Fatal("accepted", v)
		}
	}
	for _, v := range []string{"0", "1", "0.123456789123456789"} {
		if !ValidPrice(v) {
			t.Fatal("rejected", v)
		}
	}
}
func TestPlanResumesFailedWindowsWithoutDroppingHistory(t *testing.T) {
	from := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	asset := testAsset()
	end := from.Add(40 * 24 * time.Hour)
	target := Window{asset, from, end}
	data := Dataset{Series: map[string]Series{asset.Key(): {Coverage: []Coverage{{From: from, To: from.Add(14 * 24 * time.Hour), State: "complete"}, {From: from.Add(14 * 24 * time.Hour), To: from.Add(28 * 24 * time.Hour), State: "error"}}}}}
	plan := Plan([]Window{target}, data, end.Add(time.Hour))
	if len(plan) != 2 || !plan[0].From.Equal(from.Add(14*24*time.Hour)) || !plan[1].To.Equal(end) {
		t.Fatal(plan)
	}
	data.Series[asset.Key()] = Series{Settlements: []Settlement{{EffectiveAt: from.Add(time.Hour), Price: "0"}}, Coverage: []Coverage{{From: from, To: from.Add(time.Hour), State: "complete"}}}
	if got := Plan([]Window{target}, data, end); len(got) != 0 {
		t.Fatal("terminal asset kept requesting", got)
	}
}
func TestPlanRecentOverlapAndLateRevision(t *testing.T) {
	a := testAsset()
	now := time.Date(2026, 11, 5, 8, 17, 0, 0, time.UTC)
	from := now.Add(-10 * time.Hour)
	data := Dataset{Series: map[string]Series{a.Key(): {Coverage: []Coverage{{From: from, To: now.Truncate(time.Hour), State: "complete"}}}}}
	jobs := Plan([]Window{{a, from, now}}, data, now)
	if len(jobs) != 1 || jobs[0].From != now.Truncate(time.Hour).Add(-2*time.Hour) || jobs[0].To != now.Truncate(time.Hour) {
		t.Fatal(jobs)
	}
}

type memoryStore struct {
	mu      sync.Mutex
	targets []Window
	data    Dataset
	saves   []Result
	busy    bool
}

func (m *memoryStore) Lock(context.Context) (func(), bool, error)           { return func() {}, !m.busy, nil }
func (m *memoryStore) Targets(context.Context, time.Time) ([]Window, error) { return m.targets, nil }
func (m *memoryStore) Load(context.Context, []Window) (Dataset, error)      { return m.data, nil }
func (m *memoryStore) Save(_ context.Context, r Result) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saves = append(m.saves, r)
	return nil
}

type sourceFunc func(context.Context, Window) (Result, error)

func (f sourceFunc) Fetch(c context.Context, w Window) (Result, error) { return f(c, w) }
func TestSyncDryRunAndFailedWindowsNeverBecomeComplete(t *testing.T) {
	now := time.Now().UTC()
	a := testAsset()
	store := &memoryStore{targets: []Window{{a, now.Add(-48 * time.Hour), now.Add(-24 * time.Hour)}}, data: Dataset{Series: map[string]Series{}}}
	source := sourceFunc(func(context.Context, Window) (Result, error) { return Result{}, fmt.Errorf("temporary source failure") })
	r, e := Run(context.Background(), store, source, SyncOptions{Now: now, DryRun: true})
	if e != nil || r.Planned != 1 || len(store.saves) != 0 {
		t.Fatal(r, e)
	}
	r, e = Run(context.Background(), store, source, SyncOptions{Now: now})
	if e == nil || r.Failed != 1 || len(store.saves) != 1 || store.saves[0].Series.Coverage[0].State != "error" {
		t.Fatal(r, e, store.saves)
	}
	store.busy = true
	r, e = Run(context.Background(), store, source, SyncOptions{Now: now})
	if e != nil || !r.Busy {
		t.Fatal(r, e)
	}
}

func TestPriceAtCannotResurrectOlderPriceAfterNewObservationExpires(t *testing.T) {
	at := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	until := at.Add(10 * time.Hour)
	series := Series{Points: []Point{{At: at, Price: "0.1", ResolutionSeconds: 0, ValidUntil: &until}, {At: at.Add(2 * time.Hour), Price: "0.9", ResolutionSeconds: 3600}}}
	SortSeries(&series)
	if _, ok := PriceAt(series, at.Add(4*time.Hour)); ok {
		t.Fatal("resurrected older price across a newer observation")
	}
}
