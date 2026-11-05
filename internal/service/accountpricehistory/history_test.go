package accountpricehistory

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
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

type storageStateError struct{ code string }

func (e storageStateError) Error() string {
	return "password=must-not-leak SELECT private_connection_data"
}
func (e storageStateError) SQLState() string { return e.code }

type failingStorage struct {
	*memoryStore
	err error
}

func (s failingStorage) Save(context.Context, Result) error { return s.err }
func TestSyncStorageFailureReportsOnlyValidatedSQLState(t *testing.T) {
	for _, code := range []string{"42501", "invalid state", "abcde"} {
		t.Run(code, func(t *testing.T) {
			now := time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)
			a := testAsset()
			w := Window{Asset: a, From: now.Add(-48 * time.Hour), To: now.Add(-24 * time.Hour)}
			store := failingStorage{memoryStore: &memoryStore{targets: []Window{w}, data: Dataset{Series: map[string]Series{}}}, err: fmt.Errorf("sensitive wrapper: %w", storageStateError{code})}
			source := sourceFunc(func(_ context.Context, w Window) (Result, error) {
				return Result{Window: w, Series: Series{Coverage: []Coverage{{From: w.From, To: w.To, State: "complete"}}}}, nil
			})
			report, err := Run(context.Background(), store, source, SyncOptions{Now: now, Workers: 1})
			if err == nil || report.Failed != 1 || len(report.Failures) != 1 {
				t.Fatal(report, err)
			}
			want := ""
			if code == "42501" {
				want = code
			}
			if report.Failures[0].Detail != want {
				t.Fatal("unexpected storage diagnostic", report.Failures[0].Detail)
			}
			raw, _ := json.Marshal(report)
			for _, secret := range []string{"password", "SELECT", "private_connection_data", "sensitive wrapper"} {
				if strings.Contains(string(raw), secret) {
					t.Fatal("storage error text leaked", secret)
				}
			}
		})
	}
}

type awareTestSource struct {
	legacyCalls int
	known       []Settlement
}

func (s *awareTestSource) Fetch(context.Context, Window) (Result, error) {
	s.legacyCalls++
	return Result{}, fmt.Errorf("legacy Fetch must not be used when settlements are supported")
}
func (s *awareTestSource) FetchWithSettlements(_ context.Context, w Window, known []Settlement) (Result, error) {
	s.known = append([]Settlement(nil), known...)
	return Result{Window: w, Series: Series{Points: []Point{{At: w.From, Price: "0.4", ResolutionSeconds: 3600}}, Settlements: known, Coverage: []Coverage{{From: w.From, To: w.To, State: "complete"}}}}, nil
}
func TestSyncPassesVerifiedKnownSettlementsWithoutChangingAssetIdentity(t *testing.T) {
	now := time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)
	a := testAsset()
	from := now.Add(-48 * time.Hour)
	known := Settlement{EffectiveAt: from.Add(time.Hour), Price: "0", Source: "OFFICIAL_SETTLEMENT", SourceReference: "LOCAL_POSITION:original", FetchedAt: from.Add(time.Hour)}
	store := &memoryStore{targets: []Window{{Asset: a, From: from, To: now.Add(-24 * time.Hour)}}, data: Dataset{Series: map[string]Series{a.Key(): {Settlements: []Settlement{known}}}}}
	source := &awareTestSource{}
	report, e := Run(context.Background(), store, source, SyncOptions{Now: now, Workers: 1})
	if e != nil || report.Saved != 1 || source.legacyCalls != 0 || len(source.known) != 1 || source.known[0].SourceReference != known.SourceReference {
		t.Fatal(report, e, source)
	}
	if len(store.saves) != 1 || store.saves[0].Window.Asset.Key() != a.Key() || !store.saves[0].Series.Settlements[0].EffectiveAt.Equal(known.EffectiveAt) {
		t.Fatal("known evidence changed identity or original time", store.saves)
	}
	// A later run sees the persisted terminal and committed prefix. It must
	// not fetch an already complete closed lifecycle again.
	store.data.Series[a.Key()] = store.saves[0].Series
	second, err := Run(context.Background(), store, source, SyncOptions{Now: now, Workers: 1})
	if err != nil || second.Planned != 0 || second.Saved != 0 || len(store.saves) != 1 {
		t.Fatal("repeated run did not reuse committed terminal", second, err)
	}
}
