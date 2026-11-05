package accountpricehistory

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type seriesTestSource struct {
	calls int
	seen  Window
	seed  Series
}

func (s *seriesTestSource) Fetch(context.Context, Window) (Result, error) {
	return Result{}, fmt.Errorf("wrong legacy path")
}
func (s *seriesTestSource) FetchWithSeries(_ context.Context, w Window, seed Series) (Result, error) {
	s.calls++
	s.seen, s.seed = w, seed
	at := w.RequiredAt[0].Add(-time.Minute)
	return Result{Window: w, Series: Series{Points: []Point{{At: at, Price: "0.5", ResolutionSeconds: 0}}, Coverage: []Coverage{{From: w.From, To: w.To, State: "complete"}}}}, nil
}

func TestPlanRechecksRealEventSamplesWithinCompleteWindow(t *testing.T) {
	at := time.Date(2026, 10, 28, 9, 0, 0, 0, time.UTC)
	first, last := at.Add(64*time.Minute), at.Add(69*time.Minute)
	a := testAsset()
	w := Window{Asset: a, From: at, To: at.Add(74 * time.Minute), RequiredAt: []time.Time{at, at.Add(time.Hour), first, last}}
	data := Dataset{Series: map[string]Series{a.Key(): {Points: []Point{{At: at, Price: "0.5", ResolutionSeconds: 3600}, {At: at.Add(70 * time.Minute), Price: "0.4", ResolutionSeconds: 300}}, Coverage: []Coverage{{From: w.From, To: w.To, State: "complete"}}}}}
	jobs := Plan([]Window{w}, data, at.Add(24*time.Hour))
	if len(jobs) != 1 || !jobs[0].From.Equal(first) || !jobs[0].To.Equal(last) || len(jobs[0].RequiredAt) != 2 {
		t.Fatal("complete range checkpoint hid event holes", jobs)
	}
	store, source := &memoryStore{targets: []Window{w}, data: data}, &seriesTestSource{}
	r, err := Run(context.Background(), store, source, SyncOptions{Now: at.Add(24 * time.Hour), Workers: 1})
	if err != nil || r.Saved != 1 || source.calls != 1 || len(source.seed.Points) != 2 || len(source.seen.RequiredAt) != 2 {
		t.Fatal(r, err, source)
	}
}

func TestPlanSplits10001RequiredSamplesWithoutDroppingTargets(t *testing.T) {
	at := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	requested := make([]time.Time, 10001)
	for i := range requested {
		requested[i] = at.Add(time.Duration(i) * time.Second)
	}
	w := Window{Asset: testAsset(), From: at, To: requested[len(requested)-1], RequiredAt: requested}
	jobs := Plan([]Window{w}, Dataset{Series: map[string]Series{}}, w.To.Add(time.Minute))
	warm := []Window{}
	for _, job := range jobs {
		if job.RefreshAt == nil {
			warm = append(warm, job)
		}
	}
	if len(warm) != 2 || len(warm[0].RequiredAt) != 10000 || len(warm[1].RequiredAt) != 1 {
		t.Fatal("legal requirements exceeded the source sample cap", len(jobs))
	}
	next := 0
	for _, job := range warm {
		if job.RequiredAt == nil || job.Asset.Key() != w.Asset.Key() || !job.From.Equal(job.RequiredAt[0]) || !job.To.Equal(job.RequiredAt[len(job.RequiredAt)-1]) {
			t.Fatal("split changed identity or invented clipping boundaries", job)
		}
		for _, sample := range job.RequiredAt {
			if !sample.Equal(requested[next]) {
				t.Fatal("split dropped or duplicated a target", next, sample)
			}
			next++
		}
	}
	if next != len(requested) {
		t.Fatal("split did not retain all target samples", next)
	}
}

func TestExplicitRequirementsKeepCurrentHourEventsAndSkipAvailablePrices(t *testing.T) {
	at := time.Date(2026, 11, 5, 13, 0, 0, 0, time.UTC)
	now := at.Add(17 * time.Minute)
	event := at.Add(5 * time.Minute)
	a := testAsset()
	w := Window{Asset: a, From: at.Add(-time.Hour), To: now, RequiredAt: []time.Time{event}}
	store := &memoryStore{targets: []Window{w}, data: Dataset{Series: map[string]Series{a.Key(): {Points: []Point{{At: at.Add(-time.Hour), Price: "0.5", ResolutionSeconds: 3600}}, Coverage: []Coverage{{From: w.From, To: at, State: "complete"}}}}}}
	jobs := Plan(store.targets, store.data, now)
	if len(jobs) != 1 || !jobs[0].To.Equal(event) {
		t.Fatal("current-hour known cash event was postponed to next hour", jobs)
	}
	store.data.Series[a.Key()] = Series{Points: []Point{{At: at, Price: "0.5", ResolutionSeconds: 3600}}}
	source := &seriesTestSource{}
	r, err := Run(context.Background(), store, source, SyncOptions{Now: now})
	if err != nil || r.Planned != 0 || r.Saved != 0 || source.calls != 0 {
		t.Fatal("already priced requirements made unnecessary network calls", r, err, source.calls)
	}
}

func TestRunPersistsLocalTerminalWithoutNetworkEvenWhenComplete(t *testing.T) {
	at := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	a := testAsset()
	known := Settlement{EffectiveAt: at, Price: "0", Source: "OFFICIAL_SETTLEMENT", SourceReference: "LOCAL_POSITION:verified", FetchedAt: at}
	w := Window{Asset: a, From: at, To: at.Add(time.Hour), RequiredAt: []time.Time{at}}
	store := &memoryStore{targets: []Window{w}, data: Dataset{ObservedAt: at.Add(2 * time.Hour), Series: map[string]Series{a.Key(): {Settlements: []Settlement{known}, Coverage: []Coverage{{From: w.From, To: w.To, State: "complete"}}}}}}
	source := &seriesTestSource{}
	r, err := Run(context.Background(), store, source, SyncOptions{Now: at.Add(2 * time.Hour)})
	if err != nil || r.Planned != 0 || r.Saved != 0 || source.calls != 0 || len(store.saves) != 1 || len(store.saves[0].Series.Settlements) != 1 {
		t.Fatal("local terminal remained only in transient Load memory", r, err, store.saves)
	}
	store.saves = nil
	r, err = Run(context.Background(), store, source, SyncOptions{Now: at.Add(2 * time.Hour), DryRun: true})
	if err != nil || len(store.saves) != 0 {
		t.Fatal("dry run persisted terminal evidence", r, err)
	}
}
