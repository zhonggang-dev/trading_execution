package accountpricehistory

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type hourlyTestSource struct {
	now   time.Time
	calls int
	fail  bool
}

func (s *hourlyTestSource) Fetch(context.Context, Window) (Result, error) {
	return Result{}, fmt.Errorf("hourly collection lost refresh semantics")
}
func (s *hourlyTestSource) FetchWithSeries(_ context.Context, w Window, seed Series) (Result, error) {
	s.calls++
	if s.fail {
		return Result{}, fmt.Errorf("temporary official source failure")
	}
	if w.RefreshAt == nil || !w.From.Equal(*w.RefreshAt) || !w.To.Equal(*w.RefreshAt) || len(seed.Points) == 0 {
		return Result{}, fmt.Errorf("missing exact-hour refresh with immutable seed")
	}
	return Result{Window: w, Series: Series{
		Points:   []Point{{At: w.From.Add(-time.Minute), Price: "0.8", ResolutionSeconds: 0, FetchedAt: s.now}},
		Coverage: []Coverage{{From: w.From, To: w.To, State: "complete", Reason: "hourly_refresh", FetchedAt: s.now}},
	}}, nil
}

func TestLatestCompletedHourRefreshesDespiteAdmissibleOldPrice(t *testing.T) {
	hour := time.Date(2026, 11, 5, 13, 0, 0, 0, time.UTC)
	now := hour.Add(17 * time.Minute)
	a := testAsset()
	target := Window{Asset: a, From: hour.Add(-time.Hour), To: now, RequiredAt: []time.Time{hour}}
	seed := Series{Points: []Point{{At: hour.Add(-time.Hour), Price: "0.2", ResolutionSeconds: 3600}},
		Coverage: []Coverage{{From: target.From, To: now, State: "complete", FetchedAt: now}}}
	store := &memoryStore{targets: []Window{target}, data: Dataset{ObservedAt: now, Series: map[string]Series{a.Key(): seed}}}
	source := &hourlyTestSource{now: now}
	jobs := Plan(store.targets, store.data, now)
	if len(jobs) != 1 || jobs[0].RefreshAt == nil || !jobs[0].RefreshAt.Equal(hour) {
		t.Fatal("<=1h seed suppressed collection of a new completed hour", jobs)
	}
	report, err := Run(context.Background(), store, source, SyncOptions{Now: now, Workers: 1})
	if err != nil || source.calls != 1 || report.Saved != 1 || len(store.saves) != 1 {
		t.Fatal(report, err, source.calls)
	}
	// The stored value keeps its real observation time, not a fabricated H.
	if !store.saves[0].Series.Points[0].At.Equal(hour.Add(-time.Minute)) {
		t.Fatal("refresh fabricated a price at the scheduled hour", store.saves)
	}
	updated := seed
	updated.Points = append(append([]Point{}, seed.Points...), store.saves[0].Series.Points...)
	updated.Coverage = append(append([]Coverage{}, seed.Coverage...), store.saves[0].Series.Coverage...)
	SortSeries(&updated)
	store.data.Series[a.Key()] = updated
	second, err := Run(context.Background(), store, source, SyncOptions{Now: now.Add(time.Minute), Workers: 1})
	if err != nil || second.Planned != 0 || second.Saved != 0 || source.calls != 1 {
		t.Fatal("same successful hour was collected twice", second, err, source.calls)
	}
	// A new hour is independently collected even though the old proof remains.
	nextHour := hour.Add(time.Hour)
	store.targets[0].To = nextHour.Add(17 * time.Minute)
	store.targets[0].RequiredAt = []time.Time{hour, nextHour}
	refreshes := 0
	for _, job := range Plan(store.targets, store.data, store.targets[0].To) {
		if job.RefreshAt != nil {
			refreshes++
			if !job.RefreshAt.Equal(nextHour) {
				t.Fatal("new hour reused previous marker", job)
			}
		}
	}
	if refreshes != 1 {
		t.Fatal("next completed hour has no single collection", refreshes)
	}
}

func TestFailedHourlyRefreshRetriesWithoutTreatingOldPriceAsFresh(t *testing.T) {
	hour := time.Date(2026, 11, 5, 13, 0, 0, 0, time.UTC)
	now := hour.Add(17 * time.Minute)
	a := testAsset()
	w := Window{Asset: a, From: hour.Add(-time.Hour), To: now, RequiredAt: []time.Time{hour}}
	store := &memoryStore{targets: []Window{w}, data: Dataset{ObservedAt: now, Series: map[string]Series{a.Key(): {Points: []Point{{At: hour.Add(-time.Hour), Price: "0.2", ResolutionSeconds: 3600}}}}}}
	source := &hourlyTestSource{now: now, fail: true}
	first, err := Run(context.Background(), store, source, SyncOptions{Now: now, Workers: 1})
	if err == nil || first.Failed != 1 || source.calls != 1 {
		t.Fatal(first, err, source.calls)
	}
	series := store.data.Series[a.Key()]
	series.Coverage = append(series.Coverage, store.saves[0].Series.Coverage...)
	store.data.Series[a.Key()] = series
	if jobs := Plan(store.targets, store.data, now.Add(time.Minute)); len(jobs) != 1 || jobs[0].RefreshAt == nil {
		t.Fatal("failed source was marked as a successful hourly collection", jobs)
	}
}

func TestHourlyRefreshExcludesExitedUnheldAndConfirmedTerminalAssets(t *testing.T) {
	hour := time.Date(2026, 11, 5, 13, 0, 0, 0, time.UTC)
	now := hour.Add(17 * time.Minute)
	a := testAsset()
	terminal := Series{Settlements: []Settlement{{EffectiveAt: hour.Add(-time.Minute), Price: "0", Source: "OFFICIAL_SETTLEMENT", SourceReference: "confirmed"}}}
	for _, test := range []struct {
		window Window
		series Series
	}{
		{Window{Asset: a, From: hour.Add(-2 * time.Hour), To: hour.Add(-time.Minute), RequiredAt: []time.Time{hour.Add(-time.Hour)}}, Series{}},
		{Window{Asset: a, From: hour.Add(time.Minute), To: now, RequiredAt: []time.Time{hour.Add(time.Minute)}}, Series{}},
		{Window{Asset: a, From: hour.Add(-time.Hour), To: now, RequiredAt: []time.Time{hour}}, terminal},
	} {
		if _, ok := hourlyRefresh(test.window, test.series, now); ok {
			t.Fatal("collected latest hour without an unsettled holding at that hour", test)
		}
	}
}

func TestHourlyRefreshPreserves1741HistoricalTargetsIn213WarmWindows(t *testing.T) {
	hour := time.Date(2026, 11, 5, 13, 0, 0, 0, time.UTC)
	now := hour.Add(17 * time.Minute)
	targets := []Window{}
	data := Dataset{Series: map[string]Series{}}
	for i := 0; i < 213; i++ {
		a := testAsset()
		a.TokenID = fmt.Sprintf("fixture-%d", i)
		times := []time.Time{}
		count := 8
		if i < 37 {
			count++
		}
		for p := 0; p < count; p++ {
			times = append(times, hour.Add(-48*time.Hour+time.Duration(p)*time.Second))
		}
		times = append(times, hour)
		targets = append(targets, Window{Asset: a, From: times[0], To: now, RequiredAt: times})
		data.Series[a.Key()] = Series{Points: []Point{{At: hour.Add(-time.Hour), Price: "0.2", ResolutionSeconds: 3600}}, Coverage: []Coverage{{From: times[0], To: now, State: "complete"}}}
	}
	warmWindows, warmPoints, refreshes := 0, 0, 0
	for _, job := range Plan(targets, data, now) {
		if job.RefreshAt != nil {
			refreshes++
		} else {
			warmWindows++
			warmPoints += len(job.RequiredAt)
		}
	}
	if warmWindows != 213 || warmPoints != 1741 || refreshes != 213 {
		t.Fatal("latest-hour collection swallowed or expanded historical warm repair", warmWindows, warmPoints, refreshes)
	}
}
