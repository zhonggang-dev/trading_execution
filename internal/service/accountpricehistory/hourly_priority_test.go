package accountpricehistory

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

type hourlyPriorityStore struct {
	memoryStore
	hour     time.Time
	broadErr error
	trace    []string
}

func (s *hourlyPriorityStore) Load(_ context.Context, windows []Window) (Dataset, error) {
	for _, window := range windows {
		if !window.From.Equal(s.hour) || !window.To.Equal(s.hour) {
			s.trace = append(s.trace, "load history")
			return s.data, s.broadErr
		}
	}
	s.trace = append(s.trace, "load hour")
	return s.data, nil
}

func (s *hourlyPriorityStore) Save(ctx context.Context, result Result) error {
	if err := s.memoryStore.Save(ctx, result); err != nil {
		return err
	}
	w := result.Window
	if w.RefreshAt != nil {
		s.trace = append(s.trace, "save hour "+w.Asset.TokenID)
	} else {
		s.trace = append(s.trace, "save history "+w.Asset.TokenID)
	}
	key := w.Asset.Key()
	series := s.data.Series[key]
	series.Points = append(series.Points, result.Series.Points...)
	series.Coverage = append(series.Coverage, result.Series.Coverage...)
	SortSeries(&series)
	s.data.Series[key] = series
	return nil
}

func TestAllWindowsSavesMissingHourBeforeBroadHistory(t *testing.T) {
	hour := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)
	now := hour.Add(time.Minute)
	early := hour.Add(-48 * time.Hour)
	store := &hourlyPriorityStore{
		memoryStore: memoryStore{data: Dataset{Series: map[string]Series{}}},
		hour:        hour,
	}
	for _, token := range []string{"priced", "other-missing", "primary-missing"} {
		asset := testAsset()
		asset.TokenID = token
		window := Window{Asset: asset, From: early, To: now, RequiredAt: []time.Time{hour}}
		if token == "priced" {
			store.data.Series[asset.Key()] = Series{Points: []Point{
				{At: hour.Add(-time.Hour), Price: "0.3"},
			}}
		}
		if token == "primary-missing" {
			window.PrimaryHoldingAtHour = true
			window.RequiredAt = []time.Time{early, hour}
		}
		store.targets = append(store.targets, window)
	}
	source := sourceFunc(func(_ context.Context, w Window) (Result, error) {
		reason := ""
		if w.RefreshAt != nil {
			reason = "hourly_refresh"
		} else if w.From.Equal(hour) || w.To.Equal(hour) {
			t.Error("latest-hour requirement was requested again after an official refresh")
		}
		return Result{Window: w, Series: Series{
			Points:   []Point{{At: w.From, Price: "0.7", FetchedAt: now}},
			Coverage: []Coverage{{From: w.From, To: w.To, State: "complete", Reason: reason, FetchedAt: now}},
		}}, nil
	})
	options := SyncOptions{Now: now, Workers: 1, AllWindows: true}
	report, err := Run(context.Background(), store, source, options)
	want := []string{"load hour", "save hour primary-missing", "save hour other-missing", "save hour priced", "load history", "save history primary-missing"}
	if err != nil || report.Saved != 4 || report.Planned != 4 || report.Deferred != 0 || !reflect.DeepEqual(store.trace, want) {
		t.Fatalf("wrong synchronization order or counts: trace=%v report=%+v err=%v", store.trace, report, err)
	}
	store.trace = nil
	report, err = Run(context.Background(), store, source, options)
	want = []string{"load hour", "load history"}
	if err != nil || report.Saved != 0 || report.Planned != 0 || !reflect.DeepEqual(store.trace, want) {
		t.Fatalf("durable refresh or history was fetched again: trace=%v report=%+v err=%v", store.trace, report, err)
	}
}

func TestLatestHourSurvivesBroadHistoryLoadFailure(t *testing.T) {
	hour := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)
	now := hour.Add(time.Minute)
	asset := testAsset()
	w := Window{Asset: asset, From: hour.Add(-48 * time.Hour), To: now, RequiredAt: []time.Time{hour}}
	broadErr := errors.New("broad load unavailable")
	store := &hourlyPriorityStore{
		memoryStore: memoryStore{data: Dataset{Series: map[string]Series{}}, targets: []Window{w}},
		hour:        hour,
		broadErr:    broadErr,
	}
	source := sourceFunc(func(_ context.Context, window Window) (Result, error) {
		return Result{Window: window, Series: Series{
			Points:   []Point{{At: hour, Price: "0.7", FetchedAt: now}},
			Coverage: []Coverage{{From: hour, To: hour, State: "complete", Reason: "hourly_refresh", FetchedAt: now}},
		}}, nil
	})
	report, err := Run(context.Background(), store, source, SyncOptions{Now: now, Workers: 1, AllWindows: true})
	price, available := PriceAt(store.data.Series[asset.Key()], hour)
	if !errors.Is(err, broadErr) || report.Planned != 1 || report.Saved != 1 || !available || price.Price != "0.7" {
		t.Fatalf("broad load failure blocked latest mark: report=%+v price=%+v err=%v", report, price, err)
	}
}

func TestFailedLatestHourRefreshRepairsStillMissingMarksInPriorityOrder(t *testing.T) {
	hour := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)
	now := hour.Add(time.Minute)
	store := &hourlyPriorityStore{
		memoryStore: memoryStore{data: Dataset{Series: map[string]Series{}}},
		hour:        hour,
	}
	for _, token := range []string{"other", "main-error", "main-success"} {
		asset := testAsset()
		asset.TokenID = token
		store.targets = append(store.targets, Window{
			Asset: asset, From: hour.Add(-48 * time.Hour), To: now, RequiredAt: []time.Time{hour},
			PrimaryHoldingAtHour: token != "other",
		})
	}
	var attempts []string
	allowOfficialRefresh := false
	source := sourceFunc(func(_ context.Context, w Window) (Result, error) {
		reason := ""
		if w.RefreshAt != nil {
			attempts = append(attempts, "refresh "+w.Asset.TokenID)
			reason = "hourly_refresh"
			if !allowOfficialRefresh {
				switch w.Asset.TokenID {
				case "main-error":
					return Result{}, errors.New("official source temporarily unavailable")
				case "other":
					return Result{Window: w, Series: Series{Coverage: []Coverage{
						{From: hour, To: hour, State: "missing", Reason: "missing_prices", FetchedAt: now},
					}}}, nil
				}
			}
		} else {
			attempts = append(attempts, "repair "+w.Asset.TokenID)
			if !w.From.Equal(hour) || !w.To.Equal(hour) || !reflect.DeepEqual(w.RequiredAt, []time.Time{hour}) {
				t.Errorf("repair has wrong scope: %+v", w)
			}
		}
		return Result{Window: w, Series: Series{
			Points:   []Point{{At: hour, Price: "0.7", FetchedAt: now}},
			Coverage: []Coverage{{From: hour, To: hour, State: "complete", Reason: reason, FetchedAt: now}},
		}}, nil
	})
	options := SyncOptions{Now: now, Workers: 1, AllWindows: true}
	report, err := Run(context.Background(), store, source, options)
	want := []string{"refresh main-error", "refresh main-success", "repair main-error", "refresh other", "repair other"}
	if err == nil || report.Planned != 5 || report.Failed != 1 || report.Missing != 1 || report.Saved != 4 || !reflect.DeepEqual(attempts, want) {
		t.Fatalf("first attempt: report=%+v attempts=%v err=%v", report, attempts, err)
	}
	for _, target := range store.targets {
		series := store.data.Series[target.Asset.Key()]
		if _, ok := PriceAt(series, hour); !ok {
			t.Errorf("repair did not save a usable mark for %s", target.Asset.TokenID)
		}
		wantRefresh := target.Asset.TokenID != "main-success"
		if needsHourlyRefresh(series, hour) != wantRefresh {
			t.Errorf("incorrect official refresh checkpoint for %s", target.Asset.TokenID)
		}
	}
	attempts = nil
	allowOfficialRefresh = true
	report, err = Run(context.Background(), store, source, options)
	want = []string{"refresh other", "refresh main-error"}
	if err != nil || report.Planned != 2 || report.Saved != 2 || !reflect.DeepEqual(attempts, want) {
		t.Fatalf("retry: report=%+v attempts=%v err=%v", report, attempts, err)
	}
}

func TestPriorityUsesMainHoldingsAfterBoundaryEvents(t *testing.T) {
	hour := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)
	earlier := hour.Add(-time.Hour)
	facts := requirementFacts(hour.Add(time.Minute), map[string][]consolev2.CohortReplayLot{
		"main": {
			requirementLot("main", "new", "new", "1", "1", hour),
			requirementLot("main", "shared-main", "shared", "1", "0", earlier, consolev2.CohortReplayDisposition{ID: "exit", At: hour, Shares: "1"}),
			requirementLot("main", "after", "after", "1", "1", hour.Add(time.Second)),
		},
		"other": {requirementLot("other", "shared-other", "shared", "1", "1", earlier)},
	})
	windows, err := BuildRequirements(context.Background(), facts)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, window := range windows {
		if latest, exists := hourlyWindow(window, hour); exists {
			got[latest.Asset.TokenID] = latest.PrimaryHoldingAtHour
		}
	}
	if want := map[string]bool{"new": true, "shared": false}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wrong holdings prioritized at the boundary: got %v want %v", got, want)
	}
}
