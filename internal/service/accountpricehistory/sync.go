package accountpricehistory

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type SyncOptions struct {
	Now        time.Time
	MaxWindows int
	AllWindows bool
	Workers    int
	DryRun     bool
}

// Match the source's per-window sample budget as well as its 14-day span.
const maxSourceSamplesPerWindow = 10000

type Failure struct {
	AssetKey string    `json:"asset_key"`
	From     time.Time `json:"from"`
	To       time.Time `json:"to"`
	Reason   string    `json:"reason"`
	Detail   string    `json:"detail,omitempty"`
}
type SyncReport struct {
	Failures   []Failure `json:"failures,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	Targets    int       `json:"targets"`
	Planned    int       `json:"planned"`
	Saved      int       `json:"saved"`
	Missing    int       `json:"missing"`
	Failed     int       `json:"failed"`
	Deferred   int       `json:"deferred"`
	DryRun     bool      `json:"dry_run"`
	Busy       bool      `json:"busy"`
}

// Plan verifies the actual computation samples even inside complete coverage.
// Historical requirements need no request when a valid point already exists.
// The newest completed hour has its own bounded official refresh checkpoint;
// an admissible older point alone must not stand in for hourly collection.
func Plan(targets []Window, data Dataset, now time.Time) []Window {
	return planWindows(targets, data, now, false)
}

// When the hour is handled by the first phase, only the older and event
// requirements belong to the backfill phase.
func planWindows(targets []Window, data Dataset, now time.Time, hourHandled bool) []Window {
	hour := now.UTC().Truncate(time.Hour)
	legacy := []Window{}
	result := []Window{}
	refreshes := map[string]bool{}
	for _, target := range targets {
		if target.RequiredAt == nil {
			legacy = append(legacy, target)
			continue
		}
		missing := []time.Time{}
		series := data.Series[target.Asset.Key()]
		if !hourHandled && !refreshes[target.Asset.Key()] {
			if refresh, ok := hourlyRefresh(target, series, now); ok {
				result = append(result, refresh)
				refreshes[target.Asset.Key()] = true
			}
		}
		for _, at := range RequiredTimes(target) {
			if !at.Before(now) || (hourHandled && at.Equal(hour)) {
				continue
			}
			if _, ok := PriceAt(series, at); !ok {
				missing = append(missing, at)
			}
		}
		for len(missing) > 0 {
			stop := missing[0].Add(14 * 24 * time.Hour)
			n := sort.Search(len(missing), func(i int) bool { return missing[i].After(stop) })
			n = min(n, maxSourceSamplesPerWindow)
			result = append(result, Window{Asset: target.Asset, From: missing[0], To: missing[n-1], RequiredAt: append([]time.Time{}, missing[:n]...)})
			missing = missing[n:]
		}
	}
	result = append(result, planLegacy(legacy, data, now)...)
	lastAttempt := func(w Window) time.Time {
		var latest time.Time
		for _, c := range data.Series[w.Asset.Key()].Coverage {
			if !c.To.Before(w.From) && !c.From.After(w.To) && c.FetchedAt.After(latest) {
				latest = c.FetchedAt
			}
		}
		return latest
	}
	sort.SliceStable(result, func(i, j int) bool { return lastAttempt(result[i]).Before(lastAttempt(result[j])) })
	return result
}

func hourlyRefresh(target Window, series Series, now time.Time) (Window, bool) {
	window, ok := hourlyWindow(target, now.UTC().Truncate(time.Hour))
	return window, ok && needsHourlyRefresh(series, *window.RefreshAt)
}

func hourlyWindow(target Window, hour time.Time) (Window, bool) {
	if hour.Before(target.From) || hour.After(target.To) {
		return Window{}, false
	}
	for _, at := range target.RequiredAt {
		if at.Equal(hour) {
			return Window{Asset: target.Asset, From: hour, To: hour, RequiredAt: []time.Time{hour},
				RefreshAt: &hour, PrimaryHoldingAtHour: target.PrimaryHoldingAtHour}, true
		}
	}
	return Window{}, false
}

// Only a real successful refresh of this exact hour is an hourly checkpoint.
// Broad historical coverage may have reused a <=1h point without reading the
// latest official value for the new hour, so it is insufficient here.
func needsHourlyRefresh(series Series, hour time.Time) bool {
	for _, terminal := range series.Settlements {
		if terminal.Source == "OFFICIAL_SETTLEMENT" && terminal.SourceReference != "" && !terminal.EffectiveAt.After(hour) && ValidPrice(terminal.Price) {
			return false
		}
	}
	if _, priced := PriceAt(series, hour); priced {
		for _, coverage := range series.Coverage {
			if coverage.State == "complete" && coverage.Reason == "hourly_refresh" && coverage.From.Equal(hour) && coverage.To.Equal(hour) && !coverage.FetchedAt.Before(hour) {
				return false
			}
		}
	}
	return true
}

type hourlyPlan struct {
	primaryMissing []Window
	otherMissing   []Window
	priced         []Window
	data           Dataset
}

func planHourly(ctx context.Context, store Store, targets []Window, hour time.Time) (hourlyPlan, error) {
	windows := []Window{}
	indices := map[string]int{}
	for _, target := range targets {
		window, ok := hourlyWindow(target, hour)
		if !ok {
			continue
		}
		key := window.Asset.Key()
		if i, exists := indices[key]; exists {
			windows[i].PrimaryHoldingAtHour = windows[i].PrimaryHoldingAtHour || window.PrimaryHoldingAtHour
			continue
		}
		indices[key] = len(windows)
		windows = append(windows, window)
	}
	if len(windows) == 0 {
		return hourlyPlan{}, nil
	}
	data, err := store.Load(ctx, windows)
	if err != nil {
		return hourlyPlan{}, fmt.Errorf("load latest hour price history: %w", err)
	}
	plan := hourlyPlan{data: data}
	for _, window := range windows {
		series := data.Series[window.Asset.Key()]
		if !needsHourlyRefresh(series, hour) {
			continue
		}
		_, available := PriceAt(series, hour)
		switch {
		case !available && window.PrimaryHoldingAtHour:
			plan.primaryMissing = append(plan.primaryMissing, window)
		case !available:
			plan.otherMissing = append(plan.otherMissing, window)
		default:
			plan.priced = append(plan.priced, window)
		}
	}
	return plan, nil
}

// planLegacy subtracts committed range coverage and revisits two recent hours.
func planLegacy(targets []Window, data Dataset, now time.Time) []Window {
	result := []Window{}
	for _, target := range targets {
		end := target.To
		if end.After(now.Truncate(time.Hour)) {
			end = now.Truncate(time.Hour)
		}
		if end.Before(target.From) {
			continue
		}
		series := data.Series[target.Asset.Key()]
		for _, settlement := range series.Settlements {
			if !settlement.EffectiveAt.After(end) {
				if settlement.EffectiveAt.Before(end) {
					end = settlement.EffectiveAt
				}
			}
		}
		if end.Before(target.From) {
			continue
		}
		covered := []Coverage{}
		for _, c := range series.Coverage {
			if c.State == "complete" && c.To.After(target.From) && c.From.Before(end) {
				covered = append(covered, c)
			}
		}
		sort.Slice(covered, func(i, j int) bool { return covered[i].From.Before(covered[j].From) })
		cursor := target.From
		gaps := []Window{}
		for _, c := range covered {
			if c.From.After(cursor) {
				stop := minTime(c.From, end)
				if stop.After(cursor) {
					gaps = append(gaps, Window{Asset: target.Asset, From: cursor, To: stop})
				}
			}
			if c.To.After(cursor) {
				cursor = c.To
			}
			if !cursor.Before(end) {
				break
			}
		}
		if cursor.Before(end) {
			gaps = append(gaps, Window{Asset: target.Asset, From: cursor, To: end})
		}
		if target.To.After(now.Add(-2*time.Hour)) && end.After(now.Add(-2*time.Hour)) {
			start := now.Truncate(time.Hour).Add(-2 * time.Hour)
			if start.Before(target.From) {
				start = target.From
			}
			if start.Before(end) {
				gaps = append(gaps, Window{Asset: target.Asset, From: start, To: end})
			}
		}
		sort.Slice(gaps, func(i, j int) bool { return gaps[i].From.Before(gaps[j].From) })
		merged := []Window{}
		for _, g := range gaps {
			if len(merged) > 0 && !g.From.After(merged[len(merged)-1].To) {
				if g.To.After(merged[len(merged)-1].To) {
					merged[len(merged)-1].To = g.To
				}
			} else {
				merged = append(merged, g)
			}
		}
		for _, g := range merged {
			for g.From.Before(g.To) {
				stop := minTime(g.To, g.From.Add(14*24*time.Hour))
				result = append(result, Window{Asset: g.Asset, From: g.From, To: stop})
				g.From = stop
			}
		}
	}
	// Failed/missing attempts do not starve assets that have never been read.
	lastAttempt := func(w Window) time.Time {
		var latest time.Time
		for _, c := range data.Series[w.Asset.Key()].Coverage {
			if c.To.Before(w.From) || c.From.After(w.To) {
				continue
			}
			if c.FetchedAt.After(latest) {
				latest = c.FetchedAt
			}
		}
		return latest
	}
	sort.SliceStable(result, func(i, j int) bool { return lastAttempt(result[i]).Before(lastAttempt(result[j])) })
	return result
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
func Run(ctx context.Context, store Store, source Source, options SyncOptions) (SyncReport, error) {
	if options.Now.IsZero() {
		options.Now = time.Now().UTC()
	}
	if options.AllWindows && options.MaxWindows != 0 {
		return SyncReport{}, fmt.Errorf("all windows cannot be combined with a window limit")
	}
	if !options.AllWindows && options.MaxWindows == 0 {
		options.MaxWindows = 100
	}
	if options.Workers == 0 {
		options.Workers = 2
	}
	if options.Workers < 1 || options.Workers > 2 || options.MaxWindows < 0 || options.MaxWindows > MaxWindows {
		return SyncReport{}, fmt.Errorf("invalid history sync budget")
	}
	report := SyncReport{StartedAt: time.Now().UTC(), DryRun: options.DryRun}
	unlock, ok, err := store.Lock(ctx)
	if err != nil {
		return report, err
	}
	if !ok {
		report.Busy = true
		return report, nil
	}
	defer unlock()
	targets, err := store.Targets(ctx, options.Now)
	if err != nil {
		return report, err
	}
	report.Targets = len(targets)
	if options.AllWindows {
		if err := syncHourly(ctx, store, source, targets, options, &report); err != nil {
			report.FinishedAt = time.Now().UTC()
			return report, err
		}
	}
	data, err := store.Load(ctx, targets)
	if err != nil {
		return report, err
	}
	if data.ObservedAt.IsZero() {
		data.ObservedAt = options.Now
	}
	if !options.DryRun && source != nil {
		if err = PersistKnownSettlements(ctx, store, targets, data); err != nil {
			return report, err
		}
	}
	jobs := planWindows(targets, data, options.Now, options.AllWindows)
	report.Planned += len(jobs)
	if !options.AllWindows && len(jobs) > options.MaxWindows {
		report.Deferred = len(jobs) - options.MaxWindows
		jobs = jobs[:options.MaxWindows]
	}
	if !options.DryRun && len(jobs) > 0 {
		err = runWindows(ctx, store, source, jobs, data, options.Workers, &report)
	}
	report.FinishedAt = time.Now().UTC()
	if err != nil {
		return report, err
	}
	if report.Missing > 0 {
		return report, fmt.Errorf("history sync has missing price windows")
	}
	if report.Failed > 0 {
		return report, fmt.Errorf("history sync has failed windows")
	}
	return report, nil
}

func syncHourly(ctx context.Context, store Store, source Source, targets []Window, options SyncOptions, report *SyncReport) error {
	hour := options.Now.UTC().Truncate(time.Hour)
	plan, err := planHourly(ctx, store, targets, hour)
	if err != nil {
		return err
	}
	for _, group := range []struct {
		windows []Window
		repair  bool
	}{
		{plan.primaryMissing, true},
		{plan.otherMissing, true},
		{plan.priced, false},
	} {
		report.Planned += len(group.windows)
		if options.DryRun || len(group.windows) == 0 {
			continue
		}
		missing, failed := report.Missing, report.Failed
		if err := runWindows(ctx, store, source, group.windows, plan.data, options.Workers, report); err != nil {
			return err
		}
		if !group.repair || (report.Missing == missing && report.Failed == failed) {
			continue
		}
		// Recheck what was durably saved. Only still-missing marks get one
		// ordinary repair. Repairs use the existing evidence rules and
		// leave the failed official refresh eligible for a future retry.
		data, err := store.Load(ctx, group.windows)
		if err != nil {
			return fmt.Errorf("load latest hour repair history: %w", err)
		}
		repairs := []Window{}
		for _, w := range group.windows {
			if _, ok := PriceAt(data.Series[w.Asset.Key()], hour); !ok {
				w.RefreshAt = nil
				repairs = append(repairs, w)
			}
		}
		report.Planned += len(repairs)
		if len(repairs) > 0 {
			if err := runWindows(ctx, store, source, repairs, data, options.Workers, report); err != nil {
				return err
			}
		}
	}
	return nil
}

func runWindows(ctx context.Context, store Store, source Source, jobs []Window, data Dataset, workerCount int, report *SyncReport) error {
	if source == nil {
		return fmt.Errorf("history source required")
	}
	var mu sync.Mutex
	var workers sync.WaitGroup
	queue := make(chan Window)
	for i := 0; i < workerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range queue {
				var result Result
				var e error
				if aware, ok := source.(SeriesAwareSource); ok {
					result, e = aware.FetchWithSeries(ctx, job, data.Series[job.Asset.Key()])
				} else if aware, ok := source.(SettlementAwareSource); ok {
					result, e = aware.FetchWithSettlements(ctx, job, data.Series[job.Asset.Key()].Settlements)
				} else {
					result, e = source.Fetch(ctx, job)
				}
				if e != nil {
					_ = store.Save(ctx, Result{Window: job, Series: Series{Coverage: []Coverage{{From: job.From, To: job.To, State: "error", Reason: "source_unavailable", FetchedAt: time.Now().UTC()}}}})
					mu.Lock()
					report.Failed++
					if len(report.Failures) < 20 {
						report.Failures = append(report.Failures, Failure{job.Asset.Key(), job.From, job.To, "source_unavailable", e.Error()})
					}
					mu.Unlock()
					continue
				}
				if e = store.Save(ctx, result); e != nil {
					mu.Lock()
					report.Failed++
					if len(report.Failures) < 20 {
						report.Failures = append(report.Failures, Failure{job.Asset.Key(), job.From, job.To, "storage_unavailable", storageSQLState(e)})
					}
					mu.Unlock()
					continue
				}
				missing := false
				for _, c := range result.Series.Coverage {
					missing = missing || c.State != "complete"
				}
				mu.Lock()
				report.Saved++
				if missing {
					report.Missing++
				}
				mu.Unlock()
			}
		}()
	}
send:
	for _, job := range jobs {
		select {
		case queue <- job:
		case <-ctx.Done():
			break send
		}
	}
	close(queue)
	workers.Wait()
	return ctx.Err()
}

// Expose only the SQLSTATE interface. Error text may contain SQL, connection
// information or bind values and must never enter the sync report.
func storageSQLState(err error) string {
	var state interface{ SQLState() string }
	if !errors.As(err, &state) {
		return ""
	}
	code := state.SQLState()
	if len(code) != 5 {
		return ""
	}
	for _, c := range code {
		if !(c >= '0' && c <= '9') && !(c >= 'A' && c <= 'Z') {
			return ""
		}
	}
	return code
}
