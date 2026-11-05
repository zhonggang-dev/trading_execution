package accountcoremetrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

var drawdownStart = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

func replayLot(id, token, shares, principal, remaining string, at time.Time, flows ...consolev2.CohortReplayDisposition) consolev2.CohortReplayLot {
	return consolev2.CohortReplayLot{LotID: id, OriginalShares: shares, OriginalPrincipal: principal, RemainingShares: remaining, OpenedAt: at, Dispositions: flows, Asset: consolev2.CoreAssetFact{PortfolioHolding: consolev2.PortfolioHolding{ExecutionAccountID: "pm", MarketSource: "POLYMARKET", MarketID: "m-" + token, ConditionID: "c-" + token, TokenID: token, RemainingCost: "999"}}}
}

func replayFlow(id, shares, gross string, at time.Time) consolev2.CohortReplayDisposition {
	return consolev2.CohortReplayDisposition{ID: id, At: at, Shares: shares, GrossProceeds: gross}
}

func replayDataset(lots []consolev2.CohortReplayLot, prices map[string][]string) accountpricehistory.Dataset {
	d := accountpricehistory.Dataset{Series: map[string]accountpricehistory.Series{}, Version: "fixture-v1", ObservedAt: drawdownStart}
	for _, l := range lots {
		a := accountpricehistory.Asset{MarketSource: l.Asset.MarketSource, MarketID: l.Asset.MarketID, ConditionID: l.Asset.ConditionID, TokenID: l.Asset.TokenID}
		s := accountpricehistory.Series{Coverage: []accountpricehistory.Coverage{{From: drawdownStart, To: drawdownStart.Add(24 * time.Hour), State: "complete"}}}
		for hour, price := range prices[l.Asset.TokenID] {
			s.Points = append(s.Points, accountpricehistory.Point{At: drawdownStart.Add(time.Duration(hour) * time.Hour), Price: price, ResolutionSeconds: 3600, Source: "official"})
		}
		d.Series[a.Key()] = s
	}
	return d
}

func drawdownAmount(t *testing.T, got, want string) {
	t.Helper()
	a, e := number(got)
	b, f := number(want)
	if e != nil || f != nil || a.Cmp(b) != 0 {
		t.Fatalf("amount got %q want %q", got, want)
	}
}

func TestDrawdownIncludesUnrealizedAndUsesLatestEndpoint(t *testing.T) {
	lots := []consolev2.CohortReplayLot{replayLot("one", "a", "10", "5", "10", drawdownStart)}
	asOf := drawdownStart.Add(3*time.Hour + 10*time.Minute)
	r, err := prepareDrawdown(context.Background(), lots, "pm", asOf)
	if err != nil {
		t.Fatal(err)
	}
	d := replayDataset(lots, map[string][]string{"a": {"0.5", "0.9", "0.8", "0.8", "1"}})
	p := drawdownHistoricalPrefix(context.Background(), r, d, asOf)
	drawdownAmount(t, p.peak, "4")
	drawdownAmount(t, p.maximum, "1")
	// The source's future point is ignored. The current terminal valuation is
	// the same latest TotalPnL shown in the other metrics.
	s := New(nil, nil, historyFixture{data: d})
	current := "-1"
	item := CohortMetrics{Metrics: Metrics{TotalPnL: &current}, OpenedAssetCount: 1}
	s.finishDrawdown(context.Background(), &item, r, d, nil, asOf)
	if item.DrawdownState != "available" || item.DrawdownCoverage.Reason != nil {
		t.Fatal(item)
	}
	drawdownAmount(t, *item.MaxDrawdown, "5")
	current = "2"
	s.finishDrawdown(context.Background(), &item, r, d, nil, asOf.Add(time.Minute))
	drawdownAmount(t, *item.MaxDrawdown, "2")
	if len(s.drawdownCache) != 1 {
		t.Fatal("current endpoint was cached as historical profit", len(s.drawdownCache))
	}
}

func TestDrawdownPartialFullCloseAndFeesStaySeparate(t *testing.T) {
	lots := []consolev2.CohortReplayLot{replayLot("one", "a", "10", "5", "0", drawdownStart,
		replayFlow("partial", "4", "3", drawdownStart.Add(time.Hour)),
		replayFlow("closed", "6", "2.4", drawdownStart.Add(3*time.Hour)))}
	asOf := drawdownStart.Add(6 * time.Hour)
	r, err := prepareDrawdown(context.Background(), lots, "pm", asOf)
	if err != nil {
		t.Fatal(err)
	}
	d := replayDataset(lots, map[string][]string{"a": {"0.5", "0.8", "0.2"}})
	p := drawdownHistoricalPrefix(context.Background(), r, d, asOf)
	if p.reason != "" {
		t.Fatal(p)
	}
	drawdownAmount(t, p.peak, "2.8")
	drawdownAmount(t, p.maximum, "3.6")
	// No price is required after the closing cash flow; fee-inclusive
	// Asset.RemainingCost=999 is intentionally not a replay principal input.
	current := "0.4"
	item := CohortMetrics{Metrics: Metrics{TotalPnL: &current}, OpenedAssetCount: 1, BuyFees: "9", SellFees: "7"}
	s := New(nil, nil, historyFixture{data: d})
	s.finishDrawdown(context.Background(), &item, r, d, nil, asOf)
	drawdownAmount(t, *item.MaxDrawdown, "3.6")
	if item.BuyFees != "9" || item.SellFees != "7" {
		t.Fatal("independent fees changed", item)
	}
}

func TestDrawdownCacheTracksHistoryRevisionsIncludingReturnToOldPrice(t *testing.T) {
	lots := []consolev2.CohortReplayLot{replayLot("one", "a", "10", "5", "10", drawdownStart)}
	asOf := drawdownStart.Add(3*time.Hour + 10*time.Minute)
	r, _ := prepareDrawdown(context.Background(), lots, "pm", asOf)
	s := New(nil, nil, historyFixture{})
	current := "-1"
	item := CohortMetrics{Metrics: Metrics{TotalPnL: &current}, OpenedAssetCount: 1}
	for i, step := range []struct{ price, want string }{{"0.9", "5"}, {"0.7", "3"}, {"0.9", "5"}} {
		d := replayDataset(lots, map[string][]string{"a": {"0.5", step.price, "0.5", "0.5"}})
		d.Version = fmt.Sprintf("effective-revision-%d", i)
		s.finishDrawdown(context.Background(), &item, r, d, nil, asOf)
		drawdownAmount(t, *item.MaxDrawdown, step.want)
	}
	if len(s.drawdownCache) != 3 {
		t.Fatal("effective price revision was not part of the cache identity")
	}
}

func TestDrawdownSettlementIsNotAppliedBeforeItsEffectiveTime(t *testing.T) {
	lots := []consolev2.CohortReplayLot{replayLot("one", "a", "10", "5", "0", drawdownStart,
		replayFlow("zero-payout", "10", "0", drawdownStart.Add(3*time.Hour)))}
	r, err := prepareDrawdown(context.Background(), lots, "pm", drawdownStart.Add(4*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	d := replayDataset(lots, map[string][]string{"a": {"0.5", "0.9"}})
	for key, series := range d.Series {
		series.Settlements = []accountpricehistory.Settlement{{EffectiveAt: drawdownStart.Add(2 * time.Hour), Price: "0", Source: "settlement", SourceReference: "tx"}}
		d.Series[key] = series
	}
	p := drawdownHistoricalPrefix(context.Background(), r, d, drawdownStart.Add(4*time.Hour))
	if p.reason != "" {
		t.Fatal(p)
	}
	drawdownAmount(t, p.peak, "4")
	drawdownAmount(t, p.maximum, "9")
}

func TestDrawdownAtomicNettingDoesNotCreateAnIntermediatePositionPeak(t *testing.T) {
	at := drawdownStart.Add(time.Hour)
	lots := []consolev2.CohortReplayLot{
		replayLot("yes", "yes", "1", "0.2", "0", drawdownStart, replayFlow("pair-yes", "1", "0.2", at)),
		replayLot("no", "no", "1", "0.8", "0", at, replayFlow("pair-no", "1", "0.8", at)),
	}
	r, err := prepareDrawdown(context.Background(), lots, "pm", at.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	d := replayDataset(lots, map[string][]string{"yes": {"0.2"}})
	p := drawdownHistoricalPrefix(context.Background(), r, d, at.Add(time.Hour))
	if p.reason != "" {
		t.Fatal("the immediately netted leg should never need a quote", p)
	}
	drawdownAmount(t, p.maximum, "0")
}

func TestDrawdownCombinesAssetsAtTheSameInstant(t *testing.T) {
	lots := []consolev2.CohortReplayLot{replayLot("a", "a", "10", "5", "10", drawdownStart), replayLot("b", "b", "10", "5", "10", drawdownStart)}
	asOf := drawdownStart.Add(3 * time.Hour)
	r, err := prepareDrawdown(context.Background(), lots, "pm", asOf)
	if err != nil {
		t.Fatal(err)
	}
	d := replayDataset(lots, map[string][]string{"a": {"0.5", "0.9", "0.1"}, "b": {"0.5", "0.1", "0.9"}})
	p := drawdownHistoricalPrefix(context.Background(), r, d, asOf)
	drawdownAmount(t, p.maximum, "0")
}

func TestDrawdownMissingSourcePendingAndEndpointAreDistinct(t *testing.T) {
	lots := []consolev2.CohortReplayLot{replayLot("a", "a", "1", "0.5", "1", drawdownStart)}
	asOf := drawdownStart.Add(4 * time.Hour)
	r, _ := prepareDrawdown(context.Background(), lots, "pm", asOf)
	d := replayDataset(lots, map[string][]string{"a": {"0.5"}})
	current := "0"
	item := CohortMetrics{Metrics: Metrics{TotalPnL: &current}, OpenedAssetCount: 1}
	s := New(nil, nil, historyFixture{data: d})
	s.finishDrawdown(context.Background(), &item, r, d, nil, asOf)
	if *item.DrawdownCoverage.Reason != "missing_prices" || item.MaxDrawdown != nil || *item.DrawdownCoverage.MissingPointCount != 2 {
		t.Fatal(item.DrawdownCoverage)
	}
	// Old successful history does not establish a source gap in an hour
	// that the next scheduled sync has not requested yet.
	for key, series := range d.Series {
		series.Coverage[0].To = drawdownStart.Add(time.Hour)
		d.Series[key] = series
	}
	d.Version = "tail-not-yet-synced"
	s.finishDrawdown(context.Background(), &item, r, d, nil, asOf)
	if *item.DrawdownCoverage.Reason != "not_synced" {
		t.Fatal("an unrequested tail was called a permanent source gap", item.DrawdownCoverage)
	}
	empty := accountpricehistory.Dataset{Version: "empty", Series: map[string]accountpricehistory.Series{}}
	s.finishDrawdown(context.Background(), &item, r, empty, nil, asOf)
	if *item.DrawdownCoverage.Reason != "not_synced" {
		t.Fatal(item.DrawdownCoverage)
	}
	item.TotalPnL = nil
	s.finishDrawdown(context.Background(), &item, r, d, nil, asOf)
	if *item.DrawdownCoverage.Reason != "stale_endpoint" {
		t.Fatal(item.DrawdownCoverage)
	}
	item.TotalPnL = &current
	s.finishDrawdown(context.Background(), &item, r, d, context.DeadlineExceeded, asOf)
	if *item.DrawdownCoverage.Reason != "budget_exceeded" {
		t.Fatal(item.DrawdownCoverage)
	}
}

func TestDrawdownCursorMatchesNoLookaheadAndBoundedContinuation(t *testing.T) {
	until := drawdownStart.Add(12 * time.Hour)
	series := accountpricehistory.Series{Points: []accountpricehistory.Point{
		{At: drawdownStart, Price: "0.5", ResolutionSeconds: 3600, ValidUntil: &until},
		{At: drawdownStart.Add(time.Hour), Price: "0.7", ResolutionSeconds: 3600},
		{At: drawdownStart.Add(5 * time.Hour), Price: "0.9", ResolutionSeconds: 3600},
		{At: drawdownStart.Add(2 * time.Hour), Price: "1", ResolutionSeconds: 7200},
	}, Settlements: []accountpricehistory.Settlement{{EffectiveAt: drawdownStart.Add(14 * time.Hour), Price: "0"}}}
	accountpricehistory.SortSeries(&series)
	boundaryCursor, err := newDrawdownPriceCursor(context.Background(), series)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := boundaryCursor.at(drawdownStart.Add(3 * time.Hour)); ok {
		t.Fatal("expired newer trade revived an older no-trade extension")
	}
	cursor, err := newDrawdownPriceCursor(context.Background(), series)
	if err != nil {
		t.Fatal(err)
	}
	for at := drawdownStart.Add(-time.Hour); at.Before(until.Add(5 * time.Hour)); at = at.Add(15 * time.Minute) {
		want, wantOK := accountpricehistory.PriceAt(series, at)
		got, ok := cursor.at(at)
		if ok != wantOK {
			t.Fatalf("coverage differs at %s: %v %v", at, ok, wantOK)
		}
		if ok {
			drawdownAmount(t, got.FloatString(24), want.Price)
		}
	}
}

type historyFixture struct {
	data accountpricehistory.Dataset
	err  error
}

func (f historyFixture) Load(context.Context, []accountpricehistory.Window) (accountpricehistory.Dataset, error) {
	return f.data, f.err
}

type historyFunc func(context.Context, []accountpricehistory.Window) (accountpricehistory.Dataset, error)

func (f historyFunc) Load(ctx context.Context, w []accountpricehistory.Window) (accountpricehistory.Dataset, error) {
	return f(ctx, w)
}

func TestCohortDrawdownReadsHistoryInParallelAndMatchesCurrentTotal(t *testing.T) {
	asOf := drawdownStart.Add(3*time.Hour + 10*time.Minute)
	lot := replayLot("one", "a", "10", "5", "10", drawdownStart)
	asset := lot.Asset
	asset.RemainingShares, asset.RemainingCost = "10", "5"
	facts := consolev2.CohortMetricFacts{
		CoreMetricFacts: consolev2.CoreMetricFacts{ObservedAt: asOf, Accounts: []consolev2.CoreAccountFact{{FundingAddress: "private-funding", Metrics: consolev2.AccountPerformance{ExecutionAccountID: "pm", MarketSource: "POLYMARKET", PeakHoldingCost: "5", CumulativeInvestedCost: "5", RealizedPnL: "0", RealizedDrawdown14D: "0"}, Assets: []consolev2.CoreAssetFact{asset}}}},
		Cohorts:         map[string]consolev2.CohortAccountFact{"pm": {OpenedAssetCount: 1, HoldingAssetCount: 1, BuyFees: "0.1", SellFees: "0"}},
		Replay:          map[string]consolev2.CohortReplayFact{"pm": {Lots: []consolev2.CohortReplayLot{lot}}},
	}
	dataset := replayDataset([]consolev2.CohortReplayLot{lot}, map[string][]string{"a": {"0.5", "0.9", "0.8", "0.8"}})
	quoteStarted := make(chan struct{})
	windows := make(chan []accountpricehistory.Window, 1)
	history := historyFunc(func(ctx context.Context, w []accountpricehistory.Window) (accountpricehistory.Dataset, error) {
		windows <- w
		select {
		case <-quoteStarted:
			return dataset, nil
		case <-ctx.Done():
			return accountpricehistory.Dataset{}, ctx.Err()
		}
	})
	s := New(cohortReaderFixture{facts: facts}, quotesFunc(func(_ context.Context, ts []Target) map[string]Quote {
		close(quoteStarted)
		return map[string]Quote{Key(ts[0]): Priced("0.4", "PM_CURRENT_PRICE", asOf, nil)}
	}), history)
	s.now = func() time.Time { return asOf }
	result, err := s.QueryCohort(context.Background(), consolev2.CohortFilter{ExecutionAccountID: "pm"})
	if err != nil || len(result.Items) != 1 {
		t.Fatal(result, err)
	}
	item := result.Items[0]
	drawdownAmount(t, *item.TotalPnL, "-1")
	drawdownAmount(t, *item.MaxDrawdown, "5")
	if item.DrawdownState != "available" || item.DrawdownCoverage.Reason != nil || item.BuyFees != "0.1" {
		t.Fatal(item)
	}
	w := <-windows
	if len(w) != 1 || !w[0].From.Equal(drawdownStart) || !w[0].To.Equal(asOf) {
		t.Fatal("history did not use the exact selected holding interval", w)
	}
	raw, _ := json.Marshal(item)
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	coverage := object["drawdown_coverage"].(map[string]any)
	for _, key := range []string{"reason", "fidelity_seconds", "from", "to", "computed_at", "missing_asset_count", "missing_point_count"} {
		if _, ok := coverage[key]; !ok {
			t.Fatal("nullable coverage field was omitted", key)
		}
	}
	// A new asset opened after the last completed hour has no historical
	// opening mark yet. Today's terminal quote cannot backfill that instant.
	recentLot := lot
	recentLot.OpenedAt = asOf.Add(-time.Minute)
	facts.Replay["pm"] = consolev2.CohortReplayFact{Lots: []consolev2.CohortReplayLot{recentLot}}
	for _, h := range []historyFixture{{data: accountpricehistory.Dataset{Series: map[string]accountpricehistory.Series{}}}, {err: errors.New("history schema not installed")}} {
		pendingService := New(cohortReaderFixture{facts: facts}, quotesFunc(func(_ context.Context, ts []Target) map[string]Quote {
			return map[string]Quote{Key(ts[0]): Priced("0.4", "PM_CURRENT_PRICE", asOf, nil)}
		}), h)
		pendingService.now = func() time.Time { return asOf }
		pending, err := pendingService.QueryCohort(context.Background(), consolev2.CohortFilter{ExecutionAccountID: "pm"})
		if err != nil || len(pending.Items) != 1 {
			t.Fatal(pending, err)
		}
		m := pending.Items[0]
		if m.MaxDrawdown != nil || *m.DrawdownCoverage.Reason != "not_synced" {
			t.Fatal("normal sync lag misclassified as permanent source gap", m.DrawdownCoverage)
		}
		drawdownAmount(t, *m.TotalPnL, "-1")
	}
}

func TestDrawdownRejectsBrokenAllocationsAndCancels(t *testing.T) {
	lot := replayLot("a", "a", "1", "0.5", "0", drawdownStart, replayFlow("x", "2", "0", drawdownStart.Add(time.Hour)))
	if _, err := prepareDrawdown(context.Background(), []consolev2.CohortReplayLot{lot}, "pm", drawdownStart.Add(2*time.Hour)); !errors.Is(err, consolev2.ErrUnavailable) {
		t.Fatal("over-allocation accepted", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := prepareDrawdown(ctx, []consolev2.CohortReplayLot{lot}, "pm", drawdownStart.Add(2*time.Hour)); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel ignored", err)
	}
}

func representativeDrawdown() (drawdownReplay, accountpricehistory.Dataset, time.Time) {
	const assets, hours = 120, 950
	asOf := drawdownStart.Add(hours * time.Hour)
	lots := make([]consolev2.CohortReplayLot, assets)
	prices := map[string][]string{}
	for a := range lots {
		token := fmt.Sprintf("token-%d", a)
		lots[a] = replayLot(token, token, "1", "0.5", "1", drawdownStart)
		values := make([]string, hours)
		for h := range values {
			values[h] = "0.5"
		}
		values[hours-2], values[hours-1] = "0.9", "0.1"
		prices[token] = values
	}
	r, _ := prepareDrawdown(context.Background(), lots, "pm", asOf)
	return r, replayDataset(lots, prices), asOf
}

func TestDrawdownRepresentativeHistoryBudget(t *testing.T) {
	r, data, asOf := representativeDrawdown()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	started := time.Now()
	p := drawdownHistoricalPrefix(ctx, r, data, asOf)
	if p.reason != "" {
		t.Fatal("114000 asset marks exceeded bounded replay", p.reason)
	}
	drawdownAmount(t, p.maximum, "96")
	t.Logf("114000 historical asset marks: %s", time.Since(started))
}

func BenchmarkDrawdownHistorical114000Marks(b *testing.B) {
	r, data, asOf := representativeDrawdown()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := drawdownHistoricalPrefix(context.Background(), r, data, asOf)
		if p.reason != "" || p.maximum != "96" {
			b.Fatal(p.reason)
		}
	}
}
