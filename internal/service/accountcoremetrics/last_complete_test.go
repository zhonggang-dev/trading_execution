package accountcoremetrics

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

func completeFixture(at time.Time) CohortMetrics {
	dd, sh, zero := "3", "2.5", 0
	from := at.Add(-72 * time.Hour)
	day := at.Truncate(24 * time.Hour)
	return CohortMetrics{Metrics: Metrics{ExecutionAccountID: "pm", MarketSource: "POLYMARKET"}, OpenedAssetCount: 1, MaxDrawdown: &dd, DrawdownState: "available", DrawdownCoverage: &DrawdownCoverage{From: &from, To: &at, ComputedAt: &at, FidelitySeconds: 3600, MissingAssetCount: &zero, MissingPointCount: &zero}, SharpAnnual: &sh, SharpAnnualCoverage: &SharpeCoverage{From: &from, To: &day, ComputedAt: &at, PeriodSeconds: 86400, SampleDays: 2}}
}
func incompleteFixture(at time.Time) CohortMetrics {
	c := completeFixture(at)
	c.MaxDrawdown = nil
	c.SharpAnnual = nil
	c.DrawdownState = "history_unavailable"
	why := "not_synced"
	c.DrawdownCoverage.Reason = &why
	c.SharpAnnualCoverage.Reason = &why
	return c
}
func TestLastCompleteRetainsExactScopeAndDeepCopiesResults(t *testing.T) {
	at := drawdownStart.Add(96 * time.Hour)
	s := New(nil, nil)
	s.now = func() time.Time { return at }
	from, to := drawdownStart, drawdownStart.Add(time.Hour)
	filter := consolev2.CohortFilter{OpeningFrom: &from, OpeningTo: &to}
	original := completeFixture(at)
	s.retainCompleteMetrics(&original, filter, at)
	*original.MaxDrawdown = "999"
	*original.DrawdownCoverage.MissingPointCount = 999
	*original.SharpAnnual = "999"
	pending := incompleteFixture(at.Add(time.Hour))
	s.retainCompleteMetrics(&pending, filter, at.Add(time.Hour))
	if *pending.MaxDrawdown != "3" || *pending.SharpAnnual != "2.5" || !pending.DrawdownCoverage.LastComplete || !pending.SharpAnnualCoverage.LastComplete || pending.DrawdownState != "available" || *pending.DrawdownCoverage.MissingPointCount != 0 || !pending.DrawdownCoverage.To.Equal(at) {
		t.Fatal(pending)
	}
	*pending.MaxDrawdown = "888"
	again := incompleteFixture(at.Add(2 * time.Hour))
	s.retainCompleteMetrics(&again, filter, at.Add(2*time.Hour))
	if *again.MaxDrawdown != "3" {
		t.Fatal("response mutated cached value")
	}
	for _, different := range []consolev2.CohortFilter{{}, {OpeningFrom: &from}, {OpeningTo: &to}} {
		c := incompleteFixture(at.Add(time.Hour))
		s.retainCompleteMetrics(&c, different, at.Add(time.Hour))
		if c.MaxDrawdown != nil || c.SharpAnnual != nil {
			t.Fatal("different opening range reused values")
		}
	}
	other := incompleteFixture(at.Add(time.Hour))
	other.ExecutionAccountID = "other"
	s.retainCompleteMetrics(&other, filter, at.Add(time.Hour))
	if other.MaxDrawdown != nil || other.SharpAnnual != nil {
		t.Fatal("different account reused values")
	}
}
func TestLastCompleteNeverUsesFutureOrMathematicallyUndefinedValues(t *testing.T) {
	at := drawdownStart.Add(96 * time.Hour)
	s := New(nil, nil)
	s.now = func() time.Time { return at }
	filter := consolev2.CohortFilter{}
	good := completeFixture(at)
	s.retainCompleteMetrics(&good, filter, at)
	future := incompleteFixture(at.Add(-time.Hour))
	s.retainCompleteMetrics(&future, filter, at.Add(-time.Hour))
	if future.MaxDrawdown != nil || future.SharpAnnual != nil {
		t.Fatal("future historical endpoints must not leak into an earlier snapshot", future)
	}
	for _, reason := range []string{"insufficient_samples", "zero_variance", "nonpositive_nav"} {
		c := incompleteFixture(at.Add(time.Hour))
		c.SharpAnnualCoverage.Reason = &reason
		s.retainCompleteMetrics(&c, filter, at.Add(time.Hour))
		if c.SharpAnnual != nil {
			t.Fatal("undefined Sharpe replaced by old number", reason)
		}
	}
	newer := completeFixture(at.Add(time.Hour))
	value := "4"
	newer.MaxDrawdown = &value
	newer.SharpAnnual = &value
	s.retainCompleteMetrics(&newer, filter, at.Add(time.Hour))
	older := completeFixture(at)
	s.retainCompleteMetrics(&older, filter, at)
	latest := incompleteFixture(at.Add(2 * time.Hour))
	s.retainCompleteMetrics(&latest, filter, at.Add(2*time.Hour))
	if *latest.MaxDrawdown != "4" || *latest.SharpAnnual != "4" {
		t.Fatal("late old response replaced newer complete result", latest)
	}
}
func TestColdCacheRecoversPriorCompleteHourWithoutCurrentPrices(t *testing.T) {
	asOf := drawdownStart.Add(3*time.Hour + 10*time.Minute)
	lots := []consolev2.CohortReplayLot{replayLot("one", "a", "10", "5", "5", drawdownStart, replayFlow("partial", "5", "4", drawdownStart.Add(2*time.Hour+45*time.Minute))), replayLot("new", "b", "5", "2", "5", drawdownStart.Add(2*time.Hour+30*time.Minute))}
	replay := sharpReplay(t, lots, asOf)
	data := replayDataset(lots, map[string][]string{"a": {"0.5", "0.9"}})
	s := New(nil, nil, historyFixture{data: data})
	s.now = func() time.Time { return asOf }
	why := "not_synced"
	item := CohortMetrics{OpenedAssetCount: 2, DrawdownCoverage: &DrawdownCoverage{Reason: &why}, DrawdownState: "history_unavailable"}
	s.recoverCompleteHistory(context.Background(), &item, replay, data, nil, asOf)
	if item.MaxDrawdown == nil || !item.DrawdownCoverage.LastComplete || !item.DrawdownCoverage.To.Equal(drawdownStart.Add(2*time.Hour)) {
		t.Fatal(item)
	}
	r, err := replayThrough(context.Background(), replay, drawdownStart.Add(2*time.Hour))
	if err != nil || len(r.lots) != 1 || r.lots[0].RemainingShares != "10" {
		t.Fatal(r, err)
	}
}
func TestColdCacheRecoversPriorCompleteDailySharpe(t *testing.T) {
	asOf := drawdownStart.Add(4*24*time.Hour + time.Hour)
	lots := []consolev2.CohortReplayLot{replayLot("one", "a", "10", "5", "10", drawdownStart)}
	data := sharpData(lots, map[string]map[time.Time]string{"a": {drawdownStart: "0.5", drawdownStart.Add(24 * time.Hour): "0.55", drawdownStart.Add(48 * time.Hour): "0.66", drawdownStart.Add(72 * time.Hour): "0.594"}})
	replay := sharpReplay(t, lots, asOf)
	s := New(nil, nil, historyFixture{data: data})
	s.now = func() time.Time { return asOf }
	item := CohortMetrics{OpenedAssetCount: 1}
	s.finishSharpe(context.Background(), &item, replay, data, nil, asOf)
	if item.SharpAnnual != nil {
		t.Fatal("missing daily endpoint accepted")
	}
	s.recoverCompleteHistory(context.Background(), &item, replay, data, nil, asOf)
	sharpNumber(t, item.SharpAnnual, 8.33809387832792)
	if !item.SharpAnnualCoverage.LastComplete || !item.SharpAnnualCoverage.To.Equal(drawdownStart.Add(72*time.Hour)) {
		t.Fatal(item)
	}
}
func TestLastCompleteCacheIsBoundedAndConcurrent(t *testing.T) {
	at := drawdownStart.Add(96 * time.Hour)
	s := New(nil, nil)
	s.now = func() time.Time { return at }
	var group sync.WaitGroup
	for i := 0; i < maxCompleteMetricEntries+20; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			c := completeFixture(at)
			c.ExecutionAccountID = fmt.Sprintf("acct-%d", i)
			s.retainCompleteMetrics(&c, consolev2.CohortFilter{}, at)
		}(i)
	}
	group.Wait()
	if len(s.completeCache) != maxCompleteMetricEntries {
		t.Fatal(len(s.completeCache))
	}
}
