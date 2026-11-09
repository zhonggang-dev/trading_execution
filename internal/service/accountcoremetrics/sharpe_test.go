package accountcoremetrics

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

func sharpRates(t *testing.T, values ...string) []*big.Rat {
	t.Helper()
	out := make([]*big.Rat, len(values))
	for i, v := range values {
		var ok bool
		out[i], ok = new(big.Rat).SetString(v)
		if !ok {
			t.Fatal(v)
		}
	}
	return out
}
func sharpNumber(t *testing.T, value *string, want float64) {
	t.Helper()
	if value == nil {
		t.Fatal("missing annual Sharpe")
	}
	r, ok := new(big.Rat).SetString(*value)
	if !ok {
		t.Fatal(*value)
	}
	got, _ := r.Float64()
	if math.Abs(got-want) > 1e-11 {
		t.Fatalf("got %.15f want %.15f", got, want)
	}
}
func TestAnnualSharpeUsesSampleDeviationAnd365Days(t *testing.T) {
	// Independent Python statistics.stdev reference, not population deviation.
	for _, c := range []struct {
		rates []string
		want  float64
	}{
		{[]string{"0.01", "0.02", "0.03"}, 38.2099463490856},
		{[]string{"-0.01", "-0.02", "-0.03"}, -38.2099463490856},
		{[]string{"0.1", "0.2", "-0.1"}, 8.33809387832792},
		{[]string{"-0.1", "0.1"}, 0},
	} {
		v, reason := annualSharpe(sharpRates(t, c.rates...))
		if reason != "" {
			t.Fatal(reason)
		}
		sharpNumber(t, v, c.want)
	}
	for _, values := range [][]string{nil, {"0.1"}} {
		v, r := annualSharpe(sharpRates(t, values...))
		if v != nil || r != "insufficient_samples" {
			t.Fatal(v, r)
		}
	}
	for _, values := range [][]string{{"0", "0"}, {"0.1", "0.1"}, {"-0.2", "-0.2"}} {
		v, r := annualSharpe(sharpRates(t, values...))
		if v != nil || r != "zero_variance" {
			t.Fatal(v, r)
		}
	}
}
func sharpData(lots []consolev2.CohortReplayLot, values map[string]map[time.Time]string) accountpricehistory.Dataset {
	d := accountpricehistory.Dataset{Series: map[string]accountpricehistory.Series{}, Version: "sharpe-v1"}
	for _, lot := range lots {
		asset := accountpricehistory.Asset{MarketSource: lot.Asset.MarketSource, MarketID: lot.Asset.MarketID, ConditionID: lot.Asset.ConditionID, TokenID: lot.Asset.TokenID}
		s := accountpricehistory.Series{Coverage: []accountpricehistory.Coverage{{From: drawdownStart, To: drawdownStart.Add(20 * 24 * time.Hour), State: "complete"}}}
		for at, v := range values[lot.Asset.TokenID] {
			s.Points = append(s.Points, accountpricehistory.Point{At: at, Price: v, ResolutionSeconds: 3600, Source: "official"})
		}
		d.Series[asset.Key()] = s
	}
	return d
}
func sharpReplay(t *testing.T, lots []consolev2.CohortReplayLot, asOf time.Time) drawdownReplay {
	t.Helper()
	r, err := prepareDrawdown(context.Background(), lots, "pm", asOf)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func assertRates(t *testing.T, r sharpeReplayResult, want ...string) {
	t.Helper()
	if r.reason != "" || len(r.returns) != len(want) {
		t.Fatalf("reason=%s days=%d expected=%d", r.reason, len(r.returns), len(want))
	}
	for i, v := range want {
		expected, _ := new(big.Rat).SetString(v)
		if r.returns[i].Cmp(expected) != 0 {
			t.Fatalf("day%d got%s want%s", i, r.returns[i].RatString(), v)
		}
	}
}
func TestSharpeDailyReturnsIncludeFloatingProfitButNotCurrentPartialDay(t *testing.T) {
	d0 := drawdownStart
	asOf := d0.Add(3*24*time.Hour + 12*time.Hour)
	lots := []consolev2.CohortReplayLot{replayLot("one", "a", "10", "5", "10", d0)}
	data := sharpData(lots, map[string]map[time.Time]string{"a": {d0: "0.5", d0.Add(24 * time.Hour): "0.55", d0.Add(48 * time.Hour): "0.66", d0.Add(72 * time.Hour): "0.594", d0.Add(73 * time.Hour): "0.01"}})
	replay := sharpReplay(t, lots, asOf)
	rates := dailyCohortReturns(context.Background(), replay, data, asOf)
	assertRates(t, rates, "0.1", "0.2", "-0.1")
	service := New(nil, nil, historyFixture{data: data})
	service.now = func() time.Time { return asOf }
	item := CohortMetrics{OpenedAssetCount: 1, BuyFees: "999", SellFees: "888"}
	service.finishSharpe(context.Background(), &item, replay, data, nil, asOf)
	sharpNumber(t, item.SharpAnnual, 8.33809387832792)
	if item.SharpAnnualCoverage.SampleDays != 3 || item.SharpAnnualCoverage.Reason != nil || !item.SharpAnnualCoverage.To.Equal(d0.Add(72*time.Hour)) || item.BuyFees != "999" {
		t.Fatal(item)
	}
	// A missing current quote does not invalidate completed historical days.
	item.TotalPnL = nil
	service.finishSharpe(context.Background(), &item, replay, data, nil, asOf.Add(4*time.Hour))
	sharpNumber(t, item.SharpAnnual, 8.33809387832792)
}
func TestSharpeAdjustsIntradayDepositsInsteadOfCountingThemAsReturn(t *testing.T) {
	d0 := drawdownStart
	day := d0.Add(24 * time.Hour)
	noon := day.Add(12 * time.Hour)
	end := day.Add(24 * time.Hour)
	lots := []consolev2.CohortReplayLot{replayLot("one", "a", "100", "50", "100", d0), replayLot("two", "a", "100", "60", "100", noon)}
	data := sharpData(lots, map[string]map[time.Time]string{"a": {d0: "0.5", day: "0.5", noon: "0.6", end: "0.66"}})
	assertRates(t, dailyCohortReturns(context.Background(), sharpReplay(t, lots, end), data, end), "0", "0.32")
	// Constant prices and an added position cannot create positive performance.
	lots[1].OriginalPrincipal = "50"
	data = sharpData(lots, map[string]map[time.Time]string{"a": {d0: "0.5", day: "0.5", noon: "0.5", end: "0.5"}})
	assertRates(t, dailyCohortReturns(context.Background(), sharpReplay(t, lots, end), data, end), "0", "0")
}
func TestSharpePartialAndFullWithdrawalsUseGrossProceeds(t *testing.T) {
	d0 := drawdownStart
	day := d0.Add(24 * time.Hour)
	noon := day.Add(12 * time.Hour)
	end := day.Add(24 * time.Hour)
	lots := []consolev2.CohortReplayLot{replayLot("one", "a", "100", "50", "50", d0, replayFlow("sale", "50", "30", noon))}
	data := sharpData(lots, map[string]map[time.Time]string{"a": {d0: "0.5", day: "0.5", noon: "0.6", end: "0.66"}})
	assertRates(t, dailyCohortReturns(context.Background(), sharpReplay(t, lots, end), data, end), "0", "0.32")
	lots[0].RemainingShares = "0"
	lots[0].Dispositions = []consolev2.CohortReplayDisposition{replayFlow("sale", "100", "60", noon)}
	// Full exit has execution evidence and needs no mark after it. Fully unfunded
	// days after closure must not dilute volatility with artificial zero returns.
	data = sharpData(lots, map[string]map[time.Time]string{"a": {d0: "0.5", day: "0.5"}})
	assertRates(t, dailyCohortReturns(context.Background(), sharpReplay(t, lots, end.Add(72*time.Hour)), data, end.Add(72*time.Hour)), "0", "0.2")
}
func TestSharpeNettingLegsAtOneInstantAreAtomic(t *testing.T) {
	d0 := drawdownStart
	day := d0.Add(24 * time.Hour)
	at := day.Add(12 * time.Hour)
	end := day.Add(24 * time.Hour)
	lots := []consolev2.CohortReplayLot{replayLot("old", "a", "100", "50", "0", d0, replayFlow("netting-a", "100", "60", at)), replayLot("new", "b", "100", "40", "0", at, replayFlow("netting-b", "100", "40", at))}
	data := sharpData(lots, map[string]map[time.Time]string{"a": {d0: "0.5", day: "0.5", at: "0.6"}})
	for i := 0; i < 2; i++ {
		assertRates(t, dailyCohortReturns(context.Background(), sharpReplay(t, lots, end), data, end), "0", "0.2")
		lots[0], lots[1] = lots[1], lots[0]
	}
}
func TestSharpeMissingDaysAreNotSkippedAndIncompleteDaysAreExcluded(t *testing.T) {
	d0 := drawdownStart
	end := d0.Add(4 * 24 * time.Hour)
	lots := []consolev2.CohortReplayLot{replayLot("one", "a", "10", "5", "10", d0)}
	data := sharpData(lots, map[string]map[time.Time]string{"a": {d0: "0.5", d0.Add(24 * time.Hour): "0.55", d0.Add(72 * time.Hour): "0.6", end: "0.6"}})
	r := dailyCohortReturns(context.Background(), sharpReplay(t, lots, end), data, end)
	if r.reason != "missing_prices" {
		t.Fatal(r.reason)
	}
	noCoverage := sharpData(lots, map[string]map[time.Time]string{})
	for key, s := range noCoverage.Series {
		s.Coverage = nil
		noCoverage.Series[key] = s
	}
	if r = dailyCohortReturns(context.Background(), sharpReplay(t, lots, end), noCoverage, end); r.reason != "not_synced" {
		t.Fatal(r.reason)
	}
	lots[0].OpenedAt = d0.Add(12 * time.Hour)
	data = sharpData(lots, map[string]map[time.Time]string{"a": {d0.Add(24 * time.Hour): "0.5", d0.Add(48 * time.Hour): "0.55", d0.Add(72 * time.Hour): "0.66", end: "0.594"}})
	assertRates(t, dailyCohortReturns(context.Background(), sharpReplay(t, lots, end), data, end), "0.1", "0.2", "-0.1")
}
func TestSharpeEmptyZeroNAVCancellationAndHistoryErrorsStayUnavailable(t *testing.T) {
	d0 := drawdownStart
	end := d0.Add(72 * time.Hour)
	service := New(nil, nil)
	service.now = func() time.Time { return end }
	empty := CohortMetrics{}
	service.finishSharpe(context.Background(), &empty, drawdownReplay{}, accountpricehistory.Dataset{}, nil, end)
	if empty.SharpAnnual != nil || *empty.SharpAnnualCoverage.Reason != "insufficient_samples" {
		t.Fatal(empty)
	}
	lots := []consolev2.CohortReplayLot{replayLot("free", "a", "10", "0", "10", d0)}
	data := sharpData(lots, map[string]map[time.Time]string{"a": {d0: "0.5"}})
	r := sharpReplay(t, lots, end)
	if result := dailyCohortReturns(context.Background(), r, data, end); result.reason != "nonpositive_nav" {
		t.Fatal(result.reason)
	}
	service = New(nil, nil, historyFixture{data: data})
	service.now = func() time.Time { return end }
	item := CohortMetrics{OpenedAssetCount: 1}
	service.finishSharpe(context.Background(), &item, r, data, errors.New("source unavailable"), end)
	if item.SharpAnnual != nil || *item.SharpAnnualCoverage.Reason != "not_synced" {
		t.Fatal(item)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	service.finishSharpe(ctx, &item, r, data, nil, end)
	if item.SharpAnnual != nil || *item.SharpAnnualCoverage.Reason != "budget_exceeded" {
		t.Fatal(item)
	}
}

func TestCohortSharpeUsesSelectedReplayAndSurvivesMissingLatestQuote(t *testing.T) {
	d0 := drawdownStart
	asOf := d0.Add(84 * time.Hour)
	lot := replayLot("selected", "a", "10", "5", "10", d0)
	data := sharpData([]consolev2.CohortReplayLot{lot}, map[string]map[time.Time]string{"a": {d0: "0.5", d0.Add(24 * time.Hour): "0.55", d0.Add(48 * time.Hour): "0.66", d0.Add(72 * time.Hour): "0.594"}})
	base := fixture(asOf)
	base.Accounts[0].Assets = []consolev2.CoreAssetFact{lot.Asset}
	base.Accounts[0].Assets[0].RemainingCost = "5"
	base.Accounts[0].Assets[0].RemainingShares = "10"
	from, to := d0, d0.Add(time.Hour)
	facts := consolev2.CohortMetricFacts{CoreMetricFacts: base, OpeningFrom: &from, OpeningTo: &to, Cohorts: map[string]consolev2.CohortAccountFact{"pm": {OpenedAssetCount: 1, HoldingAssetCount: 1, BuyFees: "99", SellFees: "88", DrawdownState: "history_unavailable"}}, Replay: map[string]consolev2.CohortReplayFact{"pm": {Lots: []consolev2.CohortReplayLot{lot}}}}
	s := New(cohortReaderFixture{facts: facts}, nil, historyFixture{data: data})
	s.now = func() time.Time { return asOf }
	got, err := s.QueryCohort(context.Background(), consolev2.CohortFilter{OpeningFrom: &from, OpeningTo: &to})
	if err != nil {
		t.Fatal(err)
	}
	item := got.Items[0]
	sharpNumber(t, item.SharpAnnual, 8.33809387832792)
	raw, e := json.Marshal(got)
	if e != nil {
		t.Fatal(e)
	}
	var decoded map[string]any
	if e = json.Unmarshal(raw, &decoded); e != nil {
		t.Fatal(e)
	}
	dto := decoded["items"].([]any)[0].(map[string]any)
	if _, ok := dto["sharp_annual"].(string); !ok {
		t.Fatal("Sharpe must use a decimal string", dto)
	}
	coverage := dto["sharp_annual_coverage"].(map[string]any)
	if coverage["period_seconds"] != float64(86400) || coverage["sample_days"] != float64(3) || coverage["reason"] != nil {
		t.Fatal(coverage)
	}
	if item.TotalPnL != nil || item.SharpAnnualCoverage.SampleDays != 3 || item.BuyFees != "99" || item.SellFees != "88" || !got.OpeningFrom.Equal(from) || !got.OpeningTo.Equal(to) {
		t.Fatal(got)
	}
}
