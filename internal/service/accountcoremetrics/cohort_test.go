package accountcoremetrics

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

type cohortReaderFixture struct {
	facts consolev2.CohortMetricFacts
	err   error
}

func (f cohortReaderFixture) CoreMetricFacts(context.Context, string) (consolev2.CoreMetricFacts, error) {
	return f.facts.CoreMetricFacts, f.err
}
func (f cohortReaderFixture) CohortMetricFacts(context.Context, consolev2.CohortFilter) (consolev2.CohortMetricFacts, error) {
	return f.facts, f.err
}
func TestCohortValuationReusesQuotesAndGrossROI(t *testing.T) {
	now := time.Now().UTC()
	base := fixture(now)
	base.Accounts[0].Metrics.CumulativeInvestedCost = "10"
	base.Accounts[0].Metrics.RealizedPnL = "2"
	base.Accounts[0].Assets[0].RemainingCost = "1"
	facts := consolev2.CohortMetricFacts{CoreMetricFacts: base, Cohorts: map[string]consolev2.CohortAccountFact{"pm": {OpenedAssetCount: 2, HoldingAssetCount: 1, ClosedAssetCount: 1, BuyFees: "0.1", SellFees: "0.2", DrawdownState: "history_unavailable"}}}
	s := New(cohortReaderFixture{facts: facts}, quotesFunc(func(_ context.Context, ts []Target) map[string]Quote {
		return map[string]Quote{Key(ts[0]): Priced("0.5", "PM_CURRENT_PRICE", now, nil)}
	}))
	got, err := s.QueryCohort(context.Background(), consolev2.CohortFilter{})
	if err != nil {
		t.Fatal(err)
	}
	m := got.Items[0]
	if *m.MarketValue != "1.5" || *m.UnrealizedPnL != "0.5" || *m.TotalPnL != "2.5" || *m.ROI != "0.250000000000000000000000" || m.BuyFees != "0.1" || m.SellFees != "0.2" || m.MaxDrawdown != nil {
		t.Fatal(m)
	}
	raw, _ := json.Marshal(got)
	if strings.Contains(string(raw), "private-address") {
		t.Fatal("private identity leaked")
	}
	s = New(cohortReaderFixture{facts: facts}, nil)
	unknown, err := s.QueryCohort(context.Background(), consolev2.CohortFilter{})
	if err != nil {
		t.Fatal(err)
	}
	u := unknown.Items[0]
	if u.MarketValue != nil || u.UnrealizedPnL != nil || u.TotalPnL != nil || u.ReturnRate != nil || u.ROI != nil || u.RealizedPnL != "2" || u.BuyFees != "0.1" {
		t.Fatal("missing quote must preserve financial facts but no partial valuation", u)
	}
	facts.Cohorts["pm"] = consolev2.CohortAccountFact{OpenedAssetCount: 1, HoldingAssetCount: 0, BuyFees: "0", SellFees: "0"}
	s = New(cohortReaderFixture{facts: facts}, nil)
	if _, err = s.QueryCohort(context.Background(), consolev2.CohortFilter{}); !errors.Is(err, consolev2.ErrUnavailable) {
		t.Fatal("invalid count identity accepted", err)
	}
}

func TestCohortUnavailableAccountsHaveNoGuessedFinancialValues(t *testing.T) {
	now := time.Now().UTC()
	facts := consolev2.CohortMetricFacts{CoreMetricFacts: consolev2.CoreMetricFacts{ObservedAt: now, Accounts: []consolev2.CoreAccountFact{}}, Cohorts: map[string]consolev2.CohortAccountFact{}, UnavailableAccounts: []consolev2.CohortUnavailableAccount{{ExecutionAccountID: "retired", MarketSource: "POLYMARKET", Reason: "missing_disposition_evidence"}}}
	s := New(cohortReaderFixture{facts: facts}, nil)
	got, err := s.QueryCohort(context.Background(), consolev2.CohortFilter{ExecutionAccountID: "retired"})
	if err != nil || len(got.Items) != 0 || len(got.UnavailableAccounts) != 1 {
		t.Fatal(got, err)
	}
	raw, _ := json.Marshal(got)
	if strings.Contains(string(raw), "realized_pnl") || strings.Contains(string(raw), "market_value") {
		t.Fatal("financial amounts fabricated for unavailable account", string(raw))
	}
	facts.UnavailableAccounts = append(facts.UnavailableAccounts, facts.UnavailableAccounts[0])
	s = New(cohortReaderFixture{facts: facts}, nil)
	if _, err = s.QueryCohort(context.Background(), consolev2.CohortFilter{}); !errors.Is(err, consolev2.ErrUnavailable) {
		t.Fatal("duplicate warnings accepted", err)
	}
}

func TestCohortPastSnapshotValuesFromHistoryAtRangeEnd(t *testing.T) {
	asOf := drawdownStart.Add(3 * time.Hour)
	lot := replayLot("one", "a", "10", "5", "10", drawdownStart)
	asset := lot.Asset
	asset.RemainingShares, asset.RemainingCost = "10", "5"
	facts := consolev2.CohortMetricFacts{
		CoreMetricFacts: consolev2.CoreMetricFacts{ObservedAt: drawdownStart.Add(10 * time.Hour), Accounts: []consolev2.CoreAccountFact{{Metrics: consolev2.AccountPerformance{ExecutionAccountID: "pm", MarketSource: "POLYMARKET", PeakHoldingCost: "5", CumulativeInvestedCost: "5", RealizedPnL: "0", RealizedDrawdown14D: "0"}, Assets: []consolev2.CoreAssetFact{asset}}}},
		AsOf:            asOf,
		Cohorts:         map[string]consolev2.CohortAccountFact{"pm": {OpenedAssetCount: 1, HoldingAssetCount: 1, BuyFees: "0", SellFees: "0", DrawdownState: "history_unavailable"}},
		Replay:          map[string]consolev2.CohortReplayFact{"pm": {Lots: []consolev2.CohortReplayLot{lot}}},
	}
	live := quotesFunc(func(context.Context, []Target) map[string]Quote {
		t.Fatal("a past snapshot read a current quote")
		return nil
	})
	data := replayDataset([]consolev2.CohortReplayLot{lot}, map[string][]string{"a": {"0.5", "0.9", "0.8", "0.6", "0.99"}})
	s := New(cohortReaderFixture{facts: facts}, live, historyFixture{data: data})
	s.now = func() time.Time { return facts.ObservedAt }
	got, err := s.QueryCohort(context.Background(), consolev2.CohortFilter{})
	if err != nil || !got.AsOf.Equal(asOf) || len(got.Items) != 1 {
		t.Fatal(got, err)
	}
	m := got.Items[0]
	a := m.ValuationAssets[0]
	if a.PriceSource == nil || *a.PriceSource != "HISTORY_HOURLY" || !a.ObservedAt.Equal(asOf) {
		t.Fatal("snapshot price was not the hourly history at the range end", a)
	}
	drawdownAmount(t, *m.MarketValue, "6")
	drawdownAmount(t, *m.TotalPnL, "1")
	// Peak +4 at hour 1, then +1 at the range end; the later 0.99 is ignored.
	if m.MaxDrawdown == nil || m.DrawdownCoverage == nil || !m.DrawdownCoverage.To.Equal(asOf) {
		t.Fatal(m.DrawdownCoverage)
	}
	drawdownAmount(t, *m.MaxDrawdown, "3")

	// No eligible point within an hour of a fetched instant is a real gap.
	facts.AsOf = drawdownStart.Add(6 * time.Hour)
	s = New(cohortReaderFixture{facts: facts}, live, historyFixture{data: data})
	s.now = func() time.Time { return facts.ObservedAt }
	gap, err := s.QueryCohort(context.Background(), consolev2.CohortFilter{})
	if err != nil {
		t.Fatal(err)
	}
	g := gap.Items[0]
	if g.TotalPnL != nil || g.MarketValue != nil || g.ValuationAssets[0].Reason == nil || *g.ValuationAssets[0].Reason != "history_missing" || g.RealizedPnL != "0" {
		t.Fatal("missing history must leave valuation unknown", g)
	}

	// Without history the snapshot is unknown, never a current quote.
	s = New(cohortReaderFixture{facts: facts}, live)
	s.now = func() time.Time { return facts.ObservedAt }
	unsynced, err := s.QueryCohort(context.Background(), consolev2.CohortFilter{})
	if err != nil || unsynced.Items[0].ValuationAssets[0].Reason == nil || *unsynced.Items[0].ValuationAssets[0].Reason != "history_not_synced" {
		t.Fatal(unsynced, err)
	}
}
