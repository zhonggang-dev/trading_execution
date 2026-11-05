package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	core "github.com/UniPat-AI/trading_execution/internal/service/accountcoremetrics"
	"github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

func cohortReader(f *kalshiNettingFixture) *consolev2.Reader {
	return consolev2.New(f.database, consolev2.RuntimeConfig{Now: func() time.Time { return f.clock }, ManagedAccounts: []consolev2.ManagedAccount{{ExecutionAccountID: f.account, MarketSource: "KALSHI"}}, Bindings: []consolev2.Binding{{ExecutionAccountID: f.account, MarketSource: "KALSHI", Mode: "ACTIVE"}}})
}

func TestPostgresIntegrationCohortRealFillNettingSellAndSettlement(t *testing.T) {
	f := newKalshiNettingFixture(t, "kalshi:cohort-real", "1000")
	outside := f.buy("SAME", "YES", "10", "0.2", "0.1")
	from := f.clock
	inside := f.buy("SAME", "YES", "10", "0.4", "0.2")
	f.buy("NET", "YES", "3", "0.3", "0.03")
	f.buy("NET", "NO", "2", "0.5", "0.02")
	f.buy("ZERO", "YES", "2", "0.4", "0.04")
	f.buy("WIN", "NO", "3", "0.5", "0.06")
	to := f.clock.Add(time.Microsecond)
	f.sell("SAME", "YES", "lot:"+inside.Fill.Key, "4", "0.6", "0.08")
	f.sell("SAME", "YES", "lot:"+outside.Fill.Key, "2", "0.6", "0.04")
	store, err := NewKalshiSettlementStore(f.database, []string{f.account})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []domain.KalshiSettlement{kalshiRecord("ZERO", "no", "2", "0", "0", "0"), kalshiRecord("WIN", "no", "0", "3", "3", "0")} {
		if _, err = store.ApplyKalshiSettlement(context.Background(), f.account, r, f.clock); err != nil {
			t.Fatal(err)
		}
	}
	reader := cohortReader(f)
	filter := consolev2.CohortFilter{ExecutionAccountID: f.account, OpeningFrom: &from, OpeningTo: &to}
	facts, err := reader.CohortMetricFacts(context.Background(), filter)
	if err != nil {
		t.Fatal(err)
	}
	if len(facts.Accounts) != 1 {
		t.Fatal(facts)
	}
	m := facts.Accounts[0]
	c := facts.Cohorts[f.account]
	// SAME, NET Yes, NET No, ZERO Yes, WIN No. Partial closures stay holding.
	if c.OpenedAssetCount != 5 || c.HoldingAssetCount != 2 || c.ClosedAssetCount != 3 {
		t.Fatalf("counts %+v", c)
	}
	assertConsoleAmount(t, c.BuyFees, "0.35")
	assertConsoleAmount(t, c.SellFees, "0.08")
	assertConsoleAmount(t, m.Metrics.CumulativeInvestedCost, "8.2")
	// 2.4-1.6 + (2-(.6+1)) + (0-.8)+(3-1.5) = 1.9, all before fees.
	assertConsoleAmount(t, m.Metrics.RealizedPnL, "1.9")
	// Peak before later disposals: 4 + .9 + 1 -> 5.9 then netting -1.6,
	// then .8 and 1.5 -> 6.6.
	assertConsoleAmount(t, m.Metrics.PeakHoldingCost, "6.6")
	if c.MaxDrawdown != nil || c.DrawdownState != "history_unavailable" {
		t.Fatal("fake drawdown", c)
	}
	found := false
	for _, a := range m.Assets {
		if a.MarketID == "SAME" {
			found = true
			assertConsoleAmount(t, a.RemainingShares, "6")
			assertConsoleAmount(t, a.RemainingCost, "2.4")
		}
	}
	if !found {
		t.Fatal("missing selected SAME")
	}
	// Latest valuation remains independent of the opening interval.
	quotes := cohortQuotes(func(_ context.Context, ts []core.Target) map[string]core.Quote {
		q := map[string]core.Quote{}
		for _, target := range ts {
			q[core.Key(target)] = core.Priced("0.7", "KALSHI_LAST_TRADE", time.Now().UTC(), nil)
		}
		return q
	})
	service := core.New(reader, quotes)
	got, err := service.QueryCohort(context.Background(), filter)
	if err != nil {
		t.Fatal(err)
	}
	item := got.Items[0]
	assertConsoleAmount(t, *item.MarketValue, "4.9")
	assertConsoleAmount(t, item.CurrentCost, "2.7")
	assertConsoleAmount(t, *item.UnrealizedPnL, "2.2")
	assertConsoleAmount(t, *item.TotalPnL, "4.1")
	assertConsoleAmount(t, *item.ROI, "0.5")
	// The same real FillLedger/netting/settlement allocations drive history.
	// SAME marked at .9 reaches gross +5.8 together with NET/other legs;
	// the current endpoint is +4.1, hence the largest drop is 1.7. Fees never
	// enter this curve, and immediately netted legs expose no interim peak.
	history := cohortHistoricalQuotes(func(_ context.Context, windows []accountpricehistory.Window) (accountpricehistory.Dataset, error) {
		data := accountpricehistory.Dataset{Series: map[string]accountpricehistory.Series{}, Version: "real-ledger-history"}
		for _, w := range windows {
			price := "0.5"
			if w.Asset.MarketID == "SAME" {
				price = "0.9"
			}
			data.Series[w.Asset.Key()] = accountpricehistory.Series{Points: []accountpricehistory.Point{{At: from.Add(-time.Minute), Price: price, ResolutionSeconds: 3600}}, Coverage: []accountpricehistory.Coverage{{From: from, To: f.clock, State: "complete"}}}
		}
		return data, nil
	})
	withHistory, err := core.New(reader, quotes, history).QueryCohort(context.Background(), filter)
	if err != nil || len(withHistory.Items) != 1 || withHistory.Items[0].DrawdownState != "available" || withHistory.Items[0].MaxDrawdown == nil {
		t.Fatal("real cohort drawdown unavailable", withHistory, err)
	}
	assertConsoleAmount(t, *withHistory.Items[0].TotalPnL, "4.1")
	assertConsoleAmount(t, *withHistory.Items[0].MaxDrawdown, "1.7")
	assertConsoleAmount(t, withHistory.Items[0].BuyFees, "0.35")
	assertConsoleAmount(t, withHistory.Items[0].SellFees, "0.08")
	raw, _ := json.Marshal(got)
	if strings.Contains(string(raw), "key-kalshi") || strings.Contains(string(raw), "FundingAddress") {
		t.Fatal("private identity leaked")
	}
	// The actual fee-inclusive ledger was not rewritten by a read.
	assertConsoleAmount(t, scalar(t, f.database, `SELECT remaining_cost::text FROM position_lots WHERE lot_id=$1`, "lot:"+inside.Fill.Key), "2.52")
	// An exact opening-to boundary excludes the lot at that time.
	boundary := inside.Fill.MatchedAt
	empty, err := reader.CohortMetricFacts(context.Background(), consolev2.CohortFilter{ExecutionAccountID: f.account, OpeningFrom: &boundary, OpeningTo: &boundary})
	if err == nil {
		t.Fatal("zero-width accepted", empty)
	}
}

type cohortQuotes func(context.Context, []core.Target) map[string]core.Quote

func (f cohortQuotes) Quotes(ctx context.Context, t []core.Target) map[string]core.Quote {
	return f(ctx, t)
}

type cohortHistoricalQuotes func(context.Context, []accountpricehistory.Window) (accountpricehistory.Dataset, error)

func (f cohortHistoricalQuotes) Load(ctx context.Context, windows []accountpricehistory.Window) (accountpricehistory.Dataset, error) {
	return f(ctx, windows)
}

func TestPostgresIntegrationCohortSeparatesOppositeOutsideLots(t *testing.T) {
	f := newKalshiNettingFixture(t, "kalshi:cohort-cross-net", "100")
	f.buy("CROSS", "YES", "4", "0.3", "0.04")
	from := f.clock
	f.buy("CROSS", "NO", "4", "0.5", "0.08")
	to := f.clock.Add(time.Microsecond)
	reader := cohortReader(f)
	got, err := reader.CohortMetricFacts(context.Background(), consolev2.CohortFilter{ExecutionAccountID: f.account, OpeningFrom: &from, OpeningTo: &to})
	if err != nil {
		t.Fatal(err)
	}
	c := got.Cohorts[f.account]
	if c.OpenedAssetCount != 1 || c.HoldingAssetCount != 0 || c.ClosedAssetCount != 1 {
		t.Fatal(c)
	}
	assertConsoleAmount(t, c.BuyFees, "0.08")
	assertConsoleAmount(t, c.SellFees, "0")
	assertConsoleAmount(t, got.Accounts[0].Metrics.CumulativeInvestedCost, "2")
	// The recorded cost-based allocation is retained rather than assigning the
	// entire $4 paired payout to the selected NO leg.
	payout := scalar(t, f.database, `SELECT allocated_net_proceeds::text FROM position_lot_closures c JOIN position_lots l USING(lot_id) WHERE l.token_id='kalshi:CROSS:NO'`)
	want := newDecimalDifference(t, payout, "2")
	assertConsoleAmount(t, got.Accounts[0].Metrics.RealizedPnL, want)
	if _, err = f.database.Exec(`UPDATE position_lot_closures SET allocated_net_proceeds=allocated_net_proceeds+0.01 WHERE lot_id IN (SELECT lot_id FROM position_lots WHERE token_id='kalshi:CROSS:YES')`); err != nil {
		t.Fatal(err)
	}
	// The corrupted other leg is outside the selected opening interval, but it
	// still breaks the paired cash evidence for this cohort's NO allocation.
	if _, err = reader.CohortMetricFacts(context.Background(), consolev2.CohortFilter{ExecutionAccountID: f.account, OpeningFrom: &from, OpeningTo: &to}); !errors.Is(err, consolev2.ErrUnavailable) {
		t.Fatal("broken outside netting leg accepted", err)
	}
}
func newDecimalDifference(t *testing.T, a, b string) string {
	t.Helper()
	x, _ := domain.ParseDecimal(a)
	y, _ := domain.ParseDecimal(b)
	xr, _ := x.Multiply("1")
	yr, _ := y.Multiply("1")
	return xr.Sub(xr, yr).FloatString(24)
}
