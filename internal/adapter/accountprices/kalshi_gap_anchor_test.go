package accountprices

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	h "github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
)

func kalshiNoAsset() h.Asset {
	index := 1
	name := "No"
	return h.Asset{MarketSource: "KALSHI", MarketID: "KXNETFLIXRANKSHOWGLOBAL2-26NOV23-BLA",
		TokenID: "kalshi:KXNETFLIXRANKSHOWGLOBAL2-26NOV23-BLA:NO", OutcomeIndex: &index, OutcomeName: &name}
}

func kalshiTradesFixture(t *testing.T, trades string) (*Source, func()) {
	t.Helper()
	return fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/historical/cutoff":
			fmt.Fprint(w, `{"market_settled_ts":"2026-09-01T00:00:00Z","trades_created_ts":"2026-09-01T00:00:00Z"}`)
		case "/markets/trades":
			fmt.Fprintf(w, `{"trades":[%s],"cursor":""}`, trades)
		default:
			t.Error("warm Kalshi repair must only read trades", r.URL.Path)
			http.Error(w, "bad path", 400)
		}
	})
}

func mergedPriceAt(seed h.Series, result h.Result, at time.Time) (h.Point, bool) {
	merged := h.Series{Points: append(append([]h.Point(nil), seed.Points...), result.Series.Points...)}
	h.SortSeries(&merged)
	return h.PriceAt(merged, at)
}

// Reproduces 2026-11-20 14:24:38Z on KXNETFLIXRANKSHOWGLOBAL2-26NOV23-BLA:NO.
// The anchor is a public trade at 13:04:25.022815 whose instant also had a
// second print. The gap read returned both prints at the anchor instant, the
// new same-instant points shadowed the anchor's extended validity, and the
// required sample stayed missing on every hourly run.
func TestRequiredKalshiGapIgnoresTradesAtTheAnchorInstant(t *testing.T) {
	anchorAt := time.Date(2026, 11, 20, 13, 4, 25, 22815000, time.UTC)
	required := time.Date(2026, 11, 20, 14, 24, 38, 547450000, time.UTC)
	stamp := anchorAt.Format(time.RFC3339Nano)
	source, close := kalshiTradesFixture(t, fmt.Sprintf(
		`{"trade_id":"a","ticker":"KXNETFLIXRANKSHOWGLOBAL2-26NOV23-BLA","created_time":"%s","yes_price_dollars":"0.82"},`+
			`{"trade_id":"b","ticker":"KXNETFLIXRANKSHOWGLOBAL2-26NOV23-BLA","created_time":"%s","yes_price_dollars":"0.83"}`, stamp, stamp))
	defer close()
	until := anchorAt
	seed := h.Series{Points: []h.Point{
		{At: anchorAt, Price: "0.170000000000", Source: "KALSHI_PUBLIC_TRADE", SourceAt: &until, ValidUntil: &until},
		{At: time.Date(2026, 11, 20, 15, 0, 0, 0, time.UTC), Price: "0.170000000000", ResolutionSeconds: 3600, Source: "KALSHI_PREVIOUS_TRADE"},
	}}
	h.SortSeries(&seed)
	if _, ok := h.PriceAt(seed, required); ok {
		t.Fatal("production gap not reproduced")
	}
	result, err := source.FetchWithSeries(context.Background(), h.Window{Asset: kalshiNoAsset(), From: required, To: required, RequiredAt: []time.Time{required}}, seed)
	if err != nil {
		t.Fatal(err)
	}
	if result.Series.Coverage[0].State != "complete" {
		t.Fatalf("coverage = %+v, want complete", result.Series.Coverage)
	}
	if len(result.Series.Points) != 1 || !result.Series.Points[0].At.Equal(anchorAt) {
		t.Fatalf("points = %+v, want only the extended anchor", result.Series.Points)
	}
	if point, ok := mergedPriceAt(seed, result, required); !ok || point.Price != "0.170000000000" {
		t.Fatalf("PriceAt = %+v/%v, want the anchor price carried by no-trade proof", point, ok)
	}
}

func TestRequiredKalshiGapKeepsOnePointPerTradeInstant(t *testing.T) {
	anchorAt := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	tradeAt := time.Date(2026, 11, 20, 12, 30, 0, 500000000, time.UTC)
	required := time.Date(2026, 11, 20, 13, 30, 0, 0, time.UTC)
	stamp := tradeAt.Format(time.RFC3339Nano)
	source, close := kalshiTradesFixture(t, fmt.Sprintf(
		`{"trade_id":"x","ticker":"KXNETFLIXRANKSHOWGLOBAL2-26NOV23-BLA","created_time":"%s","yes_price_dollars":"0.40"},`+
			`{"trade_id":"y","ticker":"KXNETFLIXRANKSHOWGLOBAL2-26NOV23-BLA","created_time":"%s","yes_price_dollars":"0.41"}`, stamp, stamp))
	defer close()
	seed := h.Series{Points: []h.Point{{At: anchorAt, Price: "0.7", ResolutionSeconds: 3600, Source: "KALSHI_TRADE_CLOSE"}}}
	result, err := source.FetchWithSeries(context.Background(), h.Window{Asset: kalshiNoAsset(), From: required, To: required, RequiredAt: []time.Time{required}}, seed)
	if err != nil {
		t.Fatal(err)
	}
	same := 0
	for _, p := range result.Series.Points {
		if p.At.Equal(tradeAt) {
			same++
		}
	}
	if same != 1 || result.Series.Coverage[0].State != "complete" {
		t.Fatalf("same-instant points = %d coverage = %+v, want one point and complete", same, result.Series.Coverage)
	}
	if point, ok := mergedPriceAt(seed, result, required); !ok || point.Price != "0.600000000000" {
		t.Fatalf("PriceAt = %+v/%v, want the first listed print 0.60 (NO side)", point, ok)
	}
}
