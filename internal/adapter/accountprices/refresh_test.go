package accountprices

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	h "github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
)

func TestCompletedHourPMRefreshAlwaysFetchesRealAsOfInsteadOfSeed(t *testing.T) {
	at := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	var calls atomic.Int32
	source, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v2/prices-history" || r.URL.Query().Get("as_of") != strconv.FormatInt(at.Unix(), 10) {
			t.Error(r.URL.String())
			http.Error(w, "bad path", 400)
			return
		}
		fmt.Fprintf(w, `{"data":[{"timestamp":%d,"price":0.7,"resolution_seconds":0}],"pagination":{"has_more":false}}`, at.Unix())
	})
	defer close()
	seed := h.Series{Points: []h.Point{{At: at, Price: "0.2", Source: "PM_HISTORY", ResolutionSeconds: 3600}}}
	window := h.Window{Asset: pmAsset(), From: at, To: at, RequiredAt: []time.Time{at}, RefreshAt: &at}
	for range 2 {
		result, e := source.FetchWithSeries(context.Background(), window, seed)
		if e != nil {
			t.Fatal(e)
		}
		point, ok := h.PriceAt(result.Series, at)
		if !ok || point.Price != "0.7" || result.Series.Coverage[0].Reason != "hourly_refresh" {
			t.Fatal(point, result)
		}
	}
	if calls.Load() != 2 {
		t.Fatal("explicit refresh short-circuited against old seed", calls.Load())
	}
}
func TestCompletedHourPMRefreshCannotUseFutureOrOldSeedWhenSourceMissing(t *testing.T) {
	at := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	for _, future := range []bool{false, true} {
		t.Run(strconv.FormatBool(future), func(t *testing.T) {
			source, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v2/resolutions" {
					fmt.Fprint(w, `{"data":[]}`)
					return
				}
				if future {
					fmt.Fprintf(w, `{"data":[{"timestamp":%d,"price":0.9,"resolution_seconds":0}],"pagination":{"has_more":false}}`, at.Add(time.Second).Unix())
				} else {
					fmt.Fprint(w, `{"data":[],"pagination":{"has_more":false}}`)
				}
			})
			defer close()
			seed := h.Series{Points: []h.Point{{At: at, Price: "0.2", Source: "PM_HISTORY", ResolutionSeconds: 3600}}}
			result, e := source.FetchWithSeries(context.Background(), h.Window{Asset: pmAsset(), From: at, To: at, RefreshAt: &at}, seed)
			if future {
				if e == nil {
					t.Fatal("future refresh observation accepted")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			if len(result.Series.Points) != 0 || result.Series.Coverage[0].State != "missing" || result.Series.Coverage[0].Reason == "hourly_refresh" {
				t.Fatal("old seed masqueraded as a successful fresh GET", result)
			}
		})
	}
}
func TestCompletedHourKalshiRefreshReadsTheOfficialHourEvenWithSeed(t *testing.T) {
	at := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	var candles atomic.Int32
	source, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/markets/T":
			fmt.Fprint(w, `{"market":{"ticker":"T","status":"active"}}`)
		case "/historical/cutoff":
			fmt.Fprint(w, `{"market_settled_ts":"2026-09-01T00:00:00Z","trades_created_ts":"2026-09-01T00:00:00Z"}`)
		case "/markets/candlesticks":
			candles.Add(1)
			if r.URL.Query().Get("end_ts") != strconv.FormatInt(at.Unix(), 10) {
				t.Error(r.URL.Query())
			}
			fmt.Fprintf(w, `{"markets":[{"market_ticker":"T","candlesticks":[{"end_period_ts":%d,"price":{"close_dollars":"0.6"}}]}]}`, at.Unix())
		default:
			http.Error(w, "unexpected", 400)
		}
	})
	defer close()
	index := 1
	name := "No"
	asset := h.Asset{MarketSource: "KALSHI", MarketID: "T", TokenID: "kalshi:T:NO", OutcomeIndex: &index, OutcomeName: &name}
	seed := h.Series{Points: []h.Point{{At: at, Price: "0.9", Source: "KALSHI_TRADE_CLOSE", ResolutionSeconds: 3600}}}
	result, e := source.FetchWithSeries(context.Background(), h.Window{Asset: asset, From: at, To: at, RefreshAt: &at}, seed)
	if e != nil {
		t.Fatal(e)
	}
	p, ok := h.PriceAt(result.Series, at)
	if !ok || p.Price != "0.400000000000" || candles.Load() != 1 || result.Series.Coverage[0].Reason != "hourly_refresh" {
		t.Fatal(result, p, candles.Load())
	}
}
