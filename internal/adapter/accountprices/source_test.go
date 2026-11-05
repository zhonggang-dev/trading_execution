package accountprices

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	h "github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
)

func fixtureSource(t *testing.T, handler http.HandlerFunc) (*Source, func()) {
	t.Helper()
	server := httptest.NewServer(handler)
	source, e := New(Params{PMBaseURL: server.URL, GammaBaseURL: server.URL, KalshiBaseURL: server.URL, RequestInterval: time.Nanosecond, Now: func() time.Time { return time.Unix(2000000000, 0).UTC() }})
	if e != nil {
		t.Fatal(e)
	}
	return source, server.Close
}
func pmAsset() h.Asset {
	i := 1
	n := "No"
	return h.Asset{MarketSource: "POLYMARKET", MarketID: "1", ConditionID: "condition", TokenID: "no-token", OutcomeIndex: &i, OutcomeName: &n}
}
func TestPMFullCursorWalkAndBoundaryLookback(t *testing.T) {
	at := time.Unix(1793206800, 0).UTC()
	var pages atomic.Int32
	s, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Error(r.Method)
		}
		if r.URL.Path != "/v2/prices-history" {
			t.Error(r.URL.Path)
			http.Error(w, "unexpected", 400)
			return
		}
		pages.Add(1)
		if r.URL.Query().Get("cursor") == "" {
			fmt.Fprintf(w, `{"data":[{"timestamp":%d,"price":0.2,"resolution_seconds":3600}],"pagination":{"has_more":true,"next_cursor":"next"}}`, at.Unix())
		} else {
			fmt.Fprintf(w, `{"data":[{"timestamp":%d,"price":0.4,"resolution_seconds":3600}],"pagination":{"has_more":false}}`, at.Add(time.Hour).Unix())
		}
	})
	defer close()
	result, e := s.Fetch(context.Background(), h.Window{Asset: pmAsset(), From: at, To: at.Add(time.Hour)})
	if e != nil || pages.Load() != 2 || result.Series.Coverage[0].State != "complete" {
		t.Fatal(result, e, pages.Load())
	}
	if p, ok := h.PriceAt(result.Series, at.Add(30*time.Minute)); !ok || p.Price != "0.2" {
		t.Fatal(p, ok)
	}
}
func TestPMRejectsRepeatedCursorAndMalformedPrices(t *testing.T) {
	for _, price := range []string{"0.5", "2"} {
		t.Run(price, func(t *testing.T) {
			s, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, `{"data":[{"timestamp":1000,"price":%s,"resolution_seconds":3600}],"pagination":{"has_more":true,"next_cursor":"same"}}`, price)
			})
			defer close()
			if _, e := s.Fetch(context.Background(), h.Window{Asset: pmAsset(), From: time.Unix(1000, 0), To: time.Unix(2000, 0)}); e == nil {
				t.Fatal("accepted invalid/repeated response")
			}
		})
	}
}
func TestPMSettlementRequiresExactIdentityAndDoesNotTravelBackwards(t *testing.T) {
	at := time.Unix(1793206800, 0).UTC()
	for _, token := range []string{"no-token", "wrong-token"} {
		t.Run(token, func(t *testing.T) {
			s, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v2/prices-history":
					fmt.Fprint(w, `{"data":[],"pagination":{"has_more":false}}`)
				case "/v2/resolutions":
					fmt.Fprintf(w, `{"data":[{"condition_id":"condition","status":"resolved","transaction_hash":"0x%s","log_index":"1","last_update_timestamp":"%d"}]}`, strings.Repeat("a", 64), at.Unix())
				case "/markets/1":
					_ = json.NewEncoder(w).Encode(map[string]any{"id": "1", "conditionId": "condition", "closed": true, "umaResolutionStatus": "resolved", "clobTokenIds": `["yes-token","` + token + `"]`, "outcomePrices": `["0","1"]`, "outcomes": `["Yes","No"]`})
				default:
					http.Error(w, "bad path", 400)
				}
			})
			defer close()
			result, e := s.Fetch(context.Background(), h.Window{Asset: pmAsset(), From: at.Add(-time.Hour), To: at.Add(time.Hour)})
			if token == "wrong-token" {
				if e == nil {
					t.Fatal("wrong token accepted")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			if _, ok := h.PriceAt(result.Series, at.Add(-time.Second)); ok {
				t.Fatal("future settlement leaked")
			}
			if p, ok := h.PriceAt(result.Series, at); !ok || p.Price != "1" {
				t.Fatal(p, ok)
			}
		})
	}
}
func TestKalshiNoTradeProofNoConversionAndFailure(t *testing.T) {
	at := time.Unix(1793206800, 0).UTC()
	for _, fail := range []bool{false, true} {
		t.Run(strconv.FormatBool(fail), func(t *testing.T) {
			var trades atomic.Int32
			s, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/markets/T":
					fmt.Fprint(w, `{"market":{"ticker":"T","status":"active"}}`)
				case "/historical/cutoff":
					fmt.Fprint(w, `{"market_settled_ts":"2026-01-01T00:00:00Z","trades_created_ts":"2026-01-01T00:00:00Z"}`)
				case "/markets/candlesticks":
					fmt.Fprintf(w, `{"markets":[{"market_ticker":"T","candlesticks":[{"end_period_ts":%d,"price":{"close_dollars":"0.2"}}]}]}`, at.Unix())
				case "/markets/trades":
					trades.Add(1)
					if fail {
						http.Error(w, "temporary", 400)
					} else {
						fmt.Fprint(w, `{"trades":[],"cursor":""}`)
					}
				default:
					http.Error(w, "bad path", 400)
				}
			})
			defer close()
			i := 1
			n := "No"
			a := h.Asset{MarketSource: "KALSHI", MarketID: "T", TokenID: "kalshi:T:NO", OutcomeIndex: &i, OutcomeName: &n}
			result, e := s.Fetch(context.Background(), h.Window{Asset: a, From: at, To: at.Add(4 * time.Hour)})
			if trades.Load() != 1 {
				t.Fatal("gap was not checked", trades.Load())
			}
			if fail {
				if e == nil {
					t.Fatal("failed read turned into empty trades")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			p, ok := h.PriceAt(result.Series, at.Add(3*time.Hour))
			if !ok || p.Price != "0.800000000000" || result.Series.Coverage[0].State != "complete" {
				t.Fatal(p, ok, result.Series)
			}
		})
	}
}
func TestKalshiArchivedMarketUsesHistoricalCandles(t *testing.T) {
	at := time.Unix(1793206800, 0).UTC()
	var historical atomic.Bool
	s, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/markets/T":
			http.NotFound(w, r)
		case "/historical/markets/T":
			fmt.Fprint(w, `{"market":{"ticker":"T","status":"active"}}`)
		case "/historical/cutoff":
			fmt.Fprint(w, `{"market_settled_ts":"2026-01-01T00:00:00Z","trades_created_ts":"2026-01-01T00:00:00Z"}`)
		case "/historical/markets/T/candlesticks":
			historical.Store(true)
			fmt.Fprintf(w, `{"ticker":"T","candlesticks":[{"end_period_ts":%d,"price":{"previous":"0.3"}}]}`, at.Unix())
		default:
			http.Error(w, "bad path", 400)
		}
	})
	defer close()
	i := 0
	n := "Yes"
	a := h.Asset{MarketSource: "KALSHI", MarketID: "T", TokenID: "kalshi:T:YES", OutcomeIndex: &i, OutcomeName: &n}
	result, e := s.Fetch(context.Background(), h.Window{Asset: a, From: at, To: at.Add(time.Hour)})
	if e != nil || !historical.Load() || result.Series.Coverage[0].State != "complete" {
		t.Fatal(result, e)
	}
}
func TestSourceCancellation(t *testing.T) {
	s, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() })
	defer close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := s.Fetch(ctx, h.Window{Asset: pmAsset(), From: time.Unix(1000, 0), To: time.Unix(2000, 0)}); e == nil {
		t.Fatal("cancel ignored")
	}
}

func TestKalshiTradeArchiveCutoffIndependentOfLiveMarket(t *testing.T) {
	at := time.Unix(1793206800, 0).UTC()
	cutoff := at.Add(3 * time.Hour)
	var historical, live atomic.Int32
	s, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/markets/T":
			fmt.Fprint(w, `{"market":{"ticker":"T","status":"active"}}`)
		case "/historical/cutoff":
			fmt.Fprintf(w, `{"market_settled_ts":"2026-01-01T00:00:00Z","trades_created_ts":"%s"}`, cutoff.Format(time.RFC3339))
		case "/markets/candlesticks":
			fmt.Fprintf(w, `{"markets":[{"market_ticker":"T","candlesticks":[{"end_period_ts":%d,"price":{"close_dollars":"0.2"}}]}]}`, at.Unix())
		case "/historical/trades":
			historical.Add(1)
			fmt.Fprintf(w, `{"trades":[{"trade_id":"archived","ticker":"T","created_time":"%s","yes_price_dollars":"0.6"}],"cursor":""}`, at.Add(2*time.Hour).Format(time.RFC3339))
		case "/markets/trades":
			live.Add(1)
			fmt.Fprint(w, `{"trades":[],"cursor":""}`)
		default:
			http.Error(w, "bad path", 400)
		}
	})
	defer close()
	i := 1
	n := "No"
	asset := h.Asset{MarketSource: "KALSHI", MarketID: "T", TokenID: "kalshi:T:NO", OutcomeIndex: &i, OutcomeName: &n}
	result, e := s.Fetch(context.Background(), h.Window{Asset: asset, From: at, To: at.Add(5 * time.Hour)})
	if e != nil {
		t.Fatal(e)
	}
	if historical.Load() != 1 || live.Load() != 1 {
		t.Fatal("independent trade archive was not consulted", historical.Load(), live.Load())
	}
	p, ok := h.PriceAt(result.Series, at.Add(4*time.Hour))
	if !ok || p.Price != "0.400000000000" {
		t.Fatal("archived trade was incorrectly treated as no trade", p, ok)
	}
}

func TestKalshiTradeArchiveHandoffRechecksNoTradeProof(t *testing.T) {
	at := time.Unix(1793206800, 0).UTC()
	var advanced atomic.Bool
	var historical, live, cutoffs atomic.Int32
	s, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/markets/T":
			fmt.Fprint(w, `{"market":{"ticker":"T","status":"active"}}`)
		case "/historical/cutoff":
			cutoffs.Add(1)
			cutoff := at.Add(-time.Hour)
			if advanced.Load() {
				cutoff = at.Add(2 * time.Hour)
			}
			fmt.Fprintf(w, `{"market_settled_ts":"2026-01-01T00:00:00Z","trades_created_ts":"%s"}`, cutoff.Format(time.RFC3339))
		case "/markets/candlesticks":
			fmt.Fprintf(w, `{"markets":[{"market_ticker":"T","candlesticks":[{"end_period_ts":%d,"price":{"close_dollars":"0.2"}}]}]}`, at.Unix())
		case "/markets/trades":
			live.Add(1)
			// The first query reached the live tier after the trade moved out.
			advanced.Store(true)
			fmt.Fprint(w, `{"trades":[],"cursor":""}`)
		case "/historical/trades":
			historical.Add(1)
			fmt.Fprintf(w, `{"trades":[{"trade_id":"moved","ticker":"T","created_time":"%s","yes_price_dollars":"0.7"}],"cursor":""}`, at.Add(time.Hour).Format(time.RFC3339))
		default:
			http.Error(w, "bad path", 400)
		}
	})
	defer close()
	i, name := 0, "Yes"
	asset := h.Asset{MarketSource: "KALSHI", MarketID: "T", TokenID: "kalshi:T:YES", OutcomeIndex: &i, OutcomeName: &name}
	result, err := s.Fetch(context.Background(), h.Window{Asset: asset, From: at, To: at.Add(4 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if historical.Load() != 1 || live.Load() != 2 || cutoffs.Load() != 5 {
		t.Fatal("archive movement did not cause a complete retry", historical.Load(), live.Load(), cutoffs.Load())
	}
	point, ok := h.PriceAt(result.Series, at.Add(3*time.Hour))
	if !ok || point.Price != "0.7" {
		t.Fatal("empty live tier incorrectly preserved the old trade", point, ok)
	}
}

func TestKalshiTradeArchiveUnstableHandoffIsBoundedFailure(t *testing.T) {
	at := time.Unix(1793206800, 0).UTC()
	var cutoffs atomic.Int32
	s, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/markets/T":
			fmt.Fprint(w, `{"market":{"ticker":"T","status":"active"}}`)
		case "/historical/cutoff":
			n := cutoffs.Add(1)
			fmt.Fprintf(w, `{"market_settled_ts":"2026-01-01T00:00:00Z","trades_created_ts":"%s"}`, at.Add(time.Duration(n)*time.Second).Format(time.RFC3339))
		case "/markets/candlesticks":
			fmt.Fprintf(w, `{"markets":[{"market_ticker":"T","candlesticks":[{"end_period_ts":%d,"price":{"close_dollars":"0.2"}}]}]}`, at.Unix())
		case "/markets/trades", "/historical/trades":
			fmt.Fprint(w, `{"trades":[],"cursor":""}`)
		default:
			http.Error(w, "bad path", 400)
		}
	})
	defer close()
	i, name := 0, "Yes"
	asset := h.Asset{MarketSource: "KALSHI", MarketID: "T", TokenID: "kalshi:T:YES", OutcomeIndex: &i, OutcomeName: &name}
	_, err := s.Fetch(context.Background(), h.Window{Asset: asset, From: at, To: at.Add(4 * time.Hour)})
	if err == nil || !strings.Contains(err.Error(), "archive changed") || cutoffs.Load() != 7 {
		t.Fatal("unstable archive produced no-trade evidence or unbounded retries", err, cutoffs.Load())
	}
}

func TestPMScientificJSONPriceBecomesExactInternalDecimal(t *testing.T) {
	at := time.Unix(1793206800, 0).UTC()
	s, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"data":[{"timestamp":%d,"price":1e-4,"resolution_seconds":3600}],"pagination":{"has_more":false}}`, at.Unix())
	})
	defer close()
	r, e := s.Fetch(context.Background(), h.Window{Asset: pmAsset(), From: at, To: at.Add(time.Hour)})
	if e != nil {
		t.Fatal(e)
	}
	p, ok := h.PriceAt(r.Series, at)
	if !ok || p.Price != "0.0001" {
		t.Fatal(p, ok)
	}
	for raw, want := range map[string]string{"1e-40": "0." + strings.Repeat("0", 39) + "1", "1.23000e-2": "0.0123", "0.000": "0", "1e0": "1"} {
		got, e := canonicalJSONPrice(json.Number(raw))
		if e != nil || got != want {
			t.Fatal(raw, got, want, e)
		}
	}
}

func TestPMClampedSettlementTerminalIsNotHistoricalPrice(t *testing.T) {
	at := time.Unix(1793206800, 0).UTC()
	end := at.Add(2 * time.Hour)
	var asOf atomic.Int32
	s, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/prices-history" {
			t.Error("normal history should suffice after real boundary lookup", r.URL.Path)
			http.Error(w, "bad path", 400)
			return
		}
		if r.URL.Query().Get("as_of") != "" {
			asOf.Add(1)
			fmt.Fprintf(w, `{"data":[{"timestamp":%d,"price":0.4,"resolution_seconds":0}],"pagination":{"has_more":false}}`, end.Unix())
			return
		}
		fmt.Fprintf(w, `{"data":[{"timestamp":%d,"price":0.4,"resolution_seconds":3600},{"timestamp":%d,"price":1,"resolution_seconds":0}],"pagination":{"has_more":false}}`, at.Unix(), end.Unix())
	})
	defer close()
	result, e := s.Fetch(context.Background(), h.Window{Asset: pmAsset(), From: at, To: end})
	if e != nil {
		t.Fatal(e)
	}
	p, ok := h.PriceAt(result.Series, end)
	if !ok || p.Price != "0.4" || asOf.Load() != 1 {
		t.Fatal("synthetic settled terminal traveled into pre-settlement window", p, ok, asOf.Load())
	}
	for _, p := range result.Series.Points {
		if p.Price == "1" {
			t.Fatal("clamped terminal persisted as historical fact")
		}
	}
}

func TestPMKnownLocalSettlementsPreserveRealPreSettlementPrices(t *testing.T) {
	parse := func(v string) time.Time {
		at, e := time.Parse(time.RFC3339Nano, v)
		if e != nil {
			t.Fatal(e)
		}
		return at
	}
	cases := []struct{ name, from, confirmed, to, price string }{
		{"G2 Esports", "2026-09-29T11:50:26Z", "2026-09-29T12:23:35.362096Z", "2026-09-29T13:23:01.311128Z", "0"},
		{"Paper Rex", "2026-09-29T09:20:27Z", "2026-09-29T12:23:35.362096Z", "2026-09-29T13:23:01.311128Z", "1"},
		{"Navalnaya No", "2026-10-09T06:50:33Z", "2026-10-09T12:13:31.735869Z", "2026-10-19T04:29:24.160397Z", "1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			from, confirmed, to := parse(tc.from), parse(tc.confirmed), parse(tc.to)
			var hits atomic.Int32
			s, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				if r.URL.Path != "/v2/prices-history" || r.URL.Query().Get("as_of") != "" {
					t.Error("known finalization must avoid demanding missing network tx evidence", r.URL.Path)
					http.Error(w, "unexpected endpoint", 400)
					return
				}
				data := []map[string]any{}
				for at := from.Truncate(time.Hour); at.Before(confirmed); at = at.Add(time.Hour) {
					data = append(data, map[string]any{"timestamp": at.Unix(), "price": 0.4, "resolution_seconds": 3600})
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"data": data, "pagination": map[string]any{"has_more": false}})
			})
			defer close()
			known := h.Settlement{EffectiveAt: confirmed, Price: tc.price, Source: "OFFICIAL_SETTLEMENT", SourceReference: "LOCAL_POSITION:exact-account:exact-token:POLYMARKET_DATA_API:condition", FetchedAt: confirmed}
			result, e := s.FetchWithSettlements(context.Background(), h.Window{Asset: pmAsset(), From: from, To: to}, []h.Settlement{known})
			if e != nil {
				t.Fatal(e)
			}
			if hits.Load() != 1 || result.Series.Coverage[0].State != "complete" {
				t.Fatal(result, hits.Load())
			}
			if p, ok := h.PriceAt(result.Series, confirmed.Add(-time.Nanosecond)); !ok || p.Price != "0.4" {
				t.Fatal("local payout traveled backwards", p, ok)
			}
			if p, ok := h.PriceAt(result.Series, to); !ok || p.Price != tc.price {
				t.Fatal("known local tail missing", p, ok)
			}
			if len(result.Series.Settlements) != 1 || result.Series.Settlements[0].SourceReference != known.SourceReference || !result.Series.Settlements[0].FetchedAt.Equal(confirmed) {
				t.Fatal("original local evidence changed", result.Series.Settlements)
			}
		})
	}
}
func TestPMKnownSettlementCannotInventMissingPrehistory(t *testing.T) {
	from := time.Unix(1793206800, 0).UTC()
	known := h.Settlement{EffectiveAt: from.Add(4 * time.Hour), Price: "1", Source: "OFFICIAL_SETTLEMENT", SourceReference: "LOCAL_POSITION:known", FetchedAt: from}
	s, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/resolutions" {
			fmt.Fprint(w, `{"data":[]}`)
		} else {
			fmt.Fprint(w, `{"data":[],"pagination":{"has_more":false}}`)
		}
	})
	defer close()
	result, e := s.FetchWithSettlements(context.Background(), h.Window{Asset: pmAsset(), From: from, To: from.Add(5 * time.Hour)}, []h.Settlement{known})
	if e != nil {
		t.Fatal(e)
	}
	if result.Series.Coverage[0].State != "missing" {
		t.Fatal("known tail falsely replaced missing pre-settlement prices", result)
	}
	if _, ok := h.PriceAt(result.Series, from); ok {
		t.Fatal("invented prehistory")
	}
	for _, bad := range []h.Settlement{{EffectiveAt: from, Price: "0.5", Source: "OFFICIAL_SETTLEMENT", SourceReference: "LOCAL_POSITION:bad"}, {EffectiveAt: from, Price: "1", Source: "UNTRUSTED", SourceReference: "bad"}, {EffectiveAt: time.Unix(2100000000, 0), Price: "1", Source: "OFFICIAL_SETTLEMENT", SourceReference: "future"}} {
		if _, e = s.FetchWithSettlements(context.Background(), h.Window{Asset: pmAsset(), From: from, To: from.Add(time.Hour)}, []h.Settlement{bad}); e == nil {
			t.Fatal("invalid known settlement accepted", bad)
		}
	}
}
