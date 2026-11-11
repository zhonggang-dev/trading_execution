package accountprices

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	h "github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
)

func TestRequiredPMUsesEarliestRealAsOfAndRechecksLaterCashTimes(t *testing.T) {
	at := time.Date(2026, 10, 28, 10, 4, 1, 0, time.UTC)
	future := at.Truncate(time.Hour).Add(5 * time.Minute)
	var calls atomic.Int32
	source, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v2/prices-history" || r.URL.Query().Get("as_of") != strconv.FormatInt(at.Unix(), 10) {
			t.Error("warm repair must only request first missing as_of", r.URL.String())
			http.Error(w, "bad request", 400)
			return
		}
		fmt.Fprintf(w, `{"data":[{"timestamp":%d,"price":0.4,"resolution_seconds":0}],"pagination":{"has_more":false}}`, at.Truncate(time.Hour).Add(3*time.Minute).Unix())
	})
	defer close()
	seed := h.Series{Points: []h.Point{{At: at.Truncate(time.Hour).Add(-time.Hour), Price: "0.2", ResolutionSeconds: 3600, Source: "PM_HISTORY"}, {At: future, Price: "0.9", ResolutionSeconds: 300, Source: "PM_HISTORY"}}}
	h.SortSeries(&seed)
	if _, ok := h.PriceAt(seed, at); ok {
		t.Fatal("initial >1h gap was not reproduced")
	}
	result, err := source.FetchWithSeries(context.Background(), h.Window{Asset: pmAsset(), From: at, To: at.Add(10 * time.Second), RequiredAt: []time.Time{at, at.Add(10 * time.Second)}}, seed)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || len(result.Series.Points) != 1 || result.Series.Coverage[0].State != "complete" {
		t.Fatal(calls.Load(), result)
	}
	if p, ok := h.PriceAt(result.Series, at); !ok || p.Price != "0.4" || p.At.After(at) {
		t.Fatal("future 10:05 observation used", p, ok)
	}
	if _, ok := h.PriceAt(seed, at); ok {
		t.Fatal("repair mutated original loaded series")
	}
}
func TestRequiredKalshiVerifiesFractionalTailRatherThanRelaxingAge(t *testing.T) {
	at := time.Date(2026, 10, 30, 16, 50, 19, 25092000, time.UTC)
	oldUntil := time.Date(2026, 10, 30, 16, 50, 18, 965828000, time.UTC)
	for _, newTrade := range []bool{false, true} {
		t.Run(strconv.FormatBool(newTrade), func(t *testing.T) {
			var trades atomic.Int32
			source, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/historical/cutoff":
					fmt.Fprint(w, `{"market_settled_ts":"2026-09-01T00:00:00Z","trades_created_ts":"2026-09-01T00:00:00Z"}`)
				case "/markets/trades":
					trades.Add(1)
					if r.URL.Query().Get("max_ts") != strconv.FormatInt(at.Unix()+1, 10) {
						t.Error("whole-second request did not fully cover fractional target", r.URL.Query())
					}
					if newTrade {
						fmt.Fprintf(w, `{"trades":[{"trade_id":"past","ticker":"KXUSPSELECTION-26","created_time":"%s","yes_price_dollars":"0.2"},{"trade_id":"future","ticker":"KXUSPSELECTION-26","created_time":"%s","yes_price_dollars":"0.9"}],"cursor":""}`, at.Truncate(time.Second).Add(10*time.Millisecond).Format(time.RFC3339Nano), at.Truncate(time.Second).Add(500*time.Millisecond).Format(time.RFC3339Nano))
					} else {
						fmt.Fprint(w, `{"trades":[],"cursor":""}`)
					}
				default:
					t.Error("warm K repair must not refetch market/candles", r.URL.Path)
					http.Error(w, "bad path", 400)
				}
			})
			defer close()
			index := 1
			name := "No"
			asset := h.Asset{MarketSource: "KALSHI", MarketID: "KXUSPSELECTION-26", TokenID: "kalshi:KXUSPSELECTION-26:NO", OutcomeIndex: &index, OutcomeName: &name}
			anchor := at.Truncate(time.Hour).Add(-time.Hour)
			seed := h.Series{Points: []h.Point{{At: anchor, Price: "0.1", ResolutionSeconds: 3600, Source: "KALSHI_PREVIOUS_TRADE", ValidUntil: &oldUntil}, {At: at.Truncate(time.Hour).Add(time.Hour), Price: "0.9", ResolutionSeconds: 3600, Source: "KALSHI_TRADE_CLOSE"}}}
			h.SortSeries(&seed)
			if _, ok := h.PriceAt(seed, at); ok {
				t.Fatal("59ms gap not reproduced")
			}
			result, err := source.FetchWithSeries(context.Background(), h.Window{Asset: asset, From: at, To: at, RequiredAt: []time.Time{at}}, seed)
			if err != nil {
				t.Fatal(err)
			}
			merged := seed
			merged.Points = append(append([]h.Point(nil), seed.Points...), result.Series.Points...)
			h.SortSeries(&merged)
			point, ok := h.PriceAt(merged, at)
			want := "0.1"
			if newTrade {
				want = "0.800000000000"
			}
			if !ok || point.Price != want || trades.Load() != 1 {
				t.Fatal(point, ok, trades.Load(), result)
			}
			for _, p := range result.Series.Points {
				if p.At.After(at) {
					t.Fatal("future transaction persisted", p)
				}
			}
			if _, ok := h.PriceAt(seed, at); ok {
				t.Fatal("proof repair mutated seed")
			}
		})
	}
}
func TestRequiredKalshiNeverExtendsMidWithNoTradeProof(t *testing.T) {
	at := time.Unix(1793206800, 0).UTC()
	var calls atomic.Int32
	source, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); http.Error(w, "must not request", 400) })
	defer close()
	index := 0
	name := "Yes"
	a := h.Asset{MarketSource: "KALSHI", MarketID: "T", TokenID: "kalshi:T:YES", OutcomeIndex: &index, OutcomeName: &name}
	seed := h.Series{Points: []h.Point{{At: at, Price: "0.5", Source: "KALSHI_BOOK_MID", ResolutionSeconds: 3600}}}
	result, e := source.FetchWithSeries(context.Background(), h.Window{Asset: a, From: at.Add(2 * time.Hour), To: at.Add(2 * time.Hour), RequiredAt: []time.Time{at.Add(2 * time.Hour)}}, seed)
	if e != nil {
		t.Fatal(e)
	}
	if calls.Load() != 0 || result.Series.Coverage[0].State != "missing" {
		t.Fatal(result, calls.Load())
	}
}

// A quiet market has no hourly bucket, so the official as-of answer is older
// than one hour. Reproduces market 6766351 at 2026-11-10 13:00 UTC.
func TestRequiredPMCarriesOfficialAsOfAcrossAQuietGap(t *testing.T) {
	at := time.Date(2026, 11, 10, 13, 0, 0, 0, time.UTC)
	later := at.Add(30 * time.Minute)
	observed := time.Date(2026, 11, 10, 11, 10, 52, 0, time.UTC)
	var asOfCalls, resolutionCalls atomic.Int32
	source, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/resolutions" {
			resolutionCalls.Add(1)
			fmt.Fprint(w, `{"data":[]}`)
			return
		}
		asOfCalls.Add(1)
		fmt.Fprintf(w, `{"data":[{"timestamp":%d,"price":0.51,"resolution_seconds":0}],"pagination":{"has_more":false}}`, observed.Unix())
	})
	defer close()
	seed := h.Series{Points: []h.Point{{At: observed.Add(-time.Hour), Price: "0.5", ResolutionSeconds: 3600, Source: "PM_HISTORY"}}}
	result, err := source.FetchWithSeries(context.Background(), h.Window{Asset: pmAsset(), From: at, To: later, RequiredAt: []time.Time{at, later}}, seed)
	if err != nil {
		t.Fatal(err)
	}
	if result.Series.Coverage[0].State != "complete" || result.Series.Coverage[0].Reason != "" {
		t.Fatalf("coverage = %+v", result.Series.Coverage)
	}
	for _, sample := range []time.Time{at, later} {
		if point, ok := h.PriceAt(result.Series, sample); !ok || point.Price != "0.51" || !point.At.Equal(observed) {
			t.Fatalf("%s: point=%+v ok=%v", sample, point, ok)
		}
	}
	if resolutionCalls.Load() != 1 || asOfCalls.Load() != 2 {
		t.Fatalf("resolutions=%d asOf=%d, want settlement checked once and each instant asked", resolutionCalls.Load(), asOfCalls.Load())
	}
	if _, ok := h.PriceAt(seed, at); ok {
		t.Fatal("repair mutated the loaded series")
	}
}

func TestRequiredPMDoesNotCarryBeyondTheLimitOrWithoutAnAnswer(t *testing.T) {
	at := time.Date(2026, 11, 10, 13, 0, 0, 0, time.UTC)
	for name, body := range map[string]string{
		"too old":   fmt.Sprintf(`{"data":[{"timestamp":%d,"price":0.51,"resolution_seconds":0}],"pagination":{"has_more":false}}`, at.Add(-pmMaxAsOfCarry-time.Second).Unix()),
		"no answer": `{"data":[],"pagination":{"has_more":false}}`,
	} {
		t.Run(name, func(t *testing.T) {
			source, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v2/resolutions" {
					fmt.Fprint(w, `{"data":[]}`)
					return
				}
				fmt.Fprint(w, body)
			})
			defer close()
			seed := h.Series{Points: []h.Point{{At: at.Add(-48 * time.Hour), Price: "0.5", ResolutionSeconds: 3600, Source: "PM_HISTORY"}}}
			result, err := source.FetchWithSeries(context.Background(), h.Window{Asset: pmAsset(), From: at, To: at, RequiredAt: []time.Time{at}}, seed)
			if err != nil {
				t.Fatal(err)
			}
			if result.Series.Coverage[0].State != "missing" || result.Series.Coverage[0].Reason != "missing_prices" {
				t.Fatalf("coverage = %+v, want a reported gap", result.Series.Coverage)
			}
		})
	}
}

// Settlement wins: a market resolved before the instant must be valued at its
// payout, never at the stale as-of price.
func TestRequiredPMPrefersSettlementOverCarry(t *testing.T) {
	at := time.Date(2026, 11, 10, 13, 0, 0, 0, time.UTC)
	known := h.Settlement{EffectiveAt: at.Add(-time.Minute), Price: "1", Source: "OFFICIAL_SETTLEMENT", SourceReference: "0xabc:1", FetchedAt: at}
	source, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("a settled instant needs no request", r.URL.String())
		http.Error(w, "unexpected", 400)
	})
	defer close()
	result, err := source.FetchWithSeries(context.Background(), h.Window{Asset: pmAsset(), From: at, To: at, RequiredAt: []time.Time{at}}, h.Series{Settlements: []h.Settlement{known}})
	if err != nil {
		t.Fatal(err)
	}
	if point, ok := h.PriceAt(result.Series, at); !ok || point.Price != "1" || point.Source != "OFFICIAL_SETTLEMENT" {
		t.Fatalf("point=%+v ok=%v, want the settlement", point, ok)
	}
}

// The stale as-of answer must not hide a resolution only the settlement lookup
// can reveal.
func TestRequiredPMLooksUpSettlementBeforeCarrying(t *testing.T) {
	at := time.Date(2026, 11, 10, 13, 0, 0, 0, time.UTC)
	resolved := at.Add(-time.Minute)
	source, close := fixtureSource(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/prices-history":
			fmt.Fprintf(w, `{"data":[{"timestamp":%d,"price":0.51,"resolution_seconds":0}],"pagination":{"has_more":false}}`, at.Add(-2*time.Hour).Unix())
		case "/v2/resolutions":
			fmt.Fprintf(w, `{"data":[{"condition_id":"condition","status":"resolved","transaction_hash":"0x%s","log_index":"1","last_update_timestamp":"%d"}]}`, strings.Repeat("a", 64), resolved.Unix())
		case "/markets/1":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "1", "conditionId": "condition", "closed": true, "umaResolutionStatus": "resolved", "clobTokenIds": `["yes-token","no-token"]`, "outcomePrices": `["0","1"]`, "outcomes": `["Yes","No"]`})
		default:
			t.Error(r.URL.String())
			http.Error(w, "bad path", 400)
		}
	})
	defer close()
	seed := h.Series{Points: []h.Point{{At: at.Add(-3 * time.Hour), Price: "0.5", ResolutionSeconds: 3600, Source: "PM_HISTORY"}}}
	result, err := source.FetchWithSeries(context.Background(), h.Window{Asset: pmAsset(), From: at, To: at, RequiredAt: []time.Time{at}}, seed)
	if err != nil {
		t.Fatal(err)
	}
	if point, ok := h.PriceAt(result.Series, at); !ok || point.Price != "1" || point.Source != "OFFICIAL_SETTLEMENT" {
		t.Fatalf("point=%+v ok=%v, want the settlement payout", point, ok)
	}
	for _, p := range result.Series.Points {
		if p.ValidUntil != nil {
			t.Fatalf("carried a settled market: %+v", p)
		}
	}
}
