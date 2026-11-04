package accountquotes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	core "github.com/UniPat-AI/trading_execution/internal/service/accountcoremetrics"
	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

type positionsFunc func(context.Context, string) ([]domain.ExternalPosition, error)

func (f positionsFunc) ListValuationPrices(c context.Context, a string) ([]domain.ExternalPosition, error) {
	return f(c, a)
}

type marketsFunc func(context.Context, []string) ([]KalshiMarket, error)

func (f marketsFunc) CoreMarkets(c context.Context, t []string) ([]KalshiMarket, error) {
	return f(c, t)
}
func ptr(s string) *string { return &s }
func target(account, source, token string, index int) core.Target {
	name := "Yes"
	if index == 1 {
		name = "No"
	}
	return core.Target{Address: "0x1111111111111111111111111111111111111111", Asset: consolev2.CoreAssetFact{PortfolioHolding: consolev2.PortfolioHolding{ExecutionAccountID: account, MarketSource: source, MarketID: "M", ConditionID: "C", TokenID: token, OutcomeIndex: &index, OutcomeName: &name, RemainingShares: "3", RemainingCost: "1"}}}
}
func TestPMOfficialCurrentPriceFirstAndMissingOnlyBookFallback(t *testing.T) {
	now := time.Now().UTC()
	targets := []core.Target{target("a", "POLYMARKET", "official", 0), target("a", "POLYMARKET", "fallback", 0), target("a", "POLYMARKET", "zero", 1)}
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != "POST" || r.URL.Path != "/books" {
			t.Error("non-read market endpoint")
		}
		var body []map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		if len(body) != 1 || body[0]["token_id"] != "fallback" {
			t.Errorf("official prices queried CLOB: %v", body)
		}
		fmt.Fprintf(w, `[{"asset_id":"fallback","market":"C","timestamp":"%d","bids":[{"price":"0.4","size":"3"}],"asks":[{"price":"0.6","size":"2"}]}]`, now.UnixMilli())
	}))
	defer server.Close()
	source := Source{CLOBURL: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return now }, Positions: positionsFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
		return []domain.ExternalPosition{{ConditionID: "C", TokenID: "official", OutcomeIndex: targets[0].Asset.OutcomeIndex, OutcomeName: "Yes", Shares: "999", AveragePrice: "0.9", CurrentPrice: "0.2", ObservedAt: now}, {ConditionID: "C", TokenID: "zero", OutcomeIndex: targets[2].Asset.OutcomeIndex, OutcomeName: "No", CurrentPrice: "0", Redeemable: true, ObservedAt: now}}, nil
	})}
	got := source.Quotes(context.Background(), targets)
	if *got[core.Key(targets[0])].Price != "0.2" || *got[core.Key(targets[0])].Source != "PM_CURRENT_PRICE" {
		t.Fatal("current_price priority")
	}
	if *got[core.Key(targets[1])].Price != "0.5" || requests.Load() != 1 {
		t.Fatal("fallback midpoint")
	}
	if *got[core.Key(targets[2])].Price != "0" || *got[core.Key(targets[2])].Source != "OFFICIAL_SETTLEMENT" {
		t.Fatal("zero settled price")
	}
}
func TestPMBookRejectsWrongIdentityCrossedOneSideAndDuplicate(t *testing.T) {
	cases := map[string]string{"wrong": `[{"asset_id":"t","market":"WRONG","bids":[{"price":"0.4","size":"2"}],"asks":[{"price":"0.6","size":"2"}]}]`, "crossed": `[{"asset_id":"t","market":"C","bids":[{"price":"0.8","size":"2"}],"asks":[{"price":"0.6","size":"2"}]}]`, "one-side": `[{"asset_id":"t","market":"C","bids":[{"price":"0.4","size":"2"}],"asks":[]}]`, "duplicate": `[{"asset_id":"t","market":"C"},{"asset_id":"t","market":"C"}]`}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, body) }))
			defer server.Close()
			tar := target("a", "POLYMARKET", "t", 0)
			s := Source{CLOBURL: server.URL, HTTPClient: server.Client(), Positions: positionsFunc(func(context.Context, string) ([]domain.ExternalPosition, error) { return nil, nil })}
			q := s.Quotes(context.Background(), []core.Target{tar})[core.Key(tar)]
			if q.Price != nil || q.Reason == nil {
				t.Fatal("bad quote accepted")
			}
		})
	}
}
func TestPMWrongCurrentPriceIdentityCannotBeMaskedByFallback(t *testing.T) {
	tar := target("a", "POLYMARKET", "t", 0)
	s := Source{Positions: positionsFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
		return []domain.ExternalPosition{{ConditionID: "wrong", TokenID: "t", OutcomeIndex: tar.Asset.OutcomeIndex, OutcomeName: "Yes", CurrentPrice: "0.2"}}, nil
	})}
	q := s.Quotes(context.Background(), []core.Target{tar})[core.Key(tar)]
	if q.Reason == nil || *q.Reason != "identity_mismatch" {
		t.Fatal("identity not checked")
	}
}
func kalshiTarget(index int) core.Target {
	t := target("k", "KALSHI", "kalshi:M:YES", index)
	t.Asset.ConditionID = "kalshi:M"
	if index == 1 {
		t.Asset.TokenID = "kalshi:M:NO"
	}
	return t
}
func market() KalshiMarket {
	return KalshiMarket{Ticker: "M", MarketType: "binary", Status: "active", YesBid: ptr("0.4"), YesAsk: ptr("0.6"), NoBid: ptr("0.4"), NoAsk: ptr("0.6"), YesBidSize: ptr("10"), YesAskSize: ptr("20")}
}
func TestKalshiYesNoUsesActualYesLiquidityAndReferenceSource(t *testing.T) {
	m := market()
	m.YesBid = ptr("0.3")
	m.YesAsk = ptr("0.5")
	m.NoBid = ptr("0.5")
	m.NoAsk = ptr("0.7")
	s := Source{Kalshi: marketsFunc(func(context.Context, []string) ([]KalshiMarket, error) { return []KalshiMarket{m}, nil })}
	a, b := kalshiTarget(0), kalshiTarget(1)
	q := s.Quotes(context.Background(), []core.Target{a, b})
	if *q[core.Key(a)].Price != "0.4" || *q[core.Key(b)].Price != "0.6" || *q[core.Key(b)].Source != "KALSHI_BOOK_MID" {
		t.Fatal("NO liquidity complement")
	}
	m.YesAskSize = ptr("0")
	q = s.Quotes(context.Background(), []core.Target{b})
	if q[core.Key(b)].Price != nil || *q[core.Key(b)].Reason != "one_sided" {
		t.Fatal("zero-side book produced price")
	}
}
func TestKalshiOfficialLastTradeFirstAndExactNoComplement(t *testing.T) {
	for _, last := range []string{"0.01", "0.123456", "0", "1"} {
		t.Run(last, func(t *testing.T) {
			m := market()
			m.LastPrice, m.Volume = ptr(last), ptr("10.00")
			// An absent sell side must not erase a real last-traded price.
			m.YesBidSize = ptr("0")
			s := Source{Kalshi: marketsFunc(func(context.Context, []string) ([]KalshiMarket, error) { return []KalshiMarket{m}, nil })}
			yes, no := kalshiTarget(0), kalshiTarget(1)
			q := s.Quotes(context.Background(), []core.Target{yes, no})
			y, n := q[core.Key(yes)], q[core.Key(no)]
			expected := map[string]string{"0.01": "0.99", "0.123456": "0.876544", "0": "1", "1": "0"}
			if y.Price == nil || n.Price == nil || *y.Price != last || *n.Price != expected[last] || *y.Source != "KALSHI_LAST_TRADE" || *n.Source != "KALSHI_LAST_TRADE" || y.SourceAt != nil || n.SourceAt != nil {
				t.Fatalf("last trade YES=%+v NO=%+v", y, n)
			}
		})
	}
}
func TestKalshiMissingInvalidOrUntradedLastFallsBackToValidMid(t *testing.T) {
	cases := []struct {
		name         string
		last, volume *string
	}{
		{"missing", nil, ptr("1")}, {"empty", ptr(""), ptr("1")}, {"invalid", ptr("NaN"), ptr("1")},
		{"negative", ptr("-0.01"), ptr("1")}, {"above-one", ptr("1.01"), ptr("1")},
		{"never-traded-default-zero", ptr("0"), ptr("0")}, {"missing-volume", ptr("0.2"), nil},
		{"bad-volume", ptr("0.2"), ptr("bad")}, {"negative-volume", ptr("0.2"), ptr("-1")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := market()
			m.LastPrice, m.Volume = tc.last, tc.volume
			s := Source{Kalshi: marketsFunc(func(context.Context, []string) ([]KalshiMarket, error) { return []KalshiMarket{m}, nil })}
			target := kalshiTarget(1)
			q := s.Quotes(context.Background(), []core.Target{target})[core.Key(target)]
			if q.Price == nil || *q.Price != "0.5" || *q.Source != "KALSHI_BOOK_MID" {
				t.Fatalf("mid fallback: %+v", q)
			}
			m.YesBidSize = ptr("0")
			q = s.Quotes(context.Background(), []core.Target{target})[core.Key(target)]
			if q.Price != nil || q.Reason == nil || *q.Reason != "one_sided" {
				t.Fatalf("invalid last plus missing side invented price: %+v", q)
			}
		})
	}
}
func TestKalshiOnlyFinalizedIsOfficialSettlement(t *testing.T) {
	now := time.Now().UTC()
	for _, status := range []string{"closed", "determined", "disputed", "amended", "finalized"} {
		t.Run(status, func(t *testing.T) {
			m := market()
			m.Status = status
			m.LastPrice, m.Volume = ptr("0.2"), ptr("100")
			m.Result = "yes"
			m.SettlementAt = &now
			m.SettlementValue = ptr("1")
			s := Source{Now: func() time.Time { return now }, Kalshi: marketsFunc(func(context.Context, []string) ([]KalshiMarket, error) { return []KalshiMarket{m}, nil })}
			tar := kalshiTarget(1)
			q := s.Quotes(context.Background(), []core.Target{tar})[core.Key(tar)]
			if status == "finalized" {
				if *q.Price != "0" || *q.Source != "OFFICIAL_SETTLEMENT" {
					t.Fatal("finalized 0 payout")
				}
			} else if q.Source != nil && *q.Source == "OFFICIAL_SETTLEMENT" {
				t.Fatal("non-final result used as settlement")
			}
		})
	}
}
func TestKalshiRejectsSettlementConflictAndBadIdentity(t *testing.T) {
	now := time.Now().UTC()
	for _, variant := range []string{"price", "future", "token", "condition", "duplicate", "scalar"} {
		t.Run(variant, func(t *testing.T) {
			m := market()
			m.Status = "finalized"
			m.LastPrice, m.Volume = ptr("0.2"), ptr("100")
			m.Result = "yes"
			m.SettlementAt = &now
			m.SettlementValue = ptr("1")
			tar := kalshiTarget(0)
			ms := []KalshiMarket{m}
			switch variant {
			case "price":
				ms[0].SettlementValue = ptr("0")
			case "future":
				v := now.Add(time.Hour)
				ms[0].SettlementAt = &v
			case "token":
				tar.Asset.TokenID = "other"
			case "condition":
				tar.Asset.ConditionID = "wrong"
			case "duplicate":
				ms = append(ms, m)
			case "scalar":
				ms[0].MarketType = "scalar"
			}
			s := Source{Kalshi: marketsFunc(func(context.Context, []string) ([]KalshiMarket, error) { return ms, nil })}
			q := s.Quotes(context.Background(), []core.Target{tar})[core.Key(tar)]
			if q.Price != nil || q.Reason == nil {
				t.Fatal("invalid terminal/identity price")
			}
		})
	}
}
func TestPlatformFailureIsIndependent(t *testing.T) {
	now := time.Now().UTC()
	p, k := target("a", "POLYMARKET", "t", 0), kalshiTarget(0)
	s := Source{Positions: positionsFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
		return []domain.ExternalPosition{{ConditionID: "C", TokenID: "t", OutcomeIndex: p.Asset.OutcomeIndex, OutcomeName: "Yes", CurrentPrice: "0.2", ObservedAt: now}}, nil
	}), Kalshi: marketsFunc(func(context.Context, []string) ([]KalshiMarket, error) { return nil, errors.New("upstream failure") })}
	q := s.Quotes(context.Background(), []core.Target{p, k})
	if q[core.Key(p)].Price == nil || q[core.Key(k)].Price != nil {
		t.Fatal("platform failure spilled")
	}
}
func TestLimitTransportBoundsBodiesAndHonorsWaitingCancellation(t *testing.T) {
	var active, max atomic.Int32
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v := active.Add(1)
		defer active.Add(-1)
		for {
			old := max.Load()
			if v <= old || max.CompareAndSwap(old, v) {
				break
			}
		}
		w.Header().Set("Content-Length", "1")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-release
		io.WriteString(w, "x")
	}))
	defer server.Close()
	client := &http.Client{Transport: NewLimitTransport(nil)}
	responses := make(chan *http.Response, 2)
	for i := 0; i < 2; i++ {
		go func() { r, _ := client.Get(server.URL); responses <- r }()
	}
	r1, r2 := <-responses, <-responses
	c, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(c, "GET", server.URL, nil)
	if _, e := client.Do(req); e == nil {
		t.Fatal("third request did not wait")
	}
	close(release)
	r1.Body.Close()
	r2.Body.Close()
	if max.Load() > 2 {
		t.Fatal("network concurrency exceeded")
	}
}
func TestMidpointExactZeroAndMalformedSizes(t *testing.T) {
	for _, size := range []string{"NaN", "1e99999", "-1", strings.Repeat("1", 257)} {
		if _, reason := midpoint([]level{{Price: "0.2", Size: size}}, []level{{Price: "0.4", Size: "1"}}); reason == "" {
			t.Fatal("bad size")
		}
	}
	if mid, reason := midpoint([]level{{Price: "0", Size: "1"}}, []level{{Price: "0", Size: "1"}}); reason != "" || mid != "0" {
		t.Fatal("legal zero mid")
	}
}

func TestPMMissingBooksDeduplicateAcrossAccounts(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var body []map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		if len(body) != 1 {
			t.Error("same asset not deduplicated")
		}
		io.WriteString(w, `[{"asset_id":"t","market":"C","bids":[{"price":"0.4","size":"1"}],"asks":[{"price":"0.6","size":"1"}]}]`)
	}))
	defer server.Close()
	a, b := target("a", "POLYMARKET", "t", 0), target("b", "POLYMARKET", "t", 0)
	b.Address = "0x2222222222222222222222222222222222222222"
	s := Source{CLOBURL: server.URL, HTTPClient: server.Client(), Positions: positionsFunc(func(context.Context, string) ([]domain.ExternalPosition, error) { return nil, nil })}
	q := s.Quotes(context.Background(), []core.Target{a, b})
	if calls.Load() != 1 || q[core.Key(a)].Price == nil || q[core.Key(b)].Price == nil {
		t.Fatal("shared market request duplicated")
	}
}
func TestPMBatchCannotAcceptMembersFromAnotherChunk(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		io.WriteString(w, `[{"asset_id":"t0500","market":"C","bids":[{"price":"0.4","size":"1"}],"asks":[{"price":"0.6","size":"1"}]}]`)
	}))
	defer server.Close()
	targets := []core.Target{}
	for i := 0; i < 501; i++ {
		targets = append(targets, target("a", "POLYMARKET", fmt.Sprintf("t%04d", i), 0))
	}
	s := Source{CLOBURL: server.URL, HTTPClient: server.Client(), Positions: positionsFunc(func(context.Context, string) ([]domain.ExternalPosition, error) { return nil, nil })}
	q := s.Quotes(context.Background(), targets)
	if calls != 2 || q[core.Key(targets[0])].Reason == nil || *q[core.Key(targets[0])].Reason != "identity_mismatch" || q[core.Key(targets[500])].Price == nil {
		t.Fatal("batch membership was checked against entire request")
	}
}
func TestKalshiBatchCannotAcceptMembersFromAnotherChunk(t *testing.T) {
	calls := 0
	targets := []core.Target{}
	for i := 0; i < 101; i++ {
		v := kalshiTarget(0)
		ticker := fmt.Sprintf("M%03d", i)
		v.Asset.MarketID = ticker
		v.Asset.ConditionID = "kalshi:" + ticker
		v.Asset.TokenID = "kalshi:" + ticker + ":YES"
		targets = append(targets, v)
	}
	s := Source{Kalshi: marketsFunc(func(context.Context, []string) ([]KalshiMarket, error) {
		calls++
		m := market()
		m.Ticker = "M100"
		return []KalshiMarket{m}, nil
	})}
	q := s.Quotes(context.Background(), targets)
	if calls != 2 || q[core.Key(targets[0])].Reason == nil || *q[core.Key(targets[0])].Reason != "identity_mismatch" || q[core.Key(targets[100])].Price == nil {
		t.Fatal("Kalshi batch accepted another chunk")
	}
}

type factsReaderFunc func(context.Context, string) (consolev2.CoreMetricFacts, error)

func (f factsReaderFunc) CoreMetricFacts(ctx context.Context, a string) (consolev2.CoreMetricFacts, error) {
	return f(ctx, a)
}
func metricFacts(now time.Time, targets []core.Target) consolev2.CoreMetricFacts {
	f := consolev2.CoreMetricFacts{ObservedAt: now}
	index := map[string]int{}
	for _, t := range targets {
		i, ok := index[t.Asset.ExecutionAccountID]
		if !ok {
			i = len(f.Accounts)
			index[t.Asset.ExecutionAccountID] = i
			f.Accounts = append(f.Accounts, consolev2.CoreAccountFact{FundingAddress: t.Address, Metrics: consolev2.AccountPerformance{ExecutionAccountID: t.Asset.ExecutionAccountID, MarketSource: t.Asset.MarketSource, PeakHoldingCost: "10", CumulativeInvestedCost: "20", RealizedPnL: "-2", RealizedDrawdown14D: "5", DrawdownFrom: now, DrawdownTo: now}})
		}
		f.Accounts[i].Assets = append(f.Accounts[i].Assets, t.Asset)
	}
	return f
}
func TestCoreBudgetKeepsCompletedPMWhenKalshiHangs(t *testing.T) {
	now := time.Now().UTC()
	pm, k := target("a", "POLYMARKET", "t", 0), kalshiTarget(0)
	facts := metricFacts(now, []core.Target{pm, k})
	source := &Source{Positions: positionsFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
		return []domain.ExternalPosition{{ConditionID: "C", TokenID: "t", OutcomeIndex: pm.Asset.OutcomeIndex, OutcomeName: "Yes", CurrentPrice: "0.2", ObservedAt: now}}, nil
	}), Kalshi: marketsFunc(func(ctx context.Context, _ []string) ([]KalshiMarket, error) { <-ctx.Done(); return nil, ctx.Err() })}
	service := core.New(factsReaderFunc(func(context.Context, string) (consolev2.CoreMetricFacts, error) { return facts, nil }), source)
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	started := time.Now()
	r, e := service.Query(ctx, "")
	if e != nil || time.Since(started) > 350*time.Millisecond {
		t.Fatal("budget failed", e, time.Since(started))
	}
	if len(r.Items) != 2 || r.Items[0].ValuationState != "complete" || r.Items[0].MarketValue == nil || r.Items[1].ValuationState != "incomplete" {
		t.Fatalf("unrelated timeout erased completed PM price: %+v", r.Items)
	}
}
func TestCoreBudgetKeepsOfficialPriceWhenSameAccountFallbackHangs(t *testing.T) {
	now := time.Now().UTC()
	known, missing := target("a", "POLYMARKET", "known", 0), target("a", "POLYMARKET", "missing", 0)
	facts := metricFacts(now, []core.Target{known, missing})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); <-r.Context().Done() }))
	defer server.Close()
	source := &Source{CLOBURL: server.URL, HTTPClient: server.Client(), Positions: positionsFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
		return []domain.ExternalPosition{{ConditionID: "C", TokenID: "known", OutcomeIndex: known.Asset.OutcomeIndex, OutcomeName: "Yes", CurrentPrice: "0.2", ObservedAt: now}}, nil
	})}
	service := core.New(factsReaderFunc(func(context.Context, string) (consolev2.CoreMetricFacts, error) { return facts, nil }), source)
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	r, e := service.Query(ctx, "")
	if e != nil || len(r.Items) != 1 || r.Items[0].PricedAssetCount != 1 || r.Items[0].MarketValue != nil {
		t.Fatal("partial official price lost", e)
	}
	found := false
	for _, a := range r.Items[0].ValuationAssets {
		if a.TokenID == "known" {
			found = a.Price != nil && *a.Price == "0.2"
		}
	}
	if !found {
		t.Fatal("completed official price disappeared")
	}
}

func TestMidpointDoesNotRoundHighPrecisionOrTinyPositivePriceToZero(t *testing.T) {
	for _, test := range []struct{ bid, ask, want string }{{"0.12345678901234567890123456789", "0.12345678901234567890123456791", "0.1234567890123456789012345679"}, {"0.000000000000000000000000000001", "0.000000000000000000000000000003", "0.000000000000000000000000000002"}} {
		got, reason := midpoint([]level{{Price: test.bid, Size: "1"}}, []level{{Price: test.ask, Size: "1"}})
		if reason != "" || got != test.want {
			t.Fatalf("exact midpoint got %s/%s want %s", got, reason, test.want)
		}
	}
}
