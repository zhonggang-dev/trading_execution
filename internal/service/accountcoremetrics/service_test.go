package accountcoremetrics

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

type factsFunc func(context.Context, string) (consolev2.CoreMetricFacts, error)

func (f factsFunc) CoreMetricFacts(c context.Context, a string) (consolev2.CoreMetricFacts, error) {
	return f(c, a)
}

type quotesFunc func(context.Context, []Target) map[string]Quote

func (f quotesFunc) Quotes(c context.Context, t []Target) map[string]Quote { return f(c, t) }
func fixture(now time.Time) consolev2.CoreMetricFacts {
	yes := 0
	name := "Yes"
	return consolev2.CoreMetricFacts{ObservedAt: now, Accounts: []consolev2.CoreAccountFact{{FundingAddress: "private-address-never-in-response", Metrics: consolev2.AccountPerformance{ExecutionAccountID: "pm", MarketSource: "POLYMARKET", PeakHoldingCost: "10", CumulativeInvestedCost: "20", RealizedPnL: "-2", RealizedDrawdown14D: "5", DrawdownFrom: now.Add(-24 * time.Hour), DrawdownTo: now}, Assets: []consolev2.CoreAssetFact{{PortfolioHolding: consolev2.PortfolioHolding{ExecutionAccountID: "pm", MarketSource: "POLYMARKET", MarketID: "m", ConditionID: "c", TokenID: "t", OutcomeIndex: &yes, OutcomeName: &name, RemainingShares: "3", RemainingCost: "0.9"}}}}}}
}
func TestCoreMetricsExactValuesSettlementPrecedenceAndNoPrivateIdentity(t *testing.T) {
	now := time.Now().UTC()
	facts := fixture(now)
	var calls atomic.Int32
	s := New(factsFunc(func(context.Context, string) (consolev2.CoreMetricFacts, error) { return facts, nil }), quotesFunc(func(_ context.Context, ts []Target) map[string]Quote {
		calls.Add(1)
		r := map[string]Quote{}
		for _, t := range ts {
			r[Key(t)] = Priced("0.5", "PM_CURRENT_PRICE", now, nil)
		}
		return r
	}))
	s.now = func() time.Time { return now }
	got, e := s.Query(context.Background(), "")
	if e != nil {
		t.Fatal(e)
	}
	m := got.Items[0]
	for key, pair := range map[string][2]string{"value": {*m.MarketValue, "1.5"}, "unrealized": {*m.UnrealizedPnL, "0.6"}, "total": {*m.TotalPnL, "-1.4"}, "rate": {*m.ReturnRate, "-0.140000000000000000000000"}} {
		if pair[0] != pair[1] {
			t.Fatalf("%s %v", key, pair)
		}
	}
	b, _ := json.Marshal(got)
	if strings.Contains(string(b), "private-address") {
		t.Fatal("private identity leaked")
	}
	*got.Items[0].ValuationAssets[0].Price = "0.9"
	again, _ := s.Query(context.Background(), "")
	if *again.Items[0].ValuationAssets[0].Price != "0.5" || calls.Load() != 1 {
		t.Fatal("cache aliased response or missed")
	}
	price := "0"
	facts.Accounts[0].Assets[0].SettlementPrice = &price
	facts.Accounts[0].Assets[0].SettlementAt = &now
	got, e = s.Query(context.Background(), "")
	if e != nil || *got.Items[0].MarketValue != "0" || *got.Items[0].ValuationAssets[0].PriceSource != "OFFICIAL_SETTLEMENT" || calls.Load() != 1 {
		t.Fatal("settlement must precede all external quotes", e)
	}
}
func TestCoreMetricsPartialQuoteFailurePreservesLedgerAndZeroAssets(t *testing.T) {
	now := time.Now().UTC()
	facts := fixture(now)
	s := New(factsFunc(func(context.Context, string) (consolev2.CoreMetricFacts, error) { return facts, nil }), nil)
	s.now = func() time.Time { return now }
	got, e := s.Query(context.Background(), "")
	if e != nil {
		t.Fatal(e)
	}
	m := got.Items[0]
	if m.MarketValue != nil || m.UnrealizedPnL != nil || m.TotalPnL != nil || m.ReturnRate != nil || m.RealizedPnL != "-2" || m.PricedAssetCount != 0 || m.ValuationState != "incomplete" {
		t.Fatalf("partial result %#v", m)
	}
	facts.Accounts[0].Assets = nil
	facts.Accounts[0].Metrics.PeakHoldingCost = "0"
	got, e = s.Query(context.Background(), "")
	if e != nil || *got.Items[0].MarketValue != "0" || *got.Items[0].TotalPnL != "-2" || got.Items[0].ReturnRate != nil || got.Items[0].QuoteObservedFrom != nil {
		t.Fatal("zero asset semantics", e)
	}
}
func TestCoreMetricsCacheAgeNegativeTTLAndExactArithmetic(t *testing.T) {
	now := time.Now().UTC()
	facts := fixture(now)
	var calls atomic.Int32
	s := New(factsFunc(func(context.Context, string) (consolev2.CoreMetricFacts, error) { return facts, nil }), quotesFunc(func(_ context.Context, ts []Target) map[string]Quote {
		n := calls.Add(1)
		if n == 1 {
			return map[string]Quote{Key(ts[0]): Unknown("one_sided")}
		}
		return map[string]Quote{Key(ts[0]): Priced("0.333333333333333333333333333333", "PM_CLOB_MID", now, nil)}
	}))
	s.now = func() time.Time { return now }
	s.Query(context.Background(), "")
	s.Query(context.Background(), "")
	if calls.Load() != 1 {
		t.Fatal("negative cache")
	}
	now = now.Add(3 * time.Second)
	got, e := s.Query(context.Background(), "")
	if e != nil || calls.Load() != 2 || !got.Items[0].HasReferencePrices || *got.Items[0].MarketValue != "0.999999999999999999999999999999" {
		t.Fatal("negative expiry or exact decimal", e)
	}
	now = now.Add(30 * time.Second)
	s.Query(context.Background(), "")
	if calls.Load() != 3 {
		t.Fatal("positive TTL")
	}
}
func TestCoreMetricsSingleFlightCallerCancellationDoesNotPoisonWaiter(t *testing.T) {
	now := time.Now().UTC()
	facts := fixture(now)
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	s := New(factsFunc(func(context.Context, string) (consolev2.CoreMetricFacts, error) { return facts, nil }), quotesFunc(func(c context.Context, ts []Target) map[string]Quote {
		if calls.Add(1) == 1 {
			close(entered)
		}
		select {
		case <-release:
			return map[string]Quote{Key(ts[0]): Priced("0.2", "PM_CURRENT_PRICE", now, nil)}
		case <-c.Done():
			return nil
		}
	}))
	c, cancel := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { _, e := s.Query(c, ""); done1 <- e }()
	<-entered
	done2 := make(chan Response, 1)
	go func() { r, _ := s.Query(context.Background(), ""); done2 <- r }()
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		waiters := 0
		for _, f := range s.flights {
			waiters = f.waiters
		}
		s.mu.Unlock()
		if waiters == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter not joined")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if !errors.Is(<-done1, context.Canceled) {
		t.Fatal("cancellation lost")
	}
	close(release)
	r := <-done2
	if calls.Load() != 1 || r.Items[0].PricedAssetCount != 1 {
		t.Fatal("cancel poisoned other waiter")
	}
}
func TestCoreMetricsMalformedPricesAndQuoteTime(t *testing.T) {
	now := time.Now().UTC()
	for name, q := range map[string]Quote{"bad": Priced("NaN", "PM_CURRENT_PRICE", now, nil), "future": Priced("0.5", "PM_CURRENT_PRICE", now.Add(time.Second), nil), "stale": Priced("0.5", "PM_CURRENT_PRICE", now.Add(-31*time.Second), nil)} {
		t.Run(name, func(t *testing.T) {
			f := fixture(now)
			s := New(factsFunc(func(context.Context, string) (consolev2.CoreMetricFacts, error) { return f, nil }), quotesFunc(func(_ context.Context, ts []Target) map[string]Quote { return map[string]Quote{Key(ts[0]): q} }))
			r, e := s.Query(context.Background(), "")
			if e != nil || r.Items[0].MarketValue != nil || r.Items[0].ValuationAssets[0].Reason == nil {
				t.Fatal("invalid quote accepted")
			}
		})
	}
}

func TestCoreMetricsAllWaitersCancelledDoesNotPoisonNextRead(t *testing.T) {
	now := time.Now().UTC()
	facts := fixture(now)
	entered := make(chan struct{})
	var calls atomic.Int32
	s := New(factsFunc(func(context.Context, string) (consolev2.CoreMetricFacts, error) { return facts, nil }), quotesFunc(func(ctx context.Context, ts []Target) map[string]Quote {
		if calls.Add(1) == 1 {
			close(entered)
			<-ctx.Done()
			return nil
		}
		return map[string]Quote{Key(ts[0]): Priced("0.2", "PM_CURRENT_PRICE", now, nil)}
	}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := s.Query(ctx, ""); done <- e }()
	<-entered
	cancel()
	if !errors.Is(<-done, context.Canceled) {
		t.Fatal("cancel")
	}
	r, e := s.Query(context.Background(), "")
	if e != nil || calls.Load() != 2 || r.Items[0].PricedAssetCount != 1 {
		t.Fatal("abandoned fetch poisoned new subscriber", e, calls.Load())
	}
}
func TestCoreMetricsNetworkBudgetPreservesFacts(t *testing.T) {
	now := time.Now().UTC()
	facts := fixture(now)
	s := New(factsFunc(func(context.Context, string) (consolev2.CoreMetricFacts, error) { return facts, nil }), quotesFunc(func(ctx context.Context, ts []Target) map[string]Quote { <-ctx.Done(); return nil }))
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	started := time.Now()
	r, e := s.Query(ctx, "")
	if e != nil || time.Since(started) > 350*time.Millisecond || r.Items[0].RealizedPnL != "-2" || r.Items[0].MarketValue != nil {
		t.Fatal("quote timeout masked facts", e, time.Since(started))
	}
}

func TestCoreMetricsCacheExpiresFromActualObservationNotInsertion(t *testing.T) {
	now := time.Now().UTC()
	facts := fixture(now)
	var calls atomic.Int32
	s := New(factsFunc(func(context.Context, string) (consolev2.CoreMetricFacts, error) { return facts, nil }), quotesFunc(func(_ context.Context, ts []Target) map[string]Quote {
		n := calls.Add(1)
		observed := now
		if n == 1 {
			observed = now.Add(-5 * time.Second)
		}
		return map[string]Quote{Key(ts[0]): Priced("0.2", "PM_CURRENT_PRICE", observed, nil)}
	}))
	s.now = func() time.Time { return now }
	s.Query(context.Background(), "")
	now = now.Add(26 * time.Second)
	r, e := s.Query(context.Background(), "")
	if e != nil || calls.Load() != 2 || r.Items[0].ValuationState != "complete" {
		t.Fatal("expired observation reused instead of refreshed", e, calls.Load())
	}
}
