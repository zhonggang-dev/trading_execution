// Package accountcoremetrics joins frozen local ledger facts to bounded public
// prices. It never invokes an execution, settlement, or strategy write action.
package accountcoremetrics

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

type FactsReader interface {
	CoreMetricFacts(context.Context, string) (consolev2.CoreMetricFacts, error)
}
type Target struct {
	Asset   consolev2.CoreAssetFact
	Address string
}
type Quote struct {
	Price      *string
	Source     *string
	SourceAt   *time.Time
	ObservedAt *time.Time
	Reason     *string
}
type QuoteUpdateReader interface {
	QuotesWithUpdates(context.Context, []Target, func(map[string]Quote)) map[string]Quote
}
type QuoteReader interface {
	Quotes(context.Context, []Target) map[string]Quote
}

func Key(t Target) string {
	b, _ := json.Marshal([]any{t.Asset.ExecutionAccountID, t.Asset.MarketSource, t.Asset.MarketID, t.Asset.ConditionID, t.Asset.TokenID, t.Asset.OutcomeIndex, t.Asset.OutcomeName, t.Address})
	return string(b)
}
func Unknown(reason string) Quote { return Quote{Reason: &reason} }
func Priced(price, source string, observed time.Time, sourceAt *time.Time) Quote {
	return Quote{Price: &price, Source: &source, SourceAt: sourceAt, ObservedAt: &observed}
}

type Response struct {
	ObservedAt time.Time `json:"observed_at"`
	Items      []Metrics `json:"items"`
}
type Metrics struct {
	ExecutionAccountID     string     `json:"execution_account_id"`
	MarketSource           string     `json:"market_source"`
	AssetCount             int        `json:"asset_count"`
	PricedAssetCount       int        `json:"priced_asset_count"`
	CurrentCost            string     `json:"current_cost"`
	PeakHoldingCost        string     `json:"peak_holding_cost"`
	CumulativeInvestedCost string     `json:"cumulative_invested_cost"`
	RealizedPnL            string     `json:"realized_pnl"`
	MarketValue            *string    `json:"market_value"`
	UnrealizedPnL          *string    `json:"unrealized_pnl"`
	TotalPnL               *string    `json:"total_pnl"`
	ReturnRate             *string    `json:"return_rate"`
	RealizedDrawdown14D    string     `json:"realized_drawdown_14d"`
	DrawdownFrom           time.Time  `json:"drawdown_from"`
	DrawdownTo             time.Time  `json:"drawdown_to"`
	ValuationState         string     `json:"valuation_state"`
	LedgerObservedAt       time.Time  `json:"ledger_observed_at"`
	QuoteObservedFrom      *time.Time `json:"quote_observed_from"`
	QuoteObservedTo        *time.Time `json:"quote_observed_to"`
	HasReferencePrices     bool       `json:"has_reference_prices"`
	ValuationAssets        []Asset    `json:"valuation_assets"`
}
type Asset struct {
	ExecutionAccountID string     `json:"execution_account_id"`
	MarketSource       string     `json:"market_source"`
	MarketID           string     `json:"market_id"`
	ConditionID        string     `json:"condition_id"`
	TokenID            string     `json:"token_id"`
	OutcomeIndex       *int       `json:"outcome_index"`
	OutcomeName        *string    `json:"outcome_name"`
	MarketQuestion     *string    `json:"market_question"`
	RemainingShares    string     `json:"remaining_shares"`
	RemainingCost      string     `json:"remaining_cost"`
	Price              *string    `json:"price"`
	MarketValue        *string    `json:"market_value"`
	State              string     `json:"state"`
	PriceSource        *string    `json:"price_source"`
	SourceAt           *time.Time `json:"source_at"`
	ObservedAt         *time.Time `json:"observed_at"`
	Reason             *string    `json:"reason"`
}
type Service struct {
	facts   FactsReader
	quotes  QuoteReader
	now     func() time.Time
	mu      sync.Mutex
	cache   map[string]entry
	flights map[string]*flight
}
type entry struct {
	quote   Quote
	expires time.Time
}
type flight struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int
	quotes  map[string]Quote
}

func New(facts FactsReader, quotes QuoteReader) *Service {
	return &Service{facts: facts, quotes: quotes, now: time.Now, cache: map[string]entry{}, flights: map[string]*flight{}}
}
func (s *Service) Query(ctx context.Context, account string) (Response, error) {
	if s == nil || s.facts == nil {
		return Response{}, consolev2.ErrUnavailable
	}
	factsCtx, cancel := context.WithTimeout(ctx, time.Second)
	facts, err := s.facts.CoreMetricFacts(factsCtx, account)
	cancel()
	if err != nil {
		return Response{}, err
	}
	targets := []Target{}
	for _, a := range facts.Accounts {
		for _, asset := range a.Assets {
			if asset.SettlementPrice == nil {
				targets = append(targets, Target{Asset: asset, Address: a.FundingAddress})
			}
		}
	}
	quoteBudget := 3500 * time.Millisecond
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline) - 150*time.Millisecond
		if remaining < quoteBudget {
			quoteBudget = remaining
		}
	}
	quotes := map[string]Quote{}
	if len(targets) > 0 && quoteBudget > 0 {
		quoteCtx, c := context.WithTimeout(ctx, quoteBudget)
		quotes = s.readQuotes(quoteCtx, targets)
		c()
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return Response{}, ctx.Err()
	}
	result := Response{ObservedAt: facts.ObservedAt, Items: make([]Metrics, 0, len(facts.Accounts))}
	for _, f := range facts.Accounts {
		for _, raw := range []string{f.Metrics.PeakHoldingCost, f.Metrics.CumulativeInvestedCost, f.Metrics.RealizedPnL, f.Metrics.RealizedDrawdown14D} {
			if _, e := number(raw); e != nil {
				return Response{}, consolev2.ErrUnavailable
			}
		}
		m := Metrics{ExecutionAccountID: f.Metrics.ExecutionAccountID, MarketSource: f.Metrics.MarketSource, AssetCount: len(f.Assets), CurrentCost: "0", PeakHoldingCost: f.Metrics.PeakHoldingCost, CumulativeInvestedCost: f.Metrics.CumulativeInvestedCost, RealizedPnL: f.Metrics.RealizedPnL, RealizedDrawdown14D: f.Metrics.RealizedDrawdown14D, DrawdownFrom: f.Metrics.DrawdownFrom, DrawdownTo: f.Metrics.DrawdownTo, LedgerObservedAt: facts.ObservedAt, ValuationState: "complete", ValuationAssets: make([]Asset, 0, len(f.Assets))}
		cost, value := new(big.Rat), new(big.Rat)
		for _, a := range f.Assets {
			shares, e := number(a.RemainingShares)
			if e != nil || shares.Sign() < 0 {
				return Response{}, consolev2.ErrUnavailable
			}
			c, e := number(a.RemainingCost)
			if e != nil || c.Sign() < 0 {
				return Response{}, consolev2.ErrUnavailable
			}
			cost.Add(cost, c)
			q, ok := quotes[Key(Target{Asset: a, Address: f.FundingAddress})]
			if !ok {
				q = Unknown("timeout")
			}
			if a.SettlementPrice != nil {
				q = Priced(*a.SettlementPrice, "OFFICIAL_SETTLEMENT", facts.ObservedAt, a.SettlementAt)
			}
			q = validateQuote(q, s.now().UTC())
			item := Asset{ExecutionAccountID: a.ExecutionAccountID, MarketSource: a.MarketSource, MarketID: a.MarketID, ConditionID: a.ConditionID, TokenID: a.TokenID, OutcomeIndex: a.OutcomeIndex, OutcomeName: a.OutcomeName, MarketQuestion: a.MarketQuestion, RemainingShares: a.RemainingShares, RemainingCost: a.RemainingCost, State: "unpriced", Reason: q.Reason}
			if q.Price != nil {
				p, _ := number(*q.Price)
				v := new(big.Rat).Mul(shares, p)
				vtext := exact(v)
				item.Price = q.Price
				item.MarketValue = &vtext
				item.PriceSource = q.Source
				item.SourceAt = q.SourceAt
				item.ObservedAt = q.ObservedAt
				item.State = "priced"
				item.Reason = nil
				m.PricedAssetCount++
				value.Add(value, v)
				if *q.Source == "PM_CLOB_MID" || *q.Source == "KALSHI_BOOK_MID" {
					m.HasReferencePrices = true
				}
				if m.QuoteObservedFrom == nil || q.ObservedAt.Before(*m.QuoteObservedFrom) {
					v := *q.ObservedAt
					m.QuoteObservedFrom = &v
				}
				if m.QuoteObservedTo == nil || q.ObservedAt.After(*m.QuoteObservedTo) {
					v := *q.ObservedAt
					m.QuoteObservedTo = &v
				}
			}
			m.ValuationAssets = append(m.ValuationAssets, item)
		}
		m.CurrentCost = exact(cost)
		if m.PricedAssetCount == m.AssetCount {
			realized, e := number(m.RealizedPnL)
			if e != nil {
				return Response{}, consolev2.ErrUnavailable
			}
			peak, e := number(m.PeakHoldingCost)
			if e != nil || peak.Sign() < 0 {
				return Response{}, consolev2.ErrUnavailable
			}
			unrealized := new(big.Rat).Sub(value, cost)
			total := new(big.Rat).Add(realized, unrealized)
			v, u, t := exact(value), exact(unrealized), exact(total)
			m.MarketValue, m.UnrealizedPnL, m.TotalPnL = &v, &u, &t
			if peak.Sign() > 0 {
				r := new(big.Rat).Quo(total, peak).FloatString(24)
				m.ReturnRate = &r
			}
		} else {
			m.ValuationState = "incomplete"
		}
		result.Items = append(result.Items, m)
	}
	sort.Slice(result.Items, func(i, j int) bool {
		if result.Items[i].MarketSource != result.Items[j].MarketSource {
			return result.Items[i].MarketSource > result.Items[j].MarketSource
		}
		return result.Items[i].ExecutionAccountID < result.Items[j].ExecutionAccountID
	})
	encoded, e := json.Marshal(result)
	if e != nil || len(encoded) > 8*1024*1024 {
		return Response{}, consolev2.ErrUnavailable
	}
	return result, nil
}
func validateQuote(q Quote, now time.Time) Quote {
	if q.Price == nil {
		return qWithReason(q, "missing_price")
	}
	v, err := number(*q.Price)
	if err != nil || v.Sign() < 0 || v.Cmp(big.NewRat(1, 1)) > 0 {
		return Unknown("invalid_price")
	}
	if q.Source == nil || q.ObservedAt == nil || q.ObservedAt.After(now) {
		return Unknown("invalid_price")
	}
	switch *q.Source {
	case "PM_CURRENT_PRICE", "PM_CLOB_MID", "KALSHI_BOOK_MID":
		if now.Sub(*q.ObservedAt) > 30*time.Second {
			return Unknown("stale")
		}
	case "OFFICIAL_SETTLEMENT":
		if q.SourceAt != nil && q.SourceAt.After(now) {
			return Unknown("unconfirmed_settlement")
		}
	default:
		return Unknown("invalid_price")
	}
	return q
}
func qWithReason(q Quote, fallback string) Quote {
	if q.Reason == nil {
		return Unknown(fallback)
	}
	return Unknown(*q.Reason)
}
func number(s string) (*big.Rat, error) {
	d, e := domain.ParseDecimal(s)
	if e != nil {
		return nil, e
	}
	v, ok := new(big.Rat).SetString(d.String())
	if !ok {
		return nil, consolev2.ErrUnavailable
	}
	return v, nil
}
func exact(v *big.Rat) string {
	den := new(big.Int).Set(v.Denom())
	two, five := 0, 0
	r := new(big.Int)
	for {
		q, _ := new(big.Int).QuoRem(den, big.NewInt(2), r)
		if r.Sign() != 0 {
			break
		}
		den = q
		two++
	}
	for {
		q, _ := new(big.Int).QuoRem(den, big.NewInt(5), r)
		if r.Sign() != 0 {
			break
		}
		den = q
		five++
	}
	places := two
	if five > places {
		places = five
	}
	if den.Cmp(big.NewInt(1)) != 0 {
		places = 24
	}
	text := v.FloatString(places)
	if strings.Contains(text, ".") {
		text = strings.TrimRight(strings.TrimRight(text, "0"), ".")
	}
	if text == "-0" {
		return "0"
	}
	return text
}

func (s *Service) readQuotes(ctx context.Context, targets []Target) map[string]Quote {
	result := map[string]Quote{}
	missing := []Target{}
	s.mu.Lock()
	for _, t := range targets {
		k := Key(t)
		if c, ok := s.cache[k]; ok && s.now().Before(c.expires) {
			result[k] = cloneQuote(c.quote)
		} else {
			missing = append(missing, t)
		}
	}
	if len(missing) == 0 {
		s.mu.Unlock()
		return result
	}
	keys := make([]string, 0, len(missing))
	for _, t := range missing {
		keys = append(keys, Key(t))
	}
	sort.Strings(keys)
	key := strings.Join(keys, "\n")
	f := s.flights[key]
	if f == nil {
		shared, cancel := context.WithTimeout(context.Background(), 3500*time.Millisecond)
		f = &flight{done: make(chan struct{}), cancel: cancel, quotes: map[string]Quote{}}
		s.flights[key] = f
		go func() {
			defer cancel()
			qs := map[string]Quote{}
			if incremental, ok := s.quotes.(QuoteUpdateReader); ok {
				publish := func(completed map[string]Quote) {
					now := s.now().UTC()
					s.mu.Lock()
					defer s.mu.Unlock()
					for _, target := range missing {
						k := Key(target)
						q, found := completed[k]
						if !found {
							continue
						}
						q = validateQuote(q, now)
						if q.Price != nil {
							q = cloneQuote(q)
							f.quotes[k] = q
							if !errors.Is(shared.Err(), context.Canceled) {
								s.cache[k] = entry{quote: cloneQuote(q), expires: quoteExpiry(q, now)}
							}
						}
					}
				}
				qs = incremental.QuotesWithUpdates(shared, missing, publish)
			} else if s.quotes != nil {
				qs = s.quotes.Quotes(shared, missing)
			}
			if qs == nil {
				qs = map[string]Quote{}
			}
			now := s.now().UTC()
			s.mu.Lock()
			for _, t := range missing {
				k := Key(t)
				q, ok := qs[k]
				if !ok {
					q = Unknown("source_unavailable")
				}
				q = validateQuote(q, now)
				if !errors.Is(shared.Err(), context.Canceled) {
					s.cache[k] = entry{quote: cloneQuote(q), expires: quoteExpiry(q, now)}
				}
				qs[k] = q
			}
			for len(s.cache) > 4000 {
				for k := range s.cache {
					delete(s.cache, k)
					break
				}
			}
			for k, q := range qs {
				f.quotes[k] = q
			}
			if s.flights[key] == f {
				delete(s.flights, key)
			}
			close(f.done)
			s.mu.Unlock()
		}()
	}
	f.waiters++
	s.mu.Unlock()
	select {
	case <-f.done:
		s.mu.Lock()
		for k, q := range f.quotes {
			result[k] = cloneQuote(q)
		}
		f.waiters--
		s.mu.Unlock()
	case <-ctx.Done():
		s.mu.Lock()
		for k, q := range f.quotes {
			result[k] = cloneQuote(q)
		}
		f.waiters--
		if f.waiters == 0 {
			f.cancel()
			if s.flights[key] == f {
				delete(s.flights, key)
			}
		}
		s.mu.Unlock()
	}
	return result
}

func cloneQuote(q Quote) Quote {
	if q.Price != nil {
		v := *q.Price
		q.Price = &v
	}
	if q.Source != nil {
		v := *q.Source
		q.Source = &v
	}
	if q.Reason != nil {
		v := *q.Reason
		q.Reason = &v
	}
	if q.SourceAt != nil {
		v := *q.SourceAt
		q.SourceAt = &v
	}
	if q.ObservedAt != nil {
		v := *q.ObservedAt
		q.ObservedAt = &v
	}
	return q
}

func quoteExpiry(q Quote, now time.Time) time.Time {
	if q.Price == nil {
		return now.Add(3 * time.Second)
	}
	expiry := now.Add(30 * time.Second)
	if q.Source != nil && *q.Source != "OFFICIAL_SETTLEMENT" && q.ObservedAt != nil {
		observedExpiry := q.ObservedAt.Add(30 * time.Second)
		if observedExpiry.Before(expiry) {
			expiry = observedExpiry
		}
	}
	return expiry
}
