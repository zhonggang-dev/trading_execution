// Package accountquotes implements only market-data reads for managed assets.
package accountquotes

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	core "github.com/UniPat-AI/trading_execution/internal/service/accountcoremetrics"
)

type AddressPositions interface {
	ListValuationPrices(context.Context, string) ([]domain.ExternalPosition, error)
}
type KalshiMarketReader interface {
	CoreMarkets(context.Context, []string) ([]KalshiMarket, error)
}
type KalshiMarket struct {
	Ticker          string     `json:"ticker"`
	MarketType      string     `json:"market_type"`
	Status          string     `json:"status"`
	Result          string     `json:"result"`
	SettlementValue *string    `json:"settlement_value_dollars"`
	SettlementAt    *time.Time `json:"settlement_ts"`
	LastPrice       *string    `json:"last_price_dollars"`
	Volume          *string    `json:"volume_fp"`
	YesBid          *string    `json:"yes_bid_dollars"`
	YesAsk          *string    `json:"yes_ask_dollars"`
	NoBid           *string    `json:"no_bid_dollars"`
	NoAsk           *string    `json:"no_ask_dollars"`
	YesBidSize      *string    `json:"yes_bid_size_fp"`
	YesAskSize      *string    `json:"yes_ask_size_fp"`
}

type Source struct {
	Positions  AddressPositions
	Kalshi     KalshiMarketReader
	CLOBURL    string
	GammaURL   string
	HTTPClient *http.Client
	Now        func() time.Time
}

// LimitTransport is shared by the dedicated valuation HTTP clients, bounding
// all platform requests together. It neither shares nor locks execution clients.
type LimitTransport struct {
	Base      http.RoundTripper
	semaphore chan struct{}
}

func NewLimitTransport(base http.RoundTripper) *LimitTransport {
	if base == nil {
		base = http.DefaultTransport
	}
	return &LimitTransport{Base: base, semaphore: make(chan struct{}, 2)}
}
func (t *LimitTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	select {
	case t.semaphore <- struct{}{}:
	case <-r.Context().Done():
		return nil, r.Context().Err()
	}
	response, err := t.Base.RoundTrip(r)
	if err != nil {
		<-t.semaphore
		return nil, err
	}
	response.Body = &limitedBody{ReadCloser: response.Body, release: func() { <-t.semaphore }}
	return response, nil
}
func (s *Source) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}
func (s *Source) Quotes(ctx context.Context, targets []core.Target) map[string]core.Quote {
	return s.QuotesWithUpdates(ctx, targets, nil)
}

// QuotesWithUpdates publishes completed valid prices without waiting for an
// unrelated source or fallback to exhaust the caller's remaining budget.
func (s *Source) QuotesWithUpdates(ctx context.Context, targets []core.Target, emit func(map[string]core.Quote)) map[string]core.Quote {
	result := map[string]core.Quote{}
	var mu sync.Mutex
	groups := map[string][]core.Target{}
	ks := []core.Target{}
	for _, t := range targets {
		switch t.Asset.MarketSource {
		case "POLYMARKET":
			groups[t.Address] = append(groups[t.Address], t)
		case "KALSHI":
			ks = append(ks, t)
		default:
			result[core.Key(t)] = core.Unknown("identity_mismatch")
		}
	}
	settlementsDone := make(chan struct{})
	var settlements map[string]core.Quote
	go func() { settlements = s.pmSettlements(ctx, targets, emit); close(settlementsDone) }()
	var pmWait, all sync.WaitGroup
	for address, group := range groups {
		address, group := address, group
		pmWait.Add(1)
		go func() {
			defer pmWait.Done()
			q := s.pm(ctx, address, group)
			if emit != nil {
				emit(q)
			}
			<-settlementsDone
			for _, target := range group {
				if quote, ok := settlements[core.Key(target)]; ok {
					q[core.Key(target)] = quote
				}
			}
			mu.Lock()
			for k, v := range q {
				result[k] = v
			}
			mu.Unlock()
			if emit != nil {
				emit(q)
			}
		}()
	}
	all.Add(1)
	go func() {
		defer all.Done()
		pmWait.Wait()
		missing := []core.Target{}
		pmResults := map[string]core.Quote{}
		mu.Lock()
		for _, t := range targets {
			if t.Asset.MarketSource != "POLYMARKET" {
				continue
			}
			q := result[core.Key(t)]
			pmResults[core.Key(t)] = q
			if q.Price == nil && (q.Reason == nil || *q.Reason != "identity_mismatch") {
				missing = append(missing, t)
			}
		}
		mu.Unlock()
		if len(missing) > 0 && ctx.Err() == nil {
			s.pmBooks(ctx, missing, pmResults, emit)
		}
		mu.Lock()
		for k, v := range pmResults {
			result[k] = v
		}
		mu.Unlock()
	}()
	if len(ks) > 0 {
		all.Add(1)
		go func() {
			defer all.Done()
			q := s.kalshi(ctx, ks, emit)
			mu.Lock()
			for k, v := range q {
				result[k] = v
			}
			mu.Unlock()
		}()
	}
	all.Wait()
	return result
}
func (s *Source) pm(ctx context.Context, address string, targets []core.Target) map[string]core.Quote {
	result := map[string]core.Quote{}
	positions := []domain.ExternalPosition{}
	var err error
	if s.Positions == nil || !validFundingAddress(address) {
		err = errors.New("source unavailable")
	} else {
		positions, err = s.Positions.ListValuationPrices(ctx, address)
	}
	byToken := map[string]domain.ExternalPosition{}
	for _, p := range positions {
		byToken[p.TokenID] = p
	}
	for _, t := range targets {
		k := core.Key(t)
		p, ok := byToken[t.Asset.TokenID]
		if err != nil {
			result[k] = core.Unknown(errorReason(err))
			continue
		}
		if !ok || p.CurrentPrice.IsEmpty() {
			result[k] = core.Unknown("missing_price")
			continue
		}
		if p.ConditionID != t.Asset.ConditionID || !sameIndex(p.OutcomeIndex, t.Asset.OutcomeIndex) || !sameName(p.OutcomeName, t.Asset.OutcomeName) {
			result[k] = core.Unknown("identity_mismatch")
			continue
		}
		price, e := price(p.CurrentPrice.String())
		if e != nil {
			result[k] = core.Unknown("invalid_price")
			continue
		}
		source := "PM_CURRENT_PRICE"
		if p.Redeemable && (price == "0" || price == "1") {
			source = "OFFICIAL_SETTLEMENT"
		}
		observed := p.ObservedAt
		if observed.IsZero() {
			observed = s.now()
		}
		result[k] = core.Priced(price, source, observed, nil)
	}
	return result
}

type level struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}
type book struct {
	AssetID   string          `json:"asset_id"`
	Market    string          `json:"market"`
	Timestamp json.RawMessage `json:"timestamp"`
	Bids      []level         `json:"bids"`
	Asks      []level         `json:"asks"`
}

func (s *Source) pmBooks(ctx context.Context, targets []core.Target, result map[string]core.Quote, emit func(map[string]core.Quote)) {
	byToken := map[string][]core.Target{}
	for _, t := range targets {
		byToken[t.Asset.TokenID] = append(byToken[t.Asset.TokenID], t)
	}
	tokens := make([]string, 0, len(byToken))
	for token := range byToken {
		tokens = append(tokens, token)
	}
	sort.Strings(tokens)
	for start := 0; start < len(tokens); start += 500 {
		end := start + 500
		if end > len(tokens) {
			end = len(tokens)
		}
		body := []map[string]string{}
		for _, token := range tokens[start:end] {
			body = append(body, map[string]string{"token_id": token})
		}
		encoded, _ := json.Marshal(body)
		base, err := url.Parse(s.CLOBURL)
		if err != nil || base.Host == "" {
			continue
		}
		endpoint := base.ResolveReference(&url.URL{Path: "/books"})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(encoded))
		if err != nil {
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		requestCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		req = req.WithContext(requestCtx)
		books := []book{}
		err = s.read(req, &books)
		cancel()
		if err != nil {
			for _, token := range tokens[start:end] {
				for _, t := range byToken[token] {
					result[core.Key(t)] = core.Unknown(errorReason(err))
				}
			}
			continue
		}
		allowed := map[string]bool{}
		for _, token := range tokens[start:end] {
			allowed[token] = true
		}
		seen := map[string]bool{}
		invalid := false
		for _, b := range books {
			if seen[b.AssetID] || !allowed[b.AssetID] {
				invalid = true
			}
			seen[b.AssetID] = true
		}
		if invalid {
			for _, token := range tokens[start:end] {
				for _, t := range byToken[token] {
					result[core.Key(t)] = core.Unknown("identity_mismatch")
				}
			}
			continue
		}
		observed := s.now()
		for _, b := range books {
			for _, t := range byToken[b.AssetID] {
				k := core.Key(t)
				if b.Market != t.Asset.ConditionID {
					result[k] = core.Unknown("identity_mismatch")
					continue
				}
				mid, reason := midpoint(b.Bids, b.Asks)
				if reason != "" {
					result[k] = core.Unknown(reason)
					continue
				}
				var sourceAt *time.Time
				if len(b.Timestamp) > 0 {
					var text string
					if json.Unmarshal(b.Timestamp, &text) != nil {
						text = string(b.Timestamp)
					}
					if millis, e := strconv.ParseInt(text, 10, 64); e == nil {
						at := time.UnixMilli(millis).UTC()
						if at.After(observed) {
							result[k] = core.Unknown("invalid_price")
							continue
						}
						sourceAt = &at
					}
				}
				result[k] = core.Priced(mid, "PM_CLOB_MID", observed, sourceAt)
			}
		}
		if emit != nil {
			emit(copyQuoteMap(result))
		}
	}
}
func (s *Source) read(req *http.Request, dest any) error {
	client := s.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	r, err := client.Do(req)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return errors.New("quote unavailable")
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 8*1024*1024+1))
	if err != nil {
		return err
	}
	if len(body) > 8*1024*1024 {
		return errors.New("quote response too large")
	}
	return json.Unmarshal(body, dest)
}
func midpoint(bids, asks []level) (string, string) {
	bestBid, bestAsk := (*big.Rat)(nil), (*big.Rat)(nil)
	for side, levels := range [][]level{bids, asks} {
		for _, l := range levels {
			if len(l.Size) > 256 || strings.ContainsAny(l.Size, "/eE") {
				return "", "invalid_price"
			}
			size, ok := new(big.Rat).SetString(l.Size)
			if !ok || size.Sign() < 0 {
				return "", "invalid_price"
			}
			if size.Sign() == 0 {
				continue
			}
			p, e := price(l.Price)
			if e != nil {
				return "", "invalid_price"
			}
			v, _ := new(big.Rat).SetString(p)
			if side == 0 {
				if bestBid == nil || v.Cmp(bestBid) > 0 {
					bestBid = v
				}
			} else if bestAsk == nil || v.Cmp(bestAsk) < 0 {
				bestAsk = v
			}
		}
	}
	if bestBid == nil || bestAsk == nil {
		return "", "one_sided"
	}
	if bestBid.Cmp(bestAsk) > 0 {
		return "", "invalid_price"
	}
	return ratText(new(big.Rat).Quo(new(big.Rat).Add(bestBid, bestAsk), big.NewRat(2, 1))), ""
}
func price(text string) (string, error) {
	if len(text) > 256 {
		return "", errors.New("price too long")
	}
	d, e := domain.ParseDecimal(text)
	if e != nil {
		return "", e
	}
	v, ok := new(big.Rat).SetString(d.String())
	if !ok || v.Sign() < 0 || v.Cmp(big.NewRat(1, 1)) > 0 {
		return "", errors.New("invalid price")
	}
	return d.Canonical().String(), nil
}
func ratText(v *big.Rat) string {
	den := new(big.Int).Set(v.Denom())
	counts := [2]int{}
	for i, factor := range []int64{2, 5} {
		for {
			q, r := new(big.Int).QuoRem(den, big.NewInt(factor), new(big.Int))
			if r.Sign() != 0 {
				break
			}
			den = q
			counts[i]++
		}
	}
	scale := counts[0]
	if counts[1] > scale {
		scale = counts[1]
	}
	text := v.FloatString(scale)
	if strings.Contains(text, ".") {
		text = strings.TrimRight(strings.TrimRight(text, "0"), ".")
	}
	return text
}
func sameIndex(a, b *int) bool { return (a == nil && b == nil) || (a != nil && b != nil && *a == *b) }
func sameName(a string, b *string) bool {
	return b != nil && strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(*b))
}
func errorReason(err error) string {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "timeout"
	}
	var e *port.VenueError
	if errors.As(err, &e) && strings.Contains(e.Code, "TIMEOUT") {
		return "timeout"
	}
	return "source_unavailable"
}
func (s *Source) kalshi(ctx context.Context, targets []core.Target, emit func(map[string]core.Quote)) map[string]core.Quote {
	result := map[string]core.Quote{}
	byTicker := map[string][]core.Target{}
	for _, t := range targets {
		byTicker[t.Asset.MarketID] = append(byTicker[t.Asset.MarketID], t)
		result[core.Key(t)] = core.Unknown("missing_price")
	}
	tickers := make([]string, 0, len(byTicker))
	for t := range byTicker {
		tickers = append(tickers, t)
	}
	sort.Strings(tickers)
	if s.Kalshi == nil {
		for _, t := range targets {
			result[core.Key(t)] = core.Unknown("source_unavailable")
		}
		return result
	}
	for start := 0; start < len(tickers); start += 100 {
		end := start + 100
		if end > len(tickers) {
			end = len(tickers)
		}
		requestCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		markets, err := s.Kalshi.CoreMarkets(requestCtx, tickers[start:end])
		cancel()
		if err != nil {
			for _, ticker := range tickers[start:end] {
				for _, t := range byTicker[ticker] {
					result[core.Key(t)] = core.Unknown(errorReason(err))
				}
			}
			continue
		}
		allowed := map[string]bool{}
		for _, ticker := range tickers[start:end] {
			allowed[ticker] = true
		}
		seen := map[string]bool{}
		invalid := false
		for _, m := range markets {
			if seen[m.Ticker] || !allowed[m.Ticker] {
				invalid = true
			}
			seen[m.Ticker] = true
		}
		if invalid {
			for _, ticker := range tickers[start:end] {
				for _, t := range byTicker[ticker] {
					result[core.Key(t)] = core.Unknown("identity_mismatch")
				}
			}
			continue
		}
		observed := s.now()
		for _, m := range markets {
			for _, t := range byTicker[m.Ticker] {
				k := core.Key(t)
				if m.MarketType != "binary" || t.Asset.OutcomeIndex == nil || (*t.Asset.OutcomeIndex != 0 && *t.Asset.OutcomeIndex != 1) {
					result[k] = core.Unknown("identity_mismatch")
					continue
				}
				yes := *t.Asset.OutcomeIndex == 0
				// Token convention is produced by the existing Kalshi adapter, never by
				// the browser. Check it rather than trusting an outcome index on its own.
				expected := "kalshi:" + m.Ticker + ":YES"
				if !yes {
					expected = "kalshi:" + m.Ticker + ":NO"
				}
				if t.Asset.TokenID != expected || t.Asset.ConditionID != "kalshi:"+m.Ticker {
					result[k] = core.Unknown("identity_mismatch")
					continue
				}
				if m.Status == "finalized" {
					if (m.Result != "yes" && m.Result != "no") || m.SettlementValue == nil || m.SettlementAt == nil || m.SettlementAt.After(observed) {
						result[k] = core.Unknown("unconfirmed_settlement")
						continue
					}
					p, e := price(*m.SettlementValue)
					want := "0"
					if m.Result == "yes" {
						want = "1"
					}
					if e != nil || p != want {
						result[k] = core.Unknown("unconfirmed_settlement")
						continue
					}
					if !yes {
						if p == "0" {
							p = "1"
						} else {
							p = "0"
						}
					}
					result[k] = core.Priced(p, "OFFICIAL_SETTLEMENT", observed, m.SettlementAt)
					continue
				}
				// last_price_dollars is a YES price, including for NO holdings.
				// Positive traded volume distinguishes a real zero price from the
				// default value of a market that has never traded. Market updated_time
				// is metadata time, not trade time, so SourceAt stays unknown.
				if m.LastPrice != nil && m.Volume != nil && len(*m.Volume) <= 256 {
					volume, err := domain.ParseDecimal(*m.Volume)
					if sign, signErr := volume.Sign(); err == nil && signErr == nil && sign > 0 {
						if p, priceErr := price(*m.LastPrice); priceErr == nil {
							if !yes {
								value, _ := new(big.Rat).SetString(p)
								p = ratText(new(big.Rat).Sub(big.NewRat(1, 1), value))
							}
							result[k] = core.Priced(p, "KALSHI_LAST_TRADE", observed, nil)
							continue
						}
					}
				}
				bid, ask, bidSize, askSize := m.YesBid, m.YesAsk, m.YesBidSize, m.YesAskSize
				if !yes {
					bid, ask, bidSize, askSize = m.NoBid, m.NoAsk, m.YesAskSize, m.YesBidSize
				}
				if m.YesBid != nil && m.YesAsk != nil && m.NoBid != nil && m.NoAsk != nil {
					yb, e1 := price(*m.YesBid)
					ya, e2 := price(*m.YesAsk)
					nb, e3 := price(*m.NoBid)
					na, e4 := price(*m.NoAsk)
					if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
						result[k] = core.Unknown("invalid_price")
						continue
					}
					ybr, _ := new(big.Rat).SetString(yb)
					yar, _ := new(big.Rat).SetString(ya)
					nbr, _ := new(big.Rat).SetString(nb)
					nar, _ := new(big.Rat).SetString(na)
					if new(big.Rat).Add(yar, nbr).Cmp(big.NewRat(1, 1)) != 0 || new(big.Rat).Add(ybr, nar).Cmp(big.NewRat(1, 1)) != 0 {
						result[k] = core.Unknown("invalid_price")
						continue
					}
				}
				if bid == nil || ask == nil || bidSize == nil || askSize == nil {
					result[k] = core.Unknown("one_sided")
					continue
				}
				mid, reason := midpoint([]level{{Price: *bid, Size: *bidSize}}, []level{{Price: *ask, Size: *askSize}})
				if reason != "" {
					result[k] = core.Unknown(reason)
					continue
				}
				result[k] = core.Priced(mid, "KALSHI_BOOK_MID", observed, nil)
			}
		}
		if emit != nil {
			emit(copyQuoteMap(result))
		}
	}
	return result
}

type limitedBody struct {
	io.ReadCloser
	once    sync.Once
	release func()
}

func (b *limitedBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	if e != nil {
		b.once.Do(b.release)
	}
	return n, e
}
func (b *limitedBody) Close() error { e := b.ReadCloser.Close(); b.once.Do(b.release); return e }

func validFundingAddress(address string) bool {
	if len(address) != 42 || !strings.HasPrefix(address, "0x") {
		return false
	}
	_, err := hex.DecodeString(address[2:])
	return err == nil
}

func copyQuoteMap(m map[string]core.Quote) map[string]core.Quote {
	r := make(map[string]core.Quote, len(m))
	for k, v := range m {
		r[k] = v
	}
	return r
}
