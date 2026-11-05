package accountprices

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	h "github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
)

func (s *Source) kalshiMarket(ctx context.Context, ticker string) (map[string]json.RawMessage, bool, error) {
	s.cacheMu.Lock()
	cached := s.markets[ticker]
	s.cacheMu.Unlock()
	if cached != nil {
		return cached, text(cached["_archived"]) == "true", nil
	}
	var envelope struct {
		Market map[string]json.RawMessage `json:"market"`
	}
	archived := false
	err := s.get(ctx, s.p.KalshiBaseURL, "/markets/"+url.PathEscape(ticker), nil, &envelope)
	if err == errNotFound {
		archived = true
		err = s.get(ctx, s.p.KalshiBaseURL, "/historical/markets/"+url.PathEscape(ticker), nil, &envelope)
	}
	if err != nil {
		return nil, false, err
	}
	if text(envelope.Market["ticker"]) != ticker {
		return nil, false, fmt.Errorf("Kalshi market identity mismatch")
	}
	if archived {
		envelope.Market["_archived"] = json.RawMessage(`true`)
	}
	s.cacheMu.Lock()
	s.markets[ticker] = envelope.Market
	s.cacheMu.Unlock()
	return envelope.Market, archived, nil
}
func (s *Source) kalshiCutoff(ctx context.Context) (time.Time, error) {
	s.cacheMu.Lock()
	at := s.cutoff
	s.cacheMu.Unlock()
	if !at.IsZero() {
		return at, nil
	}
	var err error
	at, _, err = s.readKalshiCutoffs(ctx)
	if err != nil {
		return time.Time{}, err
	}
	s.cacheMu.Lock()
	s.cutoff = at
	s.cacheMu.Unlock()
	return at, nil
}

func (s *Source) readKalshiCutoffs(ctx context.Context) (time.Time, time.Time, error) {
	var envelope map[string]json.RawMessage
	if err := s.get(ctx, s.p.KalshiBaseURL, "/historical/cutoff", nil, &envelope); err != nil {
		return time.Time{}, time.Time{}, err
	}
	market, ok := timestamp(envelope["market_settled_ts"])
	if !ok {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid Kalshi historical cutoff")
	}
	trades, ok := timestamp(envelope["trades_created_ts"])
	if !ok {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid Kalshi trade cutoff")
	}
	return market, trades, nil
}

type kalshiCandle struct {
	At    int64                      `json:"end_period_ts"`
	Price map[string]json.RawMessage `json:"price"`
	Bid   map[string]json.RawMessage `json:"yes_bid"`
	Ask   map[string]json.RawMessage `json:"yes_ask"`
}

func field(m map[string]json.RawMessage, n string) string {
	v := text(m[n+"_dollars"])
	if v == "" || v == "null" {
		v = text(m[n])
	}
	return v
}
func (s *Source) kalshi(ctx context.Context, w h.Window, series *h.Series) error {
	no := false
	if w.Asset.OutcomeName != nil {
		switch strings.ToLower(*w.Asset.OutcomeName) {
		case "yes":
		case "no":
			no = true
		default:
			return fmt.Errorf("unknown Kalshi outcome")
		}
	} else {
		return fmt.Errorf("Kalshi outcome missing")
	}
	expected := "YES"
	index := 0
	if no {
		expected = "NO"
		index = 1
	}
	if w.Asset.TokenID != "kalshi:"+w.Asset.MarketID+":"+expected || (w.Asset.OutcomeIndex != nil && *w.Asset.OutcomeIndex != index) {
		return fmt.Errorf("Kalshi outcome identity mismatch")
	}
	market, archived, err := s.kalshiMarket(ctx, w.Asset.MarketID)
	if err != nil {
		return err
	}
	cutoff, err := s.kalshiCutoff(ctx)
	if err != nil {
		return err
	}
	settlementAt, hasSettlement := timestamp(market["settlement_ts"])
	if hasSettlement && settlementAt.Before(cutoff) {
		archived = true
	}
	if text(market["status"]) == "finalized" && hasSettlement {
		yes := ""
		switch strings.ToLower(text(market["result"])) {
		case "yes":
			yes = "1"
		case "no":
			yes = "0"
		}
		if yes != "" {
			value := text(market["settlement_value_dollars"])
			if value != "" && value != "null" {
				n, ok := new(big.Rat).SetString(value)
				want, _ := new(big.Rat).SetString(yes)
				if !ok || n.Cmp(want) != 0 {
					return fmt.Errorf("Kalshi settlement result mismatch")
				}
			}
			price := yes
			if no {
				price, _ = subtractOne(price)
			}
			series.Settlements = append(series.Settlements, h.Settlement{EffectiveAt: settlementAt, Price: price, Source: "OFFICIAL_SETTLEMENT", SourceReference: w.Asset.MarketID + ":" + settlementAt.Format(time.RFC3339Nano), FetchedAt: s.p.Now().UTC()})
		}
	}
	q := url.Values{"start_ts": {strconv.FormatInt(w.From.Truncate(time.Hour).Add(-time.Hour).Unix(), 10)}, "end_ts": {strconv.FormatInt(w.To.Unix(), 10)}, "period_interval": {"60"}, "include_latest_before_start": {"true"}}
	candles := []kalshiCandle{}
	if archived {
		var e struct {
			Ticker  string         `json:"ticker"`
			Candles []kalshiCandle `json:"candlesticks"`
		}
		if err = s.get(ctx, s.p.KalshiBaseURL, "/historical/markets/"+url.PathEscape(w.Asset.MarketID)+"/candlesticks", q, &e); err != nil {
			return err
		}
		if e.Candles == nil {
			return fmt.Errorf("Kalshi archive omitted candles")
		}
		if e.Ticker != w.Asset.MarketID {
			return fmt.Errorf("Kalshi archive identity mismatch")
		}
		candles = e.Candles
	} else {
		q.Set("market_tickers", w.Asset.MarketID)
		var e struct {
			Markets []struct {
				Ticker  string         `json:"market_ticker"`
				Candles []kalshiCandle `json:"candlesticks"`
			} `json:"markets"`
		}
		if err = s.get(ctx, s.p.KalshiBaseURL, "/markets/candlesticks", q, &e); err != nil {
			return err
		}
		if len(e.Markets) != 1 || e.Markets[0].Ticker != w.Asset.MarketID {
			return fmt.Errorf("Kalshi candle identity mismatch")
		}
		candles = e.Markets[0].Candles
	}
	if len(candles) > 10000 {
		return fmt.Errorf("Kalshi candle budget exceeded")
	}
	for _, c := range candles {
		at := time.Unix(c.At, 0).UTC()
		if c.At <= 0 || at.After(w.To) {
			continue
		}
		price := field(c.Price, "close")
		source := "KALSHI_TRADE_CLOSE"
		if !h.ValidPrice(price) {
			price = field(c.Price, "previous")
			source = "KALSHI_PREVIOUS_TRADE"
		}
		if !h.ValidPrice(price) {
			bid, ask := field(c.Bid, "close"), field(c.Ask, "close")
			b, bok := new(big.Rat).SetString(bid)
			a, aok := new(big.Rat).SetString(ask)
			if bok && aok && h.ValidPrice(bid) && h.ValidPrice(ask) && b.Sign() > 0 && a.Cmp(big.NewRat(1, 1)) < 0 && b.Cmp(a) <= 0 {
				price = new(big.Rat).Quo(new(big.Rat).Add(b, a), big.NewRat(2, 1)).FloatString(12)
				source = "KALSHI_BOOK_MID"
			}
		}
		if !h.ValidPrice(price) {
			continue
		}
		if no {
			price, _ = subtractOne(price)
		}
		series.Points = append(series.Points, h.Point{At: at, Price: price, FetchedAt: s.p.Now().UTC(), ResolutionSeconds: 3600, Source: source})
	}
	return s.fillKalshiGaps(ctx, w, series, no)
}
func (s *Source) fillKalshiGaps(ctx context.Context, w h.Window, series *h.Series, no bool) error {
	h.SortSeries(series)
	// Merge missing marks into windows anchored at their last known price. An
	// empty complete trade response is positive no-trade evidence, not a timeout.
	type gap struct{ from, to time.Time }
	gaps := []gap{}
	for _, at := range marks(w) {
		if _, ok := h.PriceAt(*series, at); ok {
			continue
		}
		i := sort.Search(len(series.Points), func(i int) bool { return series.Points[i].At.After(at) }) - 1
		if i < 0 {
			continue
		}
		p := series.Points[i]
		if p.Source == "KALSHI_BOOK_MID" {
			continue
		}
		if len(gaps) > 0 && gaps[len(gaps)-1].from.Equal(p.At) {
			gaps[len(gaps)-1].to = at
		} else {
			gaps = append(gaps, gap{p.At, at})
		}
	}
	for _, g := range gaps {
		trades, e := s.kalshiTrades(ctx, w.Asset.MarketID, g.from, g.to)
		if e != nil {
			return e
		}
		tradePoints := []h.Point{}
		for _, trade := range trades {
			at, ok := timestamp(trade["created_time"])
			if !ok || at.Before(g.from.Truncate(time.Second)) || at.After(ceilHistorySecond(g.to)) {
				return fmt.Errorf("Kalshi trade time outside requested range")
			}
			if text(trade["ticker"]) != w.Asset.MarketID {
				return fmt.Errorf("Kalshi trade identity mismatch")
			}
			// The API accepts whole-second bounds. Query a superset, then
			// exclude later fractions rather than using future transactions.
			if at.Before(g.from) || at.After(g.to) {
				continue
			}
			price := text(trade["yes_price_dollars"])
			if !h.ValidPrice(price) {
				return fmt.Errorf("invalid Kalshi trade price")
			}
			if no {
				price, _ = subtractOne(price)
			}
			sourceAt, until := at, g.to
			tradePoints = append(tradePoints, h.Point{At: at, Price: price, SourceAt: &sourceAt, FetchedAt: s.p.Now().UTC(), ResolutionSeconds: 0, Source: "KALSHI_PUBLIC_TRADE", ValidUntil: &until})
		}
		sort.Slice(tradePoints, func(i, j int) bool { return tradePoints[i].At.Before(tradePoints[j].At) })
		firstUntil := g.to
		if len(tradePoints) > 0 {
			firstUntil = tradePoints[0].At
		}
		for i := range series.Points {
			if series.Points[i].At.Equal(g.from) && series.Points[i].Source != "KALSHI_BOOK_MID" {
				until := firstUntil
				if len(tradePoints) == 0 && series.Points[i].ValidUntil != nil && series.Points[i].ValidUntil.After(until) {
					until = *series.Points[i].ValidUntil
				}
				series.Points[i].ValidUntil = &until
				series.Points[i].FetchedAt = s.p.Now().UTC()
			}
		}
		for i := range tradePoints {
			if i+1 < len(tradePoints) {
				until := tradePoints[i+1].At
				tradePoints[i].ValidUntil = &until
			}
		}
		series.Points = append(series.Points, tradePoints...)
	}
	return nil
}
func (s *Source) kalshiTrades(ctx context.Context, ticker string, from, to time.Time) ([]map[string]json.RawMessage, error) {
	// A long backfill may straddle the daily archive handoff. An empty live
	// response is only proof of no trades if the partition remained stable.
	for attempt := 0; attempt < 3; attempt++ {
		_, cutoff, err := s.readKalshiCutoffs(ctx)
		if err != nil {
			return nil, err
		}
		trades, err := s.kalshiTradesAtCutoff(ctx, ticker, from, to, cutoff)
		if err != nil {
			return nil, err
		}
		_, after, err := s.readKalshiCutoffs(ctx)
		if err != nil {
			return nil, err
		}
		if cutoff.Equal(after) {
			return trades, nil
		}
	}
	return nil, fmt.Errorf("Kalshi trade archive changed during historical read")
}

func (s *Source) kalshiTradesAtCutoff(ctx context.Context, ticker string, from, to, cutoff time.Time) ([]map[string]json.RawMessage, error) {
	result := []map[string]json.RawMessage{}
	if from.Before(cutoff) {
		end := to
		if cutoff.Before(end) {
			end = cutoff
		}
		rows, err := s.kalshiTradesPart(ctx, "/historical/trades", ticker, from, end)
		if err != nil {
			return nil, err
		}
		result = append(result, rows...)
	}
	if !to.Before(cutoff) {
		start := from
		if cutoff.After(start) {
			start = cutoff
		}
		rows, err := s.kalshiTradesPart(ctx, "/markets/trades", ticker, start, to)
		if err != nil {
			return nil, err
		}
		result = append(result, rows...)
	}
	// Inclusive source boundaries can overlap at the handoff.
	unique := []map[string]json.RawMessage{}
	seen := map[string]bool{}
	for _, trade := range result {
		key := text(trade["trade_id"])
		if key == "" {
			raw, _ := json.Marshal(trade)
			key = string(raw)
		}
		if !seen[key] {
			seen[key] = true
			unique = append(unique, trade)
		}
	}
	if len(unique) > 100000 {
		return nil, fmt.Errorf("Kalshi trade point budget exceeded")
	}
	return unique, nil
}
func (s *Source) kalshiTradesPart(ctx context.Context, path, ticker string, from, to time.Time) ([]map[string]json.RawMessage, error) {
	q := url.Values{"ticker": {ticker}, "min_ts": {strconv.FormatInt(from.Unix(), 10)}, "max_ts": {strconv.FormatInt(ceilHistorySecond(to).Unix(), 10)}, "limit": {"1000"}}
	seen := map[string]bool{}
	result := []map[string]json.RawMessage{}
	for page := 0; page < 100; page++ {
		var e struct {
			Trades []map[string]json.RawMessage `json:"trades"`
			Cursor string                       `json:"cursor"`
		}
		if err := s.get(ctx, s.p.KalshiBaseURL, path, q, &e); err != nil {
			return nil, err
		}
		if e.Trades == nil {
			return nil, fmt.Errorf("Kalshi trade response omitted trades")
		}
		result = append(result, e.Trades...)
		if len(result) > 100000 {
			return nil, fmt.Errorf("Kalshi trade point budget exceeded")
		}
		if e.Cursor == "" {
			return result, nil
		}
		if seen[e.Cursor] || page == 99 {
			return nil, fmt.Errorf("Kalshi trade pagination incomplete")
		}
		seen[e.Cursor] = true
		q.Set("cursor", e.Cursor)
	}
	return nil, fmt.Errorf("Kalshi trade pagination incomplete")
}

func ceilHistorySecond(at time.Time) time.Time {
	base := at.Truncate(time.Second)
	if base.Before(at) {
		return base.Add(time.Second)
	}
	return base
}
