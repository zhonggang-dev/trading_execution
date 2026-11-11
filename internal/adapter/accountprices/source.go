// Package accountprices reads public historical market data for valuation.
// It does not submit orders or change exchange/position state.
package accountprices

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	h "github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
)

type Params struct {
	Client                                 *http.Client
	PMBaseURL, GammaBaseURL, KalshiBaseURL string
	RequestInterval                        time.Duration
	Now                                    func() time.Time
}
type Source struct {
	p       Params
	gate    sync.Mutex
	next    time.Time
	slots   chan struct{}
	cacheMu sync.Mutex
	cutoff  time.Time
	markets map[string]map[string]json.RawMessage
}

func New(p Params) (*Source, error) {
	if p.Client == nil {
		p.Client = &http.Client{Timeout: 12 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	if p.PMBaseURL == "" {
		p.PMBaseURL = "https://data-api.polymarket.com"
	}
	if p.GammaBaseURL == "" {
		p.GammaBaseURL = "https://gamma-api.polymarket.com"
	}
	if p.KalshiBaseURL == "" {
		p.KalshiBaseURL = "https://external-api.kalshi.com/trade-api/v2"
	}
	for _, base := range []string{p.PMBaseURL, p.GammaBaseURL, p.KalshiBaseURL} {
		u, e := url.Parse(base)
		if e != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil {
			return nil, fmt.Errorf("invalid public history base URL")
		}
	}
	if p.RequestInterval == 0 {
		p.RequestInterval = 500 * time.Millisecond
	}
	if p.Now == nil {
		p.Now = time.Now
	}
	return &Source{p: p, slots: make(chan struct{}, 2), markets: map[string]map[string]json.RawMessage{}}, nil
}
func wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
func (s *Source) get(ctx context.Context, base, path string, q url.Values, out any) error {
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	case <-ctx.Done():
		return ctx.Err()
	}
	for attempt := 0; attempt < 3; attempt++ {
		s.gate.Lock()
		delay := time.Until(s.next)
		if delay < 0 {
			delay = 0
		}
		s.next = time.Now().Add(delay + s.p.RequestInterval)
		s.gate.Unlock()
		if delay > 0 {
			if e := wait(ctx, delay); e != nil {
				return e
			}
		}
		u := strings.TrimRight(base, "/") + path
		if len(q) > 0 {
			u += "?" + q.Encode()
		}
		req, e := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if e != nil {
			return e
		}
		req.Header.Set("Accept", "application/json")
		resp, e := s.p.Client.Do(req)
		if e != nil {
			if attempt == 2 || ctx.Err() != nil {
				return fmt.Errorf("history source transport unavailable")
			}
			if e = wait(ctx, time.Duration(attempt+1)*time.Second); e != nil {
				return e
			}
			continue
		}
		raw, e := io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
		_ = resp.Body.Close()
		if e != nil || len(raw) > 8<<20 {
			return fmt.Errorf("history response budget exceeded")
		}
		if resp.StatusCode == http.StatusOK {
			if e = json.Unmarshal(raw, out); e != nil {
				return fmt.Errorf("invalid history response")
			}
			return nil
		}
		if resp.StatusCode == 404 {
			return errNotFound
		}
		if resp.StatusCode != 429 && resp.StatusCode < 500 {
			return fmt.Errorf("history source HTTP %d", resp.StatusCode)
		}
		if attempt == 2 {
			return fmt.Errorf("history source HTTP %d", resp.StatusCode)
		}
		backoff := time.Duration(attempt+1) * time.Second
		if seconds, e := strconv.Atoi(resp.Header.Get("Retry-After")); e == nil && seconds > 0 {
			backoff = time.Duration(min(seconds, 10)) * time.Second
		} else if at, e := http.ParseTime(resp.Header.Get("Retry-After")); e == nil && at.After(time.Now()) {
			backoff = min(time.Until(at), 10*time.Second)
		}
		if e = wait(ctx, backoff); e != nil {
			return e
		}
	}
	return fmt.Errorf("history request exhausted")
}

var errNotFound = fmt.Errorf("history market not found")

func (s *Source) Fetch(ctx context.Context, w h.Window) (h.Result, error) {
	return s.FetchWithSettlements(ctx, w, nil)
}

func (s *Source) FetchWithSettlements(ctx context.Context, w h.Window, known []h.Settlement) (h.Result, error) {
	if w.RequiredAt != nil || w.RefreshAt != nil {
		return s.FetchWithSeries(ctx, w, h.Series{Settlements: known})
	}
	if e := w.Asset.Validate(); e != nil {
		return h.Result{}, e
	}
	if w.To.Before(w.From) || w.To.Sub(w.From) > 14*24*time.Hour {
		return h.Result{}, fmt.Errorf("history window exceeds 14 days")
	}
	verified, err := h.MergeSettlements(nil, known, s.p.Now().UTC())
	if err != nil {
		return h.Result{}, err
	}
	result := h.Result{Window: w, Series: h.Series{Points: []h.Point{}, Settlements: verified, Coverage: []h.Coverage{}}}
	if w.Asset.MarketSource == "POLYMARKET" {
		err = s.polymarket(ctx, w, &result.Series)
	} else {
		err = s.kalshi(ctx, w, &result.Series)
	}
	if err != nil {
		return h.Result{}, err
	}
	result.Series.Settlements, err = h.MergeSettlements(result.Series.Settlements, nil, s.p.Now().UTC())
	if err != nil {
		return h.Result{}, err
	}
	h.SortSeries(&result.Series)
	missing := false
	for _, at := range marks(w) {
		if _, ok := h.PriceAt(result.Series, at); !ok {
			missing = true
			break
		}
	}
	state, reason := "complete", ""
	if missing {
		state, reason = "missing", "missing_prices"
	}
	result.Series.Coverage = append(result.Series.Coverage, h.Coverage{From: w.From, To: w.To, State: state, Reason: reason, FetchedAt: s.p.Now().UTC()})
	return result, nil
}
func marks(w h.Window) []time.Time { return h.RequiredTimes(w) }
func text(raw json.RawMessage) string {
	var v string
	if json.Unmarshal(raw, &v) == nil {
		return v
	}
	return string(raw)
}
func timestamp(raw json.RawMessage) (time.Time, bool) {
	v := text(raw)
	if t, e := time.Parse(time.RFC3339Nano, v); e == nil {
		return t.UTC(), true
	}
	if n, e := strconv.ParseInt(v, 10, 64); e == nil && n > 0 {
		return time.Unix(n, 0).UTC(), true
	}
	return time.Time{}, false
}
func subtractOne(v string) (string, bool) {
	if !h.ValidPrice(v) {
		return "", false
	}
	n, _ := new(big.Rat).SetString(v)
	return new(big.Rat).Sub(big.NewRat(1, 1), n).FloatString(12), true
}
func (s *Source) polymarket(ctx context.Context, w h.Window, series *h.Series) error {
	if err := s.pmHistory(ctx, w, series); err != nil {
		return err
	}
	h.SortSeries(series)
	for _, at := range []time.Time{w.From, w.To} {
		if _, ok := h.PriceAt(*series, at); ok {
			continue
		}
		if _, err := s.pmAsOf(ctx, w, at, series); err != nil {
			return err
		}
		h.SortSeries(series)
	}
	for _, at := range marks(w) {
		if _, ok := h.PriceAt(*series, at); !ok {
			return s.pmSettlement(ctx, w, series)
		}
	}
	return nil
}
func (s *Source) pmSettlement(ctx context.Context, w h.Window, series *h.Series) error {
	var response struct {
		Data []map[string]json.RawMessage `json:"data"`
	}
	if err := s.get(ctx, s.p.PMBaseURL, "/v2/resolutions", url.Values{"condition": {w.Asset.ConditionID}}, &response); err != nil {
		return err
	}
	if response.Data == nil {
		return fmt.Errorf("PM resolutions omitted data")
	}
	var resolved map[string]json.RawMessage
	for _, r := range response.Data {
		if text(r["condition_id"]) == w.Asset.ConditionID && text(r["status"]) == "resolved" {
			if resolved != nil {
				return fmt.Errorf("duplicate PM resolution")
			}
			resolved = r
		}
	}
	if resolved == nil {
		return nil
	}
	ref := text(resolved["transaction_hash"])
	if !strings.HasPrefix(ref, "0x") || len(ref) != 66 || text(resolved["log_index"]) == "" {
		return fmt.Errorf("PM resolution lacks transaction evidence")
	}
	at, ok := timestamp(resolved["resolved_at"])
	if !ok {
		at, ok = timestamp(resolved["last_update_timestamp"])
	}
	if !ok {
		return fmt.Errorf("PM resolution lacks effective time")
	}
	var market struct {
		ID        string `json:"id"`
		Condition string `json:"conditionId"`
		Closed    bool   `json:"closed"`
		Status    string `json:"umaResolutionStatus"`
		Tokens    string `json:"clobTokenIds"`
		Prices    string `json:"outcomePrices"`
		Outcomes  string `json:"outcomes"`
	}
	if err := s.get(ctx, s.p.GammaBaseURL, "/markets/"+url.PathEscape(w.Asset.MarketID), nil, &market); err != nil {
		return err
	}
	var tokens, prices, outcomes []string
	if market.ID != w.Asset.MarketID || market.Condition != w.Asset.ConditionID || !market.Closed || market.Status != "resolved" || json.Unmarshal([]byte(market.Tokens), &tokens) != nil || json.Unmarshal([]byte(market.Prices), &prices) != nil || json.Unmarshal([]byte(market.Outcomes), &outcomes) != nil || len(tokens) != 2 || len(prices) != 2 || len(outcomes) != 2 {
		return fmt.Errorf("PM resolution identity mismatch")
	}
	if !((prices[0] == "0" && prices[1] == "1") || (prices[0] == "1" && prices[1] == "0")) {
		return fmt.Errorf("PM settlement not final binary payout")
	}
	index := -1
	for i, t := range tokens {
		if t == w.Asset.TokenID {
			if index >= 0 {
				return fmt.Errorf("duplicate PM outcome token")
			}
			index = i
		}
	}
	if index < 0 || (w.Asset.OutcomeIndex != nil && *w.Asset.OutcomeIndex != index) || (w.Asset.OutcomeName != nil && !strings.EqualFold(*w.Asset.OutcomeName, outcomes[index])) {
		return fmt.Errorf("PM outcome mismatch")
	}
	series.Settlements = append(series.Settlements, h.Settlement{EffectiveAt: at, Price: prices[index], Source: "OFFICIAL_SETTLEMENT", SourceReference: ref + ":" + text(resolved["log_index"]), FetchedAt: s.p.Now().UTC()})
	return nil
}

// canonicalJSONPrice preserves finite base-ten JSON values exactly, including
// exponent notation, while the internal Decimal contract stays exponent-free.
func canonicalJSONPrice(value json.Number) (string, error) {
	raw := string(value)
	if len(raw) == 0 || len(raw) > 128 || strings.ContainsAny(raw, "/ \t\r\n") {
		return "", fmt.Errorf("invalid price number")
	}
	mantissa, exponent := raw, 0
	if i := strings.IndexAny(raw, "eE"); i >= 0 {
		mantissa = raw[:i]
		n, e := strconv.Atoi(raw[i+1:])
		if e != nil || n < -128 || n > 128 {
			return "", fmt.Errorf("price exponent exceeds precision budget")
		}
		exponent = n
	}
	places := 0
	if i := strings.IndexByte(mantissa, '.'); i >= 0 {
		places = len(mantissa) - i - 1
	}
	places -= exponent
	if places < 0 {
		places = 0
	}
	if places > 256 {
		return "", fmt.Errorf("price precision exceeds budget")
	}
	n, ok := new(big.Rat).SetString(raw)
	if !ok || n.Sign() < 0 || n.Cmp(big.NewRat(1, 1)) > 0 {
		return "", fmt.Errorf("price outside payout range")
	}
	text := n.FloatString(places)
	if strings.Contains(text, ".") {
		text = strings.TrimRight(strings.TrimRight(text, "0"), ".")
	}
	return text, nil
}
