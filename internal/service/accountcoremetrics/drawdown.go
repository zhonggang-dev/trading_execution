package accountcoremetrics

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

const maxDrawdownSamples = 2_000_000
const maxDrawdownTimelinePoints = 100_000
const maxDrawdownCacheEntries = 128

type DrawdownCoverage struct {
	Reason            *string    `json:"reason"`
	FidelitySeconds   int        `json:"fidelity_seconds"`
	From              *time.Time `json:"from"`
	To                *time.Time `json:"to"`
	ComputedAt        *time.Time `json:"computed_at"`
	MissingAssetCount *int       `json:"missing_asset_count"`
	MissingPointCount *int       `json:"missing_point_count"`
}

type drawdownChange struct {
	at                   time.Time
	asset                string
	shares, cost, profit *big.Rat
}

type drawdownReplay struct {
	lots    []consolev2.CohortReplayLot
	changes []drawdownChange
	assets  map[string]accountpricehistory.Asset
	windows []accountpricehistory.Window
	from    time.Time
}

type drawdownPrefix struct {
	peak, maximum string
	missingAssets int
	missingPoints int
	reason        string
}

type drawdownCacheEntry struct {
	prefix  drawdownPrefix
	expires time.Time
}

// prepareDrawdown consumes only the immutable allocations already validated by
// CohortMetricFacts. Principal and gross proceeds deliberately exclude fees.
func prepareDrawdown(ctx context.Context, lots []consolev2.CohortReplayLot, account string, asOf time.Time) (drawdownReplay, error) {
	r := drawdownReplay{lots: lots, assets: map[string]accountpricehistory.Asset{}}
	seen := map[string]bool{}
	for index, l := range lots {
		if index%256 == 0 && ctx.Err() != nil {
			return r, ctx.Err()
		}
		if l.LotID == "" || seen[l.LotID] || l.Asset.ExecutionAccountID != account || l.OpenedAt.IsZero() || l.OpenedAt.After(asOf) {
			return r, consolev2.ErrUnavailable
		}
		seen[l.LotID] = true
		asset := accountpricehistory.Asset{MarketSource: l.Asset.MarketSource, MarketID: l.Asset.MarketID, ConditionID: l.Asset.ConditionID, TokenID: l.Asset.TokenID, OutcomeIndex: l.Asset.OutcomeIndex, OutcomeName: l.Asset.OutcomeName}
		if asset.Validate() != nil {
			return r, consolev2.ErrUnavailable
		}
		qty, e1 := number(l.OriginalShares)
		remaining, e2 := number(l.RemainingShares)
		principal, e3 := number(l.OriginalPrincipal)
		if e1 != nil || e2 != nil || e3 != nil || qty.Sign() <= 0 || remaining.Sign() < 0 || remaining.Cmp(qty) > 0 || principal.Sign() < 0 {
			return r, consolev2.ErrUnavailable
		}
		key := asset.Key()
		r.assets[key] = asset
		if r.from.IsZero() || l.OpenedAt.Before(r.from) {
			r.from = l.OpenedAt
		}
		r.changes = append(r.changes, drawdownChange{at: l.OpenedAt, asset: key, shares: new(big.Rat).Set(qty), cost: new(big.Rat).Set(principal), profit: new(big.Rat)})
		closed, last := new(big.Rat), l.OpenedAt
		flows := map[string]bool{}
		for _, d := range l.Dispositions {
			q, e1 := number(d.Shares)
			gross, e2 := number(d.GrossProceeds)
			if d.ID == "" || flows[d.ID] || e1 != nil || e2 != nil || q.Sign() <= 0 || gross.Sign() < 0 || d.At.Before(l.OpenedAt) || d.At.After(asOf) {
				return r, consolev2.ErrUnavailable
			}
			flows[d.ID] = true
			closed.Add(closed, q)
			if closed.Cmp(qty) > 0 {
				return r, consolev2.ErrUnavailable
			}
			cost := new(big.Rat).Quo(new(big.Rat).Mul(principal, q), qty)
			r.changes = append(r.changes, drawdownChange{at: d.At, asset: key, shares: new(big.Rat).Neg(q), cost: new(big.Rat).Neg(cost), profit: new(big.Rat).Sub(gross, cost)})
			if d.At.After(last) {
				last = d.At
			}
		}
		if new(big.Rat).Add(closed, remaining).Cmp(qty) != 0 {
			return r, consolev2.ErrUnavailable
		}
		if remaining.Sign() > 0 {
			last = asOf
		}
		r.windows = append(r.windows, accountpricehistory.Window{Asset: asset, From: l.OpenedAt, To: last})
	}
	sort.Slice(r.changes, func(i, j int) bool { return r.changes[i].at.Before(r.changes[j].at) })
	return r, nil
}

func mergeDrawdownWindows(replays map[string]drawdownReplay) []accountpricehistory.Window {
	byAsset := map[string][]accountpricehistory.Window{}
	for _, r := range replays {
		for _, w := range r.windows {
			key := w.Asset.Key()
			byAsset[key] = append(byAsset[key], w)
		}
	}
	keys := make([]string, 0, len(byAsset))
	for key := range byAsset {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	merged := []accountpricehistory.Window{}
	for _, key := range keys {
		windows := byAsset[key]
		sort.Slice(windows, func(i, j int) bool { return windows[i].From.Before(windows[j].From) })
		start := len(merged)
		for _, w := range windows {
			i := len(merged) - 1
			if i >= start && !w.From.After(merged[i].To) {
				if w.To.After(merged[i].To) {
					merged[i].To = w.To
				}
			} else {
				merged = append(merged, w)
			}
		}
	}
	return merged
}

type drawdownPrice struct {
	point accountpricehistory.Point
	value *big.Rat
}

// A cursor parses every price once and advances monotonically. It preserves
// PriceAt's latest eligible observation and confirmed-settlement precedence,
// without rescanning all historical points at every account sample.
type drawdownPriceCursor struct {
	points      []drawdownPrice
	settlements []accountpricehistory.Settlement
	next        int
	latest      *drawdownPrice
	settled     int
}

func newDrawdownPriceCursor(ctx context.Context, series accountpricehistory.Series) (*drawdownPriceCursor, error) {
	c := &drawdownPriceCursor{settled: -1}
	for i, p := range series.Points {
		if i%256 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		v, err := number(p.Price)
		if err == nil && v.Sign() >= 0 && v.Cmp(big.NewRat(1, 1)) <= 0 && p.ResolutionSeconds >= 0 && p.ResolutionSeconds <= accountpricehistory.FidelitySeconds {
			c.points = append(c.points, drawdownPrice{point: p, value: v})
		}
	}
	for i, s := range series.Settlements {
		if i%256 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if accountpricehistory.ValidPrice(s.Price) {
			c.settlements = append(c.settlements, s)
		}
	}
	sort.Slice(c.points, func(i, j int) bool {
		if c.points[i].point.At.Equal(c.points[j].point.At) {
			return c.points[i].point.FetchedAt.Before(c.points[j].point.FetchedAt)
		}
		return c.points[i].point.At.Before(c.points[j].point.At)
	})
	sort.Slice(c.settlements, func(i, j int) bool { return c.settlements[i].EffectiveAt.Before(c.settlements[j].EffectiveAt) })
	return c, ctx.Err()
}

func (c *drawdownPriceCursor) at(at time.Time) (*big.Rat, bool) {
	for c.settled+1 < len(c.settlements) && !c.settlements[c.settled+1].EffectiveAt.After(at) {
		c.settled++
	}
	if c.settled >= 0 {
		v, err := number(c.settlements[c.settled].Price)
		return v, err == nil
	}
	for c.next < len(c.points) && !c.points[c.next].point.At.After(at) {
		c.latest = &c.points[c.next]
		c.next++
	}
	if c.latest != nil {
		p := c.latest
		if at.Sub(p.point.At) <= time.Hour || p.point.ValidUntil != nil && !p.point.ValidUntil.Before(at) {
			return p.value, true
		}
	}
	return nil, false
}

// drawdownHistoricalPrefix samples only historical instants. The current mark
// is appended separately from the same latest valuation used by other metrics.
func drawdownHistoricalPrefix(ctx context.Context, replay drawdownReplay, data accountpricehistory.Dataset, asOf time.Time) drawdownPrefix {
	result := drawdownPrefix{peak: "0", maximum: "0"}
	if len(replay.lots) == 0 {
		return result
	}
	if asOf.Sub(replay.from)/time.Hour > maxDrawdownTimelinePoints || len(replay.changes) > maxDrawdownTimelinePoints {
		result.reason = "budget_exceeded"
		return result
	}
	marks := map[time.Time]struct{}{}
	for _, e := range replay.changes {
		if e.at.Before(asOf) {
			marks[e.at.UTC()] = struct{}{}
		}
	}
	for at := replay.from.Truncate(time.Hour); at.Before(asOf); at = at.Add(time.Hour) {
		if !at.Before(replay.from) {
			marks[at] = struct{}{}
		}
		if len(marks) > maxDrawdownTimelinePoints || len(marks)%256 == 0 && ctx.Err() != nil {
			result.reason = "budget_exceeded"
			return result
		}
	}
	times := make([]time.Time, 0, len(marks))
	for at := range marks {
		times = append(times, at)
	}
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	shares := map[string]*big.Rat{}
	cursors := map[string]*drawdownPriceCursor{}
	for key := range replay.assets {
		cursor, err := newDrawdownPriceCursor(ctx, data.Series[key])
		if err != nil {
			result.reason = "budget_exceeded"
			return result
		}
		cursors[key] = cursor
	}
	principal, realized, high, maximum := new(big.Rat), new(big.Rat), new(big.Rat), new(big.Rat)
	missing := map[string]bool{}
	pending := false
	event, samples := 0, 0
	for _, at := range times {
		if ctx.Err() != nil {
			result.reason = "budget_exceeded"
			return result
		}
		// All legs sharing a timestamp are applied before a valuation. No
		// intermediate netting/redemption state can become a false high.
		for event < len(replay.changes) && !replay.changes[event].at.After(at) {
			e := replay.changes[event]
			if shares[e.asset] == nil {
				shares[e.asset] = new(big.Rat)
			}
			shares[e.asset].Add(shares[e.asset], e.shares)
			principal.Add(principal, e.cost)
			realized.Add(realized, e.profit)
			event++
		}
		value := new(big.Rat)
		complete := true
		for key, quantity := range shares {
			if quantity.Sign() == 0 {
				continue
			}
			samples++
			if samples > maxDrawdownSamples || samples%256 == 0 && ctx.Err() != nil {
				result.reason = "budget_exceeded"
				return result
			}
			price, ok := cursors[key].at(at)
			if !ok {
				complete = false
				missing[key] = true
				result.missingPoints++
				// Only a successful request covering this missing instant can
				// establish a real source gap. Errors or an unsynced new hour
				// must not be described as unavailable official history.
				attempted := false
				for _, coverage := range data.Series[key].Coverage {
					if !at.Before(coverage.From) && !at.After(coverage.To) && (coverage.State == "complete" || coverage.State == "missing") {
						attempted = true
						break
					}
				}
				pending = pending || !attempted
				continue
			}
			value.Add(value, new(big.Rat).Mul(quantity, price))
		}
		if !complete {
			continue
		}
		pnl := new(big.Rat).Sub(new(big.Rat).Add(realized, value), principal)
		if pnl.Cmp(high) > 0 {
			high.Set(pnl)
		}
		drop := new(big.Rat).Sub(high, pnl)
		if drop.Cmp(maximum) > 0 {
			maximum.Set(drop)
		}
	}
	result.peak, result.maximum = exact(high), exact(maximum)
	result.missingAssets = len(missing)
	if result.missingPoints > 0 {
		result.reason = "missing_prices"
		if pending {
			result.reason = "not_synced"
		}
	}
	return result
}

func (s *Service) drawdownPrefix(ctx context.Context, replay drawdownReplay, data accountpricehistory.Dataset, asOf time.Time) drawdownPrefix {
	boundary := asOf.Truncate(time.Hour)
	if asOf.Equal(boundary) {
		boundary = boundary.Add(-time.Hour)
	}
	version := data.Version
	if version == "" {
		raw, _ := json.Marshal(data.Series)
		version = fmt.Sprintf("%x", sha256.Sum256(raw))
	}
	includedEvents := sort.Search(len(replay.changes), func(i int) bool { return !replay.changes[i].at.Before(asOf) })
	raw, _ := json.Marshal([]any{replay.lots, version, boundary, includedEvents, "gross-hourly-dd-v1"})
	key := fmt.Sprintf("%x", sha256.Sum256(raw))
	now := s.now().UTC()
	s.drawdownMu.Lock()
	if cached, ok := s.drawdownCache[key]; ok && now.Before(cached.expires) {
		s.drawdownMu.Unlock()
		return cached.prefix
	}
	s.drawdownMu.Unlock()
	prefix := drawdownHistoricalPrefix(ctx, replay, data, asOf)
	if prefix.reason == "budget_exceeded" || ctx.Err() != nil {
		return prefix
	}
	s.drawdownMu.Lock()
	if s.drawdownCache == nil || len(s.drawdownCache) >= maxDrawdownCacheEntries {
		s.drawdownCache = map[string]drawdownCacheEntry{}
	}
	s.drawdownCache[key] = drawdownCacheEntry{prefix: prefix, expires: now.Add(5 * time.Minute)}
	s.drawdownMu.Unlock()
	return prefix
}

func (s *Service) finishDrawdown(ctx context.Context, item *CohortMetrics, replay drawdownReplay, data accountpricehistory.Dataset, historyErr error, asOf time.Time) {
	now, end := s.now().UTC(), asOf.UTC()
	coverage := &DrawdownCoverage{FidelitySeconds: 3600, To: &end, ComputedAt: &now}
	if !replay.from.IsZero() {
		start := replay.from.UTC()
		coverage.From = &start
	}
	item.DrawdownCoverage = coverage
	item.MaxDrawdown = nil
	item.DrawdownState = "history_unavailable"
	unavailable := func(reason string) { coverage.Reason = &reason }
	if item.OpenedAssetCount == 0 {
		z, zero := "0", 0
		item.MaxDrawdown, item.DrawdownState = &z, "available"
		coverage.MissingAssetCount, coverage.MissingPointCount = &zero, &zero
		return
	}
	if item.TotalPnL == nil {
		unavailable("stale_endpoint")
		return
	}
	if s.history == nil || len(replay.lots) == 0 || historyErr != nil {
		if ctx.Err() != nil || errors.Is(historyErr, context.DeadlineExceeded) {
			unavailable("budget_exceeded")
		} else {
			unavailable("not_synced")
		}
		return
	}
	prefix := s.drawdownPrefix(ctx, replay, data, asOf)
	coverage.MissingAssetCount, coverage.MissingPointCount = &prefix.missingAssets, &prefix.missingPoints
	if prefix.reason != "" {
		unavailable(prefix.reason)
		return
	}
	high, _ := number(prefix.peak)
	maximum, _ := number(prefix.maximum)
	current, err := number(*item.TotalPnL)
	if err != nil {
		unavailable("stale_endpoint")
		return
	}
	if drop := new(big.Rat).Sub(high, current); drop.Cmp(maximum) > 0 {
		maximum.Set(drop)
	}
	value := exact(maximum)
	item.MaxDrawdown, item.DrawdownState = &value, "available"
}
