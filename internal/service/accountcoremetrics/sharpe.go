package accountcoremetrics

import (
	"context"
	"errors"
	"math"
	"math/big"
	"sort"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
)

const annualSharpeDays = 365
const maxSharpeSamples = 2_000_000

type SharpeCoverage struct {
	LastComplete  bool       `json:"last_complete,omitempty"`
	Reason        *string    `json:"reason"`
	PeriodSeconds int        `json:"period_seconds"`
	SampleDays    int        `json:"sample_days"`
	From          *time.Time `json:"from"`
	To            *time.Time `json:"to"`
	ComputedAt    *time.Time `json:"computed_at"`
}

// Annual Sharpe uses the sample standard deviation (n-1), zero risk-free
// return, and a bounded high-precision Welford accumulator.
func annualSharpe(returns []*big.Rat) (*string, string) {
	if len(returns) < 2 {
		return nil, "insufficient_samples"
	}
	makeFloat := func() *big.Float { return new(big.Float).SetPrec(256) }
	mean, m2 := makeFloat(), makeFloat()
	same := true
	for i, r := range returns {
		if r == nil {
			return nil, "nonpositive_nav"
		}
		same = same && r.Cmp(returns[0]) == 0
		x := makeFloat().SetRat(r)
		delta := makeFloat().Sub(x, mean)
		mean.Add(mean, makeFloat().Quo(delta, makeFloat().SetInt64(int64(i+1))))
		m2.Add(m2, makeFloat().Mul(delta, makeFloat().Sub(x, mean)))
	}
	if same {
		return nil, "zero_variance"
	}
	if m2.Sign() <= 0 {
		return nil, "budget_exceeded"
	}
	variance := makeFloat().Quo(m2, makeFloat().SetInt64(int64(len(returns)-1)))
	sd := makeFloat().Sqrt(variance)
	value := makeFloat().Quo(mean, sd)
	value.Mul(value, makeFloat().Sqrt(makeFloat().SetInt64(annualSharpeDays)))
	asFloat, _ := value.Float64()
	if math.IsInf(asFloat, 0) || math.IsNaN(asFloat) {
		return nil, "budget_exceeded"
	}
	text := value.Text('f', 24)
	return &text, ""
}

type sharpeReplayResult struct {
	returns []*big.Rat
	reason  string
}

// Each completed UTC day links returns at the cohort's cash-flow instants.
// Opening principal is an inflow immediately before a trade; gross exit cash
// is an outflow immediately after it. Cash and assets outside the selected lots
// are excluded. Entirely unfunded days are not invented as zero-return samples.
func dailyCohortReturns(ctx context.Context, replay drawdownReplay, data accountpricehistory.Dataset, end time.Time) sharpeReplayResult {
	result := sharpeReplayResult{}
	if len(replay.lots) == 0 || replay.from.IsZero() {
		return result
	}
	start := replay.from.UTC().Truncate(24 * time.Hour)
	if start.Before(replay.from) {
		start = start.Add(24 * time.Hour)
	}
	end = end.UTC().Truncate(24 * time.Hour)
	if !start.Before(end) {
		return result
	}
	if end.Sub(start)/(24*time.Hour) > maxDrawdownTimelinePoints || len(replay.changes) > maxDrawdownTimelinePoints {
		result.reason = "budget_exceeded"
		return result
	}
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
	apply := func(e drawdownChange) {
		if shares[e.asset] == nil {
			shares[e.asset] = new(big.Rat)
		}
		shares[e.asset].Add(shares[e.asset], e.shares)
	}
	event, samples := 0, 0
	for event < len(replay.changes) && replay.changes[event].at.Before(start) {
		apply(replay.changes[event])
		event++
	}
	mark := func(at time.Time) (*big.Rat, string) {
		value := new(big.Rat)
		for key, qty := range shares {
			if qty.Sign() == 0 {
				continue
			}
			samples++
			if samples > maxSharpeSamples || ctx.Err() != nil {
				return nil, "budget_exceeded"
			}
			price, ok := cursors[key].at(at)
			if !ok {
				for _, c := range data.Series[key].Coverage {
					if !at.Before(c.From) && !at.After(c.To) && (c.State == "complete" || c.State == "missing") {
						return nil, "missing_prices"
					}
				}
				return nil, "not_synced"
			}
			value.Add(value, new(big.Rat).Mul(qty, price))
		}
		return value, ""
	}
	previous, reason := mark(start)
	if reason != "" {
		result.reason = reason
		return result
	}
	for day := start; day.Before(end); day = day.Add(24 * time.Hour) {
		if ctx.Err() != nil {
			result.reason = "budget_exceeded"
			return result
		}
		dayEnd := day.Add(24 * time.Hour)
		growth := big.NewRat(1, 1)
		funded := previous.Sign() > 0
		link := func(numerator, denominator *big.Rat) string {
			if ctx.Err() != nil {
				return "budget_exceeded"
			}
			if denominator.Sign() == 0 {
				if numerator.Sign() == 0 {
					return ""
				}
				return "nonpositive_nav"
			}
			if denominator.Sign() < 0 || numerator.Sign() < 0 {
				return "nonpositive_nav"
			}
			growth.Mul(growth, new(big.Rat).Quo(numerator, denominator))
			if growth.Num().BitLen() > 32768 || growth.Denom().BitLen() > 32768 {
				return "budget_exceeded"
			}
			return ""
		}
		for event < len(replay.changes) && replay.changes[event].at.Before(dayEnd) {
			at := replay.changes[event].at
			last := sort.Search(len(replay.changes)-event, func(i int) bool { return replay.changes[event+i].at.After(at) }) + event
			in, out := new(big.Rat), new(big.Rat)
			for _, e := range replay.changes[event:last] {
				if e.shares.Sign() > 0 {
					in.Add(in, e.cost)
				} else {
					out.Add(out, new(big.Rat).Sub(e.profit, e.cost))
				}
			}
			denominator := previous
			if in.Sign() > 0 {
				before, why := mark(at)
				if why != "" {
					result.reason = why
					return result
				}
				if why := link(before, previous); why != "" {
					result.reason = why
					return result
				}
				denominator = new(big.Rat).Add(before, in)
			}
			for _, e := range replay.changes[event:last] {
				apply(e)
			}
			after, why := mark(at)
			if why != "" {
				result.reason = why
				return result
			}
			if why := link(new(big.Rat).Add(after, out), denominator); why != "" {
				result.reason = why
				return result
			}
			funded = funded || in.Sign() > 0 || previous.Sign() > 0
			previous, event = after, last
		}
		closing, why := mark(dayEnd)
		if why != "" {
			result.reason = why
			return result
		}
		if why := link(closing, previous); why != "" {
			result.reason = why
			return result
		}
		if funded || previous.Sign() > 0 {
			result.returns = append(result.returns, new(big.Rat).Sub(growth, big.NewRat(1, 1)))
		}
		previous = closing
	}
	return result
}

func (s *Service) finishSharpe(ctx context.Context, item *CohortMetrics, replay drawdownReplay, data accountpricehistory.Dataset, historyErr error, asOf time.Time) {
	now, end := s.now().UTC(), asOf.UTC().Truncate(24*time.Hour)
	coverage := &SharpeCoverage{PeriodSeconds: 86400, To: &end, ComputedAt: &now}
	item.SharpAnnual, item.SharpAnnualCoverage = nil, coverage
	unavailable := func(reason string) { coverage.Reason = &reason }
	if item.OpenedAssetCount == 0 {
		unavailable("insufficient_samples")
		return
	}
	if !replay.from.IsZero() {
		start := replay.from.UTC().Truncate(24 * time.Hour)
		if start.Before(replay.from) {
			start = start.Add(24 * time.Hour)
		}
		if !start.After(end) {
			coverage.From = &start
		}
		if end.Sub(start) < 48*time.Hour {
			unavailable("insufficient_samples")
			return
		}
	}
	if s.history == nil || len(replay.lots) == 0 || historyErr != nil {
		if ctx.Err() != nil || errors.Is(historyErr, context.DeadlineExceeded) {
			unavailable("budget_exceeded")
		} else {
			unavailable("not_synced")
		}
		return
	}
	result := dailyCohortReturns(ctx, replay, data, asOf)
	coverage.SampleDays = len(result.returns)
	if result.reason != "" {
		unavailable(result.reason)
		return
	}
	value, reason := annualSharpe(result.returns)
	item.SharpAnnual = value
	if reason != "" {
		unavailable(reason)
	}
}
