package accountcoremetrics

import (
	"context"
	"encoding/json"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

const maxCompleteMetricEntries = 128

type completeMetricEntry struct {
	drawdown           *string
	drawdownCoverage   *DrawdownCoverage
	sharpe             *string
	sharpeCoverage     *SharpeCoverage
	used               time.Time
	drawdownObservedAt time.Time
	sharpeObservedAt   time.Time
}

func copyMetricPointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
func copyDrawdownCoverage(c *DrawdownCoverage) *DrawdownCoverage {
	if c == nil {
		return nil
	}
	v := *c
	v.Reason = copyMetricPointer(c.Reason)
	v.From = copyMetricPointer(c.From)
	v.To = copyMetricPointer(c.To)
	v.ComputedAt = copyMetricPointer(c.ComputedAt)
	v.MissingAssetCount = copyMetricPointer(c.MissingAssetCount)
	v.MissingPointCount = copyMetricPointer(c.MissingPointCount)
	return &v
}
func copySharpeCoverage(c *SharpeCoverage) *SharpeCoverage {
	if c == nil {
		return nil
	}
	v := *c
	v.Reason = copyMetricPointer(c.Reason)
	v.From = copyMetricPointer(c.From)
	v.To = copyMetricPointer(c.To)
	v.ComputedAt = copyMetricPointer(c.ComputedAt)
	return &v
}
func completeMetricKey(item *CohortMetrics, filter consolev2.CohortFilter) string {
	raw, _ := json.Marshal([]any{item.ExecutionAccountID, item.MarketSource, filter.OpeningFrom, filter.OpeningTo, "gross-hourly-dd-and-daily-twr-v1"})
	return string(raw)
}
func transientMetricReason(reason *string) bool {
	return reason != nil && (*reason == "not_synced" || *reason == "missing_prices" || *reason == "budget_exceeded" || *reason == "stale_endpoint")
}
func notNewer(coverageTo *time.Time, asOf time.Time) bool {
	return coverageTo != nil && !coverageTo.After(asOf)
}

// Keep successful values per account and exact opening range. A failed refresh
// cannot clear them or replace them with another filter's financial result.
func (s *Service) retainCompleteMetrics(item *CohortMetrics, filter consolev2.CohortFilter, asOf time.Time) {
	key := completeMetricKey(item, filter)
	s.completeMu.Lock()
	defer s.completeMu.Unlock()
	if s.completeCache == nil {
		s.completeCache = map[string]completeMetricEntry{}
	}
	cached := s.completeCache[key]
	if item.OpenedAssetCount > 0 && item.MaxDrawdown != nil && item.DrawdownCoverage != nil && item.DrawdownCoverage.Reason == nil && notNewer(item.DrawdownCoverage.To, asOf) {
		if cached.drawdownCoverage == nil || item.DrawdownCoverage.To.After(*cached.drawdownCoverage.To) || (item.DrawdownCoverage.To.Equal(*cached.drawdownCoverage.To) && !asOf.Before(cached.drawdownObservedAt)) {
			cached.drawdown = copyMetricPointer(item.MaxDrawdown)
			cached.drawdownCoverage = copyDrawdownCoverage(item.DrawdownCoverage)
			cached.drawdownCoverage.LastComplete = false
			cached.drawdownObservedAt = asOf
		}
	} else if item.MaxDrawdown == nil && item.DrawdownCoverage != nil && transientMetricReason(item.DrawdownCoverage.Reason) && cached.drawdown != nil && notNewer(cached.drawdownCoverage.To, asOf) {
		item.MaxDrawdown = copyMetricPointer(cached.drawdown)
		item.DrawdownCoverage = copyDrawdownCoverage(cached.drawdownCoverage)
		item.DrawdownCoverage.LastComplete = true
		item.DrawdownState = "available"
	}
	if item.SharpAnnual != nil && item.SharpAnnualCoverage != nil && item.SharpAnnualCoverage.Reason == nil && notNewer(item.SharpAnnualCoverage.To, asOf) {
		if cached.sharpeCoverage == nil || item.SharpAnnualCoverage.To.After(*cached.sharpeCoverage.To) || (item.SharpAnnualCoverage.To.Equal(*cached.sharpeCoverage.To) && !asOf.Before(cached.sharpeObservedAt)) {
			cached.sharpe = copyMetricPointer(item.SharpAnnual)
			cached.sharpeCoverage = copySharpeCoverage(item.SharpAnnualCoverage)
			cached.sharpeCoverage.LastComplete = false
			cached.sharpeObservedAt = asOf
		}
	} else if item.SharpAnnual == nil && item.SharpAnnualCoverage != nil && transientMetricReason(item.SharpAnnualCoverage.Reason) && cached.sharpe != nil && notNewer(cached.sharpeCoverage.To, asOf) {
		item.SharpAnnual = copyMetricPointer(cached.sharpe)
		item.SharpAnnualCoverage = copySharpeCoverage(cached.sharpeCoverage)
		item.SharpAnnualCoverage.LastComplete = true
	}
	if cached.drawdown == nil && cached.sharpe == nil {
		return
	}
	cached.used = s.now().UTC()
	if _, exists := s.completeCache[key]; !exists && len(s.completeCache) >= maxCompleteMetricEntries {
		var oldest string
		var at time.Time
		for k, v := range s.completeCache {
			if oldest == "" || v.used.Before(at) {
				oldest, at = k, v.used
			}
		}
		delete(s.completeCache, oldest)
	}
	s.completeCache[key] = cached
}

// On a cold cache, an unsynced current hour can often be recovered from the
// previous complete hour, without starting an external fetch or writing history.
func replayThrough(ctx context.Context, replay drawdownReplay, at time.Time) (drawdownReplay, error) {
	lots := []consolev2.CohortReplayLot{}
	for _, original := range replay.lots {
		if original.OpenedAt.After(at) {
			continue
		}
		lot := original
		lot.Dispositions = nil
		remaining, _ := number(original.OriginalShares)
		for _, flow := range original.Dispositions {
			if !flow.At.After(at) {
				lot.Dispositions = append(lot.Dispositions, flow)
				q, _ := number(flow.Shares)
				remaining.Sub(remaining, q)
			}
		}
		lot.RemainingShares = exact(remaining)
		lots = append(lots, lot)
	}
	if len(lots) == 0 {
		return drawdownReplay{}, nil
	}
	return prepareDrawdown(ctx, lots, lots[0].Asset.ExecutionAccountID, at)
}
func (s *Service) recoverCompleteHistory(ctx context.Context, item *CohortMetrics, replay drawdownReplay, data accountpricehistory.Dataset, historyErr error, asOf time.Time) {
	if historyErr != nil || s.history == nil || len(replay.lots) == 0 || ctx.Err() != nil {
		return
	}
	if item.MaxDrawdown == nil && item.DrawdownCoverage != nil && transientMetricReason(item.DrawdownCoverage.Reason) {
		end := asOf.UTC().Truncate(time.Hour).Add(-time.Hour)
		if !end.Before(replay.from) {
			historical, err := replayThrough(ctx, replay, end)
			if err == nil && len(historical.lots) > 0 {
				prefix := s.drawdownPrefix(ctx, historical, data, end.Add(time.Nanosecond))
				if prefix.reason == "" && ctx.Err() == nil {
					value := prefix.maximum
					now, from := s.now().UTC(), historical.from.UTC()
					zero := 0
					item.MaxDrawdown = &value
					item.DrawdownState = "available"
					item.DrawdownCoverage = &DrawdownCoverage{FidelitySeconds: 3600, From: &from, To: &end, ComputedAt: &now, MissingAssetCount: &zero, MissingPointCount: &zero, LastComplete: true}
				}
			}
		}
	}
	if item.SharpAnnual == nil && item.SharpAnnualCoverage != nil && transientMetricReason(item.SharpAnnualCoverage.Reason) && ctx.Err() == nil {
		candidate := *item
		s.finishSharpe(ctx, &candidate, replay, data, nil, asOf.UTC().Truncate(24*time.Hour).Add(-24*time.Hour))
		if candidate.SharpAnnual != nil {
			item.SharpAnnual = copyMetricPointer(candidate.SharpAnnual)
			item.SharpAnnualCoverage = copySharpeCoverage(candidate.SharpAnnualCoverage)
			item.SharpAnnualCoverage.LastComplete = true
		}
	}
}
