package accountpricehistory

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

const maxRequirementTimeline = 100000

// RequiredTimes is the single sampling contract used by planning, sources and
// coverage diagnostics. Explicit requirements are exact: a repair window must
// not invent price requirements at arbitrary clipping edges. Nil preserves the
// legacy boundary/hour contract for existing callers.
func RequiredTimes(w Window) []time.Time {
	values := w.RequiredAt
	if values == nil {
		values = []time.Time{w.From, w.To}
		for at := w.From.UTC().Truncate(time.Hour).Add(time.Hour); at.Before(w.To); at = at.Add(time.Hour) {
			values = append(values, at)
		}
	}
	seen := map[int64]time.Time{}
	for _, at := range values {
		at = at.UTC()
		if !at.Before(w.From) && !at.After(w.To) {
			seen[at.UnixNano()] = at
		}
	}
	result := make([]time.Time, 0, len(seen))
	for _, at := range seen {
		result = append(result, at)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Before(result[j]) })
	return result
}

type requirementChange struct {
	at     time.Time
	key    string
	shares *big.Rat
}

func requirementNumber(s string) (*big.Rat, error) {
	if s == "" || strings.TrimSpace(s) != s || strings.ContainsAny(s, "eE/") {
		return nil, fmt.Errorf("invalid requirement quantity")
	}
	v, ok := new(big.Rat).SetString(s)
	if !ok {
		return nil, fmt.Errorf("invalid requirement quantity")
	}
	return v, nil
}

// BuildRequirements uses the already verified cohort ownership/disposition
// projection. Each account's complete cash-event clock values every asset that
// remains held after that event. It shares prices across accounts, never lots.
// The frozen latest endpoint is excluded: that value remains a current quote.
func BuildRequirements(ctx context.Context, facts consolev2.CohortMetricFacts) ([]Window, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if facts.ObservedAt.IsZero() || len(facts.UnavailableAccounts) != 0 {
		return nil, fmt.Errorf("historical requirements lack complete ledger facts")
	}
	asOf := facts.ObservedAt.UTC()
	assets := map[string]Asset{}
	holdings := map[string][]Window{}
	required := map[string]map[int64]time.Time{}
	totalSamples := 0
	accounts := map[string]bool{}
	for _, account := range facts.Accounts {
		id := account.Metrics.ExecutionAccountID
		if id == "" || accounts[id] {
			return nil, fmt.Errorf("duplicate requirement account")
		}
		accounts[id] = true
		if facts.Cohorts[id].OpenedAssetCount > 0 && len(facts.Replay[id].Lots) == 0 {
			return nil, fmt.Errorf("historical requirements lack cohort replay")
		}
		changes := []requirementChange{}
		first := asOf
		seenLots := map[string]bool{}
		for i, lot := range facts.Replay[id].Lots {
			if i%256 == 0 && ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if lot.LotID == "" || seenLots[lot.LotID] || lot.Asset.ExecutionAccountID != id || lot.OpenedAt.IsZero() || lot.OpenedAt.After(asOf) || lot.Asset.MarketSource != account.Metrics.MarketSource {
				return nil, fmt.Errorf("requirement lot identity mismatch")
			}
			seenLots[lot.LotID] = true
			a := Asset{MarketSource: lot.Asset.MarketSource, MarketID: lot.Asset.MarketID, ConditionID: lot.Asset.ConditionID, TokenID: lot.Asset.TokenID, OutcomeIndex: lot.Asset.OutcomeIndex, OutcomeName: lot.Asset.OutcomeName}
			if err := a.Validate(); err != nil {
				return nil, err
			}
			quantity, e1 := requirementNumber(lot.OriginalShares)
			remaining, e2 := requirementNumber(lot.RemainingShares)
			if e1 != nil || e2 != nil || quantity.Sign() <= 0 || remaining.Sign() < 0 || remaining.Cmp(quantity) > 0 {
				return nil, fmt.Errorf("invalid requirement lot quantities")
			}
			key := a.Key()
			assets[key] = a
			changes = append(changes, requirementChange{lot.OpenedAt.UTC(), key, new(big.Rat).Set(quantity)})
			if lot.OpenedAt.Before(first) {
				first = lot.OpenedAt.UTC()
			}
			closed := new(big.Rat)
			seenFlows := map[string]bool{}
			for _, flow := range lot.Dispositions {
				q, err := requirementNumber(flow.Shares)
				if err != nil || q.Sign() <= 0 || flow.ID == "" || seenFlows[flow.ID] || flow.At.Before(lot.OpenedAt) || flow.At.After(asOf) {
					return nil, fmt.Errorf("invalid requirement disposition")
				}
				seenFlows[flow.ID] = true
				closed.Add(closed, q)
				changes = append(changes, requirementChange{flow.At.UTC(), key, new(big.Rat).Neg(q)})
			}
			if new(big.Rat).Add(closed, remaining).Cmp(quantity) != 0 {
				return nil, fmt.Errorf("incomplete requirement dispositions")
			}
		}
		if len(changes) == 0 {
			continue
		}
		if asOf.Sub(first)/time.Hour > maxRequirementTimeline || len(changes) > maxRequirementTimeline {
			return nil, fmt.Errorf("requirement timeline budget exceeded")
		}
		sort.Slice(changes, func(i, j int) bool { return changes[i].at.Before(changes[j].at) })
		marks := map[int64]time.Time{asOf.UnixNano(): asOf}
		for _, change := range changes {
			marks[change.at.UnixNano()] = change.at
		}
		for at := first.Truncate(time.Hour); at.Before(asOf); at = at.Add(time.Hour) {
			if !at.Before(first) {
				marks[at.UnixNano()] = at
			}
		}
		times := make([]time.Time, 0, len(marks))
		for _, at := range marks {
			times = append(times, at)
		}
		sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
		quantities, opened := map[string]*big.Rat{}, map[string]time.Time{}
		next := 0
		for _, at := range times {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			previous := map[string]bool{}
			for next < len(changes) && !changes[next].at.After(at) {
				change := changes[next]
				if quantities[change.key] == nil {
					quantities[change.key] = new(big.Rat)
				}
				if _, ok := previous[change.key]; !ok {
					previous[change.key] = quantities[change.key].Sign() > 0
				}
				quantities[change.key].Add(quantities[change.key], change.shares)
				next++
			}
			for key, wasHeld := range previous {
				if quantities[key].Sign() < 0 {
					return nil, fmt.Errorf("negative historical requirement holdings")
				}
				held := quantities[key].Sign() > 0
				if !wasHeld && held {
					opened[key] = at
				} else if wasHeld && !held {
					holdings[key] = append(holdings[key], Window{Asset: assets[key], From: opened[key], To: at})
					delete(opened, key)
				}
				if !held {
					delete(quantities, key)
				}
			}
			if at.Equal(asOf) {
				for key, from := range opened {
					holdings[key] = append(holdings[key], Window{Asset: assets[key], From: from, To: asOf})
				}
				continue
			}
			for key, quantity := range quantities {
				if quantity.Sign() <= 0 {
					continue
				}
				if required[key] == nil {
					required[key] = map[int64]time.Time{}
				}
				if _, exists := required[key][at.UnixNano()]; !exists {
					totalSamples++
					if totalSamples > MaxPoints {
						return nil, fmt.Errorf("required price sample budget exceeded")
					}
					required[key][at.UnixNano()] = at
				}
			}
		}
	}
	for id := range facts.Replay {
		if !accounts[id] {
			return nil, fmt.Errorf("requirement replay has unknown account")
		}
	}
	result := []Window{}
	for key, windows := range holdings {
		sort.Slice(windows, func(i, j int) bool { return windows[i].From.Before(windows[j].From) })
		merged := []Window{}
		for _, window := range windows {
			if len(merged) > 0 && !window.From.After(merged[len(merged)-1].To) {
				if window.To.After(merged[len(merged)-1].To) {
					merged[len(merged)-1].To = window.To
				}
			} else {
				merged = append(merged, window)
			}
		}
		for _, window := range merged {
			window.RequiredAt = []time.Time{}
			for _, at := range required[key] {
				if !at.Before(window.From) && !at.After(window.To) {
					window.RequiredAt = append(window.RequiredAt, at)
				}
			}
			if len(window.RequiredAt) > 0 {
				window.RequiredAt = RequiredTimes(window)
				result = append(result, window)
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Asset.Key() == result[j].Asset.Key() {
			return result[i].From.Before(result[j].From)
		}
		return result[i].Asset.Key() < result[j].Asset.Key()
	})
	return result, nil
}
