package accountprices

import (
	"context"
	"fmt"
	"time"

	h "github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
)

// refreshCompletedHour is an explicit observation, not a missing-point repair.
// It always asks the official source and never substitutes an old seed point
// if that request returns no usable value. Successful markers remain zero-width.
func (s *Source) refreshCompletedHour(ctx context.Context, w h.Window, known []h.Settlement) (h.Result, error) {
	if err := w.Asset.Validate(); err != nil {
		return h.Result{}, err
	}
	at := *w.RefreshAt
	now := s.p.Now().UTC()
	if !at.Equal(at.UTC().Truncate(time.Hour)) || at.After(now.Truncate(time.Hour)) || !w.From.Equal(at) || !w.To.Equal(at) {
		return h.Result{}, fmt.Errorf("invalid completed-hour refresh")
	}
	verified, err := h.MergeSettlements(nil, known, now)
	if err != nil {
		return h.Result{}, err
	}
	fresh := h.Series{Points: []h.Point{}, Settlements: verified, Coverage: []h.Coverage{}}
	sampleWindow := w
	sampleWindow.RequiredAt = []time.Time{at}
	if w.Asset.MarketSource == "POLYMARKET" {
		triedSettlement := false
		err = s.pmSample(ctx, sampleWindow, at, &fresh, &triedSettlement)
	} else {
		err = s.kalshi(ctx, sampleWindow, &fresh)
	}
	if err != nil {
		return h.Result{}, err
	}
	fresh.Settlements, err = h.MergeSettlements(fresh.Settlements, nil, s.p.Now().UTC())
	if err != nil {
		return h.Result{}, err
	}
	h.SortSeries(&fresh)
	state, reason := "complete", "hourly_refresh"
	if _, ok := h.PriceAt(fresh, at); !ok {
		state, reason = "missing", "missing_prices"
	}
	fresh.Coverage = append(fresh.Coverage, h.Coverage{From: at, To: at, State: state, Reason: reason, FetchedAt: s.p.Now().UTC()})
	return h.Result{Window: w, Series: fresh}, nil
}
