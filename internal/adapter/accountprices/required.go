package accountprices

import (
	"context"
	"fmt"
	"strings"
	"time"

	h "github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
)

// FetchWithSeries repairs exactly the missing required samples. Existing facts
// are read-only seeds; only newly acquired points or extended proofs are saved.
func (s *Source) FetchWithSeries(ctx context.Context, w h.Window, seed h.Series) (h.Result, error) {
	if w.RefreshAt != nil {
		return s.refreshCompletedHour(ctx, w, seed.Settlements)
	}
	if w.RequiredAt == nil {
		return s.FetchWithSettlements(ctx, w, seed.Settlements)
	}
	if err := w.Asset.Validate(); err != nil {
		return h.Result{}, err
	}
	if w.To.Before(w.From) || len(w.RequiredAt) > 10000 {
		return h.Result{}, fmt.Errorf("required sample budget exceeded")
	}
	known, err := h.MergeSettlements(nil, seed.Settlements, s.p.Now().UTC())
	if err != nil {
		return h.Result{}, err
	}
	combined := h.Series{Points: append([]h.Point(nil), seed.Points...), Settlements: known}
	h.SortSeries(&combined)
	original := make(map[h.Point]bool, len(seed.Points))
	for _, p := range seed.Points {
		original[p] = true
	}
	required := marks(w)
	need := false
	for _, at := range required {
		if _, ok := h.PriceAt(combined, at); !ok {
			need = true
			break
		}
	}
	warm := len(seed.Points) > 0
	if need && w.Asset.MarketSource == "POLYMARKET" {
		if !warm && w.To.Sub(w.From) <= 14*24*time.Hour {
			if err = s.pmHistory(ctx, w, &combined); err != nil {
				return h.Result{}, err
			}
			h.SortSeries(&combined)
		}
		triedSettlement := false
		for _, at := range required {
			if err = ctx.Err(); err != nil {
				return h.Result{}, err
			}
			if _, ok := h.PriceAt(combined, at); ok {
				continue
			}
			if err = s.pmSample(ctx, w, at, &combined, &triedSettlement); err != nil {
				return h.Result{}, err
			}
		}
	} else if need {
		if !warm {
			if w.To.Sub(w.From) > 14*24*time.Hour {
				return h.Result{}, fmt.Errorf("cold required window exceeds budget")
			}
			err = s.kalshi(ctx, w, &combined)
		} else {
			no := false
			if w.Asset.OutcomeName == nil {
				return h.Result{}, fmt.Errorf("Kalshi outcome missing")
			}
			switch strings.ToLower(*w.Asset.OutcomeName) {
			case "no":
				no = true
			case "yes":
			default:
				return h.Result{}, fmt.Errorf("unknown Kalshi outcome")
			}
			name, index := "YES", 0
			if no {
				name, index = "NO", 1
			}
			if w.Asset.TokenID != "kalshi:"+w.Asset.MarketID+":"+name || (w.Asset.OutcomeIndex != nil && *w.Asset.OutcomeIndex != index) {
				return h.Result{}, fmt.Errorf("Kalshi outcome identity mismatch")
			}
			err = s.fillKalshiGaps(ctx, w, &combined, no)
		}
		if err != nil {
			return h.Result{}, err
		}
	}
	combined.Settlements, err = h.MergeSettlements(combined.Settlements, nil, s.p.Now().UTC())
	if err != nil {
		return h.Result{}, err
	}
	h.SortSeries(&combined)
	result := h.Result{Window: w, Series: h.Series{Points: []h.Point{}, Settlements: combined.Settlements, Coverage: []h.Coverage{}}}
	for _, p := range combined.Points {
		if !original[p] {
			result.Series.Points = append(result.Series.Points, p)
		}
	}
	state, reason := "complete", ""
	for _, at := range required {
		if _, ok := h.PriceAt(combined, at); !ok {
			state, reason = "missing", "missing_prices"
			break
		}
	}
	result.Series.Coverage = append(result.Series.Coverage, h.Coverage{From: w.From, To: w.To, State: state, Reason: reason, FetchedAt: s.p.Now().UTC()})
	return result, nil
}
