package accountcoremetrics

import (
	"context"
	"encoding/json"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

type CohortFactsReader interface {
	CohortMetricFacts(context.Context, consolev2.CohortFilter) (consolev2.CohortMetricFacts, error)
}

type CohortMetrics struct {
	Metrics
	OpenedAssetCount    int               `json:"opened_asset_count"`
	HoldingAssetCount   int               `json:"holding_asset_count"`
	ClosedAssetCount    int               `json:"closed_asset_count"`
	ROI                 *string           `json:"roi"`
	BuyFees             string            `json:"buy_fees"`
	SellFees            string            `json:"sell_fees"`
	MaxDrawdown         *string           `json:"max_drawdown"`
	DrawdownState       string            `json:"drawdown_state"`
	DrawdownCoverage    *DrawdownCoverage `json:"drawdown_coverage,omitempty"`
	SharpAnnual         *string           `json:"sharp_annual"`
	SharpAnnualCoverage *SharpeCoverage   `json:"sharp_annual_coverage"`
}

type CohortResponse struct {
	ObservedAt          time.Time                            `json:"observed_at"`
	OpeningFrom         *time.Time                           `json:"opening_from"`
	OpeningTo           *time.Time                           `json:"opening_to"`
	Items               []CohortMetrics                      `json:"items"`
	UnavailableAccounts []consolev2.CohortUnavailableAccount `json:"unavailable_accounts"`
}

func (s *Service) QueryCohort(ctx context.Context, filter consolev2.CohortFilter) (CohortResponse, error) {
	if s == nil {
		return CohortResponse{}, consolev2.ErrUnavailable
	}
	if err := consolev2.ValidateCohortFilter(filter); err != nil {
		return CohortResponse{}, err
	}
	reader, ok := s.facts.(CohortFactsReader)
	if !ok {
		return CohortResponse{}, consolev2.ErrUnavailable
	}
	factsCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	facts, err := reader.CohortMetricFacts(factsCtx, filter)
	cancel()
	if err != nil {
		return CohortResponse{}, err
	}
	replays := map[string]drawdownReplay{}
	if s.history != nil {
		for account, replay := range facts.Replay {
			r, e := prepareDrawdown(ctx, replay.Lots, account, facts.ObservedAt)
			if e != nil {
				return CohortResponse{}, e
			}
			replays[account] = r
		}
	}
	type historyResult struct {
		data accountpricehistory.Dataset
		err  error
	}
	history := make(chan historyResult, 1)
	historyCtx, historyCancel := context.WithTimeout(ctx, 2*time.Second)
	defer historyCancel()
	windows := mergeDrawdownWindows(replays)
	if s.history != nil && len(windows) > 0 {
		go func() {
			data, err := s.history.Load(historyCtx, windows)
			history <- historyResult{data: data, err: err}
		}()
	} else {
		history <- historyResult{}
	}
	priced, err := s.queryFacts(ctx, facts.CoreMetricFacts)
	if err != nil {
		return CohortResponse{}, err
	}
	var loaded historyResult
	select {
	case loaded = <-history:
	case <-historyCtx.Done():
		loaded.err = historyCtx.Err()
	}
	if ctx.Err() != nil {
		return CohortResponse{}, ctx.Err()
	}
	result := CohortResponse{ObservedAt: priced.ObservedAt, OpeningFrom: facts.OpeningFrom, OpeningTo: facts.OpeningTo, Items: make([]CohortMetrics, 0, len(priced.Items)), UnavailableAccounts: append([]consolev2.CohortUnavailableAccount{}, facts.UnavailableAccounts...)}
	seen := map[string]bool{}
	for _, a := range facts.UnavailableAccounts {
		if a.Reason != "missing_disposition_evidence" || (a.MarketSource != "POLYMARKET" && a.MarketSource != "KALSHI") || a.ExecutionAccountID == "" || seen[a.ExecutionAccountID] || consolev2.ValidatePerformanceAccountID(a.ExecutionAccountID) != nil || (filter.ExecutionAccountID != "" && a.ExecutionAccountID != filter.ExecutionAccountID) {
			return CohortResponse{}, consolev2.ErrUnavailable
		}
		seen[a.ExecutionAccountID] = true
	}
	for _, m := range priced.Items {
		if seen[m.ExecutionAccountID] {
			return CohortResponse{}, consolev2.ErrUnavailable
		}
		seen[m.ExecutionAccountID] = true
		c, ok := facts.Cohorts[m.ExecutionAccountID]
		if !ok || c.HoldingAssetCount != m.AssetCount || c.OpenedAssetCount != c.HoldingAssetCount+c.ClosedAssetCount {
			return CohortResponse{}, consolev2.ErrUnavailable
		}
		for _, v := range []string{c.BuyFees, c.SellFees} {
			n, e := number(v)
			if e != nil || n.Sign() < 0 {
				return CohortResponse{}, consolev2.ErrUnavailable
			}
		}
		item := CohortMetrics{Metrics: m, OpenedAssetCount: c.OpenedAssetCount, HoldingAssetCount: c.HoldingAssetCount, ClosedAssetCount: c.ClosedAssetCount, BuyFees: c.BuyFees, SellFees: c.SellFees, MaxDrawdown: c.MaxDrawdown, DrawdownState: c.DrawdownState}
		s.finishDrawdown(ctx, &item, replays[m.ExecutionAccountID], loaded.data, loaded.err, facts.ObservedAt)
		s.finishSharpe(ctx, &item, replays[m.ExecutionAccountID], loaded.data, loaded.err, facts.ObservedAt)
		s.retainCompleteMetrics(&item, filter, facts.ObservedAt)
		s.recoverCompleteHistory(ctx, &item, replays[m.ExecutionAccountID], loaded.data, loaded.err, facts.ObservedAt)
		s.retainCompleteMetrics(&item, filter, facts.ObservedAt)
		if m.TotalPnL != nil {
			invested, e := number(m.CumulativeInvestedCost)
			if e != nil || invested.Sign() < 0 {
				return CohortResponse{}, consolev2.ErrUnavailable
			}
			if invested.Sign() > 0 {
				total, e := number(*m.TotalPnL)
				if e != nil {
					return CohortResponse{}, consolev2.ErrUnavailable
				}
				r := total.Quo(total, invested).FloatString(24)
				item.ROI = &r
			}
		}
		result.Items = append(result.Items, item)
	}
	if ctx.Err() != nil {
		return CohortResponse{}, ctx.Err()
	}
	raw, e := json.Marshal(result)
	if e != nil || len(raw) > 8*1024*1024 {
		return CohortResponse{}, consolev2.ErrUnavailable
	}
	return result, nil
}
