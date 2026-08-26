package marketbooks

import (
	"context"
	"fmt"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type HistorySource struct {
	polymarket port.MidPriceHistorySource
	kalshi     port.MidPriceHistorySource
	now        func() time.Time
}

type HistoryParams struct {
	Polymarket port.MidPriceHistorySource
	Kalshi     port.MidPriceHistorySource
	Now        func() time.Time
}

func NewHistorySource(params HistoryParams) (*HistorySource, error) {
	if params.Polymarket == nil {
		return nil, fmt.Errorf("Polymarket mid-price history source is required")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &HistorySource{polymarket: params.Polymarket, kalshi: params.Kalshi, now: params.Now}, nil
}

func (source *HistorySource) Capture(
	ctx context.Context,
	decisionAt time.Time,
	lookback time.Duration,
	targets []domain.BookTarget,
) ([]domain.MidPriceHistory, error) {
	result := make([]domain.MidPriceHistory, len(targets))
	bySource := map[domain.MarketSource][]int{
		domain.MarketSourcePolymarket: []int{},
		domain.MarketSourceKalshi:     []int{},
	}
	for index, target := range targets {
		marketSource := target.MarketSource.Normalize()
		if _, supported := bySource[marketSource]; !supported {
			return nil, fmt.Errorf("unsupported history market source %q", marketSource)
		}
		bySource[marketSource] = append(bySource[marketSource], index)
	}
	for _, marketSource := range []domain.MarketSource{domain.MarketSourcePolymarket, domain.MarketSourceKalshi} {
		indices := bySource[marketSource]
		if len(indices) == 0 {
			continue
		}
		delegate := source.polymarket
		if marketSource == domain.MarketSourceKalshi {
			delegate = source.kalshi
		}
		if delegate == nil {
			for _, index := range indices {
				result[index] = missingHistory(targets[index], decisionAt, lookback, source.now().UTC())
			}
			continue
		}
		venueTargets := make([]domain.BookTarget, 0, len(indices))
		for _, index := range indices {
			venueTargets = append(venueTargets, targets[index])
		}
		histories, err := delegate.Capture(ctx, decisionAt, lookback, venueTargets)
		if err != nil {
			return nil, fmt.Errorf("capture %s mid-price histories: %w", marketSource, err)
		}
		if len(histories) != len(indices) {
			return nil, fmt.Errorf("%s history source returned %d histories for %d targets", marketSource, len(histories), len(indices))
		}
		for offset, index := range indices {
			result[index] = histories[offset]
		}
	}
	return result, nil
}

func missingHistory(target domain.BookTarget, decisionAt time.Time, lookback time.Duration, fetchedAt time.Time) domain.MidPriceHistory {
	decisionAt = decisionAt.UTC()
	return domain.MidPriceHistory{
		MarketID: target.MarketID, ConditionID: target.ConditionID,
		OutcomeIndex: target.OutcomeIndex, TokenID: target.TokenID,
		Status:      domain.MidPriceHistoryStatusMissing,
		WindowStart: decisionAt.Add(-lookback), WindowEnd: decisionAt, FidelitySeconds: 60,
		Sampling: domain.MidPriceSamplingUpstreamRaw, MissingValues: domain.MidPriceMissingValuePolicyNoFill,
		TimestampSemantics: domain.MidPriceTimestampSemanticsIntervalEndUTC,
		FetchedAt:          fetchedAt, MidPrices: []domain.MidPricePoint{},
		ErrorCode: "VENUE_MID_PRICE_HISTORY_NOT_CONFIGURED",
	}
}

var _ port.MidPriceHistorySource = (*HistorySource)(nil)
