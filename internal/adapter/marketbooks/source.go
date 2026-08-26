package marketbooks

import (
	"context"
	"fmt"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// Source routes venue-owned book targets while preserving the original target
// order expected by the strategy contract. A missing venue dependency is
// represented per instrument, so one venue cannot make the other unavailable.
type Source struct {
	polymarket port.OrderBookSource
	kalshi     port.OrderBookSource
	now        func() time.Time
}

type Params struct {
	Polymarket port.OrderBookSource
	Kalshi     port.OrderBookSource
	Now        func() time.Time
}

func New(params Params) (*Source, error) {
	if params.Polymarket == nil {
		return nil, fmt.Errorf("Polymarket orderbook source is required")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &Source{polymarket: params.Polymarket, kalshi: params.Kalshi, now: params.Now}, nil
}

func (source *Source) Capture(ctx context.Context, decisionAt time.Time, targets []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	result := make([]domain.OrderBookSnapshot, len(targets))
	bySource := map[domain.MarketSource][]int{
		domain.MarketSourcePolymarket: []int{},
		domain.MarketSourceKalshi:     []int{},
	}
	for index, target := range targets {
		marketSource := target.MarketSource.Normalize()
		if _, supported := bySource[marketSource]; !supported {
			return nil, fmt.Errorf("unsupported orderbook market source %q", marketSource)
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
				result[index] = missingBook(targets[index], source.now().UTC(), "VENUE_ORDERBOOK_NOT_CONFIGURED")
			}
			continue
		}
		venueTargets := make([]domain.BookTarget, 0, len(indices))
		for _, index := range indices {
			venueTargets = append(venueTargets, targets[index])
		}
		books, err := delegate.Capture(ctx, decisionAt, venueTargets)
		if err != nil {
			return nil, fmt.Errorf("capture %s orderbooks: %w", marketSource, err)
		}
		if len(books) != len(indices) {
			return nil, fmt.Errorf("%s orderbook source returned %d books for %d targets", marketSource, len(books), len(indices))
		}
		for offset, index := range indices {
			result[index] = books[offset]
		}
	}
	return result, nil
}

func missingBook(target domain.BookTarget, observedAt time.Time, code string) domain.OrderBookSnapshot {
	return domain.OrderBookSnapshot{
		MarketSource: target.MarketSource,
		MarketID:     target.MarketID, ConditionID: target.ConditionID,
		OutcomeIndex: target.OutcomeIndex, OutcomeID: target.OutcomeID, TokenID: target.TokenID,
		Status: domain.OrderBookStatusMissing, ObservedAt: observedAt,
		DepthLimit: domain.StrategyOrderBookDepth, Bids: []domain.PriceLevel{}, Asks: []domain.PriceLevel{},
		ErrorCode: code,
	}
}

var _ port.OrderBookSource = (*Source)(nil)
