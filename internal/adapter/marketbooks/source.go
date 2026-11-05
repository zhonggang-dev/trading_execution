package marketbooks

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// DefaultVenueTimeout bounds one venue's whole capture. Polymarket finishes in
// under 20s (P99 17.9s) and Kalshi in about 2s; each request also has its own
// 5s timeout, so 90s only stops a venue that would otherwise take the cycle.
const DefaultVenueTimeout = 90 * time.Second

// Source routes venue-owned book targets while preserving the original target
// order expected by the strategy contract. A missing venue dependency is
// represented per instrument, so one venue cannot make the other unavailable.
// A venue whose capture fails as a whole (error, wrong book count, or its own
// budget expired) has all its books marked ERROR with
// domain.OrderBookErrorVenueCaptureFailed; the other venue is unaffected.
type Source struct {
	polymarket   port.OrderBookSource
	kalshi       port.OrderBookSource
	now          func() time.Time
	venueTimeout time.Duration
	logger       *slog.Logger
}

type Params struct {
	Polymarket port.OrderBookSource
	Kalshi     port.OrderBookSource
	Now        func() time.Time
	// VenueTimeout bounds one venue's capture; zero uses DefaultVenueTimeout.
	VenueTimeout time.Duration
	// Logger records the original error of a degraded venue. Optional.
	Logger *slog.Logger
}

func New(params Params) (*Source, error) {
	if params.Polymarket == nil {
		return nil, fmt.Errorf("Polymarket orderbook source is required")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	if params.VenueTimeout < 0 {
		return nil, fmt.Errorf("venue orderbook capture timeout must not be negative")
	}
	if params.VenueTimeout == 0 {
		params.VenueTimeout = DefaultVenueTimeout
	}
	if params.Logger == nil {
		params.Logger = slog.Default()
	}
	return &Source{
		polymarket: params.Polymarket, kalshi: params.Kalshi, now: params.Now,
		venueTimeout: params.VenueTimeout, logger: params.Logger,
	}, nil
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
		venueContext, cancel := context.WithTimeout(ctx, source.venueTimeout)
		books, err := delegate.Capture(venueContext, decisionAt, venueTargets)
		cancel()
		if err == nil && len(books) != len(indices) {
			err = fmt.Errorf("%s orderbook source returned %d books for %d targets", marketSource, len(books), len(indices))
		}
		if err != nil {
			// The cycle's own context ended: the whole capture is abandoned and
			// the caller must see it. Only a venue-local failure degrades.
			if ctx.Err() != nil {
				return nil, fmt.Errorf("capture %s orderbooks: %w", marketSource, err)
			}
			source.logger.Warn("venue orderbook capture failed; marking venue books ERROR",
				"market_source", marketSource, "targets", len(indices),
				"venue_timeout", source.venueTimeout, "error", err)
			observedAt := source.now().UTC()
			for _, index := range indices {
				result[index] = failedBook(targets[index], observedAt, domain.OrderBookErrorVenueCaptureFailed)
			}
			continue
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

func failedBook(target domain.BookTarget, observedAt time.Time, code string) domain.OrderBookSnapshot {
	book := missingBook(target, observedAt, code)
	book.Status = domain.OrderBookStatusError
	return book
}

var _ port.OrderBookSource = (*Source)(nil)
