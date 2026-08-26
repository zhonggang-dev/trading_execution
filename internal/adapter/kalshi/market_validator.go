package kalshi

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type MarketValidator struct {
	books port.OrderBookSource
	now   func() time.Time
}

func NewMarketValidator(books port.OrderBookSource) (*MarketValidator, error) {
	if books == nil {
		return nil, fmt.Errorf("Kalshi orderbook source is required")
	}
	return &MarketValidator{books: books, now: time.Now}, nil
}

func (validator *MarketValidator) Validate(ctx context.Context, intent domain.OrderIntent) (domain.MarketValidation, error) {
	intent = intent.Normalize()
	if intent.MarketSource.Normalize() != domain.MarketSourceKalshi || intent.MarketSnapshotAt == nil ||
		intent.OutcomeIndex == nil || intent.ExpectedNegRisk == nil || intent.ConditionID == "" {
		return domain.MarketValidation{}, &port.Rejection{Code: "KALSHI_MARKET_CONTEXT_REQUIRED", Reason: "complete Kalshi market identity is required"}
	}
	target, err := (domain.BookTargetParams{MarketSource: domain.MarketSourceKalshi, MarketID: intent.MarketID,
		ConditionID: intent.ConditionID, OutcomeIndex: *intent.OutcomeIndex, OutcomeID: intent.OutcomeID, TokenID: intent.TokenID}).Build()
	if err != nil {
		return domain.MarketValidation{}, err
	}
	books, err := validator.books.Capture(ctx, validator.now().UTC(), []domain.BookTarget{target})
	if err != nil || len(books) != 1 || books[0].Status != domain.OrderBookStatusOK || len(books[0].Bids) == 0 || len(books[0].Asks) == 0 {
		return domain.MarketValidation{}, &port.Rejection{Code: "KALSHI_LATEST_BOOK_UNAVAILABLE", Reason: "latest Kalshi orderbook is unavailable"}
	}
	book := books[0]
	expected := book.Asks[0].Price
	if intent.Side == domain.SideSell {
		expected = book.Bids[0].Price
	}
	if !intent.WorstPrice.Equal(expected) {
		return domain.MarketValidation{}, &port.Rejection{Code: "KALSHI_PRICE_MOVED", Reason: "latest Kalshi top of book differs from strategy worst price"}
	}
	now := validator.now().UTC()
	if now.Sub(book.SourceAt.UTC()) > 10*time.Second || book.SourceAt.After(now.Add(2*time.Second)) {
		return domain.MarketValidation{}, &port.Rejection{Code: "KALSHI_BOOK_STALE", Reason: "latest Kalshi orderbook is stale"}
	}
	return (domain.MarketValidationParams{Mode: "KALSHI_LIVE_CHECK", ValidatedAt: now,
		MarketObservedAt: book.SourceAt, StrategySnapshotAt: intent.MarketSnapshotAt.UTC(), LatestBookSourceAt: book.SourceAt,
		LatestBookObservedAt: book.ObservedAt, OutcomeIndex: *intent.OutcomeIndex, OutcomeName: strings.TrimSpace(intent.OutcomeName),
		TokenID: intent.TokenID, NegRisk: *intent.ExpectedNegRisk, TickSize: book.TickSize, MinOrderSize: book.MinOrderSize,
		BestBid: book.Bids[0].Price, BestAsk: book.Asks[0].Price, WorstPrice: intent.WorstPrice}).Build()
}
