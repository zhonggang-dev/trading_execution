package kalshi

import (
	"context"
	"fmt"
	"math/big"
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
	now := validator.now().UTC()
	if now.Sub(book.SourceAt.UTC()) > 10*time.Second || book.SourceAt.After(now.Add(2*time.Second)) {
		return domain.MarketValidation{}, &port.Rejection{Code: "KALSHI_BOOK_STALE", Reason: "latest Kalshi orderbook is stale"}
	}
	executableSize, err := validateKalshiDepthAwareLimit(intent, book)
	if err != nil {
		return domain.MarketValidation{}, err
	}
	return (domain.MarketValidationParams{Mode: "KALSHI_LIVE_CHECK", ValidatedAt: now,
		MarketObservedAt: book.SourceAt, StrategySnapshotAt: intent.MarketSnapshotAt.UTC(), LatestBookSourceAt: book.SourceAt,
		LatestBookObservedAt: book.ObservedAt, OutcomeIndex: *intent.OutcomeIndex, OutcomeName: strings.TrimSpace(intent.OutcomeName),
		TokenID: intent.TokenID, NegRisk: *intent.ExpectedNegRisk, TickSize: book.TickSize, MinOrderSize: book.MinOrderSize,
		BestBid: book.Bids[0].Price, BestAsk: book.Asks[0].Price, WorstPrice: intent.WorstPrice,
		ExecutableSize: executableSize}).Build()
}

// validateKalshiDepthAwareLimit keeps the strategy's immutable protection
// range anchored to the strategy snapshot while requiring the current,
// authoritative book to contain some immediately executable liquidity. Kalshi
// orders use IOC, so the venue may fill that protected subset and cancel the
// remainder. A price move in the trader's favour is allowed; an adverse move
// beyond the strategy worst price or zero protected liquidity fails closed.
func validateKalshiDepthAwareLimit(intent domain.OrderIntent, book domain.OrderBookSnapshot) (domain.Decimal, error) {
	if book.TickSize.IsEmpty() {
		return "", kalshiMarketRejection("KALSHI_TICK_SIZE_INVALID", "latest Kalshi orderbook omitted tick_size")
	}
	if sign, err := book.TickSize.Sign(); err != nil || sign <= 0 {
		return "", kalshiMarketRejection("KALSHI_TICK_SIZE_INVALID", "latest Kalshi orderbook tick_size is invalid")
	}
	if multiple, err := intent.WorstPrice.IsMultipleOf(book.TickSize); err != nil || !multiple {
		return "", kalshiMarketRejection("KALSHI_PRICE_TICK_MISMATCH", "strategy worst price is not an exact multiple of the latest Kalshi tick_size")
	}
	referencePrice, ok := intent.Metadata["strategy_reference_price"]
	if !ok || strings.TrimSpace(referencePrice) == "" {
		return "", kalshiMarketRejection("KALSHI_STRATEGY_REFERENCE_PRICE_REQUIRED", "strategy reference price is required for DEPTH_AWARE_LIMIT")
	}
	reference := domain.Decimal(strings.TrimSpace(referencePrice))
	if sign, err := reference.Sign(); err != nil || sign <= 0 {
		return "", kalshiMarketRejection("KALSHI_STRATEGY_REFERENCE_PRICE_INVALID", "strategy reference price is invalid")
	}
	if multiple, err := reference.IsMultipleOf(book.TickSize); err != nil || !multiple {
		return "", kalshiMarketRejection("KALSHI_STRATEGY_REFERENCE_PRICE_INVALID", "strategy reference price is not an exact multiple of the latest Kalshi tick_size")
	}
	if err := validateKalshiProtectionRange(intent.Side, reference, intent.WorstPrice); err != nil {
		return "", err
	}

	levels := book.Asks
	top := book.Asks[0].Price
	if intent.Side == domain.SideSell {
		levels = book.Bids
		top = book.Bids[0].Price
	}
	comparison, err := top.Compare(intent.WorstPrice)
	if err != nil {
		return "", kalshiMarketRejection("KALSHI_PRICE_INVALID", "latest Kalshi top of book cannot be compared with the strategy worst price")
	}
	if intent.Side == domain.SideBuy && comparison > 0 || intent.Side == domain.SideSell && comparison < 0 {
		return "", kalshiMarketRejection("KALSHI_PRICE_MOVED", "latest Kalshi top of book moved beyond the strategy worst price")
	}

	available := new(big.Rat)
	for _, level := range levels {
		comparison, err := level.Price.Compare(intent.WorstPrice)
		if err != nil {
			return "", kalshiMarketRejection("KALSHI_LATEST_BOOK_INVALID", "latest Kalshi orderbook contains an invalid price")
		}
		executable := intent.Side == domain.SideBuy && comparison <= 0 || intent.Side == domain.SideSell && comparison >= 0
		if !executable {
			continue
		}
		size, err := level.Size.Multiply("1")
		if err != nil || size.Sign() <= 0 {
			return "", kalshiMarketRejection("KALSHI_LATEST_BOOK_INVALID", "latest Kalshi orderbook contains an invalid size")
		}
		available.Add(available, size)
	}
	requested, err := intent.Size.Multiply("1")
	if err != nil || requested.Sign() <= 0 {
		return "", kalshiMarketRejection("KALSHI_ORDER_SIZE_INVALID", "Kalshi order size is invalid")
	}
	if intent.TimeInForce == domain.TimeInForceIOC {
		if available.Sign() <= 0 {
			return "", kalshiMarketRejection("KALSHI_NO_EXECUTABLE_VISIBLE_DEPTH", "latest Kalshi orderbook has no executable depth within the strategy worst price")
		}
		if available.Cmp(requested) > 0 {
			available.Set(requested)
		}
		executableSize, err := exactKalshiContractCount(available)
		if err != nil {
			return "", kalshiMarketRejection("KALSHI_LATEST_BOOK_INVALID", err.Error())
		}
		if !book.MinOrderSize.IsEmpty() {
			if comparison, err := executableSize.Compare(book.MinOrderSize); err != nil || comparison < 0 {
				return "", kalshiMarketRejection("KALSHI_INSUFFICIENT_VISIBLE_DEPTH", "latest Kalshi executable depth is below min_order_size")
			}
		}
		return executableSize, nil
	}
	if available.Cmp(requested) < 0 {
		return "", kalshiMarketRejection("KALSHI_INSUFFICIENT_VISIBLE_DEPTH", "latest Kalshi orderbook lacks cumulative depth within the strategy worst price")
	}
	return "", nil
}

func exactKalshiContractCount(value *big.Rat) (domain.Decimal, error) {
	if value == nil || value.Sign() <= 0 {
		return "", fmt.Errorf("latest Kalshi executable depth is invalid")
	}
	formatted := value.FloatString(2)
	roundTrip, ok := new(big.Rat).SetString(formatted)
	if !ok || roundTrip.Cmp(value) != 0 {
		return "", fmt.Errorf("latest Kalshi executable depth exceeds two contract decimals")
	}
	return domain.Decimal(formatted), nil
}

func validateKalshiProtectionRange(side domain.Side, reference, worst domain.Decimal) error {
	referenceValue, referenceErr := reference.Multiply("1")
	worstValue, worstErr := worst.Multiply("1")
	if referenceErr != nil || worstErr != nil {
		return kalshiMarketRejection("KALSHI_PRICE_INVALID", "Kalshi DEPTH_AWARE_LIMIT prices are invalid")
	}
	switch side {
	case domain.SideBuy:
		if worstValue.Cmp(referenceValue) < 0 {
			return kalshiMarketRejection("KALSHI_PRICE_PROTECTION_INVALID", "BUY worst price is better than the strategy reference ask")
		}
	case domain.SideSell:
		if worstValue.Cmp(referenceValue) > 0 {
			return kalshiMarketRejection("KALSHI_PRICE_PROTECTION_INVALID", "SELL worst price is better than the strategy reference bid")
		}
	default:
		return kalshiMarketRejection("KALSHI_SIDE_INVALID", "Kalshi order side is invalid")
	}
	return nil
}

func kalshiMarketRejection(code, reason string) error {
	return &port.Rejection{Code: code, Reason: reason}
}
