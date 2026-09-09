package kalshi

import (
	"context"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestKalshiSellAcceptsSuccessfullyFetchedEmptyBooks(t *testing.T) {
	for _, shape := range []string{"bid-only", "ask-only", "empty"} {
		t.Run(shape, func(t *testing.T) {
			now, intent, book := validKalshiValidationFixtures(domain.SideSell)
			book.Status = domain.OrderBookStatusEmpty
			if shape != "bid-only" {
				book.Bids = nil
			}
			if shape != "ask-only" {
				book.Asks = nil
			}
			// The Kalshi source omits both top-level quotes for EMPTY books.
			book.BestBid, book.BestAsk = "", ""
			intent.TimeInForce = domain.TimeInForceIOC
			validator := newKalshiValidatorForTest(t, now, book)
			validation, err := validator.Validate(context.Background(), intent)
			if err != nil {
				t.Fatal(err)
			}
			if validation.BookStatus != domain.OrderBookStatusEmpty || !validation.ExecutableSize.IsEmpty() ||
				!validation.LatestBookObservedAt.Equal(book.ObservedAt) || validation.WorstPrice != intent.WorstPrice {
				t.Fatalf("unexpected SELL evidence: %#v", validation)
			}
			if validation.BestBid.IsEmpty() != (len(book.Bids) == 0) || validation.BestAsk.IsEmpty() != (len(book.Asks) == 0) {
				t.Fatalf("SELL fabricated a missing quote: %#v", validation)
			}
			if len(book.Bids) > 0 && !validation.BestBid.Equal(book.Bids[0].Price) ||
				len(book.Asks) > 0 && !validation.BestAsk.Equal(book.Asks[0].Price) {
				t.Fatalf("SELL changed the actual quote: %#v", validation)
			}
			intent.Side = domain.SideBuy
			assertKalshiRejectionCode(t, validator, intent, "KALSHI_LATEST_BOOK_UNAVAILABLE")
		})
	}
}

func TestKalshiSellEmptyBooksKeepIdentityAndFormatChecks(t *testing.T) {
	tests := []struct {
		name   string
		code   string
		mutate func(*domain.OrderBookSnapshot)
	}{
		{"fetch error", "KALSHI_LATEST_BOOK_UNAVAILABLE", func(b *domain.OrderBookSnapshot) { b.Status = domain.OrderBookStatusError }},
		{"missing", "KALSHI_LATEST_BOOK_UNAVAILABLE", func(b *domain.OrderBookSnapshot) { b.Status = domain.OrderBookStatusMissing }},
		{"malformed OK", "KALSHI_LATEST_BOOK_UNAVAILABLE", func(b *domain.OrderBookSnapshot) { b.Status = domain.OrderBookStatusOK }},
		{"wrong source", "KALSHI_LATEST_BOOK_IDENTITY_MISMATCH", func(b *domain.OrderBookSnapshot) { b.MarketSource = domain.MarketSourcePolymarket }},
		{"wrong token", "KALSHI_LATEST_BOOK_IDENTITY_MISMATCH", func(b *domain.OrderBookSnapshot) { b.TokenID = "other" }},
		{"wrong outcome", "KALSHI_LATEST_BOOK_IDENTITY_MISMATCH", func(b *domain.OrderBookSnapshot) { b.OutcomeID = "NO" }},
		{"invalid level", "KALSHI_LATEST_BOOK_INVALID", func(b *domain.OrderBookSnapshot) { b.Bids[0].Size = "-1" }},
		{"invalid tick", "KALSHI_LATEST_BOOK_INVALID", func(b *domain.OrderBookSnapshot) { b.TickSize = "0" }},
		{"invented ask", "KALSHI_LATEST_BOOK_INVALID", func(b *domain.OrderBookSnapshot) { b.BestAsk = "0.9" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now, intent, book := validKalshiValidationFixtures(domain.SideSell)
			book.Status = domain.OrderBookStatusEmpty
			book.Asks, book.BestAsk = nil, ""
			test.mutate(&book)
			assertKalshiRejectionCode(t, newKalshiValidatorForTest(t, now, book), intent, test.code)
		})
	}
}
