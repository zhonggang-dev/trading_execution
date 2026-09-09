package marketvalidation

import (
	"context"
	"errors"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

func TestSellAcceptsSuccessfullyFetchedEmptyBooksWithoutInventingQuotes(t *testing.T) {
	for _, shape := range []string{"bid-only", "ask-only", "empty"} {
		t.Run(shape, func(t *testing.T) {
			now, intent, market, book := validFixtures()
			book.Status = domain.OrderBookStatusEmpty
			if shape != "bid-only" {
				book.Bids, book.BestBid = nil, ""
			}
			if shape != "ask-only" {
				book.Asks, book.BestAsk = nil, ""
			}
			service := newValidator(t, now, market, &fakeBooks{books: []domain.OrderBookSnapshot{book}})
			intent.Side = domain.SideSell
			intent.TimeInForce = domain.TimeInForceIOC
			validation, err := service.Validate(context.Background(), intent)
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
			_, err = service.Validate(context.Background(), intent)
			var rejection *port.Rejection
			if !errors.As(err, &rejection) || rejection.Code != "LATEST_BOOK_UNAVAILABLE" {
				t.Fatalf("BUY error = %v, want LATEST_BOOK_UNAVAILABLE", err)
			}
		})
	}
}

func TestSellEmptyBooksStillRequireAuthenticValidEvidence(t *testing.T) {
	tests := []struct {
		name   string
		code   string
		mutate func(*domain.OrderBookSnapshot)
	}{
		{"fetch error", "LATEST_BOOK_UNAVAILABLE", func(b *domain.OrderBookSnapshot) { b.Status = domain.OrderBookStatusError }},
		{"missing book", "LATEST_BOOK_UNAVAILABLE", func(b *domain.OrderBookSnapshot) { b.Status = domain.OrderBookStatusMissing }},
		{"malformed OK", "LATEST_BOOK_UNAVAILABLE", func(b *domain.OrderBookSnapshot) { b.Status = domain.OrderBookStatusOK }},
		{"wrong token", "LATEST_BOOK_IDENTITY_MISMATCH", func(b *domain.OrderBookSnapshot) { b.TokenID = "other" }},
		{"negative size", "LATEST_BOOK_INVALID", func(b *domain.OrderBookSnapshot) { b.Bids[0].Size = "-1" }},
		{"invented ask", "LATEST_BOOK_INVALID", func(b *domain.OrderBookSnapshot) { b.BestAsk = "0.8" }},
		{"changed tick", "TICK_SIZE_CHANGED", func(b *domain.OrderBookSnapshot) { b.TickSize = "0.001" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now, intent, market, book := validFixtures()
			intent.Side = domain.SideSell
			book.Status = domain.OrderBookStatusEmpty
			book.Asks, book.BestAsk = nil, ""
			test.mutate(&book)
			service := newValidator(t, now, market, &fakeBooks{books: []domain.OrderBookSnapshot{book}})
			_, err := service.Validate(context.Background(), intent)
			var rejection *port.Rejection
			if !errors.As(err, &rejection) || rejection.Code != test.code {
				t.Fatalf("SELL error = %v, want %s", err, test.code)
			}
		})
	}
}
