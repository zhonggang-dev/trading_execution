package marketbooks

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type fakeSource struct {
	seen []domain.BookTarget
}

func (source *fakeSource) Capture(_ context.Context, _ time.Time, targets []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	source.seen = append([]domain.BookTarget(nil), targets...)
	books := make([]domain.OrderBookSnapshot, len(targets))
	for index, target := range targets {
		books[index] = missingBook(target, time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC), "FAKE")
	}
	return books, nil
}

func TestSourceRoutesAndRestoresTargetOrder(t *testing.T) {
	polymarket := &fakeSource{}
	kalshi := &fakeSource{}
	source, err := New(Params{Polymarket: polymarket, Kalshi: kalshi})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	targets := []domain.BookTarget{
		{MarketSource: domain.MarketSourceKalshi, MarketID: "K", ConditionID: "kalshi:K", OutcomeIndex: 0, OutcomeID: "YES", TokenID: "kalshi:K:YES"},
		{MarketID: "P", ConditionID: "condition", OutcomeIndex: 0, TokenID: "token"},
	}
	books, err := source.Capture(context.Background(), time.Now(), targets)
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if len(kalshi.seen) != 1 || kalshi.seen[0].MarketID != "K" || len(polymarket.seen) != 1 || polymarket.seen[0].MarketID != "P" {
		t.Fatalf("routes: Kalshi=%#v Polymarket=%#v", kalshi.seen, polymarket.seen)
	}
	if books[0].MarketID != "K" || books[1].MarketID != "P" {
		t.Fatalf("books reordered: %#v", books)
	}
}

func TestSourceMarksUnconfiguredKalshiWithoutBlockingPolymarket(t *testing.T) {
	polymarket := &fakeSource{}
	observedAt := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	source, err := New(Params{Polymarket: polymarket, Now: func() time.Time { return observedAt }})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	books, err := source.Capture(context.Background(), observedAt, []domain.BookTarget{{
		MarketSource: domain.MarketSourceKalshi, MarketID: "K", ConditionID: "kalshi:K",
		OutcomeIndex: 0, OutcomeID: "YES", TokenID: "kalshi:K:YES",
	}})
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if books[0].Status != domain.OrderBookStatusMissing || books[0].ErrorCode != "VENUE_ORDERBOOK_NOT_CONFIGURED" {
		t.Fatalf("book = %#v", books[0])
	}
	if err := books[0].Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

type failingSource struct {
	err   error
	books int // when >= 0 and err is nil, return this many books
	block bool
}

func (source failingSource) Capture(ctx context.Context, _ time.Time, targets []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	if source.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if source.err != nil {
		return nil, source.err
	}
	books := make([]domain.OrderBookSnapshot, source.books)
	for index := range books {
		books[index] = missingBook(targets[0], time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC), "FAKE")
	}
	return books, nil
}

func mixedVenueTargets() []domain.BookTarget {
	return []domain.BookTarget{
		{MarketSource: domain.MarketSourceKalshi, MarketID: "K", ConditionID: "kalshi:K", OutcomeIndex: 0, OutcomeID: "YES", TokenID: "kalshi:K:YES"},
		{MarketID: "P", ConditionID: "condition", OutcomeIndex: 0, TokenID: "token"},
		{MarketSource: domain.MarketSourceKalshi, MarketID: "K", ConditionID: "kalshi:K", OutcomeIndex: 1, OutcomeID: "NO", TokenID: "kalshi:K:NO"},
	}
}

func assertVenueFailed(t *testing.T, books []domain.OrderBookSnapshot, venue domain.MarketSource) {
	t.Helper()
	for _, book := range books {
		failed := book.Status == domain.OrderBookStatusError && book.ErrorCode == domain.OrderBookErrorVenueCaptureFailed
		if book.MarketSource.Normalize() == venue {
			if !failed {
				t.Fatalf("book %#v of failed venue %s is not marked failed", book, venue)
			}
			if err := book.Validate(); err != nil {
				t.Fatalf("failed book is invalid: %v", err)
			}
			if len(book.Bids) != 0 || len(book.Asks) != 0 || !book.BestBid.IsEmpty() || !book.BestAsk.IsEmpty() {
				t.Fatalf("failed book retains levels: %#v", book)
			}
			continue
		}
		if failed || book.ErrorCode != "FAKE" {
			t.Fatalf("book %#v of the healthy venue was changed", book)
		}
	}
}

func TestSourceDegradesOnlyTheFailedVenue(t *testing.T) {
	observedAt := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	for name, testCase := range map[string]struct {
		polymarket, kalshi port.OrderBookSource
		failed             domain.MarketSource
	}{
		"Kalshi error":              {polymarket: &fakeSource{}, kalshi: failingSource{err: errors.New("boom")}, failed: domain.MarketSourceKalshi},
		"Polymarket error":          {polymarket: failingSource{err: errors.New("boom")}, kalshi: &fakeSource{}, failed: domain.MarketSourcePolymarket},
		"Kalshi wrong book count":   {polymarket: &fakeSource{}, kalshi: failingSource{books: 1}, failed: domain.MarketSourceKalshi},
		"Kalshi exceeds budget":     {polymarket: &fakeSource{}, kalshi: failingSource{block: true}, failed: domain.MarketSourceKalshi},
		"Polymarket exceeds budget": {polymarket: failingSource{block: true}, kalshi: &fakeSource{}, failed: domain.MarketSourcePolymarket},
	} {
		t.Run(name, func(t *testing.T) {
			source, err := New(Params{
				Polymarket: testCase.polymarket, Kalshi: testCase.kalshi,
				Now: func() time.Time { return observedAt }, VenueTimeout: 50 * time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			targets := mixedVenueTargets()
			books, err := source.Capture(context.Background(), observedAt, targets)
			if err != nil {
				t.Fatalf("Capture() error = %v, want the failed venue degraded", err)
			}
			if len(books) != len(targets) {
				t.Fatalf("books = %d, want %d", len(books), len(targets))
			}
			for index, book := range books {
				if book.TokenID != targets[index].TokenID || book.OutcomeID != targets[index].OutcomeID {
					t.Fatalf("book %d identity = %#v, want target %#v", index, book, targets[index])
				}
			}
			assertVenueFailed(t, books, testCase.failed)
		})
	}
}

func TestSourceReturnsErrorWhenTheCycleContextEnds(t *testing.T) {
	source, err := New(Params{Polymarket: &fakeSource{}, Kalshi: failingSource{block: true}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := source.Capture(ctx, time.Now(), mixedVenueTargets()); err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Capture() error = %v, want the cycle deadline", err)
	}
	cancelled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if _, err := source.Capture(cancelled, time.Now(), mixedVenueTargets()); err == nil {
		t.Fatal("Capture() swallowed a cancelled cycle context")
	}
}
