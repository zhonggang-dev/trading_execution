package marketbooks

import (
	"context"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
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
