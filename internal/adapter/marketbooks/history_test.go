package marketbooks

import (
	"context"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

type fakeHistorySource struct {
	seen []domain.BookTarget
}

func (source *fakeHistorySource) Capture(_ context.Context, decisionAt time.Time, lookback time.Duration, targets []domain.BookTarget) ([]domain.MidPriceHistory, error) {
	source.seen = append([]domain.BookTarget(nil), targets...)
	histories := make([]domain.MidPriceHistory, len(targets))
	for index, target := range targets {
		histories[index] = missingHistory(target, decisionAt, lookback, decisionAt)
	}
	return histories, nil
}

func TestHistorySourceRoutesVenueTargets(t *testing.T) {
	polymarket := &fakeHistorySource{}
	kalshi := &fakeHistorySource{}
	source, err := NewHistorySource(HistoryParams{Polymarket: polymarket, Kalshi: kalshi})
	if err != nil {
		t.Fatalf("NewHistorySource() error = %v", err)
	}
	decisionAt := time.Date(2026, 8, 26, 4, 20, 0, 0, time.UTC)
	targets := []domain.BookTarget{
		{MarketID: "P", ConditionID: "condition", OutcomeIndex: 0, TokenID: "token"},
		{MarketSource: domain.MarketSourceKalshi, MarketID: "K", ConditionID: "kalshi:K", OutcomeIndex: 0, OutcomeID: "YES", TokenID: "kalshi:K:YES"},
	}
	histories, err := source.Capture(context.Background(), decisionAt, 3*time.Hour, targets)
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if len(polymarket.seen) != 1 || polymarket.seen[0].MarketID != "P" || len(kalshi.seen) != 1 || kalshi.seen[0].MarketID != "K" {
		t.Fatalf("routes: Polymarket=%#v Kalshi=%#v", polymarket.seen, kalshi.seen)
	}
	if histories[0].MarketID != "P" || histories[1].MarketID != "K" {
		t.Fatalf("histories reordered: %#v", histories)
	}
}
