package orderstate

import (
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestApplyAllowsProtectedPolymarketBuyPriceImprovementShares(t *testing.T) {
	now := time.Date(2026, 9, 1, 11, 1, 6, 0, time.UTC)
	order := domain.Order{
		ID: "order-wallet-6",
		Intent: domain.OrderIntent{
			Venue: "polymarket", Side: domain.SideBuy, Size: "30",
		},
		Status: domain.OrderStatusManualReview, FilledSize: "0", CreatedAt: now.Add(-time.Minute),
		UpdatedAt: now.Add(-time.Minute), Revision: 1,
	}
	next, _, err := Apply(order, Transition{
		EventID: "event-wallet-6", To: domain.OrderStatusFilled,
		Trigger: domain.TransitionTriggerFill, FilledSize: "30.147057",
		FilledNotional: "10.199999", TotalFees: "0", AverageFillPrice: "0.3383414507", At: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if next.Status != domain.OrderStatusFilled || !next.FilledSize.Equal("30.147057") {
		t.Fatalf("price-improved order = %#v", next)
	}
}

func TestApplyRejectsBuyOverfillForOtherVenues(t *testing.T) {
	now := time.Date(2026, 9, 1, 11, 1, 6, 0, time.UTC)
	order := domain.Order{
		ID: "order-kalshi", Intent: domain.OrderIntent{Venue: "kalshi", Side: domain.SideBuy, Size: "30"},
		Status: domain.OrderStatusManualReview, FilledSize: "0", CreatedAt: now.Add(-time.Minute),
		UpdatedAt: now.Add(-time.Minute), Revision: 1,
	}
	if _, _, err := Apply(order, Transition{
		EventID: "event-kalshi", To: domain.OrderStatusFilled, Trigger: domain.TransitionTriggerFill,
		FilledSize: "30.147057", FilledNotional: "10.199999", At: now,
	}); err == nil {
		t.Fatal("non-Polymarket BUY overfill was accepted")
	}
}
