package postgres

import (
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	domainorderstate "github.com/UniPat-AI/trading_execution/internal/domain/orderstate"
)

// TestFillOrderTransitionParamsUsesDomainStateMachine 验证成交 DAO 构建的迁移可由统一领域状态机安全应用。
func TestFillOrderTransitionParamsUsesDomainStateMachine(t *testing.T) {
	updatedAt := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	venueObservedAt := updatedAt.Add(time.Second)
	order := domain.Order{
		ID: "order-1", Intent: domain.OrderIntent{Size: "10"},
		VenueOrderID: "venue-1", Status: domain.OrderStatusLive,
		FilledSize: "0", FilledNotional: "0", TotalFees: "0",
		CreatedAt: updatedAt, UpdatedAt: updatedAt, VenueLastObservedAt: &venueObservedAt, Revision: 4,
	}
	params := fillOrderTransitionParams{
		Target: domain.OrderStatusPartiallyFilled,
		Fill: domain.Fill{
			Key: "fill-1", VenueOrderID: "venue-1", Status: domain.FillStatusConfirmed,
			VenueUpdatedAt: updatedAt.Add(-time.Minute), ObservedAt: updatedAt.Add(2 * time.Second),
		},
		FilledSize: "2", FilledNotional: "1", TotalFees: "0.01", AverageFillPrice: "0.5",
		IncludeAmounts: true,
	}

	next, event, err := domainorderstate.Apply(order, params.Build(order))
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if next.Status != domain.OrderStatusPartiallyFilled || next.Revision != 5 ||
		!next.FilledSize.Equal("2") || !next.FilledNotional.Equal("1") || !next.TotalFees.Equal("0.01") {
		t.Fatalf("next order = %#v", next)
	}
	if next.VenueLastObservedAt == nil || !next.VenueLastObservedAt.Equal(venueObservedAt) {
		t.Fatalf("venue observation moved backward: %v", next.VenueLastObservedAt)
	}
	if event.FillKey != "fill-1" || event.Trigger != domain.TransitionTriggerFill || event.Revision != next.Revision {
		t.Fatalf("event = %#v", event)
	}
}
