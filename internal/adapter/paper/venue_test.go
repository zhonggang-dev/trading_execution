package paper

import (
	"context"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// TestVenueRestoresDurableOrderAfterRestart verifies that a fresh paper venue
// can inspect and cancel an order whose durable state was created by a previous
// process instance.
func TestVenueRestoresDurableOrderAfterRestart(t *testing.T) {
	order := domain.Order{
		ID: "order-1", VenueOrderID: "paper-order-1", Status: domain.OrderStatusLive,
		FilledSize: "0", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	venue := NewVenue("polymarket-paper")
	observed, err := venue.Get(context.Background(), order)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if observed.ID != order.VenueOrderID || observed.State != port.VenueOrderLive {
		t.Fatalf("Get() = %#v", observed)
	}
	cancelled, err := venue.Cancel(context.Background(), order)
	if err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if cancelled.State != port.VenueOrderCancelled {
		t.Fatalf("Cancel() state = %s", cancelled.State)
	}
}

// TestVenueRestoreRejectsForeignIdentity prevents arbitrary durable IDs from
// being accepted as paper venue orders.
func TestVenueRestoreRejectsForeignIdentity(t *testing.T) {
	venue := NewVenue("polymarket-paper")
	_, err := venue.Get(context.Background(), domain.Order{ID: "order-1", VenueOrderID: "foreign"})
	if err == nil {
		t.Fatal("Get() error = nil, want identity rejection")
	}
}
