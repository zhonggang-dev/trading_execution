package clobheartbeat

import (
	"context"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type capabilityVenue struct{ native bool }

func (*capabilityVenue) Name() string { return "polymarket" }
func (*capabilityVenue) Place(context.Context, domain.Order) (port.VenueOrder, error) {
	return port.VenueOrder{}, nil
}
func (*capabilityVenue) Cancel(context.Context, domain.Order) (port.VenueOrder, error) {
	return port.VenueOrder{}, nil
}
func (*capabilityVenue) Get(context.Context, domain.Order) (port.VenueOrder, error) {
	return port.VenueOrder{}, nil
}
func (venue *capabilityVenue) SupportsTimeInForce(timeInForce domain.TimeInForce) bool {
	return venue.native || timeInForce != domain.TimeInForceIOC
}

func TestHeartbeatVenueForwardsTimeInForceCapability(t *testing.T) {
	status := &Service{}
	guarded, err := NewVenue(&capabilityVenue{native: false}, status)
	if err != nil {
		t.Fatal(err)
	}
	if guarded.SupportsTimeInForce(domain.TimeInForceIOC) {
		t.Fatal("heartbeat venue hides the missing native IOC of the wrapped venue")
	}
	if !guarded.SupportsTimeInForce(domain.TimeInForceGTC) {
		t.Fatal("heartbeat venue hides native GTC support")
	}
}
