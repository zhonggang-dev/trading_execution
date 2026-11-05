package main

import (
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/port"
)

type expiringReadinessPlacement struct{ expiresAt time.Time }

func (expiringReadinessPlacement) ExpectedVenueOrderID() string { return "0xabc" }
func (placement expiringReadinessPlacement) VenueExpiresAt() (time.Time, bool) {
	return placement.expiresAt, true
}

func TestPlacementReadinessPreparedForwardsVenueExpiry(t *testing.T) {
	expiresAt := time.Date(2026, 11, 5, 9, 5, 0, 0, time.UTC)
	got, ok := port.PreparedVenueExpiry(placementReadinessPrepared{inner: expiringReadinessPlacement{expiresAt: expiresAt}})
	if !ok || !got.Equal(expiresAt) {
		t.Fatalf("placement readiness wrapper expiry = %s, %v", got, ok)
	}
}
