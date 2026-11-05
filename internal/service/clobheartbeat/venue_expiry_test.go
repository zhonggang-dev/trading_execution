package clobheartbeat

import (
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/port"
)

type expiringPlacement struct{ expiresAt time.Time }

func (expiringPlacement) ExpectedVenueOrderID() string { return "0xabc" }
func (placement expiringPlacement) VenueExpiresAt() (time.Time, bool) {
	return placement.expiresAt, !placement.expiresAt.IsZero()
}

func TestHeartbeatPreparedForwardsVenueExpiry(t *testing.T) {
	expiresAt := time.Date(2026, 11, 5, 9, 5, 0, 0, time.UTC)
	if got, ok := port.PreparedVenueExpiry(heartbeatPrepared{inner: expiringPlacement{expiresAt: expiresAt}}); !ok || !got.Equal(expiresAt) {
		t.Fatalf("heartbeat wrapper expiry = %s, %v", got, ok)
	}
	if _, ok := port.PreparedVenueExpiry(heartbeatPrepared{}); ok {
		t.Fatal("an empty wrapper must not report an expiry")
	}
}
