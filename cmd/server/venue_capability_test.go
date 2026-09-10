package main

import (
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// iocIncapableVenue mimics the Polymarket CLOB, which has no native IOC.
type iocIncapableVenue struct{ placementTestVenue }

func (*iocIncapableVenue) SupportsTimeInForce(timeInForce domain.TimeInForce) bool {
	return timeInForce != domain.TimeInForceIOC
}

// TestPlacementReadinessVenueForwardsTimeInForceCapability 验证生产封装把底层
// venue 的 IOC 能力透传给执行服务；否则模拟 IOC 的未成交余量永远不会被撤销。
func TestPlacementReadinessVenueForwardsTimeInForceCapability(t *testing.T) {
	venue, err := newPlacementReadinessVenue(&iocIncapableVenue{})
	if err != nil {
		t.Fatal(err)
	}
	var asVenue port.Venue = venue
	support, ok := asVenue.(port.TimeInForceSupport)
	if !ok {
		t.Fatal("placement readiness venue does not expose TimeInForceSupport")
	}
	if support.SupportsTimeInForce(domain.TimeInForceIOC) {
		t.Fatal("wrapper reports native IOC although the underlying venue has none")
	}
	if !support.SupportsTimeInForce(domain.TimeInForceGTC) {
		t.Fatal("wrapper hides native GTC support")
	}

	plain, err := newPlacementReadinessVenue(&placementTestVenue{})
	if err != nil {
		t.Fatal(err)
	}
	if !plain.SupportsTimeInForce(domain.TimeInForceIOC) {
		t.Fatal("a venue without the capability interface must be treated as native for every time in force")
	}
}
