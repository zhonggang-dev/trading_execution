package polymarket

import (
	"context"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type capabilityTestVenue struct{}

func (*capabilityTestVenue) Name() string { return "polymarket" }
func (*capabilityTestVenue) Place(context.Context, domain.Order) (port.VenueOrder, error) {
	return port.VenueOrder{}, nil
}
func (*capabilityTestVenue) Cancel(context.Context, domain.Order) (port.VenueOrder, error) {
	return port.VenueOrder{}, nil
}
func (*capabilityTestVenue) Get(context.Context, domain.Order) (port.VenueOrder, error) {
	return port.VenueOrder{}, nil
}
func (*capabilityTestVenue) SupportsTimeInForce(timeInForce domain.TimeInForce) bool {
	return timeInForce != domain.TimeInForceIOC
}

type capabilityTestEligibility struct{}

func (capabilityTestEligibility) Check(context.Context, string) (GeographicEligibility, error) {
	return GeographicEligibility{}, nil
}

type capabilityTestFunding struct{}

func (capabilityTestFunding) GetBalanceAllowance(context.Context, string, BalanceAssetType, string) (BalanceAllowance, error) {
	return BalanceAllowance{}, nil
}

// TestPolymarketVenueDecoratorsForwardTimeInForceCapability 验证 eligibility 与
// conditional funding 两层封装都透传 CLOB 没有原生 IOC 的事实。
func TestPolymarketVenueDecoratorsForwardTimeInForceCapability(t *testing.T) {
	funding, err := NewConditionalFundingVenue(&capabilityTestVenue{}, capabilityTestFunding{})
	if err != nil {
		t.Fatal(err)
	}
	if funding.SupportsTimeInForce(domain.TimeInForceIOC) || !funding.SupportsTimeInForce(domain.TimeInForceGTC) {
		t.Fatal("conditional funding venue does not forward the time-in-force capability")
	}
	eligible, err := NewEligibilityVenue(funding, capabilityTestEligibility{})
	if err != nil {
		t.Fatal(err)
	}
	if eligible.SupportsTimeInForce(domain.TimeInForceIOC) || !eligible.SupportsTimeInForce(domain.TimeInForceGTC) {
		t.Fatal("eligibility venue does not forward the time-in-force capability")
	}
}
