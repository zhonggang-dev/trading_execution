package polymarket

import (
	"context"
	"fmt"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// BalanceAllowanceSource exposes the authenticated, read-only CLOB funding
// cache used immediately before a SELL. The signed order is not submitted
// unless the exact outcome token balance and relevant exchange approval are
// sufficient.
type BalanceAllowanceSource interface {
	GetBalanceAllowance(context.Context, string, BalanceAssetType, string) (BalanceAllowance, error)
}

type ConditionalFundingVenue struct {
	venue  port.Venue
	source BalanceAllowanceSource
}

func NewConditionalFundingVenue(venue port.Venue, source BalanceAllowanceSource) (*ConditionalFundingVenue, error) {
	if venue == nil || source == nil {
		return nil, fmt.Errorf("venue and conditional funding source are required")
	}
	return &ConditionalFundingVenue{venue: venue, source: source}, nil
}

func (venue *ConditionalFundingVenue) Name() string { return venue.venue.Name() }

func (venue *ConditionalFundingVenue) Place(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	if err := venue.checkPlace(ctx, order); err != nil {
		return port.VenueOrder{}, err
	}
	return venue.venue.Place(ctx, order)
}

func (venue *ConditionalFundingVenue) checkPlace(ctx context.Context, order domain.Order) error {
	if order.Intent.Side != domain.SideSell {
		return nil
	}
	if order.MarketValidation == nil || strings.TrimSpace(order.Intent.TokenID) == "" {
		return preSubmitFundingRejection(
			"CLOB_CONDITIONAL_FUNDING_IDENTITY_MISSING",
			"SELL requires validated market identity before checking outcome-token funding",
			nil,
		)
	}
	allowance, err := venue.source.GetBalanceAllowance(
		ctx, order.Intent.ExecutionAccountID, BalanceAssetConditional, order.Intent.TokenID,
	)
	if err != nil {
		return preSubmitFundingRejection(
			"CLOB_CONDITIONAL_FUNDING_CHECK_FAILED",
			"SELL outcome-token funding could not be verified before submission",
			err,
		)
	}
	requiredExchange := StandardExchangeV2Address
	if order.MarketValidation.NegRisk {
		requiredExchange = NegRiskExchangeV2Address
	}
	if !allowance.RequiredAllowancesPositive(requiredExchange) {
		return preSubmitFundingRejection(
			"CLOB_CONDITIONAL_ALLOWANCE_INSUFFICIENT",
			"SELL outcome token is not approved for the validated CLOB V2 exchange",
			nil,
		)
	}
	balance, err := decimalFromBaseUnits(allowance.Balance, 6)
	if err != nil {
		return preSubmitFundingRejection(
			"CLOB_CONDITIONAL_BALANCE_INVALID",
			"SELL outcome-token balance response is malformed",
			err,
		)
	}
	comparison, err := balance.Compare(order.Intent.Size)
	if err != nil || comparison < 0 {
		return preSubmitFundingRejection(
			"CLOB_CONDITIONAL_BALANCE_INSUFFICIENT",
			"SELL outcome-token balance is below the requested size",
			err,
		)
	}
	return nil
}

type conditionalFundingPrepared struct{ inner port.PreparedPlacement }

func (prepared conditionalFundingPrepared) ExpectedVenueOrderID() string {
	if prepared.inner == nil {
		return ""
	}
	return prepared.inner.ExpectedVenueOrderID()
}

func (venue *ConditionalFundingVenue) PreparePlace(ctx context.Context, order domain.Order) (port.PreparedPlacement, error) {
	if err := venue.checkPlace(ctx, order); err != nil {
		return nil, err
	}
	underlying, ok := venue.venue.(port.PreparedVenue)
	if !ok {
		return nil, preSubmitFundingRejection("CLOB_PREPARED_PLACEMENT_UNSUPPORTED", "underlying live venue does not support prepared placement", nil)
	}
	prepared, err := underlying.PreparePlace(ctx, order)
	if err != nil {
		return nil, err
	}
	return conditionalFundingPrepared{inner: prepared}, nil
}

func (venue *ConditionalFundingVenue) PlacePrepared(ctx context.Context, order domain.Order, placement port.PreparedPlacement) (port.VenueOrder, error) {
	prepared, ok := placement.(conditionalFundingPrepared)
	underlying, supported := venue.venue.(port.PreparedVenue)
	if !ok || !supported || prepared.inner == nil {
		return port.VenueOrder{}, preSubmitFundingRejection("CLOB_PREPARED_PLACEMENT_INVALID", "conditional funding prepared placement is invalid", nil)
	}
	// Recheck immediately before delegation because signing and the durable
	// StartAttempt happen between the prepare check and the placement POST.
	if err := venue.checkPlace(ctx, order); err != nil {
		return port.VenueOrder{}, err
	}
	return underlying.PlacePrepared(ctx, order, prepared.inner)
}

func (venue *ConditionalFundingVenue) Cancel(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	return venue.venue.Cancel(ctx, order)
}

func (venue *ConditionalFundingVenue) Get(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	return venue.venue.Get(ctx, order)
}

// A failed read-only pre-submit check proves that no order bytes were sent.
// Treat it as a definitive local rejection so the reservation is released
// rather than incorrectly frozen as an ambiguous exchange submission.
func preSubmitFundingRejection(code, message string, cause error) error {
	return &port.VenueError{
		Kind: port.VenueErrorRejected, Code: code, Message: message, Cause: cause,
	}
}

var _ port.Venue = (*ConditionalFundingVenue)(nil)
var _ port.PreparedVenue = (*ConditionalFundingVenue)(nil)
