package kalshi

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// Venue adapts one isolated Kalshi credential to the durable execution state
// machine. Binding/account routing happens above this object, so a Venue can
// never silently fall back to another API key.
type Venue struct{ client *Client }

type preparedPlacement struct{ order PreparedOrder }

func (placement preparedPlacement) ExpectedVenueOrderID() string {
	return placement.order.Request.ClientOrderID
}

func NewVenue(client *Client) (*Venue, error) {
	if client == nil {
		return nil, fmt.Errorf("Kalshi venue client is required")
	}
	return &Venue{client: client}, nil
}

func (venue *Venue) Name() string { return "kalshi" }

func (venue *Venue) PreparePlace(_ context.Context, order domain.Order) (port.PreparedPlacement, error) {
	prepared, err := venue.client.PrepareOrder(order.Intent)
	if err != nil {
		return nil, err
	}
	return preparedPlacement{order: prepared}, nil
}

func (venue *Venue) PlacePrepared(ctx context.Context, local domain.Order, raw port.PreparedPlacement) (port.VenueOrder, error) {
	placement, ok := raw.(preparedPlacement)
	if !ok {
		return port.VenueOrder{}, fmt.Errorf("unexpected Kalshi prepared placement type")
	}
	order, err := venue.client.SubmitPrepared(ctx, placement.order)
	if err != nil {
		var venueError *port.VenueError
		if errors.As(err, &venueError) && venueError.Kind == port.VenueErrorAmbiguous && strings.TrimSpace(venueError.VenueOrderID) == "" {
			venueError.VenueOrderID = strings.TrimSpace(placement.order.Request.ClientOrderID)
		}
		return port.VenueOrder{}, err
	}
	return submittedVenueOrder(order, local.Intent), nil
}

func (venue *Venue) Place(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	prepared, err := venue.PreparePlace(ctx, order)
	if err != nil {
		return port.VenueOrder{}, err
	}
	return venue.PlacePrepared(ctx, order, prepared)
}

func (venue *Venue) Get(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	remote, err := venue.remoteOrder(ctx, order)
	if err != nil {
		return port.VenueOrder{}, err
	}
	return monotonicVenueObservation(remote, order, venue.client.now().UTC()), nil
}

func (venue *Venue) Cancel(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	remote, err := venue.remoteOrder(ctx, order)
	if err != nil {
		return port.VenueOrder{}, err
	}
	preCancelObservation := monotonicVenueObservation(remote, order, venue.client.now().UTC())
	if preCancelObservation.State == port.VenueOrderCancelled || preCancelObservation.State == port.VenueOrderFilled {
		return preCancelObservation, nil
	}
	prepared, err := venue.client.PrepareOrder(order.Intent)
	if err != nil {
		return port.VenueOrder{}, fmt.Errorf("rebuild Kalshi cancellation route: %w", err)
	}
	authoritativeOrderID := remote.OrderID
	remote, err = venue.client.CancelOrder(ctx, authoritativeOrderID, remote.Ticker, prepared.Request.Subaccount)
	if err != nil {
		return port.VenueOrder{}, err
	}
	if err := venue.validateFetchedRemoteOrder(remote, order, authoritativeOrderID); err != nil {
		return port.VenueOrder{}, kalshiAmbiguousOrderError("KALSHI_CANCEL_IDENTITY_MISMATCH", authoritativeOrderID, err)
	}
	return monotonicVenueObservation(remote, order, venue.client.now().UTC()), nil
}

// monotonicVenueObservation uses Kalshi's stable matching-engine update time
// for cancellation finality without allowing an eventually visible order to
// move the local observation clock backwards after an ambiguous request.
func monotonicVenueObservation(remote Order, local domain.Order, observedNow time.Time) port.VenueOrder {
	observed := observedVenueOrder(remote)
	filledComparison, filledErr := remote.FillCount.Compare(local.Intent.Size)
	remainingSign, remainingErr := remote.RemainingCount.Sign()
	remoteStatus := strings.ToLower(strings.TrimSpace(remote.Status))
	terminal := remoteStatus == "canceled" || remoteStatus == "executed"
	if filledErr == nil && remainingErr == nil && terminal && remainingSign == 0 && filledComparison < 0 {
		// Local size is the requested size; Kalshi may reduce the effective size
		// of reduce-only SELLs. Any terminal order that executed fewer contracts
		// therefore remains a cancelled-with-partial-fill result locally.
		observed.State = port.VenueOrderCancelled
		observed.RawStatus = "terminal_remainder_cancelled"
	}
	if observed.ObservedAt.IsZero() {
		if local.VenueLastObservedAt != nil {
			observed.ObservedAt = local.VenueLastObservedAt.UTC()
		} else {
			observed.ObservedAt = observedNow.UTC()
		}
	} else if local.VenueLastObservedAt != nil && observed.ObservedAt.Before(local.VenueLastObservedAt.UTC()) {
		observed.ObservedAt = local.VenueLastObservedAt.UTC()
	}
	return observed
}

func (venue *Venue) remoteOrder(ctx context.Context, order domain.Order) (Order, error) {
	storedID := strings.TrimSpace(order.VenueOrderID)
	clientOrderID := strings.TrimSpace(order.Intent.ClientOrderID)
	var (
		remote          Order
		authoritativeID = storedID
		err             error
	)
	if storedID != "" && storedID == clientOrderID {
		var listed Order
		listed, err = venue.client.FindOrderByClientOrderID(ctx, clientOrderID)
		if err == nil {
			if strings.TrimSpace(listed.OrderID) == "" || strings.TrimSpace(listed.ClientOrderID) != clientOrderID ||
				strings.TrimSpace(listed.Ticker) != strings.TrimSpace(order.Intent.MarketID) {
				return Order{}, fmt.Errorf("Kalshi listed order identity does not match local intent")
			}
			authoritativeID = listed.OrderID
			remote, err = venue.client.GetOrder(ctx, listed.OrderID)
		}
	} else {
		remote, err = venue.client.GetOrder(ctx, storedID)
	}
	if err != nil {
		return Order{}, err
	}
	if err := venue.validateFetchedRemoteOrder(remote, order, authoritativeID); err != nil {
		return Order{}, err
	}
	return remote, nil
}

func (venue *Venue) validateFetchedRemoteOrder(remote Order, local domain.Order, authoritativeID string) error {
	clientOrderID := strings.TrimSpace(local.Intent.ClientOrderID)
	if strings.TrimSpace(remote.OrderID) == "" || strings.TrimSpace(remote.ClientOrderID) != clientOrderID ||
		strings.TrimSpace(remote.Ticker) != strings.TrimSpace(local.Intent.MarketID) {
		return fmt.Errorf("Kalshi order identity does not match local intent")
	}
	if strings.TrimSpace(remote.OrderID) != strings.TrimSpace(authoritativeID) {
		return fmt.Errorf("Kalshi venue order id does not match the authoritative order")
	}
	prepared, err := venue.client.PrepareOrder(local.Intent)
	if err != nil {
		return fmt.Errorf("rebuild Kalshi order identity: %w", err)
	}
	if err := validateRemoteOrderIdentity(remote, prepared.Request); err != nil {
		return err
	}
	return nil
}

func validateRemoteOrderIdentity(order Order, expected OrderRequestV2) error {
	expectedOutcomeSide := "yes"
	if strings.EqualFold(expected.Side, "ask") {
		expectedOutcomeSide = "no"
	}
	initialCountComparison, initialCountErr := order.InitialCount.Compare(domain.Decimal(expected.Count))
	initialCountMatches := initialCountErr == nil && initialCountComparison == 0
	if expected.ReduceOnly {
		initialCountMatches = initialCountErr == nil && initialCountComparison <= 0
	}
	if !strings.EqualFold(order.OutcomeSide, expectedOutcomeSide) || !strings.EqualFold(order.BookSide, expected.Side) ||
		!strings.EqualFold(order.Type, "limit") || !order.YesPrice.Equal(domain.Decimal(expected.Price)) ||
		!initialCountMatches {
		return fmt.Errorf("Kalshi order canonical direction, type, price, or initial count does not match local intent")
	}
	if remoteTimeInForce := strings.TrimSpace(order.TimeInForce); remoteTimeInForce != "" &&
		!strings.EqualFold(remoteTimeInForce, expected.TimeInForce) {
		return fmt.Errorf("Kalshi order time_in_force does not match local intent")
	}
	if expected.TimeInForce == "immediate_or_cancel" || expected.TimeInForce == "fill_or_kill" {
		switch strings.ToLower(strings.TrimSpace(order.Status)) {
		case "canceled", "executed":
		default:
			return fmt.Errorf("Kalshi immediate order did not reach a terminal status")
		}
	}
	if order.CancelOrderOnPause != nil && !*order.CancelOrderOnPause {
		return fmt.Errorf("Kalshi order cancel_order_on_pause does not match local intent")
	}
	if order.SubaccountNumber != nil && *order.SubaccountNumber != expected.Subaccount {
		return fmt.Errorf("Kalshi order subaccount does not match local intent")
	}
	return nil
}

func submittedVenueOrder(order SubmittedOrder, intent domain.OrderIntent) port.VenueOrder {
	state := port.VenueOrderAcknowledged
	rawStatus := "submitted"
	comparison, _ := order.FillCount.Compare(intent.Size)
	filledSign, _ := order.FillCount.Sign()
	remainingSign, _ := order.RemainingCount.Sign()
	if comparison == 0 {
		state = port.VenueOrderFilled
	} else if intent.TimeInForce == domain.TimeInForceIOC {
		// IOC cancels every unfilled contract before returning. Keep the terminal
		// cancellation even when the response also proves a partial fill; the
		// authoritative fill ledger records each fill against that terminal order.
		state = port.VenueOrderCancelled
		rawStatus = "ioc_remainder_cancelled"
	} else if remainingSign == 0 {
		state = port.VenueOrderCancelled
	} else if filledSign > 0 {
		state = port.VenueOrderPartiallyFilled
	}
	observedAt := time.UnixMilli(order.TimestampMS).UTC()
	if order.TimestampMS == 0 {
		observedAt = time.Now().UTC()
	}
	return port.VenueOrder{ID: order.OrderID, State: state, RawStatus: rawStatus, FilledSize: order.FillCount,
		AverageFillPrice: order.AverageFillPrice, ObservedAt: observedAt}
}

func observedVenueOrder(order Order) port.VenueOrder {
	state := port.VenueOrderUnknown
	switch strings.ToLower(strings.TrimSpace(order.Status)) {
	case "resting":
		state = port.VenueOrderLive
	case "executed":
		state = port.VenueOrderFilled
	case "canceled":
		state = port.VenueOrderCancelled
	}
	price := domain.Decimal("")
	if sign, _ := order.FillCount.Sign(); sign > 0 {
		cost, costOK := new(big.Rat).SetString(order.TakerFillCost.String())
		maker, makerOK := new(big.Rat).SetString(order.MakerFillCost.String())
		count, countOK := new(big.Rat).SetString(order.FillCount.String())
		if costOK && makerOK && countOK && count.Sign() > 0 {
			price = domain.Decimal(new(big.Rat).Quo(new(big.Rat).Add(cost, maker), count).FloatString(4))
		}
	}
	observedAt := order.LastUpdateTime.UTC()
	return port.VenueOrder{ID: order.OrderID, State: state, RawStatus: order.Status,
		FilledSize: order.FillCount, AverageFillPrice: price, ObservedAt: observedAt}
}
