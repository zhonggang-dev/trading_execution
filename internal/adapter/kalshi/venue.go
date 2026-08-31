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

func (venue *Venue) PlacePrepared(ctx context.Context, _ domain.Order, raw port.PreparedPlacement) (port.VenueOrder, error) {
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
	return submittedVenueOrder(order), nil
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
	observed := observedVenueOrder(remote)
	observed.ObservedAt = venue.client.now().UTC()
	return observed, nil
}

func (venue *Venue) Cancel(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	remote, err := venue.remoteOrder(ctx, order)
	if err != nil {
		return port.VenueOrder{}, err
	}
	remote, err = venue.client.CancelOrder(ctx, remote.OrderID)
	if err != nil {
		return port.VenueOrder{}, err
	}
	observed := observedVenueOrder(remote)
	observed.ObservedAt = venue.client.now().UTC()
	return observed, nil
}

func (venue *Venue) remoteOrder(ctx context.Context, order domain.Order) (Order, error) {
	storedID := strings.TrimSpace(order.VenueOrderID)
	clientOrderID := strings.TrimSpace(order.Intent.ClientOrderID)
	var (
		remote Order
		err    error
	)
	if storedID != "" && storedID == clientOrderID {
		remote, err = venue.client.FindOrderByClientOrderID(ctx, clientOrderID)
	} else {
		remote, err = venue.client.GetOrder(ctx, storedID)
	}
	if err != nil {
		return Order{}, err
	}
	if strings.TrimSpace(remote.OrderID) == "" || strings.TrimSpace(remote.ClientOrderID) != clientOrderID ||
		strings.TrimSpace(remote.Ticker) != strings.TrimSpace(order.Intent.MarketID) {
		return Order{}, fmt.Errorf("Kalshi order identity does not match local intent")
	}
	if storedID != clientOrderID && strings.TrimSpace(remote.OrderID) != storedID {
		return Order{}, fmt.Errorf("Kalshi venue order id does not match local order")
	}
	return remote, nil
}

func submittedVenueOrder(order SubmittedOrder) port.VenueOrder {
	state := port.VenueOrderAcknowledged
	if sign, _ := order.RemainingCount.Sign(); sign == 0 {
		if filled, _ := order.FillCount.Sign(); filled > 0 {
			state = port.VenueOrderFilled
		} else {
			state = port.VenueOrderCancelled
		}
	} else if filled, _ := order.FillCount.Sign(); filled > 0 {
		state = port.VenueOrderPartiallyFilled
	}
	observedAt := time.UnixMilli(order.TimestampMS).UTC()
	if order.TimestampMS == 0 {
		observedAt = time.Now().UTC()
	}
	return port.VenueOrder{ID: order.OrderID, State: state, RawStatus: "submitted", FilledSize: order.FillCount,
		AverageFillPrice: order.AverageFillPrice, ObservedAt: observedAt}
}

func observedVenueOrder(order Order) port.VenueOrder {
	state := port.VenueOrderUnknown
	switch strings.ToLower(strings.TrimSpace(order.Status)) {
	case "resting", "open":
		state = port.VenueOrderLive
	case "executed", "filled":
		state = port.VenueOrderFilled
	case "canceled", "cancelled":
		state = port.VenueOrderCancelled
	case "pending":
		state = port.VenueOrderAcknowledged
	case "rejected":
		state = port.VenueOrderRejected
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
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	return port.VenueOrder{ID: order.OrderID, State: state, RawStatus: order.Status,
		FilledSize: order.FillCount, AverageFillPrice: price, ObservedAt: observedAt}
}
