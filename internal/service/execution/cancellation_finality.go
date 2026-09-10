package execution

import (
	"context"
	"fmt"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// alreadyFinalizedCancellation prevents a historical, fully released order
// from being re-gated when the venue later retires its GET-order record. This
// cannot release assets: the durable release must already be complete, and
// authoritative trade synchronization is retained for possible late fills.
func (service *Service) alreadyFinalizedCancellation(ctx context.Context, order domain.Order) (bool, domain.Order, error) {
	reader, ok := service.reservations.(port.OrderReservationReader)
	if !ok {
		return false, order, nil
	}
	reservation, err := reader.GetOrderReservation(ctx, order.ID)
	if err != nil {
		return false, order, fmt.Errorf("read cancellation reservation: %w", err)
	}
	if reservation.Status != domain.ReservationStatusReleased {
		return false, order, nil
	}
	if !releasedCancellationMatches(order, reservation) {
		return false, order, fmt.Errorf("released cancellation reservation does not match order accounting")
	}
	if service.authoritativeFills {
		result, err := service.fillSynchronizer.SyncOrder(ctx, order.ID)
		if err != nil {
			return false, order, fmt.Errorf("sync finalized cancellation trades: %w", err)
		}
		for _, application := range result.Applications {
			if application.Fill.Status != domain.FillStatusConfirmed {
				return false, order, ErrCancelFinalityPending
			}
		}
		order, err = service.repository.Get(ctx, order.ID)
		if err != nil {
			return false, order, fmt.Errorf("reload finalized cancellation: %w", err)
		}
		if order.Status == domain.OrderStatusFilled {
			return true, order, nil
		}
		reservation, err = reader.GetOrderReservation(ctx, order.ID)
		if err != nil {
			return false, order, fmt.Errorf("reload released reservation: %w", err)
		}
		if !releasedCancellationMatches(order, reservation) {
			return false, order, fmt.Errorf("finalized cancellation changed during trade synchronization")
		}
	}
	return true, order, nil
}

func releasedCancellationMatches(order domain.Order, r domain.AssetReservation) bool {
	return order.Status == domain.OrderStatusCancelled && r.Status == domain.ReservationStatusReleased && r.ReleasedAt != nil &&
		r.OrderID == order.ID && r.ClientOrderID == order.Intent.ClientOrderID && r.ExecutionAccountID == order.Intent.ExecutionAccountID &&
		r.StrategyID == order.Intent.StrategyID && r.MarketID == order.Intent.MarketID && r.TokenID == order.Intent.TokenID && r.TargetLotID == order.Intent.TargetLotID && r.Side == order.Intent.Side &&
		r.RequestedShares.Equal(order.Intent.Size) && r.RemainingReservedBalance.Equal("0") && r.RemainingReservedShares.Equal("0") &&
		r.SettledShares.Equal(order.FilledSize) && r.SettledNotional.Equal(order.FilledNotional) && r.SettledFees.Equal(order.TotalFees)
}
