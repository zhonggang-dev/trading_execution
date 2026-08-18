package paper

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// ReservationManager is an in-memory paper-only implementation. It models the
// reservation lifecycle for local execution wiring but is intentionally not a
// balance authority and must never be used in live mode.
type ReservationManager struct {
	mu           sync.Mutex
	reservations map[string]domain.AssetReservation
	now          func() time.Time
}

var _ port.AssetReservationManager = (*ReservationManager)(nil)

// NewReservationManager 创建并初始化 Reservation Manager。
func NewReservationManager() *ReservationManager {
	return &ReservationManager{
		reservations: make(map[string]domain.AssetReservation),
		now:          time.Now,
	}
}

// Get 按标识查询并返回当前组件管理的记录。
func (manager *ReservationManager) Get(orderID string) (domain.AssetReservation, bool) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	reservation, ok := manager.reservations[orderID]
	return reservation, ok
}

// Reserve 幂等检查并原子预占订单所需的资金或份额。
func (manager *ReservationManager) Reserve(_ context.Context, order domain.Order) (domain.AssetReservation, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if existing, ok := manager.reservations[order.ID]; ok {
		if existing.ClientOrderID != order.Intent.ClientOrderID || existing.ExecutionAccountID != order.Intent.ExecutionAccountID {
			return domain.AssetReservation{}, port.ErrReservationConflict
		}
		return existing, nil
	}
	now := manager.now().UTC()
	reservation := domain.AssetReservation{
		OrderID:            order.ID,
		ClientOrderID:      order.Intent.ClientOrderID,
		ExecutionAccountID: order.Intent.ExecutionAccountID,
		StrategyID:         order.Intent.StrategyID,
		MarketID:           order.Intent.MarketID,
		TokenID:            order.Intent.TokenID,
		Side:               order.Intent.Side,
		RequestedShares:    order.Intent.Size,
		ReserveUnitPrice:   order.Intent.WorstPrice,
		SettledShares:      "0",
		SettledNotional:    "0",
		SettledFees:        "0",
		Status:             domain.ReservationStatusActive,
		CreatedAt:          now,
		UpdatedAt:          now,
		Revision:           1,
	}
	if reservation.ReserveUnitPrice.IsEmpty() {
		reservation.ReserveUnitPrice = order.Intent.Price
	}
	if order.Intent.Side == domain.SideBuy {
		reservation.InitialReservedBalance = "0"
		reservation.RemainingReservedBalance = "0"
		reservation.InitialReservedShares = "0"
		reservation.RemainingReservedShares = "0"
	} else {
		reservation.InitialReservedBalance = "0"
		reservation.RemainingReservedBalance = "0"
		reservation.InitialReservedShares = order.Intent.Size
		reservation.RemainingReservedShares = order.Intent.Size
	}
	manager.reservations[order.ID] = reservation
	return reservation, nil
}

// Reconcile 按订单累计成交和终态结算或释放资产预占。
func (manager *ReservationManager) Reconcile(_ context.Context, order domain.Order) (domain.AssetReservation, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	reservation, ok := manager.reservations[order.ID]
	if !ok {
		return domain.AssetReservation{}, port.ErrReservationNotFound
	}
	reservation.SettledShares = order.FilledSize
	if reservation.SettledShares.IsEmpty() {
		reservation.SettledShares = "0"
	}
	reservation.UpdatedAt = manager.now().UTC()
	reservation.Revision++
	switch order.Status {
	case domain.OrderStatusFilled:
		reservation.Status = domain.ReservationStatusSettled
		reservation.RemainingReservedBalance = "0"
		reservation.RemainingReservedShares = "0"
		value := reservation.UpdatedAt
		reservation.ReleasedAt = &value
	case domain.OrderStatusCanceled, domain.OrderStatusRejected:
		reservation.Status = domain.ReservationStatusReleased
		reservation.RemainingReservedBalance = "0"
		reservation.RemainingReservedShares = "0"
		value := reservation.UpdatedAt
		reservation.ReleasedAt = &value
	default:
		reservation.Status = domain.ReservationStatusActive
	}
	manager.reservations[order.ID] = reservation
	return reservation, nil
}

// MarkUncertain 将订单资产预占标记为不确定并继续冻结。
func (manager *ReservationManager) MarkUncertain(_ context.Context, order domain.Order, reason string) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	reservation, ok := manager.reservations[order.ID]
	if !ok {
		return fmt.Errorf("%w: %s", port.ErrReservationNotFound, order.ID)
	}
	if reservation.Status == domain.ReservationStatusReleased || reservation.Status == domain.ReservationStatusSettled {
		return nil
	}
	reservation.Status = domain.ReservationStatusReconciliationRequired
	reservation.UncertainReason = reason
	reservation.UpdatedAt = manager.now().UTC()
	reservation.Revision++
	manager.reservations[order.ID] = reservation
	return nil
}
