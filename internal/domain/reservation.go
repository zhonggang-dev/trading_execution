package domain

import "time"

// ReservationStatus describes the durable collateral state for an order. An
// uncertain reservation deliberately keeps its remaining collateral locked
// until venue reconciliation proves that it is safe to release.
type ReservationStatus string

const (
	ReservationStatusActive                 ReservationStatus = "ACTIVE"
	ReservationStatusReleased               ReservationStatus = "RELEASED"
	ReservationStatusSettled                ReservationStatus = "SETTLED"
	ReservationStatusReconciliationRequired ReservationStatus = "RECONCILIATION_REQUIRED"
)

// AssetReservation is the execution-owned view of cash or shares locked for
// one order. BUY locks balance at worst_price * size; SELL locks size shares.
// Fill fields are cumulative so repeated venue observations are idempotent.
type AssetReservation struct {
	OrderID                  string            `json:"order_id"`
	ClientOrderID            string            `json:"client_order_id"`
	ExecutionAccountID       string            `json:"execution_account_id"`
	StrategyID               string            `json:"strategy_id"`
	MarketID                 string            `json:"market_id"`
	TokenID                  string            `json:"token_id"`
	TargetLotID              string            `json:"target_lot_id,omitempty"`
	Side                     Side              `json:"side"`
	RequestedShares          Decimal           `json:"requested_shares"`
	ReserveUnitPrice         Decimal           `json:"reserve_unit_price"`
	InitialReservedBalance   Decimal           `json:"initial_reserved_balance"`
	RemainingReservedBalance Decimal           `json:"remaining_reserved_balance"`
	InitialReservedShares    Decimal           `json:"initial_reserved_shares"`
	RemainingReservedShares  Decimal           `json:"remaining_reserved_shares"`
	SettledShares            Decimal           `json:"settled_shares"`
	SettledNotional          Decimal           `json:"settled_notional"`
	SettledFees              Decimal           `json:"settled_fees"`
	Status                   ReservationStatus `json:"status"`
	UncertainReason          string            `json:"uncertain_reason,omitempty"`
	CreatedAt                time.Time         `json:"created_at"`
	UpdatedAt                time.Time         `json:"updated_at"`
	ReleasedAt               *time.Time        `json:"released_at,omitempty"`
	Revision                 int64             `json:"revision"`
}
