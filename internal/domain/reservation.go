package domain

import "time"

// ReservationStatus 表示订单的持久化资产预占状态，结果不明确时持续锁定剩余资产直到对账证明可以释放。
type ReservationStatus string

const (
	ReservationStatusActive                 ReservationStatus = "ACTIVE"
	ReservationStatusReleased               ReservationStatus = "RELEASED"
	ReservationStatusSettled                ReservationStatus = "SETTLED"
	ReservationStatusReconciliationRequired ReservationStatus = "RECONCILIATION_REQUIRED"
)

// AssetReservation 表示执行模块为单个订单锁定的资金或份额；BUY 按 worst_price 乘数量锁定资金，SELL 锁定份额。
// 成交字段使用累计口径，以保证重复读取交易所状态时仍然幂等。
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
