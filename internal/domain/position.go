package domain

import "time"

// Position 表示后端使用的 Position 类型。
type Position struct {
	ExecutionAccountID string                  `json:"execution_account_id"`
	MarketID           string                  `json:"market_id"`
	ConditionID        string                  `json:"condition_id,omitempty"`
	TokenID            string                  `json:"token_id"`
	OutcomeIndex       *int                    `json:"outcome_index,omitempty"`
	OutcomeName        string                  `json:"outcome_name,omitempty"`
	TotalShares        Decimal                 `json:"total_shares"`
	AvailableShares    Decimal                 `json:"available_shares"`
	ReservedShares     Decimal                 `json:"reserved_shares"`
	CostBasis          Decimal                 `json:"cost_basis"`
	AverageCostPrice   Decimal                 `json:"average_cost_price"`
	RealizedPnL        Decimal                 `json:"realized_pnl"`
	MarkPrice          Decimal                 `json:"mark_price,omitempty"`
	MarketValue        Decimal                 `json:"market_value,omitempty"`
	UnrealizedPnL      Decimal                 `json:"unrealized_pnl"`
	IsDust             bool                    `json:"is_dust"`
	LifecycleStatus    PositionLifecycleStatus `json:"lifecycle_status"`
	LastMarkedAt       *time.Time              `json:"last_marked_at,omitempty"`
	UpdatedAt          time.Time               `json:"updated_at"`
	Revision           int64                   `json:"revision"`
}

// PositionLotStatus 表示后端使用的 PositionLotStatus 类型。
type PositionLotStatus string

const (
	PositionLotOpen                 PositionLotStatus = "OPEN"
	PositionLotSettledPendingRedeem PositionLotStatus = "SETTLED_PENDING_REDEEM"
	PositionLotClosed               PositionLotStatus = "CLOSED"
)

// PositionLot 表示后端使用的 PositionLot 类型。
type PositionLot struct {
	LotID              string            `json:"lot_id"`
	ExecutionAccountID string            `json:"execution_account_id"`
	MarketID           string            `json:"market_id"`
	ConditionID        string            `json:"condition_id"`
	TokenID            string            `json:"token_id"`
	OutcomeIndex       *int              `json:"outcome_index,omitempty"`
	OutcomeName        string            `json:"outcome_name"`
	NegRisk            *bool             `json:"neg_risk,omitempty"`
	OriginModelID      string            `json:"-"`
	ModelID            string            `json:"model_id"`
	StrategyID         string            `json:"strategy_id"`
	OpeningOrderID     string            `json:"opening_order_id"`
	OpeningFillKey     string            `json:"opening_fill_key"`
	OriginalShares     Decimal           `json:"original_shares"`
	RemainingShares    Decimal           `json:"remaining_shares"`
	OriginalCost       Decimal           `json:"original_cost"`
	RemainingCost      Decimal           `json:"remaining_cost"`
	AverageEntryPrice  Decimal           `json:"average_entry_price"`
	Status             PositionLotStatus `json:"status"`
	OpenedAt           time.Time         `json:"opened_at"`
	ClosedAt           *time.Time        `json:"closed_at,omitempty"`
}

// PositionEventType 表示后端使用的 PositionEventType 类型。
type PositionEventType string

const (
	PositionEventBought  PositionEventType = "BOUGHT"
	PositionEventSold    PositionEventType = "SOLD"
	PositionEventMarked  PositionEventType = "MARKED"
	PositionEventSettled PositionEventType = "SETTLED"
)

// PositionEvent 表示后端使用的 PositionEvent 类型。
type PositionEvent struct {
	EventID            string            `json:"event_id"`
	EventType          PositionEventType `json:"event_type"`
	ExecutionAccountID string            `json:"execution_account_id"`
	MarketID           string            `json:"market_id"`
	TokenID            string            `json:"token_id"`
	OrderID            string            `json:"order_id,omitempty"`
	FillKey            string            `json:"fill_key,omitempty"`
	ModelID            string            `json:"model_id,omitempty"`
	StrategyID         string            `json:"strategy_id,omitempty"`
	SharesDelta        Decimal           `json:"shares_delta"`
	CashDelta          Decimal           `json:"cash_delta"`
	CostBasisDelta     Decimal           `json:"cost_basis_delta"`
	RealizedPnLDelta   Decimal           `json:"realized_pnl_delta"`
	SharesAfter        Decimal           `json:"shares_after"`
	CostBasisAfter     Decimal           `json:"cost_basis_after"`
	AverageCostAfter   Decimal           `json:"average_cost_after"`
	RealizedPnLAfter   Decimal           `json:"realized_pnl_after"`
	MarkPrice          Decimal           `json:"mark_price,omitempty"`
	UnrealizedPnLAfter Decimal           `json:"unrealized_pnl_after"`
	OccurredAt         time.Time         `json:"occurred_at"`
}

// PositionMark 表示后端使用的 PositionMark 类型。
type PositionMark struct {
	ExecutionAccountID string    `json:"execution_account_id"`
	TokenID            string    `json:"token_id"`
	Price              Decimal   `json:"price"`
	ObservedAt         time.Time `json:"observed_at"`
}

// AccountEvent 表示后端使用的 AccountEvent 类型。
type AccountEvent struct {
	EventID            string    `json:"event_id"`
	ExecutionAccountID string    `json:"execution_account_id"`
	EventType          string    `json:"event_type"`
	OrderID            string    `json:"order_id,omitempty"`
	FillKey            string    `json:"fill_key,omitempty"`
	TotalBalanceDelta  Decimal   `json:"total_balance_delta"`
	AvailableDelta     Decimal   `json:"available_balance_delta"`
	ReservedDelta      Decimal   `json:"reserved_balance_delta"`
	TotalBalanceAfter  Decimal   `json:"total_balance_after"`
	AvailableAfter     Decimal   `json:"available_balance_after"`
	ReservedAfter      Decimal   `json:"reserved_balance_after"`
	OccurredAt         time.Time `json:"occurred_at"`
}

// AccountBalance 表示后端使用的 AccountBalance 类型。
type AccountBalance struct {
	ExecutionAccountID string     `json:"execution_account_id"`
	WalletAddress      string     `json:"wallet_address"`
	CollateralAsset    string     `json:"collateral_asset"`
	TotalBalance       Decimal    `json:"total_balance"`
	AvailableBalance   Decimal    `json:"available_balance"`
	ReservedBalance    Decimal    `json:"reserved_balance"`
	ReconciledAt       *time.Time `json:"reconciled_at,omitempty"`
	UpdatedAt          time.Time  `json:"updated_at"`
	Revision           int64      `json:"revision"`
}
