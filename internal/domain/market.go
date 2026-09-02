package domain

import "time"

// MarketOutcome 表示 Market Universe Service 返回的权威 Outcome 与 Token 映射，Index 用于稳定关联二元市场结果。
type MarketOutcome struct {
	Index   int    `json:"index"`
	Name    string `json:"name"`
	TokenID string `json:"token_id"`
}

// MarketSnapshot 表示供交易执行使用的最新 Market 元数据，不等同于策略订单簿快照。
type MarketSnapshot struct {
	MarketID        string          `json:"market_id"`
	ConditionID     string          `json:"condition_id"`
	Active          bool            `json:"active"`
	Closed          bool            `json:"closed"`
	Resolved        bool            `json:"resolved"`
	ClosedAt        *time.Time      `json:"closed_at,omitempty"`
	Paused          bool            `json:"paused"`
	AcceptingOrders bool            `json:"accepting_orders"`
	NegRisk         bool            `json:"neg_risk"`
	TickSize        Decimal         `json:"tick_size"`
	Outcomes        []MarketOutcome `json:"outcomes"`
	ObservedAt      time.Time       `json:"observed_at"`
}

// MarketValidation 表示随订单持久化并传给交易所适配器的不可变执行时校验证据。
type MarketValidation struct {
	Mode                 string    `json:"mode"`
	ValidatedAt          time.Time `json:"validated_at"`
	MarketObservedAt     time.Time `json:"market_observed_at,omitempty"`
	StrategySnapshotAt   time.Time `json:"strategy_snapshot_at,omitempty"`
	LatestBookSourceAt   time.Time `json:"latest_book_source_at,omitempty"`
	LatestBookObservedAt time.Time `json:"latest_book_observed_at,omitempty"`
	OutcomeIndex         int       `json:"outcome_index"`
	OutcomeName          string    `json:"outcome_name,omitempty"`
	TokenID              string    `json:"token_id,omitempty"`
	NegRisk              bool      `json:"neg_risk"`
	TickSize             Decimal   `json:"tick_size,omitempty"`
	MinOrderSize         Decimal   `json:"min_order_size,omitempty"`
	BestBid              Decimal   `json:"best_bid,omitempty"`
	BestAsk              Decimal   `json:"best_ask,omitempty"`
	WorstPrice           Decimal   `json:"worst_price,omitempty"`
	// ExecutableSize is the venue-visible quantity inside WorstPrice at
	// validation time. Venues that support partial immediate execution may use
	// it to cap the submitted quantity while retaining the strategy-requested
	// size on OrderIntent for audit and reservation finality.
	ExecutableSize Decimal `json:"executable_size,omitempty"`
	// ExecutionPrice is the exact wire price the venue adapter must submit.
	// Polymarket treats a marketable FOK/FAK BUY as a collateral budget
	// (maker amount) rather than a share count, so signing size*worst_price
	// would buy more than Size shares whenever the book is better than the
	// protection line. Market validation pins the BUY wire price to the fresh
	// best ask so the signed budget equals Size shares at the price that will
	// actually match; WorstPrice stays the strategy's protection ceiling only.
	// Empty means the adapter submits the strategy price unchanged.
	ExecutionPrice Decimal `json:"execution_price,omitempty"`
}
