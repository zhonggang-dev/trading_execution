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
}
