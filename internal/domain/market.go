package domain

import "time"

// MarketOutcome is the authoritative outcome/token mapping returned by Market
// Universe Service. The index is the stable bridge for Yes/No and A/B markets.
type MarketOutcome struct {
	Index   int    `json:"index"`
	Name    string `json:"name"`
	TokenID string `json:"token_id"`
}

// MarketSnapshot is the latest execution-facing market metadata. It is not the
// strategy orderbook snapshot.
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

// MarketValidation is the immutable execution-time evidence persisted with an
// order and passed to the Venue adapter.
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
