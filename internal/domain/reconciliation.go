package domain

import (
	"encoding/json"
	"time"
)

// ReconciliationTrigger 表示后端使用的 ReconciliationTrigger 类型。
type ReconciliationTrigger string

const (
	ReconciliationTriggerStartup       ReconciliationTrigger = "STARTUP"
	ReconciliationTriggerScheduled     ReconciliationTrigger = "SCHEDULED"
	ReconciliationTriggerOrderUnknown  ReconciliationTrigger = "ORDER_UNKNOWN"
	ReconciliationTriggerCancelUnknown ReconciliationTrigger = "CANCEL_UNKNOWN"
	ReconciliationTriggerAssetDrift    ReconciliationTrigger = "ASSET_DRIFT"
)

// ReconciliationRunStatus 表示后端使用的 ReconciliationRunStatus 类型。
type ReconciliationRunStatus string

const (
	ReconciliationRunRunning           ReconciliationRunStatus = "RUNNING"
	ReconciliationRunCompleted         ReconciliationRunStatus = "COMPLETED"
	ReconciliationRunAttentionRequired ReconciliationRunStatus = "ATTENTION_REQUIRED"
	ReconciliationRunFailed            ReconciliationRunStatus = "FAILED"
)

// ReconciliationIssueType 表示后端使用的 ReconciliationIssueType 类型。
type ReconciliationIssueType string

const (
	ReconciliationIssueMissedBuyFill                 ReconciliationIssueType = "MISSED_BUY_FILL"
	ReconciliationIssueMissedSellFill                ReconciliationIssueType = "MISSED_SELL_FILL"
	ReconciliationIssueLocalOrderCancelled           ReconciliationIssueType = "LOCAL_ORDER_CANCELLED"
	ReconciliationIssueSubmitUnconfirmed             ReconciliationIssueType = "SUBMIT_UNCONFIRMED"
	ReconciliationIssueExternalOrder                 ReconciliationIssueType = "EXTERNAL_ORDER"
	ReconciliationIssueExternalTrade                 ReconciliationIssueType = "EXTERNAL_TRADE"
	ReconciliationIssuePositionSettled               ReconciliationIssueType = "POSITION_SETTLED"
	ReconciliationIssuePositionDrift                 ReconciliationIssueType = "POSITION_DRIFT"
	ReconciliationIssuePhantomPosition               ReconciliationIssueType = "PHANTOM_POSITION"
	ReconciliationIssueExternalPositionBaselineDrift ReconciliationIssueType = "EXTERNAL_POSITION_BASELINE_DRIFT"
	ReconciliationIssueBalanceDrift                  ReconciliationIssueType = "BALANCE_DRIFT"
	ReconciliationIssueSourceUnavailable             ReconciliationIssueType = "SOURCE_UNAVAILABLE"
	ReconciliationIssueSourceConflict                ReconciliationIssueType = "SOURCE_CONFLICT"
)

// ReconciliationResolution 表示后端使用的 ReconciliationResolution 类型。
type ReconciliationResolution string

const (
	ReconciliationResolutionAutomatic ReconciliationResolution = "AUTOMATIC"
	ReconciliationResolutionManual    ReconciliationResolution = "MANUAL_REVIEW"
	ReconciliationResolutionObserved  ReconciliationResolution = "OBSERVED_ONLY"
	ReconciliationResolutionRetry     ReconciliationResolution = "RETRY_LATER"
)

// ReconciliationIssueStatus 表示后端使用的 ReconciliationIssueStatus 类型。
type ReconciliationIssueStatus string

const (
	ReconciliationIssueOpen     ReconciliationIssueStatus = "OPEN"
	ReconciliationIssueResolved ReconciliationIssueStatus = "RESOLVED"
)

// ReconciliationRun 表示后端使用的 ReconciliationRun 类型。
type ReconciliationRun struct {
	RunID              string                  `json:"run_id"`
	ExecutionAccountID string                  `json:"execution_account_id"`
	Trigger            ReconciliationTrigger   `json:"trigger"`
	FocusOrderID       string                  `json:"focus_order_id,omitempty"`
	Status             ReconciliationRunStatus `json:"status"`
	StartedAt          time.Time               `json:"started_at"`
	CompletedAt        *time.Time              `json:"completed_at,omitempty"`
	Summary            map[string]int          `json:"summary"`
	Error              string                  `json:"error,omitempty"`
}

// ReconciliationIssue 表示后端使用的 ReconciliationIssue 类型。
type ReconciliationIssue struct {
	IssueID            string                    `json:"issue_id"`
	RunID              string                    `json:"run_id"`
	Fingerprint        string                    `json:"fingerprint"`
	ExecutionAccountID string                    `json:"execution_account_id"`
	Type               ReconciliationIssueType   `json:"type"`
	Resolution         ReconciliationResolution  `json:"resolution"`
	Status             ReconciliationIssueStatus `json:"status"`
	OrderID            string                    `json:"order_id,omitempty"`
	VenueOrderID       string                    `json:"venue_order_id,omitempty"`
	VenueTradeID       string                    `json:"venue_trade_id,omitempty"`
	MarketID           string                    `json:"market_id,omitempty"`
	ConditionID        string                    `json:"condition_id,omitempty"`
	TokenID            string                    `json:"token_id,omitempty"`
	LocalValue         Decimal                   `json:"local_value,omitempty"`
	RemoteValue        Decimal                   `json:"remote_value,omitempty"`
	Source             string                    `json:"source,omitempty"`
	Details            string                    `json:"details"`
	ObservedAt         time.Time                 `json:"observed_at"`
	ResolvedAt         *time.Time                `json:"resolved_at,omitempty"`
}

// VenueOrderSnapshot 表示后端使用的 VenueOrderSnapshot 类型。
type VenueOrderSnapshot struct {
	VenueOrderID string    `json:"venue_order_id"`
	ConditionID  string    `json:"condition_id"`
	TokenID      string    `json:"token_id"`
	Side         Side      `json:"side"`
	OriginalSize Decimal   `json:"original_size"`
	FilledSize   Decimal   `json:"filled_size"`
	Price        Decimal   `json:"price"`
	Status       string    `json:"status"`
	ObservedAt   time.Time `json:"observed_at"`
}

// VenueTradeSnapshot 表示后端使用的 VenueTradeSnapshot 类型。
type VenueTradeSnapshot struct {
	VenueTradeID string     `json:"venue_trade_id"`
	OrderIDs     []string   `json:"order_ids"`
	ConditionID  string     `json:"condition_id"`
	TokenID      string     `json:"token_id"`
	Status       FillStatus `json:"status"`
	ObservedAt   time.Time  `json:"observed_at"`
}

// ExternalPosition 表示后端使用的 ExternalPosition 类型。
type ExternalPosition struct {
	ConditionID  string    `json:"condition_id"`
	TokenID      string    `json:"token_id"`
	OutcomeIndex *int      `json:"outcome_index,omitempty"`
	OutcomeName  string    `json:"outcome_name"`
	Shares       Decimal   `json:"shares"`
	AveragePrice Decimal   `json:"average_price,omitempty"`
	CurrentPrice Decimal   `json:"current_price,omitempty"`
	NegRisk      bool      `json:"neg_risk"`
	Redeemable   bool      `json:"redeemable"`
	Source       string    `json:"source"`
	ObservedAt   time.Time `json:"observed_at"`
}

// ExternalPositionBaseline is immutable cutover evidence for shares that
// predate trading_execution ownership. It is not a Position or PositionLot and
// therefore cannot be routed to a strategy or reserved for an order.
type ExternalPositionBaseline struct {
	BaselineID         string          `json:"baseline_id"`
	ExecutionAccountID string          `json:"execution_account_id"`
	ConditionID        string          `json:"condition_id"`
	TokenID            string          `json:"token_id"`
	OutcomeIndex       *int            `json:"outcome_index,omitempty"`
	OutcomeName        string          `json:"outcome_name"`
	NegRisk            bool            `json:"neg_risk"`
	Shares             Decimal         `json:"shares"`
	Source             string          `json:"source"`
	ObservedAt         time.Time       `json:"observed_at"`
	Evidence           json.RawMessage `json:"evidence"`
	Actor              string          `json:"actor"`
	Reason             string          `json:"reason"`
}

// ExternalBalance 表示后端使用的 ExternalBalance 类型。
type ExternalBalance struct {
	Asset      string    `json:"asset"`
	Amount     Decimal   `json:"amount"`
	Source     string    `json:"source"`
	ObservedAt time.Time `json:"observed_at"`
}

// PositionLifecycleStatus 表示后端使用的 PositionLifecycleStatus 类型。
type PositionLifecycleStatus string

const (
	PositionLifecycleOpen                 PositionLifecycleStatus = "OPEN"
	PositionLifecycleSettledPendingRedeem PositionLifecycleStatus = "SETTLED_PENDING_REDEEM"
	PositionLifecycleClosed               PositionLifecycleStatus = "CLOSED"
	PositionLifecycleManualReview         PositionLifecycleStatus = "MANUAL_REVIEW"
)
