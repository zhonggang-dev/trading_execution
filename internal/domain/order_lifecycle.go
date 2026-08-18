package domain

import "time"

// OrderTransitionTrigger 表示后端使用的 OrderTransitionTrigger 类型。
type OrderTransitionTrigger string

const (
	TransitionTriggerReceived       OrderTransitionTrigger = "RECEIVED"
	TransitionTriggerValidation     OrderTransitionTrigger = "VALIDATION"
	TransitionTriggerReservation    OrderTransitionTrigger = "RESERVATION"
	TransitionTriggerSubmit         OrderTransitionTrigger = "SUBMIT"
	TransitionTriggerVenueResponse  OrderTransitionTrigger = "VENUE_RESPONSE"
	TransitionTriggerVenueObserve   OrderTransitionTrigger = "VENUE_OBSERVATION"
	TransitionTriggerFill           OrderTransitionTrigger = "FILL"
	TransitionTriggerCancel         OrderTransitionTrigger = "CANCEL"
	TransitionTriggerReconciliation OrderTransitionTrigger = "RECONCILIATION"
	TransitionTriggerOperator       OrderTransitionTrigger = "OPERATOR"
)

// OrderEvent is an immutable audit record. It intentionally stores normalized
// outcome metadata rather than signed request bodies or credentials.
type OrderEvent struct {
	ID              string                 `json:"event_id"`
	OrderID         string                 `json:"order_id"`
	Revision        int64                  `json:"revision"`
	FromStatus      OrderStatus            `json:"from_status,omitempty"`
	ToStatus        OrderStatus            `json:"to_status"`
	Trigger         OrderTransitionTrigger `json:"trigger"`
	AttemptID       string                 `json:"attempt_id,omitempty"`
	FillKey         string                 `json:"fill_key,omitempty"`
	ReasonCode      string                 `json:"reason_code,omitempty"`
	Reason          string                 `json:"reason,omitempty"`
	VenueStatus     string                 `json:"venue_status,omitempty"`
	VenueOrderID    string                 `json:"venue_order_id,omitempty"`
	FilledSize      Decimal                `json:"filled_size"`
	FilledNotional  Decimal                `json:"filled_notional"`
	TotalFees       Decimal                `json:"total_fees"`
	FillPrice       Decimal                `json:"average_fill_price,omitempty"`
	VenueObservedAt *time.Time             `json:"venue_observed_at,omitempty"`
	OccurredAt      time.Time              `json:"occurred_at"`
}

// OrderAttemptKind 表示后端使用的 OrderAttemptKind 类型。
type OrderAttemptKind string

const (
	OrderAttemptSubmit    OrderAttemptKind = "SUBMIT"
	OrderAttemptCancel    OrderAttemptKind = "CANCEL"
	OrderAttemptReconcile OrderAttemptKind = "RECONCILE"
)

// OrderAttemptOutcome 表示后端使用的 OrderAttemptOutcome 类型。
type OrderAttemptOutcome string

const (
	AttemptOutcomeStarted   OrderAttemptOutcome = "STARTED"
	AttemptOutcomeSucceeded OrderAttemptOutcome = "SUCCEEDED"
	AttemptOutcomeRejected  OrderAttemptOutcome = "REJECTED"
	AttemptOutcomeUnknown   OrderAttemptOutcome = "UNKNOWN"
	AttemptOutcomeFailed    OrderAttemptOutcome = "FAILED"
)

// OrderAttempt records exactly one state-changing venue call or one
// reconciliation pass. RequestFingerprint is a SHA-256 of the non-secret
// canonical request identity; signatures and credentials are never persisted.
type OrderAttempt struct {
	ID                 string              `json:"attempt_id"`
	OrderID            string              `json:"order_id"`
	Sequence           int                 `json:"sequence"`
	Kind               OrderAttemptKind    `json:"kind"`
	Outcome            OrderAttemptOutcome `json:"outcome"`
	RequestFingerprint string              `json:"request_fingerprint,omitempty"`
	VenueOrderID       string              `json:"venue_order_id,omitempty"`
	VenueStatus        string              `json:"venue_status,omitempty"`
	HTTPStatus         int                 `json:"http_status,omitempty"`
	ErrorCode          string              `json:"error_code,omitempty"`
	ErrorMessage       string              `json:"error_message,omitempty"`
	StartedAt          time.Time           `json:"started_at"`
	CompletedAt        *time.Time          `json:"completed_at,omitempty"`
}
