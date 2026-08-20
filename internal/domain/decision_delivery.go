package domain

import "time"

// DecisionIntentDeliveryStatus is the durable hand-off state between a
// validated strategy output and the idempotent execution service.
type DecisionIntentDeliveryStatus string

const (
	DecisionIntentPending    DecisionIntentDeliveryStatus = "PENDING"
	DecisionIntentSubmitting DecisionIntentDeliveryStatus = "SUBMITTING"
	DecisionIntentSubmitted  DecisionIntentDeliveryStatus = "SUBMITTED"
	DecisionIntentFailed     DecisionIntentDeliveryStatus = "FAILED"
	DecisionIntentUnknown    DecisionIntentDeliveryStatus = "UNKNOWN"
)

// DecisionIntentDelivery contains the immutable intent plus the current
// delivery state. Attempt is incremented by every exclusive claim and is used
// as a fencing token when the worker completes the claim.
type DecisionIntentDelivery struct {
	CycleID       string
	ClientOrderID string
	Sequence      int
	Intent        OrderIntent
	Status        DecisionIntentDeliveryStatus
	Attempt       int
	ClaimedAt     *time.Time
	CompletedAt   *time.Time
	OrderID       string
	OrderStatus   OrderStatus
	LastError     string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// DecisionIntentCompletion is the terminal result written with a fencing
// attempt. UNKNOWN is deliberately terminal for automatic delivery; order
// reconciliation, rather than another Place request, resolves it.
type DecisionIntentCompletion struct {
	Status      DecisionIntentDeliveryStatus
	OrderID     string
	OrderStatus OrderStatus
	LastError   string
}
