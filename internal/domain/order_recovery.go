package domain

import (
	"strings"
	"time"
)

// OrderRecoveryOutcomeKind classifies how one recovery attempt on an order
// ended. It drives the retry schedule that the lease store persists.
type OrderRecoveryOutcomeKind string

const (
	// OrderRecoveryOutcomeResolved means the order left every recovery status;
	// the lease row is removed so a later recovery starts with a clean budget.
	OrderRecoveryOutcomeResolved OrderRecoveryOutcomeKind = "RESOLVED"
	// OrderRecoveryOutcomeWaiting means the attempt succeeded but the order is
	// still in a recovery status (for example fill evidence is propagating).
	// The retry budget is not consumed; only the pending age keeps growing.
	OrderRecoveryOutcomeWaiting OrderRecoveryOutcomeKind = "WAITING"
	// OrderRecoveryOutcomeFailed means the attempt hit a source failure or a
	// per-order timeout. The next retry backs off and the attempt counter grows.
	OrderRecoveryOutcomeFailed OrderRecoveryOutcomeKind = "FAILED"
)

// OrderRecoveryLease is the durable, order-level exclusion and retry record
// shared by the fast order coordinator and the scheduled reconciliation. Only
// the holder may act on the order while the lease is active; the retry fields
// survive process restarts so backoff and escalation are not reset by a crash.
type OrderRecoveryLease struct {
	OrderID            string     `json:"order_id"`
	ExecutionAccountID string     `json:"execution_account_id"`
	Holder             string     `json:"holder,omitempty"`
	OrderRevision      int64      `json:"order_revision"`
	AcquiredAt         *time.Time `json:"acquired_at,omitempty"`
	ExpiresAt          *time.Time `json:"expires_at,omitempty"`
	Attempts           int        `json:"attempts"`
	FirstPendingAt     *time.Time `json:"first_pending_at,omitempty"`
	LastAttemptAt      *time.Time `json:"last_attempt_at,omitempty"`
	NextRetryAt        *time.Time `json:"next_retry_at,omitempty"`
	LastError          string     `json:"last_error,omitempty"`
	EscalatedAt        *time.Time `json:"escalated_at,omitempty"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

// HeldAt reports whether another worker currently owns the lease.
func (lease OrderRecoveryLease) HeldAt(now time.Time, holder string) bool {
	if strings.TrimSpace(lease.Holder) == "" || lease.ExpiresAt == nil {
		return false
	}
	return lease.Holder != holder && lease.ExpiresAt.After(now)
}

// RetryDueAt reports whether the persisted backoff allows a new attempt.
func (lease OrderRecoveryLease) RetryDueAt(now time.Time) bool {
	return lease.NextRetryAt == nil || !lease.NextRetryAt.After(now)
}

// PendingSince returns how long the order has been in recovery without a
// resolved outcome. Zero means no unresolved attempt has been recorded.
func (lease OrderRecoveryLease) PendingSince(now time.Time) time.Duration {
	if lease.FirstPendingAt == nil || lease.FirstPendingAt.After(now) {
		return 0
	}
	return now.Sub(*lease.FirstPendingAt)
}

// Escalated reports whether the order has already been handed to the manual
// queue. Escalation never releases reservations; it only changes who acts.
func (lease OrderRecoveryLease) Escalated() bool {
	return lease.EscalatedAt != nil && !lease.EscalatedAt.IsZero()
}

// OrderRecoveryLeaseRequest identifies one worker's attempt to own an order's
// recovery. OrderRevision is the revision the worker observed; a lease that
// already recorded a newer revision rejects the stale snapshot.
type OrderRecoveryLeaseRequest struct {
	OrderID            string
	ExecutionAccountID string
	Holder             string
	OrderRevision      int64
	Now                time.Time
	TTL                time.Duration
	// BypassBackoff lets an immediate, order-focused trigger run ahead of the
	// persisted retry schedule. It never overrides an active holder.
	BypassBackoff bool
}

// OrderRecoveryRelease records how the holder's attempt ended. NextRetryAt is
// only honored for WAITING/FAILED; Escalate marks the manual queue hand-off.
type OrderRecoveryRelease struct {
	OrderID     string
	Holder      string
	Now         time.Time
	Kind        OrderRecoveryOutcomeKind
	Error       string
	NextRetryAt time.Time
	Escalate    bool
}

// IsOrderRecoveryStatus reports the statuses whose next step is a recovery
// action against the venue rather than ordinary lifecycle polling. Only these
// orders are serialized through the order recovery lease.
func IsOrderRecoveryStatus(status OrderStatus) bool {
	switch status {
	case OrderStatusSubmitting, OrderStatusUnknown, OrderStatusReconciling, OrderStatusCancelPending:
		return true
	default:
		return false
	}
}
