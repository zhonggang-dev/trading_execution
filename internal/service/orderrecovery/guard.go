// Package orderrecovery serializes and paces recovery work on a single order.
//
// Two workers act on orders whose venue outcome is uncertain: the fast order
// coordinator (every few seconds) and the scheduled reconciliation (every few
// minutes). Both already lose gracefully on the order revision compare-and-
// swap, but a losing worker still spends a venue round-trip and both retry a
// permanently failing order at full speed. The Guard puts one durable lease
// in front of that work, bounds each attempt with a timeout, backs off after
// failures, and hands long-running failures to the manual queue without ever
// touching the order's reservation.
package orderrecovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// Policy bounds one recovery attempt and paces the retries of one order.
type Policy struct {
	// LeaseTTL is how long a holder may keep an order before another worker
	// may take it over; it must exceed Timeout so a live attempt is never
	// stolen.
	LeaseTTL time.Duration
	// Timeout bounds a single attempt so one hanging venue call cannot delay
	// every other order in the same sweep.
	Timeout time.Duration
	// BaseBackoff and MaxBackoff shape the exponential retry schedule after a
	// failed attempt. A WAITING outcome (order still propagating) never backs
	// off.
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	// EscalateAfter is the pending age after which the order is handed to the
	// manual queue. Retries continue at the capped backoff; nothing is released.
	EscalateAfter time.Duration
}

const (
	defaultLeaseTTL      = 2 * time.Minute
	defaultTimeout       = 30 * time.Second
	defaultBaseBackoff   = 30 * time.Second
	defaultMaxBackoff    = 10 * time.Minute
	defaultEscalateAfter = 30 * time.Minute
)

// Normalize fills defaults and rejects contradictory settings.
func (policy Policy) Normalize() (Policy, error) {
	if policy.LeaseTTL == 0 {
		policy.LeaseTTL = defaultLeaseTTL
	}
	if policy.Timeout == 0 {
		policy.Timeout = defaultTimeout
	}
	if policy.BaseBackoff == 0 {
		policy.BaseBackoff = defaultBaseBackoff
	}
	if policy.MaxBackoff == 0 {
		policy.MaxBackoff = defaultMaxBackoff
	}
	if policy.EscalateAfter == 0 {
		policy.EscalateAfter = defaultEscalateAfter
	}
	if policy.Timeout < time.Second {
		return Policy{}, fmt.Errorf("order recovery timeout must be at least one second")
	}
	if policy.LeaseTTL <= policy.Timeout {
		return Policy{}, fmt.Errorf("order recovery lease ttl must exceed the per-order timeout")
	}
	if policy.BaseBackoff < time.Second {
		return Policy{}, fmt.Errorf("order recovery base backoff must be at least one second")
	}
	if policy.MaxBackoff < policy.BaseBackoff {
		return Policy{}, fmt.Errorf("order recovery max backoff must not be below the base backoff")
	}
	if policy.EscalateAfter < time.Minute {
		return Policy{}, fmt.Errorf("order recovery escalation window must be at least one minute")
	}
	return policy, nil
}

// Backoff returns the wait before the next attempt after `attempts` failures.
func (policy Policy) Backoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	backoff := policy.BaseBackoff
	for index := 1; index < attempts; index++ {
		backoff *= 2
		if backoff >= policy.MaxBackoff || backoff <= 0 {
			return policy.MaxBackoff
		}
	}
	if backoff > policy.MaxBackoff {
		return policy.MaxBackoff
	}
	return backoff
}

// SkipReason explains why the guard did not run the work.
type SkipReason string

const (
	SkipReasonHeld             SkipReason = "HELD_BY_ANOTHER_WORKER"
	SkipReasonBackoff          SkipReason = "RETRY_NOT_DUE"
	SkipReasonStaleView        SkipReason = "STALE_ORDER_SNAPSHOT"
	SkipReasonLeaseUnavailable SkipReason = "LEASE_STORE_UNAVAILABLE"
)

// Work performs one recovery attempt. It reports whether the order left every
// recovery status (RESOLVED), is still propagating (WAITING), or failed.
type Work func(ctx context.Context) (domain.OrderRecoveryOutcomeKind, error)

// Outcome describes what the guard did for one order.
type Outcome struct {
	Skipped    bool
	SkipReason SkipReason
	Lease      domain.OrderRecoveryLease
	Kind       domain.OrderRecoveryOutcomeKind
	TimedOut   bool
	Escalated  bool
	// Err is the work error (nil for RESOLVED/WAITING). StoreErr reports a
	// lease store failure; the order is then treated as unresolved.
	Err      error
	StoreErr error
}

// Resolved reports that the work ran and the order left recovery.
func (outcome Outcome) Resolved() bool {
	return !outcome.Skipped && outcome.StoreErr == nil && outcome.Err == nil &&
		outcome.Kind == domain.OrderRecoveryOutcomeResolved
}

// Unresolved is the complement used by reconciliation to exclude the order's
// token from asset comparison and to report it as still in recovery.
func (outcome Outcome) Unresolved() bool { return !outcome.Resolved() }

// GuardParams 表示后端使用的 GuardParams 类型。
type GuardParams struct {
	Store  port.OrderRecoveryLeaseStore
	Policy Policy
	Now    func() time.Time
	Logger *slog.Logger
}

// Guard 表示后端使用的 Guard 类型。
type Guard struct {
	store  port.OrderRecoveryLeaseStore
	policy Policy
	now    func() time.Time
	logger *slog.Logger
}

// NewGuard 校验依赖和策略后创建订单恢复守卫。
func NewGuard(params GuardParams) (*Guard, error) {
	if params.Store == nil {
		return nil, fmt.Errorf("order recovery lease store is required")
	}
	policy, err := params.Policy.Normalize()
	if err != nil {
		return nil, err
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	if params.Logger == nil {
		params.Logger = slog.Default()
	}
	return &Guard{store: params.Store, policy: policy, now: params.Now, logger: params.Logger}, nil
}

// Policy returns the normalized policy in effect.
func (guard *Guard) Policy() Policy { return guard.policy }

// RunParams 收拢单次受保护恢复所需的参数。
type RunParams struct {
	Order  domain.Order
	Holder string
	// BypassBackoff is used by an order-focused immediate trigger.
	BypassBackoff bool
	Work          Work
}

// Run acquires the order lease, executes the work under the per-order timeout,
// and persists the outcome. It never returns without releasing an acquired
// lease, and a store failure is reported rather than bypassing the lease.
func (guard *Guard) Run(ctx context.Context, params RunParams) Outcome {
	holder := strings.TrimSpace(params.Holder)
	if holder == "" || params.Work == nil {
		return Outcome{Skipped: true, SkipReason: SkipReasonLeaseUnavailable, StoreErr: fmt.Errorf("order recovery holder and work are required")}
	}
	now := guard.now().UTC()
	lease, err := guard.store.AcquireOrderRecoveryLease(ctx, domain.OrderRecoveryLeaseRequest{
		OrderID: params.Order.ID, ExecutionAccountID: params.Order.Intent.ExecutionAccountID,
		Holder: holder, OrderRevision: params.Order.Revision, Now: now, TTL: guard.policy.LeaseTTL,
		BypassBackoff: params.BypassBackoff,
	})
	switch {
	case errors.Is(err, port.ErrOrderRecoveryLeaseHeld):
		return Outcome{Skipped: true, SkipReason: SkipReasonHeld, Lease: lease}
	case errors.Is(err, port.ErrOrderRecoveryBackoff):
		return Outcome{Skipped: true, SkipReason: SkipReasonBackoff, Lease: lease}
	case errors.Is(err, port.ErrOrderRecoveryStaleView):
		return Outcome{Skipped: true, SkipReason: SkipReasonStaleView, Lease: lease}
	case err != nil:
		return Outcome{Skipped: true, SkipReason: SkipReasonLeaseUnavailable, StoreErr: err}
	}

	workCtx, cancel := context.WithTimeout(ctx, guard.policy.Timeout)
	kind, workErr := params.Work(workCtx)
	timedOut := workCtx.Err() != nil && ctx.Err() == nil
	cancel()
	if timedOut {
		if workErr == nil {
			workErr = context.DeadlineExceeded
		}
		workErr = fmt.Errorf("order recovery attempt exceeded %s: %w", guard.policy.Timeout, workErr)
	}
	if workErr != nil {
		kind = domain.OrderRecoveryOutcomeFailed
	}
	if ctx.Err() != nil && kind == domain.OrderRecoveryOutcomeFailed {
		// Shutdown is not a venue failure; keep the order retryable at once.
		kind = domain.OrderRecoveryOutcomeWaiting
	}
	if kind == "" {
		kind = domain.OrderRecoveryOutcomeWaiting
	}

	finished := guard.now().UTC()
	release := domain.OrderRecoveryRelease{OrderID: params.Order.ID, Holder: holder, Now: finished, Kind: kind}
	if workErr != nil {
		release.Error = workErr.Error()
	}
	switch kind {
	case domain.OrderRecoveryOutcomeFailed:
		release.NextRetryAt = finished.Add(guard.policy.Backoff(lease.Attempts + 1))
	case domain.OrderRecoveryOutcomeWaiting:
		release.NextRetryAt = finished
	}
	pendingSince := lease.PendingSince(finished)
	if kind != domain.OrderRecoveryOutcomeResolved && lease.FirstPendingAt == nil {
		pendingSince = 0
	}
	if kind != domain.OrderRecoveryOutcomeResolved && (lease.Escalated() || pendingSince >= guard.policy.EscalateAfter) {
		release.Escalate = true
	}
	// Release with the parent context even when the attempt itself timed out.
	releaseCtx := ctx
	if ctx.Err() != nil {
		var releaseCancel context.CancelFunc
		releaseCtx, releaseCancel = context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer releaseCancel()
	}
	released, releaseErr := guard.store.ReleaseOrderRecoveryLease(releaseCtx, release)
	outcome := Outcome{Lease: released, Kind: kind, TimedOut: timedOut, Escalated: released.Escalated(), Err: workErr, StoreErr: releaseErr}
	if releaseErr != nil {
		outcome.Lease = lease
	}
	guard.alert(params.Order, outcome)
	return outcome
}

// alert logs the operator-facing signals: a timed-out attempt and the first
// escalation into the manual queue. Routine retries stay at debug level.
func (guard *Guard) alert(order domain.Order, outcome Outcome) {
	attrs := []any{
		"order_id", order.ID,
		"execution_account_id", order.Intent.ExecutionAccountID,
		"order_status", order.Status,
		"attempts", outcome.Lease.Attempts,
	}
	if outcome.Lease.NextRetryAt != nil {
		attrs = append(attrs, "next_retry_at", outcome.Lease.NextRetryAt.UTC().Format(time.RFC3339))
	}
	if outcome.Err != nil {
		attrs = append(attrs, "error", outcome.Err.Error())
	}
	switch {
	case outcome.StoreErr != nil:
		guard.logger.Error("order recovery lease store failed; order stays unresolved", append(attrs, "store_error", outcome.StoreErr.Error())...)
	case outcome.TimedOut:
		guard.logger.Error("order recovery attempt timed out; reservation stays frozen and the order will retry with backoff", attrs...)
	case outcome.Escalated && outcome.Kind != domain.OrderRecoveryOutcomeResolved:
		guard.logger.Error("order recovery exceeded the escalation window; order is in the manual queue, reservation stays frozen", attrs...)
	case outcome.Kind == domain.OrderRecoveryOutcomeFailed:
		guard.logger.Warn("order recovery attempt failed; retrying with backoff", attrs...)
	}
}
