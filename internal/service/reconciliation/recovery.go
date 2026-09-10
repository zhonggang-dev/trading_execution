package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/orderrecovery"
)

// defaultRecoveryPendingGrace is how long an order may sit in a WAITING
// recovery state (fill evidence still propagating, no failure) before the run
// reports it as ORDER_RECOVERY_PENDING. Inside the grace the token is still
// excluded from asset comparison, silently: the chain may have moved.
const defaultRecoveryPendingGrace = 2 * time.Minute

// orderRecoveryOutcome pairs one order with what the guard did for it.
type orderRecoveryOutcome struct {
	order   domain.Order
	outcome orderrecovery.Outcome
	// evidencePending is a WAITING outcome caused by missing CLOB fill details.
	evidencePending bool
	// guarded is false for orders outside the recovery statuses; those run the
	// ordinary evidence steps and are never treated as unresolved.
	guarded bool
}

// unresolvedOrders is the per-run view of orders whose venue outcome is still
// uncertain after this run's recovery attempts. Their reservations are frozen,
// so any on-chain cash or share movement they caused is bounded by what was
// reserved. Asset comparison excludes their tokens and explains a bounded
// balance drift by them instead of recording manual drift for the account.
type unresolvedOrders struct {
	orders     []domain.Order
	tokens     map[string]struct{}
	cashLower  *big.Rat
	cashUpper  *big.Rat
	boundKnown bool
}

func newUnresolvedOrders() unresolvedOrders {
	return unresolvedOrders{tokens: make(map[string]struct{}), cashLower: new(big.Rat), cashUpper: new(big.Rat), boundKnown: true}
}

// recoveryHolder is the lease holder identity of this run.
func (state *runState) recoveryHolder() string {
	return "reconciliation:" + state.run.RunID
}

// recordRecoveryOutcome keeps the guard result for classification after the
// finality view is loaded, and counts it in the run summary immediately.
func (state *runState) recordRecoveryOutcome(order domain.Order, outcome orderrecovery.Outcome, evidencePending, guarded bool) {
	state.recovery.outcomes = append(state.recovery.outcomes, orderRecoveryOutcome{order: order, outcome: outcome, evidencePending: evidencePending, guarded: guarded})
	if !guarded {
		return
	}
	switch {
	case outcome.Skipped && outcome.StoreErr != nil:
		state.run.Summary["orders_recovery_lease_unavailable"]++
	case outcome.Skipped:
		state.run.Summary["orders_recovery_deferred"]++
	case outcome.TimedOut:
		state.run.Summary["orders_recovery_timed_out"]++
	case outcome.Resolved():
		state.run.Summary["orders_recovery_resolved"]++
	case outcome.Kind == domain.OrderRecoveryOutcomeWaiting:
		state.run.Summary["orders_recovery_waiting"]++
	default:
		state.run.Summary["orders_recovery_failed"]++
	}
	if outcome.Escalated {
		state.run.Summary["orders_recovery_escalated"]++
	}
}

// classifyUnresolvedOrders turns the recovery outcomes into the unresolved
// view and records the order-scoped issues. It must run after the finality
// view is loaded: a WAITING order whose fills are already MINED is explained
// by that view and is not unresolved.
func (state *runState) classifyUnresolvedOrders(ctx context.Context) {
	state.recovery.unresolved = newUnresolvedOrders()
	finalityOrders := make(map[string]struct{}, len(state.finality.fills))
	for _, fill := range state.finality.fills {
		finalityOrders[fill.OrderID] = struct{}{}
	}
	for _, entry := range state.recovery.outcomes {
		if !entry.guarded || entry.outcome.Resolved() {
			continue
		}
		if entry.evidencePending && entry.outcome.Err == nil {
			if _, explained := finalityOrders[entry.order.ID]; explained {
				state.run.Summary["orders_recovery_explained_by_finality_pending_fills"]++
				continue
			}
		}
		state.markUnresolved(ctx, entry)
	}
	state.run.Summary["orders_unresolved"] = len(state.recovery.unresolved.orders)
}

// markUnresolved adds the order to the unresolved view and records the issue
// that keeps its token gated. Silent propagation inside the grace window only
// excludes the token; anything older, deferred, failed, or escalated is
// reported so a later clean run cannot auto-resolve the order's open issues
// while it is still uncertain.
func (state *runState) markUnresolved(ctx context.Context, entry orderRecoveryOutcome) {
	view := &state.recovery.unresolved
	view.orders = append(view.orders, entry.order)
	if token := strings.TrimSpace(entry.order.Intent.TokenID); token != "" {
		view.tokens[token] = struct{}{}
	}
	state.addUnresolvedCashBound(ctx, entry.order)

	lease := entry.outcome.Lease
	if lease.Escalated() {
		state.recordRecoveryStalled(ctx, entry)
	}
	switch {
	case entry.outcome.Skipped:
		state.recordRecoveryPending(ctx, entry, "recovery is "+strings.ToLower(strings.ReplaceAll(string(entry.outcome.SkipReason), "_", " ")))
	case entry.outcome.Err != nil:
		// The failing step already recorded an order-scoped SOURCE_UNAVAILABLE
		// issue; a second issue for the same run adds nothing.
		if entry.outcome.StoreErr != nil {
			state.recordRecoveryPending(ctx, entry, "order recovery lease store failed: "+entry.outcome.StoreErr.Error())
		}
	default:
		if lease.PendingSince(state.now) < state.service.recoveryPendingGrace {
			state.run.Summary["orders_recovery_propagating"]++
			return
		}
		state.recordRecoveryPending(ctx, entry, "the venue outcome is still propagating")
	}
}

func (state *runState) recordRecoveryPending(ctx context.Context, entry orderRecoveryOutcome, reason string) {
	lease := entry.outcome.Lease
	details := fmt.Sprintf("%s; attempts=%d", reason, lease.Attempts)
	if lease.NextRetryAt != nil {
		details += "; next_retry_at=" + lease.NextRetryAt.UTC().Format(time.RFC3339)
	}
	if lease.FirstPendingAt != nil {
		details += "; pending_for=" + lease.PendingSince(state.now).Truncate(time.Second).String()
	}
	if strings.TrimSpace(lease.LastError) != "" {
		details += "; last_error=" + lease.LastError
	}
	if entry.outcome.StoreErr != nil && !entry.outcome.Skipped {
		details += "; lease_store_error=" + entry.outcome.StoreErr.Error()
	}
	details += "; the reservation stays frozen and only this order's market is gated"
	state.issue(ctx, domain.ReconciliationIssueParams{
		Type: domain.ReconciliationIssueOrderRecoveryPending, Resolution: domain.ReconciliationResolutionRetry,
		Status: domain.ReconciliationIssueOpen, OrderID: entry.order.ID, VenueOrderID: entry.order.VenueOrderID,
		MarketID: entry.order.Intent.MarketID, ConditionID: entry.order.Intent.ConditionID,
		TokenID: entry.order.Intent.TokenID, Source: "ORDER_RECOVERY", Details: details,
	})
}

func (state *runState) recordRecoveryStalled(ctx context.Context, entry orderRecoveryOutcome) {
	lease := entry.outcome.Lease
	details := fmt.Sprintf(
		"order recovery has been pending for %s (attempts=%d, escalated_at=%s) and is in the manual queue; "+
			"automatic retries continue with capped backoff, the reservation is never released automatically",
		lease.PendingSince(state.now).Truncate(time.Second), lease.Attempts, lease.EscalatedAt.UTC().Format(time.RFC3339),
	)
	if strings.TrimSpace(lease.LastError) != "" {
		details += "; last_error=" + lease.LastError
	}
	state.issue(ctx, domain.ReconciliationIssueParams{
		Type: domain.ReconciliationIssueOrderRecoveryStalled, Resolution: domain.ReconciliationResolutionManual,
		Status: domain.ReconciliationIssueOpen, OrderID: entry.order.ID, VenueOrderID: entry.order.VenueOrderID,
		MarketID: entry.order.Intent.MarketID, ConditionID: entry.order.Intent.ConditionID,
		TokenID: entry.order.Intent.TokenID, Source: "ORDER_RECOVERY", Details: details,
	})
}

// addUnresolvedCashBound widens the interval of on-chain cash movement the
// unresolved order could have caused: a BUY can only have spent its frozen
// reservation, a SELL can only have received at most one collateral unit per
// reserved share. The exact reservation is preferred; the intent is the
// fallback. An order whose bound cannot be established makes the whole bound
// unknown, so a drift is then reported the conservative way.
func (state *runState) addUnresolvedCashBound(ctx context.Context, order domain.Order) {
	view := &state.recovery.unresolved
	if !view.boundKnown {
		return
	}
	spend, receive, err := state.unresolvedOrderCashExposure(ctx, order)
	if err != nil {
		view.boundKnown = false
		state.run.Summary["orders_unresolved_bound_unknown"]++
		return
	}
	view.cashLower.Sub(view.cashLower, spend)
	view.cashUpper.Add(view.cashUpper, receive)
}

func (state *runState) unresolvedOrderCashExposure(ctx context.Context, order domain.Order) (spend, receive *big.Rat, err error) {
	spend, receive = new(big.Rat), new(big.Rat)
	if state.service.reservations != nil {
		reservation, readErr := state.service.reservations.GetOrderReservation(ctx, order.ID)
		switch {
		case readErr == nil:
			if order.Intent.Side == domain.SideSell {
				receive, err = ratOf(reservation.RemainingReservedShares)
			} else {
				spend, err = ratOf(reservation.RemainingReservedBalance)
			}
			return spend, receive, err
		case !errors.Is(readErr, port.ErrReservationNotFound):
			return nil, nil, readErr
		}
	}
	if order.Intent.Side == domain.SideSell {
		receive, err = ratOf(order.Intent.Size)
		return spend, receive, err
	}
	price := order.Intent.WorstPrice
	if price.IsEmpty() {
		price = order.Intent.Price
	}
	if price.IsEmpty() {
		return nil, nil, fmt.Errorf("order %s has no price to bound its BUY exposure", order.ID)
	}
	product, err := order.Intent.Size.Multiply(price)
	if err != nil {
		return nil, nil, err
	}
	return product, receive, nil
}

func ratOf(value domain.Decimal) (*big.Rat, error) {
	text := strings.TrimSpace(value.String())
	if text == "" {
		return new(big.Rat), nil
	}
	rat, ok := new(big.Rat).SetString(text)
	if !ok || strings.ContainsAny(text, "/eE") {
		return nil, fmt.Errorf("invalid decimal %q", text)
	}
	return rat, nil
}

// tokenUnresolved reports whether asset comparison must skip the token because
// an order on it is still being recovered.
func (state *runState) tokenUnresolved(tokenID string) bool {
	_, unresolved := state.recovery.unresolved.tokens[strings.TrimSpace(tokenID)]
	return unresolved
}

// balanceExplainedByUnresolvedOrders reports whether the remaining balance
// drift (after finality-pending fills) lies inside the interval the unresolved
// orders' frozen reservations allow. It records the drift as RETRY_LATER with
// the attribution, and forces the pending issues of the attributed orders so
// their tokens stay gated until the orders resolve.
func (state *runState) balanceExplainedByUnresolvedOrders(ctx context.Context, local, external domain.Decimal) bool {
	view := state.recovery.unresolved
	if len(view.orders) == 0 || !view.boundKnown {
		return false
	}
	projected := local
	if len(state.finality.fills) > 0 {
		var err error
		if projected, err = addDecimals(local, state.finality.cashDelta); err != nil {
			return false
		}
	}
	projectedRat, err := ratOf(projected)
	if err != nil {
		return false
	}
	externalRat, err := ratOf(external)
	if err != nil {
		return false
	}
	epsilon, err := ratOf(state.service.balanceEpsilon)
	if err != nil {
		return false
	}
	drift := new(big.Rat).Sub(externalRat, projectedRat)
	lower := new(big.Rat).Sub(view.cashLower, epsilon)
	upper := new(big.Rat).Add(view.cashUpper, epsilon)
	if drift.Cmp(lower) < 0 || drift.Cmp(upper) > 0 {
		return false
	}
	state.run.Summary["balance_explained_by_unresolved_orders"]++
	orderIDs := make([]string, 0, len(view.orders))
	for _, order := range view.orders {
		orderIDs = append(orderIDs, order.ID)
	}
	sort.Strings(orderIDs)
	state.issue(ctx, domain.ReconciliationIssueParams{
		Type: domain.ReconciliationIssueBalanceDrift, Resolution: domain.ReconciliationResolutionRetry,
		Status: domain.ReconciliationIssueOpen, LocalValue: local, RemoteValue: external, Source: "ORDER_RECOVERY",
		Details: fmt.Sprintf(
			"on-chain balance differs from the ledger by an amount inside the frozen reservations of unresolved orders [%s]; "+
				"the drift will be attributed by exact fills once the orders recover, nothing is overwritten",
			strings.Join(orderIDs, ","),
		),
	})
	// A drift that is already visible ends the silent propagation grace.
	for _, entry := range state.recovery.outcomes {
		if !entry.guarded || entry.outcome.Resolved() || state.hasIssueForOrder(entry.order.ID) || !state.isUnresolved(entry.order.ID) {
			continue
		}
		state.recordRecoveryPending(ctx, entry, "a bounded balance drift is currently attributed to this order")
	}
	return true
}

func (state *runState) isUnresolved(orderID string) bool {
	for _, order := range state.recovery.unresolved.orders {
		if order.ID == orderID {
			return true
		}
	}
	return false
}

func (state *runState) hasIssueForOrder(orderID string) bool {
	for _, issue := range state.issues {
		if issue.OrderID == orderID && issue.Status == domain.ReconciliationIssueOpen {
			return true
		}
	}
	return false
}
