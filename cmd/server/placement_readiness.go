package main

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// placementReadinessVenue closes the construction cycle between execution and
// reconciliation. It fails Place while unbound or unhealthy, but deliberately
// never gates Cancel/Get so operators and the reconciler can still reduce risk.
type placementReadinessVenue struct {
	venue port.Venue

	mu      sync.RWMutex
	checker placementAccountReadinessChecker
}

type placementAccountReadinessChecker interface {
	CheckAccount(context.Context, string) error
}

type placementAccountReadiness struct {
	reconciliation placementAccountReadinessChecker
	global         readinessChecker
}

func newPlacementAccountReadiness(
	reconciliation placementAccountReadinessChecker,
	global readinessChecker,
) (*placementAccountReadiness, error) {
	if reconciliation == nil {
		return nil, fmt.Errorf("account reconciliation readiness is required")
	}
	return &placementAccountReadiness{reconciliation: reconciliation, global: global}, nil
}

func (checker *placementAccountReadiness) CheckAccount(ctx context.Context, accountID string) error {
	if err := checker.reconciliation.CheckAccount(ctx, accountID); err != nil {
		return fmt.Errorf("reconciliation: %w", err)
	}
	if checker.global != nil {
		if err := checker.global.Check(ctx); err != nil {
			return fmt.Errorf("account authorization: %w", err)
		}
	}
	return nil
}

func newPlacementReadinessVenue(venue port.Venue) (*placementReadinessVenue, error) {
	if venue == nil || strings.TrimSpace(venue.Name()) == "" {
		return nil, fmt.Errorf("placement readiness requires a named venue")
	}
	return &placementReadinessVenue{venue: venue}, nil
}

func (venue *placementReadinessVenue) Bind(checker placementAccountReadinessChecker) error {
	if checker == nil {
		return fmt.Errorf("placement readiness checker is required")
	}
	venue.mu.Lock()
	defer venue.mu.Unlock()
	if venue.checker != nil {
		return fmt.Errorf("placement readiness checker is already bound")
	}
	venue.checker = checker
	return nil
}

func (venue *placementReadinessVenue) Name() string { return venue.venue.Name() }

func (venue *placementReadinessVenue) Place(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	if err := venue.checkPlace(ctx, order.Intent.ExecutionAccountID); err != nil {
		return port.VenueOrder{}, err
	}
	return venue.venue.Place(ctx, order)
}

func (venue *placementReadinessVenue) checkPlace(ctx context.Context, accountID string) error {
	venue.mu.RLock()
	checker := venue.checker
	venue.mu.RUnlock()
	if checker == nil {
		return reconciliationNotReadyError(fmt.Errorf("reconciliation readiness is not bound"))
	}
	if err := checker.CheckAccount(ctx, strings.TrimSpace(accountID)); err != nil {
		return reconciliationNotReadyError(err)
	}
	return nil
}

type placementReadinessPrepared struct{ inner port.PreparedPlacement }

func (prepared placementReadinessPrepared) ExpectedVenueOrderID() string {
	if prepared.inner == nil {
		return ""
	}
	return prepared.inner.ExpectedVenueOrderID()
}

func (venue *placementReadinessVenue) PreparePlace(ctx context.Context, order domain.Order) (port.PreparedPlacement, error) {
	if err := venue.checkPlace(ctx, order.Intent.ExecutionAccountID); err != nil {
		return nil, err
	}
	underlying, ok := venue.venue.(port.PreparedVenue)
	if !ok {
		return nil, reconciliationNotReadyError(fmt.Errorf("underlying live venue does not support prepared placement"))
	}
	prepared, err := underlying.PreparePlace(ctx, order)
	if err != nil {
		return nil, err
	}
	return placementReadinessPrepared{inner: prepared}, nil
}

func (venue *placementReadinessVenue) PlacePrepared(ctx context.Context, order domain.Order, placement port.PreparedPlacement) (port.VenueOrder, error) {
	prepared, ok := placement.(placementReadinessPrepared)
	underlying, supported := venue.venue.(port.PreparedVenue)
	if !ok || !supported || prepared.inner == nil {
		return port.VenueOrder{}, reconciliationNotReadyError(fmt.Errorf("placement readiness prepared placement is invalid"))
	}
	if err := venue.checkPlace(ctx, order.Intent.ExecutionAccountID); err != nil {
		return port.VenueOrder{}, err
	}
	return underlying.PlacePrepared(ctx, order, prepared.inner)
}

func (venue *placementReadinessVenue) Cancel(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	return venue.venue.Cancel(ctx, order)
}

func (venue *placementReadinessVenue) Get(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	return venue.venue.Get(ctx, order)
}

func reconciliationNotReadyError(cause error) error {
	message := "new order rejected locally because account reconciliation is not healthy"
	if cause != nil {
		// The cause contains only server-owned account/status/timing diagnostics.
		// Preserve it in the durable rejection reason so an account-local gate
		// can be distinguished from an unrelated global-readiness failure.
		message += ": " + cause.Error()
	}
	return &port.VenueError{
		Kind:    port.VenueErrorRejected,
		Code:    "RECONCILIATION_NOT_READY",
		Message: message,
		Cause:   cause,
	}
}

var _ port.Venue = (*placementReadinessVenue)(nil)
var _ port.PreparedVenue = (*placementReadinessVenue)(nil)
