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
	checker readinessChecker
}

func newPlacementReadinessVenue(venue port.Venue) (*placementReadinessVenue, error) {
	if venue == nil || strings.TrimSpace(venue.Name()) == "" {
		return nil, fmt.Errorf("placement readiness requires a named venue")
	}
	return &placementReadinessVenue{venue: venue}, nil
}

func (venue *placementReadinessVenue) Bind(checker readinessChecker) error {
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
	venue.mu.RLock()
	checker := venue.checker
	venue.mu.RUnlock()
	if checker == nil {
		return port.VenueOrder{}, reconciliationNotReadyError(fmt.Errorf("reconciliation readiness is not bound"))
	}
	if err := checker.Check(ctx); err != nil {
		return port.VenueOrder{}, reconciliationNotReadyError(err)
	}
	return venue.venue.Place(ctx, order)
}

func (venue *placementReadinessVenue) Cancel(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	return venue.venue.Cancel(ctx, order)
}

func (venue *placementReadinessVenue) Get(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	return venue.venue.Get(ctx, order)
}

func reconciliationNotReadyError(cause error) error {
	return &port.VenueError{
		Kind:    port.VenueErrorRejected,
		Code:    "RECONCILIATION_NOT_READY",
		Message: "new order rejected locally because account reconciliation is not healthy",
		Cause:   cause,
	}
}

var _ port.Venue = (*placementReadinessVenue)(nil)
