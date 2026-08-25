package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type placementCheckerFunc func(context.Context, string) error

func (checker placementCheckerFunc) CheckAccount(ctx context.Context, accountID string) error {
	return checker(ctx, accountID)
}

type placementTestVenue struct {
	places  int
	cancels int
	gets    int
}

func (*placementTestVenue) Name() string { return "polymarket" }

func (venue *placementTestVenue) Place(context.Context, domain.Order) (port.VenueOrder, error) {
	venue.places++
	return port.VenueOrder{ID: "placed"}, nil
}

func (venue *placementTestVenue) Cancel(context.Context, domain.Order) (port.VenueOrder, error) {
	venue.cancels++
	return port.VenueOrder{ID: "cancelled"}, nil
}

func (venue *placementTestVenue) Get(context.Context, domain.Order) (port.VenueOrder, error) {
	venue.gets++
	return port.VenueOrder{ID: "read"}, nil
}

func TestPlacementReadinessVenueGatesOnlyPlace(t *testing.T) {
	underlying := &placementTestVenue{}
	venue, err := newPlacementReadinessVenue(underlying)
	if err != nil {
		t.Fatal(err)
	}

	order := placementTestOrder("wallet-7")
	placed, placeErr := venue.Place(context.Background(), order)
	assertReconciliationGate(t, placed, placeErr)
	if underlying.places != 0 {
		t.Fatalf("unbound gate delegated %d placements, want 0", underlying.places)
	}

	readinessErr := errors.New("latest reconciliation requires attention")
	if err := venue.Bind(placementCheckerFunc(func(_ context.Context, accountID string) error {
		if accountID != "wallet-7" {
			t.Fatalf("placement readiness account = %q, want wallet-7", accountID)
		}
		return readinessErr
	})); err != nil {
		t.Fatal(err)
	}
	placed, placeErr = venue.Place(context.Background(), order)
	assertReconciliationGate(t, placed, placeErr)
	if !strings.Contains(placeErr.Error(), readinessErr.Error()) {
		t.Fatalf("Place() error = %v, want account-local readiness cause", placeErr)
	}
	if underlying.places != 0 {
		t.Fatalf("unhealthy gate delegated %d placements, want 0", underlying.places)
	}

	if _, err := venue.Cancel(context.Background(), domain.Order{}); err != nil {
		t.Fatalf("Cancel() = %v, must bypass placement readiness", err)
	}
	if _, err := venue.Get(context.Background(), domain.Order{}); err != nil {
		t.Fatalf("Get() = %v, must bypass placement readiness", err)
	}
	if underlying.cancels != 1 || underlying.gets != 1 {
		t.Fatalf("bypass calls = cancel:%d get:%d, want 1 each", underlying.cancels, underlying.gets)
	}
}

func TestPlacementReadinessVenueAllowsHealthyPlace(t *testing.T) {
	underlying := &placementTestVenue{}
	venue, err := newPlacementReadinessVenue(underlying)
	if err != nil {
		t.Fatal(err)
	}
	if err := venue.Bind(placementCheckerFunc(func(context.Context, string) error { return nil })); err != nil {
		t.Fatal(err)
	}
	placed, err := venue.Place(context.Background(), placementTestOrder("wallet-7"))
	if err != nil || placed.ID != "placed" || underlying.places != 1 {
		t.Fatalf("Place() = %#v, %v, calls=%d", placed, err, underlying.places)
	}
}

func placementTestOrder(accountID string) domain.Order {
	return domain.Order{Intent: domain.OrderIntent{ExecutionAccountID: accountID}}
}

func assertReconciliationGate(t *testing.T, _ port.VenueOrder, err error) {
	t.Helper()
	var venueErr *port.VenueError
	if !errors.As(err, &venueErr) || venueErr.Code != "RECONCILIATION_NOT_READY" {
		t.Fatalf("Place() error = %v, want RECONCILIATION_NOT_READY", err)
	}
}
