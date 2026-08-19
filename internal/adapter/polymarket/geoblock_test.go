package polymarket

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

func TestGeoblockClientValidatesOfficialResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/api/geoblock" {
			t.Fatalf("request = %s %s", request.Method, request.URL.Path)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"blocked":false,"ip":"203.0.113.9","country":"jp","region":"13"}`))
	}))
	defer server.Close()
	client, err := NewGeoblockClient(GeoblockClientParams{URL: server.URL + "/api/geoblock"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Check(context.Background(), "account-1")
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if result.Blocked || result.IP != "203.0.113.9" || result.Country != "JP" || result.Region != "13" {
		t.Fatalf("Check() = %#v", result)
	}
}

func TestGeoblockClientFailsClosedOnMalformedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"ip":"not-an-ip","country":"JP"}`))
	}))
	defer server.Close()
	client, err := NewGeoblockClient(GeoblockClientParams{URL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Check(context.Background(), "account-1"); err == nil {
		t.Fatal("Check() error = nil, want malformed response rejection")
	}
}

func TestEligibilityVenueBlocksOnlyPlace(t *testing.T) {
	underlying := &eligibilityTestVenue{}
	wrapped, err := NewEligibilityVenue(underlying, eligibilityTestChecker{
		result: GeographicEligibility{Blocked: true, IP: "203.0.113.9", Country: "JP", Region: "13"},
	})
	if err != nil {
		t.Fatal(err)
	}
	order := domain.Order{ID: "order-1"}
	_, placeErr := wrapped.Place(context.Background(), order)
	var venueError *port.VenueError
	if !errors.As(placeErr, &venueError) || venueError.Kind != port.VenueErrorRejected || venueError.Code != "POLYMARKET_GEO_BLOCKED" {
		t.Fatalf("Place() error = %#v", placeErr)
	}
	if underlying.placeCalls != 0 {
		t.Fatalf("underlying Place calls = %d, want 0", underlying.placeCalls)
	}
	if _, err := wrapped.Cancel(context.Background(), order); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if _, err := wrapped.Get(context.Background(), order); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if underlying.cancelCalls != 1 || underlying.getCalls != 1 {
		t.Fatalf("cancel/get calls = %d/%d, want 1/1", underlying.cancelCalls, underlying.getCalls)
	}
}

func TestEligibilityVenueRejectsWhenCheckUnavailable(t *testing.T) {
	underlying := &eligibilityTestVenue{}
	wrapped, err := NewEligibilityVenue(underlying, eligibilityTestChecker{err: context.DeadlineExceeded})
	if err != nil {
		t.Fatal(err)
	}
	_, placeErr := wrapped.Place(context.Background(), domain.Order{ID: "order-1"})
	var venueError *port.VenueError
	if !errors.As(placeErr, &venueError) || venueError.Kind != port.VenueErrorRejected || venueError.Code != "POLYMARKET_GEO_CHECK_UNAVAILABLE" {
		t.Fatalf("Place() error = %#v", placeErr)
	}
	if underlying.placeCalls != 0 {
		t.Fatalf("underlying Place calls = %d, want 0", underlying.placeCalls)
	}
}

func TestEligibilityVenueAllowsEligiblePlace(t *testing.T) {
	underlying := &eligibilityTestVenue{}
	wrapped, err := NewEligibilityVenue(underlying, eligibilityTestChecker{
		result: GeographicEligibility{IP: "203.0.113.9", Country: "IE"},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := wrapped.Place(context.Background(), domain.Order{ID: "order-1"})
	if err != nil {
		t.Fatalf("Place() error = %v", err)
	}
	if result.ID != "venue-order" || underlying.placeCalls != 1 {
		t.Fatalf("Place() = %#v, calls = %d", result, underlying.placeCalls)
	}
}

type eligibilityTestChecker struct {
	result GeographicEligibility
	err    error
}

func (checker eligibilityTestChecker) Check(context.Context, string) (GeographicEligibility, error) {
	return checker.result, checker.err
}

func TestCLOBEligibilityCheckerAllowsJPOnlyWhenAccountIsNotClosedOnly(t *testing.T) {
	checker, err := NewCLOBEligibilityChecker(CLOBEligibilityCheckerParams{
		Geoblock: eligibilityTestChecker{result: GeographicEligibility{
			Blocked: true, IP: "203.0.113.9", Country: "JP", Region: "13",
		}},
		ClosedOnly:         eligibilityClosedOnlyChecker(false),
		APIExemptCountries: []string{"JP"},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := checker.Check(context.Background(), "account-1")
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if result.Blocked || result.Reason != "FRONTEND_ONLY_COUNTRY_API_ELIGIBLE" {
		t.Fatalf("Check() = %#v", result)
	}

	closedChecker, err := NewCLOBEligibilityChecker(CLOBEligibilityCheckerParams{
		Geoblock:           eligibilityTestChecker{result: GeographicEligibility{Blocked: true, IP: "203.0.113.9", Country: "JP"}},
		ClosedOnly:         eligibilityClosedOnlyChecker(true),
		APIExemptCountries: []string{"JP"},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err = closedChecker.Check(context.Background(), "account-1")
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if !result.Blocked || result.Reason != "CLOB_ACCOUNT_CLOSED_ONLY" {
		t.Fatalf("Check() = %#v", result)
	}
}

type eligibilityClosedOnlyChecker bool

func (checker eligibilityClosedOnlyChecker) ClosedOnly(context.Context, string) (bool, error) {
	return bool(checker), nil
}

type eligibilityTestVenue struct {
	placeCalls  int
	cancelCalls int
	getCalls    int
}

func (venue *eligibilityTestVenue) Name() string { return "polymarket" }

func (venue *eligibilityTestVenue) Place(context.Context, domain.Order) (port.VenueOrder, error) {
	venue.placeCalls++
	return port.VenueOrder{ID: "venue-order", State: port.VenueOrderLive, FilledSize: "0", ObservedAt: time.Now().UTC()}, nil
}

func (venue *eligibilityTestVenue) Cancel(context.Context, domain.Order) (port.VenueOrder, error) {
	venue.cancelCalls++
	return port.VenueOrder{ID: "venue-order", State: port.VenueOrderCancelled, FilledSize: "0", ObservedAt: time.Now().UTC()}, nil
}

func (venue *eligibilityTestVenue) Get(context.Context, domain.Order) (port.VenueOrder, error) {
	venue.getCalls++
	return port.VenueOrder{ID: "venue-order", State: port.VenueOrderLive, FilledSize: "0", ObservedAt: time.Now().UTC()}, nil
}
