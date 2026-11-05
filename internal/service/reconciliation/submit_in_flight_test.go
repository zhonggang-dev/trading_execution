package reconciliation

import (
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/orderrecovery"
)

// TestInFlightSubmitIsNotASourceFailure: a SUBMITTING order whose placement is
// still running is a wait. No SOURCE_UNAVAILABLE is recorded, its token stays
// excluded from the asset comparison, and its lease records no failure.
func TestInFlightSubmitIsNotASourceFailure(t *testing.T) {
	fixture := newRecoveryFixture()
	submitting := unknownBuyOrder()
	submitting.Status = domain.OrderStatusSubmitting
	fixture.orders.orders = []domain.Order{submitting}
	fixture.refresher.orders[submitting.ID] = submitting
	fixture.refresher.errors = map[string]error{submitting.ID: port.ErrSubmitInFlight}
	fixture.balance = "98.5"
	service := fixture.service(t, orderrecovery.Policy{})

	result := runScheduled(t, service)
	assertNoIssue(t, result.Issues, domain.ReconciliationIssueSourceUnavailable)
	assertNoIssue(t, result.Issues, domain.ReconciliationIssuePositionDrift)
	if result.Run.Summary["orders_submit_in_flight"] != 1 || result.Run.Summary["orders_unresolved"] != 1 {
		t.Fatalf("summary = %#v; want one in-flight order kept unresolved", result.Run.Summary)
	}
	lease, exists := fixture.leases.Lease(submitting.ID)
	if !exists || lease.Attempts != 0 || lease.LastError != "" {
		t.Fatalf("lease = %#v exists=%v; want a WAITING lease without failure", lease, exists)
	}
}

// TestNoFillReleasedOrderIsNotReportedAsVenueCancel: a no-fill release is not
// a venue-confirmed cancel; no LOCAL_ORDER_CANCELLED issue is written.
func TestNoFillReleasedOrderIsNotReportedAsVenueCancel(t *testing.T) {
	fixture := newRecoveryFixture()
	unknown := unknownBuyOrder()
	released := unknown
	released.Status = domain.OrderStatusCancelled
	released.FailureCode = domain.NoFillReleaseAutoReasonCode
	fixture.refresher.orders[unknown.ID] = released
	service := fixture.service(t, orderrecovery.Policy{})

	result := runScheduled(t, service)
	assertNoIssue(t, result.Issues, domain.ReconciliationIssueLocalOrderCancelled)
	if result.Run.Summary["orders_no_fill_released"] != 1 {
		t.Fatalf("summary = %#v", result.Run.Summary)
	}
}
