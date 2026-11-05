package domain

import "testing"

// TestNoFillHistoryRejectsVenueSuccessSubmit: a SUBMIT that ended after the
// venue answered with a success response (accepted, unusable body or another
// order id) is never an unaccepted order, whatever the later reads show.
func TestNoFillHistoryRejectsVenueSuccessSubmit(t *testing.T) {
	statuses := []OrderStatus{OrderStatusReserved, OrderStatusSubmitting, OrderStatusUnknown}
	submit := func(code string, httpStatus int) []OrderAttempt {
		return []OrderAttempt{{
			ID: "attempt-1", Kind: OrderAttemptSubmit, Outcome: AttemptOutcomeUnknown,
			ErrorCode: code, HTTPStatus: httpStatus,
		}}
	}
	for _, code := range []string{"CLOB_SERVER_ERROR", "CLOB_TRANSPORT_OUTCOME_UNKNOWN", ""} {
		if err := NoFillHistoryError(submit(code, 0), statuses); err != nil {
			t.Fatalf("ambiguous submit %q rejected: %v", code, err)
		}
	}
	if err := NoFillHistoryError(submit("CLOB_SERVER_ERROR", 500), statuses); err != nil {
		t.Fatalf("5xx submit rejected: %v", err)
	}
	for _, code := range []string{"CLOB_ORDER_ID_MISMATCH", "CLOB_ORDER_ID_MISSING", "CLOB_INVALID_POST_RESPONSE", "INVALID_VENUE_RESPONSE"} {
		if err := NoFillHistoryError(submit(code, 0), statuses); err == nil {
			t.Fatalf("venue-success submit %q was admitted", code)
		}
	}
	if err := NoFillHistoryError(submit("VENUE_PLACE_OUTCOME_UNKNOWN", 200), statuses); err == nil {
		t.Fatal("2xx submit was admitted")
	}
}
