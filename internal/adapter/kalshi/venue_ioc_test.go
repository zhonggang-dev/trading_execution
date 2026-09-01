package kalshi

import (
	"fmt"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

func TestSubmittedIOCStateUsesRequestedCount(t *testing.T) {
	intent := validKalshiIntent(domain.SideBuy, "YES")
	intent.Size = "10"
	intent.TimeInForce = domain.TimeInForceIOC
	for _, testCase := range []struct {
		name      string
		filled    domain.Decimal
		remaining domain.Decimal
		want      port.VenueOrderState
	}{
		{name: "zero fill", filled: "0", remaining: "0", want: port.VenueOrderCancelled},
		{name: "partial fill", filled: "4", remaining: "0", want: port.VenueOrderCancelled},
		{name: "full fill", filled: "10", remaining: "0", want: port.VenueOrderFilled},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			observed := submittedVenueOrder(SubmittedOrder{
				OrderID: "venue-order", ClientOrderID: intent.ClientOrderID,
				FillCount: testCase.filled, RemainingCount: testCase.remaining,
				TimestampMS: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
			}, intent)
			if observed.State != testCase.want || !observed.FilledSize.Equal(testCase.filled) {
				t.Fatalf("submittedVenueOrder() = %#v", observed)
			}
		})
	}
}

func TestDetailedOrderRejectsInconsistentTerminalCounts(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		status    string
		filled    string
		remaining string
		initial   string
	}{
		{name: "cancelled order retains remainder", status: "canceled", filled: "4", remaining: "6", initial: "10"},
		{name: "cancelled order is fully filled", status: "canceled", filled: "10", remaining: "0", initial: "10"},
		{name: "executed order is not fully filled", status: "executed", filled: "9", remaining: "0", initial: "10"},
		{name: "counts exceed initial", status: "resting", filled: "6", remaining: "5", initial: "10"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			payload := fmt.Sprintf(
				`{"order_id":"o","client_order_id":"c","ticker":"T","status":%q,"fill_count_fp":%q,"remaining_count_fp":%q,"initial_count_fp":%q,"last_update_time":"2026-09-01T00:00:00Z"}`,
				testCase.status, testCase.filled, testCase.remaining, testCase.initial,
			)
			if _, err := decodeDetailedOrder([]byte(payload)); err == nil {
				t.Fatal("inconsistent Kalshi terminal order was accepted")
			}
		})
	}
}

func TestSubmittedOrderValidationFailsClosed(t *testing.T) {
	request := OrderRequestV2{Count: "10.00", TimeInForce: "immediate_or_cancel"}
	for _, testCase := range []struct {
		name     string
		payload  string
		validate bool
	}{
		{name: "missing fill count", payload: `{"order_id":"o","client_order_id":"c","remaining_count":"0","ts_ms":1}`},
		{name: "null remaining count", payload: `{"order_id":"o","client_order_id":"c","fill_count":"0","remaining_count":null,"ts_ms":1}`},
		{name: "negative fill", payload: `{"order_id":"o","client_order_id":"c","fill_count":"-1","remaining_count":"0","ts_ms":1}`, validate: true},
		{name: "overfill", payload: `{"order_id":"o","client_order_id":"c","fill_count":"11","remaining_count":"0","ts_ms":1}`, validate: true},
		{name: "combined counts exceed request", payload: `{"order_id":"o","client_order_id":"c","fill_count":"6","remaining_count":"5","ts_ms":1}`, validate: true},
		{name: "ioc active remainder", payload: `{"order_id":"o","client_order_id":"c","fill_count":"4","remaining_count":"6","ts_ms":1}`, validate: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			order, err := decodeSubmittedOrder([]byte(testCase.payload))
			if err == nil && testCase.validate {
				err = validateSubmittedOrderCounts(order, request)
			}
			if err == nil {
				t.Fatal("malformed Kalshi acknowledgement was accepted")
			}
		})
	}
}

func TestReduceOnlyFOKAcknowledgementMayUseCappedEffectiveCount(t *testing.T) {
	acknowledgement := SubmittedOrder{FillCount: "8", RemainingCount: "0", TimestampMS: 1}
	reduceOnly := OrderRequestV2{Count: "12", TimeInForce: "fill_or_kill", ReduceOnly: true}
	if err := validateSubmittedOrderCounts(acknowledgement, reduceOnly); err != nil {
		t.Fatalf("reduce-only capped FOK acknowledgement error = %v", err)
	}
	nonReduceOnly := reduceOnly
	nonReduceOnly.ReduceOnly = false
	if err := validateSubmittedOrderCounts(acknowledgement, nonReduceOnly); err == nil {
		t.Fatal("non-reduce-only partial FOK acknowledgement was accepted")
	}
}

func TestRemoteOrderIdentityBindsCanonicalRequest(t *testing.T) {
	request := OrderRequestV2{Side: "ask", Count: "12.00", Price: "0.4200", TimeInForce: "immediate_or_cancel", Subaccount: 0}
	cancelOnPause := true
	subaccount := 0
	valid := Order{
		OutcomeSide: "no", BookSide: "ask", Type: "limit", TimeInForce: "immediate_or_cancel",
		Status: "canceled", YesPrice: "0.4200", InitialCount: "12.00", CancelOrderOnPause: &cancelOnPause, SubaccountNumber: &subaccount,
	}
	if err := validateRemoteOrderIdentity(valid, request); err != nil {
		t.Fatalf("validateRemoteOrderIdentity() error = %v", err)
	}
	for _, mutate := range []func(*Order){
		func(order *Order) { order.BookSide = "bid" },
		func(order *Order) { order.Type = "market" },
		func(order *Order) { order.YesPrice = "0.43" },
		func(order *Order) { order.InitialCount = "11" },
		func(order *Order) { order.TimeInForce = "fill_or_kill" },
		func(order *Order) { order.Status = "resting" },
	} {
		candidate := valid
		mutate(&candidate)
		if err := validateRemoteOrderIdentity(candidate, request); err == nil {
			t.Fatalf("mismatched remote order accepted: %#v", candidate)
		}
	}
}

func TestReduceOnlyIOCAllowsCappedInitialCountAndTreatsUnfilledRequestAsCancelled(t *testing.T) {
	request := OrderRequestV2{
		Side: "ask", Count: "12.00", Price: "0.4200", TimeInForce: "immediate_or_cancel", ReduceOnly: true,
	}
	remote := Order{
		OutcomeSide: "no", BookSide: "ask", Type: "limit", TimeInForce: "immediate_or_cancel",
		Status: "executed", YesPrice: "0.4200", FillCount: "8.00", RemainingCount: "0", InitialCount: "8.00",
		LastUpdateTime: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	}
	if err := validateRemoteOrderIdentity(remote, request); err != nil {
		t.Fatalf("validateRemoteOrderIdentity(reduce-only cap) error = %v", err)
	}
	nonReduceOnly := request
	nonReduceOnly.ReduceOnly = false
	if err := validateRemoteOrderIdentity(remote, nonReduceOnly); err == nil {
		t.Fatal("non-reduce-only capped initial count was accepted")
	}

	local := domain.Order{Intent: validKalshiIntent(domain.SideSell, "YES")}
	local.Intent.Size = "12"
	local.Intent.TimeInForce = domain.TimeInForceIOC
	observed := monotonicVenueObservation(remote, local, time.Date(2026, 9, 1, 0, 0, 1, 0, time.UTC))
	if observed.State != port.VenueOrderCancelled || observed.RawStatus != "terminal_remainder_cancelled" ||
		!observed.FilledSize.Equal("8") {
		t.Fatalf("reduce-only capped IOC observation = %#v", observed)
	}
}
