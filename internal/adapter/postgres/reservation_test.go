package postgres

import (
	"errors"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// TestValidateReservationOrderRequiresWorstPriceForBuy 验证 Validate Reservation Order Requires Worst Price For Buy 场景下的行为。
func TestValidateReservationOrderRequiresWorstPriceForBuy(t *testing.T) {
	order := reservationOrder(domain.SideBuy)
	order.Intent.WorstPrice = ""
	if _, _, err := validateReservationOrder(order); err == nil {
		t.Fatal("validateReservationOrder() error = nil, want missing worst_price rejection")
	}
	order.Intent.WorstPrice = "0.53"
	if _, price, err := validateReservationOrder(order); err != nil || price != "0.53" {
		t.Fatalf("validateReservationOrder() = %q, %v", price, err)
	}
}

// TestIntentFingerprintIsStableAndCoversOrderSemantics 验证 Intent Fingerprint Is Stable And Covers Order Semantics 场景下的行为。
func TestIntentFingerprintIsStableAndCoversOrderSemantics(t *testing.T) {
	first := reservationOrder(domain.SideBuy).Intent
	first.Metadata = map[string]string{"b": "2", "a": "1"}
	second := first
	second.Metadata = map[string]string{"a": "1", "b": "2"}
	left, err := intentFingerprint(first)
	if err != nil {
		t.Fatal(err)
	}
	right, err := intentFingerprint(second)
	if err != nil {
		t.Fatal(err)
	}
	if left != right {
		t.Fatalf("map order changed fingerprint: %q != %q", left, right)
	}
	second.Size = "11"
	changed, err := intentFingerprint(second)
	if err != nil {
		t.Fatal(err)
	}
	if changed == left {
		t.Fatal("changed size did not change fingerprint")
	}
}

// TestReservationStatusMapping 验证 Reservation Status Mapping 场景下的行为。
func TestReservationStatusMapping(t *testing.T) {
	for status, want := range map[domain.OrderStatus]domain.ReservationStatus{
		domain.OrderStatusOpen:     domain.ReservationStatusActive,
		domain.OrderStatusCanceled: domain.ReservationStatusReleased,
		domain.OrderStatusRejected: domain.ReservationStatusReleased,
		domain.OrderStatusFilled:   domain.ReservationStatusSettled,
	} {
		if got := nextReservationStatus(status); got != want {
			t.Fatalf("nextReservationStatus(%s) = %s, want %s", status, got, want)
		}
	}
}

// TestRetryablePostgresErrors 验证 Retryable Postgres Errors 场景下的行为。
func TestRetryablePostgresErrors(t *testing.T) {
	if !retryablePostgresError(stateError("40001")) || !retryablePostgresError(stateError("40P01")) {
		t.Fatal("serialization failure or deadlock was not retryable")
	}
	if retryablePostgresError(stateError("23505")) || retryablePostgresError(errors.New("plain error")) {
		t.Fatal("non-retryable database error was retried")
	}
}

// stateError 表示后端使用的 stateError 类型。
type stateError string

// Error 返回测试错误文本。
func (err stateError) Error() string { return string(err) }

// SQLState 实现当前测试场景所需的辅助行为。
func (err stateError) SQLState() string { return string(err) }

// reservationOrder 实现当前测试场景所需的辅助行为。
func reservationOrder(side domain.Side) domain.Order {
	intent := domain.OrderIntent{
		ModelID:            "model-a",
		StrategyID:         "strategy-v1",
		ExecutionAccountID: "account-a-v1",
		SignalID:           "signal-1",
		ClientOrderID:      "client-1",
		Venue:              "polymarket",
		MarketID:           "market-1",
		ConditionID:        "condition-1",
		TokenID:            "token-yes",
		Side:               side,
		Type:               domain.OrderTypeLimit,
		Price:              "0.51",
		WorstPrice:         "0.53",
		Size:               "10",
		TimeInForce:        domain.TimeInForceGTC,
	}
	return domain.Order{ID: "order-1", Intent: intent, Status: domain.OrderStatusAccepted, FilledSize: "0"}
}
