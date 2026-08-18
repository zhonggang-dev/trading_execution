package orderstate_test

import (
	"errors"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/orderstate"
)

// TestHappyPathAndAcceptedIsNotFilled 验证 Happy Path And Accepted Is Not Filled 场景下的行为。
func TestHappyPathAndAcceptedIsNotFilled(t *testing.T) {
	order := baseOrder(domain.OrderStatusReceived)
	for index, status := range []domain.OrderStatus{
		domain.OrderStatusValidating,
		domain.OrderStatusReserved,
		domain.OrderStatusSubmitting,
		domain.OrderStatusAcknowledged,
		domain.OrderStatusLive,
		domain.OrderStatusPartiallyFilled,
		domain.OrderStatusFilled,
	} {
		filled := domain.Decimal("0")
		if status == domain.OrderStatusPartiallyFilled {
			filled = "4"
		}
		if status == domain.OrderStatusFilled {
			filled = "10"
		}
		next, event, err := orderstate.Apply(order, orderstate.Transition{
			EventID:    "event-" + string(status),
			To:         status,
			Trigger:    domain.TransitionTriggerVenueResponse,
			FilledSize: filled,
			At:         order.UpdatedAt.Add(time.Duration(index+1) * time.Second),
		})
		if err != nil {
			t.Fatalf("Apply(%s -> %s) error = %v", order.Status, status, err)
		}
		if event.FromStatus != order.Status || event.ToStatus != status || event.Revision != next.Revision {
			t.Fatalf("event = %#v, next = %#v", event, next)
		}
		order = next
	}
	if !order.Terminal() || order.Status != domain.OrderStatusFilled {
		t.Fatalf("final order = %#v", order)
	}
}

// TestIllegalTransitionIsRejected 验证 Illegal Transition Is Rejected 场景下的行为。
func TestIllegalTransitionIsRejected(t *testing.T) {
	order := baseOrder(domain.OrderStatusSubmitting)
	_, _, err := orderstate.Apply(order, orderstate.Transition{
		EventID: "event-bad",
		To:      domain.OrderStatusFilled,
		At:      order.UpdatedAt.Add(time.Second),
	})
	if !errors.Is(err, orderstate.ErrIllegalTransition) {
		t.Fatalf("Apply() error = %v, want ErrIllegalTransition", err)
	}
}

// TestCancelRaceCanFinishFilled 验证 Cancel Race Can Finish Filled 场景下的行为。
func TestCancelRaceCanFinishFilled(t *testing.T) {
	order := baseOrder(domain.OrderStatusCancelPending)
	next, _, err := orderstate.Apply(order, orderstate.Transition{
		EventID:    "event-race",
		To:         domain.OrderStatusFilled,
		Trigger:    domain.TransitionTriggerVenueObserve,
		FilledSize: "10",
		At:         order.UpdatedAt.Add(time.Second),
	})
	if err != nil || next.Status != domain.OrderStatusFilled {
		t.Fatalf("Apply() = %#v, %v", next, err)
	}
}

// TestUnknownMustPassThroughReconciling 验证 Unknown Must Pass Through Reconciling 场景下的行为。
func TestUnknownMustPassThroughReconciling(t *testing.T) {
	order := baseOrder(domain.OrderStatusUnknown)
	_, _, err := orderstate.Apply(order, orderstate.Transition{
		EventID: "event-skip",
		To:      domain.OrderStatusLive,
		At:      order.UpdatedAt.Add(time.Second),
	})
	if !errors.Is(err, orderstate.ErrIllegalTransition) {
		t.Fatalf("Apply() error = %v, want illegal transition", err)
	}
	order, _, err = orderstate.Apply(order, orderstate.Transition{
		EventID: "event-reconciling",
		To:      domain.OrderStatusReconciling,
		At:      order.UpdatedAt.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	order, _, err = orderstate.Apply(order, orderstate.Transition{
		EventID:    "event-live",
		To:         domain.OrderStatusLive,
		FilledSize: "0",
		At:         order.UpdatedAt.Add(time.Second),
	})
	if err != nil || order.Status != domain.OrderStatusLive {
		t.Fatalf("reconciliation = %#v, %v", order, err)
	}
}

// baseOrder 构建测试使用的基础领域对象。
func baseOrder(status domain.OrderStatus) domain.Order {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	return domain.Order{
		ID: "order-1",
		Intent: domain.OrderIntent{
			Size: "10",
		},
		Status:     status,
		FilledSize: "0",
		CreatedAt:  now,
		UpdatedAt:  now,
		Revision:   1,
	}
}
