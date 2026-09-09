package decisioncycle

import (
	"context"
	"errors"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type firstSellFailureExecutor struct {
	first fixedResultExecutor
	calls []domain.OrderIntent
}

func (executor *firstSellFailureExecutor) Submit(ctx context.Context, intent domain.OrderIntent) (port.OrderSubmitResult, error) {
	executor.calls = append(executor.calls, intent)
	if len(executor.calls) == 1 {
		return executor.first.Submit(ctx, intent)
	}
	return port.OrderSubmitResult{Order: domain.Order{ID: "order-" + intent.ClientOrderID, Status: domain.OrderStatusAcknowledged}}, nil
}

func TestFailedSellDeliveryDoesNotStopFollowingSellsOrBuys(t *testing.T) {
	for _, test := range []struct {
		name   string
		first  fixedResultExecutor
		status domain.DecisionIntentDeliveryStatus
	}{
		{"rejected", fixedResultExecutor{result: port.OrderSubmitResult{Order: domain.Order{ID: "order-first", Status: domain.OrderStatusRejected}}}, domain.DecisionIntentFailed},
		{"unknown", fixedResultExecutor{result: port.OrderSubmitResult{Order: domain.Order{ID: "order-first", Status: domain.OrderStatusUnknown}}}, domain.DecisionIntentUnknown},
		{"pre-order failure", fixedResultExecutor{err: errors.New("database unavailable")}, domain.DecisionIntentSubmitting},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := &fakeRecorder{}
			for index, side := range []domain.Side{domain.SideSell, domain.SideSell, domain.SideBuy} {
				clientID := []string{"first", "second", "third"}[index]
				recorder.deliveries = append(recorder.deliveries, domain.DecisionIntentDelivery{
					CycleID: "cycle-1", ClientOrderID: clientID, Status: domain.DecisionIntentPending,
					Intent: domain.OrderIntent{ClientOrderID: clientID, ExecutionAccountID: "wallet-7", Side: side},
				})
			}
			executor := &firstSellFailureExecutor{first: test.first}
			service := &Service{recorder: recorder, executor: executor,
				activeExecutionAccountIDs: []string{"wallet-7"}, entryEnabledExecutionAccountIDs: []string{"wallet-7"}}
			results, err := service.deliverPending(context.Background(), "cycle-1")
			if err == nil || len(results) != 3 || len(executor.calls) != 3 {
				t.Fatalf("delivery stopped or hid failure: results=%#v calls=%d err=%v", results, len(executor.calls), err)
			}
			if recorder.deliveries[0].Status != test.status || results[0].Error == nil {
				t.Fatalf("first failure not preserved: %#v", results[0])
			}
			for index := 1; index < 3; index++ {
				if recorder.deliveries[index].Status != domain.DecisionIntentSubmitted || results[index].Error != nil {
					t.Fatalf("subsequent delivery blocked: %#v", results[index])
				}
			}
		})
	}
}
