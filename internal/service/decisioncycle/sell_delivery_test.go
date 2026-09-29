package decisioncycle

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type serialTestAccountGate struct {
	slot chan struct{}
}

func newSerialTestAccountGate() *serialTestAccountGate {
	return &serialTestAccountGate{slot: make(chan struct{}, 1)}
}

func (gate *serialTestAccountGate) AcquireExecutionAccount(ctx context.Context, _ string) (func(), error) {
	select {
	case gate.slot <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-gate.slot }) }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type controlledBatchExecutor struct {
	started chan int
	proceed chan struct{}
	calls   int
}

func (executor *controlledBatchExecutor) Submit(_ context.Context, intent domain.OrderIntent) (port.OrderSubmitResult, error) {
	executor.calls++
	executor.started <- executor.calls
	<-executor.proceed
	status := domain.OrderStatusAcknowledged
	if executor.calls == 2 {
		status = domain.OrderStatusRejected
	}
	return port.OrderSubmitResult{Order: domain.Order{ID: "order-" + intent.ClientOrderID, Status: status}}, nil
}

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

func TestAccountGateCoversWholeDeliveryBatchAndReleasesAfterFailure(t *testing.T) {
	recorder := &fakeRecorder{}
	for _, clientID := range []string{"first", "second"} {
		recorder.deliveries = append(recorder.deliveries, domain.DecisionIntentDelivery{
			CycleID: "cycle-1", ClientOrderID: clientID, Status: domain.DecisionIntentPending,
			Intent: domain.OrderIntent{ClientOrderID: clientID, ExecutionAccountID: "wallet-6", Side: domain.SideBuy},
		})
	}
	gate := newSerialTestAccountGate()
	executor := &controlledBatchExecutor{started: make(chan int, 2), proceed: make(chan struct{}, 2)}
	service := &Service{
		recorder: recorder, executor: executor, accountGate: gate,
		activeExecutionAccountIDs: []string{"wallet-6"}, entryEnabledExecutionAccountIDs: []string{"wallet-6"},
	}
	deliveryDone := make(chan error, 1)
	go func() {
		_, err := service.deliverPending(context.Background(), "cycle-1")
		deliveryDone <- err
	}()

	if call := <-executor.started; call != 1 {
		t.Fatalf("first submit call = %d", call)
	}
	reconciliationAcquired := make(chan func(), 1)
	go func() {
		release, err := gate.AcquireExecutionAccount(context.Background(), "wallet-6")
		if err == nil {
			reconciliationAcquired <- release
		}
	}()
	select {
	case <-reconciliationAcquired:
		t.Fatal("reconciliation acquired the account gate during the first submit")
	case <-time.After(20 * time.Millisecond):
	}

	executor.proceed <- struct{}{}
	if call := <-executor.started; call != 2 {
		t.Fatalf("second submit call = %d", call)
	}
	select {
	case <-reconciliationAcquired:
		t.Fatal("reconciliation acquired the account gate between batch submissions")
	case <-time.After(20 * time.Millisecond):
	}
	executor.proceed <- struct{}{}

	if err := <-deliveryDone; err == nil || !strings.Contains(err.Error(), "completed durable delivery as FAILED") {
		t.Fatalf("deliverPending() error = %v, want durable rejection", err)
	}
	select {
	case release := <-reconciliationAcquired:
		release()
	case <-time.After(time.Second):
		t.Fatal("account gate was not released after the failed batch")
	}
	if recorder.deliveries[0].Status != domain.DecisionIntentSubmitted ||
		recorder.deliveries[1].Status != domain.DecisionIntentFailed {
		t.Fatalf("delivery statuses = %s/%s", recorder.deliveries[0].Status, recorder.deliveries[1].Status)
	}
}
