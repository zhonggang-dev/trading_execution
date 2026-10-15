package decisioncycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// lockedRecorder makes the shared fake safe for concurrent CompleteIntent calls
// (the real recorder is a database) and counts completions per delivery.
type lockedRecorder struct {
	*fakeRecorder
	mu        sync.Mutex
	completed map[string]int
}

func newLockedRecorder(accountID string, count int) *lockedRecorder {
	recorder := &lockedRecorder{fakeRecorder: &fakeRecorder{}, completed: map[string]int{}}
	for index := 0; index < count; index++ {
		clientID := deliveryID(index)
		recorder.deliveries = append(recorder.deliveries, domain.DecisionIntentDelivery{
			CycleID: "cycle-1", ClientOrderID: clientID, Sequence: index, Status: domain.DecisionIntentPending,
			Intent: domain.OrderIntent{ClientOrderID: clientID, ExecutionAccountID: accountID, Side: domain.SideBuy},
		})
	}
	return recorder
}

func deliveryID(index int) string { return fmt.Sprintf("d%02d", index) }

func (recorder *lockedRecorder) CompleteIntent(ctx context.Context, clientOrderID string, attempt int, completion domain.DecisionIntentCompletion) error {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.completed[clientOrderID]++
	return recorder.fakeRecorder.CompleteIntent(ctx, clientOrderID, attempt, completion)
}

// turnExecutor stands in for execution.Service: it works for a while before
// reservation, reserves in turn when the context carries one, then keeps
// working (placement). It records concurrency and the reservation order.
type turnExecutor struct {
	mu          sync.Mutex
	inFlight    int
	maxInFlight int
	started     []string
	reserved    []string
	sawTurn     bool

	preDelay  func(index int) time.Duration
	postDelay time.Duration
	// skipTurn returns without touching the turn (rejected before reservation).
	skipTurn map[string]bool
	// failEarly returns a pre-order infrastructure error without a durable order.
	failEarly map[string]bool
	// blockUntilCancel waits for ctx to end before doing anything else.
	blockUntilCancel map[string]bool
}

func (executor *turnExecutor) Submit(ctx context.Context, intent domain.OrderIntent) (port.OrderSubmitResult, error) {
	id := intent.ClientOrderID
	var index int
	_, _ = fmt.Sscanf(id, "d%02d", &index)

	executor.mu.Lock()
	executor.inFlight++
	if executor.inFlight > executor.maxInFlight {
		executor.maxInFlight = executor.inFlight
	}
	executor.started = append(executor.started, id)
	executor.mu.Unlock()
	defer func() {
		executor.mu.Lock()
		executor.inFlight--
		executor.mu.Unlock()
	}()

	if executor.blockUntilCancel[id] {
		<-ctx.Done()
		return port.OrderSubmitResult{}, ctx.Err()
	}
	if executor.preDelay != nil {
		time.Sleep(executor.preDelay(index))
	}
	if executor.failEarly[id] {
		return port.OrderSubmitResult{}, errors.New("database unavailable before order creation")
	}
	if !executor.skipTurn[id] {
		turn, hasTurn := port.ReservationTurnFrom(ctx)
		if hasTurn {
			if err := turn.Wait(ctx); err != nil {
				return port.OrderSubmitResult{}, err
			}
		}
		executor.mu.Lock()
		executor.sawTurn = executor.sawTurn || hasTurn
		executor.reserved = append(executor.reserved, id)
		executor.mu.Unlock()
		if hasTurn {
			turn.Release()
		}
	}
	time.Sleep(executor.postDelay)
	return port.OrderSubmitResult{Order: domain.Order{ID: "order-" + id, Status: domain.OrderStatusAcknowledged}}, nil
}

func newDeliveryService(recorder port.DecisionRecorder, executor port.OrderExecutor, concurrency int) *Service {
	return &Service{
		recorder: recorder, executor: executor, deliveryConcurrency: concurrency,
		activeExecutionAccountIDs:       []string{"wallet-6"},
		entryEnabledExecutionAccountIDs: []string{"wallet-6"},
	}
}

func deliverWithin(t *testing.T, service *Service) accountDeliveryResult {
	t.Helper()
	type outcome struct {
		result accountDeliveryResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := service.deliverPendingForAccount(context.Background(), "wallet-6", "cycle-1", "")
		done <- outcome{result, err}
	}()
	select {
	case out := <-done:
		if out.err != nil {
			t.Fatalf("deliverPendingForAccount() error = %v", out.err)
		}
		return out.result
	case <-time.After(10 * time.Second):
		t.Fatal("delivery did not finish: a reservation turn was probably never released")
		return accountDeliveryResult{}
	}
}

func claimOrder(count int) []string {
	order := make([]string, count)
	for index := range order {
		order[index] = deliveryID(index)
	}
	return order
}

func TestSequentialDeliveryIsUnchangedByDefaultAndAtOne(t *testing.T) {
	for _, concurrency := range []int{0, 1} {
		t.Run(fmt.Sprintf("concurrency=%d", concurrency), func(t *testing.T) {
			recorder := newLockedRecorder("wallet-6", 6)
			executor := &turnExecutor{postDelay: 5 * time.Millisecond}
			result := deliverWithin(t, newDeliveryService(recorder, executor, concurrency))
			if executor.maxInFlight != 1 {
				t.Fatalf("max in flight = %d, want strictly sequential", executor.maxInFlight)
			}
			if executor.sawTurn {
				t.Fatal("sequential delivery must not attach a reservation turn")
			}
			if got := strings.Join(executor.started, ","); got != strings.Join(claimOrder(6), ",") {
				t.Fatalf("submit order = %s", got)
			}
			if len(result.intents) != 6 || len(result.errors) != 0 {
				t.Fatalf("results = %d, errors = %v", len(result.intents), result.errors)
			}
		})
	}
}

func TestParallelDeliveryKeepsReservationOrderResultsAndCompletionIntact(t *testing.T) {
	const count = 12
	recorder := newLockedRecorder("wallet-6", count)
	// Earlier orders are slower before reservation, so without the reservation
	// sequence the reservation order would come out reversed.
	executor := &turnExecutor{
		preDelay:  func(index int) time.Duration { return time.Duration(count-index) * 4 * time.Millisecond },
		postDelay: 20 * time.Millisecond,
	}
	result := deliverWithin(t, newDeliveryService(recorder, executor, 4))

	if got := strings.Join(executor.reserved, ","); got != strings.Join(claimOrder(count), ",") {
		t.Fatalf("reservation order = %s, want claim order", got)
	}
	if executor.maxInFlight > 4 {
		t.Fatalf("max in flight = %d, want at most 4", executor.maxInFlight)
	}
	if executor.maxInFlight < 2 {
		t.Fatalf("max in flight = %d, delivery did not run in parallel", executor.maxInFlight)
	}
	if !executor.sawTurn {
		t.Fatal("parallel delivery did not attach reservation turns")
	}
	if len(result.intents) != count || len(result.errors) != 0 {
		t.Fatalf("results = %d, errors = %v", len(result.intents), result.errors)
	}
	for index, intentResult := range result.intents {
		if intentResult.Intent.ClientOrderID != deliveryID(index) {
			t.Fatalf("result %d is %s, want claim order", index, intentResult.Intent.ClientOrderID)
		}
		if intentResult.DeliveryStatus != domain.DecisionIntentSubmitted {
			t.Fatalf("result %d status = %s", index, intentResult.DeliveryStatus)
		}
	}
	for index := 0; index < count; index++ {
		if recorder.completed[deliveryID(index)] != 1 {
			t.Fatalf("%s completed %d times, want exactly once", deliveryID(index), recorder.completed[deliveryID(index)])
		}
	}
}

// Orders that leave before reservation (rejected, failed, disabled) must not
// stall the ones behind them, and errors still come out in claim order.
func TestParallelDeliveryDoesNotStallBehindEarlyExits(t *testing.T) {
	const count = 10
	recorder := newLockedRecorder("wallet-6", count)
	executor := &turnExecutor{
		preDelay:  func(index int) time.Duration { return time.Duration((index%3)*3) * time.Millisecond },
		postDelay: 5 * time.Millisecond,
		skipTurn:  map[string]bool{"d00": true, "d04": true},
		failEarly: map[string]bool{"d02": true, "d07": true},
	}
	service := newDeliveryService(recorder, executor, 3)
	service.submissionPolicy = disableOnly{"d05"}
	result := deliverWithin(t, service)

	if len(result.intents) != count {
		t.Fatalf("results = %d, want %d", len(result.intents), count)
	}
	if !result.intents[5].SubmissionDisabled {
		t.Fatalf("d05 should be reported as submission disabled: %#v", result.intents[5])
	}
	// d05 never reached the executor; d02/d07 failed before creating an order.
	wantErrors := []string{"submit d02:", "submit d07:"}
	if len(result.errors) != len(wantErrors) {
		t.Fatalf("errors = %v", result.errors)
	}
	for index, want := range wantErrors {
		if !strings.HasPrefix(result.errors[index].Error(), want) {
			t.Fatalf("error %d = %q, want prefix %q (claim order)", index, result.errors[index], want)
		}
	}
	reserved := strings.Join(executor.reserved, ",")
	if reserved != "d01,d03,d06,d08,d09" {
		t.Fatalf("reservation order = %s", reserved)
	}
	if recorder.deliveries[2].Status != domain.DecisionIntentSubmitting || recorder.deliveries[7].Status != domain.DecisionIntentSubmitting {
		t.Fatal("deliveries that failed before an order existed must stay leased for recovery")
	}
}

type disableOnly struct{ id string }

func (policy disableOnly) Enabled(intent domain.OrderIntent) bool {
	return intent.ClientOrderID != policy.id
}

// A cancelled context stops everything promptly; nothing waits for a turn that
// will never come.
func TestParallelDeliveryReturnsWhenContextIsCancelled(t *testing.T) {
	const count = 8
	recorder := newLockedRecorder("wallet-6", count)
	executor := &turnExecutor{
		blockUntilCancel: map[string]bool{"d00": true},
		postDelay:        time.Millisecond,
	}
	service := newDeliveryService(recorder, executor, 4)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan accountDeliveryResult, 1)
	go func() {
		result, _ := service.deliverPendingForAccount(ctx, "wallet-6", "cycle-1", "")
		done <- result
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case result := <-done:
		if len(result.intents) != count {
			t.Fatalf("results = %d, want %d", len(result.intents), count)
		}
		if len(result.errors) == 0 {
			t.Fatal("cancellation must surface as delivery errors")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("delivery ignored context cancellation")
	}
	// The blocked first order never reserved, so nothing behind it did either.
	if len(executor.reserved) != 0 {
		t.Fatalf("reserved %v although the first order never reached reservation", executor.reserved)
	}
}

func TestNewRejectsOutOfRangeDeliveryConcurrency(t *testing.T) {
	for _, concurrency := range []int{-1, maxDeliveryConcurrency + 1} {
		_, err := newTestService(Params{
			PredictionSource: fakePredictionSource{}, PositionSource: fakePositionSource{},
			OrderBookSource: &fakeOrderBookSource{}, Strategy: &fakeStrategy{},
			Recorder: &fakeRecorder{}, Executor: &fakeExecutor{},
			Bindings:            []domain.StrategyExecutionBinding{testExecutionBinding()},
			Venue:               "polymarket-paper",
			DeliveryConcurrency: concurrency,
		})
		if err == nil || !strings.Contains(err.Error(), "delivery concurrency") {
			t.Fatalf("New(DeliveryConcurrency=%d) error = %v", concurrency, err)
		}
	}
	for _, concurrency := range []int{0, 1, 4, maxDeliveryConcurrency} {
		if _, err := newTestService(Params{
			PredictionSource: fakePredictionSource{}, PositionSource: fakePositionSource{},
			OrderBookSource: &fakeOrderBookSource{}, Strategy: &fakeStrategy{},
			Recorder: &fakeRecorder{}, Executor: &fakeExecutor{},
			Bindings:            []domain.StrategyExecutionBinding{testExecutionBinding()},
			Venue:               "polymarket-paper",
			DeliveryConcurrency: concurrency,
		}); err != nil {
			t.Fatalf("New(DeliveryConcurrency=%d) error = %v", concurrency, err)
		}
	}
}
