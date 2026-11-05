package execution_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/adapter/paper"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/execution"
)

// testClock is a mutable clock shared by the service and the test.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *testClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *testClock) Set(value time.Time) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = value
}

type expiringTestPlacement struct {
	expectedHash string
	expiresAt    time.Time
}

func (placement expiringTestPlacement) ExpectedVenueOrderID() string { return placement.expectedHash }
func (placement expiringTestPlacement) VenueExpiresAt() (time.Time, bool) {
	return placement.expiresAt, !placement.expiresAt.IsZero()
}

// hookPreparedVenue runs a hook in the middle of the POST, the window in which
// the order is durably SUBMITTING and the outcome is not yet recorded.
type hookPreparedVenue struct {
	expectedHash string
	expiresAt    time.Time
	duringPost   func(ctx context.Context, order domain.Order) (port.VenueOrder, error)
	getErr       error
	getCalls     atomic.Int64
	cancelCalls  atomic.Int64
}

func (*hookPreparedVenue) Name() string { return "polymarket-paper" }

func (venue *hookPreparedVenue) PreparePlace(context.Context, domain.Order) (port.PreparedPlacement, error) {
	return expiringTestPlacement{expectedHash: venue.expectedHash, expiresAt: venue.expiresAt}, nil
}

func (venue *hookPreparedVenue) PlacePrepared(ctx context.Context, order domain.Order, _ port.PreparedPlacement) (port.VenueOrder, error) {
	if venue.duringPost != nil {
		return venue.duringPost(ctx, order)
	}
	return port.VenueOrder{ID: venue.expectedHash, State: port.VenueOrderLive, RawStatus: "live", FilledSize: "0", ObservedAt: order.UpdatedAt}, nil
}

func (*hookPreparedVenue) Place(context.Context, domain.Order) (port.VenueOrder, error) {
	return port.VenueOrder{}, errors.New("legacy Place must not be used")
}

func (venue *hookPreparedVenue) Cancel(_ context.Context, order domain.Order) (port.VenueOrder, error) {
	venue.cancelCalls.Add(1)
	return port.VenueOrder{ID: order.VenueOrderID, State: port.VenueOrderCancelled, RawStatus: "cancelled", FilledSize: "0", ObservedAt: order.UpdatedAt}, nil
}

func (venue *hookPreparedVenue) Get(_ context.Context, order domain.Order) (port.VenueOrder, error) {
	venue.getCalls.Add(1)
	if venue.getErr != nil {
		return port.VenueOrder{}, venue.getErr
	}
	return port.VenueOrder{ID: order.VenueOrderID, State: port.VenueOrderLive, RawStatus: "live", FilledSize: "0", ObservedAt: order.UpdatedAt}, nil
}

func newInFlightService(t *testing.T, repository port.OrderRepository, venue port.Venue, clock *testClock, id string) *execution.Service {
	t.Helper()
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: paper.NewReservationManager(),
		RequirePreparedPlacement: true, AccountScope: allowAllAccountScope{},
		Now: clock.Now, NewID: func() string { return id },
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

// TestRefreshDoesNotSeizeInFlightSubmit reproduces the coordinator race: the
// order is SUBMITTING while the POST takes 2-3.8 seconds. Recovery must wait
// instead of forcing UNKNOWN, and the submission then records its own result.
func TestRefreshDoesNotSeizeInFlightSubmit(t *testing.T) {
	for _, elapsed := range []time.Duration{500 * time.Millisecond, 2200 * time.Millisecond, 3780 * time.Millisecond} {
		t.Run(elapsed.String(), func(t *testing.T) {
			start := time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)
			clock := &testClock{now: start}
			repository := memory.NewOrderRepository()
			venue := &hookPreparedVenue{expectedHash: "0x" + strings.Repeat("ab", 32)}
			service := newInFlightService(t, repository, venue, clock, "ord-in-flight")
			var refreshErr error
			venue.duringPost = func(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
				clock.Set(start.Add(elapsed))
				_, refreshErr = service.Refresh(ctx, order.ID)
				return port.VenueOrder{ID: venue.expectedHash, State: port.VenueOrderLive, RawStatus: "live", FilledSize: "0", ObservedAt: start.Add(elapsed)}, nil
			}
			result, err := service.Submit(context.Background(), validIntent("client-in-flight"))
			if !errors.Is(refreshErr, execution.ErrSubmitInFlight) || venue.getCalls.Load() != 0 {
				t.Fatalf("Refresh during POST error = %v, get calls = %d; want in-flight wait", refreshErr, venue.getCalls.Load())
			}
			if err != nil || result.Order.Status != domain.OrderStatusLive {
				t.Fatalf("Submit() = %s, %v; want the submission to record its own LIVE result", result.Order.Status, err)
			}
			events, _ := repository.Events(context.Background(), result.Order.ID)
			for _, event := range events {
				if event.ReasonCode == "INCOMPLETE_SUBMIT_ATTEMPT" {
					t.Fatalf("in-flight submission was forced to UNKNOWN: %#v", event)
				}
			}
		})
	}
}

// TestRefreshTakesOverStaleOrPreviousProcessSubmit: past the grace, or when the
// attempt was started by an earlier process, recovery forces UNKNOWN at once.
func TestRefreshTakesOverStaleOrPreviousProcessSubmit(t *testing.T) {
	cases := []struct {
		name       string
		elapsed    time.Duration
		newProcess bool
		wantWait   bool
	}{
		{name: "within grace", elapsed: 89 * time.Second, wantWait: true},
		{name: "past grace", elapsed: 91 * time.Second},
		{name: "previous process", elapsed: 2 * time.Second, newProcess: true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			start := time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)
			clock := &testClock{now: start}
			repository := &failFirstFinishRepository{OrderRepository: memory.NewOrderRepository()}
			venue := &hookPreparedVenue{expectedHash: "0x" + strings.Repeat("cd", 32)}
			service := newInFlightService(t, repository, venue, clock, "ord-stale-submit")
			if _, err := service.Submit(context.Background(), validIntent("client-stale-submit")); err == nil {
				t.Fatal("Submit() error = nil, want the injected outcome persistence failure")
			}
			stored, _ := repository.Get(context.Background(), "ord-stale-submit")
			if stored.Status != domain.OrderStatusSubmitting {
				t.Fatalf("status = %s, want SUBMITTING left behind", stored.Status)
			}
			clock.Set(start.Add(testCase.elapsed))
			refresher := service
			if testCase.newProcess {
				refresher = newInFlightService(t, repository, venue, clock, "unused")
			}
			refreshed, err := refresher.Refresh(context.Background(), stored.ID)
			if testCase.wantWait {
				if !errors.Is(err, execution.ErrSubmitInFlight) || venue.getCalls.Load() != 0 {
					t.Fatalf("Refresh() = %s, %v; want in-flight wait", refreshed.Status, err)
				}
				return
			}
			if errors.Is(err, execution.ErrSubmitInFlight) || venue.getCalls.Load() != 1 {
				t.Fatalf("Refresh() = %s, %v, get calls %d; want takeover", refreshed.Status, err, venue.getCalls.Load())
			}
			events, _ := repository.Events(context.Background(), stored.ID)
			found := false
			for _, event := range events {
				found = found || event.ReasonCode == "INCOMPLETE_SUBMIT_ATTEMPT"
			}
			if !found {
				t.Fatal("takeover did not record INCOMPLETE_SUBMIT_ATTEMPT")
			}
		})
	}
}

type attemptsFailRepository struct {
	*failFirstFinishRepository
	fail atomic.Bool
}

func (repository *attemptsFailRepository) Attempts(ctx context.Context, orderID string) ([]domain.OrderAttempt, error) {
	if repository.fail.Load() {
		return nil, errors.New("attempt read failed")
	}
	return repository.failFirstFinishRepository.Attempts(ctx, orderID)
}

func TestRefreshTreatsUnreadableAttemptsAsInFlight(t *testing.T) {
	start := time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)
	clock := &testClock{now: start}
	repository := &attemptsFailRepository{failFirstFinishRepository: &failFirstFinishRepository{OrderRepository: memory.NewOrderRepository()}}
	venue := &hookPreparedVenue{expectedHash: "0x" + strings.Repeat("ef", 32)}
	service := newInFlightService(t, repository, venue, clock, "ord-attempts-fail")
	_, _ = service.Submit(context.Background(), validIntent("client-attempts-fail"))
	clock.Set(start.Add(10 * time.Minute))
	repository.fail.Store(true)
	if _, err := service.Refresh(context.Background(), "ord-attempts-fail"); !errors.Is(err, execution.ErrSubmitInFlight) {
		t.Fatalf("Refresh() error = %v, want in-flight wait when attempts are unreadable", err)
	}
}

// ctxCheckingRepository fails ledger writes whose context is already done, as
// PostgreSQL does.
type ctxCheckingRepository struct {
	*memory.OrderRepository
}

func (repository *ctxCheckingRepository) FinishAttempt(ctx context.Context, order domain.Order, event domain.OrderEvent, attempt domain.OrderAttempt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return repository.OrderRepository.FinishAttempt(ctx, order, event, attempt)
}

func TestSubmitVenueCallsShareTimeoutAndOutcomeStillPersists(t *testing.T) {
	start := time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)
	clock := &testClock{now: start}
	repository := &ctxCheckingRepository{OrderRepository: memory.NewOrderRepository()}
	venue := &hookPreparedVenue{expectedHash: "0x" + strings.Repeat("12", 32)}
	venue.duringPost = func(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
		<-ctx.Done() // a hanging upstream is cut by the submit deadline
		return port.VenueOrder{}, &port.VenueError{Kind: port.VenueErrorAmbiguous, Code: "CLOB_TRANSPORT_OUTCOME_UNKNOWN", VenueOrderID: venue.expectedHash, Cause: ctx.Err()}
	}
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: paper.NewReservationManager(),
		RequirePreparedPlacement: true, AccountScope: allowAllAccountScope{},
		SubmitAttemptTimeout: time.Second, SubmitInFlightGrace: 3 * time.Second,
		Now: clock.Now, NewID: func() string { return "ord-submit-timeout" },
	})
	if err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	result, err := service.Submit(context.Background(), validIntent("client-submit-timeout"))
	if elapsed := time.Since(began); elapsed > 5*time.Second {
		t.Fatalf("Submit() took %s; the venue deadline did not apply", elapsed)
	}
	if err == nil || result.Order.Status != domain.OrderStatusUnknown {
		t.Fatalf("Submit() = %s, %v; want persisted UNKNOWN", result.Order.Status, err)
	}
	attempts, _ := repository.Attempts(context.Background(), result.Order.ID)
	if len(attempts) != 1 || attempts[0].Outcome != domain.AttemptOutcomeUnknown {
		t.Fatalf("attempts = %#v; want one UNKNOWN submit attempt", attempts)
	}
}

func TestSubmitOutcomePersistsAfterCallerCancellation(t *testing.T) {
	start := time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)
	clock := &testClock{now: start}
	repository := &ctxCheckingRepository{OrderRepository: memory.NewOrderRepository()}
	venue := &hookPreparedVenue{expectedHash: "0x" + strings.Repeat("34", 32)}
	ctx, cancel := context.WithCancel(context.Background())
	venue.duringPost = func(context.Context, domain.Order) (port.VenueOrder, error) {
		cancel() // the caller goes away while the POST is in flight
		return port.VenueOrder{}, &port.VenueError{Kind: port.VenueErrorAmbiguous, Code: "CLOB_SERVER_ERROR", HTTPStatus: 500, VenueOrderID: venue.expectedHash}
	}
	service := newInFlightService(t, repository, venue, clock, "ord-caller-cancel")
	result, _ := service.Submit(ctx, validIntent("client-caller-cancel"))
	stored, err := repository.Get(context.Background(), result.Order.ID)
	if err != nil || stored.Status != domain.OrderStatusUnknown {
		t.Fatalf("stored = %s, %v; want UNKNOWN recorded despite caller cancellation", stored.Status, err)
	}
}

func TestSubmitRecordsVenueExpiryWithStartedAttempt(t *testing.T) {
	start := time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)
	clock := &testClock{now: start}
	repository := memory.NewOrderRepository()
	expiresAt := start.Add(5 * time.Minute)
	venue := &hookPreparedVenue{expectedHash: "0x" + strings.Repeat("56", 32), expiresAt: expiresAt}
	var observed *time.Time
	venue.duringPost = func(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
		attempts, _ := repository.Attempts(ctx, order.ID)
		if len(attempts) == 1 {
			observed = attempts[0].VenueExpiresAt
		}
		return port.VenueOrder{}, &port.VenueError{Kind: port.VenueErrorAmbiguous, Code: "CLOB_SERVER_ERROR", HTTPStatus: 500}
	}
	service := newInFlightService(t, repository, venue, clock, "ord-venue-expiry")
	_, _ = service.Submit(context.Background(), validIntent("client-venue-expiry"))
	if observed == nil || !observed.Equal(expiresAt) {
		t.Fatalf("venue expiry before POST = %v, want %s written with the STARTED attempt", observed, expiresAt)
	}
}

type fakeUnacceptedResolver struct {
	calls    atomic.Int64
	released bool
	result   func(domain.Order) domain.Order
	err      error
}

func (resolver *fakeUnacceptedResolver) ResolveUnaccepted(_ context.Context, order domain.Order) (domain.Order, bool, error) {
	resolver.calls.Add(1)
	if resolver.result != nil {
		return resolver.result(order), resolver.released, resolver.err
	}
	return order, resolver.released, resolver.err
}

func newResolverTestService(t *testing.T, resolver port.UnacceptedOrderResolver, synchronizer port.FillSynchronizer, trigger port.ReconciliationTriggerer) (*execution.Service, *fakeVenue, *trackingReservations) {
	t.Helper()
	now := time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)
	venue := &fakeVenue{}
	reservations := &trackingReservations{delegate: paper.NewReservationManager()}
	service, err := execution.New(execution.Params{
		Repository: memory.NewOrderRepository(), Venue: venue, Guard: allowGuard{}, MarketValidator: allowMarketValidator{},
		Reservations: reservations, FillSynchronizer: synchronizer, AuthoritativeFills: true,
		Reconciliation: trigger, UnacceptedResolver: resolver,
		Now: func() time.Time { return now }, NewID: func() string { return "ord-resolver-hook" },
	})
	if err != nil {
		t.Fatal(err)
	}
	return service, venue, reservations
}

func TestRefreshOrderNotFoundUsesResolverOnlyAfterCleanFillSync(t *testing.T) {
	ctx := context.Background()
	notFound := &port.VenueError{Kind: port.VenueErrorUnavailable, Code: "CLOB_ORDER_NOT_FOUND", Message: "CLOB returned no order"}

	t.Run("released", func(t *testing.T) {
		trigger := &reconciliationTrigger{}
		resolver := &fakeUnacceptedResolver{released: true, result: func(order domain.Order) domain.Order {
			order.Status = domain.OrderStatusCancelled
			return order
		}}
		service, venue, _ := newResolverTestService(t, resolver, &fakeFillSynchronizer{}, trigger)
		result, err := service.Submit(ctx, validIntent("client-resolver-released"))
		if err != nil {
			t.Fatal(err)
		}
		venue.getErr = notFound
		refreshed, err := service.Refresh(ctx, result.Order.ID)
		if err != nil || refreshed.Status != domain.OrderStatusCancelled || resolver.calls.Load() != 1 {
			t.Fatalf("Refresh() = %s, %v, resolver calls %d", refreshed.Status, err, resolver.calls.Load())
		}
		found := false
		for _, call := range trigger.calls {
			found = found || call.trigger == domain.ReconciliationTriggerAssetDrift
		}
		if !found {
			t.Fatal("release did not trigger an asset-drift reconciliation")
		}
	})
	t.Run("not released keeps UNKNOWN and frozen", func(t *testing.T) {
		resolver := &fakeUnacceptedResolver{err: errors.New("rpc failed")}
		service, venue, reservations := newResolverTestService(t, resolver, &fakeFillSynchronizer{}, nil)
		result, err := service.Submit(ctx, validIntent("client-resolver-kept"))
		if err != nil {
			t.Fatal(err)
		}
		venue.getErr = notFound
		refreshed, err := service.Refresh(ctx, result.Order.ID)
		if err == nil || refreshed.Status != domain.OrderStatusUnknown || reservations.uncertainCalls.Load() == 0 {
			t.Fatalf("Refresh() = %s, %v; want UNKNOWN with frozen reservation", refreshed.Status, err)
		}
	})
	t.Run("fill sync failure skips resolver", func(t *testing.T) {
		resolver := &fakeUnacceptedResolver{released: true}
		synchronizer := &fakeFillSynchronizer{sync: func(context.Context, string) error { return errors.New("trades unavailable") }}
		service, venue, _ := newResolverTestService(t, resolver, synchronizer, nil)
		result, err := service.Submit(ctx, validIntent("client-resolver-sync-fail"))
		if err != nil {
			t.Fatal(err)
		}
		venue.getErr = notFound
		refreshed, _ := service.Refresh(ctx, result.Order.ID)
		if resolver.calls.Load() != 0 || refreshed.Status != domain.OrderStatusUnknown {
			t.Fatalf("resolver calls = %d, status %s; want no resolver call after a failed fill sync", resolver.calls.Load(), refreshed.Status)
		}
	})
}
