package execution_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/adapter/paper"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/execution"
)

// orderedReservations records the client order ids in the order Reserve was
// called, and can be told to fail every reservation.
type orderedReservations struct {
	port.AssetReservationManager
	mu      sync.Mutex
	order   []string
	failure error
}

func (reservations *orderedReservations) Reserve(ctx context.Context, order domain.Order) (domain.AssetReservation, error) {
	reservations.mu.Lock()
	reservations.order = append(reservations.order, order.Intent.ClientOrderID)
	reservations.mu.Unlock()
	if reservations.failure != nil {
		return domain.AssetReservation{}, reservations.failure
	}
	return reservations.AssetReservationManager.Reserve(ctx, order)
}

func (reservations *orderedReservations) reserved() []string {
	reservations.mu.Lock()
	defer reservations.mu.Unlock()
	return append([]string(nil), reservations.order...)
}

// signalingGuard reports every intent that reached the risk check, so a test
// can prove the step before reservation really ran while Reserve was held back.
type signalingGuard struct{ reached chan string }

func (guard signalingGuard) Check(_ context.Context, intent domain.OrderIntent) error {
	guard.reached <- intent.ClientOrderID
	return nil
}

type failingMarketValidator struct{}

func (failingMarketValidator) Validate(context.Context, domain.OrderIntent) (domain.MarketValidation, error) {
	return domain.MarketValidation{}, &port.Rejection{Code: "BOOK_TOO_THIN", Reason: "no depth inside the protection price"}
}

// gatedVenue blocks Place until released so a test can observe a submission
// that is past its reservation but not yet finished.
type gatedVenue struct {
	*fakeVenue
	entered chan struct{}
	release chan struct{}
}

func (venue *gatedVenue) Place(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	close(venue.entered)
	<-venue.release
	return venue.fakeVenue.Place(ctx, order)
}

func newTurnService(t *testing.T, venue port.Venue, guard port.Guard, validator port.MarketValidator, reservations port.AssetReservationManager) *execution.Service {
	t.Helper()
	service, err := execution.New(execution.Params{
		Repository:      memory.NewOrderRepository(),
		Venue:           venue,
		Guard:           guard,
		MarketValidator: validator,
		Reservations:    reservations,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return service
}

func turnContext(ctx context.Context, sequence *port.ReservationSequence, index int) context.Context {
	return port.WithReservationTurn(ctx, sequence.Turn(index))
}

func assertTurnReleased(t *testing.T, sequence *port.ReservationSequence, index int, reason string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := sequence.Turn(index + 1).Wait(ctx); err != nil {
		t.Fatalf("turn %d was not released %s: %v", index, reason, err)
	}
}

// The step before reservation runs without waiting; Reserve itself waits for
// the previous turn, so concurrent submissions fund orders in batch order.
func TestSubmitReservesOnlyAfterPreviousTurnReleased(t *testing.T) {
	reservations := &orderedReservations{AssetReservationManager: paper.NewReservationManager()}
	guard := signalingGuard{reached: make(chan string, 4)}
	service := newTurnService(t, &fakeVenue{}, guard, allowMarketValidator{}, reservations)
	sequence := port.NewReservationSequence(2)

	second := make(chan error, 1)
	go func() {
		_, err := service.Submit(turnContext(context.Background(), sequence, 1), validIntent("client-second"))
		second <- err
	}()
	select {
	case id := <-guard.reached:
		if id != "client-second" {
			t.Fatalf("risk check saw %q first", id)
		}
	case <-time.After(time.Second):
		t.Fatal("the risk check must not wait for the previous reservation turn")
	}
	time.Sleep(50 * time.Millisecond)
	if got := reservations.reserved(); len(got) != 0 {
		t.Fatalf("reserved %v before the previous turn was released", got)
	}

	if _, err := service.Submit(turnContext(context.Background(), sequence, 0), validIntent("client-first")); err != nil {
		t.Fatalf("first Submit() error = %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("second Submit() error = %v", err)
	}
	got := reservations.reserved()
	if len(got) != 2 || got[0] != "client-first" || got[1] != "client-second" {
		t.Fatalf("reservation order = %v, want [client-first client-second]", got)
	}
}

// Reserve is the only step that has to be ordered, so the turn must be released
// as soon as it returns, not when the whole submission (placement, fills) ends.
func TestSubmitReleasesTurnRightAfterReserve(t *testing.T) {
	venue := &gatedVenue{fakeVenue: &fakeVenue{}, entered: make(chan struct{}), release: make(chan struct{})}
	service := newTurnService(t, venue, allowGuard{}, allowMarketValidator{}, paper.NewReservationManager())
	sequence := port.NewReservationSequence(2)

	done := make(chan error, 1)
	go func() {
		_, err := service.Submit(turnContext(context.Background(), sequence, 0), validIntent("client-first"))
		done <- err
	}()
	select {
	case <-venue.entered:
	case <-time.After(time.Second):
		t.Fatal("submission never reached placement")
	}
	assertTurnReleased(t, sequence, 0, "while the first order was still being placed")
	close(venue.release)
	if err := <-done; err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
}

// Every way out before or at reservation must free the next order.
func TestSubmitReleasesTurnOnEveryEarlyExit(t *testing.T) {
	t.Run("risk rejection", func(t *testing.T) {
		service := newTurnService(t, &fakeVenue{}, rejectGuard{}, allowMarketValidator{}, paper.NewReservationManager())
		sequence := port.NewReservationSequence(2)
		if _, err := service.Submit(turnContext(context.Background(), sequence, 0), validIntent("client-risk")); err == nil {
			t.Fatal("Submit() error = nil, want risk rejection")
		}
		assertTurnReleased(t, sequence, 0, "after a risk rejection")
	})
	t.Run("market validation failure", func(t *testing.T) {
		service := newTurnService(t, &fakeVenue{}, allowGuard{}, failingMarketValidator{}, paper.NewReservationManager())
		sequence := port.NewReservationSequence(2)
		if _, err := service.Submit(turnContext(context.Background(), sequence, 0), validIntent("client-book")); err == nil {
			t.Fatal("Submit() error = nil, want market validation failure")
		}
		assertTurnReleased(t, sequence, 0, "after a market validation failure")
	})
	t.Run("reservation rejection", func(t *testing.T) {
		reservations := &orderedReservations{
			AssetReservationManager: paper.NewReservationManager(),
			failure:                 &port.Rejection{Code: "INSUFFICIENT_AVAILABLE_BALANCE", Reason: "no money"},
		}
		venue := &fakeVenue{}
		service := newTurnService(t, venue, allowGuard{}, allowMarketValidator{}, reservations)
		sequence := port.NewReservationSequence(2)
		if _, err := service.Submit(turnContext(context.Background(), sequence, 0), validIntent("client-money")); err == nil {
			t.Fatal("Submit() error = nil, want reservation rejection")
		}
		assertTurnReleased(t, sequence, 0, "after a reservation rejection")
		if venue.placeCalls.Load() != 0 {
			t.Fatal("venue was called after the reservation was rejected")
		}
	})
	t.Run("idempotent replay", func(t *testing.T) {
		service := newTurnService(t, &fakeVenue{}, allowGuard{}, allowMarketValidator{}, paper.NewReservationManager())
		if _, err := service.Submit(context.Background(), validIntent("client-replay")); err != nil {
			t.Fatalf("first Submit() error = %v", err)
		}
		sequence := port.NewReservationSequence(2)
		result, err := service.Submit(turnContext(context.Background(), sequence, 0), validIntent("client-replay"))
		if err != nil || result.Created {
			t.Fatalf("replay = %#v, %v; want existing order", result, err)
		}
		assertTurnReleased(t, sequence, 0, "after an idempotent replay")
	})
}

// If the batch is cancelled while an order waits for its turn, it must neither
// reserve money nor reach the venue.
func TestSubmitDoesNotReserveWhenTurnWaitIsCancelled(t *testing.T) {
	reservations := &orderedReservations{AssetReservationManager: paper.NewReservationManager()}
	venue := &fakeVenue{}
	service := newTurnService(t, venue, allowGuard{}, allowMarketValidator{}, reservations)
	sequence := port.NewReservationSequence(2)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := service.Submit(turnContext(ctx, sequence, 1), validIntent("client-waiting"))
		result <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-result:
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("Submit() error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Submit() ignored cancellation while waiting for its reservation turn")
	}
	if got := reservations.reserved(); len(got) != 0 {
		t.Fatalf("reserved %v although the turn wait was cancelled", got)
	}
	if venue.placeCalls.Load() != 0 {
		t.Fatal("venue was called although the turn wait was cancelled")
	}
}

// Without a turn (HTTP submissions, position exits) nothing changes.
func TestSubmitWithoutReservationTurnReservesImmediately(t *testing.T) {
	reservations := &orderedReservations{AssetReservationManager: paper.NewReservationManager()}
	service := newTurnService(t, &fakeVenue{}, allowGuard{}, allowMarketValidator{}, reservations)
	if _, err := service.Submit(context.Background(), validIntent("client-plain")); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if got := reservations.reserved(); len(got) != 1 || got[0] != "client-plain" {
		t.Fatalf("reserved = %v", got)
	}
}
