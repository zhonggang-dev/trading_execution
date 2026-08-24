package execution_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/adapter/paper"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/domain/orderstate"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/execution"
)

// TestSubmitIsIdempotentBeforeVenue 验证 Submit Is Idempotent Before Venue 场景下的行为。
func TestSubmitIsIdempotentBeforeVenue(t *testing.T) {
	venue := &fakeVenue{}
	service := newService(t, venue, allowGuard{})
	intent := validIntent("client-1")
	first, err := service.Submit(context.Background(), intent)
	if err != nil {
		t.Fatalf("first Submit() error = %v", err)
	}
	intent.Price = "0.50"
	second, err := service.Submit(context.Background(), intent)
	if err != nil {
		t.Fatalf("second Submit() error = %v", err)
	}
	if !first.Created || second.Created || first.Order.ID != second.Order.ID {
		t.Fatalf("submit results = %#v / %#v, want create then replay", first, second)
	}
	if calls := venue.placeCalls.Load(); calls != 1 {
		t.Fatalf("venue place calls = %d, want 1", calls)
	}
}

// TestSubmitRejectsClientOrderIDConflict 验证 Submit Rejects Client Order ID Conflict 场景下的行为。
func TestSubmitRejectsClientOrderIDConflict(t *testing.T) {
	venue := &fakeVenue{}
	service := newService(t, venue, allowGuard{})
	intent := validIntent("client-1")
	if _, err := service.Submit(context.Background(), intent); err != nil {
		t.Fatalf("first Submit() error = %v", err)
	}
	intent.TokenID = "another-token"
	if _, err := service.Submit(context.Background(), intent); !errors.Is(err, execution.ErrIdempotencyConflict) {
		t.Fatalf("second Submit() error = %v, want ErrIdempotencyConflict", err)
	}
	if calls := venue.placeCalls.Load(); calls != 1 {
		t.Fatalf("venue place calls = %d, want 1", calls)
	}
}

func TestAccountScopeRejectsRetiredSubmitBeforeAnyWrite(t *testing.T) {
	repository := memory.NewOrderRepository()
	venue := &fakeVenue{}
	reservations := &trackingReservations{delegate: paper.NewReservationManager()}
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: reservations,
		AccountScope: fixedAccountScope{
			active:  map[string]struct{}{"main": {}},
			managed: map[string]struct{}{"main": {}, "wallet-6": {}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	intent := validIntent("retired-submit")
	intent.ExecutionAccountID = "wallet-2"
	_, submitErr := service.Submit(context.Background(), intent)
	var rejection *port.Rejection
	if !errors.As(submitErr, &rejection) || rejection.Code != "EXECUTION_ACCOUNT_NOT_ACTIVE" {
		t.Fatalf("Submit() error = %v, rejection=%#v", submitErr, rejection)
	}
	if _, getErr := repository.GetByClientOrderID(context.Background(), intent.ClientOrderID); !errors.Is(getErr, port.ErrOrderNotFound) {
		t.Fatalf("retired Submit() wrote an order: %v", getErr)
	}
	if venue.placeCalls.Load() != 0 || reservations.reserveCalls.Load() != 0 ||
		reservations.reconcileCalls.Load() != 0 || reservations.uncertainCalls.Load() != 0 {
		t.Fatalf("retired Submit() side effects: venue=%d reserve=%d reconcile=%d uncertain=%d",
			venue.placeCalls.Load(), reservations.reserveCalls.Load(),
			reservations.reconcileCalls.Load(), reservations.uncertainCalls.Load())
	}
}

func TestAccountScopeAllowsActiveMainSubmit(t *testing.T) {
	venue := &fakeVenue{}
	service, err := execution.New(execution.Params{
		Repository: memory.NewOrderRepository(), Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: paper.NewReservationManager(),
		AccountScope: fixedAccountScope{
			active:  map[string]struct{}{"main": {}},
			managed: map[string]struct{}{"main": {}, "wallet-6": {}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	intent := validIntent("active-main-submit")
	intent.ExecutionAccountID = "main"
	if _, err := service.Submit(context.Background(), intent); err != nil {
		t.Fatalf("active main Submit() error = %v", err)
	}
	if venue.placeCalls.Load() != 1 {
		t.Fatalf("active main venue calls = %d, want 1", venue.placeCalls.Load())
	}
}

func TestAccountScopeQuarantinedWalletBlocksPlacementButAllowsManagedMaintenance(t *testing.T) {
	ctx := context.Background()
	repository := memory.NewOrderRepository()
	venue := &fakeVenue{}
	now := time.Date(2026, 8, 22, 1, 0, 0, 0, time.UTC)
	reservations := &successfulReservations{}
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: reservations,
		AccountScope: fixedAccountScope{
			active:  map[string]struct{}{"main": {}},
			managed: map[string]struct{}{"main": {}, "wallet-6": {}},
		},
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}

	blockedIntent := validIntent("quarantined-submit")
	blockedIntent.ExecutionAccountID = "wallet-6"
	if _, err := service.Submit(ctx, blockedIntent); err == nil {
		t.Fatal("quarantined Submit() error = nil")
	}

	create := func(id string, status domain.OrderStatus) domain.Order {
		intent := validIntent("quarantined-" + id)
		intent.ExecutionAccountID = "wallet-6"
		observedAt := now.Add(-time.Hour)
		order := domain.Order{
			ID: id, Intent: intent, Status: status, VenueOrderID: "venue-" + id,
			FilledSize: "0", CreatedAt: observedAt, UpdatedAt: observedAt, Revision: 1,
		}
		if status == domain.OrderStatusCancelled {
			order.VenueLastObservedAt = &observedAt
		}
		if _, created, err := repository.Create(ctx, order); err != nil || !created {
			t.Fatalf("Create(%s) = %t, %v", id, created, err)
		}
		return order
	}

	resumeOrder := create("resume-wallet6", domain.OrderStatusReceived)
	if _, err := service.Resume(ctx, resumeOrder.ID); err == nil {
		t.Fatal("quarantined Resume() error = nil")
	}

	refreshOrder := create("refresh-wallet6", domain.OrderStatusUnknown)
	if _, err := service.Refresh(ctx, refreshOrder.ID); err != nil {
		t.Fatalf("managed quarantined Refresh() error = %v", err)
	}

	cancelOrder := create("cancel-wallet6", domain.OrderStatusLive)
	if _, err := service.Cancel(ctx, cancelOrder.ID); err != nil {
		t.Fatalf("managed quarantined Cancel() error = %v", err)
	}

	finalizeOrder := create("finalize-wallet6", domain.OrderStatusCancelled)
	if _, err := service.FinalizeCancellation(ctx, finalizeOrder.ID); err != nil {
		t.Fatalf("managed quarantined FinalizeCancellation() error = %v", err)
	}
	if venue.placeCalls.Load() != 0 || venue.getCalls.Load() != 1 || venue.cancelCalls.Load() != 1 {
		t.Fatalf("managed maintenance venue calls place/get/cancel = %d/%d/%d",
			venue.placeCalls.Load(), venue.getCalls.Load(), venue.cancelCalls.Load())
	}
	if reservations.reconcileCalls.Load() == 0 {
		t.Fatal("managed cancellation finality did not reconcile reservation")
	}
}

func TestAccountScopeRejectsRetiredOrderMutationsBeforeAnyWrite(t *testing.T) {
	methods := []struct {
		name   string
		status domain.OrderStatus
		call   func(*execution.Service, context.Context, string) (domain.Order, error)
	}{
		{name: "resume", status: domain.OrderStatusReceived, call: (*execution.Service).Resume},
		{name: "refresh", status: domain.OrderStatusUnknown, call: (*execution.Service).Refresh},
		{name: "cancel", status: domain.OrderStatusLive, call: (*execution.Service).Cancel},
		{name: "finalize", status: domain.OrderStatusCancelled, call: (*execution.Service).FinalizeCancellation},
	}
	for _, test := range methods {
		t.Run(test.name, func(t *testing.T) {
			repository := memory.NewOrderRepository()
			venue := &fakeVenue{}
			reservations := &trackingReservations{delegate: paper.NewReservationManager()}
			service, err := execution.New(execution.Params{
				Repository: repository, Venue: venue, Guard: allowGuard{},
				MarketValidator: allowMarketValidator{}, Reservations: reservations,
				AccountScope: fixedAccountScope{
					active:  map[string]struct{}{"main": {}},
					managed: map[string]struct{}{"main": {}, "wallet-6": {}},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			intent := validIntent("retired-" + test.name)
			intent.ExecutionAccountID = "wallet-2"
			now := time.Date(2026, 8, 22, 1, 0, 0, 0, time.UTC)
			order := domain.Order{
				ID: "order-retired-" + test.name, Intent: intent, Status: test.status,
				FilledSize: "0", CreatedAt: now, UpdatedAt: now, Revision: 1,
			}
			if _, created, err := repository.Create(context.Background(), order); err != nil || !created {
				t.Fatalf("Create() = %t, %v", created, err)
			}
			beforeEvents, _ := repository.Events(context.Background(), order.ID)
			_, mutationErr := test.call(service, context.Background(), order.ID)
			want := "EXECUTION_ACCOUNT_NOT_MANAGED"
			if test.name == "resume" {
				want = "EXECUTION_ACCOUNT_NOT_ACTIVE"
			}
			var rejection *port.Rejection
			if !errors.As(mutationErr, &rejection) || rejection.Code != want {
				t.Fatalf("%s error=%v rejection=%#v, want %s", test.name, mutationErr, rejection, want)
			}
			after, _ := repository.Get(context.Background(), order.ID)
			afterEvents, _ := repository.Events(context.Background(), order.ID)
			attempts, _ := repository.Attempts(context.Background(), order.ID)
			if after.Revision != order.Revision || len(afterEvents) != len(beforeEvents) || len(attempts) != 0 ||
				venue.placeCalls.Load() != 0 || venue.getCalls.Load() != 0 || venue.cancelCalls.Load() != 0 ||
				reservations.reserveCalls.Load() != 0 || reservations.reconcileCalls.Load() != 0 || reservations.uncertainCalls.Load() != 0 {
				t.Fatalf("%s mutated retired order: order=%#v events=%d/%d attempts=%d venue=%d/%d/%d reservations=%d/%d/%d",
					test.name, after, len(beforeEvents), len(afterEvents), len(attempts),
					venue.placeCalls.Load(), venue.getCalls.Load(), venue.cancelCalls.Load(),
					reservations.reserveCalls.Load(), reservations.reconcileCalls.Load(), reservations.uncertainCalls.Load())
			}
		})
	}
}

func TestSellOnlyGateBlocksDirectBuyButAllowsSell(t *testing.T) {
	venue := &fakeVenue{}
	service, err := execution.New(execution.Params{
		Repository: memory.NewOrderRepository(), Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: paper.NewReservationManager(),
		EntrySubmissionDisabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	var rejection *port.Rejection
	if _, submitErr := service.Submit(context.Background(), validIntent("blocked-direct-buy")); !errors.As(submitErr, &rejection) || rejection.Code != domain.StrategyEntryBlockSubmissionDisabled {
		t.Fatalf("blocked BUY error = %v, rejection = %#v", submitErr, rejection)
	}
	if venue.placeCalls.Load() != 0 {
		t.Fatalf("blocked BUY reached venue %d times", venue.placeCalls.Load())
	}

	sell := validIntent("allowed-direct-sell")
	sell.Side = domain.SideSell
	sell.TargetLotID = "lot-1"
	sell.WorstPrice = sell.Price
	sell.TimeInForce = domain.TimeInForceFOK
	if _, submitErr := service.Submit(context.Background(), sell); submitErr != nil {
		t.Fatalf("SELL Submit() error = %v", submitErr)
	}
	if venue.placeCalls.Load() != 1 {
		t.Fatalf("SELL venue calls = %d, want 1", venue.placeCalls.Load())
	}
}

func TestAccountEntryGateBlocksMainBuyBeforeWriteAndAllowsWallet6BuyAndMainSell(t *testing.T) {
	repository := memory.NewOrderRepository()
	venue := &fakeVenue{}
	reservations := &trackingReservations{delegate: paper.NewReservationManager()}
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: reservations,
		EntryDisabledAccounts: []string{"main", "wallet-1"},
	})
	if err != nil {
		t.Fatal(err)
	}

	blocked := validIntent("main-account-entry-blocked")
	blocked.ExecutionAccountID = "main"
	var rejection *port.Rejection
	if _, submitErr := service.Submit(context.Background(), blocked); !errors.As(submitErr, &rejection) || rejection.Code != domain.StrategyEntryBlockSubmissionDisabled {
		t.Fatalf("blocked main BUY error=%v rejection=%#v", submitErr, rejection)
	}
	if _, getErr := repository.GetByClientOrderID(context.Background(), blocked.ClientOrderID); !errors.Is(getErr, port.ErrOrderNotFound) {
		t.Fatalf("blocked main BUY wrote order: %v", getErr)
	}
	if venue.placeCalls.Load() != 0 || reservations.reserveCalls.Load() != 0 {
		t.Fatalf("blocked main BUY side effects: venue=%d reserve=%d", venue.placeCalls.Load(), reservations.reserveCalls.Load())
	}

	wallet6Buy := validIntent("wallet6-account-entry-allowed")
	wallet6Buy.ExecutionAccountID = "wallet-6"
	if _, submitErr := service.Submit(context.Background(), wallet6Buy); submitErr != nil {
		t.Fatalf("wallet-6 BUY Submit() error=%v", submitErr)
	}
	mainSell := validIntent("main-account-exit-allowed")
	mainSell.ExecutionAccountID = "main"
	mainSell.Side = domain.SideSell
	mainSell.TargetLotID = "lot-main"
	mainSell.WorstPrice = mainSell.Price
	mainSell.TimeInForce = domain.TimeInForceFOK
	if _, submitErr := service.Submit(context.Background(), mainSell); submitErr != nil {
		t.Fatalf("main SELL Submit() error=%v", submitErr)
	}
	if venue.placeCalls.Load() != 2 {
		t.Fatalf("allowed venue calls=%d, want wallet-6 BUY + main SELL", venue.placeCalls.Load())
	}
}

func TestExecutionRejectsInvalidEntryDisabledAccountList(t *testing.T) {
	for _, accounts := range [][]string{{""}, {"main", " main "}} {
		_, err := execution.New(execution.Params{
			Repository: memory.NewOrderRepository(), Venue: &fakeVenue{}, Guard: allowGuard{},
			MarketValidator: allowMarketValidator{}, Reservations: paper.NewReservationManager(),
			EntryDisabledAccounts: accounts,
		})
		if err == nil {
			t.Fatalf("New() accepted invalid entry-disabled accounts %#v", accounts)
		}
	}
}

func TestLiveAccountEntryGateBlocksMainBuyBeforePrepareOrPlace(t *testing.T) {
	repository := memory.NewOrderRepository()
	venue := &preparedTestVenue{expectedHash: "expected-order", repository: repository}
	reservations := &trackingReservations{delegate: paper.NewReservationManager()}
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: reservations,
		AccountScope: fixedAccountScope{
			active:  map[string]struct{}{"main": {}, "wallet-6": {}},
			managed: map[string]struct{}{"main": {}, "wallet-6": {}},
		},
		EntryDisabledAccounts:    []string{"main", "wallet-1"},
		RequirePreparedPlacement: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	intent := validIntent("live-main-account-entry-blocked")
	intent.ExecutionAccountID = "main"
	if _, submitErr := service.Submit(context.Background(), intent); submitErr == nil {
		t.Fatal("live main BUY unexpectedly passed account entry gate")
	}
	if venue.prepareCalls.Load() != 0 || venue.preparedPlaceCalls.Load() != 0 ||
		venue.legacyPlaceCalls.Load() != 0 || reservations.reserveCalls.Load() != 0 {
		t.Fatalf("blocked live BUY side effects: prepare=%d prepared=%d legacy=%d reserve=%d",
			venue.prepareCalls.Load(), venue.preparedPlaceCalls.Load(), venue.legacyPlaceCalls.Load(), reservations.reserveCalls.Load())
	}
}

func TestSellOnlyGatePreservesIdempotentReplayOfExistingBuy(t *testing.T) {
	repository := memory.NewOrderRepository()
	venue := &fakeVenue{}
	baseParams := execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: paper.NewReservationManager(),
		NewID: func() string { return "existing-buy" },
	}
	initialService, err := execution.New(baseParams)
	if err != nil {
		t.Fatal(err)
	}
	intent := validIntent("existing-buy-client-id")
	created, err := initialService.Submit(context.Background(), intent)
	if err != nil || !created.Created {
		t.Fatalf("initial Submit() = %#v, %v", created, err)
	}

	baseParams.EntrySubmissionDisabled = true
	sellOnlyService, err := execution.New(baseParams)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := sellOnlyService.Submit(context.Background(), intent)
	if err != nil || replayed.Created || replayed.Order.ID != created.Order.ID {
		t.Fatalf("sell-only replay = %#v, %v; want existing order", replayed, err)
	}
	if venue.placeCalls.Load() != 1 {
		t.Fatalf("idempotent replay venue calls = %d, want 1", venue.placeCalls.Load())
	}
}

// TestSubmitPersistsAmbiguousVenueFailureWithoutRetry 验证 Submit Persists Ambiguous Venue Failure Without Retry 场景下的行为。
func TestSubmitPersistsAmbiguousVenueFailureWithoutRetry(t *testing.T) {
	venue := &fakeVenue{placeErr: errors.New("venue timeout")}
	reservations := paper.NewReservationManager()
	service := newServiceWithReservations(t, venue, allowGuard{}, reservations)
	intent := validIntent("client-failed")
	first, err := service.Submit(context.Background(), intent)
	if err == nil || first.Order.Status != domain.OrderStatusUnknown {
		t.Fatalf("first Submit() = %#v, %v; want persisted UNKNOWN", first, err)
	}
	second, err := service.Submit(context.Background(), intent)
	if err != nil || second.Created || second.Order.Status != domain.OrderStatusUnknown {
		t.Fatalf("replay Submit() = %#v, %v; want UNKNOWN order replay", second, err)
	}
	if calls := venue.placeCalls.Load(); calls != 1 {
		t.Fatalf("venue place calls = %d, want no blind retry", calls)
	}
	reservation, ok := reservations.Get(first.Order.ID)
	if !ok || reservation.Status != domain.ReservationStatusReconciliationRequired {
		t.Fatalf("reservation = %#v, %v; want retained reconciliation-required collateral", reservation, ok)
	}
}

func TestPreparedPlacementPersistsExpectedHashBeforeNetworkCall(t *testing.T) {
	repository := memory.NewOrderRepository()
	expectedHash := "0x" + strings.Repeat("ab", 32)
	venue := &preparedTestVenue{expectedHash: expectedHash, repository: repository}
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: paper.NewReservationManager(),
		RequirePreparedPlacement: true,
		AccountScope:             allowAllAccountScope{},
		Now:                      func() time.Time { return time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC) },
		NewID:                    func() string { return "ord-prepared-crash-window" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Submit(context.Background(), validIntent("client-prepared-crash-window"))
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if !venue.observedDurableStart.Load() {
		t.Fatal("PlacePrepared was called before repository contained SUBMITTING plus expected hash")
	}
	if venue.legacyPlaceCalls.Load() != 0 || venue.preparedPlaceCalls.Load() != 1 {
		t.Fatalf("legacy/prepared place calls = %d/%d", venue.legacyPlaceCalls.Load(), venue.preparedPlaceCalls.Load())
	}
	if result.Order.VenueOrderID != expectedHash || result.Order.Status != domain.OrderStatusLive {
		t.Fatalf("submitted order = %#v", result.Order)
	}
}

func TestPreparedPlacementIDMismatchKeepsExpectedHashAndBecomesUnknown(t *testing.T) {
	repository := memory.NewOrderRepository()
	expectedHash := "0x" + strings.Repeat("cd", 32)
	venue := &preparedTestVenue{
		expectedHash: expectedHash, repository: repository,
		placeErr: &port.VenueError{
			Kind: port.VenueErrorAmbiguous, Code: "CLOB_ORDER_ID_MISMATCH",
			Message: "response id differs from signed hash", VenueOrderID: expectedHash,
		},
	}
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: paper.NewReservationManager(),
		RequirePreparedPlacement: true,
		AccountScope:             allowAllAccountScope{},
		Now:                      func() time.Time { return time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC) },
		NewID:                    func() string { return "ord-prepared-mismatch" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, submitErr := service.Submit(context.Background(), validIntent("client-prepared-mismatch"))
	if submitErr == nil || result.Order.Status != domain.OrderStatusUnknown || result.Order.VenueOrderID != expectedHash {
		t.Fatalf("Submit() = %#v, %v; want UNKNOWN retaining expected hash", result, submitErr)
	}
	attempts, err := repository.Attempts(context.Background(), result.Order.ID)
	if err != nil || len(attempts) != 1 || attempts[0].Outcome != domain.AttemptOutcomeUnknown || attempts[0].VenueOrderID != expectedHash {
		t.Fatalf("attempts = %#v, %v", attempts, err)
	}
	second, err := service.Submit(context.Background(), validIntent("client-prepared-mismatch"))
	if err != nil || second.Created || venue.preparedPlaceCalls.Load() != 1 {
		t.Fatalf("replay = %#v, %v, calls=%d; want no second POST", second, err, venue.preparedPlaceCalls.Load())
	}
}

func TestPreparedPlacementPersistenceFailureNeverCallsNetwork(t *testing.T) {
	repository := &failFirstStartRepository{OrderRepository: memory.NewOrderRepository()}
	venue := &preparedTestVenue{
		expectedHash: "0x" + strings.Repeat("ef", 32),
		repository:   repository,
	}
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: paper.NewReservationManager(),
		RequirePreparedPlacement: true,
		AccountScope:             allowAllAccountScope{},
		Now:                      func() time.Time { return time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC) },
		NewID:                    func() string { return "ord-prepared-persist-failure" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, submitErr := service.Submit(context.Background(), validIntent("client-prepared-persist-failure"))
	if submitErr == nil || result.Order.Status != domain.OrderStatusReserved {
		t.Fatalf("Submit() = %#v, %v; want persistence failure before POST", result, submitErr)
	}
	if venue.preparedPlaceCalls.Load() != 0 || venue.legacyPlaceCalls.Load() != 0 {
		t.Fatalf("persistence failure reached venue: prepared=%d legacy=%d", venue.preparedPlaceCalls.Load(), venue.legacyPlaceCalls.Load())
	}
}

func TestCrashAfterPreparedPostReconcilesByPersistedExpectedHash(t *testing.T) {
	repository := &failFirstFinishRepository{OrderRepository: memory.NewOrderRepository()}
	expectedHash := "0x" + strings.Repeat("12", 32)
	venue := &preparedTestVenue{
		expectedHash: expectedHash, repository: repository,
		getOrder: &port.VenueOrder{
			ID: expectedHash, State: port.VenueOrderLive, RawStatus: "live",
			FilledSize: "0", ObservedAt: time.Date(2026, 8, 18, 8, 0, 1, 0, time.UTC),
		},
	}
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: paper.NewReservationManager(),
		RequirePreparedPlacement: true,
		AccountScope:             allowAllAccountScope{},
		Now:                      func() time.Time { return time.Date(2026, 8, 18, 8, 0, 1, 0, time.UTC) },
		NewID:                    func() string { return "ord-prepared-crash-after-post" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, submitErr := service.Submit(context.Background(), validIntent("client-prepared-crash-after-post"))
	if submitErr == nil {
		t.Fatal("Submit() error = nil, want injected acknowledgement persistence failure")
	}
	stored, err := repository.Get(context.Background(), result.Order.ID)
	if err != nil || stored.Status != domain.OrderStatusSubmitting || stored.VenueOrderID != expectedHash {
		t.Fatalf("crash state = %#v, %v", stored, err)
	}
	refreshed, err := service.Refresh(context.Background(), stored.ID)
	if err != nil || refreshed.Status != domain.OrderStatusLive || refreshed.VenueOrderID != expectedHash || venue.getCalls.Load() != 1 {
		t.Fatalf("Refresh() = %#v, %v, get calls=%d", refreshed, err, venue.getCalls.Load())
	}
}

func TestLiveExecutionRejectsVenueWithoutPreparedPlacement(t *testing.T) {
	_, err := execution.New(execution.Params{
		Repository: memory.NewOrderRepository(), Venue: &fakeVenue{}, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: paper.NewReservationManager(),
		RequirePreparedPlacement: true,
	})
	if err == nil || !strings.Contains(err.Error(), "must support prepared placement") {
		t.Fatalf("New() error = %v, want fail-closed live venue rejection", err)
	}
}

func TestLiveExecutionRejectsMissingAccountScope(t *testing.T) {
	repository := memory.NewOrderRepository()
	_, err := execution.New(execution.Params{
		Repository: repository,
		Venue: &preparedTestVenue{
			expectedHash: "0x" + strings.Repeat("34", 32), repository: repository,
		},
		Guard: allowGuard{}, MarketValidator: allowMarketValidator{},
		Reservations: paper.NewReservationManager(), RequirePreparedPlacement: true,
	})
	if err == nil || !strings.Contains(err.Error(), "explicit account scope") {
		t.Fatalf("New() error = %v, want missing live account scope rejection", err)
	}
}

func TestSubmitLocalRiskGateRejectionNeverMarksVenueOutcomeUncertain(t *testing.T) {
	venue := &fakeVenue{}
	repository := &localRejectStartRepository{OrderRepository: memory.NewOrderRepository()}
	paperReservations := paper.NewReservationManager()
	reservations := &trackingReservations{delegate: paperReservations}
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: reservations,
		Now:   func() time.Time { return time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC) },
		NewID: func() string { return "ord-local-risk-reject" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, submitErr := service.Submit(context.Background(), validIntent("client-local-risk-reject"))
	if submitErr == nil || result.Order.Status != domain.OrderStatusRejected || result.Order.FailureCode != "GLOBAL_KILL_SWITCH" {
		t.Fatalf("Submit() = %#v, %v; want local REJECTED", result, submitErr)
	}
	if venue.placeCalls.Load() != 0 || reservations.uncertainCalls.Load() != 0 || reservations.reconcileCalls.Load() != 1 {
		t.Fatalf("place/uncertain/reconcile = %d/%d/%d", venue.placeCalls.Load(), reservations.uncertainCalls.Load(), reservations.reconcileCalls.Load())
	}
	reservation, ok := paperReservations.Get(result.Order.ID)
	if !ok || reservation.Status != domain.ReservationStatusReleased {
		t.Fatalf("reservation = %#v, %v; want released", reservation, ok)
	}
}

// TestSubmitUnknownTriggersAccountReconciliation 验证 Submit Unknown Triggers Account Reconciliation 场景下的行为。
func TestSubmitUnknownTriggersAccountReconciliation(t *testing.T) {
	venue := &fakeVenue{placeErr: errors.New("venue timeout")}
	trigger := &reconciliationTrigger{}
	service, err := execution.New(execution.Params{
		Repository: memory.NewOrderRepository(), Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: paper.NewReservationManager(),
		Reconciliation: trigger,
		Now:            func() time.Time { return time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC) },
		NewID:          func() string { return "ord-reconcile-trigger" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, submitErr := service.Submit(context.Background(), validIntent("client-reconcile-trigger"))
	if submitErr == nil || result.Order.Status != domain.OrderStatusUnknown {
		t.Fatalf("Submit() = %#v, %v; want UNKNOWN", result, submitErr)
	}
	if len(trigger.calls) != 1 || trigger.calls[0].accountID != result.Order.Intent.ExecutionAccountID ||
		trigger.calls[0].orderID != result.Order.ID || trigger.calls[0].trigger != domain.ReconciliationTriggerOrderUnknown {
		t.Fatalf("reconciliation triggers = %#v", trigger.calls)
	}
}

// TestVenueBalanceRejectionTriggersAssetReconciliation 验证 Venue Balance Rejection Triggers Asset Reconciliation 场景下的行为。
func TestVenueBalanceRejectionTriggersAssetReconciliation(t *testing.T) {
	venue := &fakeVenue{placeErr: &port.VenueError{
		Kind: port.VenueErrorRejected, Code: "CLOB_INSUFFICIENT_BALANCE_ALLOWANCE", Message: "balance stale",
	}}
	trigger := &reconciliationTrigger{}
	service, err := execution.New(execution.Params{
		Repository: memory.NewOrderRepository(), Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: paper.NewReservationManager(),
		Reconciliation: trigger,
		Now:            func() time.Time { return time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC) },
		NewID:          func() string { return "ord-balance-trigger" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, submitErr := service.Submit(context.Background(), validIntent("client-balance-trigger"))
	if submitErr == nil || result.Order.Status != domain.OrderStatusRejected {
		t.Fatalf("Submit() = %#v, %v; want REJECTED", result, submitErr)
	}
	if len(trigger.calls) != 1 || trigger.calls[0].trigger != domain.ReconciliationTriggerAssetDrift {
		t.Fatalf("reconciliation triggers = %#v", trigger.calls)
	}
}

// TestSubmitPersistsInvalidVenueResponseAsFailure 验证 Submit Persists Invalid Venue Response As Failure 场景下的行为。
func TestSubmitPersistsInvalidVenueResponseAsFailure(t *testing.T) {
	venue := &fakeVenue{invalidFilledSize: true}
	service := newService(t, venue, allowGuard{})
	intent := validIntent("client-invalid-response")
	result, err := service.Submit(context.Background(), intent)
	if err == nil || result.Order.Status != domain.OrderStatusUnknown || result.Order.FailureCode != "INVALID_VENUE_RESPONSE" {
		t.Fatalf("Submit() = %#v, %v; want invalid venue response UNKNOWN", result, err)
	}
	stored, getErr := service.Get(context.Background(), result.Order.ID)
	if getErr != nil || stored.Status != domain.OrderStatusUnknown {
		t.Fatalf("Get() = %#v, %v; want persisted UNKNOWN", stored, getErr)
	}
}

// TestSubmitConcurrentReplayPlacesOnce 验证 Submit Concurrent Replay Places Once 场景下的行为。
func TestSubmitConcurrentReplayPlacesOnce(t *testing.T) {
	venue := &fakeVenue{}
	service := newService(t, venue, allowGuard{})
	const workers = 20
	results := make(chan execution.SubmitResult, workers)
	errorsChannel := make(chan error, workers)
	var waitGroup sync.WaitGroup
	for index := 0; index < workers; index++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			result, err := service.Submit(context.Background(), validIntent("client-concurrent"))
			results <- result
			errorsChannel <- err
		}()
	}
	waitGroup.Wait()
	close(results)
	close(errorsChannel)
	for err := range errorsChannel {
		if err != nil {
			t.Fatalf("concurrent Submit() error = %v", err)
		}
	}
	orderID := ""
	for result := range results {
		if orderID == "" {
			orderID = result.Order.ID
		}
		if result.Order.ID != orderID {
			t.Fatalf("order ID = %q, want %q", result.Order.ID, orderID)
		}
	}
	if calls := venue.placeCalls.Load(); calls != 1 {
		t.Fatalf("venue place calls = %d, want 1", calls)
	}
}

// TestSubmitPersistsMarketValidationAndDoesNotRevalidateReplay 验证 Submit Persists Market Validation And Does Not Revalidate Replay 场景下的行为。
func TestSubmitPersistsMarketValidationAndDoesNotRevalidateReplay(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	validator := &countingMarketValidator{validation: domain.MarketValidation{
		Mode:        "LIVE_CHECK",
		ValidatedAt: now,
		BestAsk:     "0.51",
		WorstPrice:  "0.52",
	}}
	venue := &fakeVenue{}
	service, err := execution.New(execution.Params{
		Repository:      memory.NewOrderRepository(),
		Venue:           venue,
		Guard:           allowGuard{},
		MarketValidator: validator,
		Reservations:    paper.NewReservationManager(),
		Now:             func() time.Time { return now },
		NewID:           func() string { return "ord-market-validation" },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	intent := validIntent("client-market-validation")
	first, err := service.Submit(context.Background(), intent)
	if err != nil {
		t.Fatalf("first Submit() error = %v", err)
	}
	second, err := service.Submit(context.Background(), intent)
	if err != nil {
		t.Fatalf("second Submit() error = %v", err)
	}
	if validator.calls.Load() != 1 || first.Order.MarketValidation == nil ||
		first.Order.MarketValidation.BestAsk != "0.51" || second.Order.MarketValidation == nil {
		t.Fatalf("validator calls = %d, first/second = %#v / %#v", validator.calls.Load(), first.Order, second.Order)
	}
}

// TestResumeRevalidatesReservedOrderWithoutDuplicateSubmit 验证 Resume Revalidates Reserved Order Without Duplicate Submit 场景下的行为。
func TestResumeRevalidatesReservedOrderWithoutDuplicateSubmit(t *testing.T) {
	baseRepository := memory.NewOrderRepository()
	repository := &failFirstStartRepository{OrderRepository: baseRepository}
	venue := &fakeVenue{}
	reservations := paper.NewReservationManager()
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: reservations,
		Now: func() time.Time { return now }, NewID: func() string { return "ord-resume" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Submit(context.Background(), validIntent("client-resume"))
	if err == nil || result.Order.Status != domain.OrderStatusReserved || venue.placeCalls.Load() != 0 {
		t.Fatalf("Submit() = %#v, %v, calls=%d", result, err, venue.placeCalls.Load())
	}
	resumed, err := service.Resume(context.Background(), result.Order.ID)
	if err != nil || resumed.Status != domain.OrderStatusLive || venue.placeCalls.Load() != 1 {
		t.Fatalf("Resume() = %#v, %v, calls=%d", resumed, err, venue.placeCalls.Load())
	}
}

func TestSellOnlyGateRejectsReservedBuyDuringResumeWithoutVenueCall(t *testing.T) {
	baseRepository := memory.NewOrderRepository()
	repository := &failFirstStartRepository{OrderRepository: baseRepository}
	venue := &fakeVenue{}
	reservations := paper.NewReservationManager()
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	initialService, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: reservations,
		Now: func() time.Time { return now }, NewID: func() string { return "ord-resume-blocked-buy" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, submitErr := initialService.Submit(context.Background(), validIntent("client-resume-blocked-buy"))
	if submitErr == nil || result.Order.Status != domain.OrderStatusReserved || venue.placeCalls.Load() != 0 {
		t.Fatalf("initial Submit() = %#v, %v, calls=%d", result, submitErr, venue.placeCalls.Load())
	}

	sellOnlyService, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: reservations,
		EntrySubmissionDisabled: true,
		Now:                     func() time.Time { return now.Add(time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	resumed, resumeErr := sellOnlyService.Resume(context.Background(), result.Order.ID)
	var rejection *port.Rejection
	if !errors.As(resumeErr, &rejection) || rejection.Code != domain.StrategyEntryBlockSubmissionDisabled ||
		resumed.Status != domain.OrderStatusRejected || venue.placeCalls.Load() != 0 {
		t.Fatalf("Resume() = %#v, %v, rejection=%#v, calls=%d", resumed, resumeErr, rejection, venue.placeCalls.Load())
	}
	reservation, ok := reservations.Get(result.Order.ID)
	if !ok || reservation.Status != domain.ReservationStatusReleased {
		t.Fatalf("reservation = %#v, found=%v; want RELEASED", reservation, ok)
	}
}

func TestAccountEntryGateRejectsReservedMainBuyDuringResumeAndReleasesReservation(t *testing.T) {
	baseRepository := memory.NewOrderRepository()
	repository := &failFirstStartRepository{OrderRepository: baseRepository}
	venue := &fakeVenue{}
	reservations := paper.NewReservationManager()
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	initialService, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: reservations,
		Now: func() time.Time { return now }, NewID: func() string { return "ord-account-entry-resume" },
	})
	if err != nil {
		t.Fatal(err)
	}
	intent := validIntent("client-account-entry-resume")
	intent.ExecutionAccountID = "main"
	result, submitErr := initialService.Submit(context.Background(), intent)
	if submitErr == nil || result.Order.Status != domain.OrderStatusReserved || venue.placeCalls.Load() != 0 {
		t.Fatalf("initial Submit()=%#v error=%v calls=%d", result, submitErr, venue.placeCalls.Load())
	}

	gatedService, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: reservations,
		EntryDisabledAccounts: []string{"main", "wallet-1"},
		Now:                   func() time.Time { return now.Add(time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	resumed, resumeErr := gatedService.Resume(context.Background(), result.Order.ID)
	var rejection *port.Rejection
	if !errors.As(resumeErr, &rejection) || rejection.Code != domain.StrategyEntryBlockSubmissionDisabled ||
		resumed.Status != domain.OrderStatusRejected || venue.placeCalls.Load() != 0 {
		t.Fatalf("Resume()=%#v error=%v rejection=%#v calls=%d", resumed, resumeErr, rejection, venue.placeCalls.Load())
	}
	reservation, ok := reservations.Get(result.Order.ID)
	if !ok || reservation.Status != domain.ReservationStatusReleased {
		t.Fatalf("reservation=%#v found=%v; want RELEASED", reservation, ok)
	}
}

// TestCancelIsIdempotentAfterTerminalState 验证 Cancel Is Idempotent After Terminal State 场景下的行为。
func TestCancelIsIdempotentAfterTerminalState(t *testing.T) {
	venue := &fakeVenue{}
	reservations := paper.NewReservationManager()
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	service, newErr := execution.New(execution.Params{
		Repository: memory.NewOrderRepository(), Venue: venue, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: reservations,
		CancelFillFinalityGrace: 30 * time.Second,
		Now:                     func() time.Time { return now }, NewID: func() string { return "ord-cancel-finality" },
	})
	if newErr != nil {
		t.Fatal(newErr)
	}
	result, err := service.Submit(context.Background(), validIntent("client-cancel"))
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	first, err := service.Cancel(context.Background(), result.Order.ID)
	if err != nil || first.Status != domain.OrderStatusCanceled {
		t.Fatalf("first Cancel() = %#v, %v", first, err)
	}
	second, err := service.Cancel(context.Background(), result.Order.ID)
	if err != nil || second.Status != domain.OrderStatusCanceled {
		t.Fatalf("second Cancel() = %#v, %v", second, err)
	}
	if calls := venue.cancelCalls.Load(); calls != 1 {
		t.Fatalf("venue cancel calls = %d, want 1", calls)
	}
	reservation, ok := reservations.Get(result.Order.ID)
	if !ok || reservation.Status != domain.ReservationStatusReconciliationRequired {
		t.Fatalf("reservation = %#v, %v; want retained through cancel finality", reservation, ok)
	}
	if _, err := service.FinalizeCancellation(context.Background(), result.Order.ID); !errors.Is(err, execution.ErrCancelFinalityPending) {
		t.Fatalf("early FinalizeCancellation() error = %v", err)
	}
	now = now.Add(31 * time.Second)
	if _, err := service.FinalizeCancellation(context.Background(), result.Order.ID); err != nil {
		t.Fatalf("FinalizeCancellation() error = %v", err)
	}
	reservation, ok = reservations.Get(result.Order.ID)
	if !ok || reservation.Status != domain.ReservationStatusReleased {
		t.Fatalf("reservation = %#v, %v; want released after finality", reservation, ok)
	}
}

// TestPaperCancellationCanReleaseImmediately verifies that a synchronous paper
// venue does not leave collateral frozen for a reconciliation job that cannot
// observe any external state.
func TestPaperCancellationCanReleaseImmediately(t *testing.T) {
	venue := &fakeVenue{}
	reservations := paper.NewReservationManager()
	service, err := execution.New(execution.Params{
		Repository:              memory.NewOrderRepository(),
		Venue:                   venue,
		Guard:                   allowGuard{},
		MarketValidator:         allowMarketValidator{},
		Reservations:            reservations,
		ImmediateCancelFinality: true,
		Now:                     func() time.Time { return time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC) },
		NewID:                   func() string { return "ord-paper-cancel" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Submit(context.Background(), validIntent("client-paper-cancel"))
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	canceled, err := service.Cancel(context.Background(), result.Order.ID)
	if err != nil || canceled.Status != domain.OrderStatusCanceled {
		t.Fatalf("Cancel() = %#v, %v", canceled, err)
	}
	reservation, ok := reservations.Get(result.Order.ID)
	if !ok || reservation.Status != domain.ReservationStatusReleased {
		t.Fatalf("reservation = %#v, %v; want immediately released", reservation, ok)
	}
}

// TestAuthoritativeFillsRequireSafeDependencies verifies that live fill
// accounting cannot be enabled without its confirmed-fill pipeline.
func TestAuthoritativeFillsRequireSafeDependencies(t *testing.T) {
	params := execution.Params{
		Repository:         memory.NewOrderRepository(),
		Venue:              &fakeVenue{},
		Guard:              allowGuard{},
		MarketValidator:    allowMarketValidator{},
		Reservations:       paper.NewReservationManager(),
		AuthoritativeFills: true,
	}
	if _, err := execution.New(params); err == nil || !strings.Contains(err.Error(), "fill synchronizer") {
		t.Fatalf("New() error = %v, want missing fill synchronizer", err)
	}
	params.FillSynchronizer = &fakeFillSynchronizer{}
	params.ImmediateCancelFinality = true
	if _, err := execution.New(params); err == nil || !strings.Contains(err.Error(), "immediate cancel finality") {
		t.Fatalf("New() error = %v, want unsafe cancel finality rejection", err)
	}
}

// TestPaperSubmitKeepsLegacyVenueFillAccounting protects the existing paper
// behavior when authoritative fill accounting is not enabled.
func TestPaperSubmitKeepsLegacyVenueFillAccounting(t *testing.T) {
	venueOrder := port.VenueOrder{
		ID: "venue-paper-partial", State: port.VenueOrderPartiallyFilled, RawStatus: "matched",
		FilledSize: "4", AverageFillPrice: "0.5", ObservedAt: time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC),
	}
	reservations := &trackingReservations{delegate: paper.NewReservationManager()}
	service := newServiceWithReservations(t, &fakeVenue{placeOrder: &venueOrder}, allowGuard{}, reservations)
	result, err := service.Submit(context.Background(), validIntent("client-paper-partial"))
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if result.Order.Status != domain.OrderStatusPartiallyFilled || !result.Order.FilledSize.Equal("4") ||
		!result.Order.AverageFillPrice.Equal("0.5") {
		t.Fatalf("Submit() order = %#v, want legacy paper partial fill", result.Order)
	}
	if reservations.reconcileCalls.Load() != 1 {
		t.Fatalf("paper reconcile calls = %d, want 1", reservations.reconcileCalls.Load())
	}
}

// TestSubmitDoesNotBookVenueSizeMatchedWithoutConfirmedFill verifies that a
// synchronous CLOB match is only evidence until FillLedger applies a confirmed
// trade.
func TestSubmitDoesNotBookVenueSizeMatchedWithoutConfirmedFill(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	repository := memory.NewOrderRepository()
	venueOrder := port.VenueOrder{
		ID: "venue-authoritative-pending", State: port.VenueOrderFilled, RawStatus: "matched",
		FilledSize: "10", AverageFillPrice: "0.5", ObservedAt: now,
	}
	venue := &fakeVenue{placeOrder: &venueOrder}
	synchronizer := &fakeFillSynchronizer{}
	reservations := &trackingReservations{delegate: paper.NewReservationManager()}
	trigger := &reconciliationTrigger{}
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{}, MarketValidator: allowMarketValidator{},
		Reservations: reservations, Reconciliation: trigger, FillSynchronizer: synchronizer,
		AuthoritativeFills: true, Now: func() time.Time { return now }, NewID: func() string { return "ord-authoritative-pending" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Submit(context.Background(), validIntent("client-authoritative-pending"))
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if result.Order.Status != domain.OrderStatusUnknown || result.Order.FailureCode != "VENUE_FILL_EVIDENCE_PENDING" ||
		!result.Order.FilledSize.Equal("0") || !result.Order.AverageFillPrice.IsEmpty() {
		t.Fatalf("Submit() order = %#v, want recoverable UNKNOWN with no ledger-confirmed fill", result.Order)
	}
	if synchronizer.calls.Load() != 1 || reservations.reconcileCalls.Load() != 0 || reservations.uncertainCalls.Load() != 1 {
		t.Fatalf("sync/reconcile/uncertain calls = %d/%d/%d, want 1/0/1",
			synchronizer.calls.Load(), reservations.reconcileCalls.Load(), reservations.uncertainCalls.Load())
	}
	if len(trigger.calls) != 1 || trigger.calls[0].trigger != domain.ReconciliationTriggerOrderUnknown {
		t.Fatalf("reconciliation triggers = %#v", trigger.calls)
	}
}

func TestSubmitTreatsNonTerminalTradeEvidenceAsDurablePending(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	repository := memory.NewOrderRepository()
	venueOrder := port.VenueOrder{
		ID: "venue-nonterminal-fill", State: port.VenueOrderFilled, RawStatus: "matched",
		FilledSize: "10", TradeIDs: []string{"trade-pending"}, ObservedAt: now,
	}
	synchronizer := &fakeFillSynchronizer{sync: func(context.Context, string) error {
		return &port.VenueError{
			Kind: port.VenueErrorUnavailable, Code: "CLOB_FILL_DETAILS_UNAVAILABLE",
			Message: "owned trade is not terminal",
		}
	}}
	reservations := &trackingReservations{delegate: paper.NewReservationManager()}
	trigger := &reconciliationTrigger{}
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: &fakeVenue{placeOrder: &venueOrder}, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: reservations, Reconciliation: trigger,
		FillSynchronizer: synchronizer, AuthoritativeFills: true,
		Now: func() time.Time { return now }, NewID: func() string { return "ord-nonterminal-fill" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Submit(context.Background(), validIntent("client-nonterminal-fill"))
	if err != nil {
		t.Fatalf("Submit() error = %v, want asynchronous pending state", err)
	}
	if result.Order.Status != domain.OrderStatusUnknown || result.Order.FailureCode != "VENUE_FILL_EVIDENCE_PENDING" ||
		!result.Order.FilledSize.Equal("0") {
		t.Fatalf("Submit() order = %#v, want durable pending evidence without provisional fill", result.Order)
	}
	if reservations.uncertainCalls.Load() != 1 || len(trigger.calls) != 1 {
		t.Fatalf("uncertain/triggers = %d/%d, want 1/1", reservations.uncertainCalls.Load(), len(trigger.calls))
	}
}

func TestSubmitTradeIDsWithoutEnrichedSizeRemainPendingFillEvidence(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	for _, state := range []port.VenueOrderState{port.VenueOrderLive, port.VenueOrderUnknown} {
		t.Run(string(state), func(t *testing.T) {
			suffix := strings.ToLower(string(state))
			venueOrder := port.VenueOrder{
				ID: "venue-trade-id-pending-" + suffix, State: state, RawStatus: suffix,
				FilledSize: "0", TradeIDs: []string{"trade-before-size-enrichment"}, ObservedAt: now,
			}
			synchronizer := &fakeFillSynchronizer{}
			reservations := &trackingReservations{delegate: paper.NewReservationManager()}
			trigger := &reconciliationTrigger{}
			service, err := execution.New(execution.Params{
				Repository: memory.NewOrderRepository(), Venue: &fakeVenue{placeOrder: &venueOrder},
				Guard: allowGuard{}, MarketValidator: allowMarketValidator{}, Reservations: reservations,
				Reconciliation: trigger, FillSynchronizer: synchronizer, AuthoritativeFills: true,
				Now: func() time.Time { return now }, NewID: func() string { return "ord-trade-id-pending-" + suffix },
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := service.Submit(context.Background(), validIntent("client-trade-id-pending-"+suffix))
			if err != nil {
				t.Fatalf("Submit() error = %v", err)
			}
			if result.Order.Status != domain.OrderStatusUnknown || result.Order.FailureCode != "VENUE_FILL_EVIDENCE_PENDING" ||
				!result.Order.FilledSize.Equal("0") {
				t.Fatalf("Submit() order = %#v, want durable pending fill evidence", result.Order)
			}
			if synchronizer.calls.Load() != 1 || reservations.uncertainCalls.Load() != 1 || len(trigger.calls) != 1 {
				t.Fatalf("sync/uncertain/triggers = %d/%d/%d, want 1/1/1",
					synchronizer.calls.Load(), reservations.uncertainCalls.Load(), len(trigger.calls))
			}
		})
	}
}

// TestSubmitReloadsConfirmedFillLedgerOrder verifies that the order returned by
// Submit is reloaded after the synchronizer commits its authoritative update.
func TestSubmitReloadsConfirmedFillLedgerOrder(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	repository := memory.NewOrderRepository()
	venueOrder := port.VenueOrder{
		ID: "venue-authoritative-confirmed", State: port.VenueOrderPartiallyFilled, RawStatus: "matched",
		FilledSize: "4", AverageFillPrice: "0.5", ObservedAt: now,
	}
	synchronizer := &fakeFillSynchronizer{sync: func(ctx context.Context, orderID string) error {
		return persistConfirmedFill(ctx, repository, orderID, domain.OrderStatusPartiallyFilled, "4", "0.5")
	}}
	reservations := &trackingReservations{delegate: paper.NewReservationManager()}
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: &fakeVenue{placeOrder: &venueOrder}, Guard: allowGuard{},
		MarketValidator: allowMarketValidator{}, Reservations: reservations, FillSynchronizer: synchronizer,
		AuthoritativeFills: true, Now: func() time.Time { return now }, NewID: func() string { return "ord-authoritative-confirmed" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Submit(context.Background(), validIntent("client-authoritative-confirmed"))
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if result.Order.Status != domain.OrderStatusPartiallyFilled || !result.Order.FilledSize.Equal("4") ||
		!result.Order.AverageFillPrice.Equal("0.5") {
		t.Fatalf("Submit() order = %#v, want reloaded confirmed fill", result.Order)
	}
	if synchronizer.calls.Load() != 1 || reservations.reconcileCalls.Load() != 0 {
		t.Fatalf("sync/reconcile calls = %d/%d, want 1/0", synchronizer.calls.Load(), reservations.reconcileCalls.Load())
	}
}

// TestRefreshUsesConfirmedFillLedgerAsAuthority covers the GET-order path and
// prevents its cumulative size_matched value from directly settling assets.
func TestRefreshUsesConfirmedFillLedgerAsAuthority(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	repository := memory.NewOrderRepository()
	venue := &fakeVenue{}
	synchronizer := &fakeFillSynchronizer{sync: func(ctx context.Context, orderID string) error {
		return persistConfirmedFill(ctx, repository, orderID, domain.OrderStatusPartiallyFilled, "3", "0.5")
	}}
	reservations := &trackingReservations{delegate: paper.NewReservationManager()}
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{}, MarketValidator: allowMarketValidator{},
		Reservations: reservations, FillSynchronizer: synchronizer, AuthoritativeFills: true,
		Now: func() time.Time { return now }, NewID: func() string { return "ord-authoritative-refresh" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Submit(context.Background(), validIntent("client-authoritative-refresh"))
	if err != nil {
		t.Fatal(err)
	}
	getOrder := port.VenueOrder{
		ID: result.Order.VenueOrderID, State: port.VenueOrderPartiallyFilled, RawStatus: "matched",
		FilledSize: "3", AverageFillPrice: "0.5", ObservedAt: now,
	}
	venue.getOrder = &getOrder
	refreshed, err := service.Refresh(context.Background(), result.Order.ID)
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if refreshed.Status != domain.OrderStatusPartiallyFilled || !refreshed.FilledSize.Equal("3") {
		t.Fatalf("Refresh() order = %#v, want ledger-confirmed partial fill", refreshed)
	}
	if synchronizer.calls.Load() != 1 || reservations.reconcileCalls.Load() != 0 {
		t.Fatalf("sync/reconcile calls = %d/%d, want 1/0", synchronizer.calls.Load(), reservations.reconcileCalls.Load())
	}
}

func TestRefreshFillEvidencePendingDoesNotSelfTriggerReconciliation(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name    string
		syncErr error
	}{
		{name: "ledger lag"},
		{name: "fill source unavailable", syncErr: errors.New("fill source unavailable")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			suffix := strings.ReplaceAll(testCase.name, " ", "-")
			repository := memory.NewOrderRepository()
			venue := &fakeVenue{}
			trigger := &reconciliationTrigger{}
			synchronizer := &fakeFillSynchronizer{sync: func(context.Context, string) error {
				return testCase.syncErr
			}}
			service, err := execution.New(execution.Params{
				Repository: repository, Venue: venue, Guard: allowGuard{}, MarketValidator: allowMarketValidator{},
				Reservations: paper.NewReservationManager(), Reconciliation: trigger,
				FillSynchronizer: synchronizer, AuthoritativeFills: true,
				Now: func() time.Time { return now }, NewID: func() string { return "ord-refresh-no-self-trigger-" + suffix },
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := service.Submit(context.Background(), validIntent("client-refresh-no-self-trigger-"+suffix))
			if err != nil {
				t.Fatal(err)
			}
			venue.getOrder = &port.VenueOrder{
				ID: result.Order.VenueOrderID, State: port.VenueOrderPartiallyFilled, RawStatus: "matched",
				FilledSize: "4", TradeIDs: []string{"trade-pending"}, ObservedAt: now,
			}
			refreshed, refreshErr := service.Refresh(context.Background(), result.Order.ID)
			if (refreshErr != nil) != (testCase.syncErr != nil) {
				t.Fatalf("Refresh() error = %v, sync error = %v", refreshErr, testCase.syncErr)
			}
			if refreshed.Status != domain.OrderStatusUnknown || refreshed.FailureCode != "VENUE_FILL_EVIDENCE_PENDING" {
				t.Fatalf("Refresh() order = %#v, want durable fill-evidence UNKNOWN", refreshed)
			}
			if len(trigger.calls) != 0 {
				t.Fatalf("reconciliation triggers = %#v, want none from Refresh", trigger.calls)
			}
		})
	}
}

func TestRefreshFillDetailsUnavailableStaysRecoverableWithoutUsingGenericRetryBudget(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	repository := memory.NewOrderRepository()
	venue := &fakeVenue{}
	paperReservations := paper.NewReservationManager()
	reservations := &trackingReservations{delegate: paperReservations}
	trigger := &reconciliationTrigger{}
	synchronizer := &fakeFillSynchronizer{}
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{}, MarketValidator: allowMarketValidator{},
		Reservations: reservations, Reconciliation: trigger, FillSynchronizer: synchronizer,
		AuthoritativeFills: true, MaxReconcileAttempts: 2,
		Now: func() time.Time { return now }, NewID: func() string { return "ord-fill-details-pending" },
	})
	if err != nil {
		t.Fatal(err)
	}
	intent := validIntent("client-fill-details-pending")
	intent.Side = domain.SideSell
	intent.TargetLotID = "lot-fill-details-pending"
	result, err := service.Submit(ctx, intent)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	venue.getErr = &port.VenueError{
		Kind: port.VenueErrorUnavailable, Code: "CLOB_FILL_DETAILS_UNAVAILABLE",
		Message: "order fill was observed but exact trade details are not complete",
	}
	venue.getOrder = &port.VenueOrder{
		ID: result.Order.VenueOrderID, State: port.VenueOrderPartiallyFilled, RawStatus: "matched",
		FilledSize: "4", TradeIDs: []string{"trade-pending"}, ObservedAt: now,
	}
	for attempt := 0; attempt < 4; attempt++ {
		refreshed, refreshErr := service.Refresh(ctx, result.Order.ID)
		if refreshErr != nil || refreshed.Status != domain.OrderStatusUnknown ||
			refreshed.FailureCode != "CLOB_FILL_DETAILS_UNAVAILABLE" {
			t.Fatalf("special Refresh() #%d = %#v, %v; want recoverable UNKNOWN", attempt+1, refreshed, refreshErr)
		}
		if !refreshed.FilledSize.Equal("0") || !refreshed.FilledNotional.Equal("0") {
			t.Fatalf("special Refresh() #%d applied unconfirmed venue amounts: %#v", attempt+1, refreshed)
		}
	}
	if venue.getCalls.Load() != 4 || synchronizer.calls.Load() != 4 {
		t.Fatalf("venue GET/fill sync calls = %d/%d, want 4/4", venue.getCalls.Load(), synchronizer.calls.Load())
	}
	if len(trigger.calls) != 0 {
		t.Fatalf("recursive reconciliation triggers = %#v, want none", trigger.calls)
	}
	reservation, ok := paperReservations.Get(result.Order.ID)
	if !ok || reservation.Status != domain.ReservationStatusReconciliationRequired ||
		!reservation.RemainingReservedShares.Equal(intent.Size) || reservation.UncertainReason != "VENUE_FILL_EVIDENCE_PENDING" {
		t.Fatalf("reservation = %#v, %v; want all shares held for fill evidence", reservation, ok)
	}
	attempts, err := repository.Attempts(ctx, result.Order.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 5 {
		t.Fatalf("attempt count = %d, want submit plus four reconciliations", len(attempts))
	}
	for index, attempt := range attempts[1:] {
		if attempt.Kind != domain.OrderAttemptReconcile || attempt.Outcome != domain.AttemptOutcomeUnknown ||
			attempt.ErrorCode != "CLOB_FILL_DETAILS_UNAVAILABLE" || attempt.VenueStatus != "matched" {
			t.Fatalf("special reconcile attempt #%d = %#v", index+1, attempt)
		}
	}

	// The special attempts above do not consume the ordinary bounded retry
	// budget: two subsequent generic failures still receive the full 2 attempts.
	venue.getErr = errors.New("venue timeout")
	firstGeneric, firstGenericErr := service.Refresh(ctx, result.Order.ID)
	if firstGenericErr == nil || firstGeneric.Status != domain.OrderStatusUnknown || firstGeneric.FailureCode != "RECONCILE_FAILED" {
		t.Fatalf("first generic Refresh() = %#v, %v; want UNKNOWN", firstGeneric, firstGenericErr)
	}
	secondGeneric, secondGenericErr := service.Refresh(ctx, result.Order.ID)
	if secondGenericErr == nil || secondGeneric.Status != domain.OrderStatusManualReview || secondGeneric.FailureCode != "RECONCILE_FAILED" {
		t.Fatalf("second generic Refresh() = %#v, %v; want MANUAL_REVIEW", secondGeneric, secondGenericErr)
	}
	attempts, err = repository.Attempts(ctx, result.Order.ID)
	if err != nil {
		t.Fatal(err)
	}
	if attempts[len(attempts)-2].Outcome != domain.AttemptOutcomeUnknown ||
		attempts[len(attempts)-1].Outcome != domain.AttemptOutcomeFailed {
		t.Fatalf("generic reconcile outcomes = %#v", attempts[len(attempts)-2:])
	}
}

func TestRefreshFillDetailsUnavailableReturnsOrderReloadedFromFillLedger(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	repository := memory.NewOrderRepository()
	venue := &fakeVenue{}
	synchronizer := &fakeFillSynchronizer{sync: func(ctx context.Context, orderID string) error {
		return persistConfirmedFill(ctx, repository, orderID, domain.OrderStatusPartiallyFilled, "4", "0.5")
	}}
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{}, MarketValidator: allowMarketValidator{},
		Reservations: paper.NewReservationManager(), FillSynchronizer: synchronizer, AuthoritativeFills: true,
		MaxReconcileAttempts: 1, Now: func() time.Time { return now },
		NewID: func() string { return "ord-fill-details-recovered" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Submit(ctx, validIntent("client-fill-details-recovered"))
	if err != nil {
		t.Fatal(err)
	}
	venue.getOrder = &port.VenueOrder{
		ID: result.Order.VenueOrderID, State: port.VenueOrderPartiallyFilled, RawStatus: "matched",
		FilledSize: "4", TradeIDs: []string{"trade-recovered"}, ObservedAt: now,
	}
	venue.getErr = &port.VenueError{
		Kind: port.VenueErrorUnavailable, Code: "CLOB_FILL_DETAILS_UNAVAILABLE",
		Message: "order fill was observed but exact trade details are not complete",
	}
	refreshed, refreshErr := service.Refresh(ctx, result.Order.ID)
	if refreshErr != nil || refreshed.Status != domain.OrderStatusPartiallyFilled ||
		!refreshed.FilledSize.Equal("4") || !refreshed.AverageFillPrice.Equal("0.5") {
		t.Fatalf("Refresh() = %#v, %v; want ledger-reloaded partial fill", refreshed, refreshErr)
	}
	if synchronizer.calls.Load() != 1 {
		t.Fatalf("fill sync calls = %d, want 1", synchronizer.calls.Load())
	}
	attempts, err := repository.Attempts(ctx, result.Order.ID)
	if err != nil || len(attempts) != 2 || attempts[1].ErrorCode != "CLOB_FILL_DETAILS_UNAVAILABLE" ||
		attempts[1].Outcome != domain.AttemptOutcomeUnknown {
		t.Fatalf("attempts = %#v, %v; want audited incomplete GET", attempts, err)
	}
}

func TestRefreshFillDetailsUnavailableKeepsRecoveredPartialBehindVenuePending(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	currentTime := now
	repository := memory.NewOrderRepository()
	venue := &fakeVenue{}
	synchronizer := &fakeFillSynchronizer{sync: func(ctx context.Context, orderID string) error {
		err := persistConfirmedFill(ctx, repository, orderID, domain.OrderStatusPartiallyFilled, "2", "0.5")
		currentTime = now.Add(time.Second)
		return err
	}}
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{}, MarketValidator: allowMarketValidator{},
		Reservations: paper.NewReservationManager(), FillSynchronizer: synchronizer, AuthoritativeFills: true,
		MaxReconcileAttempts: 1, Now: func() time.Time { return currentTime },
		NewID: func() string { return "ord-fill-details-still-pending" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Submit(ctx, validIntent("client-fill-details-still-pending"))
	if err != nil {
		t.Fatal(err)
	}
	venue.getOrder = &port.VenueOrder{
		ID: result.Order.VenueOrderID, State: port.VenueOrderPartiallyFilled, RawStatus: "matched",
		FilledSize: "4", TradeIDs: []string{"trade-confirmed", "trade-pending"}, ObservedAt: now,
	}
	venue.getErr = &port.VenueError{
		Kind: port.VenueErrorUnavailable, Code: "CLOB_FILL_DETAILS_UNAVAILABLE",
		Message: "order fill was observed but exact trade details are not complete",
	}
	refreshed, refreshErr := service.Refresh(ctx, result.Order.ID)
	if refreshErr != nil || refreshed.Status != domain.OrderStatusUnknown ||
		refreshed.FailureCode != "VENUE_FILL_EVIDENCE_PENDING" || !refreshed.FilledSize.Equal("2") {
		t.Fatalf("Refresh() = %#v, %v; want ledger partial retained in durable pending state", refreshed, refreshErr)
	}
}

// TestCancelSyncsConfirmedFillsBeforeHoldingFinality covers fills racing a
// successful cancellation and preserves the delayed-release contract.
func TestCancelSyncsConfirmedFillsBeforeHoldingFinality(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	repository := memory.NewOrderRepository()
	venue := &fakeVenue{}
	synchronizer := &fakeFillSynchronizer{sync: func(ctx context.Context, orderID string) error {
		return persistConfirmedFill(ctx, repository, orderID, domain.OrderStatusCancelled, "3", "0.5")
	}}
	reservations := &trackingReservations{delegate: paper.NewReservationManager()}
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: allowGuard{}, MarketValidator: allowMarketValidator{},
		Reservations: reservations, FillSynchronizer: synchronizer, AuthoritativeFills: true,
		Now: func() time.Time { return now }, NewID: func() string { return "ord-authoritative-cancel" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Submit(context.Background(), validIntent("client-authoritative-cancel"))
	if err != nil {
		t.Fatal(err)
	}
	cancelOrder := port.VenueOrder{
		ID: result.Order.VenueOrderID, State: port.VenueOrderCancelled, RawStatus: "cancelled",
		FilledSize: "3", AverageFillPrice: "0.5", ObservedAt: now,
	}
	venue.cancelOrder = &cancelOrder
	cancelled, err := service.Cancel(context.Background(), result.Order.ID)
	if err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if cancelled.Status != domain.OrderStatusCancelled || !cancelled.FilledSize.Equal("3") {
		t.Fatalf("Cancel() order = %#v, want cancelled order with ledger-confirmed fill", cancelled)
	}
	if synchronizer.calls.Load() != 1 || reservations.reconcileCalls.Load() != 0 || reservations.uncertainCalls.Load() != 1 {
		t.Fatalf("sync/reconcile/uncertain calls = %d/%d/%d, want 1/0/1",
			synchronizer.calls.Load(), reservations.reconcileCalls.Load(), reservations.uncertainCalls.Load())
	}
}

// TestAuthoritativeFillsStillReleaseDefinitiveRejection verifies that an order
// rejected before acceptance has no fill race and may release its reservation.
func TestAuthoritativeFillsStillReleaseDefinitiveRejection(t *testing.T) {
	venue := &fakeVenue{placeErr: &port.VenueError{Kind: port.VenueErrorRejected, Code: "CLOB_REJECTED", Message: "rejected"}}
	synchronizer := &fakeFillSynchronizer{}
	reservations := &trackingReservations{delegate: paper.NewReservationManager()}
	service, err := execution.New(execution.Params{
		Repository: memory.NewOrderRepository(), Venue: venue, Guard: allowGuard{}, MarketValidator: allowMarketValidator{},
		Reservations: reservations, FillSynchronizer: synchronizer, AuthoritativeFills: true,
		Now:   func() time.Time { return time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC) },
		NewID: func() string { return "ord-authoritative-rejected" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, submitErr := service.Submit(context.Background(), validIntent("client-authoritative-rejected"))
	if submitErr == nil || result.Order.Status != domain.OrderStatusRejected {
		t.Fatalf("Submit() = %#v, %v; want definitive rejection", result, submitErr)
	}
	if synchronizer.calls.Load() != 0 || reservations.reconcileCalls.Load() != 1 {
		t.Fatalf("sync/reconcile calls = %d/%d, want 0/1", synchronizer.calls.Load(), reservations.reconcileCalls.Load())
	}
}

// TestReservationRejectionNeverCallsVenue 验证 Reservation Rejection Never Calls Venue 场景下的行为。
func TestReservationRejectionNeverCallsVenue(t *testing.T) {
	venue := &fakeVenue{}
	service := newServiceWithReservations(t, venue, allowGuard{}, rejectingReservations{})
	result, err := service.Submit(context.Background(), validIntent("client-no-balance"))
	var rejection *port.Rejection
	if !errors.As(err, &rejection) || rejection.Code != "INSUFFICIENT_AVAILABLE_BALANCE" {
		t.Fatalf("Submit() error = %v, want reservation rejection", err)
	}
	if result.Order.Status != domain.OrderStatusRejected || result.Order.FailureCode != "INSUFFICIENT_AVAILABLE_BALANCE" {
		t.Fatalf("rejected order = %#v", result.Order)
	}
	if venue.placeCalls.Load() != 0 {
		t.Fatal("venue was called after collateral reservation rejection")
	}
}

// TestSubmitRejectsExpiredIntentAndGuardFailure 验证 Submit Rejects Expired Intent And Guard Failure 场景下的行为。
func TestSubmitRejectsExpiredIntentAndGuardFailure(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	repository := memory.NewOrderRepository()
	venue := &fakeVenue{}
	service, err := execution.New(execution.Params{
		Repository:      repository,
		Venue:           venue,
		Guard:           rejectGuard{},
		MarketValidator: allowMarketValidator{},
		Reservations:    paper.NewReservationManager(),
		Now:             func() time.Time { return now },
		NewID:           func() string { return "ord-fixed" },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	expiredAt := now.Add(-time.Second)
	intent := validIntent("client-expired")
	intent.ExpiresAt = &expiredAt
	if _, err := service.Submit(context.Background(), intent); !errors.Is(err, execution.ErrIntentExpired) {
		t.Fatalf("expired Submit() error = %v, want ErrIntentExpired", err)
	}
	intent = validIntent("client-rejected")
	var rejection *port.Rejection
	if _, err := service.Submit(context.Background(), intent); !errors.As(err, &rejection) || rejection.Code != "KILL_SWITCH" {
		t.Fatalf("guard Submit() error = %v, want KILL_SWITCH", err)
	}
	if venue.placeCalls.Load() != 0 {
		t.Fatal("venue was called for rejected intent")
	}
}

// newService 创建测试所需的模拟对象。
func newService(t *testing.T, venue port.Venue, guard port.Guard) *execution.Service {
	return newServiceWithReservations(t, venue, guard, paper.NewReservationManager())
}

// newServiceWithReservations 创建测试所需的模拟对象。
func newServiceWithReservations(t *testing.T, venue port.Venue, guard port.Guard, reservations port.AssetReservationManager) *execution.Service {
	t.Helper()
	var sequence atomic.Int64
	service, err := execution.New(execution.Params{
		Repository:      memory.NewOrderRepository(),
		Venue:           venue,
		Guard:           guard,
		MarketValidator: allowMarketValidator{},
		Reservations:    reservations,
		Now: func() time.Time {
			return time.Date(2026, 8, 18, 8, 0, int(sequence.Load()), 0, time.UTC)
		},
		NewID: func() string { return fmt.Sprintf("ord-%d", sequence.Add(1)) },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return service
}

// rejectingReservations 表示后端使用的 rejectingReservations 类型。
type rejectingReservations struct{}

type successfulReservations struct {
	reserveCalls   atomic.Int64
	reconcileCalls atomic.Int64
	uncertainCalls atomic.Int64
}

func (reservations *successfulReservations) Reserve(context.Context, domain.Order) (domain.AssetReservation, error) {
	reservations.reserveCalls.Add(1)
	return domain.AssetReservation{}, nil
}

func (reservations *successfulReservations) Reconcile(context.Context, domain.Order) (domain.AssetReservation, error) {
	reservations.reconcileCalls.Add(1)
	return domain.AssetReservation{}, nil
}

func (reservations *successfulReservations) MarkUncertain(context.Context, domain.Order, string) error {
	reservations.uncertainCalls.Add(1)
	return nil
}

// trackingReservations records whether execution bypassed the authoritative
// fill ledger and attempted legacy aggregate reconciliation.
type trackingReservations struct {
	delegate       port.AssetReservationManager
	reserveCalls   atomic.Int64
	reconcileCalls atomic.Int64
	uncertainCalls atomic.Int64
}

// Reserve delegates the reservation operation.
func (reservations *trackingReservations) Reserve(ctx context.Context, order domain.Order) (domain.AssetReservation, error) {
	reservations.reserveCalls.Add(1)
	return reservations.delegate.Reserve(ctx, order)
}

// Reconcile records and delegates a legacy aggregate reconciliation.
func (reservations *trackingReservations) Reconcile(ctx context.Context, order domain.Order) (domain.AssetReservation, error) {
	reservations.reconcileCalls.Add(1)
	return reservations.delegate.Reconcile(ctx, order)
}

// MarkUncertain records and delegates retention of an uncertain reservation.
func (reservations *trackingReservations) MarkUncertain(ctx context.Context, order domain.Order, reason string) error {
	reservations.uncertainCalls.Add(1)
	return reservations.delegate.MarkUncertain(ctx, order, reason)
}

// fakeFillSynchronizer simulates a confirmed-fill ledger transaction and lets
// tests independently mutate the repository before execution reloads it.
type fakeFillSynchronizer struct {
	calls atomic.Int64
	sync  func(context.Context, string) error
}

// SyncOrder implements the production fill synchronization boundary.
func (synchronizer *fakeFillSynchronizer) SyncOrder(ctx context.Context, orderID string) (port.FillSyncResult, error) {
	synchronizer.calls.Add(1)
	result := port.FillSyncResult{OrderID: orderID, Observed: 1}
	if synchronizer.sync == nil {
		return result, nil
	}
	if err := synchronizer.sync(ctx, orderID); err != nil {
		return result, err
	}
	result.Applied = 1
	return result, nil
}

// persistConfirmedFill models the order aggregate portion of an atomic
// FillLedger commit; reservation and position effects remain behind that port.
func persistConfirmedFill(ctx context.Context, repository port.OrderRepository, orderID string, target domain.OrderStatus, filledSize, averagePrice domain.Decimal) error {
	order, err := repository.Get(ctx, orderID)
	if err != nil {
		return err
	}
	// Production FillLedger moves UNKNOWN through RECONCILING before applying a
	// confirmed partial fill; mirror that legal state-machine path in the fake.
	if order.Status == domain.OrderStatusUnknown {
		next, event, transitionErr := orderstate.Apply(order, orderstate.Transition{
			EventID: fmt.Sprintf("event:%s:%d", order.ID, order.Revision+1),
			To:      domain.OrderStatusReconciling,
			Trigger: domain.TransitionTriggerFill,
			At:      order.UpdatedAt.Add(time.Nanosecond),
		})
		if transitionErr != nil {
			return transitionErr
		}
		if transitionErr = repository.Transition(ctx, next, event); transitionErr != nil {
			return transitionErr
		}
		order = next
	}
	next, event, err := orderstate.Apply(order, orderstate.Transition{
		EventID:          fmt.Sprintf("event:%s:%d", order.ID, order.Revision+1),
		To:               target,
		Trigger:          domain.TransitionTriggerFill,
		FillKey:          fmt.Sprintf("fill:%s:%d", order.ID, order.Revision+1),
		FilledSize:       filledSize,
		AverageFillPrice: averagePrice,
		At:               order.UpdatedAt.Add(time.Nanosecond),
	})
	if err != nil {
		return err
	}
	return repository.Transition(ctx, next, event)
}

// failFirstStartRepository 表示后端使用的 failFirstStartRepository 类型。
type failFirstStartRepository struct {
	*memory.OrderRepository
	failed atomic.Bool
}

type failFirstFinishRepository struct {
	*memory.OrderRepository
	failed atomic.Bool
}

func (repository *failFirstFinishRepository) FinishAttempt(ctx context.Context, order domain.Order, event domain.OrderEvent, attempt domain.OrderAttempt) error {
	if repository.failed.CompareAndSwap(false, true) {
		return errors.New("simulated crash after venue POST before acknowledgement commit")
	}
	return repository.OrderRepository.FinishAttempt(ctx, order, event, attempt)
}

type localRejectStartRepository struct {
	*memory.OrderRepository
}

func (repository *localRejectStartRepository) StartAttempt(
	context.Context,
	domain.Order,
	domain.OrderEvent,
	domain.OrderAttempt,
) error {
	return &port.Rejection{Code: "GLOBAL_KILL_SWITCH", Reason: "test local database gate"}
}

// StartAttempt 模拟外部尝试开始。
func (repository *failFirstStartRepository) StartAttempt(ctx context.Context, order domain.Order, event domain.OrderEvent, attempt domain.OrderAttempt) error {
	if repository.failed.CompareAndSwap(false, true) {
		return errors.New("simulated crash before submit attempt")
	}
	return repository.OrderRepository.StartAttempt(ctx, order, event, attempt)
}

// Reserve 模拟资产预占。
func (rejectingReservations) Reserve(context.Context, domain.Order) (domain.AssetReservation, error) {
	return domain.AssetReservation{}, &port.Rejection{Code: "INSUFFICIENT_AVAILABLE_BALANCE", Reason: "test balance exhausted"}
}

// Reconcile 模拟资产预占或账户对账。
func (rejectingReservations) Reconcile(context.Context, domain.Order) (domain.AssetReservation, error) {
	return domain.AssetReservation{}, errors.New("unexpected reconcile")
}

// MarkUncertain 记录模拟状态变更。
func (rejectingReservations) MarkUncertain(context.Context, domain.Order, string) error {
	return errors.New("unexpected uncertain state")
}

// validIntent 构建测试使用的合法输入。
func validIntent(clientOrderID string) domain.OrderIntent {
	return domain.OrderIntent{
		ModelID:            "model-1",
		StrategyID:         "strategy-1",
		ExecutionAccountID: "account-model-1-strategy-1",
		SignalID:           "signal-1",
		ClientOrderID:      clientOrderID,
		Venue:              "polymarket-paper",
		MarketID:           "market-1",
		ConditionID:        "condition-1",
		TokenID:            "token-1",
		Side:               domain.SideBuy,
		Type:               domain.OrderTypeLimit,
		Price:              "0.5",
		Size:               "10",
		TimeInForce:        domain.TimeInForceGTC,
	}
}

// allowGuard 表示后端使用的 allowGuard 类型。
type allowGuard struct{}

// Check 模拟检查并返回配置结果。
func (allowGuard) Check(context.Context, domain.OrderIntent) error { return nil }

type allowAllAccountScope struct{}

func (allowAllAccountScope) IsActive(string) bool  { return true }
func (allowAllAccountScope) IsManaged(string) bool { return true }

type fixedAccountScope struct {
	active  map[string]struct{}
	managed map[string]struct{}
}

func (scope fixedAccountScope) IsActive(accountID string) bool {
	_, exists := scope.active[accountID]
	return exists
}

func (scope fixedAccountScope) IsManaged(accountID string) bool {
	_, exists := scope.managed[accountID]
	return exists
}

// allowMarketValidator 表示后端使用的 allowMarketValidator 类型。
type allowMarketValidator struct{}

// Validate 构建测试使用的合法输入。
func (allowMarketValidator) Validate(context.Context, domain.OrderIntent) (domain.MarketValidation, error) {
	return domain.MarketValidation{Mode: "TEST"}, nil
}

// countingMarketValidator 表示后端使用的 countingMarketValidator 类型。
type countingMarketValidator struct {
	calls      atomic.Int64
	validation domain.MarketValidation
}

// Validate 构建测试使用的合法输入。
func (validator *countingMarketValidator) Validate(context.Context, domain.OrderIntent) (domain.MarketValidation, error) {
	validator.calls.Add(1)
	return validator.validation, nil
}

// rejectGuard 表示后端使用的 rejectGuard 类型。
type rejectGuard struct{}

// Check 模拟检查并返回配置结果。
func (rejectGuard) Check(context.Context, domain.OrderIntent) error {
	return &port.Rejection{Code: "KILL_SWITCH", Reason: "trading is disabled"}
}

// reconciliationCall 表示后端使用的 reconciliationCall 类型。
type reconciliationCall struct {
	accountID string
	trigger   domain.ReconciliationTrigger
	orderID   string
}

// reconciliationTrigger 表示后端使用的 reconciliationTrigger 类型。
type reconciliationTrigger struct {
	calls []reconciliationCall
}

// Trigger 实现当前测试场景所需的辅助行为。
func (trigger *reconciliationTrigger) Trigger(accountID string, reason domain.ReconciliationTrigger, orderID string) {
	trigger.calls = append(trigger.calls, reconciliationCall{accountID: accountID, trigger: reason, orderID: orderID})
}

// fakeVenue 表示后端使用的 fakeVenue 类型。
type fakeVenue struct {
	placeCalls        atomic.Int64
	cancelCalls       atomic.Int64
	getCalls          atomic.Int64
	placeErr          error
	getErr            error
	invalidFilledSize bool
	placeOrder        *port.VenueOrder
	cancelOrder       *port.VenueOrder
	getOrder          *port.VenueOrder
}

type preparedTestPlacement struct{ expectedHash string }

func (placement preparedTestPlacement) ExpectedVenueOrderID() string { return placement.expectedHash }

type preparedTestVenue struct {
	expectedHash         string
	repository           port.OrderRepository
	placeErr             error
	getOrder             *port.VenueOrder
	legacyPlaceCalls     atomic.Int64
	prepareCalls         atomic.Int64
	preparedPlaceCalls   atomic.Int64
	observedDurableStart atomic.Bool
	getCalls             atomic.Int64
}

func (*preparedTestVenue) Name() string { return "polymarket-paper" }

func (venue *preparedTestVenue) PreparePlace(context.Context, domain.Order) (port.PreparedPlacement, error) {
	venue.prepareCalls.Add(1)
	return preparedTestPlacement{expectedHash: venue.expectedHash}, nil
}

func (venue *preparedTestVenue) PlacePrepared(ctx context.Context, order domain.Order, placement port.PreparedPlacement) (port.VenueOrder, error) {
	venue.preparedPlaceCalls.Add(1)
	if placement == nil || placement.ExpectedVenueOrderID() != venue.expectedHash || order.VenueOrderID != venue.expectedHash {
		return port.VenueOrder{}, errors.New("prepared placement identity mismatch")
	}
	stored, err := venue.repository.Get(ctx, order.ID)
	if err != nil || stored.Status != domain.OrderStatusSubmitting || stored.VenueOrderID != venue.expectedHash {
		return port.VenueOrder{}, fmt.Errorf("order was not durably prepared before POST: %#v, %w", stored, err)
	}
	attempts, err := venue.repository.Attempts(ctx, order.ID)
	if err != nil || len(attempts) != 1 || attempts[0].Outcome != domain.AttemptOutcomeStarted || attempts[0].VenueOrderID != venue.expectedHash {
		return port.VenueOrder{}, fmt.Errorf("attempt was not durably prepared before POST: %#v, %w", attempts, err)
	}
	venue.observedDurableStart.Store(true)
	if venue.placeErr != nil {
		return port.VenueOrder{}, venue.placeErr
	}
	return port.VenueOrder{
		ID: venue.expectedHash, State: port.VenueOrderLive, RawStatus: "live",
		FilledSize: "0", ObservedAt: order.UpdatedAt,
	}, nil
}

func (venue *preparedTestVenue) Place(context.Context, domain.Order) (port.VenueOrder, error) {
	venue.legacyPlaceCalls.Add(1)
	return port.VenueOrder{}, errors.New("legacy Place must not be used")
}

func (*preparedTestVenue) Cancel(context.Context, domain.Order) (port.VenueOrder, error) {
	return port.VenueOrder{}, nil
}

func (venue *preparedTestVenue) Get(_ context.Context, order domain.Order) (port.VenueOrder, error) {
	venue.getCalls.Add(1)
	if order.VenueOrderID != venue.expectedHash {
		return port.VenueOrder{}, fmt.Errorf("reconciliation used venue id %q, want expected hash %q", order.VenueOrderID, venue.expectedHash)
	}
	if venue.getOrder != nil {
		return *venue.getOrder, nil
	}
	return port.VenueOrder{}, nil
}

// Name 返回模拟组件名称。
func (venue *fakeVenue) Name() string { return "polymarket-paper" }

// Place 模拟交易所下单。
func (venue *fakeVenue) Place(_ context.Context, order domain.Order) (port.VenueOrder, error) {
	venue.placeCalls.Add(1)
	if venue.placeErr != nil {
		return port.VenueOrder{}, venue.placeErr
	}
	if venue.placeOrder != nil {
		return *venue.placeOrder, nil
	}
	filledSize := domain.Decimal("0")
	if venue.invalidFilledSize {
		filledSize = "11"
	}
	return port.VenueOrder{
		ID:         "venue-" + order.ID,
		State:      port.VenueOrderLive,
		RawStatus:  "live",
		FilledSize: filledSize,
		ObservedAt: order.CreatedAt,
	}, nil
}

// Cancel 记录模拟订单撤销。
func (venue *fakeVenue) Cancel(_ context.Context, order domain.Order) (port.VenueOrder, error) {
	venue.cancelCalls.Add(1)
	if venue.cancelOrder != nil {
		return *venue.cancelOrder, nil
	}
	return port.VenueOrder{
		ID:         order.VenueOrderID,
		State:      port.VenueOrderCancelled,
		RawStatus:  "cancelled",
		FilledSize: order.FilledSize,
		ObservedAt: order.UpdatedAt.Add(time.Second),
	}, nil
}

// Get 返回模拟仓储中的测试记录。
func (venue *fakeVenue) Get(_ context.Context, order domain.Order) (port.VenueOrder, error) {
	venue.getCalls.Add(1)
	if venue.getErr != nil {
		if venue.getOrder != nil {
			return *venue.getOrder, venue.getErr
		}
		return port.VenueOrder{}, venue.getErr
	}
	if venue.getOrder != nil {
		return *venue.getOrder, nil
	}
	state := port.VenueOrderLive
	if order.Status == domain.OrderStatusPartiallyFilled {
		state = port.VenueOrderPartiallyFilled
	}
	return port.VenueOrder{
		ID:         order.VenueOrderID,
		State:      state,
		RawStatus:  "live",
		FilledSize: order.FilledSize,
		ObservedAt: order.UpdatedAt,
	}, nil
}
