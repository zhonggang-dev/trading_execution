package execution_test

import (
	"context"
	"errors"
	"fmt"
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

// failFirstStartRepository 表示后端使用的 failFirstStartRepository 类型。
type failFirstStartRepository struct {
	*memory.OrderRepository
	failed atomic.Bool
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
	placeErr          error
	invalidFilledSize bool
}

// Name 返回模拟组件名称。
func (venue *fakeVenue) Name() string { return "polymarket-paper" }

// Place 模拟交易所下单。
func (venue *fakeVenue) Place(_ context.Context, order domain.Order) (port.VenueOrder, error) {
	venue.placeCalls.Add(1)
	if venue.placeErr != nil {
		return port.VenueOrder{}, venue.placeErr
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
