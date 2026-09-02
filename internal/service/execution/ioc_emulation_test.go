package execution_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/adapter/paper"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/execution"
)

// emulatedIOCVenue 模拟没有原生 IOC 的交易所（Polymarket CLOB）。
type emulatedIOCVenue struct{ fakeVenue }

func (venue *emulatedIOCVenue) SupportsTimeInForce(timeInForce domain.TimeInForce) bool {
	return timeInForce != domain.TimeInForceIOC
}

func iocIntent(clientOrderID string) domain.OrderIntent {
	intent := validIntent(clientOrderID)
	intent.Venue = "polymarket"
	intent.WorstPrice = "0.5"
	intent.TimeInForce = domain.TimeInForceIOC
	return intent
}

// TestSubmitCancelsRestingRemainderOfEmulatedIOC 验证交易所没有原生 IOC 时，
// 下单响应返回后立刻撤掉仍在盘口上的剩余部分。
func TestSubmitCancelsRestingRemainderOfEmulatedIOC(t *testing.T) {
	venue := &emulatedIOCVenue{fakeVenue{name: "polymarket"}}
	service := newService(t, venue, allowGuard{})
	result, err := service.Submit(context.Background(), iocIntent("client-ioc-emulated"))
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if result.Order.Status != domain.OrderStatusCancelled {
		t.Fatalf("order status = %s, want CANCELLED after the emulated IOC cancel", result.Order.Status)
	}
	if venue.placeCalls.Load() != 1 || venue.cancelCalls.Load() != 1 {
		t.Fatalf("venue calls place=%d cancel=%d, want one placement and one immediate cancel", venue.placeCalls.Load(), venue.cancelCalls.Load())
	}
}

// TestSubmitLeavesNativeIOCAndFullyMatchedEmulatedIOCAlone 验证原生 IOC 交易所不撤单，
// 完全成交的模拟 IOC 也无需撤单。
func TestSubmitLeavesNativeIOCAndFullyMatchedEmulatedIOCAlone(t *testing.T) {
	// fakeVenue does not declare TimeInForceSupport, so execution treats its
	// IOC as native and leaves the venue to cancel any remainder itself.
	native := &fakeVenue{name: "polymarket"}
	service := newService(t, native, allowGuard{})
	result, err := service.Submit(context.Background(), iocIntent("client-ioc-native"))
	if err != nil || result.Order.Status != domain.OrderStatusLive || native.cancelCalls.Load() != 0 {
		t.Fatalf("native IOC result = %#v, err = %v, cancels = %d", result.Order.Status, err, native.cancelCalls.Load())
	}

	filled := &emulatedIOCVenue{fakeVenue{name: "polymarket", placeOrder: &port.VenueOrder{
		ID: "venue-filled", State: port.VenueOrderFilled, RawStatus: "matched", FilledSize: "10",
		AverageFillPrice: "0.5", ObservedAt: time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC),
	}}}
	service = newService(t, filled, allowGuard{})
	result, err = service.Submit(context.Background(), iocIntent("client-ioc-filled"))
	if err != nil || result.Order.Status != domain.OrderStatusFilled || filled.cancelCalls.Load() != 0 {
		t.Fatalf("fully matched emulated IOC = %s, err = %v, cancels = %d", result.Order.Status, err, filled.cancelCalls.Load())
	}
}

// TestRefreshCompletesEmulatedIOCLeftRestingByCrash 验证进程在下单与撤单之间崩溃后，
// Refresh 会补上模拟 IOC 的撤单。
func TestRefreshCompletesEmulatedIOCLeftRestingByCrash(t *testing.T) {
	repository := memory.NewOrderRepository()
	reservations := paper.NewReservationManager()
	var sequence atomic.Int64
	newWithVenue := func(venue port.Venue) *execution.Service {
		service, err := execution.New(execution.Params{
			Repository: repository, Venue: venue, Guard: allowGuard{}, MarketValidator: allowMarketValidator{},
			Reservations: reservations,
			Now:          func() time.Time { return time.Date(2026, 8, 18, 8, 0, int(sequence.Load()), 0, time.UTC) },
			NewID:        func() string { return fmt.Sprintf("ord-ioc-%d", sequence.Add(1)) },
		})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		return service
	}
	// The "crashed" process: placement persisted LIVE but the emulated cancel
	// never ran. A native venue stands in for that half-finished state.
	crashed := newWithVenue(&fakeVenue{name: "polymarket"})
	placed, err := crashed.Submit(context.Background(), iocIntent("client-ioc-recovered"))
	if err != nil || placed.Order.Status != domain.OrderStatusLive {
		t.Fatalf("Submit() = %s, err = %v", placed.Order.Status, err)
	}

	venue := &emulatedIOCVenue{fakeVenue{name: "polymarket"}}
	recovered := newWithVenue(venue)
	order, err := recovered.Refresh(context.Background(), placed.Order.ID)
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if order.Status != domain.OrderStatusCancelled || venue.cancelCalls.Load() != 1 {
		t.Fatalf("recovered order = %s, cancels = %d, want the resting remainder cancelled", order.Status, venue.cancelCalls.Load())
	}
}
