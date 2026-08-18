package paper

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// Venue 实现确定性的模拟交易，不发起网络请求，因此不会提交真实订单。
type Venue struct {
	name   string
	mu     sync.RWMutex
	orders map[string]port.VenueOrder
	now    func() time.Time
}

// NewVenue 创建并初始化 Venue。
func NewVenue(name string) *Venue {
	return &Venue{
		name:   name,
		orders: make(map[string]port.VenueOrder),
		now:    time.Now,
	}
}

// Name 返回当前交易场所适配器名称。
func (venue *Venue) Name() string {
	return venue.name
}

// Place 将已校验订单提交到当前交易场所。
func (venue *Venue) Place(_ context.Context, order domain.Order) (port.VenueOrder, error) {
	venue.mu.Lock()
	defer venue.mu.Unlock()
	venueOrder := port.VenueOrder{
		ID:         "paper-" + order.ID,
		State:      port.VenueOrderLive,
		RawStatus:  "paper-live",
		FilledSize: domain.Decimal("0"),
		ObservedAt: venue.now().UTC(),
	}
	venue.orders[venueOrder.ID] = venueOrder
	return venueOrder, nil
}

// Cancel 向当前交易场所撤销指定订单。
func (venue *Venue) Cancel(_ context.Context, order domain.Order) (port.VenueOrder, error) {
	venue.mu.Lock()
	defer venue.mu.Unlock()
	venueOrder, exists := venue.orders[order.VenueOrderID]
	if !exists {
		var err error
		venueOrder, err = venue.restore(order)
		if err != nil {
			return port.VenueOrder{}, err
		}
	}
	if venueOrder.State != port.VenueOrderFilled {
		venueOrder.State = port.VenueOrderCancelled
		venueOrder.RawStatus = "paper-cancelled"
	}
	venueOrder.ObservedAt = venue.now().UTC()
	venue.orders[venueOrder.ID] = venueOrder
	return venueOrder, nil
}

// Get 按标识查询并返回当前组件管理的记录。
func (venue *Venue) Get(_ context.Context, order domain.Order) (port.VenueOrder, error) {
	venue.mu.RLock()
	venueOrder, exists := venue.orders[order.VenueOrderID]
	venue.mu.RUnlock()
	if !exists {
		return venue.restore(order)
	}
	return venueOrder, nil
}

// restore reconstructs a deterministic paper venue observation from the
// durable order. Paper orders have no remote venue state, so this makes
// refresh and cancel safe across process restarts without pretending to be a
// live exchange adapter.
func (venue *Venue) restore(order domain.Order) (port.VenueOrder, error) {
	expectedID := "paper-" + order.ID
	if order.ID == "" || order.VenueOrderID != expectedID {
		return port.VenueOrder{}, fmt.Errorf("paper venue order %q not found", order.VenueOrderID)
	}
	state := port.VenueOrderLive
	rawStatus := "paper-live"
	switch order.Status {
	case domain.OrderStatusFilled:
		state, rawStatus = port.VenueOrderFilled, "paper-filled"
	case domain.OrderStatusPartiallyFilled:
		state, rawStatus = port.VenueOrderPartiallyFilled, "paper-partially-filled"
	case domain.OrderStatusCancelled:
		state, rawStatus = port.VenueOrderCancelled, "paper-cancelled"
	case domain.OrderStatusRejected:
		state, rawStatus = port.VenueOrderRejected, "paper-rejected"
	}
	return port.VenueOrder{
		ID: expectedID, State: state, RawStatus: rawStatus,
		FilledSize: order.FilledSize, AverageFillPrice: order.AverageFillPrice,
		ObservedAt: venue.now().UTC(),
	}, nil
}
