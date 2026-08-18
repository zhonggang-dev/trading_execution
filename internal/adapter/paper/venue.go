package paper

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// Venue implements deterministic paper execution. It never performs network
// calls and therefore cannot place a real order.
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
		return port.VenueOrder{}, fmt.Errorf("paper venue order %q not found", order.VenueOrderID)
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
	defer venue.mu.RUnlock()
	venueOrder, exists := venue.orders[order.VenueOrderID]
	if !exists {
		return port.VenueOrder{}, fmt.Errorf("paper venue order %q not found", order.VenueOrderID)
	}
	return venueOrder, nil
}
