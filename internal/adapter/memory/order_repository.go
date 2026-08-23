package memory

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// OrderRepository 表示仅适用于本地开发和契约测试的内存仓库，生产实现必须原子持久化幂等声明与订单。
type OrderRepository struct {
	mu              sync.RWMutex
	byID            map[string]domain.Order
	byClientOrderID map[string]string
	events          map[string][]domain.OrderEvent
	attempts        map[string][]domain.OrderAttempt
}

// NewOrderRepository 创建并初始化 Order Repository。
func NewOrderRepository() *OrderRepository {
	return &OrderRepository{
		byID:            make(map[string]domain.Order),
		byClientOrderID: make(map[string]string),
		events:          make(map[string][]domain.OrderEvent),
		attempts:        make(map[string][]domain.OrderAttempt),
	}
}

// Create 幂等创建并持久化新的订单记录。
func (repository *OrderRepository) Create(_ context.Context, order domain.Order) (domain.Order, bool, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if orderID, exists := repository.byClientOrderID[order.Intent.ClientOrderID]; exists {
		return domain.CloneOrder(repository.byID[orderID]), false, nil
	}
	stored := domain.CloneOrder(order)
	repository.byID[stored.ID] = stored
	repository.byClientOrderID[stored.Intent.ClientOrderID] = stored.ID
	repository.events[stored.ID] = []domain.OrderEvent{{
		ID:             "event:" + stored.ID + ":1",
		OrderID:        stored.ID,
		Revision:       stored.Revision,
		ToStatus:       stored.Status,
		Trigger:        domain.TransitionTriggerReceived,
		FilledSize:     stored.FilledSize,
		FilledNotional: stored.FilledNotional,
		TotalFees:      stored.TotalFees,
		OccurredAt:     stored.CreatedAt,
	}}
	return domain.CloneOrder(stored), true, nil
}

// Get 按标识查询并返回当前组件管理的记录。
func (repository *OrderRepository) Get(_ context.Context, orderID string) (domain.Order, error) {
	repository.mu.RLock()
	defer repository.mu.RUnlock()
	order, exists := repository.byID[orderID]
	if !exists {
		return domain.Order{}, port.ErrOrderNotFound
	}
	return domain.CloneOrder(order), nil
}

// GetByClientOrderID 按客户端订单幂等键查询订单。
func (repository *OrderRepository) GetByClientOrderID(_ context.Context, clientOrderID string) (domain.Order, error) {
	repository.mu.RLock()
	defer repository.mu.RUnlock()
	orderID, exists := repository.byClientOrderID[clientOrderID]
	if !exists {
		return domain.Order{}, port.ErrOrderNotFound
	}
	return domain.CloneOrder(repository.byID[orderID]), nil
}

// Transition 以乐观锁原子持久化订单状态和审计事件。
func (repository *OrderRepository) Transition(_ context.Context, order domain.Order, event domain.OrderEvent) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	current, exists := repository.byID[order.ID]
	if !exists {
		return port.ErrOrderNotFound
	}
	if order.Revision != current.Revision+1 {
		return port.ErrOrderRevisionConflict
	}
	if event.OrderID != order.ID || event.Revision != order.Revision ||
		event.FromStatus != current.Status || event.ToStatus != order.Status {
		return port.ErrOrderRevisionConflict
	}
	repository.byID[order.ID] = domain.CloneOrder(order)
	repository.events[order.ID] = append(repository.events[order.ID], event)
	return nil
}

// Update 更新旧版内存订单记录并保留兼容行为。
func (repository *OrderRepository) Update(ctx context.Context, order domain.Order) error {
	current, err := repository.Get(ctx, order.ID)
	if err != nil {
		return err
	}
	return repository.Transition(ctx, order, domain.OrderEvent{
		ID:         "event:" + order.ID + ":legacy",
		OrderID:    order.ID,
		Revision:   order.Revision,
		FromStatus: current.Status,
		ToStatus:   order.Status,
		Trigger:    domain.TransitionTriggerOperator,
		FilledSize: order.FilledSize,
		OccurredAt: order.UpdatedAt,
	})
}

// StartAttempt 原子持久化外部操作开始状态、订单迁移和尝试记录。
func (repository *OrderRepository) StartAttempt(_ context.Context, order domain.Order, event domain.OrderEvent, attempt domain.OrderAttempt) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	current, exists := repository.byID[attempt.OrderID]
	if !exists {
		return port.ErrOrderNotFound
	}
	if order.ID != attempt.OrderID || order.Revision != current.Revision+1 ||
		event.OrderID != order.ID || event.Revision != order.Revision ||
		event.FromStatus != current.Status || event.ToStatus != order.Status {
		return port.ErrOrderRevisionConflict
	}
	for _, existing := range repository.attempts[attempt.OrderID] {
		if existing.ID == attempt.ID || existing.Sequence == attempt.Sequence {
			return port.ErrOrderRevisionConflict
		}
	}
	if attempt.Outcome != domain.AttemptOutcomeStarted || attempt.CompletedAt != nil {
		return port.ErrOrderRevisionConflict
	}
	repository.byID[order.ID] = domain.CloneOrder(order)
	repository.events[order.ID] = append(repository.events[order.ID], event)
	repository.attempts[attempt.OrderID] = append(repository.attempts[attempt.OrderID], attempt)
	return nil
}

// FinishAttempt 原子持久化外部操作结果、订单迁移和尝试记录。
func (repository *OrderRepository) FinishAttempt(_ context.Context, order domain.Order, event domain.OrderEvent, attempt domain.OrderAttempt) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	current, exists := repository.byID[attempt.OrderID]
	if !exists || order.ID != attempt.OrderID || order.Revision != current.Revision+1 ||
		event.OrderID != order.ID || event.Revision != order.Revision ||
		event.FromStatus != current.Status || event.ToStatus != order.Status {
		return port.ErrOrderRevisionConflict
	}
	attempts := repository.attempts[attempt.OrderID]
	for index := range attempts {
		if attempts[index].ID != attempt.ID {
			continue
		}
		if attempts[index].Outcome != domain.AttemptOutcomeStarted || attempt.CompletedAt == nil || attempt.Outcome == domain.AttemptOutcomeStarted {
			return port.ErrOrderRevisionConflict
		}
		attempts[index] = attempt
		repository.byID[order.ID] = domain.CloneOrder(order)
		repository.events[order.ID] = append(repository.events[order.ID], event)
		repository.attempts[attempt.OrderID] = attempts
		return nil
	}
	return port.ErrOrderNotFound
}

// Events 查询指定订单的不可变状态事件。
func (repository *OrderRepository) Events(_ context.Context, orderID string) ([]domain.OrderEvent, error) {
	repository.mu.RLock()
	defer repository.mu.RUnlock()
	if _, exists := repository.byID[orderID]; !exists {
		return nil, port.ErrOrderNotFound
	}
	return append([]domain.OrderEvent(nil), repository.events[orderID]...), nil
}

// Attempts 查询指定订单的外部操作尝试。
func (repository *OrderRepository) Attempts(_ context.Context, orderID string) ([]domain.OrderAttempt, error) {
	repository.mu.RLock()
	defer repository.mu.RUnlock()
	if _, exists := repository.byID[orderID]; !exists {
		return nil, port.ErrOrderNotFound
	}
	return append([]domain.OrderAttempt(nil), repository.attempts[orderID]...), nil
}

// ListPending 分页查询需要协调器继续处理的非终态订单。
func (repository *OrderRepository) ListPending(_ context.Context, before time.Time, limit int) ([]domain.Order, error) {
	return repository.listPending(before, limit, nil), nil
}

// ListPendingForAccounts returns coordinator work only for active accounts.
func (repository *OrderRepository) ListPendingForAccounts(
	_ context.Context,
	executionAccountIDs []string,
	before time.Time,
	limit int,
) ([]domain.Order, error) {
	accounts, err := normalizePendingAccounts(executionAccountIDs)
	if err != nil {
		return nil, err
	}
	return repository.listPending(before, limit, accounts), nil
}

func (repository *OrderRepository) listPending(
	before time.Time,
	limit int,
	accounts map[string]struct{},
) []domain.Order {
	repository.mu.RLock()
	defer repository.mu.RUnlock()
	orders := make([]domain.Order, 0)
	for _, order := range repository.byID {
		if accounts != nil {
			if _, active := accounts[order.Intent.ExecutionAccountID]; !active {
				continue
			}
		}
		if coordinatorEligible(order) && !order.UpdatedAt.After(before) {
			orders = append(orders, domain.CloneOrder(order))
		}
	}
	sort.Slice(orders, func(left, right int) bool {
		if orders[left].UpdatedAt.Equal(orders[right].UpdatedAt) {
			return orders[left].ID < orders[right].ID
		}
		return orders[left].UpdatedAt.Before(orders[right].UpdatedAt)
	})
	if limit > 0 && len(orders) > limit {
		orders = orders[:limit]
	}
	return orders
}

func normalizePendingAccounts(accountIDs []string) (map[string]struct{}, error) {
	if len(accountIDs) == 0 {
		return nil, fmt.Errorf("active execution accounts are required for pending-order selection")
	}
	result := make(map[string]struct{}, len(accountIDs))
	for index, raw := range accountIDs {
		accountID := strings.TrimSpace(raw)
		if accountID == "" {
			return nil, fmt.Errorf("active execution account %d is empty", index)
		}
		if _, duplicate := result[accountID]; duplicate {
			return nil, fmt.Errorf("active execution account %q is duplicated", accountID)
		}
		result[accountID] = struct{}{}
	}
	return result, nil
}

// coordinatorEligible 判断当前业务条件是否成立。等待权威成交明细的 UNKNOWN
// 由低频 scheduled reconciliation 持续处理，避免快速协调器每轮重复刷新同一状态。
func coordinatorEligible(order domain.Order) bool {
	if order.Status == domain.OrderStatusUnknown {
		switch order.FailureCode {
		case "CLOB_FILL_DETAILS_UNAVAILABLE", "VENUE_FILL_EVIDENCE_PENDING":
			return false
		}
	}
	switch order.Status {
	case domain.OrderStatusReceived, domain.OrderStatusValidating, domain.OrderStatusReserved,
		domain.OrderStatusSubmitting, domain.OrderStatusAcknowledged,
		domain.OrderStatusLive, domain.OrderStatusPartiallyFilled,
		domain.OrderStatusUnknown, domain.OrderStatusCancelPending,
		domain.OrderStatusReconciling:
		return true
	default:
		return false
	}
}
