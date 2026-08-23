package ordercoordinator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// Execution 表示后端使用的 Execution 类型。
type Execution interface {
	Resume(context.Context, string) (domain.Order, error)
	Refresh(context.Context, string) (domain.Order, error)
	Cancel(context.Context, string) (domain.Order, error)
}

// Params 表示后端使用的 Params 类型。
type Params struct {
	Repository   port.OrderRepository
	Execution    Execution
	PollInterval time.Duration
	BatchSize    int
	Accounts     []string
	Now          func() time.Time
}

// Coordinator 表示后端使用的 Coordinator 类型。
type Coordinator struct {
	repository   port.OrderRepository
	execution    Execution
	pollInterval time.Duration
	batchSize    int
	accounts     []string
	now          func() time.Time
}

// SweepResult 表示后端使用的 SweepResult 类型。
type SweepResult struct {
	Selected  int
	Resumed   int
	Refreshed int
	Cancelled int
	Errors    []error
}

// sweepAction 表示订单协调器成功完成的单次动作。
type sweepAction uint8

const (
	sweepActionNone sweepAction = iota
	sweepActionResume
	sweepActionRefresh
	sweepActionCancel
)

// New 校验依赖和配置后创建当前服务实例。
func New(params Params) (*Coordinator, error) {
	if params.Repository == nil || params.Execution == nil {
		return nil, fmt.Errorf("order repository and execution service are required")
	}
	if params.PollInterval == 0 {
		params.PollInterval = 2 * time.Second
	}
	if params.PollInterval < 100*time.Millisecond || params.PollInterval > time.Minute {
		return nil, fmt.Errorf("order poll interval must be between 100ms and 1m")
	}
	if params.BatchSize == 0 {
		params.BatchSize = 100
	}
	if params.BatchSize < 1 || params.BatchSize > 1000 {
		return nil, fmt.Errorf("order coordinator batch size must be between 1 and 1000")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	accounts, err := normalizeAccounts(params.Accounts)
	if err != nil {
		return nil, err
	}
	return &Coordinator{
		repository: params.Repository, execution: params.Execution,
		pollInterval: params.PollInterval, batchSize: params.BatchSize,
		accounts: accounts, now: params.Now,
	}, nil
}

// Sweep 执行一次有界扫描并处理选中的记录。
func (coordinator *Coordinator) Sweep(ctx context.Context) SweepResult {
	now := coordinator.now().UTC()
	var orders []domain.Order
	var err error
	if len(coordinator.accounts) == 0 {
		orders, err = coordinator.repository.ListPending(
			ctx, now.Add(-coordinator.pollInterval), coordinator.batchSize,
		)
	} else {
		orders, err = coordinator.repository.ListPendingForAccounts(
			ctx, coordinator.accounts, now.Add(-coordinator.pollInterval), coordinator.batchSize,
		)
	}
	if err != nil {
		return SweepResult{Errors: []error{err}}
	}
	result := SweepResult{Selected: len(orders)}
	for _, order := range orders {
		if err := ctx.Err(); err != nil {
			result.Errors = append(result.Errors, err)
			break
		}
		action, err := coordinator.processOrder(ctx, order, now)
		if err != nil {
			result.Errors = append(result.Errors, err)
			continue
		}
		result.record(action)
	}
	return result
}

func normalizeAccounts(accountIDs []string) ([]string, error) {
	result := make([]string, 0, len(accountIDs))
	seen := make(map[string]struct{}, len(accountIDs))
	for index, raw := range accountIDs {
		accountID := strings.TrimSpace(raw)
		if accountID == "" {
			return nil, fmt.Errorf("order coordinator active account %d is empty", index)
		}
		if _, duplicate := seen[accountID]; duplicate {
			return nil, fmt.Errorf("order coordinator active account %q is duplicated", accountID)
		}
		seen[accountID] = struct{}{}
		result = append(result, accountID)
	}
	sort.Strings(result)
	return result, nil
}

// processOrder 根据订单状态选择并执行一次恢复、刷新或撤单动作。
func (coordinator *Coordinator) processOrder(ctx context.Context, order domain.Order, now time.Time) (sweepAction, error) {
	if cancelDue(order, now) {
		return coordinator.cancelOrder(ctx, order)
	}
	switch order.Status {
	case domain.OrderStatusReceived, domain.OrderStatusValidating, domain.OrderStatusReserved:
		return coordinator.resumeOrder(ctx, order)
	case domain.OrderStatusSubmitting, domain.OrderStatusAcknowledged,
		domain.OrderStatusLive, domain.OrderStatusPartiallyFilled,
		domain.OrderStatusUnknown, domain.OrderStatusCancelPending,
		domain.OrderStatusReconciling:
		return coordinator.refreshOrder(ctx, order)
	default:
		return sweepActionNone, nil
	}
}

// cancelOrder 撤销一张已经过期的活动订单。
func (coordinator *Coordinator) cancelOrder(ctx context.Context, order domain.Order) (sweepAction, error) {
	if _, err := coordinator.execution.Cancel(ctx, order.ID); err != nil {
		return sweepActionNone, fmt.Errorf("cancel expired order %s: %w", order.ID, err)
	}
	return sweepActionCancel, nil
}

// resumeOrder 恢复一张尚未提交到交易所的中断订单。
func (coordinator *Coordinator) resumeOrder(ctx context.Context, order domain.Order) (sweepAction, error) {
	if _, err := coordinator.execution.Resume(ctx, order.ID); err != nil {
		return sweepActionNone, ignoreRevisionConflict("resume", order.ID, err)
	}
	return sweepActionResume, nil
}

// refreshOrder 刷新一张已经进入交易所生命周期的订单。
func (coordinator *Coordinator) refreshOrder(ctx context.Context, order domain.Order) (sweepAction, error) {
	if _, err := coordinator.execution.Refresh(ctx, order.ID); err != nil {
		return sweepActionNone, ignoreRevisionConflict("refresh", order.ID, err)
	}
	return sweepActionRefresh, nil
}

// ignoreRevisionConflict 忽略其他并发工作者已经完成的乐观锁竞争。
func ignoreRevisionConflict(operation, orderID string, err error) error {
	if errors.Is(err, port.ErrOrderRevisionConflict) {
		return nil
	}
	return fmt.Errorf("%s order %s: %w", operation, orderID, err)
}

// record 累加一次成功完成的协调器动作。
func (result *SweepResult) record(action sweepAction) {
	switch action {
	case sweepActionResume:
		result.Resumed++
	case sweepActionRefresh:
		result.Refreshed++
	case sweepActionCancel:
		result.Cancelled++
	}
}

// cancelDue 撤销并处理 Due。
func cancelDue(order domain.Order, now time.Time) bool {
	if order.Intent.ExpiresAt == nil || order.Intent.ExpiresAt.After(now) {
		return false
	}
	return order.Status == domain.OrderStatusAcknowledged || order.Status == domain.OrderStatusLive || order.Status == domain.OrderStatusPartiallyFilled
}
