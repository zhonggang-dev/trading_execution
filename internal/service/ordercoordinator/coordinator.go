package ordercoordinator

import (
	"context"
	"errors"
	"fmt"
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
	Now          func() time.Time
}

// Coordinator 表示后端使用的 Coordinator 类型。
type Coordinator struct {
	repository   port.OrderRepository
	execution    Execution
	pollInterval time.Duration
	batchSize    int
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
	return &Coordinator{
		repository: params.Repository, execution: params.Execution,
		pollInterval: params.PollInterval, batchSize: params.BatchSize, now: params.Now,
	}, nil
}

// Sweep 执行一次有界扫描并处理选中的记录。
func (coordinator *Coordinator) Sweep(ctx context.Context) SweepResult {
	now := coordinator.now().UTC()
	orders, err := coordinator.repository.ListPending(ctx, now.Add(-coordinator.pollInterval), coordinator.batchSize)
	if err != nil {
		return SweepResult{Errors: []error{err}}
	}
	result := SweepResult{Selected: len(orders)}
	for _, order := range orders {
		if err := ctx.Err(); err != nil {
			result.Errors = append(result.Errors, err)
			break
		}
		if cancelDue(order, now) {
			if _, err := coordinator.execution.Cancel(ctx, order.ID); err != nil {
				result.Errors = append(result.Errors, fmt.Errorf("cancel expired order %s: %w", order.ID, err))
			} else {
				result.Cancelled++
			}
			continue
		}
		switch order.Status {
		case domain.OrderStatusReceived, domain.OrderStatusValidating, domain.OrderStatusReserved:
			if _, err := coordinator.execution.Resume(ctx, order.ID); err != nil {
				if !errors.Is(err, port.ErrOrderRevisionConflict) {
					result.Errors = append(result.Errors, fmt.Errorf("resume order %s: %w", order.ID, err))
				}
			} else {
				result.Resumed++
			}
		case domain.OrderStatusSubmitting, domain.OrderStatusAcknowledged,
			domain.OrderStatusLive, domain.OrderStatusPartiallyFilled,
			domain.OrderStatusUnknown, domain.OrderStatusCancelPending,
			domain.OrderStatusReconciling:
			if _, err := coordinator.execution.Refresh(ctx, order.ID); err != nil {
				if !errors.Is(err, port.ErrOrderRevisionConflict) {
					result.Errors = append(result.Errors, fmt.Errorf("refresh order %s: %w", order.ID, err))
				}
			} else {
				result.Refreshed++
			}
		}
	}
	return result
}

// cancelDue 撤销并处理 Due。
func cancelDue(order domain.Order, now time.Time) bool {
	if order.Intent.ExpiresAt == nil || order.Intent.ExpiresAt.After(now) {
		return false
	}
	return order.Status == domain.OrderStatusAcknowledged || order.Status == domain.OrderStatusLive || order.Status == domain.OrderStatusPartiallyFilled
}
