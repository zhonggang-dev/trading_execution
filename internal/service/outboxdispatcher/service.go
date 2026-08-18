package outboxdispatcher

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/port"
)

// Params 表示后端使用的 Params 类型。
type Params struct {
	Outbox      port.OutboxRepository
	Publisher   port.EventPublisher
	Now         func() time.Time
	BatchSize   int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

// Service 表示后端使用的 Service 类型。
type Service struct {
	outbox      port.OutboxRepository
	publisher   port.EventPublisher
	now         func() time.Time
	batchSize   int
	baseBackoff time.Duration
	maxBackoff  time.Duration
}

// Result 表示后端使用的 Result 类型。
type Result struct {
	Claimed   int `json:"claimed"`
	Published int `json:"published"`
	Failed    int `json:"failed"`
}

// New 校验依赖和配置后创建当前服务实例。
func New(params Params) (*Service, error) {
	if params.Outbox == nil || params.Publisher == nil {
		return nil, fmt.Errorf("outbox repository and event publisher are required")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	if params.BatchSize == 0 {
		params.BatchSize = 100
	}
	if params.BatchSize < 1 || params.BatchSize > 1000 {
		return nil, fmt.Errorf("outbox batch size must be between 1 and 1000")
	}
	if params.BaseBackoff == 0 {
		params.BaseBackoff = time.Second
	}
	if params.MaxBackoff == 0 {
		params.MaxBackoff = 5 * time.Minute
	}
	if params.BaseBackoff <= 0 || params.MaxBackoff < params.BaseBackoff {
		return nil, fmt.Errorf("invalid outbox retry backoff")
	}
	return &Service{
		outbox: params.Outbox, publisher: params.Publisher, now: params.Now,
		batchSize: params.BatchSize, baseBackoff: params.BaseBackoff, maxBackoff: params.MaxBackoff,
	}, nil
}

// RunOnce 认领并发布一批 Outbox 事件并安排失败重试。
func (service *Service) RunOnce(ctx context.Context) (Result, error) {
	now := service.now().UTC()
	events, err := service.outbox.Claim(ctx, service.batchSize, now)
	if err != nil {
		return Result{}, err
	}
	result := Result{Claimed: len(events)}
	var failures []error
	for _, event := range events {
		if err := service.publisher.Publish(ctx, event.Topic, event.EventKey, event.Payload); err != nil {
			result.Failed++
			next := service.now().UTC().Add(service.backoff(event.Attempts))
			if scheduleErr := service.outbox.MarkFailed(ctx, event.ID, next); scheduleErr != nil {
				failures = append(failures, fmt.Errorf("publish %s: %v; reschedule: %w", event.ID, err, scheduleErr))
			} else {
				failures = append(failures, fmt.Errorf("publish %s: %w", event.ID, err))
			}
			continue
		}
		if err := service.outbox.MarkPublished(ctx, event.ID, service.now().UTC()); err != nil {
			result.Failed++
			failures = append(failures, fmt.Errorf("acknowledge published event %s: %w", event.ID, err))
			continue
		}
		result.Published++
	}
	return result, errors.Join(failures...)
}

// backoff 根据失败次数计算有上限的指数退避时长。
func (service *Service) backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	value := service.baseBackoff
	for step := 1; step < attempt && value < service.maxBackoff; step++ {
		if value > service.maxBackoff/2 {
			return service.maxBackoff
		}
		value *= 2
	}
	if value > service.maxBackoff {
		return service.maxBackoff
	}
	return value
}
