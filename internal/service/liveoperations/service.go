package liveoperations

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// ErrSnapshotUnavailable 表示当前没有足够新鲜且包含核心资金数据的可信快照。
var ErrSnapshotUnavailable = errors.New("live operations snapshot unavailable")

// Params 表示实盘运维后台聚合器的依赖和刷新策略。
type Params struct {
	Repository     port.LiveOperationsRepository
	Venue          port.VenueReconciliationSource
	PositionSource port.ExternalPositionSource
	BalanceSource  port.ExternalBalanceSource
	OrderBooks     port.OrderBookSource
	Accounts       []string
	VenueName      string
	StartedAt      time.Time
	RunID          string
	Interval       time.Duration
	RefreshTimeout time.Duration
	MaxSnapshotAge time.Duration
	EventLimit     int
	Now            func() time.Time
	Logger         *slog.Logger
}

// Service 在后台聚合外部事实与本地账本，并为 HTTP 层提供原子只读快照。
type Service struct {
	repository     port.LiveOperationsRepository
	venue          port.VenueReconciliationSource
	positionSource port.ExternalPositionSource
	balanceSource  port.ExternalBalanceSource
	orderBooks     port.OrderBookSource
	accounts       []string
	venueName      string
	startedAt      time.Time
	runID          string
	interval       time.Duration
	refreshTimeout time.Duration
	maxAge         time.Duration
	eventLimit     int
	now            func() time.Time
	logger         *slog.Logger

	cache            atomic.Pointer[cacheEntry]
	errMu            sync.RWMutex
	lastRefreshError error
}

// cacheEntry 保存一次成功生成后不可变的运维快照。
type cacheEntry struct {
	snapshot domain.LiveOperationsSnapshot
}

// New 校验依赖并创建后台实盘运维聚合器。
func New(params Params) (*Service, error) {
	if params.Repository == nil || params.Venue == nil || params.PositionSource == nil || params.BalanceSource == nil || params.OrderBooks == nil {
		return nil, fmt.Errorf("live operations repository, venue, position, balance, and orderbook sources are required")
	}
	accounts, err := normalizeAccounts(params.Accounts)
	if err != nil {
		return nil, err
	}
	params.VenueName = strings.TrimSpace(params.VenueName)
	params.RunID = strings.TrimSpace(params.RunID)
	if params.VenueName == "" || params.RunID == "" {
		return nil, fmt.Errorf("live operations venue name and run id are required")
	}
	if params.StartedAt.IsZero() {
		return nil, fmt.Errorf("live operations started_at is required")
	}
	if params.Interval == 0 {
		params.Interval = 10 * time.Second
	}
	if params.Interval < time.Second || params.Interval > time.Minute {
		return nil, fmt.Errorf("live operations interval must be between one second and one minute")
	}
	if params.RefreshTimeout == 0 {
		params.RefreshTimeout = 8 * time.Second
	}
	if params.RefreshTimeout <= 0 || params.RefreshTimeout >= params.Interval {
		return nil, fmt.Errorf("live operations refresh timeout must be positive and less than interval")
	}
	if params.MaxSnapshotAge == 0 {
		params.MaxSnapshotAge = params.Interval * 3
	}
	if params.MaxSnapshotAge <= params.Interval || params.MaxSnapshotAge > 10*time.Minute {
		return nil, fmt.Errorf("live operations max snapshot age must be greater than interval and at most ten minutes")
	}
	if params.EventLimit == 0 {
		params.EventLimit = 50
	}
	if params.EventLimit < 1 || params.EventLimit > 200 {
		return nil, fmt.Errorf("live operations event limit must be between 1 and 200")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &Service{
		repository: params.Repository, venue: params.Venue,
		positionSource: params.PositionSource, balanceSource: params.BalanceSource,
		orderBooks: params.OrderBooks, accounts: accounts, venueName: params.VenueName,
		startedAt: params.StartedAt.UTC(), runID: params.RunID,
		interval: params.Interval, refreshTimeout: params.RefreshTimeout,
		maxAge: params.MaxSnapshotAge, eventLimit: params.EventLimit, now: params.Now,
		logger: params.Logger,
	}, nil
}

// Refresh 立即执行一次聚合；只有完整核心资金数据才能替换上一份成功快照。
func (service *Service) Refresh(ctx context.Context) error {
	snapshot, err := service.build(ctx)
	if err != nil {
		service.setRefreshError(err)
		if service.logger != nil {
			service.logger.Warn("live operations snapshot refresh failed", "error", err)
		}
		return err
	}
	service.cache.Store(&cacheEntry{snapshot: snapshot})
	service.setRefreshError(nil)
	return nil
}

// Run 按固定周期更新快照，单次失败保留上一份成功结果并记录降级原因。
func (service *Service) Run(ctx context.Context) error {
	if service.cache.Load() == nil {
		refreshContext, cancel := context.WithTimeout(ctx, service.refreshTimeout)
		err := service.Refresh(refreshContext)
		cancel()
		if err != nil {
			return err
		}
	}
	ticker := time.NewTicker(service.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			refreshContext, cancel := context.WithTimeout(ctx, service.refreshTimeout)
			_ = service.Refresh(refreshContext)
			cancel()
		}
	}
}

// Snapshot 返回缓存快照；过期或从未成功生成时拒绝用零值伪造资金数据。
func (service *Service) Snapshot(ctx context.Context) (domain.LiveOperationsSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return domain.LiveOperationsSnapshot{}, err
	}
	entry := service.cache.Load()
	if entry == nil {
		return domain.LiveOperationsSnapshot{}, fmt.Errorf("%w: initial refresh has not completed", ErrSnapshotUnavailable)
	}
	now := service.now().UTC()
	if entry.snapshot.SourceObservedAt.IsZero() || entry.snapshot.SourceObservedAt.After(now) {
		return domain.LiveOperationsSnapshot{}, fmt.Errorf("%w: source observation time is invalid", ErrSnapshotUnavailable)
	}
	age := now.Sub(entry.snapshot.SourceObservedAt)
	if age > service.maxAge {
		return domain.LiveOperationsSnapshot{}, fmt.Errorf("%w: cached snapshot is stale by %s", ErrSnapshotUnavailable, age)
	}
	cloned, err := domain.CloneLiveOperationsSnapshot(entry.snapshot)
	if err != nil {
		return domain.LiveOperationsSnapshot{}, fmt.Errorf("clone live operations snapshot: %w", err)
	}
	cloned.DataFreshnessSeconds = int64(age / time.Second)
	if refreshErr := service.refreshError(); refreshErr != nil {
		cloned.Engine.Health = worstHealth(cloned.Engine.Health, domain.LiveHealthDegraded)
		cloned.DataQuality = append(cloned.DataQuality, domain.LiveDataQuality{
			ID: "aggregator", Name: "后台快照聚合", Status: domain.LiveHealthDegraded,
			Detail: "最近一次刷新失败，当前返回上一份成功快照；原始错误仅记录在服务日志中",
		})
	}
	return cloned, nil
}

// setRefreshError 并发安全地保存最近一次后台刷新错误。
func (service *Service) setRefreshError(err error) {
	service.errMu.Lock()
	defer service.errMu.Unlock()
	service.lastRefreshError = err
}

// refreshError 返回最近一次后台刷新错误。
func (service *Service) refreshError() error {
	service.errMu.RLock()
	defer service.errMu.RUnlock()
	return service.lastRefreshError
}

// normalizeAccounts 规范化并去重配置的执行账户。
func normalizeAccounts(values []string) ([]string, error) {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" {
			return nil, fmt.Errorf("live operations execution account id is empty")
		}
		if _, duplicate := seen[value]; duplicate {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("at least one live operations execution account is required")
	}
	return result, nil
}

// worstHealth 返回两个健康等级中更严重的一个。
func worstHealth(left domain.LiveHealth, right domain.LiveHealth) domain.LiveHealth {
	severity := map[domain.LiveHealth]int{domain.LiveHealthHealthy: 0, domain.LiveHealthDegraded: 1, domain.LiveHealthStopped: 2}
	if severity[right] > severity[left] {
		return right
	}
	return left
}
