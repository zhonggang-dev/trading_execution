package tradehistory

import (
	"context"
	"fmt"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// Service 表示后端使用的 Service 类型。
type Service struct {
	repository port.TradeHistoryRepository
}

// New 校验仓储依赖后创建只读交易历史服务。
func New(repository port.TradeHistoryRepository) (*Service, error) {
	if repository == nil {
		return nil, fmt.Errorf("trade history repository is required")
	}
	return &Service{repository: repository}, nil
}

// List 校验筛选条件并返回仓储中的一致性交易快照。
func (service *Service) List(ctx context.Context, filter domain.TradeHistoryFilter) (domain.TradeHistoryPage, error) {
	filter = filter.Normalize()
	if err := filter.Validate(); err != nil {
		return domain.TradeHistoryPage{}, err
	}
	return service.repository.ListTradeHistory(ctx, filter)
}

// ListLedgerActivities 校验筛选条件并返回成交与赎回结算的统一账本活动快照。
func (service *Service) ListLedgerActivities(ctx context.Context, filter domain.LedgerActivityFilter) (domain.LedgerActivityPage, error) {
	filter = filter.Normalize()
	if err := filter.Validate(); err != nil {
		return domain.LedgerActivityPage{}, err
	}
	return service.repository.ListLedgerActivities(ctx, filter)
}

// DailyPnL 返回按 UTC 自然日、执行账户与开仓策略归因的净已实现盈亏（SELL 平仓 + REDEEM 赎回）。
func (service *Service) DailyPnL(ctx context.Context, filter domain.DailyPnLFilter) (domain.DailyPnLReport, error) {
	filter = filter.Normalize()
	if err := filter.Validate(); err != nil {
		return domain.DailyPnLReport{}, err
	}
	return service.repository.DailyPnL(ctx, filter)
}
