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
