package memory

import (
	"context"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// TradeHistoryRepository is the safe paper-mode view. Paper order status is
// never converted into a fake Fill, so this repository remains empty.
type TradeHistoryRepository struct{}

var _ port.TradeHistoryRepository = (*TradeHistoryRepository)(nil)

// NewTradeHistoryRepository 创建纸交易模式使用的空交易历史仓储。
func NewTradeHistoryRepository() *TradeHistoryRepository {
	return &TradeHistoryRepository{}
}

// ListTradeHistory 返回不伪造任何成交记录的空交易历史页面。
func (*TradeHistoryRepository) ListTradeHistory(_ context.Context, filter domain.TradeHistoryFilter) (domain.TradeHistoryPage, error) {
	filter = filter.Normalize()
	return domain.TradeHistoryPage{
		Items: []domain.TradeRecord{},
		Summary: domain.TradeHistorySummary{
			BuyNotional: "0", SellNotional: "0", NetCashFlow: "0", TotalFee: "0", RealizedPnL: "0",
		},
		Limit: filter.Limit, Offset: filter.Offset,
	}, nil
}
