package memory

import (
	"context"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// TradeHistoryRepository 表示安全的模拟模式交易视图，不会把模拟订单状态转换成虚假成交，因此始终为空。
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

// DailyPnL 在模拟执行模式下返回空报告，不把纸交易伪装成真实收益。
func (*TradeHistoryRepository) DailyPnL(_ context.Context, filter domain.DailyPnLFilter) (domain.DailyPnLReport, error) {
	filter = filter.Normalize()
	toDay := utcDay(filter.AsOf)
	fromDay := toDay.AddDate(0, 0, 1-filter.Days)
	return domain.DailyPnLReport{
		Items: []domain.DailyPnLPoint{}, Days: filter.Days,
		FromDay: fromDay.Format(time.DateOnly), ToDay: toDay.Format(time.DateOnly),
		Timezone: "UTC", GeneratedAt: filter.AsOf,
	}, nil
}

func utcDay(value time.Time) time.Time {
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
}
