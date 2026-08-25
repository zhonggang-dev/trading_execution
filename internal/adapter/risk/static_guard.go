package risk

import (
	"context"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// StaticGuardParams 表示后端使用的 StaticGuardParams 类型。
type StaticGuardParams struct {
	AllowMarketOrders bool
}

// StaticGuard enforces execution-format policy only. Capital allocation and
// exposure decisions belong to the upstream AI strategy layer.
type StaticGuard struct {
	params StaticGuardParams
}

// NewStaticGuard 创建并初始化 Static Guard。
func NewStaticGuard(params StaticGuardParams) (*StaticGuard, error) {
	return &StaticGuard{params: params}, nil
}

// Check 根据当前硬风控策略检查订单意图。
func (guard *StaticGuard) Check(_ context.Context, intent domain.OrderIntent) error {
	if intent.Type == domain.OrderTypeMarket && !guard.params.AllowMarketOrders {
		return &port.Rejection{Code: "MARKET_ORDERS_DISABLED", Reason: "market orders are disabled by execution policy"}
	}
	if intent.Type == domain.OrderTypeMarket {
		if intent.WorstPrice.IsEmpty() {
			return &port.Rejection{Code: "MARKET_PRICE_PROTECTION_REQUIRED", Reason: "market order requires worst_price for execution notional protection"}
		}
	}
	return nil
}
