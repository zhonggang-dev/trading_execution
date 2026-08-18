package risk

import (
	"context"
	"fmt"
	"math/big"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// StaticGuardParams 表示后端使用的 StaticGuardParams 类型。
type StaticGuardParams struct {
	AllowMarketOrders bool
	MaxOrderSize      domain.Decimal
	MaxOrderNotional  domain.Decimal
}

// StaticGuard 包含执行模块持有的静态硬限制，Alpha 决策和动态仓位计算仍由策略服务负责。
type StaticGuard struct {
	params StaticGuardParams
}

// NewStaticGuard 创建并初始化 Static Guard。
func NewStaticGuard(params StaticGuardParams) (*StaticGuard, error) {
	if sign, err := params.MaxOrderSize.Sign(); err != nil || sign <= 0 {
		return nil, fmt.Errorf("max order size must be positive")
	}
	if sign, err := params.MaxOrderNotional.Sign(); err != nil || sign <= 0 {
		return nil, fmt.Errorf("max order notional must be positive")
	}
	return &StaticGuard{params: params}, nil
}

// Check 根据当前硬风控策略检查订单意图。
func (guard *StaticGuard) Check(_ context.Context, intent domain.OrderIntent) error {
	if intent.Type == domain.OrderTypeMarket && !guard.params.AllowMarketOrders {
		return &port.Rejection{Code: "MARKET_ORDERS_DISABLED", Reason: "market orders are disabled by execution policy"}
	}
	if comparison, _ := intent.Size.Compare(guard.params.MaxOrderSize); comparison > 0 {
		return &port.Rejection{Code: "ORDER_SIZE_LIMIT", Reason: "order size exceeds the execution limit"}
	}
	price := intent.Price
	if intent.Type == domain.OrderTypeMarket {
		price = intent.WorstPrice
		if price.IsEmpty() {
			return &port.Rejection{Code: "MARKET_PRICE_PROTECTION_REQUIRED", Reason: "market order requires worst_price for execution notional protection"}
		}
	}
	notional, err := price.Multiply(intent.Size)
	if err != nil {
		return &port.Rejection{Code: "INVALID_NOTIONAL", Reason: err.Error()}
	}
	maximum, _ := new(big.Rat).SetString(guard.params.MaxOrderNotional.String())
	if notional.Cmp(maximum) > 0 {
		return &port.Rejection{Code: "ORDER_NOTIONAL_LIMIT", Reason: "order notional exceeds the execution limit"}
	}
	return nil
}
