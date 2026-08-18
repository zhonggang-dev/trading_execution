package paper

import (
	"context"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// MarketValidator 表示显式的模拟模式校验旁路，命名适配器可防止实盘在缺少 Market 校验时意外启动。
type MarketValidator struct {
	now func() time.Time
}

// NewMarketValidator 创建并初始化 Market Validator。
func NewMarketValidator() *MarketValidator {
	return &MarketValidator{now: time.Now}
}

// Validate 校验当前模型的字段完整性和业务约束。
func (validator *MarketValidator) Validate(_ context.Context, intent domain.OrderIntent) (domain.MarketValidation, error) {
	return domain.MarketValidation{
		Mode:        "PAPER_BYPASS",
		ValidatedAt: validator.now().UTC(),
		WorstPrice:  intent.WorstPrice,
	}, nil
}
