package risk

import (
	"context"
	"errors"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// TestStaticGuardExecutionSafety 验证静态门禁只处理执行安全规则。
func TestStaticGuardExecutionSafety(t *testing.T) {
	guard, err := NewStaticGuard(StaticGuardParams{})
	if err != nil {
		t.Fatalf("NewStaticGuard() error = %v", err)
	}
	tests := []struct {
		name string
		edit func(*domain.OrderIntent)
		code string
	}{
		{name: "market disabled", edit: func(intent *domain.OrderIntent) {
			intent.Type = domain.OrderTypeMarket
			intent.Price = ""
			intent.TimeInForce = domain.TimeInForceIOC
		}, code: "MARKET_ORDERS_DISABLED"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			intent := guardIntent()
			test.edit(&intent)
			err := guard.Check(context.Background(), intent)
			var rejection *port.Rejection
			if !errors.As(err, &rejection) || rejection.Code != test.code {
				t.Fatalf("Check() error = %v, want rejection %s", err, test.code)
			}
		})
	}
	if err := guard.Check(context.Background(), guardIntent()); err != nil {
		t.Fatalf("valid Check() error = %v", err)
	}
	large := guardIntent()
	large.Price = "0.99"
	large.Size = "1000000"
	if err := guard.Check(context.Background(), large); err != nil {
		t.Fatalf("strategy-sized order Check() error = %v", err)
	}
}

// TestStaticGuardRequiresWorstPriceForMarketOrder 验证市价单仍需要价格保护。
func TestStaticGuardRequiresWorstPriceForMarketOrder(t *testing.T) {
	guard, err := NewStaticGuard(StaticGuardParams{
		AllowMarketOrders: true,
	})
	if err != nil {
		t.Fatalf("NewStaticGuard() error = %v", err)
	}
	intent := guardIntent()
	intent.Type = domain.OrderTypeMarket
	intent.Price = ""
	intent.TimeInForce = domain.TimeInForceIOC
	var rejection *port.Rejection
	if err := guard.Check(context.Background(), intent); !errors.As(err, &rejection) || rejection.Code != "MARKET_PRICE_PROTECTION_REQUIRED" {
		t.Fatalf("Check() error = %v, want MARKET_PRICE_PROTECTION_REQUIRED", err)
	}
	intent.WorstPrice = "0.51"
	if err := guard.Check(context.Background(), intent); err != nil {
		t.Fatalf("protected market order Check() error = %v", err)
	}
}

func TestStaticGuardDoesNotSecondGuessStrategyShareSize(t *testing.T) {
	guard, err := NewStaticGuard(StaticGuardParams{})
	if err != nil {
		t.Fatalf("NewStaticGuard() error = %v", err)
	}
	intent := guardIntent()
	intent.Price = "0.12"
	intent.Size = "83.33"
	if err := guard.Check(context.Background(), intent); err != nil {
		t.Fatalf("strategy-sized order Check() error = %v", err)
	}
}

// guardIntent 实现当前测试场景所需的辅助行为。
func guardIntent() domain.OrderIntent {
	return domain.OrderIntent{
		Type:        domain.OrderTypeLimit,
		Price:       "0.5",
		Size:        "50",
		TimeInForce: domain.TimeInForceGTC,
	}
}
