package risk

import (
	"context"
	"errors"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// TestStaticGuardHardLimits 验证 Static Guard Hard Limits 场景下的行为。
func TestStaticGuardHardLimits(t *testing.T) {
	guard, err := NewStaticGuard(StaticGuardParams{
		MaxOrderSize:     "100",
		MaxOrderNotional: "25",
	})
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
		{name: "size", edit: func(intent *domain.OrderIntent) { intent.Size = "101" }, code: "ORDER_SIZE_LIMIT"},
		{name: "notional", edit: func(intent *domain.OrderIntent) {
			intent.Price = "0.51"
			intent.Size = "50"
		}, code: "ORDER_NOTIONAL_LIMIT"},
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
}

// TestStaticGuardUsesWorstPriceForMarketOrderNotional 验证 Static Guard Uses Worst Price For Market Order Notional 场景下的行为。
func TestStaticGuardUsesWorstPriceForMarketOrderNotional(t *testing.T) {
	guard, err := NewStaticGuard(StaticGuardParams{
		AllowMarketOrders: true,
		MaxOrderSize:      "100",
		MaxOrderNotional:  "25",
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
	if err := guard.Check(context.Background(), intent); !errors.As(err, &rejection) || rejection.Code != "ORDER_NOTIONAL_LIMIT" {
		t.Fatalf("Check() error = %v, want ORDER_NOTIONAL_LIMIT", err)
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
