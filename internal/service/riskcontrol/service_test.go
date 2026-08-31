package riskcontrol

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// fakeRiskSource 表示后端使用的 fakeRiskSource 类型。
type fakeRiskSource struct {
	snapshot domain.HardRiskSnapshot
	err      error
	intent   domain.OrderIntent
}

// Snapshot 返回模拟预测快照。
func (source *fakeRiskSource) Snapshot(_ context.Context, intent domain.OrderIntent, _ time.Time) (domain.HardRiskSnapshot, error) {
	source.intent = intent
	return source.snapshot, source.err
}

// TestCheckAcceptsSafeBuyWithoutMutatingStrategyIntent 验证 Check Accepts Safe Buy Without Mutating Strategy Intent 场景下的行为。
func TestCheckAcceptsSafeBuyWithoutMutatingStrategyIntent(t *testing.T) {
	now, intent, snapshot := validRiskFixtures()
	original := intent.Normalize()
	source := &fakeRiskSource{snapshot: snapshot}
	service := newRiskService(t, now, source)

	if err := service.Check(context.Background(), intent); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if !source.intent.Equivalent(original) || source.intent.Side != domain.SideBuy || source.intent.Size != "10" {
		t.Fatalf("hard risk changed strategy intent: %#v", source.intent)
	}
}

// TestCheckAcceptsOldStrategySnapshot defers execution-price freshness to the
// official book captured by the live market validator.
func TestCheckAcceptsOldStrategySnapshot(t *testing.T) {
	now, intent, snapshot := validRiskFixtures()
	oldSnapshotAt := now.Add(-4 * time.Hour)
	intent.MarketSnapshotAt = &oldSnapshotAt
	service := newRiskService(t, now, &fakeRiskSource{snapshot: snapshot})

	if err := service.Check(context.Background(), intent); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
}

// TestCheckRejectsUnsafeOrders 验证 Check Rejects Unsafe Orders 场景下的行为。
func TestCheckRejectsUnsafeOrders(t *testing.T) {
	tests := []struct {
		name   string
		code   string
		mutate func(*domain.OrderIntent, *domain.HardRiskSnapshot, time.Time)
	}{
		{name: "global kill switch", code: "GLOBAL_KILL_SWITCH", mutate: func(_ *domain.OrderIntent, snapshot *domain.HardRiskSnapshot, _ time.Time) {
			snapshot.Controls.GlobalKillSwitch = true
		}},
		{name: "account paused", code: "EXECUTION_ACCOUNT_PAUSED", mutate: func(_ *domain.OrderIntent, snapshot *domain.HardRiskSnapshot, _ time.Time) {
			snapshot.Controls.ExecutionAccountPaused = true
		}},
		{name: "strategy paused", code: "STRATEGY_PAUSED", mutate: func(_ *domain.OrderIntent, snapshot *domain.HardRiskSnapshot, _ time.Time) {
			snapshot.Controls.StrategyPaused = true
		}},
		{name: "market paused", code: "MARKET_RISK_PAUSED", mutate: func(_ *domain.OrderIntent, snapshot *domain.HardRiskSnapshot, _ time.Time) {
			snapshot.Controls.MarketPaused = true
		}},
		{name: "price timestamp required", code: "PRICE_TIMESTAMP_REQUIRED", mutate: func(intent *domain.OrderIntent, _ *domain.HardRiskSnapshot, _ time.Time) {
			intent.MarketSnapshotAt = nil
		}},
		{name: "signal timestamp required", code: "SIGNAL_TIMESTAMP_REQUIRED", mutate: func(intent *domain.OrderIntent, _ *domain.HardRiskSnapshot, _ time.Time) {
			intent.SignalAt = nil
		}},
		{name: "future price", code: "PRICE_TIMESTAMP_FUTURE", mutate: func(intent *domain.OrderIntent, _ *domain.HardRiskSnapshot, now time.Time) {
			future := now.Add(3 * time.Second)
			intent.MarketSnapshotAt = &future
		}},
		{name: "stale signal", code: "SIGNAL_STALE", mutate: func(intent *domain.OrderIntent, _ *domain.HardRiskSnapshot, now time.Time) {
			stale := now.Add(-2 * time.Minute)
			intent.SignalAt = &stale
		}},
		{name: "stale risk state", code: "RISK_STATE_STALE", mutate: func(_ *domain.OrderIntent, snapshot *domain.HardRiskSnapshot, now time.Time) {
			snapshot.ObservedAt = now.Add(-11 * time.Second)
		}},
		{name: "same direction order", code: "SAME_DIRECTION_ORDER_EXISTS", mutate: func(_ *domain.OrderIntent, snapshot *domain.HardRiskSnapshot, _ time.Time) {
			snapshot.OpenOrders = []domain.RiskOpenOrder{openOrder(domain.SideBuy, "yes-token", "other-strategy", "market-1", "1", "0.5")}
		}},
		{name: "wallet balance", code: "INSUFFICIENT_WALLET_BALANCE", mutate: func(_ *domain.OrderIntent, snapshot *domain.HardRiskSnapshot, _ time.Time) {
			snapshot.AvailableBalance = "4"
			snapshot.ReservedBalance = "96"
		}},
		{name: "duplicate sell", code: "DUPLICATE_SELL_ORDER", mutate: func(intent *domain.OrderIntent, snapshot *domain.HardRiskSnapshot, _ time.Time) {
			intent.Side = domain.SideSell
			snapshot.Positions = []domain.RiskPosition{position("strategy-v1", "market-1", "yes-token", "20", "10")}
			snapshot.OpenOrders = []domain.RiskOpenOrder{openOrder(domain.SideSell, "yes-token", "strategy-v1", "market-1", "5", "0.5")}
		}},
		{name: "oversell", code: "INSUFFICIENT_SELL_POSITION", mutate: func(intent *domain.OrderIntent, snapshot *domain.HardRiskSnapshot, _ time.Time) {
			intent.Side = domain.SideSell
			snapshot.Positions = []domain.RiskPosition{position("strategy-v1", "market-1", "yes-token", "4", "2")}
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now, intent, snapshot := validRiskFixtures()
			test.mutate(&intent, &snapshot, now)
			service := newRiskService(t, now, &fakeRiskSource{snapshot: snapshot})
			err := service.Check(context.Background(), intent)
			var rejection *port.Rejection
			if !errors.As(err, &rejection) || rejection.Code != test.code {
				t.Fatalf("Check() error = %v, want %s", err, test.code)
			}
		})
	}
}

// TestCheckAllowsRiskReducingSellWhenExposureAlreadyExceedsLimits 验证 Check Allows Risk Reducing Sell When Exposure Already Exceeds Limits 场景下的行为。
func TestCheckAllowsRiskReducingSellWhenExposureAlreadyExceedsLimits(t *testing.T) {
	now, intent, snapshot := validRiskFixtures()
	intent.Side = domain.SideSell
	snapshot.Positions = []domain.RiskPosition{
		position("strategy-v1", "market-1", "yes-token", "20", "200"),
	}
	service := newRiskService(t, now, &fakeRiskSource{snapshot: snapshot})
	if err := service.Check(context.Background(), intent); err != nil {
		t.Fatalf("risk-reducing Check() error = %v", err)
	}
}

// TestCheckUsesAvailableSharesWithoutDoubleSubtractingSellReservations 验证 Check Uses Available Shares Without Double Subtracting Sell Reservations 场景下的行为。
func TestCheckUsesAvailableSharesWithoutDoubleSubtractingSellReservations(t *testing.T) {
	now, intent, snapshot := validRiskFixtures()
	intent.Side = domain.SideSell
	intent.Size = "6"
	snapshot.Positions = []domain.RiskPosition{{
		StrategyID:      "strategy-v1",
		MarketID:        "market-1",
		TokenID:         "yes-token",
		TotalShares:     "20",
		AvailableShares: "5",
		ReservedShares:  "15",
		RiskValue:       "10",
	}}
	service := newRiskService(t, now, &fakeRiskSource{snapshot: snapshot})
	err := service.Check(context.Background(), intent)
	var rejection *port.Rejection
	if !errors.As(err, &rejection) || rejection.Code != "INSUFFICIENT_SELL_POSITION" {
		t.Fatalf("Check() error = %v, want available-shares rejection", err)
	}
}

// TestCheckRejectsBrokenBalanceInvariant 验证 Check Rejects Broken Balance Invariant 场景下的行为。
func TestCheckRejectsBrokenBalanceInvariant(t *testing.T) {
	now, intent, snapshot := validRiskFixtures()
	snapshot.TotalBalance = "99"
	service := newRiskService(t, now, &fakeRiskSource{snapshot: snapshot})
	if err := service.Check(context.Background(), intent); err == nil {
		t.Fatal("Check() error = nil, want broken balance identity rejection")
	}
}

// TestCheckDoesNotApplyMonetaryAllocationCaps 验证金额与曝口分配由上游策略决定。
func TestCheckDoesNotApplyMonetaryAllocationCaps(t *testing.T) {
	now, intent, snapshot := validRiskFixtures()
	snapshot.Limits.MaxOrderNotional = "1"
	snapshot.Limits.MaxMarketExposure = "1"
	snapshot.Limits.MaxStrategyExposure = "1"
	snapshot.Limits.MaxWalletExposure = "10"
	snapshot.Limits.MaxDailyTradedNotional = "1"
	snapshot.DailyTradedNotional = "1000000"
	snapshot.Positions = []domain.RiskPosition{
		position("strategy-v1", "market-1", "other-token", "1", "1000000"),
	}
	snapshot.OpenOrders = []domain.RiskOpenOrder{
		openOrder(domain.SideBuy, "other-token", "other-strategy", "other-market", "11", "0.5"),
	}
	service := newRiskService(t, now, &fakeRiskSource{snapshot: snapshot})
	if err := service.Check(context.Background(), intent); err != nil {
		t.Fatalf("Check() error = %v, monetary allocation caps must not be enforced", err)
	}
}

// TestCheckFailsClosedWhenRiskSnapshotCannotBeLoaded 验证 Check Fails Closed When Risk Snapshot Cannot Be Loaded 场景下的行为。
func TestCheckFailsClosedWhenRiskSnapshotCannotBeLoaded(t *testing.T) {
	now, intent, _ := validRiskFixtures()
	service := newRiskService(t, now, &fakeRiskSource{err: errors.New("risk store unavailable")})
	if err := service.Check(context.Background(), intent); err == nil {
		t.Fatal("Check() error = nil, want fail-closed source error")
	}
}

// newRiskService 创建测试所需的模拟对象。
func newRiskService(t *testing.T, now time.Time, source port.HardRiskSource) *Service {
	t.Helper()
	service, err := New(Params{Source: source, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return service
}

// validRiskFixtures 构建测试使用的合法输入。
func validRiskFixtures() (time.Time, domain.OrderIntent, domain.HardRiskSnapshot) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	priceAt := now.Add(-5 * time.Second)
	signalAt := now.Add(-2 * time.Second)
	intent := domain.OrderIntent{
		ModelID:            "model-a",
		StrategyID:         "strategy-v1",
		ExecutionAccountID: "account-model-a-strategy-v1",
		SignalID:           "signal-1",
		ClientOrderID:      "client-1",
		Venue:              "polymarket",
		MarketID:           "market-1",
		ConditionID:        "condition-1",
		TokenID:            "yes-token",
		MarketSnapshotAt:   &priceAt,
		SignalAt:           &signalAt,
		Side:               domain.SideBuy,
		Type:               domain.OrderTypeLimit,
		Price:              "0.5",
		WorstPrice:         "0.5",
		Size:               "10",
		TimeInForce:        domain.TimeInForceGTC,
	}
	snapshot := domain.HardRiskSnapshot{
		SnapshotID:          "risk-snapshot-1",
		ExecutionAccountID:  intent.ExecutionAccountID,
		ObservedAt:          now.Add(-time.Second),
		TotalBalance:        "100",
		AvailableBalance:    "100",
		ReservedBalance:     "0",
		DailyTradedNotional: "10",
		Limits: domain.HardRiskLimits{
			PolicyID:               "policy-1",
			MaxOrderNotional:       "20",
			MaxMarketExposure:      "50",
			MaxStrategyExposure:    "70",
			MaxWalletExposure:      "100",
			MaxDailyTradedNotional: "200",
			MaxPriceAge:            time.Minute,
			MaxSignalAge:           time.Minute,
			MaxStateAge:            10 * time.Second,
		},
		Positions:  []domain.RiskPosition{},
		OpenOrders: []domain.RiskOpenOrder{},
	}
	return now, intent, snapshot
}

// position 实现当前测试场景所需的辅助行为。
func position(strategyID, marketID, tokenID, size, riskValue string) domain.RiskPosition {
	return domain.RiskPosition{
		StrategyID:      strategyID,
		MarketID:        marketID,
		TokenID:         tokenID,
		TotalShares:     domain.Decimal(size),
		AvailableShares: domain.Decimal(size),
		ReservedShares:  "0",
		RiskValue:       domain.Decimal(riskValue),
	}
}

// openOrder 实现当前测试场景所需的辅助行为。
func openOrder(side domain.Side, tokenID, strategyID, marketID, remainingSize, worstPrice string) domain.RiskOpenOrder {
	return domain.RiskOpenOrder{
		OrderID:       "open-order-1",
		ClientOrderID: "open-client-1",
		StrategyID:    strategyID,
		MarketID:      marketID,
		TokenID:       tokenID,
		Side:          side,
		RemainingSize: domain.Decimal(remainingSize),
		WorstPrice:    domain.Decimal(worstPrice),
	}
}
