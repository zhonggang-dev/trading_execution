package postgres

import (
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// TestBuildTradeHistoryWhereUsesBoundParameters 验证交易历史查询条件始终使用绑定参数。
func TestBuildTradeHistoryWhereUsesBoundParameters(t *testing.T) {
	from := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(24 * time.Hour)
	where, args := buildTradeHistoryWhere(domain.TradeHistoryFilter{
		From: &from, To: &to, Side: domain.SideSell, ModelID: "model-v2",
		StrategyID: "multfactor_v2", ExecutionAccountID: "wallet-v2", Search: "fill-17",
	})
	for _, fragment := range []string{"fill.status='CONFIRMED'", "fill.applied_at IS NOT NULL", "$1", "$7"} {
		if !strings.Contains(where, fragment) {
			t.Fatalf("where clause %q does not contain %q", where, fragment)
		}
	}
	if strings.Contains(where, "fill-17") || len(args) != 7 || args[6] != "%fill-17%" {
		t.Fatalf("where=%q args=%#v", where, args)
	}
}

// TestDailyPnLQueryAttributesClosuresToOpeningStrategy 验证聚合使用来源批次策略并补齐启用绑定的零值日期。
func TestDailyPnLQueryAttributesClosuresToOpeningStrategy(t *testing.T) {
	for _, fragment := range []string{
		"JOIN position_lots AS lot ON lot.lot_id = closure.lot_id",
		"lot.model_id, lot.strategy_id",
		"FROM days CROSS JOIN identities",
		"FROM execution_strategy_bindings",
		"fill.status = 'CONFIRMED'",
		"AT TIME ZONE 'UTC'",
	} {
		if !strings.Contains(dailyPnLStatement, fragment) {
			t.Fatalf("daily pnl query does not contain %q", fragment)
		}
	}
}
