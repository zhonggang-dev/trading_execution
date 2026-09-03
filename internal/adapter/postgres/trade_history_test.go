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

// TestDailyPnLQueryAttributesClosuresToOpeningStrategy 验证聚合使用来源批次策略、纳入已入账赎回并补齐启用绑定的零值日期。
func TestDailyPnLQueryAttributesClosuresToOpeningStrategy(t *testing.T) {
	for _, fragment := range []string{
		"JOIN position_lots AS lot ON lot.lot_id = closure.lot_id",
		"lot.model_id, lot.strategy_id",
		"FROM days CROSS JOIN identities",
		"FROM execution_strategy_bindings",
		"fill.status = 'CONFIRMED'",
		"AT TIME ZONE 'UTC'",
		"FROM position_lot_redemptions AS redemption",
		"JOIN position_lots AS lot ON lot.lot_id = redemption.lot_id",
		"(redemption.redeemed_at AT TIME ZONE 'UTC')::date AS day",
		"parent.status = 'APPLIED'",
		"COUNT(DISTINCT event_key) FILTER (WHERE kind = 'SELL')",
		"COUNT(DISTINCT event_key) FILTER (WHERE kind = 'REDEEM')",
		"SUM(realized_pnl) FILTER (WHERE kind = 'REDEEM')",
		"redemption.realized_pnl, redemption.redeemed_shares",
	} {
		if !strings.Contains(dailyPnLStatement, fragment) {
			t.Fatalf("daily pnl query does not contain %q", fragment)
		}
	}
}

// TestLedgerActivityQueryUnionsFillsWithLotRedemptions 验证统一账本把 REDEEM 按原始批次归因，
// 赎回到账不计入 SELL 卖出金额，并且 REDEEM 行没有价格与订单字段。
func TestLedgerActivityQueryUnionsFillsWithLotRedemptions(t *testing.T) {
	for _, fragment := range []string{
		"'fill:' || fill.fill_key AS activity_key",
		"fill.status = 'CONFIRMED' AND fill.applied_at IS NOT NULL AND fill.confirmed_at IS NOT NULL",
		"UNION ALL",
		"'redemption:' || redemption.redemption_id AS activity_key, 'REDEEM' AS activity_type",
		"lot.model_id, lot.strategy_id, lot.market_id",
		"'' AS order_id, '' AS venue_order_id, '' AS venue_trade_id",
		"NULL::numeric AS price, NULL::numeric AS gross_notional",
		"redemption.allocated_payout AS settlement_payout",
		"redemption.redeemed_at AS occurred_at, parent.confirmed_at, redemption.redeemed_at AS applied_at",
		"parent.status = 'APPLIED'",
	} {
		if !strings.Contains(ledgerActivityFrom, fragment) {
			t.Fatalf("ledger activity query does not contain %q", fragment)
		}
	}
	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	where, args := buildLedgerActivityWhere(domain.LedgerActivityFilter{
		From: &from, ActivityType: domain.LedgerActivityRedeem, ExecutionAccountID: "wallet-6", Search: "0xabc",
	})
	for _, fragment := range []string{"activity.occurred_at >= $1", "activity.activity_type = $2", "activity.execution_account_id = $3", "activity.transaction_hash", "LIKE LOWER($4)"} {
		if !strings.Contains(where, fragment) {
			t.Fatalf("where clause %q does not contain %q", where, fragment)
		}
	}
	if strings.Contains(where, "0xabc") || len(args) != 4 || args[1] != "REDEEM" || args[3] != "%0xabc%" {
		t.Fatalf("where=%q args=%#v", where, args)
	}
}
