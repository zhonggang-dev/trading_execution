package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// TradeHistoryRepository 表示后端使用的 TradeHistoryRepository 类型。
type TradeHistoryRepository struct {
	db *sql.DB
}

var _ port.TradeHistoryRepository = (*TradeHistoryRepository)(nil)

// NewTradeHistoryRepository 创建基于 PostgreSQL 的交易历史仓储。
func NewTradeHistoryRepository(db *sql.DB) (*TradeHistoryRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	return &TradeHistoryRepository{db: db}, nil
}

const tradeHistoryFrom = `
	FROM execution_fills AS fill
	JOIN execution_orders AS order_row ON order_row.order_id = fill.order_id
	LEFT JOIN position_lots AS opening_lot ON opening_lot.opening_fill_key = fill.fill_key
	LEFT JOIN (
		SELECT closing_fill_key, SUM(realized_pnl) AS realized_pnl
		FROM position_lot_closures
		GROUP BY closing_fill_key
	) AS closure ON closure.closing_fill_key = fill.fill_key`

// ListTradeHistory 在同一可重复读快照中查询交易明细和汇总数据。
func (repository *TradeHistoryRepository) ListTradeHistory(
	ctx context.Context,
	filter domain.TradeHistoryFilter,
) (domain.TradeHistoryPage, error) {
	filter = filter.Normalize()
	if err := filter.Validate(); err != nil {
		return domain.TradeHistoryPage{}, err
	}
	tx, err := repository.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return domain.TradeHistoryPage{}, fmt.Errorf("begin trade history query: %w", err)
	}
	defer tx.Rollback()

	where, args := buildTradeHistoryWhere(filter)
	items, err := queryTradeHistoryItems(ctx, tx, where, args, filter)
	if err != nil {
		return domain.TradeHistoryPage{}, err
	}
	summary, err := queryTradeHistorySummary(ctx, tx, where, args)
	if err != nil {
		return domain.TradeHistoryPage{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.TradeHistoryPage{}, fmt.Errorf("commit trade history query: %w", err)
	}
	return domain.TradeHistoryPage{
		Items: items, Summary: summary, Total: summary.TradeCount,
		Limit: filter.Limit, Offset: filter.Offset,
	}, nil
}

// queryTradeHistoryItems 查询一页已确认并入账的真实成交明细。
func queryTradeHistoryItems(
	ctx context.Context,
	tx *sql.Tx,
	where string,
	args []any,
	filter domain.TradeHistoryFilter,
) ([]domain.TradeRecord, error) {
	limitIndex, offsetIndex := len(args)+1, len(args)+2
	statement := `
		SELECT fill.fill_key, fill.venue, fill.venue_fill_id, fill.order_id,
		       fill.venue_order_id, order_row.status, fill.execution_account_id,
		       COALESCE(order_row.intent->>'model_id', ''),
		       COALESCE(order_row.intent->>'strategy_id', ''),
		       fill.market_id,
		       COALESCE(order_row.intent->'metadata'->>'market_question', ''),
		       fill.condition_id, fill.token_id,
		       COALESCE(order_row.intent->>'outcome_name', ''),
		       COALESCE(NULLIF(order_row.intent->>'target_lot_id', ''), opening_lot.lot_id, ''),
		       fill.side, fill.liquidity_role, fill.shares::text, fill.price::text,
		       fill.gross_notional::text, fill.total_fee::text, fill.net_cash_delta::text,
		       COALESCE(closure.realized_pnl, 0)::text,
		       fill.transaction_hash, fill.matched_at, fill.confirmed_at
	` + tradeHistoryFrom + where + fmt.Sprintf(`
		ORDER BY fill.matched_at DESC, fill.fill_key DESC
		LIMIT $%d OFFSET $%d`, limitIndex, offsetIndex)
	queryArgs := append(append([]any{}, args...), filter.Limit, filter.Offset)
	rows, err := tx.QueryContext(ctx, statement, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query trade history rows: %w", err)
	}
	defer rows.Close()
	items := make([]domain.TradeRecord, 0, filter.Limit)
	for rows.Next() {
		var item domain.TradeRecord
		var orderStatus, side, role string
		if err := rows.Scan(
			&item.FillKey, &item.Venue, &item.VenueTradeID, &item.OrderID,
			&item.VenueOrderID, &orderStatus, &item.ExecutionAccountID,
			&item.ModelID, &item.StrategyID, &item.MarketID, &item.MarketLabel,
			&item.ConditionID, &item.TokenID, &item.OutcomeName, &item.LotID,
			&side, &role, &item.Shares, &item.Price, &item.GrossNotional,
			&item.TotalFee, &item.NetCashDelta, &item.RealizedPnL,
			&item.TransactionHash, &item.MatchedAt, &item.ConfirmedAt,
		); err != nil {
			return nil, fmt.Errorf("scan trade history row: %w", err)
		}
		item.OrderStatus = domain.OrderStatus(orderStatus)
		item.Side = domain.Side(side)
		item.LiquidityRole = domain.LiquidityRole(role)
		item.MatchedAt = item.MatchedAt.UTC()
		item.ConfirmedAt = item.ConfirmedAt.UTC()
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate trade history rows: %w", err)
	}
	return items, nil
}

// queryTradeHistorySummary 汇总当前筛选范围内的成交和盈亏数据。
func queryTradeHistorySummary(
	ctx context.Context,
	tx *sql.Tx,
	where string,
	args []any,
) (domain.TradeHistorySummary, error) {
	statement := `
		SELECT COUNT(*)::bigint,
		       COALESCE(SUM(fill.gross_notional) FILTER (WHERE fill.side='BUY'), 0)::text,
		       COALESCE(SUM(fill.gross_notional) FILTER (WHERE fill.side='SELL'), 0)::text,
		       COALESCE(SUM(fill.net_cash_delta), 0)::text,
		       COALESCE(SUM(fill.total_fee), 0)::text,
		       COALESCE(SUM(closure.realized_pnl), 0)::text
	` + tradeHistoryFrom + where
	var summary domain.TradeHistorySummary
	if err := tx.QueryRowContext(ctx, statement, args...).Scan(
		&summary.TradeCount, &summary.BuyNotional, &summary.SellNotional,
		&summary.NetCashFlow, &summary.TotalFee, &summary.RealizedPnL,
	); err != nil {
		return domain.TradeHistorySummary{}, fmt.Errorf("query trade history summary: %w", err)
	}
	return summary, nil
}

// buildTradeHistoryWhere 使用绑定参数构建交易历史查询条件。
func buildTradeHistoryWhere(filter domain.TradeHistoryFilter) (string, []any) {
	clauses := []string{"fill.status='CONFIRMED'", "fill.applied_at IS NOT NULL", "fill.confirmed_at IS NOT NULL"}
	args := make([]any, 0, 8)
	add := func(clause string, value any) {
		args = append(args, value)
		clauses = append(clauses, fmt.Sprintf(clause, len(args)))
	}
	if filter.From != nil {
		add("fill.matched_at >= $%d", *filter.From)
	}
	if filter.To != nil {
		add("fill.matched_at <= $%d", *filter.To)
	}
	if filter.Side != "" {
		add("fill.side = $%d", string(filter.Side))
	}
	if filter.ModelID != "" {
		add("order_row.intent->>'model_id' = $%d", filter.ModelID)
	}
	if filter.StrategyID != "" {
		add("order_row.intent->>'strategy_id' = $%d", filter.StrategyID)
	}
	if filter.ExecutionAccountID != "" {
		add("fill.execution_account_id = $%d", filter.ExecutionAccountID)
	}
	if filter.Search != "" {
		add(`LOWER(CONCAT_WS(' ', fill.venue_fill_id, fill.order_id, fill.venue_order_id,
			fill.market_id, fill.condition_id, fill.token_id,
			order_row.intent->>'outcome_name', order_row.intent->>'model_id',
			order_row.intent->>'strategy_id', fill.execution_account_id)) LIKE LOWER($%d)`, "%"+filter.Search+"%")
	}
	return "\n\tWHERE " + strings.Join(clauses, " AND "), args
}

const dailyPnLStatement = `
	WITH bounds AS (
		SELECT $1::date AS from_day, $2::date AS to_day
	), days AS (
		SELECT generate_series(bounds.from_day, bounds.to_day, INTERVAL '1 day')::date AS day
		FROM bounds
	), closed AS (
		SELECT (fill.matched_at AT TIME ZONE 'UTC')::date AS day,
		       fill.execution_account_id, lot.model_id, lot.strategy_id,
		       SUM(closure.realized_pnl) AS realized_pnl,
		       COUNT(DISTINCT fill.fill_key)::bigint AS closed_trade_count,
		       SUM(closure.closed_shares) AS closed_shares
		FROM position_lot_closures AS closure
		JOIN execution_fills AS fill ON fill.fill_key = closure.closing_fill_key
		JOIN position_lots AS lot ON lot.lot_id = closure.lot_id
		CROSS JOIN bounds
		WHERE fill.status = 'CONFIRMED'
		  AND fill.applied_at IS NOT NULL
		  AND fill.confirmed_at IS NOT NULL
		  AND fill.matched_at >= (bounds.from_day::timestamp AT TIME ZONE 'UTC')
		  AND fill.matched_at < ((bounds.to_day + 1)::timestamp AT TIME ZONE 'UTC')
		GROUP BY day, fill.execution_account_id, lot.model_id, lot.strategy_id
	), identities AS (
		SELECT model_id, strategy_id, execution_account_id
		FROM execution_strategy_bindings
		WHERE enabled
		UNION
		SELECT model_id, strategy_id, execution_account_id FROM closed
	)
	SELECT days.day::text, identities.execution_account_id,
	       identities.model_id, identities.strategy_id,
	       COALESCE(closed.realized_pnl, 0)::text,
	       COALESCE(closed.closed_trade_count, 0)::bigint,
	       COALESCE(closed.closed_shares, 0)::text
	FROM days CROSS JOIN identities
	LEFT JOIN closed
	  ON closed.day = days.day
	 AND closed.execution_account_id = identities.execution_account_id
	 AND closed.model_id = identities.model_id
	 AND closed.strategy_id = identities.strategy_id
	ORDER BY days.day, identities.execution_account_id, identities.strategy_id, identities.model_id`

// DailyPnL 查询连续 UTC 日，并按来源批次的 model/strategy 归因每一笔平仓收益。
// 启用的绑定通过 days × identities 补齐零值点，前端无需把“无成交”误判为“数据缺失”。
func (repository *TradeHistoryRepository) DailyPnL(
	ctx context.Context,
	filter domain.DailyPnLFilter,
) (domain.DailyPnLReport, error) {
	filter = filter.Normalize()
	if err := filter.Validate(); err != nil {
		return domain.DailyPnLReport{}, err
	}
	toDay := postgresUTCDay(filter.AsOf)
	fromDay := toDay.AddDate(0, 0, 1-filter.Days)
	rows, err := repository.db.QueryContext(ctx, dailyPnLStatement, fromDay, toDay)
	if err != nil {
		return domain.DailyPnLReport{}, fmt.Errorf("query daily pnl: %w", err)
	}
	defer rows.Close()
	items := make([]domain.DailyPnLPoint, 0)
	for rows.Next() {
		var item domain.DailyPnLPoint
		if err := rows.Scan(
			&item.Day, &item.ExecutionAccountID, &item.ModelID, &item.StrategyID,
			&item.RealizedPnL, &item.ClosedTradeCount, &item.ClosedShares,
		); err != nil {
			return domain.DailyPnLReport{}, fmt.Errorf("scan daily pnl row: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return domain.DailyPnLReport{}, fmt.Errorf("iterate daily pnl rows: %w", err)
	}
	return domain.DailyPnLReport{
		Items: items, Days: filter.Days,
		FromDay: fromDay.Format(time.DateOnly), ToDay: toDay.Format(time.DateOnly),
		Timezone: "UTC", GeneratedAt: filter.AsOf,
	}, nil
}

func postgresUTCDay(value time.Time) time.Time {
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
}
