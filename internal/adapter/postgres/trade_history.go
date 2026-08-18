package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

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
