package consolev2

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const orderProjection = `
SELECT o.order_id, o.client_order_id, NULLIF(o.venue_order_id,''), o.status,
       o.execution_account_id, COALESCE(o.intent->>'model_id',''),
       COALESCE(o.intent->>'strategy_id',''), o.venue,
       COALESCE(o.intent->>'market_source',''), o.market_id,
       COALESCE(o.intent->>'condition_id',''), o.token_id,
       COALESCE(o.intent->>'side',''),
	       COALESCE(NULLIF(o.intent->>'price',''), NULLIF(o.intent->>'worst_price',''), ''),
       COALESCE(o.intent->>'size',''), o.filled_size::text,
       NULLIF(o.average_fill_price::text,''), o.created_at, o.updated_at
FROM execution_orders o
`

func (reader *Reader) Orders(ctx context.Context, filter ListFilter) (Page[Order], error) {
	filter, err := normalizeFilter(filter)
	if err != nil {
		return Page[Order]{}, err
	}
	if err := reader.requireDB(ctx); err != nil {
		return Page[Order]{}, err
	}
	now := reader.now().UTC()
	args := make([]any, 0, 14)
	conditions := []string{"1=1"}
	arg := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}
	if filter.ModelID != "" {
		conditions = append(conditions, "o.intent->>'model_id' = "+arg(filter.ModelID))
	}
	if filter.StrategyID != "" {
		conditions = append(conditions, "o.intent->>'strategy_id' = "+arg(filter.StrategyID))
	}
	if filter.ExecutionAccountID != "" {
		conditions = append(conditions, "o.execution_account_id = "+arg(filter.ExecutionAccountID))
	}
	if filter.MarketSource != "" {
		conditions = append(conditions, "o.intent->>'market_source' = "+arg(filter.MarketSource))
	}
	if filter.Status != "" {
		conditions = append(conditions, "o.status = "+arg(filter.Status))
	}
	if filter.Side != "" {
		conditions = append(conditions, "o.intent->>'side' = "+arg(filter.Side))
	}
	if filter.Query != "" {
		placeholder := arg("%" + filter.Query + "%")
		conditions = append(conditions, "(o.order_id ILIKE "+placeholder+" OR o.client_order_id ILIKE "+placeholder+
			" OR o.venue_order_id ILIKE "+placeholder+" OR o.market_id ILIKE "+placeholder+" OR o.token_id ILIKE "+placeholder+")")
	}
	if filter.From != nil {
		conditions = append(conditions, "o.updated_at >= "+arg(filter.From.UTC()))
	} else {
		conditions = append(conditions, "(o.updated_at >= "+arg(boundedSince(filter, now))+
			" OR o.status NOT IN ('FILLED','REJECTED','CANCELLED'))")
	}
	if filter.To != nil {
		conditions = append(conditions, "o.updated_at <= "+arg(filter.To.UTC()))
	}
	args = append(args, filter.Limit+1)
	limit := fmt.Sprintf("$%d", len(args))
	args = append(args, filter.Offset)
	offset := fmt.Sprintf("$%d", len(args))
	query := orderProjection + " WHERE " + strings.Join(conditions, " AND ") +
		" ORDER BY o.updated_at DESC, o.order_id DESC LIMIT " + limit + " OFFSET " + offset
	rows, err := reader.db.QueryContext(ctx, query, args...)
	if err != nil {
		return Page[Order]{}, dbUnavailable("list orders", err)
	}
	defer rows.Close()
	items := make([]Order, 0, filter.Limit)
	for rows.Next() {
		item, scanErr := scanOrder(rows)
		if scanErr != nil {
			return Page[Order]{}, dbUnavailable("scan order", scanErr)
		}
		items = append(items, item)
	}
	if err := queryRowsError(rows, "iterate orders"); err != nil {
		return Page[Order]{}, dbUnavailable("iterate orders", err)
	}
	return finishPage(items, filter, now, map[string]any{"basis": "bounded_recent_orders", "open_statuses_included": true}), nil
}

func (reader *Reader) loadOrder(ctx context.Context, orderID string) (Order, error) {
	row := reader.db.QueryRowContext(ctx, orderProjection+" WHERE o.order_id=$1", orderID)
	item, err := scanOrder(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return Order{}, ErrNotFound
		}
		return Order{}, dbUnavailable("read order", err)
	}
	return item, nil
}

func scanOrder(row rowScanner) (Order, error) {
	var item Order
	var venueOrderID, modelID, strategyID, marketSource, conditionID, side, price, shares, averageFillPrice sql.NullString
	if err := row.Scan(&item.OrderID, &item.ClientOrderID, &venueOrderID, &item.Status,
		&item.ExecutionAccountID, &modelID, &strategyID, &item.Venue, &marketSource,
		&item.MarketID, &conditionID, &item.TokenID, &side, &price, &shares,
		&item.FilledShares, &averageFillPrice, &item.CreatedAt, &item.UpdatedAt); err != nil {
		return Order{}, err
	}
	item.VenueOrderID = venueOrderID.String
	item.ModelID = modelID.String
	item.StrategyID = strategyID.String
	item.MarketSource = marketSource.String
	item.ConditionID = conditionID.String
	item.Side = side.String
	item.Price = price.String
	item.Shares = shares.String
	item.AverageFillPrice = averageFillPrice.String
	item.CreatedAt = item.CreatedAt.UTC()
	item.UpdatedAt = item.UpdatedAt.UTC()
	return item, nil
}
