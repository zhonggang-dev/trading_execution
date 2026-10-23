package consolev2

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const orderBaseSelect = `
SELECT o.order_id, o.client_order_id, NULLIF(o.venue_order_id,''), o.status,
       o.execution_account_id, COALESCE(o.intent->>'model_id',''),
       COALESCE(o.intent->>'strategy_id',''), o.venue,
       COALESCE(o.intent->>'market_source',''), o.market_id,
       COALESCE(o.intent->>'condition_id',''), o.token_id,
       NULLIF(o.intent->'metadata'->>'market_question',''),
       NULLIF(o.intent->>'outcome_name',''),
       COALESCE(o.intent->>'side',''),
	       COALESCE(NULLIF(o.intent->>'price',''), NULLIF(o.intent->>'worst_price',''), ''),
       COALESCE(o.intent->>'size',''), o.filled_size::text,
       NULLIF(o.average_fill_price::text,''), o.created_at, o.updated_at
`

const orderBaseProjection = orderBaseSelect + `
FROM execution_orders o
`

const orderProjection = orderBaseSelect + `,
       COALESCE(fill_evidence.total_count, 0), COALESCE(fill_evidence.confirmed_count, 0),
       COALESCE(fill_evidence.applied_count, 0), COALESCE(fill_evidence.pending_count, 0),
       CASE
         WHEN COALESCE(fill_evidence.total_count, 0) = 0 THEN 'none'
         WHEN COALESCE(fill_evidence.confirmed_count, 0) = fill_evidence.total_count THEN 'confirmed'
         WHEN COALESCE(fill_evidence.pending_count, 0) > 0 THEN 'finality_pending'
         ELSE 'observed'
       END,
       CASE
         WHEN COALESCE(fill_evidence.applied_count, 0) = 0 THEN 'not_applied'
         WHEN fill_evidence.applied_count = fill_evidence.total_count THEN 'applied'
         ELSE 'partially_applied'
       END,
       COALESCE(open_issues.open_issue_count, 0),
       COALESCE(reservation.status, '')
FROM execution_orders o
LEFT JOIN LATERAL (
	    SELECT count(*)::bigint AS total_count,
	           count(*) FILTER (WHERE f.confirmed_at IS NOT NULL)::bigint AS confirmed_count,
	           count(*) FILTER (WHERE f.applied_at IS NOT NULL)::bigint AS applied_count,
	           count(*) FILTER (WHERE f.status <> 'FAILED' AND f.confirmed_at IS NULL)::bigint AS pending_count
    FROM execution_fills f
    WHERE f.order_id=o.order_id
) fill_evidence ON TRUE
LEFT JOIN LATERAL (
	    SELECT count(*)::bigint AS open_issue_count
    FROM reconciliation_issues i
    WHERE i.order_id=o.order_id AND i.status='OPEN'
) open_issues ON TRUE
LEFT JOIN LATERAL (
    SELECT r.status
    FROM asset_reservations r
    WHERE r.order_id=o.order_id
    ORDER BY r.updated_at DESC NULLS LAST
    LIMIT 1
) reservation ON TRUE
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
	where := strings.Join(conditions, " AND ")
	tx, err := reader.beginReadTx(ctx, "orders")
	if err != nil {
		return Page[Order]{}, err
	}
	defer tx.Rollback()
	// Count the same execution_orders identity with the shared predicate instead
	// of replaying display-only evidence aggregates for every historical row.
	total, err := countFrom(ctx, tx, "execution_orders o", where, args, "orders")
	if err != nil {
		return Page[Order]{}, err
	}
	listArgs := append([]any(nil), args...)
	limit, offset := pageLimitOffset(filter, &listArgs)
	query := orderBaseProjection + " WHERE " + where +
		" ORDER BY o.updated_at DESC, o.order_id DESC LIMIT " + limit + " OFFSET " + offset
	rows, err := tx.QueryContext(ctx, query, listArgs...)
	if err != nil {
		return Page[Order]{}, dbUnavailable("list orders", err)
	}
	defer rows.Close()
	items := make([]Order, 0, filter.Limit)
	for rows.Next() {
		item, scanErr := scanOrderBase(rows)
		if scanErr != nil {
			return Page[Order]{}, dbUnavailable("scan order", scanErr)
		}
		items = append(items, item)
	}
	if err := queryRowsError(rows, "iterate orders"); err != nil {
		return Page[Order]{}, dbUnavailable("iterate orders", err)
	}
	// Evidence is aggregated once for the displayed page IDs. Keeping these
	// bounded aggregates out of the base list query avoids a lateral seq scan of
	// all reconciliation issues for every order row.
	evidenceCount := len(items)
	if evidenceCount > filter.Limit {
		evidenceCount = filter.Limit
	}
	if evidenceCount > 0 {
		ids := make([]string, evidenceCount)
		for index := range items[:evidenceCount] {
			ids[index] = items[index].OrderID
		}
		evidence, evidenceErr := loadOrderEvidence(ctx, tx, ids)
		if evidenceErr != nil {
			return Page[Order]{}, evidenceErr
		}
		for index := range items[:evidenceCount] {
			items[index].EvidenceSummary = evidence[items[index].OrderID]
		}
	}
	page := finishPage(items, filter, total, now, map[string]any{"basis": "bounded_recent_orders", "open_statuses_included": true})
	if err := tx.Commit(); err != nil {
		return Page[Order]{}, dbUnavailable("commit orders", err)
	}
	return page, nil
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

func scanOrderBase(row rowScanner) (Order, error) {
	var item Order
	var venueOrderID, modelID, strategyID, marketSource, conditionID, marketQuestion, outcomeName, side, price, shares, averageFillPrice sql.NullString
	if err := row.Scan(&item.OrderID, &item.ClientOrderID, &venueOrderID, &item.Status,
		&item.ExecutionAccountID, &modelID, &strategyID, &item.Venue, &marketSource,
		&item.MarketID, &conditionID, &item.TokenID, &marketQuestion, &outcomeName, &side, &price, &shares,
		&item.FilledShares, &averageFillPrice, &item.CreatedAt, &item.UpdatedAt); err != nil {
		return Order{}, err
	}
	populateOrderBase(&item, venueOrderID, modelID, strategyID, marketSource, conditionID, marketQuestion, outcomeName, side, price, shares, averageFillPrice)
	return item, nil
}

func scanOrder(row rowScanner) (Order, error) {
	var item Order
	var venueOrderID, modelID, strategyID, marketSource, conditionID, marketQuestion, outcomeName, side, price, shares, averageFillPrice sql.NullString
	var fillEvidenceState, accountingState, reservationState sql.NullString
	var totalFills, confirmedFills, appliedFills, pendingFills, openIssues int64
	if err := row.Scan(&item.OrderID, &item.ClientOrderID, &venueOrderID, &item.Status,
		&item.ExecutionAccountID, &modelID, &strategyID, &item.Venue, &marketSource,
		&item.MarketID, &conditionID, &item.TokenID, &marketQuestion, &outcomeName, &side, &price, &shares,
		&item.FilledShares, &averageFillPrice, &item.CreatedAt, &item.UpdatedAt,
		&totalFills, &confirmedFills, &appliedFills, &pendingFills,
		&fillEvidenceState, &accountingState, &openIssues, &reservationState); err != nil {
		return Order{}, err
	}
	populateOrderBase(&item, venueOrderID, modelID, strategyID, marketSource, conditionID, marketQuestion, outcomeName, side, price, shares, averageFillPrice)
	if totalFills < 0 || confirmedFills < 0 || appliedFills < 0 || pendingFills < 0 || openIssues < 0 ||
		totalFills > int64(maxInt()) || confirmedFills > int64(maxInt()) || appliedFills > int64(maxInt()) ||
		pendingFills > int64(maxInt()) || openIssues > int64(maxInt()) {
		return Order{}, fmt.Errorf("order evidence count is outside the supported integer range")
	}
	item.EvidenceSummary = OrderEvidenceSummary{
		FillEvidenceState:  fillEvidenceState.String,
		AccountingState:    accountingState.String,
		ConfirmedFillCount: int(confirmedFills),
		AppliedFillCount:   int(appliedFills),
		PendingFillCount:   int(pendingFills),
		OpenIssueCount:     int(openIssues),
		ReservationState:   reservationState.String,
	}
	return item, nil
}

func populateOrderBase(item *Order, venueOrderID, modelID, strategyID, marketSource, conditionID, marketQuestion, outcomeName, side, price, shares, averageFillPrice sql.NullString) {
	item.VenueOrderID = venueOrderID.String
	item.ModelID = modelID.String
	item.StrategyID = strategyID.String
	item.MarketSource = marketSource.String
	item.ConditionID = conditionID.String
	item.MarketQuestion = scanNullableString(marketQuestion)
	item.OutcomeName = scanNullableString(outcomeName)
	item.Side = side.String
	item.Price = price.String
	item.Shares = shares.String
	item.AverageFillPrice = averageFillPrice.String
	item.CreatedAt = item.CreatedAt.UTC()
	item.UpdatedAt = item.UpdatedAt.UTC()
}

func loadOrderEvidence(ctx context.Context, tx *sql.Tx, orderIDs []string) (map[string]OrderEvidenceSummary, error) {
	if len(orderIDs) == 0 {
		return map[string]OrderEvidenceSummary{}, nil
	}
	values := make([]string, len(orderIDs))
	args := make([]any, len(orderIDs))
	for index, orderID := range orderIDs {
		values[index] = fmt.Sprintf("($%d::text)", index+1)
		args[index] = orderID
	}
	query := `
WITH page_orders(order_id) AS MATERIALIZED (VALUES ` + strings.Join(values, ",") + `),
fill_evidence AS (
    SELECT f.order_id,
           count(*)::bigint AS total_count,
           count(*) FILTER (WHERE f.confirmed_at IS NOT NULL)::bigint AS confirmed_count,
           count(*) FILTER (WHERE f.applied_at IS NOT NULL)::bigint AS applied_count,
           count(*) FILTER (WHERE f.status <> 'FAILED' AND f.confirmed_at IS NULL)::bigint AS pending_count
    FROM execution_fills f
    JOIN page_orders p ON p.order_id=f.order_id
    GROUP BY f.order_id
),
open_issues AS (
    SELECT i.order_id, count(*)::bigint AS open_issue_count
    FROM reconciliation_issues i
    JOIN page_orders p ON p.order_id=i.order_id
    WHERE i.status='OPEN'
    GROUP BY i.order_id
)
SELECT p.order_id,
       COALESCE(f.total_count, 0), COALESCE(f.confirmed_count, 0),
       COALESCE(f.applied_count, 0), COALESCE(f.pending_count, 0),
       CASE
         WHEN COALESCE(f.total_count, 0) = 0 THEN 'none'
         WHEN COALESCE(f.confirmed_count, 0) = f.total_count THEN 'confirmed'
         WHEN COALESCE(f.pending_count, 0) > 0 THEN 'finality_pending'
         ELSE 'observed'
       END,
       CASE
         WHEN COALESCE(f.applied_count, 0) = 0 THEN 'not_applied'
         WHEN f.applied_count = f.total_count THEN 'applied'
         ELSE 'partially_applied'
       END,
       COALESCE(i.open_issue_count, 0), COALESCE(r.status, '')
FROM page_orders p
LEFT JOIN fill_evidence f ON f.order_id=p.order_id
LEFT JOIN open_issues i ON i.order_id=p.order_id
LEFT JOIN asset_reservations r ON r.order_id=p.order_id`
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, dbUnavailable("list order evidence", err)
	}
	defer rows.Close()
	evidence := make(map[string]OrderEvidenceSummary, len(orderIDs))
	for rows.Next() {
		var orderID, fillEvidenceState, accountingState, reservationState string
		var totalFills, confirmedFills, appliedFills, pendingFills, openIssues int64
		if err := rows.Scan(&orderID, &totalFills, &confirmedFills, &appliedFills, &pendingFills,
			&fillEvidenceState, &accountingState, &openIssues, &reservationState); err != nil {
			return nil, dbUnavailable("scan order evidence", err)
		}
		if totalFills < 0 || confirmedFills < 0 || appliedFills < 0 || pendingFills < 0 || openIssues < 0 ||
			totalFills > int64(maxInt()) || confirmedFills > int64(maxInt()) || appliedFills > int64(maxInt()) ||
			pendingFills > int64(maxInt()) || openIssues > int64(maxInt()) {
			return nil, dbUnavailable("scan order evidence", fmt.Errorf("order evidence count is outside the supported integer range"))
		}
		evidence[orderID] = OrderEvidenceSummary{
			FillEvidenceState:  fillEvidenceState,
			AccountingState:    accountingState,
			ConfirmedFillCount: int(confirmedFills),
			AppliedFillCount:   int(appliedFills),
			PendingFillCount:   int(pendingFills),
			OpenIssueCount:     int(openIssues),
			ReservationState:   reservationState,
		}
	}
	if err := queryRowsError(rows, "iterate order evidence"); err != nil {
		return nil, dbUnavailable("iterate order evidence", err)
	}
	return evidence, nil
}
