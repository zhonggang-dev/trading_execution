package consolev2

import (
	"context"
	"database/sql"
	"strings"
)

func (reader *Reader) OrderTrace(ctx context.Context, orderID string) (OrderTrace, error) {
	orderID = strings.TrimSpace(orderID)
	if orderID == "" || len(orderID) > maxQueryLen || strings.ContainsAny(orderID, "\x00\r\n/") {
		return OrderTrace{}, ErrNotFound
	}
	if err := reader.requireDB(ctx); err != nil {
		return OrderTrace{}, err
	}
	order, err := reader.loadOrder(ctx, orderID)
	if err != nil {
		return OrderTrace{}, err
	}
	trace := OrderTrace{Order: order, ObservedAt: reader.now().UTC()}
	trace.Deliveries, err = reader.loadOrderDeliveries(ctx, orderID)
	if err != nil {
		return OrderTrace{}, err
	}
	trace.Events, err = reader.loadOrderEvents(ctx, orderID)
	if err != nil {
		return OrderTrace{}, err
	}
	trace.Attempts, err = reader.loadOrderAttempts(ctx, orderID)
	if err != nil {
		return OrderTrace{}, err
	}
	trace.Fills, err = reader.loadOrderFills(ctx, orderID)
	if err != nil {
		return OrderTrace{}, err
	}
	trace.PositionLots, err = reader.loadOrderLots(ctx, orderID)
	if err != nil {
		return OrderTrace{}, err
	}
	trace.ReconciliationIssues, err = reader.loadOrderIssues(ctx, orderID)
	if err != nil {
		return OrderTrace{}, err
	}
	return trace, nil
}

func (reader *Reader) loadOrderDeliveries(ctx context.Context, orderID string) ([]Delivery, error) {
	rows, err := reader.db.QueryContext(ctx, `
		SELECT client_order_id, cycle_id, sequence_no, status, attempt_count,
		       order_id, order_status, created_at, completed_at
		FROM strategy_order_intent_deliveries
		WHERE order_id=$1
		ORDER BY created_at, sequence_no
		LIMIT 100`, orderID)
	if err != nil {
		return nil, dbUnavailable("list order deliveries", err)
	}
	defer rows.Close()
	items := make([]Delivery, 0)
	for rows.Next() {
		var item Delivery
		var storedOrderID, orderStatus sql.NullString
		var completed sql.NullTime
		if err := rows.Scan(&item.ClientOrderID, &item.CycleID, &item.SequenceNo, &item.Status, &item.AttemptCount,
			&storedOrderID, &orderStatus, &item.CreatedAt, &completed); err != nil {
			return nil, dbUnavailable("scan order delivery", err)
		}
		item.OrderID, item.OrderStatus = storedOrderID.String, orderStatus.String
		item.Failed = item.Status == "FAILED" || item.Status == "UNKNOWN"
		item.CreatedAt = item.CreatedAt.UTC()
		item.CompletedAt = scanNullableTime(completed)
		items = append(items, item)
	}
	if err := queryRowsError(rows, "iterate order deliveries"); err != nil {
		return nil, dbUnavailable("iterate order deliveries", err)
	}
	return items, nil
}

func (reader *Reader) loadOrderEvents(ctx context.Context, orderID string) ([]OrderEvent, error) {
	rows, err := reader.db.QueryContext(ctx, `
		SELECT event_id, order_id, revision, from_status, to_status, trigger,
		       attempt_id, fill_key, reason_code, venue_status, venue_order_id,
		       filled_size::text, COALESCE(average_fill_price::text,''),
		       venue_observed_at, occurred_at
		FROM execution_order_events
		WHERE order_id=$1
		ORDER BY revision
		LIMIT 100`, orderID)
	if err != nil {
		return nil, dbUnavailable("list order events", err)
	}
	defer rows.Close()
	items := make([]OrderEvent, 0)
	for rows.Next() {
		var item OrderEvent
		var fromStatus, attemptID, fillKey, reasonCode, venueStatus, venueOrderID, filled, average sql.NullString
		var venueObserved sql.NullTime
		if err := rows.Scan(&item.EventID, &item.OrderID, &item.Revision, &fromStatus, &item.ToStatus,
			&item.Trigger, &attemptID, &fillKey, &reasonCode, &venueStatus, &venueOrderID,
			&filled, &average, &venueObserved, &item.OccurredAt); err != nil {
			return nil, dbUnavailable("scan order event", err)
		}
		item.FromStatus, item.AttemptID, item.FillKey = fromStatus.String, attemptID.String, fillKey.String
		item.ReasonCode, item.VenueStatus, item.VenueOrderID = reasonCode.String, venueStatus.String, venueOrderID.String
		item.FilledShares, item.AverageFillPrice = filled.String, average.String
		item.VenueObservedAt = scanNullableTime(venueObserved)
		item.OccurredAt = item.OccurredAt.UTC()
		items = append(items, item)
	}
	if err := queryRowsError(rows, "iterate order events"); err != nil {
		return nil, dbUnavailable("iterate order events", err)
	}
	return items, nil
}

func (reader *Reader) loadOrderAttempts(ctx context.Context, orderID string) ([]OrderAttempt, error) {
	rows, err := reader.db.QueryContext(ctx, `
		SELECT attempt_id, order_id, sequence, kind, outcome,
		       NULLIF(venue_order_id,''), NULLIF(venue_status,''), http_status,
		       NULLIF(error_code,''), started_at, completed_at
		FROM execution_order_attempts
		WHERE order_id=$1
		ORDER BY sequence
		LIMIT 100`, orderID)
	if err != nil {
		return nil, dbUnavailable("list order attempts", err)
	}
	defer rows.Close()
	items := make([]OrderAttempt, 0)
	for rows.Next() {
		var item OrderAttempt
		var venueOrderID, venueStatus, errorCode sql.NullString
		var httpStatus sql.NullInt64
		var completed sql.NullTime
		if err := rows.Scan(&item.AttemptID, &item.OrderID, &item.Sequence, &item.Kind, &item.Outcome,
			&venueOrderID, &venueStatus, &httpStatus, &errorCode, &item.StartedAt, &completed); err != nil {
			return nil, dbUnavailable("scan order attempt", err)
		}
		item.VenueOrderID, item.VenueStatus, item.ErrorCode = venueOrderID.String, venueStatus.String, errorCode.String
		item.HTTPStatus = scanNullableInt64AsInt(httpStatus)
		item.StartedAt = item.StartedAt.UTC()
		item.CompletedAt = scanNullableTime(completed)
		items = append(items, item)
	}
	if err := queryRowsError(rows, "iterate order attempts"); err != nil {
		return nil, dbUnavailable("iterate order attempts", err)
	}
	return items, nil
}

func scanNullableInt64AsInt(value sql.NullInt64) *int {
	if !value.Valid {
		return nil
	}
	result := int(value.Int64)
	return &result
}

func (reader *Reader) loadOrderFills(ctx context.Context, orderID string) ([]Fill, error) {
	rows, err := reader.db.QueryContext(ctx, `
		SELECT fill_key, venue, venue_fill_id, order_id, venue_order_id,
		       execution_account_id, market_id, condition_id, token_id, side,
		       status, shares::text, price::text, total_fee::text, net_cash_delta::text,
		       transaction_hash, matched_at, confirmed_at, applied_at
		FROM execution_fills
		WHERE order_id=$1
		ORDER BY matched_at, fill_key
		LIMIT 100`, orderID)
	if err != nil {
		return nil, dbUnavailable("list order fills", err)
	}
	defer rows.Close()
	items := make([]Fill, 0)
	for rows.Next() {
		var item Fill
		var confirmed, applied sql.NullTime
		if err := rows.Scan(&item.FillKey, &item.Venue, &item.VenueFillID, &item.OrderID, &item.VenueOrderID,
			&item.ExecutionAccountID, &item.MarketID, &item.ConditionID, &item.TokenID, &item.Side,
			&item.Status, &item.Shares, &item.Price, &item.TotalFee, &item.NetCashDelta,
			&item.TransactionHash, &item.MatchedAt, &confirmed, &applied); err != nil {
			return nil, dbUnavailable("scan order fill", err)
		}
		item.MatchedAt = item.MatchedAt.UTC()
		item.ConfirmedAt, item.AppliedAt = scanNullableTime(confirmed), scanNullableTime(applied)
		items = append(items, item)
	}
	if err := queryRowsError(rows, "iterate order fills"); err != nil {
		return nil, dbUnavailable("iterate order fills", err)
	}
	return items, nil
}

func (reader *Reader) loadOrderLots(ctx context.Context, orderID string) ([]PositionLot, error) {
	rows, err := reader.db.QueryContext(ctx, `
		SELECT DISTINCT l.lot_id, l.execution_account_id, l.model_id, l.strategy_id,
		       COALESCE(open.venue,''), COALESCE(open.intent->>'market_source',''), l.market_id,
		       l.condition_id, l.token_id, l.opening_order_id,
		       l.opening_fill_key, l.status, l.original_shares::text, l.remaining_shares::text,
		       l.original_cost::text, l.remaining_cost::text, l.average_entry_price::text,
		       l.opened_at, l.closed_at
		FROM position_lots l
		LEFT JOIN execution_orders open ON open.order_id=l.opening_order_id
		LEFT JOIN position_lot_closures closure ON closure.lot_id=l.lot_id
		LEFT JOIN execution_fills closing_fill ON closing_fill.fill_key=closure.closing_fill_key
		WHERE l.opening_order_id=$1 OR closing_fill.order_id=$1
		ORDER BY l.opened_at, l.lot_id
		LIMIT 100`, orderID)
	if err != nil {
		return nil, dbUnavailable("list order position lots", err)
	}
	defer rows.Close()
	items := make([]PositionLot, 0)
	for rows.Next() {
		item, scanErr := scanPositionLot(rows)
		if scanErr != nil {
			return nil, dbUnavailable("scan order position lot", scanErr)
		}
		items = append(items, item)
	}
	if err := queryRowsError(rows, "iterate order position lots"); err != nil {
		return nil, dbUnavailable("iterate order position lots", err)
	}
	return items, nil
}

func (reader *Reader) loadOrderIssues(ctx context.Context, orderID string) ([]ReconciliationIssue, error) {
	rows, err := reader.db.QueryContext(ctx, reconciliationIssueProjection+`
		WHERE issue.order_id=$1 OR issue.venue_order_id=$1
		ORDER BY issue.observed_at DESC, issue.issue_id DESC
		LIMIT 100`, orderID)
	if err != nil {
		return nil, dbUnavailable("list order reconciliation issues", err)
	}
	defer rows.Close()
	items := make([]ReconciliationIssue, 0)
	for rows.Next() {
		item, scanErr := scanReconciliationIssue(rows)
		if scanErr != nil {
			return nil, dbUnavailable("scan order reconciliation issue", scanErr)
		}
		items = append(items, item)
	}
	if err := queryRowsError(rows, "iterate order reconciliation issues"); err != nil {
		return nil, dbUnavailable("iterate order reconciliation issues", err)
	}
	return items, nil
}
