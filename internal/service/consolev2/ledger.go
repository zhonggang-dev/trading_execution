package consolev2

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const positionLotProjection = `
SELECT l.lot_id, l.execution_account_id, l.model_id, l.strategy_id,
       COALESCE(open.venue,''), COALESCE(open.intent->>'market_source',''), l.market_id,
       l.condition_id, l.token_id, l.opening_order_id,
       l.opening_fill_key, l.status, l.original_shares::text, l.remaining_shares::text,
       l.original_cost::text, l.remaining_cost::text, l.average_entry_price::text,
       l.opened_at, l.closed_at
FROM position_lots l
LEFT JOIN execution_orders open ON open.order_id=l.opening_order_id
`

func (reader *Reader) PositionLots(ctx context.Context, filter ListFilter) (Page[PositionLot], error) {
	filter, err := normalizeFilter(filter)
	if err != nil {
		return Page[PositionLot]{}, err
	}
	if err := reader.requireDB(ctx); err != nil {
		return Page[PositionLot]{}, err
	}
	now := reader.now().UTC()
	args := make([]any, 0, 14)
	conditions := []string{"1=1"}
	arg := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}
	if filter.ModelID != "" {
		conditions = append(conditions, "l.model_id = "+arg(filter.ModelID))
	}
	if filter.StrategyID != "" {
		conditions = append(conditions, "l.strategy_id = "+arg(filter.StrategyID))
	}
	if filter.ExecutionAccountID != "" {
		conditions = append(conditions, "l.execution_account_id = "+arg(filter.ExecutionAccountID))
	}
	if filter.MarketSource != "" {
		conditions = append(conditions, "open.intent->>'market_source' = "+arg(filter.MarketSource))
	}
	if filter.Status != "" {
		conditions = append(conditions, "l.status = "+arg(filter.Status))
	}
	if filter.Query != "" {
		placeholder := arg("%" + filter.Query + "%")
		conditions = append(conditions, "(l.lot_id ILIKE "+placeholder+" OR l.opening_order_id ILIKE "+placeholder+
			" OR l.market_id ILIKE "+placeholder+" OR l.token_id ILIKE "+placeholder+")")
	}
	if filter.From != nil {
		conditions = append(conditions, "l.opened_at >= "+arg(filter.From.UTC()))
	} else {
		conditions = append(conditions, "(l.opened_at >= "+arg(boundedSince(filter, now))+
			" OR l.status IN ('OPEN','SETTLED_PENDING_REDEEM'))")
	}
	if filter.To != nil {
		conditions = append(conditions, "l.opened_at <= "+arg(filter.To.UTC()))
	}
	args = append(args, filter.Limit+1)
	limit := fmt.Sprintf("$%d", len(args))
	args = append(args, filter.Offset)
	offset := fmt.Sprintf("$%d", len(args))
	rows, err := reader.db.QueryContext(ctx, positionLotProjection+" WHERE "+strings.Join(conditions, " AND ")+
		" ORDER BY l.opened_at DESC, l.lot_id DESC LIMIT "+limit+" OFFSET "+offset, args...)
	if err != nil {
		return Page[PositionLot]{}, dbUnavailable("list position lots", err)
	}
	defer rows.Close()
	items := make([]PositionLot, 0, filter.Limit)
	for rows.Next() {
		item, scanErr := scanPositionLot(rows)
		if scanErr != nil {
			return Page[PositionLot]{}, dbUnavailable("scan position lot", scanErr)
		}
		items = append(items, item)
	}
	if err := queryRowsError(rows, "iterate position lots"); err != nil {
		return Page[PositionLot]{}, dbUnavailable("iterate position lots", err)
	}
	return finishPage(items, filter, now, map[string]any{"basis": "bounded_recent_or_open_lots"}), nil
}

func scanPositionLot(row rowScanner) (PositionLot, error) {
	var item PositionLot
	var venue, source, conditionID, originalShares, remainingShares, originalCost, remainingCost, averagePrice sql.NullString
	var closed sql.NullTime
	if err := row.Scan(&item.LotID, &item.ExecutionAccountID, &item.ModelID, &item.StrategyID,
		&venue, &source, &item.MarketID, &conditionID, &item.TokenID, &item.OpeningOrderID,
		&item.OpeningFillKey, &item.Status, &originalShares, &remainingShares, &originalCost,
		&remainingCost, &averagePrice, &item.OpenedAt, &closed); err != nil {
		return PositionLot{}, err
	}
	item.Venue, item.MarketSource, item.ConditionID = venue.String, source.String, conditionID.String
	item.OriginalShares, item.RemainingShares = originalShares.String, remainingShares.String
	item.OriginalCost, item.RemainingCost, item.AverageEntryPrice = originalCost.String, remainingCost.String, averagePrice.String
	item.OpenedAt = item.OpenedAt.UTC()
	item.ClosedAt = scanNullableTime(closed)
	return item, nil
}

const reconciliationRunProjection = `
SELECT run_id, execution_account_id, trigger, NULLIF(focus_order_id,''), status,
       started_at, completed_at
FROM reconciliation_runs
`

func (reader *Reader) ReconciliationRuns(ctx context.Context, filter ListFilter) (Page[ReconciliationRun], error) {
	filter, err := normalizeFilter(filter)
	if err != nil {
		return Page[ReconciliationRun]{}, err
	}
	if err := reader.requireDB(ctx); err != nil {
		return Page[ReconciliationRun]{}, err
	}
	now := reader.now().UTC()
	args := make([]any, 0, 10)
	conditions := []string{"1=1"}
	arg := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}
	if filter.ExecutionAccountID != "" {
		conditions = append(conditions, "execution_account_id = "+arg(filter.ExecutionAccountID))
	}
	if filter.Status != "" {
		conditions = append(conditions, "status = "+arg(filter.Status))
	}
	if filter.Query != "" {
		placeholder := arg("%" + filter.Query + "%")
		conditions = append(conditions, "(run_id ILIKE "+placeholder+" OR focus_order_id ILIKE "+placeholder+" OR trigger ILIKE "+placeholder+")")
	}
	if filter.From != nil {
		conditions = append(conditions, "started_at >= "+arg(filter.From.UTC()))
	} else {
		conditions = append(conditions, "(started_at >= "+arg(boundedSince(filter, now))+
			" OR status IN ('RUNNING','ATTENTION_REQUIRED'))")
	}
	if filter.To != nil {
		conditions = append(conditions, "started_at <= "+arg(filter.To.UTC()))
	}
	args = append(args, filter.Limit+1)
	limit := fmt.Sprintf("$%d", len(args))
	args = append(args, filter.Offset)
	offset := fmt.Sprintf("$%d", len(args))
	rows, err := reader.db.QueryContext(ctx, reconciliationRunProjection+" WHERE "+strings.Join(conditions, " AND ")+
		" ORDER BY started_at DESC, run_id DESC LIMIT "+limit+" OFFSET "+offset, args...)
	if err != nil {
		return Page[ReconciliationRun]{}, dbUnavailable("list reconciliation runs", err)
	}
	defer rows.Close()
	items := make([]ReconciliationRun, 0, filter.Limit)
	for rows.Next() {
		var item ReconciliationRun
		var focus sql.NullString
		var completed sql.NullTime
		if err := rows.Scan(&item.RunID, &item.ExecutionAccountID, &item.Trigger, &focus, &item.Status,
			&item.StartedAt, &completed); err != nil {
			return Page[ReconciliationRun]{}, dbUnavailable("scan reconciliation run", err)
		}
		item.FocusOrderID = focus.String
		item.StartedAt = item.StartedAt.UTC()
		item.CompletedAt = scanNullableTime(completed)
		items = append(items, item)
	}
	if err := queryRowsError(rows, "iterate reconciliation runs"); err != nil {
		return Page[ReconciliationRun]{}, dbUnavailable("iterate reconciliation runs", err)
	}
	return finishPage(items, filter, now, map[string]any{"basis": "bounded_recent_or_attention_runs"}), nil
}

const reconciliationIssueProjection = `
SELECT issue.issue_id, issue.run_id, issue.execution_account_id, issue.issue_type,
       NULL::text, issue.resolution, issue.status, issue.order_id,
       issue.venue_order_id, issue.venue_trade_id, issue.market_id, issue.condition_id,
       issue.token_id, issue.local_value::text, issue.remote_value::text, issue.source,
       issue.observed_at, issue.resolved_at
FROM reconciliation_issues issue
`

func (reader *Reader) ReconciliationIssues(ctx context.Context, filter ListFilter) (Page[ReconciliationIssue], error) {
	filter, err := normalizeFilter(filter)
	if err != nil {
		return Page[ReconciliationIssue]{}, err
	}
	if err := reader.requireDB(ctx); err != nil {
		return Page[ReconciliationIssue]{}, err
	}
	now := reader.now().UTC()
	args := make([]any, 0, 12)
	conditions := []string{"1=1"}
	arg := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}
	if filter.ExecutionAccountID != "" {
		conditions = append(conditions, "issue.execution_account_id = "+arg(filter.ExecutionAccountID))
	}
	if filter.Status != "" {
		conditions = append(conditions, "issue.status = "+arg(filter.Status))
	}
	if filter.Query != "" {
		placeholder := arg("%" + filter.Query + "%")
		conditions = append(conditions, "(issue.issue_id ILIKE "+placeholder+" OR issue.run_id ILIKE "+placeholder+
			" OR issue.order_id ILIKE "+placeholder+" OR issue.market_id ILIKE "+placeholder+
			" OR issue.token_id ILIKE "+placeholder+" OR issue.issue_type ILIKE "+placeholder+")")
	}
	if filter.From != nil {
		conditions = append(conditions, "issue.observed_at >= "+arg(filter.From.UTC()))
	} else {
		// Open issues are always included, even when they predate the recent
		// window, so an old unresolved drift cannot disappear from the console.
		conditions = append(conditions, "(issue.status = 'OPEN' OR issue.observed_at >= "+arg(boundedSince(filter, now))+")")
	}
	if filter.To != nil {
		conditions = append(conditions, "issue.observed_at <= "+arg(filter.To.UTC()))
	}
	args = append(args, filter.Limit+1)
	limit := fmt.Sprintf("$%d", len(args))
	args = append(args, filter.Offset)
	offset := fmt.Sprintf("$%d", len(args))
	rows, err := reader.db.QueryContext(ctx, reconciliationIssueProjection+" WHERE "+strings.Join(conditions, " AND ")+
		" ORDER BY CASE WHEN issue.status='OPEN' THEN 0 ELSE 1 END, issue.observed_at DESC, issue.issue_id DESC LIMIT "+limit+" OFFSET "+offset, args...)
	if err != nil {
		return Page[ReconciliationIssue]{}, dbUnavailable("list reconciliation issues", err)
	}
	defer rows.Close()
	items := make([]ReconciliationIssue, 0, filter.Limit)
	for rows.Next() {
		item, scanErr := scanReconciliationIssue(rows)
		if scanErr != nil {
			return Page[ReconciliationIssue]{}, dbUnavailable("scan reconciliation issue", scanErr)
		}
		items = append(items, item)
	}
	if err := queryRowsError(rows, "iterate reconciliation issues"); err != nil {
		return Page[ReconciliationIssue]{}, dbUnavailable("iterate reconciliation issues", err)
	}
	return finishPage(items, filter, now, map[string]any{"basis": "all_open_or_recent", "open_issues_unbounded_by_age": true}), nil
}

func scanReconciliationIssue(row rowScanner) (ReconciliationIssue, error) {
	var item ReconciliationIssue
	var reasonCode, localValue, remoteValue sql.NullString
	var resolved sql.NullTime
	if err := row.Scan(&item.IssueID, &item.RunID, &item.ExecutionAccountID, &item.IssueType,
		&reasonCode, &item.Resolution, &item.Status, &item.OrderID, &item.VenueOrderID,
		&item.VenueTradeID, &item.MarketID, &item.ConditionID, &item.TokenID,
		&localValue, &remoteValue, &item.Source, &item.ObservedAt, &resolved); err != nil {
		return ReconciliationIssue{}, err
	}
	item.ReasonCode = scanNullableString(reasonCode)
	item.LocalValue, item.RemoteValue = scanNullableString(localValue), scanNullableString(remoteValue)
	item.ObservedAt = item.ObservedAt.UTC()
	item.ResolvedAt = scanNullableTime(resolved)
	return item, nil
}

const redemptionProjection = `
SELECT execution_account_id || ':' || condition_id AS redemption_id,
       execution_account_id, condition_id, status, NULLIF(transaction_hash,''),
       payout_base_units::text, receipt_block_number, confirmations, created_at,
       updated_at, confirmed_at, applied_at
FROM polymarket_redemptions
`

func (reader *Reader) Redemptions(ctx context.Context, filter ListFilter) (Page[Redemption], error) {
	filter, err := normalizeFilter(filter)
	if err != nil {
		return Page[Redemption]{}, err
	}
	if err := reader.requireDB(ctx); err != nil {
		return Page[Redemption]{}, err
	}
	now := reader.now().UTC()
	args := make([]any, 0, 10)
	conditions := []string{"1=1"}
	arg := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}
	if filter.ExecutionAccountID != "" {
		conditions = append(conditions, "execution_account_id = "+arg(filter.ExecutionAccountID))
	}
	if filter.Status != "" {
		conditions = append(conditions, "status = "+arg(filter.Status))
	}
	if filter.Query != "" {
		placeholder := arg("%" + filter.Query + "%")
		conditions = append(conditions, "(condition_id ILIKE "+placeholder+" OR transaction_hash ILIKE "+placeholder+
			" OR execution_account_id ILIKE "+placeholder+")")
	}
	if filter.From != nil {
		conditions = append(conditions, "created_at >= "+arg(filter.From.UTC()))
	} else {
		conditions = append(conditions, "(created_at >= "+arg(boundedSince(filter, now))+
			" OR status <> 'APPLIED')")
	}
	if filter.To != nil {
		conditions = append(conditions, "created_at <= "+arg(filter.To.UTC()))
	}
	args = append(args, filter.Limit+1)
	limit := fmt.Sprintf("$%d", len(args))
	args = append(args, filter.Offset)
	offset := fmt.Sprintf("$%d", len(args))
	rows, err := reader.db.QueryContext(ctx, redemptionProjection+" WHERE "+strings.Join(conditions, " AND ")+
		" ORDER BY CASE WHEN status <> 'APPLIED' THEN 0 ELSE 1 END, updated_at DESC, redemption_id DESC LIMIT "+limit+" OFFSET "+offset, args...)
	if err != nil {
		return Page[Redemption]{}, dbUnavailable("list redemptions", err)
	}
	defer rows.Close()
	items := make([]Redemption, 0, filter.Limit)
	for rows.Next() {
		var item Redemption
		var txHash, payout sql.NullString
		var block sql.NullInt64
		var confirmed, applied sql.NullTime
		if err := rows.Scan(&item.RedemptionID, &item.ExecutionAccountID, &item.ConditionID, &item.Status,
			&txHash, &payout, &block, &item.Confirmations, &item.CreatedAt, &item.UpdatedAt,
			&confirmed, &applied); err != nil {
			return Page[Redemption]{}, dbUnavailable("scan redemption", err)
		}
		item.TransactionHash, item.PayoutBaseUnits = txHash.String, scanNullableString(payout)
		item.ReceiptBlockNumber = scanNullableInt64(block)
		item.CreatedAt, item.UpdatedAt = item.CreatedAt.UTC(), item.UpdatedAt.UTC()
		item.ConfirmedAt, item.AppliedAt = scanNullableTime(confirmed), scanNullableTime(applied)
		items = append(items, item)
	}
	if err := queryRowsError(rows, "iterate redemptions"); err != nil {
		return Page[Redemption]{}, dbUnavailable("iterate redemptions", err)
	}
	return finishPage(items, filter, now, map[string]any{"basis": "bounded_recent_or_pending_or_manual_review_redemptions", "redemption_id": "execution_account_id:condition_id"}), nil
}
