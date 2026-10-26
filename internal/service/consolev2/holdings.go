package consolev2

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// holdingGroupsSelect intentionally keeps the base group narrow. The page is
// selected from this relation before the display-only market label and mark
// joins run, so a large lot history cannot turn a small page into a full-table
// lookup.
const holdingGroupsSelect = `
SELECT l.execution_account_id, l.model_id, l.strategy_id,
       COALESCE(NULLIF(open.intent->>'market_source',''), '') AS market_source,
       l.market_id, l.condition_id, l.token_id, l.outcome_index,
       NULLIF(l.outcome_name, '') AS outcome_name,
       CASE
         WHEN bool_and(l.status = 'OPEN') THEN 'OPEN'
         WHEN bool_and(l.status = 'SETTLED_PENDING_REDEEM') THEN 'SETTLED_PENDING_REDEEM'
         ELSE 'MIXED'
       END AS status,
       SUM(l.remaining_shares) AS remaining_shares,
       SUM(l.remaining_cost) AS remaining_cost,
       COUNT(*)::bigint AS lot_count,
       MIN(l.opened_at) AS first_opened_at,
       MAX(l.opened_at) AS last_opened_at
FROM position_lots l
LEFT JOIN execution_orders open
  ON open.order_id = l.opening_order_id
 AND open.execution_account_id = l.execution_account_id
 AND open.market_id = l.market_id
 AND open.token_id = l.token_id
 AND COALESCE(open.intent->>'condition_id', '') = l.condition_id
`

const holdingGroupStatusExpression = `CASE
  WHEN bool_and(l.status = 'OPEN') THEN 'OPEN'
  WHEN bool_and(l.status = 'SETTLED_PENDING_REDEEM') THEN 'SETTLED_PENDING_REDEEM'
  ELSE 'MIXED'
END`

const holdingGroupsGroupBy = `
GROUP BY l.execution_account_id, l.model_id, l.strategy_id,
         COALESCE(NULLIF(open.intent->>'market_source',''), ''),
         l.market_id, l.condition_id, l.token_id, l.outcome_index,
         NULLIF(l.outcome_name, '')
`

// holdingLotsProjection deliberately repeats the strict opening-order join
// used by Holdings. The endpoint is an exact group-member lookup; it must not
// fall back to a fuzzy token search or leak metadata from another account.
const holdingLotsProjection = `
SELECT l.lot_id, l.execution_account_id, l.model_id, l.strategy_id,
       COALESCE(open.venue,''), COALESCE(NULLIF(open.intent->>'market_source',''), ''), l.market_id,
       l.condition_id, l.token_id, l.opening_order_id,
       l.opening_fill_key, l.status, l.original_shares::text, l.remaining_shares::text,
       l.original_cost::text, l.remaining_cost::text, l.average_entry_price::text,
       l.opened_at, l.closed_at
FROM position_lots l
LEFT JOIN execution_orders open
  ON open.order_id = l.opening_order_id
 AND open.execution_account_id = l.execution_account_id
 AND open.market_id = l.market_id
 AND open.token_id = l.token_id
 AND COALESCE(open.intent->>'condition_id', '') = l.condition_id
`

func normalizeHoldingIdentity(identity HoldingIdentity) (HoldingIdentity, error) {
	for name, value := range map[string]string{
		"execution_account_id": identity.ExecutionAccountID,
		"model_id":             identity.ModelID,
		"strategy_id":          identity.StrategyID,
		"market_source":        identity.MarketSource,
		"market_id":            identity.MarketID,
		"condition_id":         identity.ConditionID,
		"token_id":             identity.TokenID,
	} {
		if len(value) > maxQueryLen || strings.ContainsAny(value, "\x00\r\n") {
			return HoldingIdentity{}, fmt.Errorf("%s is invalid", name)
		}
	}
	if identity.OutcomeIndex != nil && (*identity.OutcomeIndex < 0 || *identity.OutcomeIndex > 1) {
		return HoldingIdentity{}, fmt.Errorf("outcome_index must be null, 0, or 1")
	}
	if identity.OutcomeName != nil {
		if len(*identity.OutcomeName) > maxQueryLen || strings.ContainsAny(*identity.OutcomeName, "\x00\r\n") {
			return HoldingIdentity{}, fmt.Errorf("outcome_name is invalid")
		}
		if *identity.OutcomeName == "" {
			identity.OutcomeName = nil
		}
	}
	return identity, nil
}

// Holdings is a current projection. It deliberately has no default age
// window: an old OPEN lot is still a current asset. Historical date filtering
// belongs to PositionLots, where opened_at is explicit.
func (reader *Reader) Holdings(ctx context.Context, filter ListFilter) (Page[Holding], error) {
	filter, err := normalizeFilter(filter)
	if err != nil {
		return Page[Holding]{}, err
	}
	if filter.From != nil || filter.To != nil {
		return Page[Holding]{}, fmt.Errorf("holdings does not support time filters")
	}
	if err := reader.requireDB(ctx); err != nil {
		return Page[Holding]{}, err
	}

	args := make([]any, 0, 12)
	conditions := []string{
		"l.status IN ('OPEN','SETTLED_PENDING_REDEEM')",
		"l.remaining_shares > 0",
	}
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
		conditions = append(conditions, "COALESCE(NULLIF(open.intent->>'market_source',''), '') = "+arg(filter.MarketSource))
	}
	if filter.Query != "" {
		placeholder := arg("%" + filter.Query + "%")
		conditions = append(conditions, "(l.model_id ILIKE "+placeholder+
			" OR l.strategy_id ILIKE "+placeholder+
			" OR l.execution_account_id ILIKE "+placeholder+
			" OR l.market_id ILIKE "+placeholder+
			" OR l.condition_id ILIKE "+placeholder+
			" OR l.token_id ILIKE "+placeholder+
			" OR l.outcome_name ILIKE "+placeholder+
			" OR open.intent->'metadata'->>'market_question' ILIKE "+placeholder+")")
	}
	where := strings.Join(conditions, " AND ")
	having := ""
	if filter.Status != "" {
		having = " HAVING " + holdingGroupStatusExpression + " = " + arg(filter.Status)
	}
	grouped := holdingGroupsSelect + " WHERE " + where + holdingGroupsGroupBy + having

	tx, err := reader.beginReadTx(ctx, "holdings")
	if err != nil {
		return Page[Holding]{}, err
	}
	defer tx.Rollback()

	total, err := countQuery(ctx, tx,
		"SELECT COUNT(*) FROM ("+grouped+") AS holding_groups",
		args, "holdings")
	if err != nil {
		return Page[Holding]{}, err
	}

	listArgs := append([]any(nil), args...)
	limit, offset := pageLimitOffset(filter, &listArgs)
	query := `
WITH holding_groups AS MATERIALIZED (` + grouped + `),
paged AS MATERIALIZED (
    SELECT *
    FROM holding_groups
    ORDER BY last_opened_at DESC, execution_account_id, model_id,
             strategy_id, market_source,
             market_id, condition_id, token_id,
             outcome_index NULLS LAST, outcome_name NULLS LAST
    LIMIT ` + limit + ` OFFSET ` + offset + `
)
SELECT paged.execution_account_id, paged.model_id, paged.strategy_id,
       paged.market_source, paged.market_id, paged.condition_id,
       paged.token_id,
       paged.outcome_index,
       paged.outcome_name,
       label.market_question, paged.status,
       paged.remaining_shares::text, paged.remaining_cost::text,
       paged.lot_count, paged.first_opened_at, paged.last_opened_at,
       position.mark_price::text, position.last_marked_at,
       CASE WHEN position.mark_price IS NULL THEN NULL
            ELSE (paged.remaining_shares * position.mark_price)::text END,
       CASE WHEN position.mark_price IS NULL THEN NULL
            ELSE (paged.remaining_shares * position.mark_price - paged.remaining_cost)::text END
FROM paged
LEFT JOIN execution_positions position
  ON position.execution_account_id = paged.execution_account_id
 AND position.market_id = paged.market_id
 AND position.condition_id = paged.condition_id
 AND position.token_id = paged.token_id
LEFT JOIN LATERAL (
    SELECT NULLIF(order_row.intent->'metadata'->>'market_question', '') AS market_question
    FROM execution_orders order_row
    WHERE order_row.execution_account_id = paged.execution_account_id
      AND order_row.market_id = paged.market_id
      AND order_row.token_id = paged.token_id
      AND COALESCE(order_row.intent->>'condition_id', '') = paged.condition_id
      AND COALESCE(order_row.intent->'metadata'->>'market_question', '') <> ''
    ORDER BY order_row.created_at DESC, order_row.order_id DESC
    LIMIT 1
) label ON TRUE
ORDER BY paged.last_opened_at DESC, paged.execution_account_id, paged.model_id,
         paged.strategy_id, paged.market_source, paged.market_id,
         paged.condition_id, paged.token_id,
         paged.outcome_index NULLS LAST, paged.outcome_name NULLS LAST
`
	rows, err := tx.QueryContext(ctx, query, listArgs...)
	if err != nil {
		return Page[Holding]{}, dbUnavailable("list holdings", err)
	}
	defer rows.Close()
	items := make([]Holding, 0, filter.Limit)
	for rows.Next() {
		item, scanErr := scanHolding(rows)
		if scanErr != nil {
			return Page[Holding]{}, dbUnavailable("scan holding", scanErr)
		}
		items = append(items, item)
	}
	if err := queryRowsError(rows, "iterate holdings"); err != nil {
		return Page[Holding]{}, dbUnavailable("iterate holdings", err)
	}
	page := finishPage(items, filter, total, reader.now().UTC(), map[string]any{
		"basis":          "current_positive_lots",
		"group_identity": "account:model:strategy:market_source:market:condition:token:outcome",
		"mark_source":    "matched_execution_position",
	})
	if err := tx.Commit(); err != nil {
		return Page[Holding]{}, dbUnavailable("commit holdings", err)
	}
	return page, nil
}

// HoldingLots returns the exact positive current lot members for one complete
// Holdings grouping identity. It intentionally has no time filter or fuzzy q:
// the caller already selected a precise group, and history belongs to
// PositionLots.
func (reader *Reader) HoldingLots(ctx context.Context, identity HoldingIdentity, filter ListFilter) (Page[PositionLot], error) {
	filter, err := normalizeFilter(filter)
	if err != nil {
		return Page[PositionLot]{}, err
	}
	if filter.From != nil || filter.To != nil {
		return Page[PositionLot]{}, fmt.Errorf("holding lots do not support time filters")
	}
	if filter.Query != "" || filter.ModelID != "" || filter.StrategyID != "" ||
		filter.ExecutionAccountID != "" || filter.MarketSource != "" ||
		filter.Status != "" || filter.Side != "" {
		return Page[PositionLot]{}, fmt.Errorf("holding lots only support limit and offset")
	}
	identity, err = normalizeHoldingIdentity(identity)
	if err != nil {
		return Page[PositionLot]{}, err
	}
	if err := reader.requireDB(ctx); err != nil {
		return Page[PositionLot]{}, err
	}

	var outcomeIndexArg any
	if identity.OutcomeIndex != nil {
		outcomeIndexArg = *identity.OutcomeIndex
	}
	var outcomeNameArg any
	if identity.OutcomeName != nil {
		outcomeNameArg = *identity.OutcomeName
	}
	args := []any{
		identity.ExecutionAccountID,
		identity.ModelID,
		identity.StrategyID,
		identity.MarketSource,
		identity.MarketID,
		identity.ConditionID,
		identity.TokenID,
		outcomeIndexArg,
		outcomeNameArg,
	}
	conditions := []string{
		"l.status IN ('OPEN','SETTLED_PENDING_REDEEM')",
		"l.remaining_shares > 0",
		"l.execution_account_id = $1",
		"l.model_id = $2",
		"l.strategy_id = $3",
		"COALESCE(NULLIF(open.intent->>'market_source',''), '') = $4",
		"l.market_id = $5",
		"l.condition_id = $6",
		"l.token_id = $7",
		"l.outcome_index IS NOT DISTINCT FROM $8",
		"NULLIF(l.outcome_name, '') IS NOT DISTINCT FROM $9",
	}
	where := strings.Join(conditions, " AND ")
	tx, err := reader.beginReadTx(ctx, "holding lots")
	if err != nil {
		return Page[PositionLot]{}, err
	}
	defer tx.Rollback()

	total, err := countProjection(ctx, tx, holdingLotsProjection, where, args, "holding lots")
	if err != nil {
		return Page[PositionLot]{}, err
	}
	listArgs := append([]any(nil), args...)
	limit, offset := pageLimitOffset(filter, &listArgs)
	rows, err := tx.QueryContext(ctx, holdingLotsProjection+" WHERE "+where+
		" ORDER BY l.opened_at DESC, l.lot_id DESC LIMIT "+limit+" OFFSET "+offset, listArgs...)
	if err != nil {
		return Page[PositionLot]{}, dbUnavailable("list holding lots", err)
	}
	defer rows.Close()
	items := make([]PositionLot, 0, filter.Limit)
	for rows.Next() {
		item, scanErr := scanPositionLot(rows)
		if scanErr != nil {
			return Page[PositionLot]{}, dbUnavailable("scan holding lot", scanErr)
		}
		items = append(items, item)
	}
	if err := queryRowsError(rows, "iterate holding lots"); err != nil {
		return Page[PositionLot]{}, dbUnavailable("iterate holding lots", err)
	}
	page := finishPage(items, filter, total, reader.now().UTC(), map[string]any{
		"basis":            "current_positive_lots",
		"holding_identity": true,
	})
	if err := tx.Commit(); err != nil {
		return Page[PositionLot]{}, dbUnavailable("commit holding lots", err)
	}
	return page, nil
}

func scanHolding(row rowScanner) (Holding, error) {
	var item Holding
	var marketSource, conditionID, outcomeName, marketQuestion, markPrice, marketValue, unrealizedPnL sql.NullString
	var outcomeIndex sql.NullInt64
	var markObservedAt sql.NullTime
	var lotCount int64
	if err := row.Scan(
		&item.ExecutionAccountID, &item.ModelID, &item.StrategyID, &marketSource,
		&item.MarketID, &conditionID, &item.TokenID, &outcomeIndex, &outcomeName,
		&marketQuestion, &item.Status, &item.RemainingShares, &item.RemainingCost,
		&lotCount, &item.FirstOpenedAt, &item.LastOpenedAt, &markPrice,
		&markObservedAt, &marketValue, &unrealizedPnL,
	); err != nil {
		return Holding{}, err
	}
	if lotCount < 0 || lotCount > int64(^uint(0)>>1) {
		return Holding{}, fmt.Errorf("holding lot count is outside the supported integer range")
	}
	item.MarketSource, item.ConditionID = marketSource.String, conditionID.String
	item.LotCount = int(lotCount)
	item.OutcomeIndex = scanNullableInt64AsInt(outcomeIndex)
	item.OutcomeName, item.MarketQuestion = scanNullableString(outcomeName), scanNullableString(marketQuestion)
	item.MarkPrice, item.MarketValue, item.UnrealizedPnL = scanNullableString(markPrice), scanNullableString(marketValue), scanNullableString(unrealizedPnL)
	item.MarkObservedAt = scanNullableTime(markObservedAt)
	item.FirstOpenedAt = item.FirstOpenedAt.UTC()
	item.LastOpenedAt = item.LastOpenedAt.UTC()
	return item, nil
}
