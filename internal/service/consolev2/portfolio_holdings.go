package consolev2

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// PortfolioHolding deliberately excludes the historical model/strategy from
// the asset identity. The member lots retain those immutable audit fields.
type PortfolioHolding struct {
	ExecutionAccountID string    `json:"execution_account_id"`
	MarketSource       string    `json:"market_source"`
	MarketID           string    `json:"market_id"`
	ConditionID        string    `json:"condition_id"`
	TokenID            string    `json:"token_id"`
	OutcomeIndex       *int      `json:"outcome_index"`
	OutcomeName        *string   `json:"outcome_name"`
	MarketQuestion     *string   `json:"market_question"`
	Status             string    `json:"status"`
	RemainingShares    string    `json:"remaining_shares"`
	RemainingCost      string    `json:"remaining_cost"`
	LotCount           int       `json:"lot_count"`
	FirstOpenedAt      time.Time `json:"first_opened_at"`
	LastOpenedAt       time.Time `json:"last_opened_at"`
}

type PortfolioHoldingIdentity struct {
	ExecutionAccountID string
	MarketSource       string
	MarketID           string
	ConditionID        string
	TokenID            string
	OutcomeIndex       *int
	OutcomeName        *string
}

// A frozen opening order is preferred to current routing. Legacy PM orders
// omit market_source, so their venue is valid evidence. Only when both are
// absent may a sealed, exact external-adoption baseline or the process-validated
// account directory supply the platform. Contradictory evidence remains NULL
// and causes a safe unavailable response.
const portfolioSourceExpression = `CASE
 WHEN NULLIF(UPPER(open.intent->>'market_source'),'') IS NOT NULL
      AND UPPER(open.intent->>'market_source') NOT IN ('POLYMARKET','KALSHI') THEN NULL
 WHEN UPPER(open.intent->>'market_source') IN ('POLYMARKET','KALSHI')
      AND UPPER(open.venue) IN ('POLYMARKET','KALSHI')
      AND UPPER(open.intent->>'market_source') <> UPPER(open.venue) THEN NULL
 WHEN l.external_adoption_id IS NOT NULL THEN
   CASE WHEN l.opening_order_id IS NULL AND l.opening_fill_key IS NULL
     AND adoption.lot_id IS NOT NULL AND baseline.source='POLYMARKET_DATA_API'
     AND baseline_item.token_id IS NOT NULL
     AND (configured.market_source IS NULL OR configured.market_source='POLYMARKET')
     AND COALESCE(NULLIF(UPPER(open.intent->>'market_source'),''),'POLYMARKET')='POLYMARKET'
     AND (UPPER(COALESCE(open.venue,'')) NOT IN ('POLYMARKET','KALSHI') OR UPPER(open.venue)='POLYMARKET')
   THEN 'POLYMARKET' ELSE NULL END
 ELSE COALESCE(NULLIF(UPPER(open.intent->>'market_source'),''),
       CASE WHEN UPPER(open.venue) IN ('POLYMARKET','KALSHI') THEN UPPER(open.venue) END,
       configured.market_source)
 END`

func (reader *Reader) portfolioBase(filter ListFilter) (string, []any, error) {
	filter, scopeErr := reader.normalizeScopedFilter(filter)
	if scopeErr != nil {
		return "", nil, scopeErr
	}
	sources := make([]ManagedAccount, 0, len(reader.config.ManagedAccounts))
	seen := map[string]string{}
	for _, account := range reader.config.ManagedAccounts {
		if account.ExecutionAccountID == "" || (account.MarketSource != "POLYMARKET" && account.MarketSource != "KALSHI") {
			return "", nil, ErrUnavailable
		}
		if previous, ok := seen[account.ExecutionAccountID]; ok {
			if previous != account.MarketSource {
				return "", nil, ErrUnavailable
			}
			continue
		}
		seen[account.ExecutionAccountID] = account.MarketSource
		sources = append(sources, ManagedAccount{ExecutionAccountID: account.ExecutionAccountID, MarketSource: account.MarketSource})
	}
	encoded, err := json.Marshal(sources)
	if err != nil {
		return "", nil, err
	}
	args := []any{string(encoded)}
	accountWhere := ""
	if filter.ExecutionAccountID != "" {
		args = append(args, filter.ExecutionAccountID)
		accountWhere = " AND l.execution_account_id=$2"
	}
	scopeClauses := appendAccountScope(nil, &args, filter, "l.execution_account_id")
	if len(scopeClauses) > 0 {
		accountWhere += " AND " + scopeClauses[0]
	}
	base := `WITH configured AS (
 SELECT execution_account_id, market_source FROM jsonb_to_recordset($1::jsonb)
 AS source(execution_account_id text, market_source text)
 ), portfolio_base AS MATERIALIZED (
 SELECT l.*, ` + portfolioSourceExpression + ` AS market_source,
 COALESCE(open.venue,'') AS venue,
 NULLIF(open.intent->'metadata'->>'market_question','') AS market_question
 FROM position_lots l
 LEFT JOIN execution_orders open ON open.order_id=l.opening_order_id
 AND open.execution_account_id=l.execution_account_id AND open.market_id=l.market_id
 AND open.token_id=l.token_id AND COALESCE(open.intent->>'condition_id','')=l.condition_id
 LEFT JOIN configured ON configured.execution_account_id=l.execution_account_id
 LEFT JOIN execution_external_position_adoptions adoption
 ON adoption.external_adoption_id=l.external_adoption_id AND adoption.lot_id=l.lot_id
 AND adoption.execution_account_id=l.execution_account_id AND adoption.market_id=l.market_id
 AND adoption.condition_id=l.condition_id AND adoption.token_id=l.token_id
 AND adoption.outcome_index IS NOT DISTINCT FROM l.outcome_index
 AND adoption.outcome_name=l.outcome_name
 LEFT JOIN execution_external_position_baselines baseline
 ON baseline.baseline_id=adoption.baseline_id AND baseline.execution_account_id=adoption.execution_account_id
 LEFT JOIN execution_external_position_baseline_items baseline_item
 ON baseline_item.baseline_id=baseline.baseline_id AND baseline_item.execution_account_id=l.execution_account_id
 AND baseline_item.condition_id=l.condition_id AND baseline_item.token_id=l.token_id
 AND baseline_item.outcome_index IS NOT DISTINCT FROM l.outcome_index AND baseline_item.outcome_name=l.outcome_name
 WHERE l.remaining_shares>0 AND l.status IN ('OPEN','SETTLED_PENDING_REDEEM')` + accountWhere + ` )`
	return base, args, nil
}

func validatePortfolioSources(ctx context.Context, tx *sql.Tx, base string, args []any) error {
	var invalid bool
	if err := tx.QueryRowContext(ctx, base+` SELECT EXISTS(SELECT 1 FROM portfolio_base WHERE market_source IS NULL)`, args...).Scan(&invalid); err != nil {
		return dbUnavailable("validate portfolio source", err)
	}
	if invalid {
		return ErrUnavailable
	}
	return nil
}

func normalizePortfolioFilter(filter ListFilter) (ListFilter, error) {
	filter, err := normalizeFilter(filter)
	if err != nil {
		return filter, err
	}
	if filter.ModelID != "" || filter.StrategyID != "" || filter.From != nil || filter.To != nil || filter.Side != "" || filter.ActivityType != "" {
		return filter, fmt.Errorf("portfolio holdings do not support model, strategy, time, side or activity filters")
	}
	if filter.MarketSource != "" && filter.MarketSource != "POLYMARKET" && filter.MarketSource != "KALSHI" {
		return filter, fmt.Errorf("market_source is invalid")
	}
	if filter.Status != "" && filter.Status != "OPEN" && filter.Status != "SETTLED_PENDING_REDEEM" && filter.Status != "MIXED" {
		return filter, fmt.Errorf("status is invalid")
	}
	return filter, nil
}

func (reader *Reader) PortfolioHoldings(ctx context.Context, filter ListFilter) (Page[PortfolioHolding], error) {
	filter, err := normalizePortfolioFilter(filter)
	if err != nil {
		return Page[PortfolioHolding]{}, err
	}
	if err = reader.requireDB(ctx); err != nil {
		return Page[PortfolioHolding]{}, err
	}
	base, args, err := reader.portfolioBase(filter)
	if err != nil {
		return Page[PortfolioHolding]{}, err
	}
	tx, err := reader.beginReadTx(ctx, "portfolio holdings")
	if err != nil {
		return Page[PortfolioHolding]{}, err
	}
	defer tx.Rollback()
	if err = validatePortfolioSources(ctx, tx, base, args); err != nil {
		return Page[PortfolioHolding]{}, err
	}
	search := "TRUE"
	if filter.Query != "" {
		args = append(args, "%"+filter.Query+"%")
		p := fmt.Sprintf("$%d", len(args))
		search = `bool_or(execution_account_id ILIKE ` + p + ` OR market_id ILIKE ` + p + ` OR condition_id ILIKE ` + p + ` OR token_id ILIKE ` + p + ` OR outcome_name ILIKE ` + p + ` OR market_question ILIKE ` + p + ` OR model_id ILIKE ` + p + ` OR strategy_id ILIKE ` + p + `)`
	}
	groups := base + `, portfolio_groups AS (
 SELECT execution_account_id,market_source,market_id,condition_id,token_id,outcome_index,NULLIF(outcome_name,'') AS outcome_name,
 CASE WHEN bool_and(status='OPEN') THEN 'OPEN'
 WHEN bool_and(status='SETTLED_PENDING_REDEEM') THEN 'SETTLED_PENDING_REDEEM' ELSE 'MIXED' END AS status,
 SUM(remaining_shares)::text AS remaining_shares,SUM(remaining_cost)::text AS remaining_cost,
 COUNT(*)::bigint AS lot_count,MIN(opened_at) AS first_opened_at,MAX(opened_at) AS last_opened_at,
 ` + search + ` AS search_match
 FROM portfolio_base GROUP BY execution_account_id,market_source,market_id,condition_id,token_id,outcome_index,NULLIF(outcome_name,'')),
 filtered AS (SELECT * FROM portfolio_groups WHERE search_match`
	if filter.MarketSource != "" {
		args = append(args, filter.MarketSource)
		groups += fmt.Sprintf(" AND market_source=$%d", len(args))
	}
	if filter.Status != "" {
		args = append(args, filter.Status)
		groups += fmt.Sprintf(" AND status=$%d", len(args))
	}
	groups += `)`
	total, err := countQuery(ctx, tx, groups+` SELECT COUNT(*) FROM filtered`, args, "portfolio holdings")
	if err != nil {
		return Page[PortfolioHolding]{}, err
	}
	listArgs := append([]any(nil), args...)
	limit, offset := pageLimitOffset(filter, &listArgs)
	order := `last_opened_at DESC,execution_account_id,market_source,market_id,condition_id,token_id,outcome_index NULLS LAST,outcome_name NULLS LAST`
	rows, err := tx.QueryContext(ctx, groups+`, paged AS MATERIALIZED (SELECT * FROM filtered ORDER BY `+order+` LIMIT `+limit+` OFFSET `+offset+`)
 SELECT paged.execution_account_id,paged.market_source,paged.market_id,paged.condition_id,paged.token_id,paged.outcome_index,paged.outcome_name,
 label.market_question,paged.status,paged.remaining_shares,paged.remaining_cost,paged.lot_count,paged.first_opened_at,paged.last_opened_at
 FROM paged LEFT JOIN LATERAL (
 SELECT member.market_question FROM portfolio_base member
 WHERE member.execution_account_id=paged.execution_account_id AND member.market_source=paged.market_source
 AND member.market_id=paged.market_id AND member.condition_id=paged.condition_id AND member.token_id=paged.token_id
 AND member.outcome_index IS NOT DISTINCT FROM paged.outcome_index
 AND NULLIF(member.outcome_name,'') IS NOT DISTINCT FROM paged.outcome_name AND member.market_question IS NOT NULL
 ORDER BY member.opened_at DESC,member.lot_id DESC LIMIT 1
 ) label ON TRUE ORDER BY `+"paged."+strings.ReplaceAll(order, ",", ",paged."), listArgs...)
	if err != nil {
		return Page[PortfolioHolding]{}, dbUnavailable("list portfolio holdings", err)
	}
	defer rows.Close()
	items := make([]PortfolioHolding, 0, filter.Limit)
	for rows.Next() {
		var item PortfolioHolding
		var name, question sql.NullString
		var index sql.NullInt64
		var count int64
		if err = rows.Scan(&item.ExecutionAccountID, &item.MarketSource, &item.MarketID, &item.ConditionID, &item.TokenID, &index, &name, &question, &item.Status, &item.RemainingShares, &item.RemainingCost, &count, &item.FirstOpenedAt, &item.LastOpenedAt); err != nil {
			return Page[PortfolioHolding]{}, dbUnavailable("scan portfolio holding", err)
		}
		if count < 0 || count > int64(maxInt()) {
			return Page[PortfolioHolding]{}, ErrUnavailable
		}
		item.LotCount = int(count)
		item.OutcomeIndex = scanNullableInt64AsInt(index)
		item.OutcomeName = scanNullableString(name)
		item.MarketQuestion = scanNullableString(question)
		item.FirstOpenedAt = item.FirstOpenedAt.UTC()
		item.LastOpenedAt = item.LastOpenedAt.UTC()
		items = append(items, item)
	}
	if err = queryRowsError(rows, "iterate portfolio holdings"); err != nil {
		return Page[PortfolioHolding]{}, err
	}
	page := finishPage(items, filter, total, reader.now().UTC(), map[string]any{"basis": "current_positive_lots", "group_identity": "account:market_source:market:condition:token:outcome"})
	if err = tx.Commit(); err != nil {
		return Page[PortfolioHolding]{}, dbUnavailable("commit portfolio holdings", err)
	}
	return page, nil
}

func normalizePortfolioIdentity(identity PortfolioHoldingIdentity) (PortfolioHoldingIdentity, error) {
	legacy, err := normalizeHoldingIdentity(HoldingIdentity{ExecutionAccountID: identity.ExecutionAccountID, MarketSource: identity.MarketSource, MarketID: identity.MarketID, ConditionID: identity.ConditionID, TokenID: identity.TokenID, OutcomeIndex: identity.OutcomeIndex, OutcomeName: identity.OutcomeName})
	if err != nil {
		return identity, err
	}
	if identity.ExecutionAccountID == "" || identity.MarketID == "" || identity.TokenID == "" || (identity.MarketSource != "POLYMARKET" && identity.MarketSource != "KALSHI") {
		return identity, fmt.Errorf("portfolio holding identity is incomplete")
	}
	identity.OutcomeName = legacy.OutcomeName
	return identity, nil
}

func (reader *Reader) PortfolioHoldingLots(ctx context.Context, identity PortfolioHoldingIdentity, filter ListFilter) (Page[PositionLot], error) {
	filter, err := normalizePortfolioFilter(filter)
	if err != nil {
		return Page[PositionLot]{}, err
	}
	if filter.Query != "" || filter.ExecutionAccountID != "" || filter.AccountScope != "" || filter.MarketSource != "" || filter.Status != "" {
		return Page[PositionLot]{}, fmt.Errorf("portfolio holding lots only support limit and offset")
	}
	identity, err = normalizePortfolioIdentity(identity)
	if err != nil {
		return Page[PositionLot]{}, err
	}
	if err = reader.requireDB(ctx); err != nil {
		return Page[PositionLot]{}, err
	}
	base, args, err := reader.portfolioBase(ListFilter{ExecutionAccountID: identity.ExecutionAccountID})
	if err != nil {
		return Page[PositionLot]{}, err
	}
	var index any
	var name any
	if identity.OutcomeIndex != nil {
		index = *identity.OutcomeIndex
	}
	if identity.OutcomeName != nil {
		name = *identity.OutcomeName
	}
	args = append(args, identity.MarketSource, identity.MarketID, identity.ConditionID, identity.TokenID, index, name)
	where := `market_source=$3 AND market_id=$4 AND condition_id=$5 AND token_id=$6 AND outcome_index IS NOT DISTINCT FROM $7 AND NULLIF(outcome_name,'') IS NOT DISTINCT FROM $8`
	tx, err := reader.beginReadTx(ctx, "portfolio holding lots")
	if err != nil {
		return Page[PositionLot]{}, err
	}
	defer tx.Rollback()
	// Source validation is account-scoped, just like the parent list.
	if err = validatePortfolioSources(ctx, tx, base, args[:2]); err != nil {
		return Page[PositionLot]{}, err
	}
	total, err := countQuery(ctx, tx, base+` SELECT COUNT(*) FROM portfolio_base WHERE `+where, args, "portfolio holding lots")
	if err != nil {
		return Page[PositionLot]{}, err
	}
	listArgs := append([]any(nil), args...)
	limit, offset := pageLimitOffset(filter, &listArgs)
	rows, err := tx.QueryContext(ctx, base+` SELECT lot_id,execution_account_id,model_id,strategy_id,venue,market_source,market_id,condition_id,token_id,COALESCE(opening_order_id,''),COALESCE(opening_fill_key,''),status,original_shares::text,remaining_shares::text,original_cost::text,remaining_cost::text,average_entry_price::text,opened_at,closed_at FROM portfolio_base WHERE `+where+` ORDER BY opened_at DESC,lot_id DESC LIMIT `+limit+` OFFSET `+offset, listArgs...)
	if err != nil {
		return Page[PositionLot]{}, dbUnavailable("list portfolio holding lots", err)
	}
	defer rows.Close()
	items := make([]PositionLot, 0, filter.Limit)
	for rows.Next() {
		item, scanErr := scanPositionLot(rows)
		if scanErr != nil {
			return Page[PositionLot]{}, dbUnavailable("scan portfolio lot", scanErr)
		}
		items = append(items, item)
	}
	if err = queryRowsError(rows, "iterate portfolio lots"); err != nil {
		return Page[PositionLot]{}, err
	}
	page := finishPage(items, filter, total, reader.now().UTC(), map[string]any{"basis": "current_positive_lots", "holding_identity": true})
	if err = tx.Commit(); err != nil {
		return Page[PositionLot]{}, dbUnavailable("commit portfolio lots", err)
	}
	return page, nil
}
