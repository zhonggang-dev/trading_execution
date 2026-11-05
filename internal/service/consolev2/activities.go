package consolev2

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type LedgerActivity struct {
	MarketSource       string     `json:"market_source"`
	QuantityUnit       string     `json:"quantity_unit"`
	ActivityKey        string     `json:"activity_key"`
	ActivityType       string     `json:"activity_type"`
	Venue              string     `json:"venue"`
	ExecutionAccountID string     `json:"execution_account_id"`
	ModelID            string     `json:"model_id"`
	StrategyID         string     `json:"strategy_id"`
	MarketID           string     `json:"market_id"`
	MarketLabel        string     `json:"market_label,omitempty"`
	ConditionID        string     `json:"condition_id,omitempty"`
	TokenID            string     `json:"token_id"`
	OutcomeName        string     `json:"outcome_name,omitempty"`
	LotID              string     `json:"lot_id,omitempty"`
	OrderID            string     `json:"order_id,omitempty"`
	VenueOrderID       string     `json:"venue_order_id,omitempty"`
	VenueTradeID       string     `json:"venue_trade_id,omitempty"`
	OrderStatus        string     `json:"order_status,omitempty"`
	LiquidityRole      string     `json:"liquidity_role,omitempty"`
	Shares             string     `json:"shares"`
	Price              string     `json:"price,omitempty"`
	GrossNotional      string     `json:"gross_notional,omitempty"`
	TotalFee           string     `json:"total_fee"`
	NetCashDelta       string     `json:"net_cash_delta"`
	CostBasis          string     `json:"cost_basis,omitempty"`
	SettlementPayout   string     `json:"settlement_payout,omitempty"`
	RealizedPnL        string     `json:"realized_pnl"`
	TransactionHash    string     `json:"transaction_hash,omitempty"`
	OccurredAt         time.Time  `json:"occurred_at"`
	ConfirmedAt        *time.Time `json:"confirmed_at"`
	AppliedAt          *time.Time `json:"applied_at"`
}

// Both the displayed projection and its integrity check use identical joins.
const nettingEvidenceFrom = ` FROM execution_account_events event
 JOIN execution_fills f ON event.fill_key='kalshi-netting:'||f.fill_key
      AND f.execution_account_id=event.execution_account_id AND f.order_id=event.order_id
      AND lower(f.venue)='kalshi' AND f.side='BUY' AND f.status='CONFIRMED' AND f.applied_at IS NOT NULL
      AND f.token_id IN ('kalshi:'||f.market_id||':YES','kalshi:'||f.market_id||':NO')
 JOIN execution_orders o ON o.order_id=f.order_id AND o.execution_account_id=f.execution_account_id
      AND o.market_id=f.market_id AND o.token_id=f.token_id
 JOIN LATERAL (
     SELECT SUM(lc.allocated_cost) cost, SUM(lc.realized_pnl) pnl,
            SUM(lc.allocated_net_proceeds) payout, SUM(lc.closed_shares) legs,
            SUM(lc.closed_shares) FILTER(WHERE l.token_id='kalshi:'||f.market_id||':YES') yes_shares,
            SUM(lc.closed_shares) FILTER(WHERE l.token_id='kalshi:'||f.market_id||':NO') no_shares
     FROM position_lot_closures lc JOIN position_lots l ON l.lot_id=lc.lot_id
     WHERE lc.closing_fill_key=f.fill_key AND l.execution_account_id=f.execution_account_id
       AND l.market_id=f.market_id AND l.condition_id=f.condition_id
 ) c ON c.payout=event.total_balance_delta AND c.legs=2*event.total_balance_delta
        AND c.yes_shares=event.total_balance_delta AND c.no_shares=event.total_balance_delta
 WHERE event.event_type='KALSHI_NETTED' AND event.account_event_id=event.fill_key
       AND event.total_balance_delta>0`

const activityProjection = `
SELECT activity_key,activity_type,venue,execution_account_id,model_id,strategy_id,
       market_id,market_label,condition_id,token_id,outcome_name,lot_id,order_id,
       venue_order_id,venue_trade_id,order_status,liquidity_role,
       shares::text AS shares,COALESCE(price::text,'') AS price,
       COALESCE(gross_notional::text,'') AS gross_notional,total_fee::text AS total_fee,
       net_cash_delta::text AS net_cash_delta,COALESCE(cost_basis::text,'') AS cost_basis,
       COALESCE(settlement_payout::text,'') AS settlement_payout,realized_pnl::text AS realized_pnl,
       transaction_hash,occurred_at,confirmed_at,applied_at,upper(venue) AS market_source,
       CASE WHEN activity_type='KALSHI_NETTING' THEN 'pairs' ELSE 'shares' END AS quantity_unit
FROM (
		SELECT 'fill:' || fill.fill_key AS activity_key, fill.side AS activity_type, fill.venue,
		       fill.execution_account_id,
		       COALESCE(order_row.intent->>'model_id', '') AS model_id,
		       COALESCE(order_row.intent->>'strategy_id', '') AS strategy_id,
		       fill.market_id,
		       COALESCE(order_row.intent->'metadata'->>'market_question', '') AS market_label,
		       fill.condition_id, fill.token_id,
		       COALESCE(order_row.intent->>'outcome_name', '') AS outcome_name,
		       COALESCE(NULLIF(order_row.intent->>'target_lot_id', ''), opening_lot.lot_id, '') AS lot_id,
		       fill.order_id, fill.venue_order_id, fill.venue_fill_id AS venue_trade_id,
		       order_row.status AS order_status, fill.liquidity_role,
		       fill.shares, fill.price, fill.gross_notional, fill.total_fee, fill.net_cash_delta,
		       CASE WHEN fill.side='SELL' THEN closure.allocated_cost END AS cost_basis,
		       NULL::numeric AS settlement_payout,
		       CASE WHEN fill.side='SELL' THEN COALESCE(closure.realized_pnl, 0) ELSE 0 END AS realized_pnl,
		       fill.transaction_hash,
		       fill.matched_at AS occurred_at, fill.confirmed_at, fill.applied_at
		FROM execution_fills AS fill
		JOIN execution_orders AS order_row ON order_row.order_id = fill.order_id
		LEFT JOIN position_lots AS opening_lot ON opening_lot.opening_fill_key = fill.fill_key
		LEFT JOIN (
			SELECT closing_fill_key, SUM(realized_pnl) AS realized_pnl, SUM(allocated_cost) AS allocated_cost
			FROM position_lot_closures
			GROUP BY closing_fill_key
		) AS closure ON closure.closing_fill_key = fill.fill_key
		WHERE fill.status = 'CONFIRMED' AND fill.applied_at IS NOT NULL AND fill.confirmed_at IS NOT NULL
		UNION ALL
		SELECT 'redemption:' || redemption.redemption_id AS activity_key, 'REDEEM' AS activity_type,
		       'polymarket' AS venue,
		       redemption.execution_account_id,
		       lot.model_id, lot.strategy_id, lot.market_id,
		       COALESCE(opening_order.intent->'metadata'->>'market_question', '') AS market_label,
		       redemption.condition_id, lot.token_id,
		       COALESCE(opening_order.intent->>'outcome_name', '') AS outcome_name,
		       lot.lot_id,
		       '' AS order_id, '' AS venue_order_id, '' AS venue_trade_id,
		       '' AS order_status, '' AS liquidity_role,
		       redemption.redeemed_shares AS shares, NULL::numeric AS price, NULL::numeric AS gross_notional,
		       0::numeric AS total_fee, redemption.allocated_payout AS net_cash_delta,
		       redemption.allocated_cost AS cost_basis,
		       redemption.allocated_payout AS settlement_payout,
		       redemption.realized_pnl,
		       redemption.transaction_hash,
		       redemption.redeemed_at AS occurred_at, parent.confirmed_at, redemption.redeemed_at AS applied_at
		FROM position_lot_redemptions AS redemption
		JOIN position_lots AS lot ON lot.lot_id = redemption.lot_id
		JOIN polymarket_redemptions AS parent
		  ON parent.execution_account_id = redemption.execution_account_id
		 AND parent.condition_id = redemption.condition_id
		JOIN execution_orders AS opening_order ON opening_order.order_id = lot.opening_order_id
		WHERE parent.status = 'APPLIED' AND parent.confirmed_at IS NOT NULL
		UNION ALL
		SELECT sell.sell_id, 'EXTERNAL_SELL', 'polymarket', sell.execution_account_id,
		       sell.model_id, sell.strategy_id, position.market_id, '' AS market_label,
		       sell.condition_id, sell.token_id, position.outcome_name, '' AS lot_id,
		       '' AS order_id, sell.venue_order_id, sell.venue_trade_id, '' AS order_status, '' AS liquidity_role,
		       sell.shares, sell.gross_notional/sell.shares, sell.gross_notional, sell.total_fee, sell.net_cash,
		       sell.allocated_cost, NULL::numeric, sell.net_cash-sell.allocated_cost, sell.transaction_hash,
		       sell.occurred_at, batch.applied_at, batch.applied_at
		FROM managed_external_sells sell
		JOIN managed_external_sell_batches batch USING (batch_id)
		JOIN execution_positions position ON position.execution_account_id=sell.execution_account_id AND position.token_id=sell.token_id
	
 UNION ALL
 SELECT event.account_event_id, 'KALSHI_NETTING', 'kalshi', event.execution_account_id,
        COALESCE(o.intent->>'model_id',''), COALESCE(o.intent->>'strategy_id',''), f.market_id,
        COALESCE(o.intent->'metadata'->>'market_question',''), f.condition_id, '', '', '',
        f.order_id, f.venue_order_id, f.venue_fill_id, o.status, '',
        event.total_balance_delta, NULL::numeric, NULL::numeric, 0::numeric, event.total_balance_delta,
        c.cost, event.total_balance_delta, c.pnl, '', event.occurred_at, NULL::timestamptz, event.occurred_at
` + nettingEvidenceFrom + `
 UNION ALL
 SELECT 'kalshi-settlement:'||s.execution_account_id||':'||s.ticker||':'||l.lot_id,
        'KALSHI_SETTLEMENT', 'kalshi', s.execution_account_id, l.model_id,l.strategy_id,s.ticker,
        COALESCE(o.intent->'metadata'->>'market_question',''), l.condition_id,l.token_id,
        COALESCE(l.outcome_name,''),l.lot_id,'','','','','',
        sl.settled_shares,NULL::numeric,NULL::numeric,0::numeric,sl.allocated_payout,
        sl.allocated_cost,sl.allocated_payout,sl.realized_pnl,'',sl.settled_at,NULL::timestamptz,s.applied_at
 FROM kalshi_settlements s JOIN kalshi_settlement_lots sl
 ON sl.execution_account_id=s.execution_account_id AND sl.ticker=s.ticker
 JOIN position_lots l ON l.lot_id=sl.lot_id AND l.execution_account_id=sl.execution_account_id
      AND l.market_id=sl.ticker AND l.token_id=sl.token_id
 LEFT JOIN execution_orders o ON o.order_id=l.opening_order_id
 WHERE s.status='APPLIED'
) AS activity`

func (reader *Reader) LedgerActivities(ctx context.Context, filter ListFilter) (Page[LedgerActivity], error) {
	filter, err := reader.normalizeScopedFilter(filter)
	if err != nil {
		return Page[LedgerActivity]{}, err
	}
	if err = reader.requireDB(ctx); err != nil {
		return Page[LedgerActivity]{}, err
	}
	args := []any{}
	clauses := []string{"TRUE"}
	add := func(clause string, value any) {
		args = append(args, value)
		clauses = append(clauses, fmt.Sprintf(clause, len(args)))
	}
	if filter.ExecutionAccountID != "" {
		add("execution_account_id=$%d", filter.ExecutionAccountID)
	}
	clauses = appendAccountScope(clauses, &args, filter, "execution_account_id")
	if filter.ActivityType != "" {
		add("activity_type=$%d", filter.ActivityType)
	}
	if filter.Query != "" {
		add("concat_ws(' ',market_id,market_label,condition_id,token_id,order_id,lot_id,execution_account_id,transaction_hash) ILIKE $%d", "%"+filter.Query+"%")
	}
	now := reader.now().UTC()
	if filter.From != nil {
		add("occurred_at >= $%d", filter.From.UTC())

	}
	if filter.To != nil {
		add("occurred_at <= $%d", filter.To.UTC())
	}
	where := strings.Join(clauses, " AND ")
	tx, err := reader.beginReadTx(ctx, "ledger activities")
	if err != nil {
		return Page[LedgerActivity]{}, err
	}
	defer tx.Rollback()
	// An inconsistent durable netting event must fail the read, never silently
	// disappear from cash/PnL totals. Scope the check to this account/time range.
	if filter.ActivityType == "" || filter.ActivityType == "KALSHI_NETTING" {
		checkArgs := []any{}
		checks := []string{"e.event_type='KALSHI_NETTED'"}
		checkAdd := func(clause string, value any) {
			checkArgs = append(checkArgs, value)
			checks = append(checks, fmt.Sprintf(clause, len(checkArgs)))
		}
		if filter.ExecutionAccountID != "" {
			checkAdd("e.execution_account_id=$%d", filter.ExecutionAccountID)
		}
		checks = appendAccountScope(checks, &checkArgs, filter, "e.execution_account_id")
		if filter.From != nil {
			checkAdd("e.occurred_at >= $%d", filter.From.UTC())

		}
		if filter.To != nil {
			checkAdd("e.occurred_at <= $%d", filter.To.UTC())
		}
		var inconsistent bool
		checkSQL := `SELECT EXISTS(SELECT 1 FROM execution_account_events e WHERE ` + strings.Join(checks, " AND ") + ` AND NOT EXISTS(SELECT 1 ` + nettingEvidenceFrom + ` AND event.account_event_id=e.account_event_id))`
		if err = tx.QueryRowContext(ctx, checkSQL, checkArgs...).Scan(&inconsistent); err != nil {
			return Page[LedgerActivity]{}, dbUnavailable("netting evidence", err)
		}
		if inconsistent {
			return Page[LedgerActivity]{}, ErrUnavailable
		}
	}
	summary := map[string]any{}
	var total, tradeCount, redemptionCount, nettingCount, settlementCount int
	amounts := make([]string, 12)
	dest := []any{&total, &tradeCount, &redemptionCount, &nettingCount, &settlementCount}
	for i := range amounts {
		dest = append(dest, &amounts[i])
	}
	err = tx.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER(WHERE activity_type IN ('BUY','SELL','EXTERNAL_SELL')),count(*) FILTER(WHERE activity_type='REDEEM'),count(*) FILTER(WHERE activity_type='KALSHI_NETTING'),count(*) FILTER(WHERE activity_type='KALSHI_SETTLEMENT'),
 COALESCE(sum(NULLIF(gross_notional,'')::numeric) FILTER(WHERE activity_type='BUY'),0)::text,
 COALESCE(sum(NULLIF(gross_notional,'')::numeric) FILTER(WHERE activity_type IN ('SELL','EXTERNAL_SELL')),0)::text,
 COALESCE(sum(NULLIF(settlement_payout,'')::numeric) FILTER(WHERE activity_type='REDEEM'),0)::text,
 COALESCE(sum(net_cash_delta::numeric),0)::text,COALESCE(sum(total_fee::numeric),0)::text,COALESCE(sum(realized_pnl::numeric),0)::text,
 COALESCE(sum(realized_pnl::numeric) FILTER(WHERE activity_type IN ('SELL','EXTERNAL_SELL')),0)::text,
 COALESCE(sum(realized_pnl::numeric) FILTER(WHERE activity_type='REDEEM'),0)::text,
 COALESCE(sum(net_cash_delta::numeric) FILTER(WHERE activity_type='KALSHI_NETTING'),0)::text,
 COALESCE(sum(net_cash_delta::numeric) FILTER(WHERE activity_type='KALSHI_SETTLEMENT'),0)::text,
 COALESCE(sum(realized_pnl::numeric) FILTER(WHERE activity_type='KALSHI_NETTING'),0)::text,
 COALESCE(sum(realized_pnl::numeric) FILTER(WHERE activity_type='KALSHI_SETTLEMENT'),0)::text
 FROM (`+activityProjection+`) rows WHERE `+where, args...).Scan(dest...)
	if err != nil {
		return Page[LedgerActivity]{}, dbUnavailable("ledger summary", err)
	}
	summary["activity_count"] = total
	summary["trade_count"] = tradeCount
	summary["redemption_count"] = redemptionCount
	summary["netting_count"] = nettingCount
	summary["settlement_count"] = settlementCount
	keys := []string{"buy_notional", "sell_notional", "redeem_payout", "net_cash_flow", "total_fee", "realized_pnl", "sell_realized_pnl", "redeem_realized_pnl", "netting_payout", "settlement_payout", "netting_realized_pnl", "settlement_realized_pnl"}
	for i, key := range keys {
		summary[key] = amounts[i]
	}
	listArgs := append([]any(nil), args...)
	limit, offset := pageLimitOffset(filter, &listArgs)
	rows, err := tx.QueryContext(ctx, `SELECT to_jsonb(rows) FROM (`+activityProjection+`) rows WHERE `+where+` ORDER BY occurred_at DESC,activity_key DESC LIMIT `+limit+` OFFSET `+offset, listArgs...)
	if err != nil {
		return Page[LedgerActivity]{}, dbUnavailable("list ledger", err)
	}
	defer rows.Close()
	items := make([]LedgerActivity, 0, filter.Limit)
	for rows.Next() {
		var raw []byte
		var item LedgerActivity
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal(raw, &item)
		}
		if err != nil {
			return Page[LedgerActivity]{}, dbUnavailable("scan ledger", err)
		}
		item.OccurredAt = item.OccurredAt.UTC()
		if item.ConfirmedAt != nil {
			v := item.ConfirmedAt.UTC()
			item.ConfirmedAt = &v
		}
		if item.AppliedAt != nil {
			v := item.AppliedAt.UTC()
			item.AppliedAt = &v
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return Page[LedgerActivity]{}, dbUnavailable("iterate ledger", err)
	}
	if err = tx.Commit(); err != nil {
		return Page[LedgerActivity]{}, dbUnavailable("commit ledger", err)
	}
	return finishPage(items, filter, total, now, summary), nil
}
