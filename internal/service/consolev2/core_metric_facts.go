package consolev2

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

const MaxCoreMetricAssets = 2000

// CoreMetricFacts never invokes a venue. Financial facts and complete current
// assets are frozen in one read-only RR snapshot before a caller obtains prices.
type CoreMetricFacts struct {
	ObservedAt time.Time
	Accounts   []CoreAccountFact
}
type CoreAccountFact struct {
	FundingAddress string
	Metrics        AccountPerformance
	Assets         []CoreAssetFact
}
type CoreAssetFact struct {
	PortfolioHolding
	SettlementPrice *string
	SettlementAt    *time.Time
}

func (reader *Reader) CoreMetricFacts(ctx context.Context, accountID string) (CoreMetricFacts, error) {
	if err := ValidatePerformanceAccountID(accountID); err != nil {
		return CoreMetricFacts{}, err
	}
	tx, err := reader.beginReadTx(ctx, "core metric facts")
	if err != nil {
		return CoreMetricFacts{}, err
	}
	defer tx.Rollback()
	now := reader.now().UTC()
	from := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -13)
	accounts, err := reader.managedAccountsInTx(ctx, tx)
	if err != nil {
		return CoreMetricFacts{}, err
	}
	selected := make([]ManagedAccount, 0, len(accounts))
	for _, a := range accounts {
		if accountID == "" || a.ExecutionAccountID == accountID {
			selected = append(selected, a)
		}
	}
	if accountID != "" && len(selected) != 1 {
		return CoreMetricFacts{}, ErrInvalidPerformanceAccount
	}
	result := CoreMetricFacts{ObservedAt: now, Accounts: make([]CoreAccountFact, 0, len(selected))}
	indices := map[string]int{}
	for _, a := range selected {
		indices[a.ExecutionAccountID] = len(result.Accounts)
		result.Accounts = append(result.Accounts, CoreAccountFact{Metrics: AccountPerformance{ExecutionAccountID: a.ExecutionAccountID, MarketSource: a.MarketSource, CurrentCost: "0", PeakHoldingCost: "0", CumulativeInvestedCost: "0", RealizedPnL: "0", RealizedDrawdown14D: "0", DrawdownFrom: from, DrawdownTo: now}, Assets: []CoreAssetFact{}})
	}
	for i := range result.Accounts {
		if result.Accounts[i].Metrics.MarketSource == "POLYMARKET" {
			var address sql.NullString
			if err = tx.QueryRowContext(ctx, `SELECT LOWER(wallet_address) FROM execution_accounts WHERE execution_account_id=$1`, result.Accounts[i].Metrics.ExecutionAccountID).Scan(&address); err != nil {
				return CoreMetricFacts{}, dbUnavailable("core funding identity", err)
			}
			// Funding identity is private price-source input. An absent/bad
			// address must not erase otherwise valid local financial facts.
			if address.Valid {
				result.Accounts[i].FundingAddress = address.String
			}

		}
	}
	if len(selected) == 0 {
		if err = tx.Commit(); err != nil {
			return CoreMetricFacts{}, dbUnavailable("commit core facts", err)
		}
		return result, nil
	}
	encoded, _ := json.Marshal(selected)
	q := `WITH accounts AS (SELECT execution_account_id FROM jsonb_to_recordset($1::jsonb) AS a(execution_account_id text)),
 events AS MATERIALIZED (SELECT e.execution_account_id,e.position_event_id,e.occurred_at,e.event_type,e.cost_basis_delta,e.realized_pnl_delta FROM position_events e WHERE execution_account_id IN(SELECT execution_account_id FROM accounts) AND occurred_at<=$2 AND (cost_basis_delta<>0 OR realized_pnl_delta<>0)),
 running AS (SELECT execution_account_id,event_type,cost_basis_delta,realized_pnl_delta,SUM(cost_basis_delta) OVER(PARTITION BY execution_account_id ORDER BY occurred_at,position_event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cost FROM events),
 accounting AS (SELECT execution_account_id,GREATEST(MAX(cost),0) AS peak,SUM(CASE WHEN event_type IN('BOUGHT','ADOPTED','EXTERNAL_BUY_IMPORTED') AND cost_basis_delta>0 THEN cost_basis_delta ELSE 0 END) AS invested,SUM(realized_pnl_delta) AS realized FROM running GROUP BY execution_account_id),
 daily AS(SELECT execution_account_id,(occurred_at AT TIME ZONE 'UTC')::date AS day,SUM(realized_pnl_delta) AS pnl FROM events WHERE occurred_at>=$3 GROUP BY execution_account_id,(occurred_at AT TIME ZONE 'UTC')::date),
 cumulative AS(SELECT execution_account_id,day,SUM(pnl) OVER(PARTITION BY execution_account_id ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pnl FROM daily),
 peaks AS(SELECT execution_account_id,pnl,GREATEST(0,MAX(pnl) OVER(PARTITION BY execution_account_id ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS peak FROM cumulative),
 drawdown AS(SELECT execution_account_id,MAX(peak-pnl) AS amount FROM peaks GROUP BY execution_account_id)
 SELECT a.execution_account_id,COALESCE(c.peak,0)::text,COALESCE(c.invested,0)::text,COALESCE(c.realized,0)::text,COALESCE(d.amount,0)::text FROM accounts a LEFT JOIN accounting c USING(execution_account_id) LEFT JOIN drawdown d USING(execution_account_id)`
	rows, err := tx.QueryContext(ctx, q, string(encoded), now, from)
	if err != nil {
		return CoreMetricFacts{}, dbUnavailable("core accounting", err)
	}
	for rows.Next() {
		var id, peak, invested, pnl, draw string
		if err = rows.Scan(&id, &peak, &invested, &pnl, &draw); err != nil {
			rows.Close()
			return CoreMetricFacts{}, dbUnavailable("scan core accounting", err)
		}
		i := indices[id]
		m := &result.Accounts[i].Metrics
		m.PeakHoldingCost, m.CumulativeInvestedCost, m.RealizedPnL, m.RealizedDrawdown14D = peak, invested, pnl, draw
	}
	if err = queryRowsError(rows, "core accounting rows"); err != nil {
		return CoreMetricFacts{}, err
	}
	base, args, err := reader.portfolioBase(ListFilter{ExecutionAccountID: accountID})
	if err != nil {
		return CoreMetricFacts{}, err
	}
	var invalidSource bool
	checkArgs := append(append([]any{}, args...), string(encoded))
	check := base + fmt.Sprintf(` SELECT EXISTS(SELECT 1 FROM portfolio_base b JOIN jsonb_to_recordset($%d::jsonb) AS a(execution_account_id text,market_source text) ON a.execution_account_id=b.execution_account_id WHERE b.market_source IS NULL OR b.market_source<>a.market_source)`, len(checkArgs))
	if err = tx.QueryRowContext(ctx, check, checkArgs...).Scan(&invalidSource); err != nil {
		return CoreMetricFacts{}, dbUnavailable("validate core source", err)
	}
	if invalidSource {
		return CoreMetricFacts{}, ErrUnavailable
	}
	args = append(args, string(encoded), now)
	q = base + fmt.Sprintf(`,allowed AS(SELECT execution_account_id,market_source FROM jsonb_to_recordset($%d::jsonb) AS a(execution_account_id text,market_source text)),
 assets AS(SELECT b.execution_account_id,b.market_source,b.market_id,b.condition_id,b.token_id,b.outcome_index,NULLIF(b.outcome_name,'') AS outcome_name,MIN(b.market_question) AS market_question,SUM(b.remaining_shares) AS shares,SUM(b.remaining_cost) AS cost,COUNT(*) AS lot_count,MIN(b.opened_at) AS first_at,MAX(b.opened_at) AS last_at
 FROM portfolio_base b JOIN allowed a ON a.execution_account_id=b.execution_account_id AND a.market_source=b.market_source
 GROUP BY b.execution_account_id,b.market_source,b.market_id,b.condition_id,b.token_id,b.outcome_index,NULLIF(b.outcome_name,''))
 SELECT a.execution_account_id,a.market_source,a.market_id,a.condition_id,a.token_id,a.outcome_index,a.outcome_name,a.market_question,a.shares::text,a.cost::text,a.lot_count,a.first_at,a.last_at,
 CASE WHEN a.market_source='POLYMARKET' AND p.lifecycle_status='SETTLED_PENDING_REDEEM' AND p.settlement_source<>'' AND p.settled_at<=$%d AND p.settlement_price IN(0,1) THEN p.settlement_price::text END,
 CASE WHEN a.market_source='POLYMARKET' AND p.lifecycle_status='SETTLED_PENDING_REDEEM' AND p.settlement_source<>'' AND p.settled_at<=$%d AND p.settlement_price IN(0,1) THEN p.settled_at END
 FROM assets a LEFT JOIN execution_positions p ON p.execution_account_id=a.execution_account_id AND p.market_id=a.market_id AND p.condition_id=a.condition_id AND p.token_id=a.token_id AND p.outcome_index IS NOT DISTINCT FROM a.outcome_index AND NULLIF(p.outcome_name,'') IS NOT DISTINCT FROM a.outcome_name
 ORDER BY a.execution_account_id,a.market_source,a.market_id,a.condition_id,a.token_id,a.outcome_index,a.outcome_name LIMIT %d`, len(args)-1, len(args), len(args), MaxCoreMetricAssets+1)
	rows, err = tx.QueryContext(ctx, q, args...)
	if err != nil {
		return CoreMetricFacts{}, dbUnavailable("core assets", err)
	}
	count := 0
	for rows.Next() {
		count++
		if count > MaxCoreMetricAssets {
			rows.Close()
			return CoreMetricFacts{}, ErrUnavailable
		}
		var a CoreAssetFact
		var price sql.NullString
		var at sql.NullTime
		var lots int64
		if err = rows.Scan(&a.ExecutionAccountID, &a.MarketSource, &a.MarketID, &a.ConditionID, &a.TokenID, &a.OutcomeIndex, &a.OutcomeName, &a.MarketQuestion, &a.RemainingShares, &a.RemainingCost, &lots, &a.FirstOpenedAt, &a.LastOpenedAt, &price, &at); err != nil {
			rows.Close()
			return CoreMetricFacts{}, dbUnavailable("scan core asset", err)
		}
		if i, ok := indices[a.ExecutionAccountID]; ok {
			if a.MarketSource != result.Accounts[i].Metrics.MarketSource {
				rows.Close()
				return CoreMetricFacts{}, ErrUnavailable
			}
			a.LotCount = int(lots)
			a.SettlementPrice = scanNullableString(price)
			if at.Valid {
				v := at.Time.UTC()
				a.SettlementAt = &v
			}
			result.Accounts[i].Assets = append(result.Accounts[i].Assets, a)
		} else {
			rows.Close()
			return CoreMetricFacts{}, ErrUnavailable
		}
	}
	if err = queryRowsError(rows, "core asset rows"); err != nil {
		return CoreMetricFacts{}, err
	}
	if err = tx.Commit(); err != nil {
		return CoreMetricFacts{}, dbUnavailable("commit core facts", err)
	}
	return result, nil
}
