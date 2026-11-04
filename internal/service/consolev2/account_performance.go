package consolev2

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// ErrInvalidPerformanceAccount is intentionally credential-free. The HTTP
// resource maps it to its usual invalid-filter response rather than a 503.
var ErrInvalidPerformanceAccount = errors.New("execution_account_id must identify a registered managed account")

type AccountPerformanceResponse struct {
	ObservedAt time.Time            `json:"observed_at"`
	Items      []AccountPerformance `json:"items"`
}

// AccountPerformance is a system-managed ledger projection, not an official
// venue-wide or net-deposit return. All amounts are decimal strings.
type AccountPerformance struct {
	ExecutionAccountID     string     `json:"execution_account_id"`
	MarketSource           string     `json:"market_source"`
	AssetCount             int        `json:"asset_count"`
	PricedAssetCount       int        `json:"priced_asset_count"`
	CurrentCost            string     `json:"current_cost"`
	PeakHoldingCost        string     `json:"peak_holding_cost"`
	CumulativeInvestedCost string     `json:"cumulative_invested_cost"`
	RealizedPnL            string     `json:"realized_pnl"`
	MarketValue            *string    `json:"market_value"`
	UnrealizedPnL          *string    `json:"unrealized_pnl"`
	TotalPnL               *string    `json:"total_pnl"`
	ReturnRate             *string    `json:"return_rate"`
	RealizedDrawdown14D    string     `json:"realized_drawdown_14d"`
	DrawdownFrom           time.Time  `json:"drawdown_from"`
	DrawdownTo             time.Time  `json:"drawdown_to"`
	ValuationState         string     `json:"valuation_state"`
	ValuationReasons       []string   `json:"valuation_reasons"`
	SnapshotDecisionAt     *time.Time `json:"snapshot_decision_at"`
	SnapshotObservedAt     *time.Time `json:"snapshot_observed_at"`
}

// ValidatePerformanceAccountID validates syntax only; membership and account
// registration are checked inside the same RR transaction as the amounts.
func ValidatePerformanceAccountID(accountID string) error {
	if len(accountID) > maxQueryLen || strings.TrimSpace(accountID) != accountID || strings.IndexFunc(accountID, unicode.IsControl) >= 0 {
		return ErrInvalidPerformanceAccount
	}
	return nil
}

// AccountPerformance reads durable events, current portfolio assets, and one
// immutable decision quote batch. It never invokes a venue or a write service.
func (reader *Reader) AccountPerformance(ctx context.Context, accountID string) (AccountPerformanceResponse, error) {
	if err := ValidatePerformanceAccountID(accountID); err != nil {
		return AccountPerformanceResponse{}, err
	}
	tx, err := reader.beginReadTx(ctx, "account performance")
	if err != nil {
		return AccountPerformanceResponse{}, err
	}
	defer tx.Rollback()
	observedAt := reader.now().UTC()
	from := time.Date(observedAt.Year(), observedAt.Month(), observedAt.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -13)
	accounts, err := reader.managedAccountsInTx(ctx, tx)
	if err != nil {
		return AccountPerformanceResponse{}, err
	}
	if accountID != "" {
		selected := make([]ManagedAccount, 0, 1)
		for _, account := range accounts {
			if account.ExecutionAccountID == accountID {
				selected = append(selected, account)
			}
		}
		if len(selected) != 1 {
			return AccountPerformanceResponse{}, ErrInvalidPerformanceAccount
		}
		accounts = selected
	}
	result := AccountPerformanceResponse{ObservedAt: observedAt, Items: make([]AccountPerformance, 0, len(accounts))}
	if len(accounts) == 0 {
		if err = tx.Commit(); err != nil {
			return AccountPerformanceResponse{}, dbUnavailable("commit account performance", err)
		}
		return result, nil
	}

	// Keep the current-asset identity and strict provenance exactly aligned
	// with portfolio-holdings. The extra whitelist avoids unrelated retired or
	// unconfigured lots affecting an all-managed-accounts request.
	base, args, err := reader.portfolioBase(ListFilter{ExecutionAccountID: accountID})
	if err != nil {
		return AccountPerformanceResponse{}, err
	}
	encodedAccounts, err := json.Marshal(accounts)
	if err != nil {
		return AccountPerformanceResponse{}, ErrUnavailable
	}
	args = append(args, string(encodedAccounts), observedAt, from, observedAt.Add(-15*time.Minute))
	accountParam, nowParam, fromParam, freshParam := len(args)-3, len(args)-2, len(args)-1, len(args)
	prefix := base + fmt.Sprintf(`, performance_accounts AS (
 SELECT execution_account_id,market_source FROM jsonb_to_recordset($%d::jsonb)
 AS a(execution_account_id text,market_source text)
 ), performance_base AS MATERIALIZED (
 SELECT b.* FROM portfolio_base b JOIN performance_accounts a USING(execution_account_id)
 )`, accountParam)
	var invalid bool
	if err = tx.QueryRowContext(ctx, prefix+` SELECT EXISTS(
 SELECT 1 FROM performance_base b JOIN performance_accounts a USING(execution_account_id)
 WHERE b.market_source IS NULL OR b.market_source<>a.market_source)`, args[:accountParam]...).Scan(&invalid); err != nil {
		return AccountPerformanceResponse{}, dbUnavailable("validate performance portfolio", err)
	}
	if invalid {
		return AccountPerformanceResponse{}, ErrUnavailable
	}

	query := prefix + fmt.Sprintf(`,
 assets AS (
 SELECT execution_account_id,market_source,market_id,condition_id,token_id,outcome_index,NULLIF(outcome_name,'') AS outcome_name,
 SUM(remaining_shares) AS shares,SUM(remaining_cost) AS cost
 FROM performance_base
 GROUP BY execution_account_id,market_source,market_id,condition_id,token_id,outcome_index,NULLIF(outcome_name,'')
 ), batch AS MATERIALIZED (
 SELECT decision_at FROM strategy_orderbook_snapshot_batches
 WHERE decision_at<=$%d AND created_at<=$%d ORDER BY decision_at DESC LIMIT 1
 ), quoted AS (
 SELECT a.*,q.observed_at,
 CASE WHEN batch.decision_at IS NULL THEN 'missing_batch'
 WHEN q.token_id IS NULL THEN 'missing_quote'
 WHEN q.status<>'OK' OR q.best_bid IS NULL OR q.best_ask IS NULL
      OR q.best_bid::text IN ('NaN','Infinity','-Infinity') OR q.best_ask::text IN ('NaN','Infinity','-Infinity')
      OR q.best_bid<0 OR q.best_ask>1 OR q.best_bid>q.best_ask THEN 'invalid_quote'
 WHEN q.observed_at>$%d THEN 'future_quote'
 WHEN q.observed_at<$%d THEN 'stale_quote'
 ELSE 'complete' END AS quote_state,
 (q.best_bid+q.best_ask)/2 AS mid
 FROM assets a LEFT JOIN batch ON TRUE
 LEFT JOIN strategy_orderbook_snapshots q ON q.decision_at=batch.decision_at
 AND q.market_source=a.market_source AND q.market_id=a.market_id AND q.condition_id=a.condition_id
 AND q.token_id=a.token_id AND q.outcome_index=a.outcome_index
 ), valuation AS (
 SELECT execution_account_id,COUNT(*) AS asset_count,
 COUNT(*) FILTER(WHERE quote_state='complete') AS priced_asset_count,SUM(cost) AS current_cost,
 SUM(CASE WHEN quote_state='complete' THEN shares*mid ELSE 0 END) AS priced_value,
 MIN(observed_at) AS snapshot_observed_at,
 bool_or(quote_state='missing_batch') AS missing_batch,
 bool_or(quote_state='missing_quote') AS missing_quote,
 bool_or(quote_state='invalid_quote') AS invalid_quote,
 bool_or(quote_state='stale_quote') AS stale_quote,
 bool_or(quote_state='future_quote') AS future_quote
 FROM quoted GROUP BY execution_account_id
 ), scoped_events AS MATERIALIZED (
 SELECT e.execution_account_id,e.position_event_id,e.occurred_at,e.event_type,e.cost_basis_delta,e.realized_pnl_delta
 FROM position_events e
 WHERE e.execution_account_id IN (SELECT execution_account_id FROM performance_accounts)
 AND e.occurred_at<=$%d AND (e.cost_basis_delta<>0 OR e.realized_pnl_delta<>0)
 ), running_cost AS (
 SELECT *,SUM(cost_basis_delta) OVER(PARTITION BY execution_account_id ORDER BY occurred_at,position_event_id
 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS deployed_cost FROM scoped_events
 ), accounting AS (
 SELECT execution_account_id,GREATEST(MAX(deployed_cost),0) AS peak_holding_cost,
 SUM(CASE WHEN event_type IN ('BOUGHT','ADOPTED','EXTERNAL_BUY_IMPORTED') AND cost_basis_delta>0
 THEN cost_basis_delta ELSE 0 END) AS cumulative_invested_cost,
 SUM(realized_pnl_delta) AS realized_pnl FROM running_cost GROUP BY execution_account_id
 ), daily AS (
 SELECT execution_account_id,(occurred_at AT TIME ZONE 'UTC')::date AS day,SUM(realized_pnl_delta) AS realized
 FROM scoped_events WHERE occurred_at>=$%d GROUP BY execution_account_id,(occurred_at AT TIME ZONE 'UTC')::date
 ), daily_running AS (
 SELECT execution_account_id,day,SUM(realized) OVER(PARTITION BY execution_account_id ORDER BY day
 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS realized FROM daily
 ), daily_peaks AS (
 SELECT execution_account_id,realized,GREATEST(0,MAX(realized) OVER(PARTITION BY execution_account_id ORDER BY day
 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS peak FROM daily_running
 ), drawdown AS (
 SELECT execution_account_id,MAX(peak-realized) AS drawdown FROM daily_peaks GROUP BY execution_account_id
 ), combined AS (
 SELECT a.execution_account_id,a.market_source,COALESCE(v.asset_count,0) AS asset_count,
 COALESCE(v.priced_asset_count,0) AS priced_asset_count,COALESCE(v.current_cost,0) AS current_cost,
 COALESCE(c.peak_holding_cost,0) AS peak_holding_cost,COALESCE(c.cumulative_invested_cost,0) AS cumulative_invested_cost,
 COALESCE(c.realized_pnl,0) AS realized_pnl,COALESCE(d.drawdown,0) AS drawdown,
 CASE WHEN COALESCE(v.priced_asset_count,0)=COALESCE(v.asset_count,0) THEN COALESCE(v.priced_value,0) END AS market_value,
 (SELECT decision_at FROM batch) AS snapshot_decision_at,v.snapshot_observed_at,
 COALESCE(v.missing_batch,FALSE) AS missing_batch,COALESCE(v.missing_quote,FALSE) AS missing_quote,
 COALESCE(v.invalid_quote,FALSE) AS invalid_quote,COALESCE(v.stale_quote,FALSE) AS stale_quote,
 COALESCE(v.future_quote,FALSE) AS future_quote
 FROM performance_accounts a LEFT JOIN valuation v USING(execution_account_id)
 LEFT JOIN accounting c USING(execution_account_id) LEFT JOIN drawdown d USING(execution_account_id)
 )
 SELECT execution_account_id,market_source,asset_count,priced_asset_count,current_cost::text,peak_holding_cost::text,
 cumulative_invested_cost::text,realized_pnl::text,market_value::text,(market_value-current_cost)::text,
 (realized_pnl+market_value-current_cost)::text,
 CASE WHEN peak_holding_cost>0 THEN ((realized_pnl+market_value-current_cost)/peak_holding_cost)::text END,
 drawdown::text,snapshot_decision_at,snapshot_observed_at,
 missing_batch,missing_quote,invalid_quote,stale_quote,future_quote
 FROM combined ORDER BY execution_account_id`, nowParam, nowParam, nowParam, freshParam, nowParam, fromParam)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return AccountPerformanceResponse{}, dbUnavailable("read account performance", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item AccountPerformance
		var assetCount, pricedCount int64
		var market, unrealized, total, rate sql.NullString
		var decisionAt, snapshotAt sql.NullTime
		var missingBatch, missingQuote, invalidQuote, staleQuote, futureQuote bool
		if err = rows.Scan(&item.ExecutionAccountID, &item.MarketSource, &assetCount, &pricedCount,
			&item.CurrentCost, &item.PeakHoldingCost, &item.CumulativeInvestedCost, &item.RealizedPnL,
			&market, &unrealized, &total, &rate, &item.RealizedDrawdown14D, &decisionAt, &snapshotAt,
			&missingBatch, &missingQuote, &invalidQuote, &staleQuote, &futureQuote); err != nil {
			return AccountPerformanceResponse{}, dbUnavailable("scan account performance", err)
		}
		if assetCount < 0 || assetCount > int64(maxInt()) || pricedCount < 0 || pricedCount > assetCount {
			return AccountPerformanceResponse{}, ErrUnavailable
		}
		item.AssetCount, item.PricedAssetCount = int(assetCount), int(pricedCount)
		item.MarketValue, item.UnrealizedPnL, item.TotalPnL, item.ReturnRate = scanNullableString(market), scanNullableString(unrealized), scanNullableString(total), scanNullableString(rate)
		for _, value := range []*string{&item.CurrentCost, &item.PeakHoldingCost, &item.CumulativeInvestedCost, &item.RealizedPnL, &item.RealizedDrawdown14D, item.MarketValue, item.UnrealizedPnL, item.TotalPnL, item.ReturnRate} {
			if value != nil {
				if _, parseErr := domain.ParseDecimal(*value); parseErr != nil {
					return AccountPerformanceResponse{}, ErrUnavailable
				}
			}
		}
		item.ValuationState = "complete"
		if item.PricedAssetCount != item.AssetCount {
			item.ValuationState = "incomplete"
		}
		item.ValuationReasons = make([]string, 0, 5)
		for _, reason := range []struct {
			name    string
			present bool
		}{
			{"missing_batch", missingBatch}, {"missing_quote", missingQuote}, {"invalid_quote", invalidQuote}, {"stale_quote", staleQuote}, {"future_quote", futureQuote},
		} {
			if reason.present {
				item.ValuationReasons = append(item.ValuationReasons, reason.name)
			}
		}
		if decisionAt.Valid {
			value := decisionAt.Time.UTC()
			item.SnapshotDecisionAt = &value
		}
		if snapshotAt.Valid {
			value := snapshotAt.Time.UTC()
			item.SnapshotObservedAt = &value
		}
		item.DrawdownFrom, item.DrawdownTo = from, observedAt
		result.Items = append(result.Items, item)
	}
	if err = queryRowsError(rows, "iterate account performance"); err != nil {
		return AccountPerformanceResponse{}, err
	}
	if err = tx.Commit(); err != nil {
		return AccountPerformanceResponse{}, dbUnavailable("commit account performance", err)
	}
	return result, nil
}
