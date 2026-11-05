package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	history "github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
)

// localHistorySettlements reuses only the binary official evidence produced by
// reconciliation.MarkPositionSettled. Closed/redeemed positions retain that
// evidence; position_events.mark_price is intentionally never consulted.
// The caller supplies the same READ ONLY RR transaction as the history read.
func localHistorySettlements(ctx context.Context, tx *sql.Tx, requests string, observedAt time.Time) (map[string][]history.Settlement, error) {
	rows, err := tx.QueryContext(ctx, `WITH requested AS(
 SELECT * FROM jsonb_to_recordset($1::jsonb) AS r(key text,asset jsonb,"to" timestamptz)
 ) SELECT DISTINCT r.key,p.execution_account_id,p.token_id,p.settlement_price::text,p.settlement_source,p.settled_at
 FROM requested r JOIN execution_positions p
 ON r.asset->>'market_source'='POLYMARKET'
 AND p.market_id=r.asset->>'market_id' AND p.condition_id=r.asset->>'condition_id' AND p.token_id=r.asset->>'token_id'
 AND p.outcome_index IS NOT DISTINCT FROM (r.asset->>'outcome_index')::smallint
 AND NULLIF(p.outcome_name,'') IS NOT DISTINCT FROM r.asset->>'outcome_name'
 WHERE p.lifecycle_status IN ('SETTLED_PENDING_REDEEM','CLOSED')
 AND p.outcome_index IS NOT NULL AND p.outcome_name<>''
 AND p.settlement_price IN (0,1)
 AND p.settlement_source='POLYMARKET_DATA_API:'||p.condition_id
 AND p.settled_at>to_timestamp(0) AND p.settled_at<=r."to" AND p.settled_at<=$2 AND p.settled_at<=p.updated_at
 ORDER BY r.key,p.settled_at,p.execution_account_id,p.token_id LIMIT 20001`, requests, observedAt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string][]history.Settlement{}
	count := 0
	for rows.Next() {
		var key, account, token, price, source string
		var at time.Time
		if err = rows.Scan(&key, &account, &token, &price, &source, &at); err != nil {
			return nil, err
		}
		count++
		if count > 20000 {
			return nil, fmt.Errorf("local settlement input budget exceeded")
		}
		at = at.UTC()
		result[key] = append(result[key], history.Settlement{EffectiveAt: at, Price: price, Source: "OFFICIAL_SETTLEMENT", SourceReference: "LOCAL_POSITION:" + account + ":" + token + ":" + source, FetchedAt: at})
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
