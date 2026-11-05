package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	history "github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

// requirementTargets assembles only the account/source catalogue here. Opening
// and cash-flow ownership comes from the existing complete cohort projection,
// not another historical-disposition SQL implementation.
func (s *AccountPriceHistoryStore) requirementTargets(ctx context.Context, now time.Time, holding []history.Window) ([]history.Window, error) {
	if len(holding) == 0 {
		return []history.Window{}, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT l.execution_account_id,
 CASE WHEN l.external_adoption_id IS NOT NULL AND b.source='POLYMARKET_DATA_API' THEN 'POLYMARKET'
 ELSE COALESCE(NULLIF(UPPER(o.intent->>'market_source'),''),NULLIF(UPPER(o.venue),'')) END
 FROM position_lots l
 LEFT JOIN execution_orders o ON o.order_id=l.opening_order_id AND o.execution_account_id=l.execution_account_id
 LEFT JOIN execution_external_position_adoptions a ON a.external_adoption_id=l.external_adoption_id AND a.lot_id=l.lot_id
 LEFT JOIN execution_external_position_baselines b ON b.baseline_id=a.baseline_id
 WHERE l.opened_at<=$1 ORDER BY l.execution_account_id LIMIT 1001`, now.UTC())
	if err != nil {
		return nil, err
	}
	managed := []consolev2.ManagedAccount{}
	sources := map[string]string{}
	for rows.Next() {
		var account string
		var source sql.NullString
		if err = rows.Scan(&account, &source); err != nil {
			rows.Close()
			return nil, err
		}
		if account == "" || !source.Valid || source.String != "POLYMARKET" && source.String != "KALSHI" {
			rows.Close()
			return nil, fmt.Errorf("historical requirement account has no explicit source")
		}
		if previous, found := sources[account]; found && previous != source.String {
			rows.Close()
			return nil, fmt.Errorf("historical requirement account source conflicts")
		}
		sources[account] = source.String
		managed = append(managed, consolev2.ManagedAccount{ExecutionAccountID: account, MarketSource: source.String})
		if len(managed) > 1000 {
			rows.Close()
			return nil, fmt.Errorf("historical requirement account budget exceeded")
		}
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	reader := consolev2.New(s.db, consolev2.RuntimeConfig{ManagedAccounts: managed, Now: func() time.Time { return now.UTC() }})
	facts, err := reader.CohortMetricFacts(ctx, consolev2.CohortFilter{AccountScope: "all"})
	if err != nil {
		return nil, err
	}
	targets, err := history.BuildRequirements(ctx, facts)
	if err != nil {
		return nil, err
	}
	identities := map[string]bool{}
	for _, window := range holding {
		identities[window.Asset.Key()] = true
	}
	for _, target := range targets {
		if !identities[target.Asset.Key()] {
			return nil, fmt.Errorf("historical requirement identity differs from holdings")
		}
	}
	return targets, nil
}
