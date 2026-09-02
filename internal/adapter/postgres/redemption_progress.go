package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// RedemptionProgressReader reads redemptions whose chain effect may precede the
// ledger application. It is read-only and independent of the auto-redeem
// runner so reconciliation can consult it even when the runner is disabled
// (the table is then simply empty).
type RedemptionProgressReader struct {
	db *sql.DB
}

var _ port.RedemptionProgressSource = (*RedemptionProgressReader)(nil)

func NewRedemptionProgressReader(database *sql.DB) (*RedemptionProgressReader, error) {
	if database == nil {
		return nil, fmt.Errorf("redemption progress reader requires PostgreSQL")
	}
	return &RedemptionProgressReader{db: database}, nil
}

// ListInFlightRedemptions returns REDEEM_SUBMITTING, REDEEM_SUBMITTED, and
// CONFIRMED redemptions for one account. The expected payout prefers the
// confirmed receipt and otherwise uses the managed binary payout that
// ApplyRedemption will later require to match the receipt exactly.
func (reader *RedemptionProgressReader) ListInFlightRedemptions(
	ctx context.Context, executionAccountID string,
) ([]domain.InFlightRedemption, error) {
	executionAccountID = strings.TrimSpace(executionAccountID)
	if executionAccountID == "" {
		return nil, fmt.Errorf("execution account id is required")
	}
	rows, err := reader.db.QueryContext(ctx, `
		SELECT redemption.condition_id, redemption.status,
		       COALESCE(
		         (redemption.payout_base_units/1000000)::text,
		         (SELECT COALESCE(SUM(position.total_shares*position.settlement_price),0)::text
		          FROM execution_positions position
		          WHERE position.execution_account_id=redemption.execution_account_id
		            AND LOWER(position.condition_id)=redemption.condition_id
		            AND position.lifecycle_status='SETTLED_PENDING_REDEEM')
		       )
		FROM polymarket_redemptions redemption
		WHERE redemption.execution_account_id=$1
		  AND redemption.status IN ('REDEEM_SUBMITTING','REDEEM_SUBMITTED','CONFIRMED')
		ORDER BY redemption.condition_id`, executionAccountID)
	if err != nil {
		return nil, fmt.Errorf("list in-flight redemptions: %w", err)
	}
	defer rows.Close()
	redemptions := make([]domain.InFlightRedemption, 0)
	for rows.Next() {
		var conditionID, status, payout string
		if err := rows.Scan(&conditionID, &status, &payout); err != nil {
			return nil, fmt.Errorf("scan in-flight redemption: %w", err)
		}
		expectedPayout, err := domain.ParseDecimal(payout)
		if err != nil {
			return nil, fmt.Errorf("in-flight redemption %s payout: %w", conditionID, err)
		}
		redemptions = append(redemptions, domain.InFlightRedemption{
			ExecutionAccountID: executionAccountID, ConditionID: conditionID,
			Status: domain.RedemptionStatus(status), ExpectedPayout: expectedPayout,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate in-flight redemptions: %w", err)
	}
	return redemptions, nil
}
