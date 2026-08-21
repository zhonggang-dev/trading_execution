package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

var _ port.ExternalPositionDispositionTradeSource = (*ExternalPositionBaselineRepository)(nil)

// ListExternalPositionDispositionTrades returns exact venue trade identities
// for append-only EXTERNAL_SELL dispositions. These rows suppress duplicate
// EXTERNAL_TRADE alerts only; they never become local execution orders/fills.
func (repository *ExternalPositionBaselineRepository) ListExternalPositionDispositionTrades(
	ctx context.Context,
	executionAccountID string,
) ([]domain.ExternalPositionDispositionTrade, error) {
	executionAccountID = strings.TrimSpace(executionAccountID)
	if executionAccountID == "" {
		return nil, fmt.Errorf("execution account id is required")
	}
	rows, err := repository.db.QueryContext(ctx, `
		SELECT execution_account_id,
		       venue_trade_id,
		       venue_order_id,
		       condition_id,
		       token_id
		FROM execution_external_position_dispositions
		WHERE execution_account_id=$1
		  AND disposition_kind IN ('EXTERNAL_SELL','BASELINE_ACCOUNTED')
		ORDER BY venue_trade_id, venue_order_id, condition_id, token_id`, executionAccountID)
	if err != nil {
		return nil, fmt.Errorf("query external position disposition trades: %w", err)
	}
	defer rows.Close()

	result := make([]domain.ExternalPositionDispositionTrade, 0)
	seen := make(map[externalPositionDispositionTradeIdentity]struct{})
	for rows.Next() {
		var value domain.ExternalPositionDispositionTrade
		if err := rows.Scan(
			&value.ExecutionAccountID,
			&value.VenueTradeID,
			&value.VenueOrderID,
			&value.ConditionID,
			&value.TokenID,
		); err != nil {
			return nil, fmt.Errorf("scan external position disposition trade: %w", err)
		}
		if err := validateExternalPositionDispositionTrade(value, executionAccountID); err != nil {
			return nil, err
		}
		identity := externalPositionDispositionTradeIdentity{
			venueTradeID: value.VenueTradeID,
			venueOrderID: value.VenueOrderID,
			conditionID:  value.ConditionID,
			tokenID:      value.TokenID,
		}
		if _, duplicate := seen[identity]; duplicate {
			return nil, fmt.Errorf("external position dispositions contain duplicate exact trade identity %q", value.VenueTradeID)
		}
		seen[identity] = struct{}{}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate external position disposition trades: %w", err)
	}
	return result, nil
}

type externalPositionDispositionTradeIdentity struct {
	venueTradeID string
	venueOrderID string
	conditionID  string
	tokenID      string
}

func validateExternalPositionDispositionTrade(value domain.ExternalPositionDispositionTrade, executionAccountID string) error {
	if value.ExecutionAccountID != executionAccountID ||
		value.ExecutionAccountID != strings.TrimSpace(value.ExecutionAccountID) ||
		value.VenueTradeID == "" || value.VenueTradeID != strings.TrimSpace(value.VenueTradeID) ||
		value.VenueOrderID == "" || value.VenueOrderID != strings.TrimSpace(value.VenueOrderID) ||
		value.ConditionID == "" || value.ConditionID != strings.TrimSpace(value.ConditionID) ||
		value.TokenID == "" || value.TokenID != strings.TrimSpace(value.TokenID) {
		return fmt.Errorf("external position disposition trade has incomplete or mismatched exact identity")
	}
	return nil
}
