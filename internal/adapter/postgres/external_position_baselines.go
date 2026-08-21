package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// ExternalPositionBaselineRepository reads immutable, unmanaged position
// ownership evidence. It intentionally has no write methods.
type ExternalPositionBaselineRepository struct {
	db *sql.DB
}

var _ port.ExternalPositionBaselineSource = (*ExternalPositionBaselineRepository)(nil)

// NewExternalPositionBaselineRepository creates the read-only baseline source.
func NewExternalPositionBaselineRepository(db *sql.DB) (*ExternalPositionBaselineRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	return &ExternalPositionBaselineRepository{db: db}, nil
}

// ListExternalPositionBaselines returns the one immutable cutover baseline for
// an execution account. A missing baseline is represented by an empty slice.
func (repository *ExternalPositionBaselineRepository) ListExternalPositionBaselines(
	ctx context.Context,
	executionAccountID string,
) ([]domain.ExternalPositionBaseline, error) {
	executionAccountID = strings.TrimSpace(executionAccountID)
	if executionAccountID == "" {
		return nil, fmt.Errorf("execution account id is required")
	}
	rows, err := repository.db.QueryContext(ctx, `
		SELECT baseline.baseline_id,
		       baseline.execution_account_id,
		       item.condition_id,
		       item.token_id,
		       item.outcome_index,
		       item.outcome_name,
		       item.neg_risk,
		       item.shares::text,
		       baseline.source,
		       baseline.observed_at,
		       baseline.evidence,
		       baseline.actor,
		       baseline.reason
		FROM execution_external_position_baselines baseline
		JOIN execution_external_position_baseline_items item
		  ON item.baseline_id=baseline.baseline_id
		 AND item.execution_account_id=baseline.execution_account_id
		WHERE baseline.execution_account_id=$1
		ORDER BY item.condition_id, item.outcome_index NULLS LAST, item.token_id`, executionAccountID)
	if err != nil {
		return nil, fmt.Errorf("query external position ownership baseline: %w", err)
	}
	defer rows.Close()

	baselines := make([]domain.ExternalPositionBaseline, 0)
	seenTokens := make(map[string]struct{})
	for rows.Next() {
		var baseline domain.ExternalPositionBaseline
		var outcomeIndex sql.NullInt64
		var shares string
		var evidence []byte
		if err := rows.Scan(
			&baseline.BaselineID,
			&baseline.ExecutionAccountID,
			&baseline.ConditionID,
			&baseline.TokenID,
			&outcomeIndex,
			&baseline.OutcomeName,
			&baseline.NegRisk,
			&shares,
			&baseline.Source,
			&baseline.ObservedAt,
			&evidence,
			&baseline.Actor,
			&baseline.Reason,
		); err != nil {
			return nil, fmt.Errorf("scan external position ownership baseline: %w", err)
		}
		baseline.BaselineID = strings.TrimSpace(baseline.BaselineID)
		baseline.ExecutionAccountID = strings.TrimSpace(baseline.ExecutionAccountID)
		baseline.ConditionID = strings.TrimSpace(baseline.ConditionID)
		baseline.TokenID = strings.TrimSpace(baseline.TokenID)
		baseline.OutcomeName = strings.TrimSpace(baseline.OutcomeName)
		baseline.Source = strings.TrimSpace(baseline.Source)
		baseline.Actor = strings.TrimSpace(baseline.Actor)
		baseline.Reason = strings.TrimSpace(baseline.Reason)
		baseline.ObservedAt = baseline.ObservedAt.UTC()
		baseline.Evidence = append(json.RawMessage(nil), evidence...)
		if outcomeIndex.Valid {
			value := int(outcomeIndex.Int64)
			baseline.OutcomeIndex = &value
		}
		parsedShares, parseErr := domain.ParseDecimal(shares)
		if parseErr != nil {
			return nil, fmt.Errorf("parse external position baseline shares for token %q: %w", baseline.TokenID, parseErr)
		}
		baseline.Shares = parsedShares
		if err := validateExternalPositionBaseline(baseline, executionAccountID); err != nil {
			return nil, err
		}
		if _, exists := seenTokens[baseline.TokenID]; exists {
			return nil, fmt.Errorf("external position baseline contains duplicate token %q", baseline.TokenID)
		}
		seenTokens[baseline.TokenID] = struct{}{}
		baselines = append(baselines, baseline)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate external position ownership baseline: %w", err)
	}
	return baselines, nil
}

func validateExternalPositionBaseline(baseline domain.ExternalPositionBaseline, executionAccountID string) error {
	if baseline.BaselineID == "" || baseline.ExecutionAccountID != executionAccountID ||
		baseline.ConditionID == "" || baseline.TokenID == "" || baseline.OutcomeName == "" ||
		baseline.Source == "" || baseline.ObservedAt.IsZero() || baseline.Actor == "" || baseline.Reason == "" {
		return fmt.Errorf("external position baseline has incomplete or mismatched identity for token %q", baseline.TokenID)
	}
	if baseline.OutcomeIndex != nil && *baseline.OutcomeIndex < 0 {
		return fmt.Errorf("external position baseline token %q has invalid outcome index", baseline.TokenID)
	}
	if sign, err := baseline.Shares.Sign(); err != nil || sign <= 0 {
		return fmt.Errorf("external position baseline token %q must have positive shares", baseline.TokenID)
	}
	if !json.Valid(baseline.Evidence) {
		return fmt.Errorf("external position baseline token %q has invalid evidence", baseline.TokenID)
	}
	var evidence map[string]any
	if err := json.Unmarshal(baseline.Evidence, &evidence); err != nil || len(evidence) == 0 {
		return fmt.Errorf("external position baseline token %q must have non-empty object evidence", baseline.TokenID)
	}
	return nil
}
