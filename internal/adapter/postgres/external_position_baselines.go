package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
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
		WITH effective_items AS (
			SELECT item.baseline_id,
			       item.execution_account_id,
			       item.token_id,
			       item.condition_id,
			       item.outcome_index,
			       item.outcome_name,
			       item.neg_risk,
			       item.shares - COALESCE(sum(disposition.shares_delta),0) AS shares
			FROM execution_external_position_baseline_items AS item
			LEFT JOIN execution_external_position_dispositions AS disposition
			  ON disposition.baseline_id=item.baseline_id
			 AND disposition.execution_account_id=item.execution_account_id
			 AND disposition.token_id=item.token_id
			 AND disposition.disposition_kind IN ('EXTERNAL_SELL','ADOPTION')
			WHERE item.execution_account_id=$1
			GROUP BY item.baseline_id,item.execution_account_id,item.token_id,
			         item.condition_id,item.outcome_index,item.outcome_name,item.neg_risk,item.shares
		)
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
		JOIN effective_items item
		  ON item.baseline_id=baseline.baseline_id
		 AND item.execution_account_id=baseline.execution_account_id
		WHERE baseline.execution_account_id=$1
		  AND item.shares>0
		  AND NOT EXISTS (
			SELECT 1
			FROM execution_accounts account
			JOIN execution_wallet_migrations migration
			  ON migration.execution_account_id=account.execution_account_id
			 AND migration.new_wallet_address=lower(account.wallet_address)
			WHERE account.execution_account_id=baseline.execution_account_id
			  AND migration.old_wallet_address=lower(baseline.evidence->>'wallet_address')
			  AND migration.occurred_at>=baseline.observed_at
		  )
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
	cutoverTokens, err := repository.listWalletCutoverTokenIDs(ctx, executionAccountID)
	if err != nil {
		return nil, err
	}
	cutoverBaselines, err := repository.listWalletCutoverBaselines(ctx, executionAccountID)
	if err != nil {
		return nil, err
	}
	if len(cutoverTokens) == 0 && len(cutoverBaselines) == 0 {
		return baselines, nil
	}
	// A wallet-reuse cutover is a newer immutable evidence boundary for the
	// selected token. Keep historical rows for every other token, while letting
	// the selected residual row supersede an old baseline row in the read model.
	filtered := make([]domain.ExternalPositionBaseline, 0, len(baselines)+len(cutoverBaselines))
	for _, value := range baselines {
		if _, superseded := cutoverTokens[value.TokenID]; !superseded {
			filtered = append(filtered, value)
		}
	}
	filtered = append(filtered, cutoverBaselines...)
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].ConditionID != filtered[j].ConditionID {
			return filtered[i].ConditionID < filtered[j].ConditionID
		}
		if filtered[i].OutcomeIndex == nil || filtered[j].OutcomeIndex == nil {
			return filtered[i].TokenID < filtered[j].TokenID
		}
		if *filtered[i].OutcomeIndex != *filtered[j].OutcomeIndex {
			return *filtered[i].OutcomeIndex < *filtered[j].OutcomeIndex
		}
		return filtered[i].TokenID < filtered[j].TokenID
	})
	return normalizeBaselineSetID(filtered), nil
}

// normalizeBaselineSetID keeps the service-level contract that one account's
// effective ownership snapshot has one set identity, while retaining the
// original immutable source ids in each row's evidence. A reused wallet can
// legitimately have an older external baseline and a newer wallet-cutover
// residual (or multiple cutover batches for disjoint tokens).
func normalizeBaselineSetID(values []domain.ExternalPositionBaseline) []domain.ExternalPositionBaseline {
	if len(values) < 2 {
		return values
	}
	setID := values[0].BaselineID
	for _, value := range values[1:] {
		if value.BaselineID != setID {
			setID = "effective-wallet-baseline:" + value.ExecutionAccountID
			break
		}
	}
	if strings.HasPrefix(setID, "effective-wallet-baseline:") {
		for index := range values {
			values[index].BaselineID = setID
		}
	}
	return values
}

func (repository *ExternalPositionBaselineRepository) listWalletCutoverTokenIDs(
	ctx context.Context,
	executionAccountID string,
) (map[string]struct{}, error) {
	var exists bool
	if err := repository.db.QueryRowContext(ctx,
		`SELECT to_regclass('execution_wallet_cutover_assets') IS NOT NULL`).Scan(&exists); err != nil {
		return nil, fmt.Errorf("inspect wallet cutover evidence schema: %w", err)
	}
	if !exists {
		return map[string]struct{}{}, nil
	}
	rows, err := repository.db.QueryContext(ctx, `
		WITH ranked AS (
			SELECT a.token_id,
			       a.chain_shares,
			       a.managed_shares,
			       row_number() OVER (
			           PARTITION BY a.execution_account_id, a.token_id
			           ORDER BY b.created_at DESC, a.batch_id DESC
			       ) AS row_number
			  FROM execution_wallet_cutover_assets a
			  JOIN execution_wallet_cutover_batches b USING (batch_id)
			 WHERE a.execution_account_id=$1
		)
		SELECT token_id
		  FROM ranked
		 WHERE row_number=1`, executionAccountID)
	if err != nil {
		return nil, fmt.Errorf("query wallet cutover token evidence: %w", err)
	}
	defer rows.Close()
	result := make(map[string]struct{})
	for rows.Next() {
		var tokenID string
		if err := rows.Scan(&tokenID); err != nil {
			return nil, fmt.Errorf("scan wallet cutover token evidence: %w", err)
		}
		result[strings.TrimSpace(tokenID)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate wallet cutover token evidence: %w", err)
	}
	return result, nil
}

func (repository *ExternalPositionBaselineRepository) listWalletCutoverBaselines(
	ctx context.Context, executionAccountID string,
) ([]domain.ExternalPositionBaseline, error) {
	var exists bool
	if err := repository.db.QueryRowContext(ctx,
		`SELECT to_regclass('execution_wallet_cutover_assets') IS NOT NULL`).Scan(&exists); err != nil {
		return nil, fmt.Errorf("inspect wallet cutover evidence schema: %w", err)
	}
	if !exists {
		return nil, nil
	}
	rows, err := repository.db.QueryContext(ctx, `
		WITH latest AS (
			SELECT DISTINCT ON (a.execution_account_id, a.token_id)
			       a.batch_id, a.execution_account_id, a.token_id
			  FROM execution_wallet_cutover_assets a
			  JOIN execution_wallet_cutover_batches b USING (batch_id)
			 WHERE a.execution_account_id=$1
			 ORDER BY a.execution_account_id, a.token_id, b.created_at DESC, a.batch_id DESC
		)
		SELECT a.batch_id, a.execution_account_id, a.condition_id, a.token_id,
		       a.outcome_index, a.outcome_name, a.neg_risk, (a.chain_shares-a.managed_shares)::text,
		       b.observed_at, b.evidence, b.actor, b.reason
		  FROM execution_wallet_cutover_assets a
		  JOIN execution_wallet_cutover_batches b USING (batch_id)
		  JOIN latest l ON l.batch_id=a.batch_id
		               AND l.execution_account_id=a.execution_account_id
		               AND l.token_id=a.token_id
		 WHERE a.execution_account_id=$1
		   AND a.ownership IN ('RETAINED_OLD','EXTERNAL')
		   AND a.chain_shares > a.managed_shares
		 ORDER BY a.condition_id, a.outcome_index NULLS LAST, a.token_id`, executionAccountID)
	if err != nil {
		return nil, fmt.Errorf("query wallet cutover residual evidence: %w", err)
	}
	defer rows.Close()
	result := make([]domain.ExternalPositionBaseline, 0)
	seen := make(map[string]struct{})
	for rows.Next() {
		var value domain.ExternalPositionBaseline
		var batchID string
		var outcomeIndex sql.NullInt64
		var shares string
		var evidence []byte
		if err := rows.Scan(&batchID, &value.ExecutionAccountID, &value.ConditionID, &value.TokenID,
			&outcomeIndex, &value.OutcomeName, &value.NegRisk, &shares, &value.ObservedAt,
			&evidence, &value.Actor, &value.Reason); err != nil {
			return nil, fmt.Errorf("scan wallet cutover residual evidence: %w", err)
		}
		value.BaselineID = "wallet-cutover:" + batchID
		value.Source = "WALLET_REUSE_CUTOVER"
		value.Evidence = append(json.RawMessage(nil), evidence...)
		if outcomeIndex.Valid {
			index := int(outcomeIndex.Int64)
			value.OutcomeIndex = &index
		}
		parsed, err := domain.ParseDecimal(shares)
		if err != nil {
			return nil, fmt.Errorf("parse wallet cutover residual shares for token %q: %w", value.TokenID, err)
		}
		value.Shares = parsed
		if err := validateExternalPositionBaseline(value, executionAccountID); err != nil {
			return nil, err
		}
		if _, duplicate := seen[value.TokenID]; duplicate {
			return nil, fmt.Errorf("wallet cutover residual evidence contains duplicate token %q", value.TokenID)
		}
		seen[value.TokenID] = struct{}{}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate wallet cutover residual evidence: %w", err)
	}
	return result, nil
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
