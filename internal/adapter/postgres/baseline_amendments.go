package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// BaselineAmendmentStore reads the facts an amendment is derived from and
// appends the audited amendment itself. It is used only by the operator
// command; the service never writes amendments.
type BaselineAmendmentStore struct {
	db *sql.DB
}

// NewBaselineAmendmentStore creates the amendment store.
func NewBaselineAmendmentStore(db *sql.DB) (*BaselineAmendmentStore, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	return &BaselineAmendmentStore{db: db}, nil
}

// BaselineAmendmentFacts is the ledger-side state of one token.
type BaselineAmendmentFacts struct {
	WalletAddress     string
	ManagedShares     domain.Decimal
	BaselineRemaining domain.Decimal
	// Position is the managed position identity, when the ledger has one.
	Position *BaselineAmendmentIdentity
	// Baseline is the sealed baseline item identity, when the token has one.
	Baseline *BaselineAmendmentIdentity
}

// BaselineAmendmentIdentity is the exact token identity of one source.
type BaselineAmendmentIdentity struct {
	ConditionID  string
	OutcomeIndex *int
	OutcomeName  string
	NegRisk      *bool
}

// Facts reads the wallet, managed shares, unmanaged remainder and identities.
func (store *BaselineAmendmentStore) Facts(ctx context.Context, accountID, tokenID string) (BaselineAmendmentFacts, error) {
	accountID, tokenID = strings.TrimSpace(accountID), strings.TrimSpace(tokenID)
	if accountID == "" || tokenID == "" {
		return BaselineAmendmentFacts{}, fmt.Errorf("execution account and token are required")
	}
	var facts BaselineAmendmentFacts
	if err := store.db.QueryRowContext(ctx,
		`SELECT lower(wallet_address) FROM execution_accounts WHERE execution_account_id=$1`, accountID,
	).Scan(&facts.WalletAddress); err != nil {
		return facts, fmt.Errorf("read execution account wallet: %w", err)
	}
	var managed, remaining string
	if err := store.db.QueryRowContext(ctx, `
		SELECT COALESCE((SELECT total_shares FROM execution_positions
		                  WHERE execution_account_id=$1 AND token_id=$2),0)::text,
		       COALESCE((SELECT sum(shares) FROM execution_external_position_baseline_remaining
		                  WHERE execution_account_id=$1 AND token_id=$2),0)::text`,
		accountID, tokenID).Scan(&managed, &remaining); err != nil {
		return facts, fmt.Errorf("read managed and baseline shares: %w", err)
	}
	var err error
	if facts.ManagedShares, err = domain.ParseDecimal(managed); err != nil {
		return facts, err
	}
	if facts.BaselineRemaining, err = domain.ParseDecimal(remaining); err != nil {
		return facts, err
	}

	var position BaselineAmendmentIdentity
	var positionIndex sql.NullInt64
	err = store.db.QueryRowContext(ctx, `
		SELECT condition_id, outcome_index, outcome_name FROM execution_positions
		WHERE execution_account_id=$1 AND token_id=$2`, accountID, tokenID,
	).Scan(&position.ConditionID, &positionIndex, &position.OutcomeName)
	switch {
	case err == nil:
		if positionIndex.Valid {
			value := int(positionIndex.Int64)
			position.OutcomeIndex = &value
		}
		var negRisk sql.NullBool
		if err := store.db.QueryRowContext(ctx, `
			SELECT neg_risk FROM position_lots
			WHERE execution_account_id=$1 AND token_id=$2 AND neg_risk IS NOT NULL
			ORDER BY opened_at DESC LIMIT 1`, accountID, tokenID).Scan(&negRisk); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return facts, fmt.Errorf("read position lot neg_risk: %w", err)
		}
		if negRisk.Valid {
			position.NegRisk = &negRisk.Bool
		}
		facts.Position = &position
	case errors.Is(err, sql.ErrNoRows):
	default:
		return facts, fmt.Errorf("read position identity: %w", err)
	}

	var baseline BaselineAmendmentIdentity
	var baselineIndex sql.NullInt64
	var baselineNegRisk bool
	err = store.db.QueryRowContext(ctx, `
		SELECT condition_id, outcome_index, outcome_name, neg_risk
		FROM execution_external_position_baseline_items
		WHERE execution_account_id=$1 AND token_id=$2`, accountID, tokenID,
	).Scan(&baseline.ConditionID, &baselineIndex, &baseline.OutcomeName, &baselineNegRisk)
	switch {
	case err == nil:
		if baselineIndex.Valid {
			value := int(baselineIndex.Int64)
			baseline.OutcomeIndex = &value
		}
		baseline.NegRisk = &baselineNegRisk
		facts.Baseline = &baseline
	case errors.Is(err, sql.ErrNoRows):
	default:
		return facts, fmt.Errorf("read baseline item identity: %w", err)
	}
	return facts, nil
}

// Append inserts the amendment; a repeat of the identical amendment is a
// no-op that reports inserted=false. The database triggers re-validate the
// exactness rules and keep the table append-only.
func (store *BaselineAmendmentStore) Append(ctx context.Context, amendment domain.BaselineAmendment) (bool, error) {
	var outcomeIndex any
	if amendment.OutcomeIndex != nil {
		outcomeIndex = *amendment.OutcomeIndex
	}
	result, err := store.db.ExecContext(ctx, `
		INSERT INTO execution_external_position_baseline_amendments (
			amendment_id, execution_account_id, token_id, condition_id, outcome_index,
			outcome_name, neg_risk, kind, shares, observed_at, evidence, actor, reason
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::numeric,$10,$11::jsonb,$12,$13)
		ON CONFLICT (amendment_id) DO NOTHING`,
		amendment.AmendmentID, amendment.ExecutionAccountID, amendment.TokenID, amendment.ConditionID,
		outcomeIndex, amendment.OutcomeName, amendment.NegRisk, string(amendment.Kind),
		amendment.Shares.String(), amendment.ObservedAt.UTC(), string(amendment.Evidence),
		amendment.Actor, amendment.Reason)
	if err != nil {
		return false, fmt.Errorf("append baseline amendment: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows == 1, nil
}
