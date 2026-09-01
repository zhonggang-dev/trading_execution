package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type RedemptionStore struct {
	db             *sql.DB
	accountIDs     []string
	managedAccount map[string]struct{}
}

var _ port.RedemptionStore = (*RedemptionStore)(nil)

func NewRedemptionStore(database *sql.DB, executionAccountIDs []string) (*RedemptionStore, error) {
	if database == nil {
		return nil, fmt.Errorf("redemption store requires PostgreSQL")
	}
	accountIDs := make([]string, 0, len(executionAccountIDs))
	managed := make(map[string]struct{}, len(executionAccountIDs))
	for _, raw := range executionAccountIDs {
		accountID := strings.TrimSpace(raw)
		if accountID == "" {
			return nil, fmt.Errorf("redemption store account id is empty")
		}
		if _, duplicate := managed[accountID]; duplicate {
			return nil, fmt.Errorf("redemption store account %q is duplicated", accountID)
		}
		managed[accountID] = struct{}{}
		accountIDs = append(accountIDs, accountID)
	}
	if len(accountIDs) == 0 {
		return nil, fmt.Errorf("redemption store requires at least one managed account")
	}
	return &RedemptionStore{db: database, accountIDs: accountIDs, managedAccount: managed}, nil
}

func (store *RedemptionStore) requireManaged(redemption domain.Redemption) error {
	if _, ok := store.managedAccount[strings.TrimSpace(redemption.ExecutionAccountID)]; !ok {
		return fmt.Errorf("redemption account %q is outside the managed account scope", redemption.ExecutionAccountID)
	}
	return nil
}

// SyncPendingRedemptions discovers only already-settled managed positions. A
// residual immutable baseline or inconsistent neg-risk identity is persisted
// as MANUAL_REVIEW, never silently swept into a wallet-wide redemption.
func (store *RedemptionStore) SyncPendingRedemptions(ctx context.Context) error {
	_, err := store.db.ExecContext(ctx, `
		WITH baseline_remaining AS (
			SELECT item.execution_account_id, LOWER(item.condition_id) AS condition_id,
			       SUM(COALESCE((
				   SELECT disposition.shares_after
				   FROM execution_external_position_dispositions disposition
				   WHERE disposition.baseline_id=item.baseline_id
				     AND disposition.token_id=item.token_id
				     AND disposition.disposition_kind IN ('EXTERNAL_SELL','ADOPTION')
				   ORDER BY disposition.transition_sequence DESC LIMIT 1
			   ), item.shares)) AS shares
			FROM execution_external_position_baseline_items item
			GROUP BY item.execution_account_id, LOWER(item.condition_id)
		), candidates AS (
			SELECT position.execution_account_id, LOWER(position.condition_id) AS condition_id,
			       LOWER(account.wallet_address) AS wallet_address,
			       COALESCE(BOOL_AND(lot.neg_risk), FALSE) AS neg_risk,
			       COUNT(DISTINCT lot.neg_risk) AS neg_risk_values,
			       COUNT(*) FILTER (WHERE lot.lot_id IS NULL OR lot.neg_risk IS NULL) AS invalid_lots,
			       COALESCE(MAX(baseline.shares),0) AS baseline_shares
			FROM execution_positions position
			JOIN execution_accounts account USING (execution_account_id)
			LEFT JOIN position_lots lot
			  ON lot.execution_account_id=position.execution_account_id
			 AND lot.token_id=position.token_id
			 AND lot.status='SETTLED_PENDING_REDEEM'
			LEFT JOIN baseline_remaining baseline
			  ON baseline.execution_account_id=position.execution_account_id
			 AND baseline.condition_id=LOWER(position.condition_id)
			WHERE position.lifecycle_status='SETTLED_PENDING_REDEEM'
			  AND position.settlement_price IN (0,1)
			  AND position.execution_account_id=ANY($1::text[])
			GROUP BY position.execution_account_id, LOWER(position.condition_id), LOWER(account.wallet_address)
		)
		INSERT INTO polymarket_redemptions (
			execution_account_id, condition_id, wallet_address, neg_risk,
			status, last_error, next_attempt_at
		)
		SELECT execution_account_id, condition_id, wallet_address, neg_risk,
		       CASE WHEN invalid_lots<>0 OR neg_risk_values<>1 OR baseline_shares<>0 THEN 'MANUAL_REVIEW' ELSE 'READY' END,
		       CASE
		         WHEN invalid_lots<>0 THEN 'settled position is missing complete settled-lot identity'
		         WHEN neg_risk_values<>1 THEN 'settled lots do not have one complete neg_risk identity'
		         WHEN baseline_shares<>0 THEN 'unmanaged baseline shares remain for this condition'
		         ELSE ''
		       END,
		       clock_timestamp()
		FROM candidates
			ON CONFLICT (execution_account_id, condition_id) DO NOTHING`, store.accountIDs)
	if err != nil {
		return fmt.Errorf("sync pending Polymarket redemptions: %w", err)
	}
	return nil
}

const redemptionSelect = `
	SELECT execution_account_id, condition_id, wallet_address, neg_risk, status,
	       submission_provider, submission_reference, transaction_hash, event_type,
	       COALESCE(payout_base_units::text,''), COALESCE(receipt_block_number,0),
	       receipt_block_hash, confirmations, attempts, last_error, next_attempt_at,
	       created_at, updated_at, submitting_at, submitted_at, confirmed_at, applied_at
	FROM polymarket_redemptions`

func (store *RedemptionStore) ListDueRedemptions(
	ctx context.Context, limit int, now time.Time,
) ([]domain.Redemption, error) {
	if limit <= 0 || limit > 1000 || now.IsZero() {
		return nil, fmt.Errorf("redemption list requires a limit in [1,1000] and current time")
	}
	rows, err := store.db.QueryContext(ctx, redemptionSelect+`
		WHERE status NOT IN ('APPLIED','MANUAL_REVIEW') AND next_attempt_at<=$1
		  AND execution_account_id=ANY($3::text[])
		ORDER BY next_attempt_at, created_at LIMIT $2`, now.UTC(), limit, store.accountIDs)
	if err != nil {
		return nil, fmt.Errorf("list due redemptions: %w", err)
	}
	defer rows.Close()
	redemptions := make([]domain.Redemption, 0)
	for rows.Next() {
		redemption, scanErr := scanRedemption(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		redemptions = append(redemptions, redemption)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return redemptions, nil
}

func scanRedemption(row rowScanner) (domain.Redemption, error) {
	var redemption domain.Redemption
	var status string
	var blockNumber, confirmations int64
	var submitting, submitted, confirmed, applied sql.NullTime
	if err := row.Scan(
		&redemption.ExecutionAccountID, &redemption.ConditionID, &redemption.WalletAddress,
		&redemption.NegRisk, &status, &redemption.SubmissionProvider,
		&redemption.SubmissionReference, &redemption.TransactionHash, &redemption.EventType,
		&redemption.PayoutBaseUnits, &blockNumber, &redemption.ReceiptBlockHash,
		&confirmations, &redemption.Attempts, &redemption.LastError, &redemption.NextAttemptAt,
		&redemption.CreatedAt, &redemption.UpdatedAt, &submitting, &submitted, &confirmed, &applied,
	); err != nil {
		return domain.Redemption{}, err
	}
	if blockNumber < 0 || confirmations < 0 {
		return domain.Redemption{}, fmt.Errorf("redemption block evidence is negative")
	}
	redemption.Status = domain.RedemptionStatus(status)
	redemption.ReceiptBlockNumber = uint64(blockNumber)
	redemption.Confirmations = uint64(confirmations)
	redemption.NextAttemptAt = redemption.NextAttemptAt.UTC()
	redemption.CreatedAt = redemption.CreatedAt.UTC()
	redemption.UpdatedAt = redemption.UpdatedAt.UTC()
	redemption.SubmittingAt = utcNullTime(submitting)
	redemption.SubmittedAt = utcNullTime(submitted)
	redemption.ConfirmedAt = utcNullTime(confirmed)
	redemption.AppliedAt = utcNullTime(applied)
	return redemption, nil
}

func utcNullTime(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	result := value.Time.UTC()
	return &result
}

func (store *RedemptionStore) BeginRedemptionSubmission(
	ctx context.Context, redemption domain.Redemption, kind domain.RedemptionSubmissionKind, startedAt time.Time,
) error {
	if err := store.requireManaged(redemption); err != nil {
		return err
	}
	if redemption.Status != domain.RedemptionReady || startedAt.IsZero() {
		return fmt.Errorf("only a READY redemption can begin submission")
	}
	status := domain.RedemptionApprovalSubmitting
	if kind == domain.RedemptionSubmissionRedeem {
		status = domain.RedemptionRedeemSubmitting
	} else if kind != domain.RedemptionSubmissionApproval {
		return fmt.Errorf("redemption submission kind is invalid")
	}
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockExecutionAccount(ctx, tx, redemption.ExecutionAccountID); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE polymarket_redemptions
		SET status=$3, attempts=attempts+1, last_error='', submitting_at=$4,
		    updated_at=$4, next_attempt_at=$4
		WHERE execution_account_id=$1 AND condition_id=$2 AND status='READY'
		  AND wallet_address=$5 AND neg_risk=$6
		  AND EXISTS (
			SELECT 1 FROM execution_positions position
			WHERE position.execution_account_id=$1 AND LOWER(position.condition_id)=$2
			  AND position.lifecycle_status='SETTLED_PENDING_REDEEM'
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM execution_positions position
			WHERE position.execution_account_id=$1 AND LOWER(position.condition_id)=$2
			  AND position.total_shares>0
			  AND (position.lifecycle_status<>'SETTLED_PENDING_REDEEM'
			       OR position.settlement_price NOT IN (0,1) OR position.reserved_shares<>0)
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM execution_positions position
			WHERE position.execution_account_id=$1 AND LOWER(position.condition_id)=$2
			  AND position.lifecycle_status='SETTLED_PENDING_REDEEM'
			  AND (
				NOT EXISTS (
					SELECT 1 FROM position_lots lot
					WHERE lot.execution_account_id=position.execution_account_id
					  AND lot.token_id=position.token_id
					  AND lot.status='SETTLED_PENDING_REDEEM'
					  AND lot.neg_risk=$6
				)
				OR position.total_shares<>(
					SELECT COALESCE(SUM(lot.remaining_shares),0) FROM position_lots lot
					WHERE lot.execution_account_id=position.execution_account_id
					  AND lot.token_id=position.token_id AND lot.status='SETTLED_PENDING_REDEEM'
				)
				OR position.cost_basis<>(
					SELECT COALESCE(SUM(lot.remaining_cost),0) FROM position_lots lot
					WHERE lot.execution_account_id=position.execution_account_id
					  AND lot.token_id=position.token_id AND lot.status='SETTLED_PENDING_REDEEM'
				)
			  )
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM position_lots lot
			WHERE lot.execution_account_id=$1 AND LOWER(lot.condition_id)=$2
			  AND lot.remaining_shares>0
			  AND (lot.status<>'SETTLED_PENDING_REDEEM' OR lot.neg_risk IS DISTINCT FROM $6)
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM execution_external_position_baseline_items item
			WHERE item.execution_account_id=$1 AND LOWER(item.condition_id)=$2
			  AND COALESCE((
				SELECT disposition.shares_after
				FROM execution_external_position_dispositions disposition
				WHERE disposition.baseline_id=item.baseline_id AND disposition.token_id=item.token_id
				  AND disposition.disposition_kind IN ('EXTERNAL_SELL','ADOPTION')
				ORDER BY disposition.transition_sequence DESC LIMIT 1
			  ), item.shares)>0
		  )`,
		redemption.ExecutionAccountID, redemption.ConditionID, string(status), startedAt.UTC(),
		strings.ToLower(redemption.WalletAddress), redemption.NegRisk)
	if err != nil {
		return err
	}
	if err := requireOneRedemption(result); err != nil {
		return fmt.Errorf("redemption safety preconditions changed before submission: %w", err)
	}
	return tx.Commit()
}

func (store *RedemptionStore) RecordRedemptionSubmission(
	ctx context.Context, redemption domain.Redemption, submission domain.RedemptionSubmission,
	submittedAt, nextAttemptAt time.Time,
) error {
	if err := store.requireManaged(redemption); err != nil {
		return err
	}
	if submittedAt.IsZero() || nextAttemptAt.IsZero() || strings.TrimSpace(submission.Provider) == "" ||
		strings.TrimSpace(submission.Reference) == "" {
		return fmt.Errorf("redemption submission identity and timestamps are required")
	}
	nextStatus := domain.RedemptionApprovalSubmitted
	if redemption.Status == domain.RedemptionRedeemSubmitting {
		nextStatus = domain.RedemptionRedeemSubmitted
	} else if redemption.Status != domain.RedemptionApprovalSubmitting {
		return fmt.Errorf("redemption is not in a submitting state")
	}
	transactionHash := strings.ToLower(strings.TrimSpace(submission.TransactionHash))
	result, err := store.db.ExecContext(ctx, `
		UPDATE polymarket_redemptions
		SET status=$4, submission_provider=$5, submission_reference=$6,
		    transaction_hash=$7, submitted_at=$8, updated_at=$8, next_attempt_at=$9
		WHERE execution_account_id=$1 AND condition_id=$2 AND status=$3`,
		redemption.ExecutionAccountID, redemption.ConditionID, string(redemption.Status), string(nextStatus),
		submission.Provider, submission.Reference, transactionHash, submittedAt.UTC(), nextAttemptAt.UTC())
	if err != nil {
		return err
	}
	return requireOneRedemption(result)
}

func (store *RedemptionStore) ResetRedemptionReady(
	ctx context.Context, redemption domain.Redemption, nextAttemptAt time.Time,
) error {
	if err := store.requireManaged(redemption); err != nil {
		return err
	}
	if nextAttemptAt.IsZero() || (redemption.Status != domain.RedemptionApprovalSubmitting && redemption.Status != domain.RedemptionApprovalSubmitted) {
		return fmt.Errorf("only an approval workflow can reset to READY")
	}
	result, err := store.db.ExecContext(ctx, `
		UPDATE polymarket_redemptions
		SET status='READY', submission_provider='', submission_reference='', transaction_hash='',
		    last_error='', submitting_at=NULL, submitted_at=NULL, updated_at=clock_timestamp(),
		    next_attempt_at=$4
		WHERE execution_account_id=$1 AND condition_id=$2 AND status=$3`,
		redemption.ExecutionAccountID, redemption.ConditionID, string(redemption.Status), nextAttemptAt.UTC())
	if err != nil {
		return err
	}
	return requireOneRedemption(result)
}

func (store *RedemptionStore) RecordRedemptionTransaction(
	ctx context.Context, redemption domain.Redemption, transactionHash string, nextAttemptAt time.Time,
) error {
	if err := store.requireManaged(redemption); err != nil {
		return err
	}
	transactionHash = strings.ToLower(strings.TrimSpace(transactionHash))
	if nextAttemptAt.IsZero() || len(transactionHash) != 66 || !strings.HasPrefix(transactionHash, "0x") {
		return fmt.Errorf("redemption transaction hash and next attempt are required")
	}
	result, err := store.db.ExecContext(ctx, `
		UPDATE polymarket_redemptions
		SET transaction_hash=$4, updated_at=clock_timestamp(), next_attempt_at=$5
		WHERE execution_account_id=$1 AND condition_id=$2 AND status=$3
		  AND (transaction_hash='' OR transaction_hash=$4)`,
		redemption.ExecutionAccountID, redemption.ConditionID, string(redemption.Status), transactionHash, nextAttemptAt.UTC())
	if err != nil {
		return err
	}
	return requireOneRedemption(result)
}

func (store *RedemptionStore) RecordRedemptionConfirmed(
	ctx context.Context, redemption domain.Redemption, receipt domain.RedemptionReceipt, confirmedAt time.Time,
) error {
	if err := store.requireManaged(redemption); err != nil {
		return err
	}
	if confirmedAt.IsZero() || receipt.EventType != "POSITIONS_REDEEMED" || receipt.BlockNumber == 0 ||
		receipt.Confirmations == 0 || receipt.TransactionHash == "" || receipt.PayoutBaseUnits == "" {
		return fmt.Errorf("complete redemption receipt evidence is required")
	}
	if receipt.BlockNumber > math.MaxInt64 || receipt.Confirmations > math.MaxInt64 {
		return fmt.Errorf("redemption receipt evidence overflows PostgreSQL bigint")
	}
	if !strings.EqualFold(receipt.WalletAddress, redemption.WalletAddress) ||
		!strings.EqualFold(receipt.ConditionID, redemption.ConditionID) {
		return fmt.Errorf("redemption receipt identity mismatch")
	}
	if redemption.Status != domain.RedemptionRedeemSubmitting && redemption.Status != domain.RedemptionRedeemSubmitted {
		return fmt.Errorf("only a redeem submission can be confirmed")
	}
	result, err := store.db.ExecContext(ctx, `
		UPDATE polymarket_redemptions
		SET status='CONFIRMED', transaction_hash=$4, event_type=$5,
		    payout_base_units=$6::numeric, receipt_block_number=$7,
		    receipt_block_hash=$8, confirmations=$9, confirmed_at=$10,
		    updated_at=$10, next_attempt_at=$10, last_error=''
		WHERE execution_account_id=$1 AND condition_id=$2 AND status=$3
		  AND (transaction_hash='' OR transaction_hash=$4)`,
		redemption.ExecutionAccountID, redemption.ConditionID, string(redemption.Status),
		strings.ToLower(receipt.TransactionHash), receipt.EventType, receipt.PayoutBaseUnits,
		int64(receipt.BlockNumber), strings.ToLower(receipt.BlockHash), int64(receipt.Confirmations), confirmedAt.UTC())
	if err != nil {
		return err
	}
	return requireOneRedemption(result)
}

func (store *RedemptionStore) RetryRedemption(
	ctx context.Context, redemption domain.Redemption, reason string, nextAttemptAt time.Time,
) error {
	if err := store.requireManaged(redemption); err != nil {
		return err
	}
	reason = boundedRedemptionError(reason)
	if reason == "" || nextAttemptAt.IsZero() {
		return fmt.Errorf("redemption retry reason and time are required")
	}
	result, err := store.db.ExecContext(ctx, `
		UPDATE polymarket_redemptions SET last_error=$4, next_attempt_at=$5, updated_at=clock_timestamp()
		WHERE execution_account_id=$1 AND condition_id=$2 AND status=$3`,
		redemption.ExecutionAccountID, redemption.ConditionID, string(redemption.Status), reason, nextAttemptAt.UTC())
	if err != nil {
		return err
	}
	return requireOneRedemption(result)
}

func (store *RedemptionStore) ReviewRedemption(
	ctx context.Context, redemption domain.Redemption, reason string, reviewedAt time.Time,
) error {
	if err := store.requireManaged(redemption); err != nil {
		return err
	}
	reason = boundedRedemptionError(reason)
	if reason == "" || reviewedAt.IsZero() {
		return fmt.Errorf("redemption review reason and time are required")
	}
	result, err := store.db.ExecContext(ctx, `
		UPDATE polymarket_redemptions
		SET status='MANUAL_REVIEW', last_error=$4, next_attempt_at=$5, updated_at=$5
		WHERE execution_account_id=$1 AND condition_id=$2 AND status=$3
		  AND status NOT IN ('APPLIED','MANUAL_REVIEW')`,
		redemption.ExecutionAccountID, redemption.ConditionID, string(redemption.Status), reason, reviewedAt.UTC())
	if err != nil {
		return err
	}
	return requireOneRedemption(result)
}

func boundedRedemptionError(reason string) string {
	reason = strings.Join(strings.Fields(reason), " ")
	if len(reason) > 2000 {
		reason = reason[:2000]
	}
	return reason
}

func requireOneRedemption(result sql.Result) error {
	if !oneRow(result) {
		return fmt.Errorf("redemption state changed concurrently")
	}
	return nil
}

type redemptionPosition struct {
	position domain.Position
	negRisk  bool
}

func (store *RedemptionStore) ApplyRedemption(
	ctx context.Context, redemption domain.Redemption, appliedAt time.Time,
) error {
	if err := store.requireManaged(redemption); err != nil {
		return err
	}
	if redemption.Status != domain.RedemptionConfirmed || appliedAt.IsZero() {
		return fmt.Errorf("only a confirmed redemption can be applied")
	}
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockExecutionAccount(ctx, tx, redemption.ExecutionAccountID); err != nil {
		return err
	}
	current, err := scanRedemption(tx.QueryRowContext(ctx, redemptionSelect+`
		WHERE execution_account_id=$1 AND condition_id=$2 FOR UPDATE`,
		redemption.ExecutionAccountID, redemption.ConditionID))
	if err != nil {
		return err
	}
	if current.Status == domain.RedemptionApplied {
		return tx.Commit()
	}
	if current.Status != domain.RedemptionConfirmed || current.TransactionHash != redemption.TransactionHash ||
		current.PayoutBaseUnits != redemption.PayoutBaseUnits {
		return fmt.Errorf("confirmed redemption evidence changed before application")
	}

	baseline, err := numeric(ctx, tx, `
		WITH remaining AS (
			SELECT COALESCE((
				SELECT disposition.shares_after
				FROM execution_external_position_dispositions disposition
				WHERE disposition.baseline_id=item.baseline_id AND disposition.token_id=item.token_id
				  AND disposition.disposition_kind IN ('EXTERNAL_SELL','ADOPTION')
				ORDER BY disposition.transition_sequence DESC LIMIT 1
			), item.shares) AS shares
			FROM execution_external_position_baseline_items item
			WHERE item.execution_account_id=$1 AND LOWER(item.condition_id)=$2
		)
		SELECT COALESCE(SUM(shares),0)::text FROM remaining`, redemption.ExecutionAccountID, redemption.ConditionID)
	if err != nil {
		return err
	}
	if sign, signErr := baseline.Sign(); signErr != nil || sign != 0 {
		return fmt.Errorf("unmanaged baseline shares remain for redeemed condition")
	}

	rows, err := tx.QueryContext(ctx, positionSelect+`
		WHERE execution_account_id=$1 AND LOWER(condition_id)=$2
		  AND lifecycle_status='SETTLED_PENDING_REDEEM'
		ORDER BY token_id FOR UPDATE`, redemption.ExecutionAccountID, redemption.ConditionID)
	if err != nil {
		return err
	}
	positions := make([]domain.Position, 0, 2)
	for rows.Next() {
		position, scanErr := scanPosition(rows)
		if scanErr != nil {
			rows.Close()
			return scanErr
		}
		positions = append(positions, position)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(positions) == 0 {
		return fmt.Errorf("confirmed redemption has no settled managed positions")
	}
	for _, position := range positions {
		if position.SettlementPrice.IsEmpty() || (!position.SettlementPrice.Equal("0") && !position.SettlementPrice.Equal("1")) ||
			!position.ReservedShares.Equal("0") {
			return fmt.Errorf("settled position %s is not safe to redeem", position.TokenID)
		}
	}

	expectedPayout, err := numeric(ctx, tx, `
		SELECT COALESCE(SUM(total_shares*settlement_price),0)::text
		FROM execution_positions
		WHERE execution_account_id=$1 AND LOWER(condition_id)=$2
		  AND lifecycle_status='SETTLED_PENDING_REDEEM'`, redemption.ExecutionAccountID, redemption.ConditionID)
	if err != nil {
		return err
	}
	payoutBase, ok := new(big.Int).SetString(current.PayoutBaseUnits, 10)
	if !ok || payoutBase.Sign() < 0 {
		return fmt.Errorf("confirmed redemption payout base units are invalid")
	}
	actualPayout, err := numeric(ctx, tx, `SELECT ($1::numeric/1000000)::text`, payoutBase.String())
	if err != nil {
		return err
	}
	if !actualPayout.Equal(expectedPayout) {
		return fmt.Errorf("redemption payout %s does not equal managed binary payout %s", actualPayout, expectedPayout)
	}

	for _, position := range positions {
		if err := applyRedeemedPosition(ctx, tx, current, position, appliedAt.UTC()); err != nil {
			return err
		}
	}
	var balanceBefore, availableBefore, reservedBefore domain.Decimal
	if err := tx.QueryRowContext(ctx, `
		SELECT total_balance::text, available_balance::text, reserved_balance::text
		FROM execution_accounts WHERE execution_account_id=$1 FOR UPDATE`, redemption.ExecutionAccountID).Scan(
		&balanceBefore, &availableBefore, &reservedBefore,
	); err != nil {
		return err
	}
	balanceAfter, err := numeric(ctx, tx, `SELECT ($1::numeric+$2::numeric)::text`, balanceBefore.String(), actualPayout.String())
	if err != nil {
		return err
	}
	availableAfter, err := numeric(ctx, tx, `SELECT ($1::numeric+$2::numeric)::text`, availableBefore.String(), actualPayout.String())
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE execution_accounts
		SET total_balance=$2::numeric, available_balance=$3::numeric,
		    version=version+1, updated_at=$4
		WHERE execution_account_id=$1`, redemption.ExecutionAccountID,
		balanceAfter.String(), availableAfter.String(), appliedAt.UTC()); err != nil {
		return err
	}
	accountEventID := "account-redemption:" + current.TransactionHash
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO execution_account_events (
			account_event_id, execution_account_id, event_type, total_balance_delta,
			available_balance_delta, reserved_balance_delta, total_balance_after,
			available_balance_after, reserved_balance_after, occurred_at
		) VALUES ($1,$2,'REDEEMED',$3::numeric,$3::numeric,0,$4::numeric,$5::numeric,$6::numeric,$7)`,
		accountEventID, redemption.ExecutionAccountID, actualPayout.String(), balanceAfter.String(),
		availableAfter.String(), reservedBefore.String(), appliedAt.UTC()); err != nil {
		return err
	}
	if _, err := insertOutbox(ctx, tx, "trading.account.redeemed.v1", accountEventID,
		redemption.ExecutionAccountID, current, appliedAt.UTC()); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE polymarket_redemptions
		SET status='APPLIED', applied_at=$3, updated_at=$3, next_attempt_at=$3
		WHERE execution_account_id=$1 AND condition_id=$2 AND status='CONFIRMED'`,
		redemption.ExecutionAccountID, redemption.ConditionID, appliedAt.UTC())
	if err != nil {
		return err
	}
	if !oneRow(result) {
		return fmt.Errorf("redemption changed before accounting commit")
	}
	return tx.Commit()
}

func applyRedeemedPosition(
	ctx context.Context, tx *sql.Tx, redemption domain.Redemption, position domain.Position, appliedAt time.Time,
) error {
	payout, err := numeric(ctx, tx, `SELECT ($1::numeric*$2::numeric)::text`, position.TotalShares.String(), position.SettlementPrice.String())
	if err != nil {
		return err
	}
	realizedDelta, err := numeric(ctx, tx, `SELECT ($1::numeric-$2::numeric)::text`, payout.String(), position.CostBasis.String())
	if err != nil {
		return err
	}
	realizedAfter, err := numeric(ctx, tx, `SELECT ($1::numeric+$2::numeric)::text`, position.RealizedPnL.String(), realizedDelta.String())
	if err != nil {
		return err
	}
	lotRows, err := tx.QueryContext(ctx, `
		SELECT lot_id, remaining_shares::text, remaining_cost::text, neg_risk
		FROM position_lots
		WHERE execution_account_id=$1 AND token_id=$2 AND status='SETTLED_PENDING_REDEEM'
		ORDER BY opened_at, lot_id FOR UPDATE`, position.ExecutionAccountID, position.TokenID)
	if err != nil {
		return err
	}
	type lot struct {
		id           string
		shares, cost domain.Decimal
		negRisk      sql.NullBool
	}
	lots := make([]lot, 0)
	for lotRows.Next() {
		var item lot
		if scanErr := lotRows.Scan(&item.id, &item.shares, &item.cost, &item.negRisk); scanErr != nil {
			lotRows.Close()
			return scanErr
		}
		lots = append(lots, item)
	}
	lotRows.Close()
	if len(lots) == 0 {
		return fmt.Errorf("settled position %s has no settled lots", position.TokenID)
	}
	lotShares, lotCost := domain.Decimal("0"), domain.Decimal("0")
	for _, item := range lots {
		lotShares, err = numeric(ctx, tx, `SELECT ($1::numeric+$2::numeric)::text`, lotShares.String(), item.shares.String())
		if err != nil {
			return err
		}
		lotCost, err = numeric(ctx, tx, `SELECT ($1::numeric+$2::numeric)::text`, lotCost.String(), item.cost.String())
		if err != nil {
			return err
		}
	}
	if !lotShares.Equal(position.TotalShares) || !lotCost.Equal(position.CostBasis) {
		return fmt.Errorf("settled lot balances do not equal position %s", position.TokenID)
	}
	for _, item := range lots {
		if !item.negRisk.Valid || item.negRisk.Bool != redemption.NegRisk {
			return fmt.Errorf("lot %s redemption adapter identity mismatch", item.id)
		}
		lotPayout, calcErr := numeric(ctx, tx, `SELECT ($1::numeric*$2::numeric)::text`, item.shares.String(), position.SettlementPrice.String())
		if calcErr != nil {
			return calcErr
		}
		lotPnL, calcErr := numeric(ctx, tx, `SELECT ($1::numeric-$2::numeric)::text`, lotPayout.String(), item.cost.String())
		if calcErr != nil {
			return calcErr
		}
		result, updateErr := tx.ExecContext(ctx, `
			UPDATE position_lots SET remaining_shares=0, remaining_cost=0,
			    status='CLOSED', closed_at=$2
			WHERE lot_id=$1 AND status='SETTLED_PENDING_REDEEM'`, item.id, appliedAt)
		if updateErr != nil {
			return updateErr
		}
		if !oneRow(result) {
			return fmt.Errorf("settled lot %s changed before redemption accounting", item.id)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO position_lot_redemptions (
				redemption_id, execution_account_id, condition_id, transaction_hash, lot_id,
				redeemed_shares, allocated_cost, allocated_payout, realized_pnl, redeemed_at
			) VALUES ($1,$2,$3,$4,$5,$6::numeric,$7::numeric,$8::numeric,$9::numeric,$10)`,
			"lot-redemption:"+redemption.TransactionHash+":"+item.id, position.ExecutionAccountID,
			redemption.ConditionID, redemption.TransactionHash, item.id, item.shares.String(),
			item.cost.String(), lotPayout.String(), lotPnL.String(), appliedAt); err != nil {
			return err
		}
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE execution_positions
		SET total_shares=0, available_shares=0, reserved_shares=0, cost_basis=0,
		    average_cost_price=0, realized_pnl=$3::numeric, mark_price=settlement_price,
		    market_value=0, unrealized_pnl=0, is_dust=FALSE, lifecycle_status='CLOSED',
		    last_marked_at=$4, updated_at=$4, version=version+1
		WHERE execution_account_id=$1 AND token_id=$2
		  AND lifecycle_status='SETTLED_PENDING_REDEEM'`,
		position.ExecutionAccountID, position.TokenID, realizedAfter.String(), appliedAt)
	if err != nil {
		return err
	}
	if !oneRow(result) {
		return fmt.Errorf("settled position changed before redemption accounting")
	}
	event := domain.PositionEvent{
		EventID:   "position-redemption:" + redemption.TransactionHash + ":" + position.TokenID,
		EventType: domain.PositionEventRedeemed, ExecutionAccountID: position.ExecutionAccountID,
		MarketID: position.MarketID, TokenID: position.TokenID,
		FillKey:     "redemption:" + redemption.TransactionHash,
		SharesDelta: domain.Decimal("-" + position.TotalShares.String()), CashDelta: payout,
		CostBasisDelta: domain.Decimal("-" + position.CostBasis.String()), RealizedPnLDelta: realizedDelta,
		SharesAfter: "0", CostBasisAfter: "0", AverageCostAfter: "0",
		RealizedPnLAfter: realizedAfter, MarkPrice: position.SettlementPrice,
		UnrealizedPnLAfter: "0", OccurredAt: appliedAt,
	}
	if err := insertPositionEvent(ctx, tx, event); err != nil {
		return err
	}
	_, err = insertOutbox(ctx, tx, "trading.position.redeemed.v1", event.EventID,
		position.ExecutionAccountID+":"+position.TokenID, event, appliedAt)
	return err
}
