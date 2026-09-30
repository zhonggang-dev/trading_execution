package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// ChainCashStore persists the on-chain pUSD transfer ledger and credits
// attributable external cash. It is the only writer of chain cash tables.
type ChainCashStore struct {
	db *sql.DB
}

// NewChainCashStore creates the chain cash ledger store.
func NewChainCashStore(db *sql.DB) (*ChainCashStore, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	return &ChainCashStore{db: db}, nil
}

// GetChainCashCursor returns the opt-in cursor of an account, if any.
func (store *ChainCashStore) GetChainCashCursor(ctx context.Context, executionAccountID string) (domain.ChainCashCursor, bool, error) {
	var cursor domain.ChainCashCursor
	var startBlock, processedBlock int64
	err := store.db.QueryRowContext(ctx, `
		SELECT execution_account_id, wallet_address, token_address, start_block,
		       start_balance::text, processed_block, updated_at
		FROM execution_chain_cash_cursors WHERE execution_account_id=$1`, executionAccountID).Scan(
		&cursor.ExecutionAccountID, &cursor.WalletAddress, &cursor.TokenAddress, &startBlock,
		&cursor.StartBalance, &processedBlock, &cursor.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ChainCashCursor{}, false, nil
	}
	if err != nil {
		return domain.ChainCashCursor{}, false, fmt.Errorf("read chain cash cursor: %w", err)
	}
	cursor.StartBlock, cursor.ProcessedBlock = uint64(startBlock), uint64(processedBlock)
	return cursor, true, nil
}

// ListRedemptionTransactionHashes returns which of the given hashes belong to
// this account's own redemptions, in any status. The payout can reach the
// wallet before the redemption row is CONFIRMED or APPLIED.
func (store *ChainCashStore) ListRedemptionTransactionHashes(
	ctx context.Context, executionAccountID string, transactionHashes []string,
) (map[string]struct{}, error) {
	result := make(map[string]struct{})
	if len(transactionHashes) == 0 {
		return result, nil
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT DISTINCT lower(transaction_hash) FROM polymarket_redemptions
		WHERE execution_account_id=$1 AND lower(transaction_hash) = ANY($2::text[])`,
		executionAccountID, transactionHashes)
	if err != nil {
		return nil, fmt.Errorf("read redemption transaction hashes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			return nil, err
		}
		result[hash] = struct{}{}
	}
	return result, rows.Err()
}

type chainCashCreditPayload struct {
	Transfer          domain.ChainCashTransfer `json:"transfer"`
	AccountEventID    string                   `json:"account_event_id"`
	TotalBalanceAfter domain.Decimal           `json:"total_balance_after"`
	AvailableAfter    domain.Decimal           `json:"available_balance_after"`
}

// ApplyChainCashRange persists (FromBlock, ToBlock], credits every uncredited
// REWARD/DEPOSIT row, advances the cursor, and reads the ledger total and the
// persisted offset, all in one transaction. The cursor must still be at
// FromBlock; a concurrent writer makes the whole call fail without effect.
func (store *ChainCashStore) ApplyChainCashRange(ctx context.Context, value domain.ChainCashRange) (domain.ChainCashRangeResult, error) {
	accountID := strings.TrimSpace(value.ExecutionAccountID)
	if accountID == "" || value.ToBlock < value.FromBlock || value.AppliedAt.IsZero() {
		return domain.ChainCashRangeResult{}, fmt.Errorf("chain cash range is invalid")
	}
	for _, transfer := range value.Transfers {
		if err := validateChainCashTransferForRange(accountID, value, transfer); err != nil {
			return domain.ChainCashRangeResult{}, err
		}
	}
	appliedAt := value.AppliedAt.UTC()
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return domain.ChainCashRangeResult{}, err
	}
	defer tx.Rollback()
	// Global lock order: the account row first, then its dependent rows.
	if err := lockExecutionAccount(ctx, tx, accountID); err != nil {
		return domain.ChainCashRangeResult{}, err
	}
	var processed int64
	if err := tx.QueryRowContext(ctx, `SELECT processed_block FROM execution_chain_cash_cursors
		WHERE execution_account_id=$1 FOR UPDATE`, accountID).Scan(&processed); err != nil {
		return domain.ChainCashRangeResult{}, fmt.Errorf("lock chain cash cursor: %w", err)
	}
	if uint64(processed) != value.FromBlock {
		return domain.ChainCashRangeResult{}, fmt.Errorf(
			"chain cash cursor moved concurrently: at %d, expected %d", processed, value.FromBlock)
	}
	result := domain.ChainCashRangeResult{ProcessedBlock: value.ToBlock}
	for _, transfer := range value.Transfers {
		if err := insertChainCashTransfer(ctx, tx, accountID, transfer); err != nil {
			return domain.ChainCashRangeResult{}, err
		}
	}
	// Re-attribute after inserting: a mint classified before autoredeem stored
	// its transaction hash (between the log read and this transaction) is
	// corrected here, before the offset below is read.
	if result.Reattributed, err = reattributeRedemptionTransfers(ctx, tx, accountID, appliedAt); err != nil {
		return domain.ChainCashRangeResult{}, err
	}
	if result.Credited, err = creditChainCashTransfers(ctx, tx, accountID, appliedAt); err != nil {
		return domain.ChainCashRangeResult{}, err
	}
	if value.ToBlock > value.FromBlock {
		if _, err := tx.ExecContext(ctx, `UPDATE execution_chain_cash_cursors
			SET processed_block=$2, updated_at=$3 WHERE execution_account_id=$1`,
			accountID, int64(value.ToBlock), appliedAt); err != nil {
			return domain.ChainCashRangeResult{}, fmt.Errorf("advance chain cash cursor: %w", err)
		}
	}
	if result.LedgerTotal, result.PersistedOffset, err = readChainCashPosition(ctx, tx, accountID); err != nil {
		return domain.ChainCashRangeResult{}, err
	}
	if result.Unreported, err = readUnreportedChainCashTransfers(ctx, tx, accountID); err != nil {
		return domain.ChainCashRangeResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.ChainCashRangeResult{}, fmt.Errorf("commit chain cash range: %w", err)
	}
	return result, nil
}

// readChainCashPosition reads, inside the caller's transaction, the ledger
// total after every credit and the persisted offset: uncredited REWARD/DEPOSIT
// plus UNATTRIBUTED_IN. UNATTRIBUTED_OUT is deliberately not an offset: the
// ledger was never debited, so the deficit must keep failing the balance
// comparison until an operator repairs the ledger. Both describe one instant.
func readChainCashPosition(ctx context.Context, tx *sql.Tx, accountID string) (domain.Decimal, domain.Decimal, error) {
	var ledgerTotal, offset domain.Decimal
	if err := tx.QueryRowContext(ctx, `SELECT total_balance::text FROM execution_accounts
		WHERE execution_account_id=$1`, accountID).Scan(&ledgerTotal); err != nil {
		return "", "", fmt.Errorf("read ledger total: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(CASE
		    WHEN classification IN ('REWARD','DEPOSIT') AND account_event_id IS NULL THEN amount
		    WHEN classification='UNATTRIBUTED_IN' THEN amount
		    ELSE 0 END),0)::text
		FROM execution_chain_cash_transfers WHERE execution_account_id=$1`, accountID).Scan(&offset); err != nil {
		return "", "", fmt.Errorf("read chain cash offset: %w", err)
	}
	return ledgerTotal, offset, nil
}

func validateChainCashTransferForRange(accountID string, value domain.ChainCashRange, transfer domain.ChainCashTransfer) error {
	if err := transfer.Validate(); err != nil {
		return err
	}
	if transfer.ExecutionAccountID != accountID || transfer.BlockNumber <= value.FromBlock || transfer.BlockNumber > value.ToBlock {
		return fmt.Errorf("chain cash transfer %s is outside account %s range (%d, %d]",
			transfer.Reference(), accountID, value.FromBlock, value.ToBlock)
	}
	switch transfer.Classification {
	case domain.ChainCashTrade:
	case domain.ChainCashRedemption, domain.ChainCashReward, domain.ChainCashDeposit, domain.ChainCashUnattributedIn:
		if transfer.Direction != domain.ChainCashIn {
			return fmt.Errorf("chain cash transfer %s classification %s requires IN", transfer.Reference(), transfer.Classification)
		}
	case domain.ChainCashUnattributedOut:
		if transfer.Direction != domain.ChainCashOut {
			return fmt.Errorf("chain cash transfer %s classification %s requires OUT", transfer.Reference(), transfer.Classification)
		}
	default:
		return fmt.Errorf("chain cash transfer %s has unknown classification %q", transfer.Reference(), transfer.Classification)
	}
	return nil
}

// reattributeRedemptionTransfers moves an uncredited UNATTRIBUTED_IN mint to
// REDEMPTION once this account recorded the same transaction as its own
// redemption, and closes the transfer's observed-only audit issue. Autoredeem
// learns the relayer transaction hash asynchronously (REDEEM_SUBMITTING may be
// recovered from Data API activity much later), so the payout mint can be
// final before the hash is known. Without this step the ledger would later
// book the redemption while the offset still counted the same cash as
// unattributed, turning a healthy account into a false manual drift.
func reattributeRedemptionTransfers(ctx context.Context, tx *sql.Tx, accountID string, at time.Time) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `
		WITH reattributed AS (
		  UPDATE execution_chain_cash_transfers transfer SET classification='REDEMPTION'
		  WHERE transfer.execution_account_id=$1 AND transfer.classification='UNATTRIBUTED_IN'
		    AND transfer.account_event_id IS NULL
		    AND transfer.counterparty='0x0000000000000000000000000000000000000000'
		    AND EXISTS (
		      SELECT 1 FROM polymarket_redemptions redemption
		      WHERE redemption.execution_account_id=transfer.execution_account_id
		        AND lower(redemption.transaction_hash)=transfer.transaction_hash)
		  RETURNING transfer.transaction_hash || ':' || transfer.log_index AS reference
		), resolved AS (
		  UPDATE reconciliation_issues issue
		  SET status='RESOLVED', resolution='AUTOMATIC', resolved_at=$2,
		      details=issue.details || '; re-attributed to this account''s own redemption transaction'
		  WHERE issue.execution_account_id=$1 AND issue.status='OPEN'
		    AND issue.issue_type='UNATTRIBUTED_CASH_IN'
		    AND issue.venue_trade_id IN (SELECT reference FROM reattributed)
		  RETURNING issue.issue_id
		)
		-- Both data-modifying CTEs always run to completion.
		SELECT count(*) FROM reattributed`, accountID, at).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("re-attribute redemption transfers: %w", err)
	}
	return count, nil
}

func insertChainCashTransfer(ctx context.Context, tx *sql.Tx, accountID string, transfer domain.ChainCashTransfer) error {
	result, err := tx.ExecContext(ctx, `
		INSERT INTO execution_chain_cash_transfers (
			execution_account_id, transaction_hash, log_index, block_number, block_hash,
			direction, counterparty, amount, classification, observed_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::numeric,$9,$10)
		ON CONFLICT (execution_account_id, transaction_hash, log_index) DO NOTHING`,
		accountID, transfer.TransactionHash, int64(transfer.LogIndex), int64(transfer.BlockNumber),
		transfer.BlockHash, string(transfer.Direction), transfer.Counterparty, transfer.Amount.String(),
		string(transfer.Classification), transfer.ObservedAt.UTC())
	if err != nil {
		return fmt.Errorf("insert chain cash transfer %s: %w", transfer.Reference(), err)
	}
	if oneRow(result) {
		return nil
	}
	// A re-read after a cursor reset must return the identical first
	// observation; anything else is a reorganization or a source defect.
	var same bool
	if err := tx.QueryRowContext(ctx, `
		SELECT block_number=$4 AND block_hash=$5 AND direction=$6 AND counterparty=$7 AND amount=$8::numeric
		FROM execution_chain_cash_transfers
		WHERE execution_account_id=$1 AND transaction_hash=$2 AND log_index=$3`,
		accountID, transfer.TransactionHash, int64(transfer.LogIndex), int64(transfer.BlockNumber),
		transfer.BlockHash, string(transfer.Direction), transfer.Counterparty, transfer.Amount.String()).Scan(&same); err != nil {
		return fmt.Errorf("verify existing chain cash transfer %s: %w", transfer.Reference(), err)
	}
	if !same {
		return fmt.Errorf("chain cash transfer %s conflicts with its first observation", transfer.Reference())
	}
	return nil
}

func creditChainCashTransfers(ctx context.Context, tx *sql.Tx, accountID string, at time.Time) ([]domain.ChainCashTransfer, error) {
	rows, err := tx.QueryContext(ctx, chainCashTransferSelect+`
		WHERE execution_account_id=$1 AND classification IN ('REWARD','DEPOSIT') AND account_event_id IS NULL
		ORDER BY block_number, log_index FOR UPDATE`, accountID)
	if err != nil {
		return nil, fmt.Errorf("read uncredited chain cash transfers: %w", err)
	}
	pending, err := scanChainCashTransfers(rows)
	if err != nil {
		return nil, err
	}
	credited := make([]domain.ChainCashTransfer, 0, len(pending))
	for _, transfer := range pending {
		eventID := domain.ChainCashAccountEventID(transfer)
		var total, available, reserved domain.Decimal
		if err := tx.QueryRowContext(ctx, `
			UPDATE execution_accounts
			SET total_balance=total_balance+$2::numeric, available_balance=available_balance+$2::numeric,
			    version=version+1, updated_at=$3
			WHERE execution_account_id=$1
			RETURNING total_balance::text, available_balance::text, reserved_balance::text`,
			accountID, transfer.Amount.String(), at).Scan(&total, &available, &reserved); err != nil {
			return nil, fmt.Errorf("credit chain cash %s: %w", transfer.Reference(), err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO execution_account_events (
				account_event_id, execution_account_id, event_type, total_balance_delta,
				available_balance_delta, reserved_balance_delta, total_balance_after,
				available_balance_after, reserved_balance_after, occurred_at
			) VALUES ($1,$2,$3,$4::numeric,$4::numeric,0,$5::numeric,$6::numeric,$7::numeric,$8)`,
			eventID, accountID, "CHAIN_CASH_"+string(transfer.Classification), transfer.Amount.String(),
			total.String(), available.String(), reserved.String(), at); err != nil {
			return nil, fmt.Errorf("insert chain cash account event %s: %w", eventID, err)
		}
		result, err := tx.ExecContext(ctx, `UPDATE execution_chain_cash_transfers SET account_event_id=$4
			WHERE execution_account_id=$1 AND transaction_hash=$2 AND log_index=$3 AND account_event_id IS NULL`,
			accountID, transfer.TransactionHash, int64(transfer.LogIndex), eventID)
		if err != nil {
			return nil, fmt.Errorf("link chain cash account event %s: %w", eventID, err)
		}
		if !oneRow(result) {
			return nil, fmt.Errorf("chain cash transfer %s was credited concurrently", transfer.Reference())
		}
		transfer.AccountEventID = eventID
		if _, err := insertOutbox(ctx, tx, "trading.account.chain_cash_credited.v1", eventID, accountID,
			chainCashCreditPayload{Transfer: transfer, AccountEventID: eventID, TotalBalanceAfter: total, AvailableAfter: available}, at); err != nil {
			return nil, fmt.Errorf("record chain cash credit outbox %s: %w", eventID, err)
		}
		credited = append(credited, transfer)
	}
	return credited, nil
}

func readUnreportedChainCashTransfers(ctx context.Context, tx *sql.Tx, accountID string) ([]domain.ChainCashTransfer, error) {
	rows, err := tx.QueryContext(ctx, chainCashTransferSelect+` transfer
		WHERE transfer.execution_account_id=$1
		  AND transfer.classification IN ('UNATTRIBUTED_IN','UNATTRIBUTED_OUT')
		  AND NOT EXISTS (
		    SELECT 1 FROM reconciliation_issues issue
		    WHERE issue.execution_account_id=transfer.execution_account_id
		      AND issue.issue_type IN ('UNATTRIBUTED_CASH_IN','UNATTRIBUTED_CASH_OUT')
		      AND issue.issue_type=CASE transfer.classification
		            WHEN 'UNATTRIBUTED_IN' THEN 'UNATTRIBUTED_CASH_IN' ELSE 'UNATTRIBUTED_CASH_OUT' END
		      AND issue.venue_trade_id=transfer.transaction_hash || ':' || transfer.log_index)
		ORDER BY transfer.block_number, transfer.log_index`, accountID)
	if err != nil {
		return nil, fmt.Errorf("read unreported chain cash transfers: %w", err)
	}
	return scanChainCashTransfers(rows)
}

const chainCashTransferSelect = `
	SELECT execution_account_id, transaction_hash, log_index, block_number, block_hash, direction,
	       counterparty, amount::text, classification, COALESCE(account_event_id,''), observed_at
	FROM execution_chain_cash_transfers`

func scanChainCashTransfers(rows *sql.Rows) ([]domain.ChainCashTransfer, error) {
	defer rows.Close()
	result := make([]domain.ChainCashTransfer, 0)
	for rows.Next() {
		var transfer domain.ChainCashTransfer
		var logIndex, blockNumber int64
		if err := rows.Scan(&transfer.ExecutionAccountID, &transfer.TransactionHash, &logIndex, &blockNumber,
			&transfer.BlockHash, &transfer.Direction, &transfer.Counterparty, &transfer.Amount,
			&transfer.Classification, &transfer.AccountEventID, &transfer.ObservedAt); err != nil {
			return nil, fmt.Errorf("scan chain cash transfer: %w", err)
		}
		transfer.LogIndex, transfer.BlockNumber = uint64(logIndex), uint64(blockNumber)
		result = append(result, transfer)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, rows.Close()
}
