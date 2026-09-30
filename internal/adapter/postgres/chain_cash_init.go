package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// ChainCashCursorInitialization is the audited one-time opt-in of an account.
// Transfers are the classified, final transfers in (StartBlock, CatchUpBlock];
// they are persisted and credited in the same transaction that inserts the
// cursor at processed_block=CatchUpBlock. Execute=false performs every check
// and write inside the transaction and then rolls it back.
type ChainCashCursorInitialization struct {
	ExecutionAccountID string
	WalletAddress      string
	TokenAddress       string
	StartBlock         uint64
	StartBlockTime     time.Time
	StartBalance       domain.Decimal
	CatchUpBlock       uint64
	Transfers          []domain.ChainCashTransfer
	InitializedBy      string
	Reason             string
	Evidence           map[string]any
	AppliedAt          time.Time
	Execute            bool
}

// ChainCashCursorInitializationReport is the local evidence checked while the
// account row was locked, plus the effect of the offline catch-up.
type ChainCashCursorInitializationReport struct {
	ExecutionAccountID   string                     `json:"execution_account_id"`
	LedgerTotal          domain.Decimal             `json:"ledger_total"`
	LedgerAvailable      domain.Decimal             `json:"ledger_available"`
	LedgerReserved       domain.Decimal             `json:"ledger_reserved"`
	LedgerEventsAfter    int                        `json:"ledger_events_after_start_block"`
	PendingFills         int                        `json:"pending_fills"`
	UncertainOrders      int                        `json:"uncertain_orders"`
	InFlightRedemptions  int                        `json:"in_flight_redemptions"`
	ExistingCursor       bool                       `json:"existing_cursor"`
	Executed             bool                       `json:"executed"`
	RejectionReasons     []string                   `json:"rejection_reasons,omitempty"`
	StartBalanceOnChain  domain.Decimal             `json:"start_balance_on_chain"`
	StartBlock           uint64                     `json:"start_block"`
	StartBlockTimeUTC    time.Time                  `json:"start_block_time"`
	LedgerMatchesOnChain bool                       `json:"ledger_matches_on_chain"`
	CatchUpBlock         uint64                     `json:"catch_up_block"`
	TransfersPersisted   int                        `json:"transfers_persisted"`
	Credited             []domain.ChainCashTransfer `json:"credited,omitempty"`
	LedgerTotalAfter     domain.Decimal             `json:"ledger_total_after_catch_up,omitempty"`
	PersistedOffsetAfter domain.Decimal             `json:"persisted_offset_after_catch_up,omitempty"`
}

// InitializeChainCashCursor validates, under the account row lock, that the
// ledger has not changed since the start block and equals balanceOf at that
// block. It then inserts the cursor already caught up to CatchUpBlock,
// persists every supplied transfer, and credits REWARD/DEPOSIT, all in one
// transaction. Any failed check rejects the initialization with every reason.
//
// Doing the catch-up in the cursor's own transaction means the running service
// never sees a cursor with a backlog: until commit the account has no cursor
// and keeps the legacy comparison; after commit only the increment beyond
// CatchUpBlock remains. The account row lock is taken first, as everywhere
// else, so fills and redemptions booked concurrently are serialized, and the
// events-after-start check below rejects the run if one slipped in after the
// transfers were read.
//
// Resting orders are allowed: a fill after the start block appears as a TRADE
// transfer and is booked later by the fill ledger. What would break the
// baseline is cash already booked after the start block (account events),
// cash moved but not yet booked (pending fills, in-flight redemptions, orders
// whose venue outcome is uncertain), or a ledger that does not equal balanceOf
// at the start block.
func (store *ChainCashStore) InitializeChainCashCursor(
	ctx context.Context, params ChainCashCursorInitialization,
) (ChainCashCursorInitializationReport, error) {
	report := ChainCashCursorInitializationReport{
		ExecutionAccountID: strings.TrimSpace(params.ExecutionAccountID), StartBalanceOnChain: params.StartBalance,
		StartBlock: params.StartBlock, StartBlockTimeUTC: params.StartBlockTime.UTC(), CatchUpBlock: params.CatchUpBlock,
	}
	wallet := strings.ToLower(strings.TrimSpace(params.WalletAddress))
	token := strings.ToLower(strings.TrimSpace(params.TokenAddress))
	if report.ExecutionAccountID == "" || params.StartBlock == 0 || params.StartBlockTime.IsZero() ||
		params.AppliedAt.IsZero() || strings.TrimSpace(params.InitializedBy) == "" || strings.TrimSpace(params.Reason) == "" {
		return report, fmt.Errorf("account, start block, block time, applied time, actor, and reason are required")
	}
	if params.CatchUpBlock < params.StartBlock {
		return report, fmt.Errorf("catch-up block %d is before start block %d", params.CatchUpBlock, params.StartBlock)
	}
	if sign, err := params.StartBalance.Sign(); err != nil || sign < 0 {
		return report, fmt.Errorf("start balance must be a non-negative decimal")
	}
	evidence, err := json.Marshal(params.Evidence)
	if err != nil || len(params.Evidence) == 0 {
		return report, fmt.Errorf("initialization evidence must be a non-empty object")
	}
	catchUp := domain.ChainCashRange{
		ExecutionAccountID: report.ExecutionAccountID, FromBlock: params.StartBlock, ToBlock: params.CatchUpBlock,
	}
	for _, transfer := range params.Transfers {
		if err := validateChainCashTransferForRange(report.ExecutionAccountID, catchUp, transfer); err != nil {
			return report, err
		}
	}
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return report, err
	}
	defer tx.Rollback()
	if err := lockExecutionAccount(ctx, tx, report.ExecutionAccountID); err != nil {
		return report, err
	}
	if err := store.inspectInitialization(ctx, tx, params, wallet, &report); err != nil {
		return report, err
	}
	if len(report.RejectionReasons) > 0 {
		return report, fmt.Errorf("chain cash initialization rejected: %s", strings.Join(report.RejectionReasons, "; "))
	}
	appliedAt := params.AppliedAt.UTC()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO execution_chain_cash_cursors (
			execution_account_id, wallet_address, token_address, start_block, start_balance,
			processed_block, initialized_by, initialized_reason, evidence, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5::numeric,$6,$7,$8,$9::jsonb,$10,$10)`,
		report.ExecutionAccountID, wallet, token, int64(params.StartBlock), params.StartBalance.String(),
		int64(params.CatchUpBlock), strings.TrimSpace(params.InitializedBy), strings.TrimSpace(params.Reason),
		evidence, appliedAt); err != nil {
		return report, fmt.Errorf("insert chain cash cursor: %w", err)
	}
	for _, transfer := range params.Transfers {
		if err := insertChainCashTransfer(ctx, tx, report.ExecutionAccountID, transfer); err != nil {
			return report, err
		}
	}
	report.TransfersPersisted = len(params.Transfers)
	if report.Credited, err = creditChainCashTransfers(ctx, tx, report.ExecutionAccountID, appliedAt); err != nil {
		return report, err
	}
	if report.LedgerTotalAfter, report.PersistedOffsetAfter, err = readChainCashPosition(ctx, tx, report.ExecutionAccountID); err != nil {
		return report, err
	}
	if !params.Execute {
		// Dry run: fire the deferred credit trigger and deferred foreign keys
		// now, so the dry run proves everything a commit would check; the
		// deferred Rollback then discards every write.
		if _, err := tx.ExecContext(ctx, `SET CONSTRAINTS ALL IMMEDIATE`); err != nil {
			return report, fmt.Errorf("validate deferred chain cash constraints: %w", err)
		}
		return report, nil
	}
	if err := tx.Commit(); err != nil {
		return report, fmt.Errorf("commit chain cash cursor: %w", err)
	}
	report.Executed = true
	return report, nil
}

func (store *ChainCashStore) inspectInitialization(
	ctx context.Context, tx *sql.Tx, params ChainCashCursorInitialization, wallet string,
	report *ChainCashCursorInitializationReport,
) error {
	var accountWallet string
	if err := tx.QueryRowContext(ctx, `
		SELECT lower(wallet_address), total_balance::text, available_balance::text, reserved_balance::text,
		       total_balance=$2::numeric
		FROM execution_accounts WHERE execution_account_id=$1`, report.ExecutionAccountID, params.StartBalance.String()).Scan(
		&accountWallet, &report.LedgerTotal, &report.LedgerAvailable, &report.LedgerReserved, &report.LedgerMatchesOnChain); err != nil {
		return fmt.Errorf("read execution account: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `
		SELECT
		  (SELECT count(*) FROM execution_account_events WHERE execution_account_id=$1 AND occurred_at>=$2
		     AND total_balance_delta<>0),
		  (SELECT count(*) FROM execution_fills WHERE execution_account_id=$1 AND applied_at IS NULL AND status<>'FAILED'),
		  (SELECT count(*) FROM execution_orders WHERE execution_account_id=$1
		     AND status IN ('SUBMITTING','UNKNOWN','CANCEL_PENDING','RECONCILING','MANUAL_REVIEW')),
		  (SELECT count(*) FROM polymarket_redemptions WHERE execution_account_id=$1
		     AND status IN ('REDEEM_SUBMITTING','REDEEM_SUBMITTED','CONFIRMED')),
		  EXISTS (SELECT 1 FROM execution_chain_cash_cursors WHERE execution_account_id=$1)`,
		report.ExecutionAccountID, params.StartBlockTime.UTC()).Scan(
		&report.LedgerEventsAfter, &report.PendingFills, &report.UncertainOrders,
		&report.InFlightRedemptions, &report.ExistingCursor); err != nil {
		return fmt.Errorf("inspect chain cash initialization evidence: %w", err)
	}
	reasons := make([]string, 0)
	if accountWallet != wallet {
		reasons = append(reasons, fmt.Sprintf("account wallet %s does not match %s", accountWallet, wallet))
	}
	if !report.LedgerMatchesOnChain {
		reasons = append(reasons, fmt.Sprintf("ledger total %s differs from balanceOf %s at block %d",
			report.LedgerTotal, params.StartBalance, params.StartBlock))
	}
	if report.LedgerEventsAfter > 0 {
		reasons = append(reasons, "the ledger total changed at or after the start block time")
	}
	if report.PendingFills > 0 {
		reasons = append(reasons, "fills are waiting for finality or application")
	}
	if report.UncertainOrders > 0 {
		reasons = append(reasons, "orders with an uncertain venue outcome exist")
	}
	if report.InFlightRedemptions > 0 {
		reasons = append(reasons, "redemptions are in flight")
	}
	if report.ExistingCursor {
		reasons = append(reasons, "the account already has a chain cash cursor")
	}
	if len(reasons) > 0 {
		report.RejectionReasons = reasons
	}
	return nil
}
