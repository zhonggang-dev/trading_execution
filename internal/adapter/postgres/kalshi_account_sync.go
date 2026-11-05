package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// SyncKalshiExecutionAccount creates the Kalshi execution account or replaces
// its local cash with the exchange balance at startup. It returns deferred=true
// and leaves the ledger untouched while the exchange balance may already
// contain money the local ledger has not booked yet; overwriting the cash then
// would book the same money a second time when the ledger catches up:
//   - an owned order still has protected assets (a venue fill may be pending);
//   - the account holds a market the exchange has settled but the ledger has
//     not applied (no settlement row, or one held for review): the exchange has
//     already paid it, and the settlement sweep would credit it again.
//
// listSettlements reads all of the account's exchange settlement records; it
// is called at most once, and only when a held market has no local row.
func SyncKalshiExecutionAccount(
	ctx context.Context,
	database *sql.DB,
	accountID, apiKeyID string,
	readBalance func() (domain.Decimal, error),
	listSettlements func() ([]domain.KalshiSettlement, error),
) (deferred bool, balance domain.Decimal, err error) {
	if database == nil || readBalance == nil || listSettlements == nil {
		return false, "", fmt.Errorf("Kalshi account balance source is unavailable")
	}
	tx, err := database.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, "", fmt.Errorf("begin Kalshi account sync: %w", err)
	}
	defer tx.Rollback()
	// Serialize account initialization and hold the same execution_accounts row
	// lock used by Reserve and FillLedger before taking the external balance
	// snapshot. Otherwise a concurrent fill could settle between GET /balance
	// and the local UPDATE, letting an older snapshot overwrite the newer ledger.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "kalshi-account-sync:"+accountID); err != nil {
		return false, "", fmt.Errorf("lock Kalshi account sync: %w", err)
	}

	walletAddress := ""
	collateralAsset := ""
	reservedBalance := "0"
	existing := true
	if err := tx.QueryRowContext(ctx, `
		SELECT wallet_address, collateral_asset, reserved_balance::text
		FROM execution_accounts WHERE execution_account_id=$1 FOR UPDATE`, accountID).
		Scan(&walletAddress, &collateralAsset, &reservedBalance); errors.Is(err, sql.ErrNoRows) {
		existing = false
	} else if err != nil {
		return false, "", fmt.Errorf("lock existing Kalshi execution account: %w", err)
	}
	if existing && (!strings.EqualFold(strings.TrimSpace(walletAddress), "kalshi:"+strings.TrimSpace(apiKeyID)) ||
		!strings.EqualFold(strings.TrimSpace(collateralAsset), "USD")) {
		return false, "", fmt.Errorf("Kalshi account identity changed")
	}
	var unresolved bool
	if existing {
		if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM asset_reservations
			WHERE execution_account_id=$1
			  AND status IN ('ACTIVE','RECONCILIATION_REQUIRED')
		)`, accountID).Scan(&unresolved); err != nil {
			return false, "", fmt.Errorf("inspect unresolved Kalshi reservations: %w", err)
		}
	}
	reservedSign, reservedErr := domain.Decimal(reservedBalance).Sign()
	if reservedErr != nil {
		return false, "", fmt.Errorf("local Kalshi reserved balance is invalid")
	}
	balance, err = readBalance()
	if err != nil {
		return false, "", err
	}
	if sign, signErr := balance.Sign(); signErr != nil || sign < 0 {
		return false, "", fmt.Errorf("Kalshi cash balance is invalid")
	}
	if existing && !unresolved && reservedSign <= 0 {
		// Checked after the balance read: a settlement the balance already
		// reflects is then visible here too.
		pending, pendingErr := kalshiUnappliedSettlement(ctx, tx, accountID, listSettlements)
		if pendingErr != nil {
			return false, "", pendingErr
		}
		unresolved = pending
	}
	if !existing {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO execution_accounts (
				execution_account_id,wallet_address,collateral_asset,
				total_balance,available_balance,reserved_balance,reconciled_at
			) VALUES ($1,$2,'USD',$3::numeric,$3::numeric,0,clock_timestamp())`,
			accountID, "kalshi:"+apiKeyID, balance.String()); err != nil {
			return false, "", fmt.Errorf("insert Kalshi execution account: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return false, "", fmt.Errorf("commit new Kalshi execution account: %w", err)
		}
		return false, balance, nil
	}
	if unresolved || reservedSign > 0 {
		if err := tx.Commit(); err != nil {
			return false, "", fmt.Errorf("commit deferred Kalshi account sync: %w", err)
		}
		return true, balance, nil
	}
	if _, _, err := bookKalshiBalanceSync(ctx, tx, accountID, balance); err != nil {
		return false, "", err
	}
	if err := tx.Commit(); err != nil {
		return false, "", fmt.Errorf("commit Kalshi execution account sync: %w", err)
	}
	return false, balance, nil
}

// bookKalshiBalanceSync replaces the local cash with the exchange balance and
// writes whatever the two disagreed by as one KALSHI_BALANCE_SYNC account event
// before it disappears. The startup sync and the running sync share it, so both
// book with the same SQL. The caller holds the execution_accounts row lock and
// has checked that no cash is reserved. eventID is empty when nothing differed;
// the account row is updated (reconciled_at, version) either way.
func bookKalshiBalanceSync(
	ctx context.Context, tx *sql.Tx, accountID string, balance domain.Decimal,
) (eventID string, delta domain.Decimal, err error) {
	rows, err := tx.QueryContext(ctx, `
		INSERT INTO execution_account_events (
			account_event_id, execution_account_id, event_type, total_balance_delta,
			available_balance_delta, reserved_balance_delta, total_balance_after,
			available_balance_after, reserved_balance_after, occurred_at
		)
		SELECT 'kalshi-balance-sync:'||execution_account_id||':'||to_char(clock_timestamp(),'YYYYMMDD"T"HH24MISSUS'),
		       execution_account_id, 'KALSHI_BALANCE_SYNC', $2::numeric - total_balance,
		       $2::numeric - available_balance, 0, $2::numeric, $2::numeric, reserved_balance, clock_timestamp()
		FROM execution_accounts
		WHERE execution_account_id=$1 AND (total_balance <> $2::numeric OR available_balance <> $2::numeric)
		RETURNING account_event_id, total_balance_delta::text`,
		accountID, balance.String())
	if err != nil {
		return "", "", fmt.Errorf("record Kalshi balance sync: %w", err)
	}
	for rows.Next() {
		if err := rows.Scan(&eventID, &delta); err != nil {
			_ = rows.Close()
			return "", "", fmt.Errorf("record Kalshi balance sync: %w", err)
		}
	}
	if err := rows.Close(); err != nil {
		return "", "", fmt.Errorf("record Kalshi balance sync: %w", err)
	}
	if err := rows.Err(); err != nil {
		return "", "", fmt.Errorf("record Kalshi balance sync: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE execution_accounts
		SET total_balance=$2::numeric, available_balance=$2::numeric,
			reconciled_at=clock_timestamp(), updated_at=clock_timestamp(), version=version+1
		WHERE execution_account_id=$1`, accountID, balance.String()); err != nil {
		return "", "", fmt.Errorf("sync settled Kalshi execution account: %w", err)
	}
	return eventID, delta, nil
}

// kalshiUnappliedSettlement reports whether the account holds open shares in a
// market whose settlement the exchange has paid but the ledger has not applied.
func kalshiUnappliedSettlement(
	ctx context.Context, tx *sql.Tx, accountID string,
	listSettlements func() ([]domain.KalshiSettlement, error),
) (bool, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT SUBSTRING(position.token_id FROM '^kalshi:(.+):(?:YES|NO)$') AS ticker,
		       COALESCE(settlement.status, '')
		FROM execution_positions position
		LEFT JOIN kalshi_settlements settlement
		  ON settlement.execution_account_id=position.execution_account_id
		 AND position.token_id IN ('kalshi:'||settlement.ticker||':YES','kalshi:'||settlement.ticker||':NO')
		WHERE position.execution_account_id=$1
		  AND position.lifecycle_status='OPEN' AND position.total_shares > 0
		  AND position.token_id ~ '^kalshi:.+:(YES|NO)$'
		ORDER BY 1`, accountID)
	if err != nil {
		return false, fmt.Errorf("list open Kalshi markets for balance sync: %w", err)
	}
	unsettled := make([]string, 0)
	pending := false
	for rows.Next() {
		var ticker, status string
		if err := rows.Scan(&ticker, &status); err != nil {
			_ = rows.Close()
			return false, err
		}
		switch status {
		case "":
			unsettled = append(unsettled, ticker)
		case "APPLIED":
		default:
			pending = true
		}
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if pending {
		return true, nil
	}
	if len(unsettled) == 0 {
		return false, nil
	}
	records, err := listSettlements()
	if err != nil {
		return false, fmt.Errorf("read Kalshi settlements before balance sync: %w", err)
	}
	settled := make(map[string]struct{}, len(records))
	for _, record := range records {
		settled[strings.TrimSpace(record.Ticker)] = struct{}{}
	}
	for _, ticker := range unsettled {
		if _, ok := settled[ticker]; ok {
			return true, nil
		}
	}
	return false, nil
}
