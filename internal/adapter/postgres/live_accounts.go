package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// ExpectedExecutionAccount contains only public identity used to bind a
// configured signer to its PostgreSQL wallet ledger.
type ExpectedExecutionAccount struct {
	ExecutionAccountID string
	WalletAddress      string
}

// CheckLiveAccountBindings fails closed when a configured wallet is missing,
// attached to another account, or still denominated in the legacy collateral.
func CheckLiveAccountBindings(
	ctx context.Context,
	db *sql.DB,
	accounts []ExpectedExecutionAccount,
	collateralAsset string,
) error {
	if db == nil || len(accounts) == 0 || strings.TrimSpace(collateralAsset) == "" {
		return fmt.Errorf("postgres, execution accounts, and collateral asset are required")
	}
	for _, expected := range accounts {
		expected.ExecutionAccountID = strings.TrimSpace(expected.ExecutionAccountID)
		expected.WalletAddress = strings.ToLower(strings.TrimSpace(expected.WalletAddress))
		if expected.ExecutionAccountID == "" || expected.WalletAddress == "" {
			return fmt.Errorf("configured execution account identity is incomplete")
		}
		var walletAddress, asset string
		err := db.QueryRowContext(ctx, `
			SELECT LOWER(wallet_address), collateral_asset
			FROM execution_accounts
			WHERE execution_account_id=$1`, expected.ExecutionAccountID).Scan(&walletAddress, &asset)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("execution account %q has no initialized PostgreSQL ledger", expected.ExecutionAccountID)
		}
		if err != nil {
			return fmt.Errorf("read execution account %q binding: %w", expected.ExecutionAccountID, err)
		}
		if walletAddress != expected.WalletAddress {
			return fmt.Errorf("execution account %q wallet does not match the configured signer", expected.ExecutionAccountID)
		}
		if asset != collateralAsset {
			return fmt.Errorf("execution account %q collateral is %q, want %q", expected.ExecutionAccountID, asset, collateralAsset)
		}
	}
	return nil
}

// CheckLiveLedgerBootstrap rejects legacy operational state that cannot be
// safely resumed by the live V2 path. Historical terminal rows may remain for
// audit, but every active reservation and open lot must have the durable
// identity introduced by the lot-addressed and atomic-risk migrations.
func CheckLiveLedgerBootstrap(ctx context.Context, db *sql.DB, accounts []ExpectedExecutionAccount) error {
	if db == nil || len(accounts) == 0 {
		return fmt.Errorf("postgres and execution accounts are required")
	}
	for _, account := range accounts {
		accountID := strings.TrimSpace(account.ExecutionAccountID)
		if accountID == "" {
			return fmt.Errorf("configured execution account identity is incomplete")
		}
		var legacyReservations, invalidSells, incompleteLots, ledgerMismatches int
		var sellIdentityMismatches, accountReservationMismatches, positionReservationMismatches int
		err := db.QueryRowContext(ctx, `
			SELECT
				(SELECT count(*) FROM asset_reservations
				 WHERE execution_account_id=$1
				   AND status IN ('ACTIVE','RECONCILIATION_REQUIRED')
				   AND risk_policy_id=''),
				(SELECT count(*) FROM asset_reservations
				 WHERE execution_account_id=$1 AND side='SELL'
				   AND status IN ('ACTIVE','RECONCILIATION_REQUIRED')
				   AND (target_lot_id IS NULL OR target_lot_id='')),
				(SELECT count(*) FROM position_lots
				 WHERE execution_account_id=$1
				   AND status IN ('OPEN','SETTLED_PENDING_REDEEM')
				   AND (condition_id='' OR outcome_index IS NULL OR outcome_name='' OR neg_risk IS NULL)),
				(SELECT count(*)
				 FROM execution_positions position
				 FULL OUTER JOIN (
					SELECT execution_account_id, token_id,
					       COALESCE(sum(remaining_shares),0) AS shares,
					       COALESCE(sum(remaining_cost),0) AS cost
					FROM position_lots
					WHERE execution_account_id=$1
					  AND status IN ('OPEN','SETTLED_PENDING_REDEEM')
					GROUP BY execution_account_id, token_id
				 ) lot ON lot.execution_account_id=position.execution_account_id
				      AND lot.token_id=position.token_id
				 WHERE COALESCE(position.execution_account_id,lot.execution_account_id)=$1
				   AND (COALESCE(position.total_shares,0)<>COALESCE(lot.shares,0)
				     OR COALESCE(position.cost_basis,0)<>COALESCE(lot.cost,0))),
				(SELECT count(*)
				 FROM asset_reservations reservation
				 LEFT JOIN position_lots lot ON lot.lot_id=reservation.target_lot_id
				 LEFT JOIN position_lot_model_routes route ON route.lot_id=lot.lot_id
				 LEFT JOIN execution_orders order_row ON order_row.order_id=reservation.order_id
				 WHERE reservation.execution_account_id=$1 AND reservation.side='SELL'
				   AND reservation.status IN ('ACTIVE','RECONCILIATION_REQUIRED')
				   AND (lot.lot_id IS NULL OR lot.status<>'OPEN'
				     OR lot.execution_account_id<>reservation.execution_account_id
				     OR lot.market_id<>reservation.market_id OR lot.token_id<>reservation.token_id
				     OR COALESCE(route.logical_model_id,lot.model_id)
				        <>COALESCE(order_row.intent->>'model_id','')
				     OR execution_canonical_strategy_id(lot.strategy_id)
				        <>execution_canonical_strategy_id(reservation.strategy_id)
				     OR reservation.remaining_reserved_shares>lot.remaining_shares)),
				(SELECT count(*)
				 FROM execution_accounts account_row
				 LEFT JOIN (
					SELECT execution_account_id, COALESCE(sum(remaining_reserved_balance),0) AS reserved
					FROM asset_reservations
					WHERE execution_account_id=$1 AND side='BUY'
					  AND status IN ('ACTIVE','RECONCILIATION_REQUIRED')
					GROUP BY execution_account_id
				 ) reservation ON reservation.execution_account_id=account_row.execution_account_id
				 WHERE account_row.execution_account_id=$1
				   AND account_row.reserved_balance<>COALESCE(reservation.reserved,0)),
				(SELECT count(*)
				 FROM execution_positions position
				 FULL OUTER JOIN (
					SELECT execution_account_id, token_id,
					       COALESCE(sum(remaining_reserved_shares),0) AS reserved
					FROM asset_reservations
					WHERE execution_account_id=$1 AND side='SELL'
					  AND status IN ('ACTIVE','RECONCILIATION_REQUIRED')
					GROUP BY execution_account_id, token_id
				 ) reservation ON reservation.execution_account_id=position.execution_account_id
				      AND reservation.token_id=position.token_id
				 WHERE COALESCE(position.execution_account_id,reservation.execution_account_id)=$1
				   AND COALESCE(position.reserved_shares,0)<>COALESCE(reservation.reserved,0))`, accountID).
			Scan(
				&legacyReservations, &invalidSells, &incompleteLots, &ledgerMismatches,
				&sellIdentityMismatches, &accountReservationMismatches, &positionReservationMismatches,
			)
		if err != nil {
			return fmt.Errorf("inspect execution account %q live ledger bootstrap: %w", accountID, err)
		}
		if legacyReservations != 0 || invalidSells != 0 || incompleteLots != 0 || ledgerMismatches != 0 ||
			sellIdentityMismatches != 0 || accountReservationMismatches != 0 || positionReservationMismatches != 0 {
			return fmt.Errorf(
				"execution account %q live ledger is not migration-safe (legacy reservations=%d, invalid active sells=%d, incomplete open lots=%d, position/lot mismatches=%d, sell identity mismatches=%d, account reservation mismatches=%d, position reservation mismatches=%d)",
				accountID, legacyReservations, invalidSells, incompleteLots, ledgerMismatches,
				sellIdentityMismatches, accountReservationMismatches, positionReservationMismatches,
			)
		}
	}
	return nil
}
