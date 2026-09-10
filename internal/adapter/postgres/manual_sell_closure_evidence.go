package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/manualsellclosure"
)

// CheckManualSellClosureEvidence is read-only. Equal balances alone do not
// authorize releasing a protected reservation: exact immutable rows must exist.
func CheckManualSellClosureEvidence(ctx context.Context, db *sql.DB, target manualsellclosure.Target, order domain.Order) error {
	if order.ID != target.Order || order.Intent.ExecutionAccountID != target.Account {
		return fmt.Errorf("closure evidence scope mismatch")
	}
	var count int
	var key, wallet string
	if err := db.QueryRowContext(ctx, `SELECT count(*),COALESCE(min(fill_key),'') FROM execution_fills WHERE order_id=$1`, order.ID).Scan(&count, &key); err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("local fill set is not the one reviewed applied trade")
	}
	ledger, err := NewFillLedger(FillLedgerParams{DB: db})
	if err != nil {
		return err
	}
	fill, err := ledger.GetFill(ctx, key)
	if err != nil {
		return err
	}
	if fill.AppliedAt == nil || fill.ConfirmedAt == nil || !fill.NetCashDelta.Equal(target.Gross) {
		return fmt.Errorf("reviewed fill is not durably applied")
	}
	if err = manualsellclosure.ValidateFill(target, fill); err != nil {
		return err
	}
	if err = db.QueryRowContext(ctx, `SELECT wallet_address FROM execution_accounts WHERE execution_account_id=$1`, target.Account).Scan(&wallet); err != nil {
		return err
	}
	if !strings.EqualFold(wallet, target.Wallet) {
		return fmt.Errorf("wallet owner changed")
	}
	var cashEvents, matchingEvents int
	if err = db.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER (WHERE event_type='FILL_SETTLED' AND execution_account_id=$2 AND total_balance_delta=$3::numeric) FROM execution_account_events WHERE order_id=$1`, order.ID, target.Account, target.Gross.String()).Scan(&cashEvents, &matchingEvents); err != nil {
		return err
	}
	if cashEvents != 1 || matchingEvents != 1 {
		return fmt.Errorf("cash event does not exactly explain the reviewed fill")
	}
	return nil
}
