package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestRedemptionStoreAppliesConfirmedBinaryPayoutAtomically(t *testing.T) {
	databaseURL := os.Getenv("REDEMPTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("REDEMPTION_TEST_DATABASE_URL is not configured")
	}
	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	suffix := fmt.Sprintf("%x", time.Now().UnixNano())
	walletHash := sha256.Sum256([]byte(suffix))
	accountID := "redeem-integration-" + suffix
	wallet := fmt.Sprintf("0x%x", walletHash[:20])
	condition := "0x" + strings.Repeat("44", 32)
	tokenID := fmt.Sprintf("%d", time.Now().UnixNano())
	lotID := "redeem-integration-lot-" + suffix
	orderID := "redeem-integration-order-" + suffix
	fillKey := "redeem-integration-fill-" + suffix
	now := time.Unix(1_800_000_000, 0).UTC()
	if _, err := database.ExecContext(ctx, `
		INSERT INTO execution_accounts (
			execution_account_id, wallet_address, collateral_asset,
			total_balance, available_balance, reserved_balance
		) VALUES ($1,$2,'pUSD',100,100,0)`, accountID, wallet); err != nil {
		t.Fatal(err)
	}
	// Audit and outbox tables are deliberately append-only, so this integration
	// test uses unique identities and must run against a disposable database.
	if _, err := database.ExecContext(ctx, `
		INSERT INTO execution_orders (
			order_id, client_order_id, execution_account_id, venue, market_id, token_id,
			intent, venue_order_id, status, filled_size, average_fill_price,
			filled_notional, total_fees, revision, created_at, updated_at
		) VALUES ($1,$2,$3,'POLYMARKET','market-1',$4,'{}'::jsonb,$5,'FILLED',48,0.21,
		          10.08,0,1,$6,$6)`, orderID, orderID, accountID, tokenID, orderID, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `
		INSERT INTO execution_fills (
			fill_key, venue, venue_fill_id, order_id, venue_order_id,
			execution_account_id, market_id, condition_id, token_id, side,
			liquidity_role, status, shares, price, gross_notional, fee_rate_bps,
			platform_fee, builder_fee_rate_bps, builder_fee, total_fee,
			net_cash_delta, fee_source, matched_at, first_observed_at, last_observed_at
		) VALUES ($1,'POLYMARKET',$1,$2,$2,$3,'market-1',$4,$5,'BUY',
		          'TAKER','MATCHED',48,0.21,10.08,0,0,0,0,0,-10.08,
		          'INTEGRATION_TEST',$6,$6,$6)`, fillKey, orderID, accountID, condition, tokenID, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `
		INSERT INTO execution_positions (
			execution_account_id, market_id, condition_id, token_id,
			outcome_index, outcome_name, total_shares, available_shares,
			reserved_shares, cost_basis, average_cost_price, realized_pnl,
			mark_price, market_value, unrealized_pnl, lifecycle_status,
			settlement_price, settlement_source, settled_at
		) VALUES ($1,'market-1',$2,$3,1,'No',48,48,0,10.08,0.21,0,1,48,37.92,
		          'SETTLED_PENDING_REDEEM',1,'data-api:test',$4)`, accountID, condition, tokenID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `
		INSERT INTO position_lots (
			lot_id, execution_account_id, market_id, token_id, model_id, strategy_id,
			opening_order_id, opening_fill_key, original_shares, remaining_shares,
			original_cost, remaining_cost, average_entry_price, status, opened_at,
			condition_id, outcome_index, outcome_name, neg_risk
		) VALUES ($1,$2,'market-1',$3,'model','strategy',$4,$5,48,48,10.08,10.08,
		          0.21,'SETTLED_PENDING_REDEEM',$6,$7,1,'No',TRUE)`,
		lotID, accountID, tokenID, orderID, fillKey, now.Add(-time.Hour), condition); err != nil {
		t.Fatal(err)
	}
	store, err := NewRedemptionStore(database, []string{accountID})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SyncPendingRedemptions(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := store.ListDueRedemptions(ctx, 1000, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	redemption := requireTestRedemption(t, rows, accountID, condition, domain.RedemptionReady)
	if err := store.BeginRedemptionSubmission(ctx, redemption, domain.RedemptionSubmissionRedeem, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	redemption.Status = domain.RedemptionRedeemSubmitting
	txHash := sha256.Sum256([]byte("redemption-transaction-" + suffix))
	hash := fmt.Sprintf("0x%x", txHash[:])
	if err := store.RecordRedemptionSubmission(ctx, redemption, domain.RedemptionSubmission{
		Provider: "POLYMARKET_RELAYER", Reference: "integration-1",
		TransactionHash: hash, State: domain.RedemptionSubmissionPending,
	}, now.Add(2*time.Minute), now.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	rows, err = store.ListDueRedemptions(ctx, 1000, now.Add(4*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	redemption = requireTestRedemption(t, rows, accountID, condition, domain.RedemptionRedeemSubmitted)
	receipt := domain.RedemptionReceipt{
		TransactionHash: hash, WalletAddress: wallet, ConditionID: condition,
		EventType: "POSITIONS_REDEEMED", PayoutBaseUnits: "48000000",
		BlockNumber: 100, BlockHash: "0x" + strings.Repeat("22", 32), Confirmations: 64,
	}
	if err := store.RecordRedemptionConfirmed(ctx, redemption, receipt, now.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	rows, err = store.ListDueRedemptions(ctx, 1000, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	redemption = requireTestRedemption(t, rows, accountID, condition, domain.RedemptionConfirmed)
	if err := store.ApplyRedemption(ctx, redemption, now.Add(5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	var total, available, realized, shares, cost domain.Decimal
	var lifecycle, lotStatus, redemptionStatus string
	if err := database.QueryRowContext(ctx, `
		SELECT account.total_balance::text, account.available_balance::text,
		       position.realized_pnl::text, position.total_shares::text,
			       position.cost_basis::text, position.lifecycle_status, lot.status, redemption.status
			FROM execution_accounts account
			JOIN execution_positions position
			  ON position.execution_account_id=account.execution_account_id
			JOIN position_lots lot
			  ON lot.execution_account_id=position.execution_account_id
			 AND lot.token_id=position.token_id
			JOIN polymarket_redemptions redemption
			  ON redemption.execution_account_id=position.execution_account_id
			 AND redemption.condition_id=position.condition_id
		WHERE account.execution_account_id=$1`, accountID).Scan(
		&total, &available, &realized, &shares, &cost, &lifecycle, &lotStatus, &redemptionStatus,
	); err != nil {
		t.Fatal(err)
	}
	if !total.Equal("148") || !available.Equal("148") || !realized.Equal("37.92") || !shares.Equal("0") || !cost.Equal("0") ||
		lifecycle != "CLOSED" || lotStatus != "CLOSED" || redemptionStatus != "APPLIED" {
		t.Fatalf("applied account/position/lot/redemption = %s/%s/%s/%s/%s/%s/%s/%s",
			total, available, realized, shares, cost, lifecycle, lotStatus, redemptionStatus)
	}
}

func requireTestRedemption(
	t *testing.T,
	rows []domain.Redemption,
	accountID, conditionID string,
	status domain.RedemptionStatus,
) domain.Redemption {
	t.Helper()
	for _, row := range rows {
		if row.ExecutionAccountID == accountID && row.ConditionID == conditionID {
			if row.Status != status {
				t.Fatalf("redemption status = %s, want %s", row.Status, status)
			}
			return row
		}
	}
	t.Fatalf("redemption %s/%s was not due; rows=%#v", accountID, conditionID, rows)
	return domain.Redemption{}
}
