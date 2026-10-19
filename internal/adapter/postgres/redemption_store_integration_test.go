package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestRedemptionStoreAppliesConfirmedBinaryPayoutAtomicallyPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("REDEMPTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("REDEMPTION_TEST_DATABASE_URL or TRADING_EXECUTION_TEST_DATABASE_URL is not configured")
	}
	database := newIntegrationDatabase(t, databaseURL)
	ctx := context.Background()
	suffix := fmt.Sprintf("%x", time.Now().UnixNano())
	walletHash := sha256.Sum256([]byte(suffix))
	accountID := "redeem-integration-" + suffix
	wallet := fmt.Sprintf("0x%x", walletHash[:20])
	condition := "0x" + strings.Repeat("44", 32)
	tokenID := fmt.Sprintf("%d", time.Now().UnixNano())
	losingTokenID := tokenID + "1"
	lotID := "redeem-integration-lot-" + suffix
	losingLotID := "redeem-integration-losing-lot-" + suffix
	orderID := "redeem-integration-order-" + suffix
	losingOrderID := "redeem-integration-losing-order-" + suffix
	fillKey := "redeem-integration-fill-" + suffix
	losingFillKey := "redeem-integration-losing-fill-" + suffix
	now := time.Unix(1_800_000_000, 0).UTC()
	if _, err := database.ExecContext(ctx, `
		INSERT INTO execution_accounts (
			execution_account_id, wallet_address, collateral_asset,
			total_balance, available_balance, reserved_balance
		) VALUES ($1,$2,'pUSD',100,100,0)`, accountID, wallet); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `
		INSERT INTO execution_orders (
			order_id, client_order_id, execution_account_id, venue, market_id, token_id,
			intent, venue_order_id, status, filled_size, average_fill_price,
			filled_notional, total_fees, revision, created_at, updated_at
		) VALUES ($1,$2,$3,'POLYMARKET','market-1',$4,'{}'::jsonb,$5,'FILLED',12,0.30,
		          3.60,0,1,$6,$6)`, losingOrderID, losingOrderID, accountID, losingTokenID,
		losingOrderID, now.Add(-time.Hour)); err != nil {
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
		          'TAKER','MATCHED',12,0.30,3.60,0,0,0,0,0,-3.60,
		          'INTEGRATION_TEST',$6,$6,$6)`, losingFillKey, losingOrderID, accountID,
		condition, losingTokenID, now.Add(-time.Hour)); err != nil {
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
		) VALUES ($1,'market-1',$2,$3,0,'Yes',12,12,0,3.60,0.30,0,0.50,6,2.40,
		          'SETTLED_PENDING_REDEEM',0,'data-api:test',$4)`, accountID, condition,
		losingTokenID, now); err != nil {
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
		) VALUES ($1,$2,'market-1',$3,'model','strategy',$4,$5,12,12,3.60,3.60,
		          0.30,'SETTLED_PENDING_REDEEM',$6,$7,0,'Yes',TRUE)`,
		losingLotID, accountID, losingTokenID, losingOrderID, losingFillKey,
		now.Add(-time.Hour), condition); err != nil {
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
	if _, err := database.ExecContext(ctx, `
		UPDATE execution_positions SET mark_price=0
		WHERE execution_account_id=$1 AND token_id=$2`, accountID, losingTokenID); err == nil {
		t.Fatal("zero mark price was accepted before the losing position closed")
	}
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
		WHERE account.execution_account_id=$1 AND position.token_id=$2`, accountID, tokenID).Scan(
		&total, &available, &realized, &shares, &cost, &lifecycle, &lotStatus, &redemptionStatus,
	); err != nil {
		t.Fatal(err)
	}
	if !total.Equal("148") || !available.Equal("148") || !realized.Equal("37.92") || !shares.Equal("0") || !cost.Equal("0") ||
		lifecycle != "CLOSED" || lotStatus != "CLOSED" || redemptionStatus != "APPLIED" {
		t.Fatalf("applied account/position/lot/redemption = %s/%s/%s/%s/%s/%s/%s/%s",
			total, available, realized, shares, cost, lifecycle, lotStatus, redemptionStatus)
	}
	var losingRealized, losingShares, losingCost, losingMark domain.Decimal
	var losingLifecycle, losingLotStatus string
	if err := database.QueryRowContext(ctx, `
		SELECT position.realized_pnl::text, position.total_shares::text,
		       position.cost_basis::text, position.mark_price::text,
		       position.lifecycle_status, lot.status
		FROM execution_positions position
		JOIN position_lots lot
		  ON lot.execution_account_id=position.execution_account_id
		 AND lot.token_id=position.token_id
		WHERE position.execution_account_id=$1 AND position.token_id=$2`,
		accountID, losingTokenID).Scan(
		&losingRealized, &losingShares, &losingCost, &losingMark, &losingLifecycle, &losingLotStatus,
	); err != nil {
		t.Fatal(err)
	}
	if !losingRealized.Equal("-3.6") || !losingShares.Equal("0") || !losingCost.Equal("0") ||
		!losingMark.Equal("0") || losingLifecycle != "CLOSED" || losingLotStatus != "CLOSED" {
		t.Fatalf("zero-settlement position/lot = %s/%s/%s/%s/%s/%s",
			losingRealized, losingShares, losingCost, losingMark, losingLifecycle, losingLotStatus)
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

func redemptionIntegrationDatabase(t *testing.T) *sql.DB {
	t.Helper()
	databaseURL := os.Getenv("REDEMPTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("REDEMPTION_TEST_DATABASE_URL or TRADING_EXECUTION_TEST_DATABASE_URL is not configured")
	}
	return newIntegrationDatabase(t, databaseURL)
}

func insertRedemptionIntegrationAccount(t *testing.T, database *sql.DB, accountID string) string {
	t.Helper()
	sum := sha256.Sum256([]byte(accountID))
	wallet := fmt.Sprintf("0x%x", sum[:20])
	if _, err := database.Exec(`
		INSERT INTO execution_accounts (
			execution_account_id, wallet_address, collateral_asset,
			total_balance, available_balance, reserved_balance
		) VALUES ($1,$2,'pUSD',100,100,0)`, accountID, wallet); err != nil {
		t.Fatal(err)
	}
	return wallet
}

// insertRedemptionInStatus writes a row that satisfies polymarket_redemptions_state_shape.
func insertRedemptionInStatus(t *testing.T, database *sql.DB, accountID, wallet, conditionByte, status string) domain.Redemption {
	t.Helper()
	condition := "0x" + strings.Repeat(conditionByte, 32)
	hash := "0x" + strings.Repeat("e"+conditionByte[1:], 32)
	now := time.Unix(1_800_000_000, 0).UTC()
	var query string
	switch status {
	case "READY":
		query = `INSERT INTO polymarket_redemptions (execution_account_id, condition_id, wallet_address, neg_risk, status)
			VALUES ($1,$2,$3,FALSE,'READY')`
	case "APPROVAL_SUBMITTING", "REDEEM_SUBMITTING":
		query = `INSERT INTO polymarket_redemptions (execution_account_id, condition_id, wallet_address, neg_risk, status, submitting_at)
			VALUES ($1,$2,$3,FALSE,'` + status + `',$4)`
	case "APPROVAL_SUBMITTED", "REDEEM_SUBMITTED":
		query = `INSERT INTO polymarket_redemptions (execution_account_id, condition_id, wallet_address, neg_risk, status,
			submission_provider, submission_reference, submitting_at, submitted_at)
			VALUES ($1,$2,$3,FALSE,'` + status + `','POLYMARKET_RELAYER','relayer-1',$4,$4)`
	case "CONFIRMED", "APPLIED":
		applied := "NULL"
		if status == "APPLIED" {
			applied = "$4"
		}
		query = `INSERT INTO polymarket_redemptions (execution_account_id, condition_id, wallet_address, neg_risk, status,
			transaction_hash, event_type, payout_base_units, receipt_block_number, receipt_block_hash, confirmed_at, applied_at)
			VALUES ($1,$2,$3,FALSE,'` + status + `','` + hash + `','POSITIONS_REDEEMED',0,1,'` + hash + `',$4,` + applied + `)`
	case "MANUAL_REVIEW":
		query = `INSERT INTO polymarket_redemptions (execution_account_id, condition_id, wallet_address, neg_risk, status, last_error)
			VALUES ($1,$2,$3,FALSE,'MANUAL_REVIEW','review')`
	default:
		t.Fatalf("unknown redemption status %s", status)
	}
	args := []any{accountID, condition, wallet}
	if strings.Contains(query, "$4") {
		args = append(args, now)
	}
	if _, err := database.Exec(query, args...); err != nil {
		t.Fatalf("insert %s redemption: %v", status, err)
	}
	return domain.Redemption{
		ExecutionAccountID: accountID, ConditionID: condition, WalletAddress: wallet,
		Status: domain.RedemptionStatus(status),
	}
}

func TestRedemptionSubmissionWaitsForInFlightRedemptionOfTheSameWalletPostgresIntegration(t *testing.T) {
	database := redemptionIntegrationDatabase(t)
	suffix := fmt.Sprintf("%x", time.Now().UnixNano())
	startedAt := time.Unix(1_800_000_100, 0).UTC()
	cases := map[string]bool{
		"APPROVAL_SUBMITTING": true, "APPROVAL_SUBMITTED": true, "REDEEM_SUBMITTING": true, "REDEEM_SUBMITTED": true,
		"READY": false, "CONFIRMED": false, "APPLIED": false, "MANUAL_REVIEW": false,
	}
	index := 0
	for status, blocks := range cases {
		index++
		accountID := fmt.Sprintf("redeem-busy-%s-%d", suffix, index)
		wallet := insertRedemptionIntegrationAccount(t, database, accountID)
		insertRedemptionInStatus(t, database, accountID, wallet, "a1", status)
		target := insertRedemptionInStatus(t, database, accountID, wallet, "b2", "READY")
		store, err := NewRedemptionStore(database, []string{accountID})
		if err != nil {
			t.Fatal(err)
		}
		err = store.BeginRedemptionSubmission(context.Background(), target, domain.RedemptionSubmissionRedeem, startedAt)
		if got := errors.Is(err, port.ErrRedemptionWalletBusy); got != blocks {
			t.Fatalf("other redemption %s: busy=%v (err %v), want %v", status, got, err, blocks)
		}
		if blocks {
			var state string
			var attempts int
			if err := database.QueryRow(`SELECT status, attempts FROM polymarket_redemptions
				WHERE execution_account_id=$1 AND condition_id=$2`, accountID, target.ConditionID).Scan(&state, &attempts); err != nil {
				t.Fatal(err)
			}
			if state != "READY" || attempts != 0 {
				t.Fatalf("deferred redemption changed to %s with %d attempts, want untouched READY", state, attempts)
			}
		}
	}

	// A different wallet's in-flight redemption never blocks.
	otherAccount, targetAccount := "redeem-busy-other-"+suffix, "redeem-busy-target-"+suffix
	otherWallet := insertRedemptionIntegrationAccount(t, database, otherAccount)
	insertRedemptionInStatus(t, database, otherAccount, otherWallet, "a1", "REDEEM_SUBMITTED")
	targetWallet := insertRedemptionIntegrationAccount(t, database, targetAccount)
	target := insertRedemptionInStatus(t, database, targetAccount, targetWallet, "b2", "READY")
	store, err := NewRedemptionStore(database, []string{otherAccount, targetAccount})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BeginRedemptionSubmission(context.Background(), target, domain.RedemptionSubmissionRedeem, startedAt); errors.Is(err, port.ErrRedemptionWalletBusy) {
		t.Fatalf("another wallet's redemption blocked this one: %v", err)
	}
}

func TestRedemptionSubmitFailuresAreAppendOnlyAndResetKeepsEvidencePostgresIntegration(t *testing.T) {
	database := redemptionIntegrationDatabase(t)
	ctx := context.Background()
	suffix := fmt.Sprintf("%x", time.Now().UnixNano())
	accountID := "redeem-failures-" + suffix
	wallet := insertRedemptionIntegrationAccount(t, database, accountID)
	redemption := insertRedemptionInStatus(t, database, accountID, wallet, "c3", "REDEEM_SUBMITTING")
	store, err := NewRedemptionStore(database, []string{accountID})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Unix(1_800_000_000, 0).UTC()

	summary, err := store.RedemptionSubmitFailures(ctx, redemption)
	if err != nil || summary.Count != 0 || summary.LatestText != "" {
		t.Fatalf("empty summary = %+v, %v", summary, err)
	}
	for index, text := range []string{`relayer HTTP 400: {"error":"invalid nonce"}`, "context deadline exceeded"} {
		outcome, status := domain.RedemptionSubmitRejected, 400
		if index == 1 {
			outcome, status = domain.RedemptionSubmitAmbiguous, 0
		}
		summary, err = store.RecordRedemptionSubmitFailure(ctx, redemption, domain.RedemptionSubmitFailure{
			Kind: domain.RedemptionSubmissionRedeem, Outcome: outcome, HTTPStatus: status,
			ErrorText: text, AttemptStartedAt: started,
		})
		if err != nil || summary.Count != index+1 || summary.LatestText != text {
			t.Fatalf("record %d summary = %+v, %v", index, summary, err)
		}
	}
	if _, err := database.ExecContext(ctx, `UPDATE polymarket_redemption_submit_failures SET error_text='x' WHERE execution_account_id=$1`, accountID); err == nil {
		t.Fatal("failure records must be append-only, but UPDATE succeeded")
	}
	if _, err := database.ExecContext(ctx, `DELETE FROM polymarket_redemption_submit_failures WHERE execution_account_id=$1`, accountID); err == nil {
		t.Fatal("failure records must be append-only, but DELETE succeeded")
	}
	if _, err := store.RecordRedemptionSubmitFailure(ctx, redemption, domain.RedemptionSubmitFailure{
		Kind: domain.RedemptionSubmissionRedeem, Outcome: domain.RedemptionSubmitRejected, AttemptStartedAt: started,
	}); err == nil {
		t.Fatal("a failure without error text was accepted")
	}

	next := started.Add(time.Minute)
	if err := store.ResetRedemptionForResubmit(ctx, redemption, "submission rejected, resubmitting", next); err != nil {
		t.Fatal(err)
	}
	var status, lastError string
	var attempts int
	var submittingAt sql.NullTime
	if err := database.QueryRowContext(ctx, `SELECT status, last_error, attempts, submitting_at FROM polymarket_redemptions
		WHERE execution_account_id=$1 AND condition_id=$2`, accountID, redemption.ConditionID).Scan(&status, &lastError, &attempts, &submittingAt); err != nil {
		t.Fatal(err)
	}
	if status != "READY" || lastError != "submission rejected, resubmitting" || submittingAt.Valid {
		t.Fatalf("reset row = %s/%q/%v, want READY with reason and no submit timestamp", status, lastError, submittingAt)
	}
	if summary, err = store.RedemptionSubmitFailures(ctx, redemption); err != nil || summary.Count != 2 {
		t.Fatalf("failures after reset = %+v, %v, want the 2 records kept", summary, err)
	}
	// READY is not a submitting state; a second reset must be refused.
	redemption.Status = domain.RedemptionReady
	if err := store.ResetRedemptionForResubmit(ctx, redemption, "again", next); err == nil {
		t.Fatal("a READY redemption was reset")
	}
	// A submitted redemption carries relayer identity and must never be reset.
	submitted := insertRedemptionInStatus(t, database, accountID, wallet, "d4", "REDEEM_SUBMITTED")
	submitted.Status = domain.RedemptionRedeemSubmitting
	if err := store.ResetRedemptionForResubmit(ctx, submitted, "x", next); err == nil {
		t.Fatal("a redemption with relayer identity was reset for resubmission")
	}
}
