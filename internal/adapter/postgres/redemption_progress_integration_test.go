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

func TestRedemptionProgressReaderListsOnlyInFlightRedemptions(t *testing.T) {
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
	accountID := "redeem-progress-" + suffix
	wallet := fmt.Sprintf("0x%x", walletHash[:20])
	submitted := "0x" + strings.Repeat("51", 32)
	confirmed := "0x" + strings.Repeat("52", 32)
	ready := "0x" + strings.Repeat("53", 32)
	now := time.Unix(1_800_000_000, 0).UTC()
	if _, err := database.ExecContext(ctx, `
		INSERT INTO execution_accounts (
			execution_account_id, wallet_address, collateral_asset,
			total_balance, available_balance, reserved_balance
		) VALUES ($1,$2,'pUSD',100,100,0)`, accountID, wallet); err != nil {
		t.Fatal(err)
	}
	for index, condition := range []string{submitted, confirmed, ready} {
		if _, err := database.ExecContext(ctx, `
			INSERT INTO execution_positions (
				execution_account_id, market_id, condition_id, token_id,
				outcome_index, outcome_name, total_shares, available_shares,
				reserved_shares, cost_basis, average_cost_price, realized_pnl,
				mark_price, market_value, unrealized_pnl, lifecycle_status,
				settlement_price, settlement_source, settled_at
			) VALUES ($1,'market-1',$2,$3,1,'No',3.35,3.35,0,0.8589,0.2564,0,1,3.35,2.4911,
			          'SETTLED_PENDING_REDEEM',1,'data-api:test',$4)`,
			accountID, condition, fmt.Sprintf("%d%d", time.Now().UnixNano(), index), now); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := database.ExecContext(ctx, `
		INSERT INTO polymarket_redemptions (
			execution_account_id, condition_id, wallet_address, neg_risk, status,
			submission_provider, submission_reference, submitting_at, submitted_at
		) VALUES ($1,$2,$3,FALSE,'REDEEM_SUBMITTED','relayer','ref-1',$4,$4)`,
		accountID, submitted, wallet, now); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `
		INSERT INTO polymarket_redemptions (
			execution_account_id, condition_id, wallet_address, neg_risk, status,
			submission_provider, submission_reference, transaction_hash, event_type,
			payout_base_units, receipt_block_number, receipt_block_hash, confirmations,
			submitting_at, submitted_at, confirmed_at
		) VALUES ($1,$2,$3,FALSE,'CONFIRMED','relayer','ref-2',$4,'POSITIONS_REDEEMED',
		          3350000,100,$5,64,$6,$6,$6)`,
		accountID, confirmed, wallet, "0x"+strings.Repeat("ab", 32), "0x"+strings.Repeat("cd", 32), now); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `
		INSERT INTO polymarket_redemptions (execution_account_id, condition_id, wallet_address, neg_risk, status)
		VALUES ($1,$2,$3,FALSE,'READY')`, accountID, ready, wallet); err != nil {
		t.Fatal(err)
	}

	reader, err := NewRedemptionProgressReader(database)
	if err != nil {
		t.Fatal(err)
	}
	redemptions, err := reader.ListInFlightRedemptions(ctx, accountID)
	if err != nil {
		t.Fatalf("ListInFlightRedemptions() error = %v", err)
	}
	if len(redemptions) != 2 {
		t.Fatalf("in-flight redemptions = %#v, want REDEEM_SUBMITTED and CONFIRMED only", redemptions)
	}
	byCondition := map[string]domain.InFlightRedemption{}
	for _, redemption := range redemptions {
		byCondition[redemption.ConditionID] = redemption
	}
	if value := byCondition[submitted]; value.Status != domain.RedemptionRedeemSubmitted || !value.ExpectedPayout.Equal("3.35") {
		t.Fatalf("submitted redemption = %#v, want managed binary payout 3.35", value)
	}
	if value := byCondition[confirmed]; value.Status != domain.RedemptionConfirmed || !value.ExpectedPayout.Equal("3.35") {
		t.Fatalf("confirmed redemption = %#v, want receipt payout 3.35", value)
	}
}
