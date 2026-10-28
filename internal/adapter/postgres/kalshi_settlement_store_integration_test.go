package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

func kalshiSettlementDatabase(t *testing.T) *sql.DB {
	t.Helper()
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not configured")
	}
	return newIntegrationDatabase(t, databaseURL)
}

func seedKalshiSettlementAccount(t *testing.T, database *sql.DB, accountID string, balance string) {
	t.Helper()
	if _, err := database.Exec(`
		INSERT INTO execution_accounts (
			execution_account_id, wallet_address, collateral_asset,
			total_balance, available_balance, reserved_balance
		) VALUES ($1,$2,'USD',$3::numeric,$3::numeric,0)`, accountID, "kalshi:key-"+accountID, balance); err != nil {
		t.Fatal(err)
	}
}

// seedKalshiHolding inserts one filled order, its fill, the open position and a
// single open lot for one side of a Kalshi market.
func seedKalshiHolding(t *testing.T, database *sql.DB, accountID, ticker, side, shares, cost string) {
	t.Helper()
	token := "kalshi:" + ticker + ":" + side
	suffix := strings.ToLower(accountID + ticker + side)
	orderID, fillKey, lotID := "order-"+suffix, "fill-"+suffix, "lot-"+suffix
	index := 0
	if side == "NO" {
		index = 1
	}
	openedAt := time.Date(2026, 10, 28, 1, 0, 0, 0, time.UTC)
	if _, err := database.Exec(`
		INSERT INTO execution_orders (
			order_id, client_order_id, execution_account_id, venue, market_id, token_id,
			intent, venue_order_id, status, filled_size, average_fill_price,
			filled_notional, total_fees, revision, created_at, updated_at
		) VALUES ($1,$1,$2,'KALSHI',$3,$4,'{}'::jsonb,$1,'FILLED',$5::numeric,0.5,$6::numeric,0,1,$7,$7)`,
		orderID, accountID, ticker, token, shares, cost, openedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`
		INSERT INTO execution_fills (
			fill_key, venue, venue_fill_id, order_id, venue_order_id,
			execution_account_id, market_id, condition_id, token_id, side,
			liquidity_role, status, shares, price, gross_notional, fee_rate_bps,
			platform_fee, builder_fee_rate_bps, builder_fee, total_fee,
			net_cash_delta, fee_source, matched_at, first_observed_at, last_observed_at
		) VALUES ($1,'KALSHI',$1,$2,$2,$3,$4,$5,$6,'BUY',
		          'TAKER','MATCHED',$7::numeric,0.5,$8::numeric,0,0,0,0,0,-$8::numeric,
		          'INTEGRATION_TEST',$9,$9,$9)`,
		fillKey, orderID, accountID, ticker, "kalshi:"+ticker, token, shares, cost, openedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`
		INSERT INTO execution_positions (
			execution_account_id, market_id, condition_id, token_id,
			outcome_index, outcome_name, total_shares, available_shares,
			reserved_shares, cost_basis, average_cost_price, realized_pnl,
			mark_price, market_value, unrealized_pnl, lifecycle_status
		) VALUES ($1,$2,$3,$4,$5,$6,$7::numeric,$7::numeric,0,$8::numeric,0.5,0,0.5,0,0,'OPEN')`,
		accountID, ticker, "kalshi:"+ticker, token, index, strings.Title(strings.ToLower(side)), shares, cost); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`
		INSERT INTO position_lots (
			lot_id, execution_account_id, market_id, token_id, model_id, strategy_id,
			opening_order_id, opening_fill_key, original_shares, remaining_shares,
			original_cost, remaining_cost, average_entry_price, status, opened_at,
			condition_id, outcome_index, outcome_name
		) VALUES ($1,$2,$3,$4,'model','strategy',$5,$6,$7::numeric,$7::numeric,$8::numeric,$8::numeric,
		          0.5,'OPEN',$9,$10,$11,$12)`,
		lotID, accountID, ticker, token, orderID, fillKey, shares, cost, openedAt, "kalshi:"+ticker,
		index, strings.Title(strings.ToLower(side))); err != nil {
		t.Fatal(err)
	}
}

func kalshiRecord(ticker, result, yes, no, revenue, payoutPerYes string) domain.KalshiSettlement {
	return domain.KalshiSettlement{
		Ticker: ticker, MarketResult: result, YesCount: domain.Decimal(yes), NoCount: domain.Decimal(no),
		Revenue: domain.Decimal(revenue), PayoutPerYes: domain.Decimal(payoutPerYes), FeeCost: "0.039700",
		SettledAt: time.Date(2026, 10, 28, 7, 0, 0, 0, time.UTC),
		Raw:       []byte(fmt.Sprintf(`{"ticker":%q,"market_result":%q}`, ticker, result)),
	}
}

func accountBalances(t *testing.T, database *sql.DB, accountID string) (total, available, reserved string) {
	t.Helper()
	if err := database.QueryRow(`
		SELECT total_balance::text, available_balance::text, reserved_balance::text
		FROM execution_accounts WHERE execution_account_id=$1`, accountID).Scan(&total, &available, &reserved); err != nil {
		t.Fatal(err)
	}
	return
}

func scalar(t *testing.T, database *sql.DB, query string, args ...any) string {
	t.Helper()
	var value string
	if err := database.QueryRow(query, args...).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestKalshiSettlementAppliesWinnerAndLoserAtomicallyPostgresIntegration(t *testing.T) {
	database := kalshiSettlementDatabase(t)
	ctx := context.Background()
	account := "kalshi:settle-win"
	seedKalshiSettlementAccount(t, database, account, "100")
	seedKalshiHolding(t, database, account, "KXWIN-1", "YES", "10", "4")
	seedKalshiHolding(t, database, account, "KXWIN-1", "NO", "5", "2.5")
	store, err := NewKalshiSettlementStore(database, []string{account})
	if err != nil {
		t.Fatal(err)
	}
	tickers, err := store.OpenKalshiSettlementTickers(ctx, account)
	if err != nil || len(tickers) != 1 || tickers[0] != "KXWIN-1" {
		t.Fatalf("open tickers = %v, %v", tickers, err)
	}
	at := time.Date(2026, 10, 28, 7, 5, 0, 0, time.UTC)
	record := kalshiRecord("KXWIN-1", "yes", "10", "5", "10", "1")
	outcome, err := store.ApplyKalshiSettlement(ctx, account, record, at)
	if err != nil || outcome.Result != domain.KalshiSettlementApplied {
		t.Fatalf("apply = %#v, %v", outcome, err)
	}
	if !outcome.Payout.Equal("10") || !outcome.RealizedPnL.Equal("3.5") {
		t.Fatalf("outcome payout/pnl = %s/%s, want 10/3.5", outcome.Payout, outcome.RealizedPnL)
	}
	total, available, reserved := accountBalances(t, database, account)
	if got := scalar(t, database, `SELECT (total_balance = 110 AND available_balance = 110 AND reserved_balance = 0)::text FROM execution_accounts WHERE execution_account_id=$1`, account); got != "true" {
		t.Fatalf("balances after apply = %s/%s/%s, want 110/110/0", total, available, reserved)
	}
	var yesStatus, noStatus string
	if err := database.QueryRow(`SELECT lifecycle_status FROM execution_positions WHERE token_id='kalshi:KXWIN-1:YES'`).Scan(&yesStatus); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRow(`SELECT lifecycle_status FROM execution_positions WHERE token_id='kalshi:KXWIN-1:NO'`).Scan(&noStatus); err != nil {
		t.Fatal(err)
	}
	if yesStatus != "CLOSED" || noStatus != "CLOSED" {
		t.Fatalf("position lifecycle = %s/%s, want CLOSED/CLOSED", yesStatus, noStatus)
	}
	if got := scalar(t, database, `SELECT (SELECT realized_pnl FROM execution_positions WHERE token_id='kalshi:KXWIN-1:YES') = 6
		AND (SELECT realized_pnl FROM execution_positions WHERE token_id='kalshi:KXWIN-1:NO') = -2.5`); got != "true" {
		t.Fatal("realized pnl was not split 6 / -2.5 across the winning and losing side")
	}
	if got := scalar(t, database, `SELECT COUNT(*)::text FROM position_lots WHERE execution_account_id=$1 AND status='CLOSED'`, account); got != "2" {
		t.Fatalf("closed lots = %s, want 2", got)
	}
	if got := scalar(t, database, `SELECT COUNT(*)::text FROM kalshi_settlement_lots WHERE execution_account_id=$1`, account); got != "2" {
		t.Fatalf("settlement lot rows = %s, want 2", got)
	}
	if got := scalar(t, database, `SELECT COUNT(*)::text FROM position_events WHERE execution_account_id=$1 AND event_type IN ('SETTLED','REDEEMED')`, account); got != "4" {
		t.Fatalf("position events = %s, want 4", got)
	}
	if got := scalar(t, database, `SELECT COUNT(*)::text FROM execution_account_events WHERE execution_account_id=$1 AND event_type='KALSHI_SETTLED'`, account); got != "1" {
		t.Fatalf("account events = %s, want 1", got)
	}
	if got := scalar(t, database, `SELECT COUNT(*)::text FROM execution_outbox WHERE topic IN ('trading.account.kalshi_settled.v1','trading.position.settled.v1','trading.position.redeemed.v1') AND aggregate_id LIKE 'kalshi:settle-win%'`); got != "5" {
		t.Fatalf("outbox rows = %s, want 5", got)
	}

	again, err := store.ApplyKalshiSettlement(ctx, account, record, at.Add(time.Minute))
	if err != nil || again.Result != domain.KalshiSettlementAlreadyApplied {
		t.Fatalf("second apply = %#v, %v", again, err)
	}
	if got := scalar(t, database, `SELECT (total_balance = 110)::text FROM execution_accounts WHERE execution_account_id=$1`, account); got != "true" {
		t.Fatal("applying the same settlement twice credited the cash twice")
	}
	tickers, err = store.OpenKalshiSettlementTickers(ctx, account)
	if err != nil || len(tickers) != 0 {
		t.Fatalf("open tickers after settlement = %v, %v", tickers, err)
	}
}

func TestKalshiSettlementAppliesLosingHoldingWithZeroPayoutPostgresIntegration(t *testing.T) {
	database := kalshiSettlementDatabase(t)
	account := "kalshi:settle-lose"
	seedKalshiSettlementAccount(t, database, account, "50")
	seedKalshiHolding(t, database, account, "KXLOSE-1", "YES", "2.34", "0.9594")
	store, _ := NewKalshiSettlementStore(database, []string{account})
	outcome, err := store.ApplyKalshiSettlement(context.Background(), account,
		kalshiRecord("KXLOSE-1", "no", "2.34", "0.00", "0", "0"), time.Now().UTC())
	if err != nil || outcome.Result != domain.KalshiSettlementApplied || !outcome.RealizedPnL.Equal("-0.9594") {
		t.Fatalf("apply = %#v, %v", outcome, err)
	}
	if got := scalar(t, database, `SELECT (total_balance = 50 AND available_balance = 50)::text FROM execution_accounts WHERE execution_account_id=$1`, account); got != "true" {
		t.Fatal("a losing settlement must not change cash")
	}
	if got := scalar(t, database, `SELECT COUNT(*)::text FROM execution_account_events WHERE execution_account_id=$1 AND event_type='KALSHI_SETTLED'`, account); got != "1" {
		t.Fatalf("a zero-payout settlement still needs its audit event, got %s", got)
	}
}

func TestKalshiSettlementHoldsDisagreeingRecordsForReviewPostgresIntegration(t *testing.T) {
	database := kalshiSettlementDatabase(t)
	ctx := context.Background()
	account := "kalshi:settle-review"
	seedKalshiSettlementAccount(t, database, account, "100")
	store, _ := NewKalshiSettlementStore(database, []string{account})
	for index, test := range []struct {
		name   string
		record domain.KalshiSettlement
		want   string
	}{
		{"share count differs", kalshiRecord("KXR-1", "yes", "9", "0", "9", "1"), "exchange held 9 YES"},
		{"revenue differs", kalshiRecord("KXR-2", "yes", "10", "0", "9.5", "1"), "exchange paid 9.5"},
		{"scalar result", kalshiRecord("KXR-3", "scalar", "10", "0", "3", "0.3"), "not a binary"},
		{"payout contradicts result", kalshiRecord("KXR-4", "yes", "10", "0", "10", "0"), "contradicts market result"},
	} {
		ticker := fmt.Sprintf("KXR-%d", index+1)
		seedKalshiHolding(t, database, account, ticker, "YES", "10", "4")
		outcome, err := store.ApplyKalshiSettlement(ctx, account, test.record, time.Now().UTC())
		if err != nil || outcome.Result != domain.KalshiSettlementManualReview || !strings.Contains(outcome.Reason, test.want) {
			t.Fatalf("%s: outcome = %#v, %v; want review containing %q", test.name, outcome, err, test.want)
		}
		if got := scalar(t, database, `SELECT lifecycle_status FROM execution_positions WHERE token_id=$1`, "kalshi:"+ticker+":YES"); got != "OPEN" {
			t.Fatalf("%s: position lifecycle = %s, want it left OPEN", test.name, got)
		}
		repeat, err := store.ApplyKalshiSettlement(ctx, account, test.record, time.Now().UTC())
		if err != nil || repeat.Result != domain.KalshiSettlementManualReview {
			t.Fatalf("%s: repeat = %#v, %v", test.name, repeat, err)
		}
	}
	if got := scalar(t, database, `SELECT (total_balance = 100)::text FROM execution_accounts WHERE execution_account_id=$1`, account); got != "true" {
		t.Fatal("a held settlement must not change cash")
	}
	if got := scalar(t, database, `SELECT COUNT(*)::text FROM kalshi_settlements WHERE execution_account_id=$1 AND status='MANUAL_REVIEW'`, account); got != "4" {
		t.Fatalf("review rows = %s, want 4", got)
	}
	tickers, err := store.OpenKalshiSettlementTickers(ctx, account)
	if err != nil || len(tickers) != 0 {
		t.Fatalf("held markets must not be retried automatically, got %v, %v", tickers, err)
	}
}

func TestKalshiSettlementWaitsWhileSharesAreReservedPostgresIntegration(t *testing.T) {
	database := kalshiSettlementDatabase(t)
	account := "kalshi:settle-reserved"
	seedKalshiSettlementAccount(t, database, account, "100")
	seedKalshiHolding(t, database, account, "KXRES-1", "YES", "10", "4")
	if _, err := database.Exec(`UPDATE execution_positions SET available_shares=4, reserved_shares=6 WHERE token_id='kalshi:KXRES-1:YES'`); err != nil {
		t.Fatal(err)
	}
	store, _ := NewKalshiSettlementStore(database, []string{account})
	outcome, err := store.ApplyKalshiSettlement(context.Background(), account,
		kalshiRecord("KXRES-1", "yes", "10", "0", "10", "1"), time.Now().UTC())
	if err != nil || outcome.Result != domain.KalshiSettlementDeferred {
		t.Fatalf("outcome = %#v, %v, want DEFERRED", outcome, err)
	}
	if got := scalar(t, database, `SELECT COUNT(*)::text FROM kalshi_settlements WHERE execution_account_id=$1`, account); got != "0" {
		t.Fatal("a deferred settlement must leave no row so it is retried")
	}
}

func TestKalshiSettlementRejectsAccountsOutsideScopePostgresIntegration(t *testing.T) {
	database := kalshiSettlementDatabase(t)
	if _, err := NewKalshiSettlementStore(database, []string{"main"}); err == nil {
		t.Fatal("a Polymarket account must not be accepted")
	}
	store, _ := NewKalshiSettlementStore(database, []string{"kalshi:main"})
	if _, err := store.ApplyKalshiSettlement(context.Background(), "kalshi:other",
		kalshiRecord("KX-1", "yes", "1", "0", "1", "1"), time.Now().UTC()); err == nil {
		t.Fatal("an unmanaged account must be rejected")
	}
}

func TestKalshiBalanceCheckTracksDriftWithoutChangingTheLedgerPostgresIntegration(t *testing.T) {
	database := kalshiSettlementDatabase(t)
	ctx := context.Background()
	account := "kalshi:drift"
	seedKalshiSettlementAccount(t, database, account, "100")
	store, _ := NewKalshiSettlementStore(database, []string{account})
	at := time.Date(2026, 10, 28, 8, 0, 0, 0, time.UTC)
	var port port.KalshiSettlementStore = store
	check, err := port.CheckKalshiBalance(ctx, account, "100.005", "0.01", at)
	if err != nil || check.Drifted || check.Skipped {
		t.Fatalf("within threshold = %#v, %v", check, err)
	}
	if got := scalar(t, database, `SELECT COUNT(*)::text FROM kalshi_balance_drift`); got != "0" {
		t.Fatalf("no drift row expected inside the threshold, got %s", got)
	}
	check, err = port.CheckKalshiBalance(ctx, account, "125", "0.01", at.Add(time.Minute))
	if err != nil || !check.Drifted || !check.Difference.Equal("25") {
		t.Fatalf("deposit = %#v, %v", check, err)
	}
	check, err = port.CheckKalshiBalance(ctx, account, "125", "0.01", at.Add(2*time.Minute))
	if err != nil || !check.Drifted {
		t.Fatalf("second observation = %#v, %v", check, err)
	}
	var status string
	var observations int
	if err := database.QueryRow(`SELECT status, observations FROM kalshi_balance_drift WHERE execution_account_id=$1`, account).Scan(&status, &observations); err != nil {
		t.Fatal(err)
	}
	if status != "OPEN" || observations != 2 {
		t.Fatalf("drift row = %s/%d, want OPEN/2", status, observations)
	}
	if got := scalar(t, database, `SELECT (total_balance = 100)::text FROM execution_accounts WHERE execution_account_id=$1`, account); got != "true" {
		t.Fatal("the balance check must never rewrite the ledger")
	}
	check, err = port.CheckKalshiBalance(ctx, account, "100", "0.01", at.Add(3*time.Minute))
	if err != nil || check.Drifted {
		t.Fatalf("recovery = %#v, %v", check, err)
	}
	if got := scalar(t, database, `SELECT status FROM kalshi_balance_drift WHERE execution_account_id=$1`, account); got != "RESOLVED" {
		t.Fatalf("drift row status = %s, want RESOLVED", got)
	}

	if _, err := database.Exec(`UPDATE execution_accounts SET reserved_balance=5, available_balance=95 WHERE execution_account_id=$1`, account); err != nil {
		t.Fatal(err)
	}
	check, err = port.CheckKalshiBalance(ctx, account, "30", "0.01", at.Add(4*time.Minute))
	if err != nil || !check.Skipped || check.Drifted {
		t.Fatalf("with a reservation the check must be skipped, got %#v, %v", check, err)
	}
}

func TestKalshiSettlementConcurrentSweepsCreditCashOncePostgresIntegration(t *testing.T) {
	database := kalshiSettlementDatabase(t)
	account := "kalshi:settle-race"
	seedKalshiSettlementAccount(t, database, account, "100")
	seedKalshiHolding(t, database, account, "KXRACE-1", "YES", "10", "4")
	store, _ := NewKalshiSettlementStore(database, []string{account})
	record := kalshiRecord("KXRACE-1", "yes", "10", "0", "10", "1")
	results := make(chan domain.KalshiSettlementResult, 8)
	var group sync.WaitGroup
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			outcome, err := store.ApplyKalshiSettlement(context.Background(), account, record, time.Now().UTC())
			if err != nil {
				t.Error(err)
				return
			}
			results <- outcome.Result
		}()
	}
	group.Wait()
	close(results)
	applied := 0
	for result := range results {
		switch result {
		case domain.KalshiSettlementApplied:
			applied++
		case domain.KalshiSettlementAlreadyApplied:
		default:
			t.Errorf("unexpected result %s", result)
		}
	}
	if applied != 1 {
		t.Fatalf("applied by %d callers, want exactly 1", applied)
	}
	if got := scalar(t, database, `SELECT (total_balance = 110)::text FROM execution_accounts WHERE execution_account_id=$1`, account); got != "true" {
		t.Fatal("concurrent sweeps credited the payout more than once")
	}
}
