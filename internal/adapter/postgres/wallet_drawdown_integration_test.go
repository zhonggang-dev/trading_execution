package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/walletdrawdown"
)

func drawdownStoreDatabase(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL required")
	}
	db := newIntegrationDatabase(t, dsn)
	var schema string
	if err := db.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"0048_wallet_drawdown_samples.sql", "0049_wallet_drawdown_order_operations.sql", "0050_wallet_drawdown_sell_links.sql", "0051_wallet_drawdown_follow_buy_links.sql", "0052_wallet_drawdown_trading_pnl.sql"} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(strings.ReplaceAll(string(raw), "search_path=public,pg_temp", "search_path="+schema+",pg_temp")); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func TestPostgresIntegrationWalletDrawdownSellDueAndClaim(t *testing.T) {
	db := drawdownStoreDatabase(t)
	store, err := NewWalletDrawdownStore(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	slot := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	matched := slot.Add(30 * time.Second)
	drawdown := "55.000000"
	if inserted, err := store.InsertSample(ctx, drawdownSample(slot, "145.000000", &drawdown, true)); err != nil || !inserted {
		t.Fatalf("insert linked sample = %v, %v", inserted, err)
	}
	insertAccount(t, db, "main", "0xwallet", "100", "100", "0")
	buy := domain.OrderIntent{
		ClientOrderID: "buy-client", ExecutionAccountID: "main", MarketID: "m", ConditionID: "c", TokenID: "100",
		Side: domain.SideBuy, TimeInForce: domain.TimeInForceFOK, WorstPrice: "0.05", Size: "100",
	}
	if err := store.RecordOrderOperation(ctx, slot, 1, buy); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_orders
		    (order_id, client_order_id, execution_account_id, venue, market_id, token_id, intent, status,
		     filled_size, filled_notional, total_fees, revision, created_at, updated_at)
		VALUES ('buy', 'buy-client', 'main', 'polymarket', 'm', '100', '{}'::jsonb, 'FILLED',
		        100, 5, 0.25, 1, $1, $1)`, matched); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_fills
		    (fill_key, venue, venue_fill_id, order_id, venue_order_id, execution_account_id, market_id, condition_id,
		     token_id, side, liquidity_role, status, shares, price, gross_notional, fee_rate_bps, platform_fee,
		     builder_fee_rate_bps, builder_fee, total_fee, net_cash_delta, fee_source,
		     matched_at, first_observed_at, last_observed_at, confirmed_at, applied_at)
		VALUES ('fill', 'polymarket', 'fill', 'buy', 'venue-buy', 'main', 'm', 'c',
		        '100', 'BUY', 'TAKER', 'CONFIRMED', 100, 0.05, 5, 0, 0.25, 0, 0, 0.25, -5.25, 'test',
		        $1, $1, $1, $1, $1)`, matched); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO position_lots
		    (lot_id, execution_account_id, market_id, condition_id, token_id, outcome_index, outcome_name, neg_risk,
		     model_id, strategy_id, opening_order_id, opening_fill_key, original_shares, remaining_shares,
		     original_cost, remaining_cost, average_entry_price, status, opened_at)
		VALUES ('lot', 'main', 'm', 'c', '100', 0, 'Yes', false, 'wallet_drawdown', 'wallet_drawdown',
		        'buy', 'fill', 100, 100, 5.25, 5.25, 0.05, 'OPEN', $1)`, matched); err != nil {
		t.Fatal(err)
	}
	if due, err := store.ListDueSells(ctx, matched.Add(-time.Second), time.Now()); err != nil || len(due) != 0 {
		t.Fatalf("early lots = %+v, %v", due, err)
	}
	due, err := store.ListDueSells(ctx, matched, time.Now())
	if err != nil || len(due) != 1 || due[0].Lot.LotID != "lot" || !due[0].BoughtCost.Equal("5.25") {
		t.Fatalf("due lots = %+v, %v", due, err)
	}
	sell := buy
	sell.ClientOrderID, sell.Side, sell.TimeInForce, sell.TargetLotID, sell.WorstPrice =
		"sell-client", domain.SideSell, domain.TimeInForceGTC, "lot", "0.66"
	if claimed, err := store.ClaimSell(ctx, due[0], sell); err != nil || !claimed {
		t.Fatalf("claim = %v, %v", claimed, err)
	}
	if claimed, err := store.ClaimSell(ctx, due[0], sell); err != nil || claimed {
		t.Fatalf("duplicate claim = %v, %v", claimed, err)
	}
	if due, err := store.ListDueSells(ctx, matched, time.Now()); err != nil || len(due) != 0 {
		t.Fatalf("lots with unresolved sale = %+v, %v", due, err)
	}
	if sells, err := store.ListOpenSellsForBuy(ctx); err != nil || len(sells) != 0 {
		t.Fatalf("unconfirmed sell must be excluded: %+v, %v", sells, err)
	}
	raw, err := json.Marshal(sell)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_orders
		    (order_id, client_order_id, execution_account_id, venue, venue_order_id, market_id, token_id,
		     intent, status, filled_size, revision, created_at, updated_at)
		VALUES ('sell', 'sell-client', 'main', 'polymarket', 'venue-sell', 'm', '100',
		        $1::jsonb, 'PARTIALLY_FILLED', 20, 1, $2, $2)`, raw, matched); err != nil {
		t.Fatal(err)
	}
	sells, err := store.ListOpenSellsForBuy(ctx)
	if err != nil || len(sells) != 1 || sells[0].Intent.ClientOrderID != "sell-client" || !sells[0].Remaining.Equal("80") {
		t.Fatalf("partially filled open sells = %+v, %v", sells, err)
	}
	follow := domain.OrderIntent{
		ClientOrderID: "follow-buy", ExecutionAccountID: "wallet-10",
		Side: domain.SideBuy, TimeInForce: domain.TimeInForceIOC, Price: "0.66", Size: "100",
	}
	if claimed, err := store.ClaimFollowBuy(ctx, "sell-client", follow); err != nil || claimed {
		t.Fatalf("claim with stale quantity = %v, %v", claimed, err)
	}
	follow.Size = "80"
	if claimed, err := store.ClaimFollowBuy(ctx, "sell-client", follow); err != nil || !claimed {
		t.Fatalf("claim open sell = %v, %v", claimed, err)
	}
	follow.ClientOrderID = "duplicate-follow-buy"
	if claimed, err := store.ClaimFollowBuy(ctx, "sell-client", follow); err != nil || claimed {
		t.Fatalf("duplicate follow-buy claim = %v, %v", claimed, err)
	}
	if sells, err := store.ListOpenSellsForBuy(ctx); err != nil || len(sells) != 0 {
		t.Fatalf("already claimed open sells = %+v, %v", sells, err)
	}
	var sampleAccount, buyerAccount string
	if err := db.QueryRow(`
		SELECT sample_account_id, execution_account_id FROM wallet_drawdown_order_operations
		WHERE source_sell_client_order_id = 'sell-client'`).Scan(&sampleAccount, &buyerAccount); err != nil ||
		sampleAccount != "main" || buyerAccount != "wallet-10" {
		t.Fatalf("follow buy audit = %q %q, %v", sampleAccount, buyerAccount, err)
	}
}

func TestPostgresIntegrationWalletDrawdownOrderOperations(t *testing.T) {
	db := drawdownStoreDatabase(t)
	store, err := NewWalletDrawdownStore(db)
	if err != nil {
		t.Fatal(err)
	}
	slot := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	ctx := context.Background()
	drawdown := "55.000000"
	if inserted, err := store.InsertSample(ctx, drawdownSample(slot, "145.000000", &drawdown, true)); err != nil || !inserted {
		t.Fatalf("insert linked sample = %v, %v", inserted, err)
	}
	var appRoleExists bool
	if err := db.QueryRow(`SELECT to_regrole('trading_execution_app') IS NOT NULL`).Scan(&appRoleExists); err != nil {
		t.Fatal(err)
	}
	if appRoleExists {
		var schema string
		if err := db.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`GRANT USAGE ON SCHEMA ` + schema + ` TO trading_execution_app`); err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		if _, err := db.Exec(`SET ROLE trading_execution_app`); err != nil {
			t.Fatal(err)
		}
	}
	intent := domain.OrderIntent{
		ClientOrderID: "drawdown:buy", ExecutionAccountID: "main",
		MarketID: "market", ConditionID: "condition", TokenID: "100",
		Side: domain.SideBuy, TimeInForce: domain.TimeInForceFOK, WorstPrice: "0.031", Size: "100.2",
	}
	if err := store.RecordOrderOperation(ctx, slot, 1, intent); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordOrderOperation(ctx, slot, 1, intent); err == nil {
		t.Fatal("duplicate operation was accepted")
	}
	if err := store.FinishOrderOperation(ctx, intent.ClientOrderID, true); err != nil {
		t.Fatal(err)
	}
	intent.ClientOrderID, intent.Side = "drawdown:sell", domain.SideSell
	if err := store.RecordOrderOperation(ctx, slot, 2, intent); err != nil {
		t.Fatal(err)
	}
	if err := store.FinishOrderOperation(ctx, intent.ClientOrderID, false); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM wallet_drawdown_order_operations
		WHERE execution_account_id = 'main' AND slot_at = $1 AND price = 0.031 AND size = 100.2
		  AND submit_finished_at IS NOT NULL AND
		  ((attempt = 1 AND side = 'BUY' AND submission_state = 'ORDER_RECORDED') OR
		   (attempt = 2 AND side = 'SELL' AND submission_state = 'SUBMIT_UNCONFIRMED'))`, slot).Scan(&count); err != nil || count != 2 {
		t.Fatalf("expected two attributed operations, got %d, %v", count, err)
	}
}

func drawdownSample(slot time.Time, pnl string, drawdown *string, exceeded bool) walletdrawdown.Sample {
	s := walletdrawdown.Sample{
		AccountID: "main", SlotAt: slot, SampledAt: slot.Add(20 * time.Second),
		PositionsValue: "0.000000", RealizedPnL: pnl, UnrealizedPnL: "0", TotalPnL: pnl, WindowMinutes: 60, Threshold: "50.00", Exceeded: exceeded,
		Positions: []walletdrawdown.PositionValue{{TokenID: "t1", Title: "A", Outcome: "No", Shares: "1", Price: "0.5", Value: "0.500000"}},
	}
	if drawdown != nil {
		peak, peakAt := "200.000000", slot.Add(-10*time.Minute)
		s.Drawdown, s.PeakPnL, s.PeakAt = drawdown, &peak, &peakAt
	}
	return s
}

func TestPostgresIntegrationWalletDrawdownStoreRoundTripAndEpisode(t *testing.T) {
	db := drawdownStoreDatabase(t)
	store, err := NewWalletDrawdownStore(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	base := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	d60, d10, d65 := "60.000000", "10.000000", "65.000000"
	legacySlot := base.Add(-10 * time.Minute)
	if _, err := db.Exec(`
		INSERT INTO wallet_drawdown_samples
		    (execution_account_id, slot_at, sampled_at, cash_pusd, cash_block, positions_value, equity,
		     window_minutes, window_peak_equity, window_peak_at, drawdown, threshold_usd, exceeded, notified, positions)
		VALUES ('main', $1, $1, 1000, 7, 0, 1000, 60, 1100, $1, 100, 50, TRUE, TRUE, '[]')`, legacySlot); err != nil {
		t.Fatal(err)
	}
	if history, err := store.WindowSamples(ctx, "main", base.Add(-time.Hour), base); err != nil || len(history) != 0 {
		t.Fatalf("legacy equity entered PnL history: %+v, %v", history, err)
	}
	if last, err := store.LastNotifiedDrawdown(ctx, "main", base); err != nil || last != nil {
		t.Fatalf("legacy notification entered PnL episode: %v, %v", last, err)
	}
	rows := []walletdrawdown.Sample{
		drawdownSample(base, "200.000000", nil, false),
		drawdownSample(base.Add(10*time.Minute), "140.000000", &d60, true),
		drawdownSample(base.Add(20*time.Minute), "190.000000", &d10, false),
		drawdownSample(base.Add(30*time.Minute), "135.000000", &d65, true),
	}
	for _, row := range rows {
		inserted, err := store.InsertSample(ctx, row)
		if err != nil || !inserted {
			t.Fatalf("insert %v: inserted=%v err=%v", row.SlotAt, inserted, err)
		}
	}
	if inserted, err := store.InsertSample(ctx, rows[0]); err != nil || inserted {
		t.Fatalf("duplicate slot inserted=%v err=%v", inserted, err)
	}
	if err := store.MarkNotified(ctx, "main", rows[1].SlotAt, true, ""); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := store.LoadSample(ctx, "main", rows[1].SlotAt)
	if err != nil || !found || !loaded.Notified || *loaded.Drawdown != "60.000000" || loaded.PeakAt == nil || len(loaded.Positions) != 1 || loaded.Positions[0].Title != "A" {
		t.Fatalf("loaded=%#v found=%v err=%v", loaded, found, err)
	}
	// Episode of rows[1] ended at rows[2]; rows[3] starts a new one.
	if last, err := store.LastNotifiedDrawdown(ctx, "main", rows[2].SlotAt); err != nil || last == nil || last.FloatString(0) != "60" {
		t.Fatalf("last before rows[2] = %v err=%v", last, err)
	}
	if last, err := store.LastNotifiedDrawdown(ctx, "main", rows[3].SlotAt.Add(10*time.Minute)); err != nil || last != nil {
		t.Fatalf("last in new episode = %v err=%v", last, err)
	}
	window, err := store.WindowSamples(ctx, "main", base.Add(5*time.Minute), rows[3].SlotAt)
	if err != nil || len(window) != 2 || !window[0].SlotAt.Equal(rows[1].SlotAt) {
		t.Fatalf("window=%d err=%v", len(window), err)
	}
	if err := store.MarkNotified(ctx, "main", base.Add(time.Hour), true, ""); err == nil {
		t.Fatal("marking a missing slot must fail")
	}
}
