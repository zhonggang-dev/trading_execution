package consolev2

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// This contract test is opt-in and must run only against the disposable
// console-v2 PostgreSQL database. It proves that current holdings are grouped
// by historical ownership and that a matched local mark produces nullable
// valuation fields without changing the lot facts.
func TestReaderHoldingsProjection(t *testing.T) {
	url := os.Getenv("TRADING_EXECUTION_CONSOLE_V2_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TRADING_EXECUTION_CONSOLE_V2_TEST_DATABASE_URL is not configured")
	}
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	accountID := "console-v2-holding-account-" + suffix
	marketID, conditionID, tokenID := "holding-market-"+suffix, "holding-condition-"+suffix, "holding-token-"+suffix
	now := time.Now().UTC().Truncate(time.Microsecond)
	orderIDs := []string{"holding-order-a-" + suffix, "holding-order-b-" + suffix, "holding-order-c-" + suffix}
	fillIDs := []string{"holding-fill-a-" + suffix, "holding-fill-b-" + suffix, "holding-fill-c-" + suffix}
	lotIDs := []string{"holding-lot-a-" + suffix, "holding-lot-b-" + suffix, "holding-lot-c-" + suffix}
	cleanup := func() {
		for _, lotID := range lotIDs {
			_, _ = db.ExecContext(context.Background(), `DELETE FROM position_lots WHERE lot_id=$1`, lotID)
		}
		for _, fillID := range fillIDs {
			_, _ = db.ExecContext(context.Background(), `DELETE FROM execution_fills WHERE fill_key=$1`, fillID)
		}
		for _, orderID := range orderIDs {
			_, _ = db.ExecContext(context.Background(), `DELETE FROM execution_orders WHERE order_id=$1`, orderID)
		}
		_, _ = db.ExecContext(context.Background(), `DELETE FROM execution_positions WHERE execution_account_id=$1 AND token_id=$2`, accountID, tokenID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM execution_accounts WHERE execution_account_id=$1`, accountID)
	}
	cleanup()
	defer cleanup()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO execution_accounts (execution_account_id, wallet_address, collateral_asset)
		VALUES ($1, $2, 'pUSD')`, accountID, "0x"+fmt.Sprintf("%040d", 1001)); err != nil {
		t.Fatal(err)
	}
	for index, orderID := range orderIDs {
		modelID, strategyID := "model-old", "strategy-old"
		if index == 2 {
			modelID, strategyID = "model-new", "strategy-new"
		}
		intent := fmt.Sprintf(`{"model_id":"%s","strategy_id":"%s","market_source":"POLYMARKET","condition_id":"%s","side":"BUY","price":"0.5","size":"2","outcome_index":0,"outcome_name":"Yes","metadata":{"market_question":"Holding question"}}`, modelID, strategyID, conditionID)
		if _, err := db.ExecContext(ctx, `
			INSERT INTO execution_orders (
				order_id, client_order_id, execution_account_id, venue, market_id, token_id,
				intent, status, filled_size, filled_notional, total_fees, revision, created_at, updated_at
			) VALUES ($1,$2,$3,'POLYMARKET',$4,$5,$6::jsonb,'FILLED',$7,$8,0,1,$9,$9)`,
			orderID, orderID+"-client", accountID, marketID, tokenID, intent,
			[]string{"2", "1", "4"}[index], []string{"1", "0.5", "2"}[index], now.Add(time.Duration(index)*time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO execution_fills (
				fill_key, venue, venue_fill_id, order_id, venue_order_id,
				execution_account_id, market_id, condition_id, token_id, side,
				liquidity_role, status, shares, price, gross_notional, fee_rate_bps,
				platform_fee, builder_fee_rate_bps, builder_fee, total_fee,
				net_cash_delta, fee_source, matched_at, first_observed_at, last_observed_at
			) VALUES ($1,'POLYMARKET',$1,$2,$2,$3,$4,$5,$6,'BUY','TAKER','MATCHED',$7,0.5,$8,0,0,0,0,0,$9,'INTEGRATION_TEST',$10,$10,$10)`,
			fillIDs[index], orderID, accountID, marketID, conditionID, tokenID,
			[]string{"2", "1", "4"}[index], []string{"1", "0.5", "2"}[index], []string{"-1", "-0.5", "-2"}[index], now.Add(time.Duration(index)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO execution_positions (
			execution_account_id, market_id, condition_id, token_id,
			outcome_index, outcome_name, total_shares, available_shares,
			reserved_shares, cost_basis, average_cost_price, realized_pnl,
			mark_price, market_value, unrealized_pnl, lifecycle_status
		) VALUES ($1,$2,$3,$4,0,'Yes',7,7,0,3.5,0.5,0,0.75,5.25,1.75,'OPEN')`, accountID, marketID, conditionID, tokenID); err != nil {
		t.Fatal(err)
	}
	for index, lotID := range lotIDs {
		modelID, strategyID, status := "model-old", "strategy-old", "OPEN"
		outcomeIndex, outcomeName := 0, "Yes"
		openedAt := now.Add(time.Duration(index) * time.Second)
		if index == 1 {
			status = "SETTLED_PENDING_REDEEM"
		}
		if index == 2 {
			modelID, strategyID = "model-new", "strategy-new"
			outcomeIndex, outcomeName = 1, "No"
		} else {
			openedAt = now.Add(-time.Duration(400-index) * 24 * time.Hour)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO position_lots (
				lot_id, execution_account_id, market_id, token_id, model_id, strategy_id,
				opening_order_id, opening_fill_key, original_shares, remaining_shares,
				original_cost, remaining_cost, average_entry_price, status, opened_at,
				condition_id, outcome_index, outcome_name, neg_risk
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9,$10,$10,0.5,$11,$12,$13,$14,$15,TRUE)`,
			lotID, accountID, marketID, tokenID, modelID, strategyID, orderIDs[index], fillIDs[index],
			[]string{"2", "1", "4"}[index], []string{"1", "0.5", "2"}[index], status,
			openedAt, conditionID, outcomeIndex, outcomeName); err != nil {
			t.Fatal(err)
		}
	}

	reader := New(db, RuntimeConfig{Now: func() time.Time { return now.Add(time.Hour) }})
	page, err := reader.Holdings(ctx, ListFilter{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 2 || len(page.Items) != 1 || !page.HasMore {
		t.Fatalf("holdings page=%#v, want total=2 one item with more", page)
	}
	old := page.Items[0]
	if old.ModelID != "model-new" || old.RemainingShares != "4" || old.RemainingCost != "2" || old.LotCount != 1 || old.OutcomeIndex == nil || *old.OutcomeIndex != 1 || old.OutcomeName == nil || *old.OutcomeName != "No" {
		t.Fatalf("new ownership group=%#v", old)
	}
	if old.MarketQuestion == nil || *old.MarketQuestion != "Holding question" || old.MarkPrice == nil || *old.MarkPrice != "0.75" {
		t.Fatalf("safe display fields=%#v", old)
	}
	if old.MarketValue == nil || *old.MarketValue != "3.00" || old.UnrealizedPnL == nil || *old.UnrealizedPnL != "1.00" {
		t.Fatalf("valuation=%#v", old)
	}
	if _, err := db.ExecContext(ctx, `UPDATE execution_orders SET market_id=$2 WHERE order_id=$1`, orderIDs[2], "unrelated-market"); err != nil {
		t.Fatal(err)
	}
	strictSource, err := reader.Holdings(ctx, ListFilter{Limit: 1})
	if err != nil || len(strictSource.Items) != 1 || strictSource.Items[0].MarketSource != "" {
		t.Fatalf("opening-order source identity was not strict: %#v err=%v", strictSource, err)
	}
	legacy, err := reader.Holdings(ctx, ListFilter{Limit: 1, Status: "MIXED"})
	if err != nil || legacy.Total != 1 || len(legacy.Items) != 1 || legacy.Items[0].ModelID != "model-old" || legacy.Items[0].Status != "MIXED" || legacy.Items[0].RemainingShares != "3" || legacy.Items[0].RemainingCost != "1.5" || legacy.Items[0].LotCount != 2 {
		t.Fatalf("old ownership group=%#v err=%v", legacy, err)
	}
	openOnly, err := reader.Holdings(ctx, ListFilter{Limit: 1, Status: "OPEN"})
	if err != nil || openOnly.Total != 1 || len(openOnly.Items) != 1 || openOnly.Items[0].ModelID != "model-new" || openOnly.Items[0].RemainingShares != "4" {
		t.Fatalf("open-only group=%#v err=%v", openOnly, err)
	}
	one := 1
	no := "No"
	newLots, err := reader.HoldingLots(ctx, HoldingIdentity{
		ExecutionAccountID: accountID,
		ModelID:            "model-new",
		StrategyID:         "strategy-new",
		MarketSource:       "",
		MarketID:           marketID,
		ConditionID:        conditionID,
		TokenID:            tokenID,
		OutcomeIndex:       &one,
		OutcomeName:        &no,
	}, ListFilter{Limit: 1})
	if err != nil || newLots.Total != 1 || len(newLots.Items) != 1 || newLots.Items[0].LotID != lotIDs[2] {
		t.Fatalf("exact new holding lots=%#v err=%v", newLots, err)
	}
	wrongSourceLots, err := reader.HoldingLots(ctx, HoldingIdentity{
		ExecutionAccountID: accountID,
		ModelID:            "model-new",
		StrategyID:         "strategy-new",
		MarketSource:       "POLYMARKET",
		MarketID:           marketID,
		ConditionID:        conditionID,
		TokenID:            tokenID,
		OutcomeIndex:       &one,
		OutcomeName:        &no,
	}, ListFilter{Limit: 1})
	if err != nil || wrongSourceLots.Total != 0 || len(wrongSourceLots.Items) != 0 {
		t.Fatalf("wrong source matched holding lots=%#v err=%v", wrongSourceLots, err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE position_lots SET outcome_index=NULL, outcome_name='' WHERE lot_id=$1`, lotIDs[2]); err != nil {
		t.Fatal(err)
	}
	nullOutcomeLots, err := reader.HoldingLots(ctx, HoldingIdentity{
		ExecutionAccountID: accountID,
		ModelID:            "model-new",
		StrategyID:         "strategy-new",
		MarketSource:       "",
		MarketID:           marketID,
		ConditionID:        conditionID,
		TokenID:            tokenID,
	}, ListFilter{Limit: 1})
	if err != nil || nullOutcomeLots.Total != 1 || len(nullOutcomeLots.Items) != 1 || nullOutcomeLots.Items[0].LotID != lotIDs[2] {
		t.Fatalf("null outcome holding lots=%#v err=%v", nullOutcomeLots, err)
	}
	zero := 0
	yes := "Yes"
	oldLotsFirst, err := reader.HoldingLots(ctx, HoldingIdentity{
		ExecutionAccountID: accountID,
		ModelID:            "model-old",
		StrategyID:         "strategy-old",
		MarketSource:       "POLYMARKET",
		MarketID:           marketID,
		ConditionID:        conditionID,
		TokenID:            tokenID,
		OutcomeIndex:       &zero,
		OutcomeName:        &yes,
	}, ListFilter{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	oldLotsSecond, err := reader.HoldingLots(ctx, HoldingIdentity{
		ExecutionAccountID: accountID,
		ModelID:            "model-old",
		StrategyID:         "strategy-old",
		MarketSource:       "POLYMARKET",
		MarketID:           marketID,
		ConditionID:        conditionID,
		TokenID:            tokenID,
		OutcomeIndex:       &zero,
		OutcomeName:        &yes,
	}, ListFilter{Limit: 1, Offset: 1})
	if err != nil || oldLotsFirst.Total != 2 || oldLotsSecond.Total != 2 || len(oldLotsFirst.Items) != 1 || len(oldLotsSecond.Items) != 1 || !oldLotsFirst.HasMore || oldLotsSecond.HasMore || oldLotsFirst.Items[0].LotID == oldLotsSecond.Items[0].LotID {
		t.Fatalf("mixed holding lot pages=%#v / %#v err=%v", oldLotsFirst, oldLotsSecond, err)
	}
	if _, err := reader.Holdings(ctx, ListFilter{From: &now}); err == nil {
		t.Fatal("holdings accepted unsupported time filter")
	}
	if _, err := reader.HoldingLots(ctx, HoldingIdentity{ExecutionAccountID: accountID}, ListFilter{From: &now}); err == nil {
		t.Fatal("holding lots accepted unsupported time filter")
	}
	if _, err := reader.HoldingLots(ctx, HoldingIdentity{ExecutionAccountID: accountID}, ListFilter{Query: tokenID}); err == nil {
		t.Fatal("holding lots accepted unsupported fuzzy filter")
	}
}

// TestReaderHoldingsPaginationStableAcrossMarketSource keeps every identity
// component equal except the safely-derived source. The source is part of the
// group identity, so it must also be part of both page ORDER BY clauses.
func TestReaderHoldingsPaginationStableAcrossMarketSource(t *testing.T) {
	url := os.Getenv("TRADING_EXECUTION_CONSOLE_V2_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TRADING_EXECUTION_CONSOLE_V2_TEST_DATABASE_URL is not configured")
	}
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	accountID := "console-v2-source-page-account-" + suffix
	marketID, conditionID, tokenID := "source-page-market-"+suffix, "source-page-condition-"+suffix, "source-page-token-"+suffix
	now := time.Now().UTC().Truncate(time.Microsecond)
	orderIDs := []string{"source-page-order-empty-" + suffix, "source-page-order-poly-" + suffix}
	fillIDs := []string{"source-page-fill-empty-" + suffix, "source-page-fill-poly-" + suffix}
	lotIDs := []string{"source-page-lot-empty-" + suffix, "source-page-lot-poly-" + suffix}
	cleanup := func() {
		for _, lotID := range lotIDs {
			_, _ = db.ExecContext(context.Background(), `DELETE FROM position_lots WHERE lot_id=$1`, lotID)
		}
		for _, fillID := range fillIDs {
			_, _ = db.ExecContext(context.Background(), `DELETE FROM execution_fills WHERE fill_key=$1`, fillID)
		}
		for _, orderID := range orderIDs {
			_, _ = db.ExecContext(context.Background(), `DELETE FROM execution_orders WHERE order_id=$1`, orderID)
		}
		_, _ = db.ExecContext(context.Background(), `DELETE FROM execution_accounts WHERE execution_account_id=$1`, accountID)
	}
	cleanup()
	defer cleanup()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO execution_accounts (execution_account_id, wallet_address, collateral_asset)
		VALUES ($1, $2, 'pUSD')`, accountID, "0x"+fmt.Sprintf("%040d", 2002)); err != nil {
		t.Fatal(err)
	}
	for index, orderID := range orderIDs {
		source := ""
		if index == 1 {
			source = "POLYMARKET"
		}
		intent := fmt.Sprintf(`{"model_id":"model-source-page","strategy_id":"strategy-source-page","market_source":"%s","condition_id":"%s","side":"BUY","price":"0.5","size":"1","outcome_index":0,"outcome_name":"Yes","metadata":{"market_question":"Source page question"}}`, source, conditionID)
		if _, err := db.ExecContext(ctx, `
			INSERT INTO execution_orders (
				order_id, client_order_id, execution_account_id, venue, market_id, token_id,
				intent, status, filled_size, filled_notional, total_fees, revision, created_at, updated_at
			) VALUES ($1,$2,$3,'POLYMARKET',$4,$5,$6::jsonb,'FILLED',1,0.5,0,1,$7,$7)`,
			orderID, orderID+"-client", accountID, marketID, tokenID, intent, now); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO execution_fills (
				fill_key, venue, venue_fill_id, order_id, venue_order_id,
				execution_account_id, market_id, condition_id, token_id, side,
				liquidity_role, status, shares, price, gross_notional, fee_rate_bps,
				platform_fee, builder_fee_rate_bps, builder_fee, total_fee,
				net_cash_delta, fee_source, matched_at, first_observed_at, last_observed_at
			) VALUES ($1,'POLYMARKET',$1,$2,$2,$3,$4,$5,$6,'BUY','TAKER','MATCHED',1,0.5,0.5,0,0,0,0,0,-0.5,'INTEGRATION_TEST',$7,$7,$7)`,
			fillIDs[index], orderID, accountID, marketID, conditionID, tokenID, now); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO position_lots (
				lot_id, execution_account_id, market_id, token_id, model_id, strategy_id,
				opening_order_id, opening_fill_key, original_shares, remaining_shares,
				original_cost, remaining_cost, average_entry_price, status, opened_at,
				condition_id, outcome_index, outcome_name, neg_risk
			) VALUES ($1,$2,$3,$4,'model-source-page','strategy-source-page',$5,$6,1,1,0.5,0.5,0.5,'OPEN',$7,$8,0,'Yes',TRUE)`,
			lotIDs[index], accountID, marketID, tokenID, orderID, fillIDs[index], now, conditionID); err != nil {
			t.Fatal(err)
		}
	}

	reader := New(db, RuntimeConfig{Now: func() time.Time { return now.Add(time.Hour) }})
	first, err := reader.Holdings(ctx, ListFilter{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	second, err := reader.Holdings(ctx, ListFilter{Limit: 1, Offset: 1})
	if err != nil {
		t.Fatal(err)
	}
	if first.Total != 2 || second.Total != 2 || len(first.Items) != 1 || len(second.Items) != 1 || !first.HasMore || second.HasMore {
		t.Fatalf("source pages=%#v / %#v, want two distinct rows and exact total", first, second)
	}
	if first.Items[0].MarketSource != "" || second.Items[0].MarketSource != "POLYMARKET" {
		t.Fatalf("source ordering=%q/%q, want empty source then POLYMARKET", first.Items[0].MarketSource, second.Items[0].MarketSource)
	}
	if first.Items[0].MarketID != second.Items[0].MarketID || first.Items[0].TokenID != second.Items[0].TokenID || first.Items[0].ModelID != second.Items[0].ModelID {
		t.Fatalf("source pages lost shared identity: %#v / %#v", first.Items[0], second.Items[0])
	}
}
