package consolev2

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// Reuse the migrated-schema holding fixture after its legacy assertions. The
// same durable lots prove that old and new projections have different keys
// without changing the audit identity or inventing a second ledger.
func assertAccountPortfolioProjection(t *testing.T, ctx context.Context, db *sql.DB, account, market, condition, token string, orders, lots []string) {
	t.Helper()
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`UPDATE execution_orders SET market_id=$2 WHERE order_id=$1`, orders[2], market)
	exec(`UPDATE position_lots SET outcome_index=0,outcome_name='Yes' WHERE lot_id=$1`, lots[2])
	// A missing legacy PM market_source is unambiguously derived from its venue.
	exec(`UPDATE execution_orders SET intent=intent-'market_source' WHERE order_id=$1`, orders[0])
	reader := New(db, RuntimeConfig{ManagedAccounts: []ManagedAccount{{ExecutionAccountID: account, MarketSource: "POLYMARKET"}}, Bindings: []Binding{{ExecutionAccountID: account, MarketSource: "POLYMARKET", Mode: "PAUSED", StrategyID: "current-strategy", PredictionModelID: "current-model", SourceMode: "SANDBOX"}}})
	page, err := reader.PortfolioHoldings(ctx, ListFilter{ExecutionAccountID: account, Limit: 1})
	if err != nil || page.Total != 1 || len(page.Items) != 1 {
		t.Fatalf("portfolio grouping=%#v err=%v", page, err)
	}
	asset := page.Items[0]
	if asset.RemainingShares != "7" || asset.RemainingCost != "3.5" || asset.Status != "MIXED" || asset.LotCount != 3 || asset.MarketSource != "POLYMARKET" {
		t.Fatalf("portfolio asset=%#v", asset)
	}
	encoded, _ := json.Marshal(asset)
	if strings.Contains(string(encoded), "model_id") || strings.Contains(string(encoded), "mark_price") {
		t.Fatalf("historical owner/valuation leaked into asset: %s", encoded)
	}
	searched, err := reader.PortfolioHoldings(ctx, ListFilter{ExecutionAccountID: account, Query: "model-new", Limit: 1})
	if err != nil || searched.Total != 1 || searched.Items[0].RemainingShares != "7" {
		t.Fatalf("q must not truncate group to matching lot: %#v %v", searched, err)
	}
	open, err := reader.PortfolioHoldings(ctx, ListFilter{ExecutionAccountID: account, Status: "OPEN", Limit: 1})
	if err != nil || open.Total != 0 {
		t.Fatalf("OPEN filter must not expose partial MIXED group: %#v %v", open, err)
	}
	zero := 0
	yes := "Yes"
	identity := PortfolioHoldingIdentity{ExecutionAccountID: account, MarketSource: "POLYMARKET", MarketID: market, ConditionID: condition, TokenID: token, OutcomeIndex: &zero, OutcomeName: &yes}
	seen := map[string]bool{}
	for offset := 0; offset < 3; offset++ {
		members, err := reader.PortfolioHoldingLots(ctx, identity, ListFilter{Limit: 1, Offset: offset})
		if err != nil || members.Total != 3 || len(members.Items) != 1 || members.HasMore != (offset < 2) {
			t.Fatalf("portfolio members page %d=%#v %v", offset, members, err)
		}
		lot := members.Items[0]
		if seen[lot.LotID] || (lot.ModelID != "model-old" && lot.ModelID != "model-new") {
			t.Fatalf("lost historical identity %#v", lot)
		}
		seen[lot.LotID] = true
	}
	wrong := identity
	wrong.ExecutionAccountID = account + "-other"
	members, err := reader.PortfolioHoldingLots(ctx, wrong, ListFilter{Limit: 5})
	if err != nil || members.Total != 0 {
		t.Fatalf("members leaked across account: %#v %v", members, err)
	}
	legacy, err := reader.Holdings(ctx, ListFilter{ExecutionAccountID: account, Limit: 10})
	if err != nil || legacy.Total != 3 {
		t.Fatalf("legacy model/source grouping changed: %#v %v", legacy, err)
	}
	directory, err := reader.Accounts(ctx)
	if err != nil || len(directory.Accounts) != 1 || directory.Accounts[0].Mode != "PAUSED" || directory.Accounts[0].StrategyID != "current-strategy" || directory.Accounts[0].PredictionModelID != "current-model" || directory.Accounts[0].SourceMode != "SANDBOX" {
		t.Fatalf("account metadata=%#v %v", directory, err)
	}
	legacyDirectory, err := New(db, RuntimeConfig{ManagedAccounts: []ManagedAccount{{ExecutionAccountID: account, MarketSource: "POLYMARKET"}}}).Accounts(ctx)
	if err != nil || legacyDirectory.Accounts[0].Mode != "" {
		t.Fatalf("missing config fabricated ACTIVE: %#v %v", legacyDirectory, err)
	}

	for _, bindings := range [][]Binding{
		{{ExecutionAccountID: account, MarketSource: "KALSHI", Mode: "ACTIVE"}},
		{{ExecutionAccountID: account, MarketSource: "POLYMARKET", Mode: "UNKNOWN"}},
		{{ExecutionAccountID: account, MarketSource: "POLYMARKET", SourceMode: "INVALID"}},
		{{ExecutionAccountID: account}, {ExecutionAccountID: account}},
	} {
		bad := New(db, RuntimeConfig{ManagedAccounts: []ManagedAccount{{ExecutionAccountID: account, MarketSource: "POLYMARKET"}}, Bindings: bindings})
		if _, err := bad.Accounts(ctx); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("ambiguous account metadata accepted: %#v %v", bindings, err)
		}
	}

	// A different outcome remains a distinct asset even for the same token.
	exec(`UPDATE position_lots SET outcome_index=1,outcome_name='No' WHERE lot_id=$1`, lots[2])
	for offset := 0; offset < 2; offset++ {
		result, err := reader.PortfolioHoldings(ctx, ListFilter{ExecutionAccountID: account, Limit: 1, Offset: offset})
		if err != nil || result.Total != 2 || len(result.Items) != 1 || *result.Items[0].OutcomeIndex != 1-offset {
			t.Fatalf("stable outcome paging %d: %#v %v", offset, result, err)
		}
	}
	// Platform evidence, not account spelling, keeps assets apart.
	exec(`UPDATE position_lots SET outcome_index=0,outcome_name='Yes' WHERE lot_id=$1`, lots[2])
	exec(`UPDATE execution_orders SET venue='KALSHI',intent=jsonb_set(intent,'{market_source}','"KALSHI"') WHERE order_id=$1`, orders[2])
	result, err := reader.PortfolioHoldings(ctx, ListFilter{ExecutionAccountID: account, Limit: 5})
	if err != nil || result.Total != 2 {
		t.Fatalf("platform isolation: %#v %v", result, err)
	}
	pm, err := reader.PortfolioHoldings(ctx, ListFilter{ExecutionAccountID: account, MarketSource: "POLYMARKET", Limit: 5})
	if err != nil || pm.Total != 1 || pm.Items[0].RemainingShares != "3" {
		t.Fatalf("platform filter: %#v %v", pm, err)
	}
	// Conflicting frozen source and venue cannot silently attribute the asset.
	exec(`UPDATE execution_orders SET venue='POLYMARKET' WHERE order_id=$1`, orders[2])
	if _, err := reader.PortfolioHoldings(ctx, ListFilter{ExecutionAccountID: account, Limit: 1}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("conflicting source accepted: %v", err)
	}
	exec(`UPDATE execution_orders SET venue='KALSHI' WHERE order_id=$1`, orders[2])
	// When strict opening-order evidence is absent, a validated account supplies
	// the platform; an unconfigured account remains explicitly unavailable.
	exec(`UPDATE execution_orders SET market_id='unrelated-again' WHERE order_id=$1`, orders[2])
	fallback, err := reader.PortfolioHoldings(ctx, ListFilter{ExecutionAccountID: account, Limit: 1})
	if err != nil || fallback.Total != 1 || fallback.Items[0].RemainingShares != "7" {
		t.Fatalf("configured fallback: %#v %v", fallback, err)
	}
	if _, err := New(db, RuntimeConfig{}).PortfolioHoldings(ctx, ListFilter{ExecutionAccountID: account, Limit: 1}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("unconfirmed source fabricated: %v", err)
	}
	// Preserve numeric precision down to one micro-share and do not drop old lots.
	exec(`UPDATE position_lots SET remaining_shares=0.000001,remaining_cost=0.0000003 WHERE lot_id=$1`, lots[2])
	precision, err := reader.PortfolioHoldings(ctx, ListFilter{ExecutionAccountID: account, Limit: 1})
	if err != nil || precision.Items[0].RemainingShares != "3.000001" || precision.Items[0].RemainingCost != "1.5000003" {
		t.Fatalf("decimal precision: %#v %v", precision, err)
	}
	// A second real account holds the same identity fields. All-account views
	// must retain two groups, and member reads must not borrow the other lot.
	other := account + "-other"
	otherOrder := orders[0] + "-other"
	otherFill := lots[0] + "-other-fill"
	otherLot := lots[0] + "-other"
	exec(`INSERT INTO execution_accounts(execution_account_id,wallet_address,collateral_asset) VALUES($1,'0xother','pUSD')`, other)
	defer func() {
		for _, statement := range []string{`DELETE FROM position_lots WHERE execution_account_id=$1`, `DELETE FROM execution_fills WHERE execution_account_id=$1`, `DELETE FROM execution_orders WHERE execution_account_id=$1`, `DELETE FROM execution_accounts WHERE execution_account_id=$1`} {
			if _, err := db.ExecContext(context.Background(), statement, other); err != nil {
				t.Error(err)
			}
		}
	}()
	exec(`INSERT INTO execution_orders(order_id,client_order_id,execution_account_id,venue,market_id,token_id,intent,status,revision,created_at,updated_at)
 SELECT $2,$2,$3,venue,market_id,token_id,intent,status,revision,created_at,updated_at FROM execution_orders WHERE order_id=$1`, orders[0], otherOrder, other)
	exec(`INSERT INTO execution_fills(fill_key,venue,venue_fill_id,order_id,venue_order_id,execution_account_id,market_id,condition_id,token_id,side,liquidity_role,status,shares,price,gross_notional,fee_rate_bps,platform_fee,builder_fee_rate_bps,builder_fee,total_fee,net_cash_delta,fee_source,matched_at,first_observed_at,last_observed_at)
 SELECT $2,venue,$2,$3,$3,$4,market_id,condition_id,token_id,side,liquidity_role,status,shares,price,gross_notional,fee_rate_bps,platform_fee,builder_fee_rate_bps,builder_fee,total_fee,net_cash_delta,fee_source,matched_at,first_observed_at,last_observed_at FROM execution_fills WHERE order_id=$1`, orders[0], otherFill, otherOrder, other)
	exec(`INSERT INTO position_lots(lot_id,execution_account_id,market_id,token_id,model_id,strategy_id,opening_order_id,opening_fill_key,original_shares,remaining_shares,original_cost,remaining_cost,average_entry_price,status,opened_at,condition_id,outcome_index,outcome_name,neg_risk)
 SELECT $2,$3,market_id,token_id,model_id,strategy_id,$4,$5,original_shares,remaining_shares,original_cost,remaining_cost,average_entry_price,status,opened_at,condition_id,outcome_index,outcome_name,neg_risk FROM position_lots WHERE lot_id=$1`, lots[0], otherLot, other, otherOrder, otherFill)
	all, err := reader.PortfolioHoldings(ctx, ListFilter{Limit: 10})
	if err != nil || all.Total != 2 {
		t.Fatalf("cross-account asset aggregation: %#v %v", all, err)
	}
	for _, item := range all.Items {
		if item.ExecutionAccountID == other && item.RemainingShares != "2" {
			t.Fatalf("other account shares merged: %#v", item)
		}
	}
	own, err := reader.PortfolioHoldingLots(ctx, identity, ListFilter{Limit: 10})
	if err != nil || own.Total != 3 {
		t.Fatalf("own lots borrowed another account: %#v %v", own, err)
	}
	for _, lot := range own.Items {
		if lot.ExecutionAccountID != account {
			t.Fatalf("cross-account lot leak: %#v", lot)
		}
	}

}
