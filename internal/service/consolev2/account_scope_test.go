package consolev2

import (
	"context"
	"encoding/json"
	"testing"
)

func TestActiveAccountScopeRequiresKnownActiveMode(t *testing.T) {
	r := New(nil, RuntimeConfig{ManagedAccounts: []ManagedAccount{
		{ExecutionAccountID: "enabled", MarketSource: "POLYMARKET"},
		{ExecutionAccountID: "exiting", MarketSource: "KALSHI"},
		{ExecutionAccountID: "paused", MarketSource: "POLYMARKET"},
		{ExecutionAccountID: "unknown", MarketSource: "POLYMARKET"},
	}, Bindings: []Binding{
		{ExecutionAccountID: "enabled", Mode: "ACTIVE"},
		{ExecutionAccountID: "exiting", Mode: "SELL_ONLY"},
		{ExecutionAccountID: "paused", Mode: "PAUSED"},
	}})
	f, err := r.normalizeScopedFilter(ListFilter{AccountScope: "active"})
	if err != nil {
		t.Fatal(err)
	}
	if len(f.scopedAccountIDs) != 2 || f.scopedAccountIDs[0] != "enabled" || f.scopedAccountIDs[1] != "exiting" {
		t.Fatalf("active scope %+v", f.scopedAccountIDs)
	}
	args := []any{"search"}
	clauses := appendAccountScope([]string{"TRUE"}, &args, f, "o.execution_account_id")
	if len(clauses) != 2 || clauses[1] != "o.execution_account_id IN (SELECT jsonb_array_elements_text($2::jsonb))" {
		t.Fatal(clauses)
	}
	var ids []string
	if err := json.Unmarshal([]byte(args[1].(string)), &ids); err != nil || len(ids) != 2 {
		t.Fatal(ids, err)
	}
	for _, scope := range []string{"", "all"} {
		all, err := r.normalizeScopedFilter(ListFilter{AccountScope: scope})
		if err != nil || len(appendAccountScope(nil, &args, all, "o.execution_account_id")) != 0 {
			t.Fatal(scope, err)
		}
	}
	for _, f := range []ListFilter{{AccountScope: "unknown"}, {AccountScope: "active", ExecutionAccountID: "paused"}, {AccountScope: "all", ExecutionAccountID: "enabled"}} {
		if _, err := r.normalizeScopedFilter(f); err == nil {
			t.Fatal("accepted invalid scope", f)
		}
	}
}

func TestActiveAccountScopeExactCountPaginationAndExplicitPausedAccount(t *testing.T) {
	f := prepareCoreFacts(t)
	f.reader.config.Bindings = []Binding{{ExecutionAccountID: "pm", Mode: "ACTIVE"}, {ExecutionAccountID: "unprefixed-k", Mode: "SELL_ONLY"}, {ExecutionAccountID: "flat", Mode: "PAUSED"}}
	f.exec(t, `INSERT INTO execution_orders(order_id,execution_account_id,venue,market_id,token_id,intent,status,created_at,updated_at) VALUES('paused-order','flat','polymarket','m','t','{}','FILLED',$1,$1)`, f.now)
	f.exec(t, `UPDATE execution_orders SET client_order_id=order_id,filled_size=0,status='FILLED',created_at=$1,updated_at=$1`, f.now)
	ctx := context.Background()
	for offset := 0; offset < 4; offset++ {
		page, err := f.reader.Orders(ctx, ListFilter{AccountScope: "active", Limit: 1, Offset: offset})
		if err != nil {
			t.Fatal(err)
		}
		if page.Total != 3 || page.HasMore != (offset < 2) {
			t.Fatalf("page %+v", page)
		}
		for _, item := range page.Items {
			if item.ExecutionAccountID == "flat" {
				t.Fatal("paused leaked", item)
			}
		}
	}
	all, err := f.reader.Orders(ctx, ListFilter{AccountScope: "all"})
	if err != nil || all.Total != 4 {
		t.Fatal(all, err)
	}
	paused, err := f.reader.Orders(ctx, ListFilter{ExecutionAccountID: "flat"})
	if err != nil || paused.Total != 1 {
		t.Fatal(paused, err)
	}
	holdings, err := f.reader.PortfolioHoldings(ctx, ListFilter{AccountScope: "active", Limit: 1})
	if err != nil || holdings.Total != 2 || !holdings.HasMore {
		t.Fatal(holdings, err)
	}
	tx, err := f.reader.beginReadTx(ctx, "scope test")
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	accounts, err := f.reader.selectedManagedAccountsInTx(ctx, tx, "", "active")
	if err != nil || len(accounts) != 2 {
		t.Fatal(accounts, err)
	}
	accounts, err = f.reader.selectedManagedAccountsInTx(ctx, tx, "flat", "")
	if err != nil || len(accounts) != 1 || accounts[0].Mode != "PAUSED" {
		t.Fatal(accounts, err)
	}
}
