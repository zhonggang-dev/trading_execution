package consolev2

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
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

// 订单列表按下单时间倒序，显式 from/to 也按下单时间；默认可见范围仍按最近变化，
// 早下单、近期才更新的订单不被隐藏，但排在后面。
func TestOrdersSortAndFilterByCreatedAt(t *testing.T) {
	f := prepareCoreFacts(t)
	f.exec(t, `DELETE FROM execution_orders`)
	day := 24 * time.Hour
	for _, o := range []struct {
		id               string
		created, updated time.Time
	}{
		{"old-created-recently-updated", f.now.Add(-20 * day), f.now.Add(-time.Hour)},
		{"newest-created", f.now.Add(-2 * day), f.now.Add(-2 * day)},
		{"middle-created", f.now.Add(-5 * day), f.now.Add(-4 * day)},
		{"created-long-ago-updated-recently", f.now.Add(-60 * day), f.now.Add(-time.Minute)},
	} {
		f.exec(t, `INSERT INTO execution_orders(order_id,client_order_id,execution_account_id,venue,market_id,token_id,intent,status,filled_size,created_at,updated_at) VALUES($1,$1,'pm','polymarket','m','t','{}','FILLED',0,$2,$3)`, o.id, o.created, o.updated)
	}
	ctx := context.Background()
	page, err := f.reader.Orders(ctx, ListFilter{ExecutionAccountID: "pm", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, item := range page.Items {
		got = append(got, item.OrderID)
	}
	want := "newest-created,middle-created,old-created-recently-updated,created-long-ago-updated-recently"
	if strings.Join(got, ",") != want || page.Total != 4 {
		t.Fatalf("default order %v total=%d", got, page.Total)
	}
	from, to := f.now.Add(-6*day), f.now.Add(-3*day)
	page, err = f.reader.Orders(ctx, ListFilter{ExecutionAccountID: "pm", Limit: 10, From: &from, To: &to})
	if err != nil || page.Total != 1 || len(page.Items) != 1 || page.Items[0].OrderID != "middle-created" {
		t.Fatalf("created_at range %+v %v", page, err)
	}
}
