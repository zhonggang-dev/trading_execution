package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

// This regression consumes the current real write model, including both FIFO
// legs, fees, the prefixed account-event fill identity, and partial netting.
func TestPostgresIntegrationConsoleV2KalshiLedger(t *testing.T) {
	f := newKalshiNettingFixture(t, "kalshi:console-kalshi", "1000")
	f.buy("NET", "YES", "10", "0.3", "0.1")
	f.buy("NET", "YES", "8", "0.4", "0.1")
	f.buy("NET", "NO", "14", "0.5", "0.2")
	f.buy("PLAIN", "YES", "2", "0.6", "0.1")
	reader := consolev2.New(f.database, consolev2.RuntimeConfig{Now: func() time.Time { return f.clock }})
	page, err := reader.LedgerActivities(context.Background(), consolev2.ListFilter{ExecutionAccountID: f.account})
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 5 || page.Summary["netting_count"] != 1 {
		t.Fatalf("unexpected counts: %#v", page)
	}
	var netting int
	for _, item := range page.Items {
		if item.ActivityType == "BUY" {
			assertConsoleAmount(t, item.RealizedPnL, "0")
			continue
		}
		if item.ActivityType != "KALSHI_NETTING" {
			t.Fatalf("unexpected type: %s", item.ActivityType)
		}
		netting++
		assertConsoleAmount(t, item.Shares, "14")
		assertConsoleAmount(t, item.NetCashDelta, "14")
		// YES FIFO cost = 3.1 + (3.3 * 4/8) = 4.75; NO = 7.2.
		assertConsoleAmount(t, item.CostBasis, "11.95")
		assertConsoleAmount(t, item.RealizedPnL, "2.05")
		if item.QuantityUnit != "pairs" || item.MarketSource != "KALSHI" || item.Price != "" || item.ConfirmedAt != nil || item.AppliedAt == nil {
			t.Fatalf("invalid netting evidence: %#v", item)
		}
	}
	if netting != 1 {
		t.Fatal("missing netting")
	}
	assertConsoleAmount(t, page.Summary["realized_pnl"].(string), "2.05")
	assertConsoleAmount(t, page.Summary["total_fee"].(string), "0.5")
	assertConsoleAmount(t, page.Summary["net_cash_flow"].(string), "-0.9")
	filtered, err := reader.LedgerActivities(context.Background(), consolev2.ListFilter{ExecutionAccountID: f.account, ActivityType: "KALSHI_NETTING", Limit: 1, Offset: 1})
	if err != nil || filtered.Total != 1 || len(filtered.Items) != 0 || filtered.HasMore {
		t.Fatalf("out of range: %#v %v", filtered, err)
	}
	other, err := reader.LedgerActivities(context.Background(), consolev2.ListFilter{ExecutionAccountID: "another"})
	if err != nil || other.Total != 0 {
		t.Fatalf("account leakage: %#v %v", other, err)
	}
	// Old four-type endpoint remains unchanged; no new enum can leak to 13000.
	legacy, _ := NewTradeHistoryRepository(f.database)
	old, err := legacy.ListLedgerActivities(context.Background(), domain.LedgerActivityFilter{ExecutionAccountID: f.account})
	if err != nil {
		t.Fatal(err)
	}
	if old.Total != 4 {
		t.Fatalf("legacy protocol changed: %#v", old)
	}
	// The reader is read-only: physical facts retain their values after reads.
	assertConsoleAmount(t, scalar(t, f.database, `SELECT sum(total_balance_delta)::text FROM execution_account_events WHERE event_type='KALSHI_NETTED'`), "14")

	// Prefix/order conflicts cannot silently remove the cash event from totals.
	if _, err = f.database.Exec(`UPDATE execution_account_events SET order_id='wrong-order' WHERE event_type='KALSHI_NETTED'`); err != nil {
		t.Fatal(err)
	}
	if _, err = reader.LedgerActivities(context.Background(), consolev2.ListFilter{ExecutionAccountID: f.account}); !errors.Is(err, consolev2.ErrUnavailable) {
		t.Fatalf("invalid evidence accepted: %v", err)
	}
}

func TestPostgresIntegrationConsoleV2KalshiSettlementsAndIssues(t *testing.T) {
	f := newKalshiNettingFixture(t, "kalshi:console-settlements", "1000")
	f.buy("ZERO", "YES", "2", "0.4", "0.1")
	f.buy("WIN", "NO", "3", "0.5", "0.1")
	f.buy("REVIEW", "YES", "1", "0.5", "0")
	store, err := NewKalshiSettlementStore(f.database, []string{f.account})
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range []domain.KalshiSettlement{kalshiRecord("ZERO", "no", "2", "0", "0", "0"), kalshiRecord("WIN", "no", "0", "3", "3", "0"), kalshiRecord("REVIEW", "yes", "5", "0", "5", "1")} {
		if _, err = store.ApplyKalshiSettlement(context.Background(), f.account, record, f.clock); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = f.database.Exec(`INSERT INTO kalshi_balance_drift(execution_account_id,status,local_balance,exchange_balance,difference,observations,first_seen_at,last_seen_at) VALUES($1,'OPEN',100,101,1,2,$2,$2)`, f.account, f.clock.Add(-90*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	condition := "0x" + strings.Repeat("a", 64)
	insertAccount(t, f.database, "console-pm", "0x"+strings.Repeat("b", 40), "10", "10", "0")
	if _, err = f.database.Exec(`INSERT INTO polymarket_redemptions(execution_account_id,condition_id,wallet_address,neg_risk,status) VALUES('console-pm',$1,$2,false,'READY')`, condition, "0x"+strings.Repeat("b", 40)); err != nil {
		t.Fatal(err)
	}
	reader := consolev2.New(f.database, consolev2.RuntimeConfig{Now: func() time.Time { return f.clock }, ManagedAccounts: []consolev2.ManagedAccount{{ExecutionAccountID: f.account, MarketSource: "KALSHI"}, {ExecutionAccountID: "console-pm", MarketSource: "POLYMARKET"}, {ExecutionAccountID: "absent", MarketSource: "POLYMARKET"}}})
	directory, err := reader.Accounts(context.Background())
	if err != nil || len(directory.Accounts) != 2 {
		t.Fatalf("accounts %#v %v", directory, err)
	}
	raw, _ := json.Marshal(directory)
	if strings.Contains(string(raw), "key-") || strings.Contains(string(raw), "wallet") {
		t.Fatal("credential leak")
	}
	progress, err := reader.Settlements(context.Background(), consolev2.ListFilter{Limit: 100})
	if err != nil || progress.Total != 4 {
		t.Fatalf("progress %#v %v", progress, err)
	}
	for _, row := range progress.Items {
		if row.MarketSource == "KALSHI" && (row.ConfirmedAt != nil || row.TransactionHash != "" || row.ConditionID != "") {
			t.Fatalf("fabricated chain stage: %#v", row)
		}
	}
	kalshi, err := reader.Settlements(context.Background(), consolev2.ListFilter{ExecutionAccountID: f.account, MarketSource: "KALSHI", Status: "APPLIED", Limit: 1, Offset: 20})
	if err != nil || kalshi.Total != 2 || len(kalshi.Items) != 0 {
		t.Fatalf("settlement total %#v %v", kalshi, err)
	}
	ledger, err := reader.LedgerActivities(context.Background(), consolev2.ListFilter{ExecutionAccountID: f.account, ActivityType: "KALSHI_SETTLEMENT"})
	if err != nil || ledger.Total != 2 {
		t.Fatalf("ledger %#v %v", ledger, err)
	}
	var zero bool
	for _, row := range ledger.Items {
		if row.MarketID == "ZERO" {
			zero = true
			assertConsoleAmount(t, row.SettlementPayout, "0")
			assertConsoleAmount(t, row.RealizedPnL, "-0.9")
		}
		if row.MarketID == "REVIEW" {
			t.Fatal("manual review credited")
		}
	}
	if !zero {
		t.Fatal("zero payout hidden")
	}
	assertConsoleAmount(t, ledger.Summary["settlement_payout"].(string), "3")
	assertConsoleAmount(t, ledger.Summary["settlement_realized_pnl"].(string), "0.5")
	issues, err := reader.ReconciliationIssues(context.Background(), consolev2.ListFilter{ExecutionAccountID: f.account, Status: "OPEN", Limit: 1})
	if err != nil || issues.Total != 2 || len(issues.Items) != 1 || !issues.HasMore {
		t.Fatalf("issues %#v %v", issues, err)
	}
	issues, err = reader.ReconciliationIssues(context.Background(), consolev2.ListFilter{ExecutionAccountID: f.account, Status: "OPEN", Limit: 1, Offset: 1})
	if err != nil || issues.Total != 2 || len(issues.Items) != 1 || issues.HasMore {
		t.Fatalf("issue second page %#v %v", issues, err)
	}
	if issues.Items[0].RunID != "" {
		t.Fatal("fabricated run")
	}
	for _, expected := range []struct{ query, id string }{
		{"KALSHI_BALANCE_DRIFT", "kalshi-balance-drift:" + f.account},
		{"KALSHI_SETTLEMENT_REVIEW", "kalshi-settlement-review:" + f.account + ":REVIEW"},
	} {
		found, err := reader.ReconciliationIssues(context.Background(), consolev2.ListFilter{ExecutionAccountID: f.account, Status: "OPEN", Query: expected.query})
		if err != nil || found.Total != 1 || len(found.Items) != 1 || found.Items[0].IssueID != expected.id || found.Items[0].Source != "KALSHI" || found.Items[0].RunID != "" {
			t.Fatalf("source-qualified issue filter: %#v %v", found, err)
		}
	}
	otherIssues, err := reader.ReconciliationIssues(context.Background(), consolev2.ListFilter{ExecutionAccountID: "console-pm", Status: "OPEN"})
	if err != nil || otherIssues.Total != 0 {
		t.Fatalf("Kalshi issues leaked to PM account: %#v %v", otherIssues, err)
	}
	// Resolve using the real balance checker; a resolved drift must leave OPEN
	// without removing the independent settlement review or inventing an audit.
	local := domain.Decimal(scalar(t, f.database, `SELECT total_balance::text FROM execution_accounts WHERE execution_account_id=$1`, f.account))
	if _, err := store.CheckKalshiBalance(context.Background(), f.account, local, "0.01", f.clock.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	for status, id := range map[string]string{
		"OPEN":     "kalshi-settlement-review:" + f.account + ":REVIEW",
		"RESOLVED": "kalshi-balance-drift:" + f.account,
	} {
		found, err := reader.ReconciliationIssues(context.Background(), consolev2.ListFilter{ExecutionAccountID: f.account, Status: status})
		if err != nil || found.Total != 1 || len(found.Items) != 1 || found.Items[0].IssueID != id {
			t.Fatalf("issue resolution filter %s: %#v %v", status, found, err)
		}
	}
	runs, err := reader.ReconciliationRuns(context.Background(), consolev2.ListFilter{ExecutionAccountID: f.account})
	if err != nil || runs.Total != 0 {
		t.Fatalf("fabricated run audit %#v %v", runs, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = reader.LedgerActivities(ctx, consolev2.ListFilter{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel lost: %v", err)
	}
}

func assertConsoleAmount(t *testing.T, actual, expected string) {
	t.Helper()
	n, err := domain.Decimal(actual).Compare(domain.Decimal(expected))
	if err != nil || n != 0 {
		t.Fatalf("amount %q want %q (%v)", actual, expected, err)
	}
}

func TestPostgresIntegrationConsoleV2FullNettingThenSell(t *testing.T) {
	f := newKalshiNettingFixture(t, "kalshi:console-full", "1000")
	f.buy("FULL", "YES", "4", "0.3", "0.1")
	f.buy("FULL", "NO", "4", "0.4", "0.1")
	f.buy("REMAINDER", "YES", "6", "0.3", "0.1")
	f.buy("REMAINDER", "NO", "2", "0.4", "0.1")
	lot := scalar(t, f.database, `SELECT lot_id FROM position_lots WHERE execution_account_id=$1 AND market_id='REMAINDER' AND remaining_shares>0`, f.account)
	f.sell("REMAINDER", "YES", lot, "2", "0.6", "0.1")
	reader := consolev2.New(f.database, consolev2.RuntimeConfig{Now: func() time.Time { return f.clock }})
	page, err := reader.LedgerActivities(context.Background(), consolev2.ListFilter{ExecutionAccountID: f.account, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 7 || page.Summary["netting_count"] != 2 {
		t.Fatalf("full/partial count: %#v", page)
	}
	realized := scalar(t, f.database, `SELECT sum(realized_pnl)::text FROM execution_positions WHERE execution_account_id=$1`, f.account)
	assertConsoleAmount(t, page.Summary["realized_pnl"].(string), realized)
	for offset := 0; offset < page.Total+1; offset++ {
		one, err := reader.LedgerActivities(context.Background(), consolev2.ListFilter{ExecutionAccountID: f.account, Limit: 1, Offset: offset})
		if err != nil || one.Total != page.Total {
			t.Fatalf("page %d %#v %v", offset, one, err)
		}
		if offset < page.Total && (len(one.Items) != 1 || one.Items[0].ActivityKey != page.Items[offset].ActivityKey) {
			t.Fatalf("unstable page %d", offset)
		}
	}
}

func TestPostgresIntegrationConsoleV2LedgerAllHistory(t *testing.T) {
	f := newKalshiNettingFixture(t, "kalshi:console-history", "1000")
	f.clock = f.clock.Add(-800 * 24 * time.Hour)
	f.buy("HISTORICAL", "YES", "2", "0.3", "0")
	f.buy("HISTORICAL", "NO", "1", "0.4", "0")
	reader := consolev2.New(f.database, consolev2.RuntimeConfig{Now: time.Now})
	all, err := reader.LedgerActivities(context.Background(), consolev2.ListFilter{ExecutionAccountID: f.account})
	if err != nil || all.Total != 3 || all.Summary["netting_count"] != 1 {
		t.Fatalf("all-history silently bounded: %#v %v", all, err)
	}
	recentFrom := time.Now().Add(-24 * time.Hour)
	recent, err := reader.LedgerActivities(context.Background(), consolev2.ListFilter{ExecutionAccountID: f.account, From: &recentFrom})
	if err != nil || recent.Total != 0 {
		t.Fatalf("explicit recent range ignored: %#v %v", recent, err)
	}
	to := f.clock
	before, err := reader.LedgerActivities(context.Background(), consolev2.ListFilter{ExecutionAccountID: f.account, To: &to})
	if err != nil || before.Total != 3 {
		t.Fatalf("to-only history incorrectly bounded: %#v %v", before, err)
	}
}

func TestPostgresIntegrationConsoleV2RejectsInvalidNettingEvidence(t *testing.T) {
	for name, mutation := range map[string]string{
		"unprefixed fill":          `UPDATE execution_account_events SET fill_key=substring(fill_key FROM length('kalshi-netting:')+1) WHERE event_type='KALSHI_NETTED'`,
		"different event identity": `UPDATE execution_account_events SET account_event_id='unrelated-event' WHERE event_type='KALSHI_NETTED'`,
		"duplicate event":          `INSERT INTO execution_account_events SELECT account_event_id||':duplicate',execution_account_id,event_type,order_id,fill_key,total_balance_delta,available_balance_delta,reserved_balance_delta,total_balance_after,available_balance_after,reserved_balance_after,occurred_at FROM execution_account_events WHERE event_type='KALSHI_NETTED'`,
		"different account":        `UPDATE execution_account_events SET execution_account_id='other-account' WHERE event_type='KALSHI_NETTED'`,
		"payout mismatch":          `UPDATE execution_account_events SET total_balance_delta=total_balance_delta+1 WHERE event_type='KALSHI_NETTED'`,
		"one leg mismatch":         `UPDATE position_lot_closures SET closed_shares=closed_shares+1 WHERE lot_id IN (SELECT lot_id FROM position_lots WHERE token_id='kalshi:IDENTITY:YES')`,
	} {
		t.Run(name, func(t *testing.T) {
			f := newKalshiNettingFixture(t, "kalshi:console-evidence", "1000")
			f.buy("IDENTITY", "YES", "3", "0.3", "0.1")
			f.buy("IDENTITY", "NO", "2", "0.4", "0.1")
			insertAccount(t, f.database, "other-account", "other-wallet", "10", "10", "0")
			if _, err := f.database.Exec(mutation); err != nil {
				t.Fatal(err)
			}
			reader := consolev2.New(f.database, consolev2.RuntimeConfig{})
			if _, err := reader.LedgerActivities(context.Background(), consolev2.ListFilter{}); !errors.Is(err, consolev2.ErrUnavailable) {
				t.Fatalf("inconsistent cash event silently accepted: %v", err)
			}
			// Errors stay within the requested account/time scope. Filtering to
			// plain BUY facts remains possible for investigating the bad event.
			before := f.clock.Add(-24 * time.Hour)
			for _, filter := range []consolev2.ListFilter{
				{ExecutionAccountID: "absent-account"},
				{ActivityType: "BUY"},
				{To: &before},
			} {
				if _, err := reader.LedgerActivities(context.Background(), filter); err != nil {
					t.Fatalf("unrelated scope unavailable: %#v %v", filter, err)
				}
			}
		})
	}
}
