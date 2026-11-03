package consolev2

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"
)

// Temporary tables are isolated to this connection. A large closed history
// must not change the current asset total or consume the HTTP read budget.
func TestPortfolioHoldingsHistoryBudgetAndPoolCancellation(t *testing.T) {
	dsn := os.Getenv("TRADING_EXECUTION_CONSOLE_V2_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TRADING_EXECUTION_CONSOLE_V2_TEST_DATABASE_URL is not configured")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	setup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, statement := range []string{
		`CREATE TEMP TABLE execution_orders(order_id text,execution_account_id text,market_id text,token_id text,intent jsonb,venue text)`,
		`CREATE TEMP TABLE position_lots(lot_id text,execution_account_id text,market_id text,condition_id text,token_id text,model_id text,strategy_id text,opening_order_id text,opening_fill_key text,external_adoption_id text,outcome_index integer,outcome_name text,status text,remaining_shares numeric,remaining_cost numeric,original_shares numeric,original_cost numeric,average_entry_price numeric,opened_at timestamptz,closed_at timestamptz)`,
		`INSERT INTO execution_orders VALUES('budget-order','budget-account','budget-market','budget-token','{"market_source":"POLYMARKET","condition_id":"budget-condition","metadata":{"market_question":"Budget question"}}','polymarket')`,
		`INSERT INTO position_lots SELECT 'closed-'||n,'budget-account','budget-market','budget-condition','budget-token','old-model','old-strategy','budget-order','closed-fill-'||n,null,0,'Yes','CLOSED',0,0,1,0.5,0.5,now()-interval '400 days',now() FROM generate_series(1,100000) n`,
		`INSERT INTO position_lots SELECT 'open-'||n,'budget-account','budget-market','budget-condition','budget-token','model-'||n,'strategy-'||n,'budget-order','open-fill-'||n,null,0,'Yes','OPEN',1,0.5,1,0.5,0.5,now(),null FROM generate_series(1,2) n`,
	} {
		if _, err := db.ExecContext(setup, statement); err != nil {
			t.Fatal(err)
		}
	}
	reader := New(db, RuntimeConfig{ManagedAccounts: []ManagedAccount{{ExecutionAccountID: "budget-account", MarketSource: "POLYMARKET"}}})
	read, stopRead := context.WithTimeout(context.Background(), 5*time.Second)
	started := time.Now()
	page, err := reader.PortfolioHoldings(read, ListFilter{Limit: 1})
	stopRead()
	if err != nil || page.Total != 1 || len(page.Items) != 1 || page.Items[0].RemainingShares != "2" || page.Items[0].RemainingCost != "1.0" || page.Items[0].LotCount != 2 {
		t.Fatalf("large-history projection %#v err=%v", page, err)
	}
	t.Logf("100000 closed lots + 2 open lots: %s", time.Since(started))

	// A request cancelled while waiting for the single pool connection cannot
	// retain that connection or prevent the following reader from succeeding.
	connection, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	waiting, cancelWaiting := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, err = reader.PortfolioHoldings(waiting, ListFilter{Limit: 1})
	cancelWaiting()
	if closeErr := connection.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pool wait did not preserve deadline: %v", err)
	}
	retry, stopRetry := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopRetry()
	page, err = reader.PortfolioHoldings(retry, ListFilter{Limit: 1})
	if err != nil || page.Total != 1 || db.Stats().InUse != 0 {
		t.Fatalf("cancelled request retained a connection: page=%#v err=%v stats=%#v", page, err, db.Stats())
	}
}
