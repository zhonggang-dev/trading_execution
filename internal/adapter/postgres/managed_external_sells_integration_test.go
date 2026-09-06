package postgres

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestManagedExternalSellAtomicRepair(t *testing.T) {
	url := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("disposable database required")
	}
	db := newIntegrationDatabase(t, url)
	wallet := "0x" + strings.Repeat("1", 40)
	condition := "0x" + strings.Repeat("2", 64)
	order := "0x" + strings.Repeat("3", 64)
	hash := "0x" + strings.Repeat("4", 64)
	insertAccount(t, db, "external-test", wallet, "100", "100", "0")
	for _, lot := range []string{"lot-a", "lot-b", "lot-c"} {
		insertOpenLotFixtureNamed(t, db, "external-test", "123", lot, "1.001")
	}
	for _, statement := range []string{
		`UPDATE execution_accounts SET collateral_asset='pUSD' WHERE execution_account_id='external-test'`,
		`UPDATE position_lots SET opened_at=now()-interval '2 hours' WHERE execution_account_id='external-test'`,
		`INSERT INTO execution_risk_controls(execution_account_id,control_scope,paused) VALUES('external-test','ACCOUNT',true)`,
		`INSERT INTO execution_positions(execution_account_id,market_id,condition_id,token_id,total_shares,available_shares,reserved_shares,cost_basis,average_cost_price) VALUES('external-test','market-1','` + condition + `','123',3.003,3.003,0,1.5015,0.5)`,
		`INSERT INTO reconciliation_runs(run_id,execution_account_id,trigger,status,started_at,completed_at) VALUES('external-run','external-test','SCHEDULED','ATTENTION_REQUIRED',now(),now())`,
		`INSERT INTO reconciliation_issues(issue_id,run_id,fingerprint,execution_account_id,issue_type,resolution,status,venue_trade_id,venue_order_id,condition_id,token_id,observed_at) VALUES('external-issue','external-run','external','external-test','EXTERNAL_TRADE','MANUAL_REVIEW','OPEN','venue-trade','` + order + `','` + condition + `','123',now())`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	m := map[string]any{"account": "external-test", "wallet": wallet, "cash": "101.79", "observed_at": time.Now().UTC(),
		"trades": []any{map[string]any{"venue_trade_id": "venue-trade", "condition_id": condition, "matched_at": time.Now().Add(-time.Hour).UTC(), "remaining_shares": "0.003",
			"evidence": map[string]any{"Side": 1, "Maker": wallet, "TokenID": "123", "TransactionHash": hash, "OrderHash": order, "LogIndex": 1, "BlockNumber": 100, "BlockHash": hash, "Confirmations": 128, "MakerAmountBaseUnits": "3000000", "TakerAmountBaseUnits": "1800000", "FeeBaseUnits": "10000"}}}}
	batch := strings.Repeat("a", 64)
	call := func(payload map[string]any, commit bool) (json.RawMessage, error) {
		data, _ := json.Marshal(payload)
		tx, err := db.Begin()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		var result json.RawMessage
		err = tx.QueryRow(`SELECT apply_managed_external_sells($1,$2,'test-operator','explicitly approved external sell')`, data, batch).Scan(&result)
		if err == nil && commit {
			err = tx.Commit()
		}
		return result, err
	}
	// Cash mismatch must roll back even though it is checked after lot updates.
	m["cash"] = "101.8"
	if _, err := call(m, true); err == nil {
		t.Fatal("accepted inconsistent cash")
	}
	assertAccount(t, db, "external-test", "100", "100", "0")
	m["cash"] = "101.79"
	// A complete rehearsal must leave every accounting table untouched.
	if _, err := call(m, false); err != nil {
		t.Fatal(err)
	}
	assertAccount(t, db, "external-test", "100", "100", "0")
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM managed_external_sells`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("dry run persisted rows: %d %v", count, err)
	}
	if _, err := db.Exec(`UPDATE execution_risk_controls SET paused=false,version=version+1 WHERE execution_account_id='external-test'`); err != nil {
		t.Fatal(err)
	}
	if _, err := call(m, true); err == nil {
		t.Fatal("accepted unpaused account")
	}
	if _, err := db.Exec(`UPDATE execution_risk_controls SET paused=true,version=version+1 WHERE execution_account_id='external-test'`); err != nil {
		t.Fatal(err)
	}
	if _, err := call(m, true); err != nil {
		t.Fatal(err)
	}
	assertAccount(t, db, "external-test", "101.79", "101.79", "0")
	assertPosition(t, db, "external-test", "123", "0.003", "0.003", "0")
	if err := db.QueryRow(`SELECT count(*) FROM position_lots WHERE execution_account_id='external-test' AND remaining_shares=0.001 AND remaining_cost=0.0005`).Scan(&count); err != nil || count != 3 {
		t.Fatalf("proportional allocation %d %v", count, err)
	}
	if _, err := call(m, true); err != nil {
		t.Fatal("idempotent replay:", err)
	}
	assertAccount(t, db, "external-test", "101.79", "101.79", "0")
	batch = strings.Repeat("b", 64)
	if _, err := call(m, true); err == nil {
		t.Fatal("different batch replay duplicated a chain event")
	}
	assertAccount(t, db, "external-test", "101.79", "101.79", "0")
	if _, err := db.Exec(`UPDATE managed_external_sells SET net_cash=0`); err == nil {
		t.Fatal("audit can be rewritten")
	}
	repo, _ := NewExternalPositionBaselineRepository(db)
	trades, err := repo.ListExternalPositionDispositionTrades(context.Background(), "external-test")
	if err != nil || len(trades) != 1 {
		t.Fatalf("disposition: %v %v", trades, err)
	}
	history, _ := NewTradeHistoryRepository(db)
	page, err := history.ListLedgerActivities(context.Background(), domain.LedgerActivityFilter{ExecutionAccountID: "external-test", ActivityType: domain.LedgerActivityExternalSell})
	if err != nil || len(page.Items) != 1 || page.Items[0].OrderID != "" || !sameNumeric(page.Summary.NetCashFlow.String(), "1.79") {
		t.Fatalf("explicit external history: %#v %v", page, err)
	}
}
