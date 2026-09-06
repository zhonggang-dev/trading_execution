package postgres

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// Optional operator rehearsal against a private, read-only production snapshot.
// Files must stay OUTSIDE source control; destination is a disposable database
// and newIntegrationDatabase always creates a separate test schema.
func TestManagedExternalSellPrivateSnapshot(t *testing.T) {
	snapshotPath := os.Getenv("EXTERNAL_SELL_SHADOW_SNAPSHOT_FILE")
	evidencePath := os.Getenv("EXTERNAL_SELL_SHADOW_EVIDENCE_FILE")
	url := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if snapshotPath == "" || evidencePath == "" || url == "" {
		t.Skip("private shadow inputs not configured")
	}
	db := newIntegrationDatabase(t, url)
	raw, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot map[string]json.RawMessage
	if err = json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"execution_accounts", "execution_risk_controls", "execution_orders", "execution_fills", "execution_positions", "position_lots", "position_events", "reconciliation_runs", "reconciliation_issues"} {
		if string(snapshot[table]) == "null" {
			continue
		}
		if _, err = db.Exec(`INSERT INTO `+table+` SELECT * FROM jsonb_populate_recordset(NULL::`+table+`,$1::jsonb)`, snapshot[table]); err != nil {
			t.Fatalf("restore %s: %v", table, err)
		}
	}
	evidence, err := os.ReadFile(evidencePath)
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Account string            `json:"account"`
		Cash    string            `json:"cash"`
		Trades  []json.RawMessage `json:"trades"`
	}
	if err = json.Unmarshal(evidence, &m); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE execution_risk_controls SET paused=true,version=version+1 WHERE execution_account_id=$1 AND control_scope='ACCOUNT'`, m.Account); err != nil {
		t.Fatal(err)
	}
	var before string
	if err = db.QueryRow(`SELECT total_balance::text FROM execution_accounts WHERE execution_account_id=$1`, m.Account).Scan(&before); err != nil {
		t.Fatal(err)
	}
	var result json.RawMessage
	for _, commit := range []bool{false, true, true} {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		err = tx.QueryRow(`SELECT apply_managed_external_sells($1,$2,'isolated-shadow-test','approved snapshot rehearsal')`, evidence, strings.Repeat("f", 64)).Scan(&result)
		if err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		if commit {
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
		} else {
			tx.Rollback()
			assertAccount(t, db, m.Account, before, before, "0")
		}
		t.Logf("commit=%v result=%s", commit, result)
	}
	assertAccount(t, db, m.Account, m.Cash, m.Cash, "0")
	var count int
	if err = db.QueryRow(`SELECT count(*) FROM managed_external_sells`).Scan(&count); err != nil || count != len(m.Trades) {
		t.Fatalf("trades %d %v", count, err)
	}
	if err = db.QueryRow(`SELECT count(*) FROM execution_positions p WHERE p.execution_account_id=$1 AND (p.total_shares<>(SELECT coalesce(sum(remaining_shares),0) FROM position_lots l WHERE l.execution_account_id=p.execution_account_id AND l.token_id=p.token_id) OR p.cost_basis<>(SELECT coalesce(sum(remaining_cost),0) FROM position_lots l WHERE l.execution_account_id=p.execution_account_id AND l.token_id=p.token_id))`, m.Account).Scan(&count); err != nil || count != 0 {
		t.Fatalf("position/lot inconsistencies %d %v", count, err)
	}
}
