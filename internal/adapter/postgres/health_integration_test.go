package postgres

import (
	"context"
	"os"
	"strings"
	"testing"
)

// TestHealthCheckerPostgresIntegration proves readiness covers the durable
// live-risk artifacts, including a trigger that exists but was disabled by an
// operator.  The shared helper installs every migration in an isolated schema.
func TestHealthCheckerPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	checker, err := NewHealthChecker(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := checker.Check(context.Background()); err != nil {
		t.Fatalf("complete schema readiness: %v", err)
	}

	if _, err := db.Exec(`ALTER TABLE execution_orders DISABLE TRIGGER execution_orders_live_submit_risk_trigger`); err != nil {
		t.Fatal(err)
	}
	err = checker.Check(context.Background())
	if err == nil || !strings.Contains(err.Error(), "required triggers") {
		t.Fatalf("disabled live submit trigger readiness error = %v", err)
	}
}

func TestLiveLedgerBootstrapPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	account := ExpectedExecutionAccount{ExecutionAccountID: "account-live-bootstrap", WalletAddress: "0xbootstrap"}
	insertAccount(t, db, account.ExecutionAccountID, account.WalletAddress, "10", "10", "0")
	if err := CheckLiveLedgerBootstrap(context.Background(), db, []ExpectedExecutionAccount{account}); err != nil {
		t.Fatalf("clean live ledger bootstrap: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_positions (
			execution_account_id, market_id, token_id,
			total_shares, available_shares, reserved_shares, cost_basis
		) VALUES ($1,'market-bootstrap','token-bootstrap',1,1,0,0.5)`, account.ExecutionAccountID); err != nil {
		t.Fatal(err)
	}
	err := CheckLiveLedgerBootstrap(context.Background(), db, []ExpectedExecutionAccount{account})
	if err == nil || !strings.Contains(err.Error(), "position/lot mismatches=1") {
		t.Fatalf("inconsistent live ledger bootstrap error = %v", err)
	}
}
