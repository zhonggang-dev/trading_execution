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

func TestExecutionAccountQuarantineCheckerPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	insertAccount(t, db, "wallet-quarantined", "0xquarantined", "10", "10", "0")
	if _, err := db.Exec(`
		INSERT INTO execution_risk_controls (
			execution_account_id, control_scope, control_key, paused, reason
		) VALUES ('wallet-quarantined','ACCOUNT','',TRUE,'TEST_QUARANTINE')`); err != nil {
		t.Fatal(err)
	}
	checker, err := NewExecutionAccountQuarantineChecker(db, []string{"wallet-quarantined"})
	if err != nil {
		t.Fatal(err)
	}
	if err := checker.Check(context.Background()); err != nil {
		t.Fatalf("paused quarantine check: %v", err)
	}

	if _, err := db.Exec(`
		UPDATE execution_risk_controls
		SET paused=FALSE, reason='TEST_UNPAUSED', version=version+1
		WHERE execution_account_id='wallet-quarantined'
		  AND control_scope='ACCOUNT' AND control_key=''`); err != nil {
		t.Fatal(err)
	}
	if err := checker.Check(context.Background()); err == nil || !strings.Contains(err.Error(), "is not paused") {
		t.Fatalf("unpaused quarantine check error = %v", err)
	}
}

func TestLiveActiveAccountAuthorizationPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	const accountID = "wallet-active"
	insertAccount(t, db, accountID, "0xactive", "10", "10", "0")
	if _, err := db.Exec(`
		INSERT INTO execution_strategy_bindings (
			model_id, strategy_id, execution_account_id, enabled
		) VALUES ('model-active','multfactor_v1',$1,TRUE)`, accountID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_risk_policies (
			execution_account_id, policy_id, enabled,
			max_order_notional, max_market_exposure, max_strategy_exposure,
			max_wallet_exposure, max_daily_traded_notional,
			max_price_age_ms, max_signal_age_ms, max_state_age_ms, daily_timezone
		) VALUES ($1,'policy-active',TRUE,1,2,2,2,1,90000,30000,600000,'UTC')`, accountID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_risk_controls (
			execution_account_id, control_scope, control_key, paused, reason
		) VALUES ($1,'ACCOUNT','',FALSE,'ACTIVE')`, accountID); err != nil {
		t.Fatal(err)
	}
	expected := []ExpectedActiveExecutionAccount{{
		ExecutionAccountID: accountID,
		ModelID:            "model-active",
		StrategyID:         "multfactor_v1",
	}}
	if err := CheckLiveActiveAccountAuthorization(context.Background(), db, expected); err != nil {
		t.Fatalf("active authorization check: %v", err)
	}

	for _, test := range []struct {
		name string
		sql  string
		want string
	}{
		{name: "binding disabled", sql: `UPDATE execution_strategy_bindings SET enabled=FALSE, version=version+1`, want: "strategy binding is disabled"},
		{name: "policy disabled", sql: `UPDATE execution_risk_policies SET enabled=FALSE, version=version+1`, want: "risk policy is disabled"},
		{name: "account paused", sql: `UPDATE execution_risk_controls SET paused=TRUE, version=version+1`, want: "ACCOUNT risk control is paused"},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, reset := range []string{
				`UPDATE execution_strategy_bindings SET enabled=TRUE, version=version+1`,
				`UPDATE execution_risk_policies SET enabled=TRUE, version=version+1`,
				`UPDATE execution_risk_controls SET paused=FALSE, version=version+1`,
			} {
				if _, err := db.Exec(reset); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := db.Exec(test.sql); err != nil {
				t.Fatal(err)
			}
			err := CheckLiveActiveAccountAuthorization(context.Background(), db, expected)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("authorization error = %v, want %q", err, test.want)
			}
		})
	}
	for _, reset := range []string{
		`UPDATE execution_strategy_bindings SET enabled=TRUE, version=version+1`,
		`UPDATE execution_risk_policies SET enabled=TRUE, version=version+1`,
		`UPDATE execution_risk_controls SET paused=FALSE, version=version+1`,
	} {
		if _, err := db.Exec(reset); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("unexpected enabled route on active account", func(t *testing.T) {
		if _, err := db.Exec(`
			INSERT INTO execution_strategy_bindings (
				model_id, strategy_id, execution_account_id, enabled
			) VALUES ('model-unexpected','multfactor_v2',$1,TRUE)`, accountID); err != nil {
			t.Fatal(err)
		}
		err := CheckLiveActiveAccountAuthorization(context.Background(), db, expected)
		if err == nil || !strings.Contains(err.Error(), "globally enabled strategy binding set differs") {
			t.Fatalf("unexpected active-account route error = %v", err)
		}
		if _, err := db.Exec(`
			DELETE FROM execution_strategy_bindings
			WHERE model_id='model-unexpected' AND strategy_id='multfactor_v2'
			  AND execution_account_id=$1`, accountID); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("unexpected whitespace variant route", func(t *testing.T) {
		if _, err := db.Exec(`
			INSERT INTO execution_strategy_bindings (
				model_id, strategy_id, execution_account_id, enabled
			) VALUES ('model-active ','multfactor_v1',$1,TRUE)`, accountID); err != nil {
			t.Fatal(err)
		}
		err := CheckLiveActiveAccountAuthorization(context.Background(), db, expected)
		if err == nil || !strings.Contains(err.Error(), "globally enabled strategy binding set differs") {
			t.Fatalf("unexpected whitespace-variant route error = %v", err)
		}
		if _, err := db.Exec(`
			DELETE FROM execution_strategy_bindings
			WHERE model_id='model-active ' AND strategy_id='multfactor_v1'
			  AND execution_account_id=$1`, accountID); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("unexpected enabled route on retired account", func(t *testing.T) {
		const retiredAccountID = "wallet-retired"
		insertAccount(t, db, retiredAccountID, "0xretired", "0", "0", "0")
		if _, err := db.Exec(`
			INSERT INTO execution_strategy_bindings (
				model_id, strategy_id, execution_account_id, enabled
			) VALUES ('model-retired','multfactor_v2',$1,TRUE)`, retiredAccountID); err != nil {
			t.Fatal(err)
		}
		err := CheckLiveActiveAccountAuthorization(context.Background(), db, expected)
		if err == nil || !strings.Contains(err.Error(), "globally enabled strategy binding set differs") {
			t.Fatalf("unexpected retired-account route error = %v", err)
		}
	})
}
