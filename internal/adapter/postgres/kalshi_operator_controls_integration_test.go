package postgres

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// TestKalshiOperatorControlsPostgresIntegration verifies that a Kalshi order is
// held by the kill switch and the account, strategy and market pauses, both
// when it reserves and when it enters SUBMITTING, while the Polymarket gates
// it has no data for (risk policy, binding, reconciliation) stay out of its way.
func TestKalshiOperatorControlsPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	ctx := context.Background()
	for _, test := range []struct {
		name string
		side domain.Side
		// before is applied before Reserve, after between Reserve and SUBMITTING.
		before, after string
		reserveCode   string
		submitCode    string
	}{
		{name: "buy-without-control-rows", side: domain.SideBuy},
		{name: "buy-with-unpaused-account-row", side: domain.SideBuy, before: "account-armed"},
		{name: "buy-kill-switch", side: domain.SideBuy, before: "kill", reserveCode: "GLOBAL_KILL_SWITCH"},
		{name: "sell-kill-switch", side: domain.SideSell, before: "kill", reserveCode: "GLOBAL_KILL_SWITCH"},
		{name: "buy-account-paused", side: domain.SideBuy, before: "account", reserveCode: "EXECUTION_ACCOUNT_PAUSED"},
		{name: "buy-strategy-paused", side: domain.SideBuy, before: "strategy", reserveCode: "STRATEGY_PAUSED"},
		{name: "buy-market-paused", side: domain.SideBuy, before: "market", reserveCode: "MARKET_RISK_PAUSED"},
		{name: "kill-after-reserve", side: domain.SideBuy, after: "kill", submitCode: "GLOBAL_KILL_SWITCH"},
		{name: "account-paused-after-reserve", side: domain.SideSell, after: "account", submitCode: "EXECUTION_ACCOUNT_PAUSED"},
		{name: "market-paused-after-reserve", side: domain.SideBuy, after: "market", submitCode: "STRATEGY_OR_MARKET_PAUSED"},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Microsecond)
			ticker := "KX-" + test.name
			accountID, tokenID := "kalshi:"+test.name, "kalshi:"+ticker+":YES"
			insertAccount(t, db, accountID, "kalshi:key-"+test.name, "100", "100", "0")
			if _, err := db.Exec(`UPDATE execution_risk_global_control SET kill_switch=FALSE, version=version+1 WHERE singleton=TRUE`); err != nil {
				t.Fatal(err)
			}
			order := kalshiIntegrationOrder(test.name, accountID, tokenID, test.side, now)
			if test.side == domain.SideSell {
				if _, err := db.Exec(`INSERT INTO execution_positions (execution_account_id, market_id, token_id, total_shares, available_shares, reserved_shares)
					VALUES ($1,$2,$3,10,10,0)`, accountID, order.Intent.MarketID, tokenID); err != nil {
					t.Fatal(err)
				}
				insertOpenLotFixture(t, db, accountID, tokenID, "10")
				if _, err := db.Exec(`UPDATE position_lots SET market_id=$1 WHERE lot_id=$2`, order.Intent.MarketID, "lot-"+tokenID); err != nil {
					t.Fatal(err)
				}
				order.Intent.TargetLotID = "lot-" + tokenID
			}
			applyKalshiControl(t, db, test.before, accountID, order.Intent)

			repository, err := NewOrderRepository(db)
			if err != nil {
				t.Fatal(err)
			}
			order.Status, order.CreatedAt, order.UpdatedAt, order.Revision = domain.OrderStatusReceived, now, now, 1
			stored, created, err := repository.Create(ctx, order)
			if err != nil || !created {
				t.Fatalf("create order: created=%v err=%v", created, err)
			}
			stored, event := applyIntegrationTransition(t, stored, domain.OrderStatusValidating, domain.TransitionTriggerValidation, now, "")
			if err := repository.Transition(ctx, stored, event); err != nil {
				t.Fatal(err)
			}
			manager, err := NewReservationManager(ReservationManagerParams{DB: db, Now: func() time.Time { return now }, MaxBuyFeeRateBPS: "0"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = manager.Reserve(ctx, stored)
			if test.reserveCode != "" {
				assertRejectionCode(t, err, test.reserveCode)
				assertAccount(t, db, accountID, "100", "100", "0")
				return
			}
			if err != nil {
				t.Fatalf("reserve: %v", err)
			}
			stored, event = applyIntegrationTransition(t, stored, domain.OrderStatusReserved, domain.TransitionTriggerReservation, now, "")
			if err := repository.Transition(ctx, stored, event); err != nil {
				t.Fatal(err)
			}
			applyKalshiControl(t, db, test.after, accountID, order.Intent)

			attempt := domain.OrderAttempt{ID: "attempt-" + test.name, OrderID: stored.ID, Sequence: 1,
				Kind: domain.OrderAttemptSubmit, Outcome: domain.AttemptOutcomeStarted, StartedAt: now}
			stored, event = applyIntegrationTransition(t, stored, domain.OrderStatusSubmitting, domain.TransitionTriggerSubmit, now, attempt.ID)
			err = repository.StartAttempt(ctx, stored, event, attempt)
			if test.submitCode != "" {
				if err == nil || !strings.Contains(err.Error(), test.submitCode) {
					t.Fatalf("submit error = %v, want %s", err, test.submitCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("database submit trigger rejected an unpaused Kalshi order: %v", err)
			}
		})
	}
}

func TestCheckKalshiOperatorControlsMigrationPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	if err := CheckKalshiOperatorControlsMigration(context.Background(), db); err != nil {
		t.Fatalf("migrated schema: %v", err)
	}
	if err := CheckBindingAuthorizationMigration(context.Background(), db); err != nil {
		t.Fatalf("0042 must keep the 0041 binding authorization: %v", err)
	}
	// Re-applying the 0041 trigger body must make startup fail closed.
	previous, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "0041_binding_authorization_without_model.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(previous)); err != nil {
		t.Fatal(err)
	}
	if err := CheckKalshiOperatorControlsMigration(context.Background(), db); err == nil ||
		!strings.Contains(err.Error(), "0042_kalshi_operator_controls.sql is not applied") {
		t.Fatalf("pre-0042 trigger error = %v", err)
	}
}

func kalshiIntegrationOrder(orderID, accountID, tokenID string, side domain.Side, now time.Time) domain.Order {
	order := integrationOrder(orderID, accountID, tokenID, side, "10", "0.5")
	order.Intent.Venue = "kalshi"
	order.Intent.MarketSource = domain.MarketSourceKalshi
	order.Intent.MarketID = "KX-" + orderID
	order.Intent.ConditionID = "kalshi:KX-" + orderID
	order.Intent.OutcomeID = "YES"
	marketAt := now.Add(-time.Second)
	order.Intent.MarketSnapshotAt = &marketAt
	order.Intent.SignalAt = &marketAt
	order.MarketValidation = &domain.MarketValidation{
		Mode: kalshiMarketValidationMode, ValidatedAt: now,
		StrategySnapshotAt: marketAt, LatestBookSourceAt: marketAt, LatestBookObservedAt: marketAt,
		WorstPrice: order.Intent.WorstPrice,
	}
	return order
}

// applyKalshiControl sets one operator control; an empty control changes nothing.
func applyKalshiControl(t *testing.T, db *sql.DB, control, accountID string, intent domain.OrderIntent) {
	t.Helper()
	upsert := func(scope, key string, paused bool) error {
		_, err := db.Exec(`
			INSERT INTO execution_risk_controls (execution_account_id, control_scope, control_key, paused, reason)
			VALUES ($1,$2,$3,$4,'TEST')
			ON CONFLICT (execution_account_id, control_scope, control_key)
			DO UPDATE SET paused=EXCLUDED.paused, version=execution_risk_controls.version+1`,
			accountID, scope, key, paused)
		return err
	}
	var err error
	switch control {
	case "":
	case "kill":
		_, err = db.Exec(`UPDATE execution_risk_global_control SET kill_switch=TRUE, version=version+1 WHERE singleton=TRUE`)
	case "account":
		err = upsert("ACCOUNT", "", true)
	case "account-armed":
		err = upsert("ACCOUNT", "", false)
	case "strategy":
		err = upsert("STRATEGY", domain.CanonicalStrategyID(intent.StrategyID), true)
	case "market":
		err = upsert("MARKET", intent.MarketID, true)
	default:
		t.Fatalf("unknown control %q", control)
	}
	if err != nil {
		t.Fatal(err)
	}
}
