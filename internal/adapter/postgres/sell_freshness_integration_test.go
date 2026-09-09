package postgres

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestSellFreshnessExemptionPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	ctx := context.Background()
	for _, test := range []struct {
		name, stale, gate string
		side              domain.Side
		reserveCode       string
		submitCode        string
	}{
		{name: "sell-old-price-and-signal", side: domain.SideSell, stale: "both"},
		{name: "buy-old-price", side: domain.SideBuy, stale: "price", reserveCode: "PRICE_STALE"},
		{name: "buy-old-signal", side: domain.SideBuy, stale: "signal", reserveCode: "SIGNAL_STALE"},
		{name: "buy-price-ages-after-reserve", side: domain.SideBuy, gate: "price", submitCode: "PRICE_STALE"},
		{name: "buy-signal-ages-after-reserve", side: domain.SideBuy, gate: "signal", submitCode: "SIGNAL_STALE"},
		{name: "sell-kill-after-reserve", side: domain.SideSell, stale: "both", gate: "kill", submitCode: "GLOBAL_KILL_SWITCH"},
		{name: "sell-pause-after-reserve", side: domain.SideSell, stale: "both", gate: "pause", submitCode: "EXECUTION_ACCOUNT_PAUSED"},
		{name: "sell-reconciliation-ages", side: domain.SideSell, stale: "both", gate: "state", submitCode: "RISK_STATE_STALE"},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Microsecond)
			old := now.Add(-4 * time.Hour)
			accountID, tokenID := "account-"+test.name, "token-"+test.name
			insertAccount(t, db, accountID, "0x"+test.name, "100", "100", "0")
			provisionLiveRisk(t, db, liveRiskFixture{accountID: accountID, now: now, binding: true,
				maxOrder: "100", maxMarket: "100", maxStrategy: "100", maxWallet: "100", maxDaily: "100"})
			if _, err := db.Exec(`UPDATE execution_risk_global_control SET kill_switch=FALSE, version=version+1 WHERE singleton=TRUE`); err != nil {
				t.Fatal(err)
			}
			order := liveIntegrationOrder(test.name, accountID, tokenID, "10", "0.5", now)
			order.Intent.Side = test.side
			if test.side == domain.SideSell {
				order.Intent.TargetLotID = "lot-" + tokenID
				if _, err := db.Exec(`INSERT INTO execution_positions (execution_account_id, market_id, token_id, total_shares, available_shares, reserved_shares)
					VALUES ($1,'market-1',$2,10,10,0)`, accountID, tokenID); err != nil {
					t.Fatal(err)
				}
				insertOpenLotFixture(t, db, accountID, tokenID, "10")
			}
			if test.stale == "both" || test.stale == "price" {
				order.MarketValidation.LatestBookObservedAt = old
			}
			if test.stale == "both" || test.stale == "signal" {
				order.Intent.SignalAt = &old
			}
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
			switch test.gate {
			case "price":
				stored.MarketValidation.LatestBookObservedAt = old
			case "signal":
				stored.Intent.SignalAt = &old
			case "kill":
				_, err = db.Exec(`UPDATE execution_risk_global_control SET kill_switch=TRUE, version=version+1 WHERE singleton=TRUE`)
			case "pause":
				_, err = db.Exec(`UPDATE execution_risk_controls SET paused=TRUE, version=version+1 WHERE execution_account_id=$1 AND control_scope='ACCOUNT'`, accountID)
			case "state":
				_, err = db.Exec(`UPDATE reconciliation_runs SET started_at=$2, completed_at=$3 WHERE execution_account_id=$1`, accountID, old.Add(-time.Second), old)
			}
			if err != nil {
				t.Fatal(err)
			}
			attempt := domain.OrderAttempt{ID: "attempt-" + test.name, OrderID: stored.ID, Sequence: 1,
				Kind: domain.OrderAttemptSubmit, Outcome: domain.AttemptOutcomeStarted, StartedAt: now}
			stored, event = applyIntegrationTransition(t, stored, domain.OrderStatusSubmitting, domain.TransitionTriggerSubmit, now, attempt.ID)
			err = repository.StartAttempt(ctx, stored, event, attempt)
			if test.submitCode != "" {
				if err == nil || !strings.Contains(err.Error(), test.submitCode) {
					t.Fatalf("submit error = %v, want %s", err, test.submitCode)
				}
			} else if err != nil {
				t.Fatalf("stale SELL did not pass database submit trigger: %v", err)
			}
			if test.side == domain.SideSell {
				assertPosition(t, db, accountID, tokenID, "10", "0", "10")
			}
		})
	}
}
