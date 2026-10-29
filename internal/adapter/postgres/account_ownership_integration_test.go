package postgres

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/fillprocessor"
)

// TestLotOwnershipIsTheAccountPostgresIntegration proves that a lot opened by
// one model and strategy is reserved, sold and closed by an order that carries
// another model and strategy on the same account, and that another account
// still can never touch it.
func TestLotOwnershipIsTheAccountPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	const accountID, tokenID = "account-ownership", "token-ownership"
	insertAccount(t, db, accountID, "0xownership", "100", "100", "0")
	insertAccount(t, db, "account-intruder", "0xintruder", "100", "100", "0")
	repository, err := NewOrderRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	reservations, err := NewReservationManager(ReservationManagerParams{DB: db, MaxBuyFeeRateBPS: "0"})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := NewFillLedger(FillLedgerParams{DB: db, DustSharesThreshold: "0.01", DustNotionalThreshold: "1"})
	if err != nil {
		t.Fatal(err)
	}
	processor, err := fillprocessor.New(fillprocessor.Params{Orders: repository, Source: noFillsSource{}, Ledger: ledger})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	buy := integrationOrder("ownership-buy", accountID, tokenID, domain.SideBuy, "10", "0.6")
	buy.Intent.Venue = "paper"
	buy.Intent.ModelID = "deepseek_masked"
	buy.Intent.StrategyID = domain.StrategyIDMultfactorV1
	buy = createAcknowledgedIntegrationOrder(t, ctx, repository, reservations, buy, "0xbuy-ownership", now)
	buyConfirmedAt := now.Add(11 * time.Second)
	if _, err := processor.Process(ctx, buy, domain.Fill{
		VenueFillID: "trade-ownership-buy", LiquidityRole: domain.LiquidityRoleMaker,
		Status: domain.FillStatusConfirmed, Shares: "10", Price: "0.5",
		GrossNotional: "5", FeeRateBPS: "0", PlatformFeeRate: "0", FeeExponent: "0",
		PlatformFee: "0", BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0",
		FeeSource: "TEST_AUTHORITATIVE_SETTLEMENT",
		MatchedAt: now.Add(10 * time.Second), VenueUpdatedAt: buyConfirmedAt,
		ObservedAt: buyConfirmedAt, ConfirmedAt: &buyConfirmedAt,
	}); err != nil {
		t.Fatal(err)
	}
	lots, err := ledger.ListLots(ctx, accountID, tokenID)
	if err != nil || len(lots) != 1 || lots[0].ModelID != "deepseek_masked" || lots[0].StrategyID != domain.StrategyIDMultfactorV1 {
		t.Fatalf("opened lot = %#v err=%v", lots, err)
	}
	lotID := lots[0].LotID

	// The account now runs another model and another strategy.
	open, err := ledger.ListOpenLots(ctx, domain.StrategyExecutionContext{
		ExecutionAccountID: accountID, ModelID: "v4.1 flash", StrategyID: "brier_mid_edge_once",
	})
	if err != nil || len(open) != 1 || open[0].LotID != lotID {
		t.Fatalf("lots visible to the current strategy = %#v err=%v", open, err)
	}

	intruder := integrationOrder("ownership-intruder", "account-intruder", tokenID, domain.SideSell, "4", "0.3")
	intruder.Intent.Venue = "paper"
	intruder.Intent.TargetLotID = lotID
	intruder.Status, intruder.CreatedAt, intruder.UpdatedAt, intruder.Revision = domain.OrderStatusReceived, now, now, 1
	storedIntruder, created, err := repository.Create(ctx, intruder)
	if err != nil || !created {
		t.Fatalf("create intruder order: created=%v err=%v", created, err)
	}
	storedIntruder, event := applyIntegrationTransition(t, storedIntruder, domain.OrderStatusValidating, domain.TransitionTriggerValidation, now, "")
	if err := repository.Transition(ctx, storedIntruder, event); err != nil {
		t.Fatal(err)
	}
	if _, err := reservations.Reserve(ctx, storedIntruder); err == nil {
		t.Fatal("another account reserved a lot it does not own")
	}

	sell := integrationOrder("ownership-sell", accountID, tokenID, domain.SideSell, "4", "0.3")
	sell.Intent.Venue = "paper"
	sell.Intent.ModelID = "v4.1 flash"
	sell.Intent.StrategyID = "brier_mid_edge_once"
	sell.Intent.TargetLotID = lotID
	sell.Intent.TimeInForce = domain.TimeInForceFAK
	sell = createAcknowledgedIntegrationOrder(t, ctx, repository, reservations, sell, "0xsell-ownership", now.Add(20*time.Second))
	sellConfirmedAt := now.Add(31 * time.Second)
	sold, err := processor.Process(ctx, sell, domain.Fill{
		VenueFillID: "trade-ownership-sell", LiquidityRole: domain.LiquidityRoleMaker,
		Status: domain.FillStatusConfirmed, Shares: "4", Price: "0.7",
		GrossNotional: "2.8", FeeRateBPS: "0", PlatformFeeRate: "0", FeeExponent: "0",
		PlatformFee: "0", BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0",
		FeeSource: "TEST_AUTHORITATIVE_SETTLEMENT",
		MatchedAt: now.Add(30 * time.Second), VenueUpdatedAt: sellConfirmedAt,
		ObservedAt: sellConfirmedAt, ConfirmedAt: &sellConfirmedAt,
	})
	if err != nil {
		t.Fatalf("sell a lot opened by another model and strategy: %v", err)
	}
	if sold.Order.Status != domain.OrderStatusFilled || sold.Position == nil || !sold.Position.TotalShares.Equal("6") {
		t.Fatalf("SELL application = %#v", sold)
	}
	lots, err = ledger.ListLots(ctx, accountID, tokenID)
	if err != nil || len(lots) != 1 || !lots[0].RemainingShares.Equal("6") || lots[0].Status != domain.PositionLotOpen ||
		lots[0].ModelID != "deepseek_masked" || lots[0].StrategyID != domain.StrategyIDMultfactorV1 {
		t.Fatalf("lot after SELL = %#v err=%v, want 6 shares left and its opening identity kept", lots, err)
	}
}

// TestLiveBindingAuthorizesStrategyNotModelPostgresIntegration covers the Go
// live-risk check and the durable submit trigger (migration 0041) together:
// an order under any model passes when its (strategy, account) binding is
// enabled, and fails closed otherwise.
func TestLiveBindingAuthorizesStrategyNotModelPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	ctx := context.Background()
	for _, test := range []struct {
		name        string
		modelID     string
		strategyID  string
		afterRes    string
		reserveCode string
		submitCode  string
	}{
		{name: "same-model", modelID: "model-a", strategyID: "strategy-v1"},
		{name: "new-model-name", modelID: "v4.1 flash", strategyID: "strategy-v1"},
		{name: "another-new-model", modelID: "kimi-k2", strategyID: domain.StrategyIDMultfactorV1},
		{name: "unbound-strategy", modelID: "model-a", strategyID: "brier_mid_edge_once", reserveCode: "STRATEGY_ACCOUNT_BINDING_DENIED"},
		{name: "binding-disabled-after-reserve", modelID: "v4.1 flash", strategyID: "strategy-v1",
			afterRes: "disable", submitCode: "STRATEGY_ACCOUNT_BINDING_DENIED"},
		{name: "binding-moved-to-other-strategy", modelID: "v4.1 flash", strategyID: "strategy-v1",
			afterRes: "move", submitCode: "STRATEGY_ACCOUNT_BINDING_DENIED"},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Microsecond)
			accountID, tokenID := "account-auth-"+test.name, "token-auth-"+test.name
			insertAccount(t, db, accountID, "0xauth-"+test.name, "100", "100", "0")
			provisionLiveRisk(t, db, liveRiskFixture{accountID: accountID, now: now, binding: true,
				maxOrder: "100", maxMarket: "100", maxStrategy: "100", maxWallet: "100", maxDaily: "100"})
			if _, err := db.Exec(`UPDATE execution_risk_global_control SET kill_switch=FALSE, version=version+1 WHERE singleton=TRUE`); err != nil {
				t.Fatal(err)
			}
			order := liveIntegrationOrder(test.name, accountID, tokenID, "10", "0.5", now)
			order.Intent.ModelID = test.modelID
			order.Intent.StrategyID = test.strategyID
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
			switch test.afterRes {
			case "disable":
				_, err = db.Exec(`UPDATE execution_strategy_bindings SET enabled=FALSE, version=version+1 WHERE execution_account_id=$1`, accountID)
			case "move":
				_, err = db.Exec(`UPDATE execution_strategy_bindings SET strategy_id='brier_mid_edge_once', version=version+1 WHERE execution_account_id=$1`, accountID)
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
				return
			}
			if err != nil {
				t.Fatalf("database submit trigger rejected an order whose strategy is bound: %v", err)
			}
		})
	}
}
