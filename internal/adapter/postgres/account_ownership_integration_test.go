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

func TestCheckBindingAuthorizationMigrationPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	if err := CheckBindingAuthorizationMigration(context.Background(), db); err != nil {
		t.Fatalf("migrated schema: %v", err)
	}
	// Re-applying the pre-0041 trigger body must make startup fail closed.
	previous, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "0025_sell_exit_freshness_exemption.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(previous)); err != nil {
		t.Fatal(err)
	}
	if err := CheckBindingAuthorizationMigration(context.Background(), db); err == nil ||
		!strings.Contains(err.Error(), "0041_binding_authorization_without_model.sql is not applied") {
		t.Fatalf("pre-0041 trigger error = %v", err)
	}
}

// ownershipFixture is one account holding a 10-share lot opened under the
// pre-cutover identity (deepseek_masked, multfactor_v1).
type ownershipFixture struct {
	db           *sql.DB
	ctx          context.Context
	now          time.Time
	accountID    string
	tokenID      string
	lotID        string
	repository   *OrderRepository
	reservations *ReservationManager
	ledger       *FillLedger
	processor    *fillprocessor.Service
}

func newOwnershipFixture(t *testing.T, databaseURL string) ownershipFixture {
	t.Helper()
	fixture := ownershipFixture{
		db: newIntegrationDatabase(t, databaseURL), ctx: context.Background(),
		now:       time.Now().UTC().Truncate(time.Microsecond),
		accountID: "account-ownership", tokenID: "token-ownership",
	}
	insertAccount(t, fixture.db, fixture.accountID, "0xownership", "100", "100", "0")
	var err error
	if fixture.repository, err = NewOrderRepository(fixture.db); err != nil {
		t.Fatal(err)
	}
	if fixture.reservations, err = NewReservationManager(ReservationManagerParams{DB: fixture.db, MaxBuyFeeRateBPS: "0"}); err != nil {
		t.Fatal(err)
	}
	if fixture.ledger, err = NewFillLedger(FillLedgerParams{DB: fixture.db, DustSharesThreshold: "0.01", DustNotionalThreshold: "1"}); err != nil {
		t.Fatal(err)
	}
	if fixture.processor, err = fillprocessor.New(fillprocessor.Params{Orders: fixture.repository, Source: noFillsSource{}, Ledger: fixture.ledger}); err != nil {
		t.Fatal(err)
	}
	buy := integrationOrder("ownership-buy", fixture.accountID, fixture.tokenID, domain.SideBuy, "10", "0.6")
	buy.Intent.Venue = "paper"
	buy.Intent.ModelID = "deepseek_masked"
	buy.Intent.StrategyID = domain.StrategyIDMultfactorV1
	buy = createAcknowledgedIntegrationOrder(t, fixture.ctx, fixture.repository, fixture.reservations, buy, "0xbuy-ownership", fixture.now)
	if _, err := fixture.processor.Process(fixture.ctx, buy, ownershipFill("trade-ownership-buy", "10", "0.5", "5", fixture.now.Add(10*time.Second))); err != nil {
		t.Fatal(err)
	}
	lots, err := fixture.ledger.ListLots(fixture.ctx, fixture.accountID, fixture.tokenID)
	if err != nil || len(lots) != 1 {
		t.Fatalf("opened lot = %#v err=%v", lots, err)
	}
	fixture.lotID = lots[0].LotID
	return fixture
}

// acknowledgedSell places a FAK SELL on the fixture lot and leaves its
// reservation ACTIVE, as a live order between acknowledgement and finality.
func (fixture ownershipFixture) acknowledgedSell(t *testing.T, orderID, modelID, strategyID, size string) domain.Order {
	t.Helper()
	sell := integrationOrder(orderID, fixture.accountID, fixture.tokenID, domain.SideSell, size, "0.3")
	sell.Intent.Venue = "paper"
	sell.Intent.ModelID = modelID
	sell.Intent.StrategyID = strategyID
	sell.Intent.TargetLotID = fixture.lotID
	sell.Intent.TimeInForce = domain.TimeInForceFAK
	sell = createAcknowledgedIntegrationOrder(t, fixture.ctx, fixture.repository, fixture.reservations, sell, "0x"+orderID, fixture.now.Add(20*time.Second))
	// The paper fixture reserves without a risk receipt, which the live
	// bootstrap counts as a legacy reservation. Give it one so the checks below
	// isolate the SELL identity rule.
	if _, err := fixture.db.Exec(`UPDATE asset_reservations
		SET risk_policy_id='ownership-policy', risk_policy_version=1, risk_day=CURRENT_DATE, daily_risk_notional=1
		WHERE order_id=$1`, sell.ID); err != nil {
		t.Fatal(err)
	}
	return sell
}

func (fixture ownershipFixture) bootstrap() error {
	return CheckLiveLedgerBootstrap(fixture.ctx, fixture.db, []ExpectedExecutionAccount{{ExecutionAccountID: fixture.accountID}})
}

// rollbackGateRows runs the runbook's rollback gate a) verbatim.
func (fixture ownershipFixture) rollbackGateRows(t *testing.T) []string {
	t.Helper()
	runbook, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "account-portfolio-cutover.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(runbook)
	marker := strings.Index(text, "-- rollback-gate: legacy-sell-identity")
	if marker < 0 {
		t.Fatal("runbook has no rollback gate a)")
	}
	end := strings.Index(text[marker:], "```")
	if end < 0 {
		t.Fatal("runbook rollback gate a) is not a closed SQL block")
	}
	rows, err := fixture.db.Query(text[marker : marker+end])
	if err != nil {
		t.Fatalf("run rollback gate: %v", err)
	}
	defer rows.Close()
	var blockers []string
	for rows.Next() {
		var accountID, orderID, reservationStatus, lotID string
		var orderStatus sql.NullString
		if err := rows.Scan(&accountID, &orderID, &reservationStatus, &orderStatus, &lotID); err != nil {
			t.Fatal(err)
		}
		blockers = append(blockers, orderID+"/"+reservationStatus+"/"+orderStatus.String)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return blockers
}

func (fixture ownershipFixture) activeSellReservedShares(t *testing.T) string {
	t.Helper()
	var reserved string
	if err := fixture.db.QueryRow(`SELECT COALESCE(sum(remaining_reserved_shares),0)::text FROM asset_reservations
		WHERE execution_account_id=$1 AND side='SELL' AND status IN ('ACTIVE','RECONCILIATION_REQUIRED')`, fixture.accountID).Scan(&reserved); err != nil {
		t.Fatal(err)
	}
	return reserved
}

func (fixture ownershipFixture) assertHolding(t *testing.T, shares string) {
	t.Helper()
	lots, err := fixture.ledger.ListLots(fixture.ctx, fixture.accountID, fixture.tokenID)
	if err != nil || len(lots) != 1 || !lots[0].RemainingShares.Equal(domain.Decimal(shares)) ||
		lots[0].ModelID != "deepseek_masked" || lots[0].StrategyID != domain.StrategyIDMultfactorV1 {
		t.Fatalf("lot = %#v err=%v, want %s shares and its opening identity kept", lots, err, shares)
	}
	var positionShares string
	if err := fixture.db.QueryRow(`SELECT total_shares::text FROM execution_positions
		WHERE execution_account_id=$1 AND token_id=$2`, fixture.accountID, fixture.tokenID).Scan(&positionShares); err != nil {
		t.Fatal(err)
	}
	if !sameNumeric(positionShares, shares) {
		t.Fatalf("position shares = %s, want %s", positionShares, shares)
	}
}

func ownershipFill(venueFillID string, shares, price, gross domain.Decimal, confirmedAt time.Time) domain.Fill {
	return domain.Fill{
		VenueFillID: venueFillID, LiquidityRole: domain.LiquidityRoleMaker,
		Status: domain.FillStatusConfirmed, Shares: shares, Price: price,
		GrossNotional: gross, FeeRateBPS: "0", PlatformFeeRate: "0", FeeExponent: "0",
		PlatformFee: "0", BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0",
		FeeSource: "TEST_AUTHORITATIVE_SETTLEMENT",
		MatchedAt: confirmedAt.Add(-time.Second), VenueUpdatedAt: confirmedAt,
		ObservedAt: confirmedAt, ConfirmedAt: &confirmedAt,
	}
}

// TestCrossIdentitySellSurvivesRestartPostgresIntegration: after the cutover
// every SELL of a pre-cutover lot carries the new model (and possibly another
// strategy). A restart while such a SELL still holds its reservation must not
// be refused by the startup ledger check; the rollback gate must flag it,
// because the previous release's identical check would refuse it.
func TestCrossIdentitySellSurvivesRestartPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	for _, test := range []struct {
		name, modelID, strategyID string
		rollbackBlocked           bool
	}{
		{name: "opening-identity", modelID: "deepseek_masked", strategyID: domain.StrategyIDMultfactorV1},
		{name: "new-model", modelID: "v4.1 flash", strategyID: domain.StrategyIDMultfactorV1, rollbackBlocked: true},
		{name: "new-model-and-strategy", modelID: "v4.1 flash", strategyID: "brier_mid_edge_once", rollbackBlocked: true},
		{name: "same-model-new-strategy", modelID: "deepseek_masked", strategyID: "brier_mid_edge_once", rollbackBlocked: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newOwnershipFixture(t, databaseURL)
			sell := fixture.acknowledgedSell(t, "ownership-sell", test.modelID, test.strategyID, "4")
			if reserved := fixture.activeSellReservedShares(t); !sameNumeric(reserved, "4") {
				t.Fatalf("active SELL reserved shares = %s, want 4", reserved)
			}
			if err := fixture.bootstrap(); err != nil {
				t.Fatalf("restart with an active SELL reservation was refused: %v", err)
			}
			if blockers := fixture.rollbackGateRows(t); (len(blockers) == 1) != test.rollbackBlocked || len(blockers) > 1 {
				t.Fatalf("rollback gate = %v, want blocked=%v", blockers, test.rollbackBlocked)
			}
			sold, err := fixture.processor.Process(fixture.ctx, sell, ownershipFill("trade-ownership-sell", "4", "0.7", "2.8", fixture.now.Add(31*time.Second)))
			if err != nil || sold.Order.Status != domain.OrderStatusFilled {
				t.Fatalf("SELL application = %#v err=%v", sold, err)
			}
			fixture.assertHolding(t, "6")
			if err := fixture.bootstrap(); err != nil {
				t.Fatalf("restart after the SELL filled: %v", err)
			}
			if blockers := fixture.rollbackGateRows(t); len(blockers) != 0 {
				t.Fatalf("rollback gate after the SELL filled = %v", blockers)
			}
		})
	}
}

// TestCancelledPartialSellLateFillPostgresIntegration: a FAK SELL fills 4 of 6
// and is CANCELLED, but the remaining 2-share reservation stays unreleased
// (RECONCILIATION_REQUIRED) until cancel finality. Restart must pass, rollback must stay blocked, and a late
// fill must close the order without booking any share or cash twice.
func TestCancelledPartialSellLateFillPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	fixture := newOwnershipFixture(t, databaseURL)
	sell := fixture.acknowledgedSell(t, "ownership-sell", "v4.1 flash", domain.StrategyIDMultfactorV1, "6")
	partial, err := fixture.processor.Process(fixture.ctx, sell, ownershipFill("trade-ownership-sell-1", "4", "0.7", "2.8", fixture.now.Add(31*time.Second)))
	if err != nil || partial.Order.Status != domain.OrderStatusCancelled {
		t.Fatalf("partial FAK SELL = %#v err=%v, want CANCELLED", partial, err)
	}
	fixture.assertHolding(t, "6")
	if reserved := fixture.activeSellReservedShares(t); !sameNumeric(reserved, "2") {
		t.Fatalf("active SELL reserved shares after cancel = %s, want 2", reserved)
	}
	if err := fixture.bootstrap(); err != nil {
		t.Fatalf("restart inside the cancel finality window was refused: %v", err)
	}
	blockers := fixture.rollbackGateRows(t)
	// The cancelled remainder waits for finality as RECONCILIATION_REQUIRED.
	if len(blockers) != 1 || !strings.HasSuffix(blockers[0], "/RECONCILIATION_REQUIRED/CANCELLED") {
		t.Fatalf("rollback gate for a CANCELLED SELL with an active reservation = %v", blockers)
	}
	var cashAfterPartial string
	if err := fixture.db.QueryRow(`SELECT total_balance::text FROM execution_accounts WHERE execution_account_id=$1`, fixture.accountID).Scan(&cashAfterPartial); err != nil {
		t.Fatal(err)
	}

	late := ownershipFill("trade-ownership-sell-2", "2", "0.7", "1.4", fixture.now.Add(45*time.Second))
	filled, err := fixture.processor.Process(fixture.ctx, partial.Order, late)
	if err != nil || filled.Order.Status != domain.OrderStatusFilled {
		t.Fatalf("late fill after cancel = %#v err=%v, want FILLED", filled, err)
	}
	fixture.assertHolding(t, "4")
	var lateCredited bool
	if err := fixture.db.QueryRow(`SELECT total_balance=$2::numeric+1.4 FROM execution_accounts WHERE execution_account_id=$1`,
		fixture.accountID, cashAfterPartial).Scan(&lateCredited); err != nil || !lateCredited {
		t.Fatalf("late fill credited the 1.4 proceeds exactly once: %v err=%v", lateCredited, err)
	}
	if reserved := fixture.activeSellReservedShares(t); !sameNumeric(reserved, "0") {
		t.Fatalf("active SELL reserved shares after the late fill = %s, want 0", reserved)
	}
	if err := fixture.bootstrap(); err != nil {
		t.Fatalf("restart after the late fill: %v", err)
	}
	if blockers := fixture.rollbackGateRows(t); len(blockers) != 0 {
		t.Fatalf("rollback gate after the late fill = %v", blockers)
	}

	// Replaying the same venue fill (reconciliation re-observing it) changes nothing.
	var cashAfterLate string
	if err := fixture.db.QueryRow(`SELECT total_balance::text FROM execution_accounts WHERE execution_account_id=$1`, fixture.accountID).Scan(&cashAfterLate); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.processor.Process(fixture.ctx, filled.Order, late); err != nil {
		t.Fatalf("replay late fill: %v", err)
	}
	fixture.assertHolding(t, "4")
	assertAccount(t, fixture.db, fixture.accountID, cashAfterLate, cashAfterLate, "0")
}

// TestCheckBindingAuthorizationMigrationIgnoresOtherSchemasPostgresIntegration:
// the guard reads the function the application's trigger calls, so a migrated
// same-named function in another schema cannot mask a pre-0041 one.
func TestCheckBindingAuthorizationMigrationIgnoresOtherSchemasPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	migratedElsewhere := newIntegrationDatabase(t, databaseURL)
	application := newIntegrationDatabase(t, databaseURL)
	ctx := context.Background()
	previous, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "0025_sell_exit_freshness_exemption.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := application.Exec(string(previous)); err != nil {
		t.Fatal(err)
	}
	if err := CheckBindingAuthorizationMigration(ctx, migratedElsewhere); err != nil {
		t.Fatalf("migrated schema: %v", err)
	}
	if err := CheckBindingAuthorizationMigration(ctx, application); err == nil ||
		!strings.Contains(err.Error(), "0041_binding_authorization_without_model.sql is not applied") {
		t.Fatalf("pre-0041 application schema next to a migrated one: %v", err)
	}
	if _, err := migratedElsewhere.Exec(`DROP TRIGGER execution_orders_live_submit_risk_trigger ON execution_orders`); err != nil {
		t.Fatal(err)
	}
	if err := CheckBindingAuthorizationMigration(ctx, migratedElsewhere); err == nil ||
		!strings.Contains(err.Error(), "has no enforce_live_order_submit_risk trigger") {
		t.Fatalf("schema without the submit trigger: %v", err)
	}
}
