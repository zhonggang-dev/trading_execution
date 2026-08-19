package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/fillprocessor"
	"github.com/UniPat-AI/trading_execution/internal/service/orderstate"
)

// TestOrderRepositoryPostgresIntegration 验证 Order Repository Postgres Integration 场景下的行为。
func TestOrderRepositoryPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	insertAccount(t, db, "account-order-lifecycle", "0xorderlifecycle", "100", "100", "0")
	repository, err := NewOrderRepository(db)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	order := integrationOrder("lifecycle", "account-order-lifecycle", "token-lifecycle", domain.SideBuy, "10", "0.8")
	order.Status = domain.OrderStatusReceived
	order.CreatedAt = now
	order.UpdatedAt = now
	order.Revision = 1
	stored, created, err := repository.Create(context.Background(), order)
	if err != nil || !created {
		t.Fatalf("create order: created=%v err=%v", created, err)
	}

	stored, event := applyIntegrationTransition(t, stored, domain.OrderStatusValidating, domain.TransitionTriggerValidation, now.Add(time.Second), "")
	if err := repository.Transition(context.Background(), stored, event); err != nil {
		t.Fatal(err)
	}
	stored, event = applyIntegrationTransition(t, stored, domain.OrderStatusReserved, domain.TransitionTriggerReservation, now.Add(2*time.Second), "")
	if err := repository.Transition(context.Background(), stored, event); err != nil {
		t.Fatal(err)
	}

	attempt := domain.OrderAttempt{
		ID: "attempt:order-lifecycle:1", OrderID: stored.ID, Sequence: 1,
		Kind: domain.OrderAttemptSubmit, Outcome: domain.AttemptOutcomeStarted,
		RequestFingerprint: "non-secret-test-fingerprint", StartedAt: now.Add(3 * time.Second),
	}
	stored, event = applyIntegrationTransition(t, stored, domain.OrderStatusSubmitting, domain.TransitionTriggerSubmit, attempt.StartedAt, attempt.ID)
	if err := repository.StartAttempt(context.Background(), stored, event, attempt); err != nil {
		t.Fatal(err)
	}
	submitting := stored
	completedAt := now.Add(4 * time.Second)
	attempt.Outcome = domain.AttemptOutcomeSucceeded
	attempt.CompletedAt = &completedAt
	attempt.VenueOrderID = "0xvenue-order"
	attempt.VenueStatus = "live"
	stored, event = applyIntegrationTransition(t, stored, domain.OrderStatusAcknowledged, domain.TransitionTriggerVenueResponse, completedAt, attempt.ID)
	stored.VenueOrderID = attempt.VenueOrderID
	event.VenueOrderID = attempt.VenueOrderID
	if err := repository.FinishAttempt(context.Background(), stored, event, attempt); err != nil {
		t.Fatal(err)
	}

	events, err := repository.Events(context.Background(), stored.ID)
	if err != nil {
		t.Fatal(err)
	}
	attempts, err := repository.Attempts(context.Background(), stored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 5 || len(attempts) != 1 || attempts[0].Outcome != domain.AttemptOutcomeSucceeded {
		t.Fatalf("events=%d attempts=%#v", len(events), attempts)
	}

	stale, staleEvent := applyIntegrationTransition(t, submitting, domain.OrderStatusUnknown, domain.TransitionTriggerVenueResponse, now.Add(5*time.Second), attempt.ID)
	if err := repository.Transition(context.Background(), stale, staleEvent); !errors.Is(err, port.ErrOrderRevisionConflict) {
		t.Fatalf("stale transition error = %v, want revision conflict", err)
	}
}

// TestReservationManagerPostgresIntegration 验证 Reservation Manager Postgres Integration 场景下的行为。
func TestReservationManagerPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	manager, err := NewReservationManager(ReservationManagerParams{DB: db, MaxBuyFeeRateBPS: "0"})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("concurrent buys cannot spend the same balance", func(t *testing.T) {
		insertAccount(t, db, "account-concurrent-buy", "0xbuy", "100", "100", "0")
		orders := []domain.Order{
			integrationOrder("buy-a", "account-concurrent-buy", "token-a", domain.SideBuy, "100", "0.8"),
			integrationOrder("buy-b", "account-concurrent-buy", "token-b", domain.SideBuy, "100", "0.8"),
		}
		errorsChannel := make(chan error, len(orders))
		var waitGroup sync.WaitGroup
		for _, order := range orders {
			waitGroup.Add(1)
			go func(order domain.Order) {
				defer waitGroup.Done()
				_, err := manager.Reserve(context.Background(), order)
				errorsChannel <- err
			}(order)
		}
		waitGroup.Wait()
		close(errorsChannel)
		successes := 0
		rejections := 0
		for err := range errorsChannel {
			if err == nil {
				successes++
				continue
			}
			var rejection *port.Rejection
			if errors.As(err, &rejection) && rejection.Code == "INSUFFICIENT_AVAILABLE_BALANCE" {
				rejections++
				continue
			}
			t.Fatalf("unexpected concurrent reserve error: %v", err)
		}
		if successes != 1 || rejections != 1 {
			t.Fatalf("successes=%d rejections=%d, want one of each", successes, rejections)
		}
		assertAccount(t, db, "account-concurrent-buy", "100", "20", "80")
	})

	t.Run("partial buy consumes fills and cancel releases only remainder", func(t *testing.T) {
		insertAccount(t, db, "account-partial-buy", "0xpartial", "100", "100", "0")
		order := integrationOrder("partial-buy", "account-partial-buy", "token-partial", domain.SideBuy, "100", "0.8")
		if _, err := manager.Reserve(context.Background(), order); err != nil {
			t.Fatal(err)
		}
		observedAt := time.Now().UTC()
		order.Status = domain.OrderStatusPartiallyFilled
		order.FilledSize = "30"
		order.AverageFillPrice = "0.7"
		order.VenueLastObservedAt = &observedAt
		reservation, err := manager.Reconcile(context.Background(), order)
		if err != nil {
			t.Fatal(err)
		}
		if !reservation.RemainingReservedBalance.Equal("56") {
			t.Fatalf("remaining buy reserve = %s, want 56", reservation.RemainingReservedBalance)
		}
		assertAccount(t, db, "account-partial-buy", "79.0", "23.0", "56.0")
		assertPosition(t, db, "account-partial-buy", "token-partial", "30", "30", "0")

		order.Status = domain.OrderStatusCanceled
		observedAt = observedAt.Add(time.Second)
		order.VenueLastObservedAt = &observedAt
		reservation, err = manager.Reconcile(context.Background(), order)
		if err != nil {
			t.Fatal(err)
		}
		if reservation.Status != domain.ReservationStatusReleased || reservation.RemainingReservedBalance != "0" {
			t.Fatalf("canceled reservation = %#v", reservation)
		}
		assertAccount(t, db, "account-partial-buy", "79.0", "79.0", "0")
	})

	t.Run("concurrent sells cannot reserve the same shares", func(t *testing.T) {
		insertAccount(t, db, "account-concurrent-sell", "0xsell", "10", "10", "0")
		_, err := db.Exec(`
			INSERT INTO execution_positions (
				execution_account_id, market_id, token_id,
				total_shares, available_shares, reserved_shares
			) VALUES ('account-concurrent-sell', 'market-1', 'token-sell', 100, 100, 0)`)
		if err != nil {
			t.Fatal(err)
		}
		insertOpenLotFixture(t, db, "account-concurrent-sell", "token-sell", "100")
		orders := []domain.Order{
			integrationOrder("sell-a", "account-concurrent-sell", "token-sell", domain.SideSell, "80", "0.5"),
			integrationOrder("sell-b", "account-concurrent-sell", "token-sell", domain.SideSell, "80", "0.5"),
		}
		errorsChannel := make(chan error, len(orders))
		var waitGroup sync.WaitGroup
		for _, order := range orders {
			waitGroup.Add(1)
			go func(order domain.Order) {
				defer waitGroup.Done()
				_, err := manager.Reserve(context.Background(), order)
				errorsChannel <- err
			}(order)
		}
		waitGroup.Wait()
		close(errorsChannel)
		successes := 0
		rejections := 0
		for err := range errorsChannel {
			if err == nil {
				successes++
				continue
			}
			var rejection *port.Rejection
			if errors.As(err, &rejection) && (rejection.Code == "DUPLICATE_SELL_ORDER" || rejection.Code == "INSUFFICIENT_AVAILABLE_SHARES") {
				rejections++
				continue
			}
			t.Fatalf("unexpected concurrent sell error: %v", err)
		}
		if successes != 1 || rejections != 1 {
			t.Fatalf("successes=%d rejections=%d, want one of each", successes, rejections)
		}
		assertPosition(t, db, "account-concurrent-sell", "token-sell", "100", "20", "80")
	})

	t.Run("partial sell is idempotent and cancel releases unsold shares", func(t *testing.T) {
		insertAccount(t, db, "account-partial-sell", "0xpartialsell", "10", "10", "0")
		_, err := db.Exec(`
			INSERT INTO execution_positions (
				execution_account_id, market_id, token_id,
				total_shares, available_shares, reserved_shares
			) VALUES ('account-partial-sell', 'market-1', 'token-partial-sell', 100, 100, 0)`)
		if err != nil {
			t.Fatal(err)
		}
		insertOpenLotFixture(t, db, "account-partial-sell", "token-partial-sell", "100")
		order := integrationOrder("partial-sell", "account-partial-sell", "token-partial-sell", domain.SideSell, "80", "0.5")
		if _, err := manager.Reserve(context.Background(), order); err != nil {
			t.Fatal(err)
		}
		observedAt := time.Now().UTC()
		order.Status = domain.OrderStatusPartiallyFilled
		order.FilledSize = "30"
		order.AverageFillPrice = "0.7"
		order.VenueLastObservedAt = &observedAt
		reservation, err := manager.Reconcile(context.Background(), order)
		if err != nil {
			t.Fatal(err)
		}
		if !reservation.RemainingReservedShares.Equal("50") {
			t.Fatalf("remaining sell reserve = %s, want 50", reservation.RemainingReservedShares)
		}
		assertAccount(t, db, "account-partial-sell", "31", "31", "0")
		assertPosition(t, db, "account-partial-sell", "token-partial-sell", "70", "20", "50")

		// Replaying the same cumulative venue observation must not credit the
		// proceeds or consume shares twice.
		if _, err := manager.Reconcile(context.Background(), order); err != nil {
			t.Fatal(err)
		}
		assertAccount(t, db, "account-partial-sell", "31", "31", "0")
		assertPosition(t, db, "account-partial-sell", "token-partial-sell", "70", "20", "50")

		order.Status = domain.OrderStatusCanceled
		observedAt = observedAt.Add(time.Second)
		order.VenueLastObservedAt = &observedAt
		reservation, err = manager.Reconcile(context.Background(), order)
		if err != nil {
			t.Fatal(err)
		}
		if reservation.Status != domain.ReservationStatusReleased || !reservation.RemainingReservedShares.Equal("0") {
			t.Fatalf("canceled sell reservation = %#v", reservation)
		}
		assertAccount(t, db, "account-partial-sell", "31", "31", "0")
		assertPosition(t, db, "account-partial-sell", "token-partial-sell", "70", "70", "0")
	})

	t.Run("different lots of the same token can reserve independently", func(t *testing.T) {
		insertAccount(t, db, "account-two-lots", "0xtwolots", "10", "10", "0")
		_, err := db.Exec(`
			INSERT INTO execution_positions (
				execution_account_id, market_id, token_id,
				total_shares, available_shares, reserved_shares
			) VALUES ('account-two-lots', 'market-1', 'token-two-lots', 100, 100, 0)`)
		if err != nil {
			t.Fatal(err)
		}
		insertOpenLotFixtureNamed(t, db, "account-two-lots", "token-two-lots", "lot-two-lots-a", "50")
		insertOpenLotFixtureNamed(t, db, "account-two-lots", "token-two-lots", "lot-two-lots-b", "50")
		first := integrationOrder("two-lots-a", "account-two-lots", "token-two-lots", domain.SideSell, "30", "0.5")
		first.Intent.TargetLotID = "lot-two-lots-a"
		second := integrationOrder("two-lots-b", "account-two-lots", "token-two-lots", domain.SideSell, "30", "0.5")
		second.Intent.TargetLotID = "lot-two-lots-b"
		if _, err := manager.Reserve(context.Background(), first); err != nil {
			t.Fatalf("reserve first lot: %v", err)
		}
		if _, err := manager.Reserve(context.Background(), second); err != nil {
			t.Fatalf("reserve second lot: %v", err)
		}
		assertPosition(t, db, "account-two-lots", "token-two-lots", "100", "40", "60")
	})
}

func TestReconciliationRecorderClosesPreviouslyOpenFingerprint(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	insertAccount(t, db, "account-reconciliation-resolution", "0xresolution", "10", "10", "0")
	recorder, err := NewReconciliationRecorder(db)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	firstRun, err := (domain.ReconciliationRunParams{
		RunID: "run-open-fingerprint", ExecutionAccountID: "account-reconciliation-resolution",
		Trigger: domain.ReconciliationTriggerScheduled, StartedAt: now,
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.Start(context.Background(), firstRun); err != nil {
		t.Fatal(err)
	}
	openIssue := domain.ReconciliationIssue{
		IssueID: "issue-open-fingerprint", RunID: firstRun.RunID, Fingerprint: "stable-fingerprint",
		ExecutionAccountID: firstRun.ExecutionAccountID, Type: domain.ReconciliationIssueBalanceDrift,
		Resolution: domain.ReconciliationResolutionRetry, Status: domain.ReconciliationIssueOpen,
		Details: "balance evidence is temporarily inconsistent", ObservedAt: now,
	}
	if err := recorder.RecordIssue(context.Background(), openIssue); err != nil {
		t.Fatal(err)
	}
	completed := now.Add(time.Second)
	firstRun.Status, firstRun.CompletedAt = domain.ReconciliationRunAttentionRequired, &completed
	if err := recorder.Complete(context.Background(), firstRun); err != nil {
		t.Fatal(err)
	}

	secondRun, err := (domain.ReconciliationRunParams{
		RunID: "run-resolved-fingerprint", ExecutionAccountID: firstRun.ExecutionAccountID,
		Trigger: domain.ReconciliationTriggerScheduled, StartedAt: now.Add(2 * time.Second),
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.Start(context.Background(), secondRun); err != nil {
		t.Fatal(err)
	}
	resolvedAt := now.Add(3 * time.Second)
	resolvedIssue := openIssue
	resolvedIssue.IssueID = "issue-resolved-fingerprint"
	resolvedIssue.RunID = secondRun.RunID
	resolvedIssue.Resolution = domain.ReconciliationResolutionAutomatic
	resolvedIssue.Status = domain.ReconciliationIssueResolved
	resolvedIssue.Details = "fresh evidence now matches"
	resolvedIssue.ObservedAt = resolvedAt
	resolvedIssue.ResolvedAt = &resolvedAt
	if err := recorder.RecordIssue(context.Background(), resolvedIssue); err != nil {
		t.Fatal(err)
	}
	var openCount, resolvedCount int
	if err := db.QueryRow(`SELECT count(*) FILTER (WHERE status='OPEN'), count(*) FILTER (WHERE status='RESOLVED')
		FROM reconciliation_issues WHERE execution_account_id=$1 AND fingerprint=$2`,
		firstRun.ExecutionAccountID, openIssue.Fingerprint).Scan(&openCount, &resolvedCount); err != nil {
		t.Fatal(err)
	}
	if openCount != 0 || resolvedCount != 1 {
		t.Fatalf("issue counts open=%d resolved=%d, want 0/1", openCount, resolvedCount)
	}
}

// TestAtomicLiveRiskPostgresIntegration verifies that LIVE_CHECK orders are
// fail-closed and that account-scoped limits are checked inside the same row
// lock transaction as the balance reservation.
func TestAtomicLiveRiskPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	now := time.Now().UTC().Truncate(time.Microsecond)
	manager, err := NewReservationManager(ReservationManagerParams{
		DB: db, Now: func() time.Time { return now }, MaxBuyFeeRateBPS: "0",
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("migration defaults the global kill switch to on", func(t *testing.T) {
		accountID := "account-live-default-kill"
		insertAccount(t, db, accountID, "0xlivedefaultkill", "100", "100", "0")
		provisionLiveRisk(t, db, liveRiskFixture{
			accountID: accountID, now: now, binding: true,
			maxOrder: "100", maxMarket: "100", maxStrategy: "100",
			maxWallet: "100", maxDaily: "100",
		})
		_, err := manager.Reserve(context.Background(), liveIntegrationOrder(
			"default-kill", accountID, "token-default-kill", "10", "0.5", now,
		))
		assertRejectionCode(t, err, "GLOBAL_KILL_SWITCH")
		assertAccount(t, db, accountID, "100", "100", "0")
	})

	if _, err := db.Exec(`
		UPDATE execution_risk_global_control
		SET kill_switch=FALSE, reason='', version=version+1, updated_at=$1
		WHERE singleton=TRUE`, now); err != nil {
		t.Fatal(err)
	}

	t.Run("risk control version cannot move backwards", func(t *testing.T) {
		if _, err := db.Exec(`
			UPDATE execution_risk_global_control
			SET version=version-1
			WHERE singleton=TRUE`); err == nil || !strings.Contains(err.Error(), "version must not decrease") {
			t.Fatalf("version downgrade error=%v, want rejection", err)
		}
	})

	t.Run("model strategy account binding is explicit", func(t *testing.T) {
		accountID := "account-live-binding"
		insertAccount(t, db, accountID, "0xlivebinding", "100", "100", "0")
		provisionLiveRisk(t, db, liveRiskFixture{
			accountID: accountID, now: now, binding: false,
			maxOrder: "100", maxMarket: "100", maxStrategy: "100",
			maxWallet: "100", maxDaily: "100",
		})
		_, err := manager.Reserve(context.Background(), liveIntegrationOrder(
			"binding", accountID, "token-binding", "10", "0.5", now,
		))
		assertRejectionCode(t, err, "STRATEGY_ACCOUNT_BINDING_DENIED")
		assertAccount(t, db, accountID, "100", "100", "0")
	})

	t.Run("concurrent pending orders cannot overbook daily limit", func(t *testing.T) {
		accountID := "account-live-daily"
		insertAccount(t, db, accountID, "0xlivedaily", "1000", "1000", "0")
		provisionLiveRisk(t, db, liveRiskFixture{
			accountID: accountID, now: now, binding: true,
			maxOrder: "100", maxMarket: "1000", maxStrategy: "1000",
			maxWallet: "1000", maxDaily: "75",
		})
		orders := []domain.Order{
			liveIntegrationOrder("daily-a", accountID, "token-daily-a", "100", "0.5", now),
			liveIntegrationOrder("daily-b", accountID, "token-daily-b", "100", "0.5", now),
		}
		assertConcurrentRiskLimit(t, manager, orders, "DAILY_TRADED_NOTIONAL_EXCEEDED")
		assertAccount(t, db, accountID, "1000", "950", "50")
		var authorized int
		if err := db.QueryRow(`
			SELECT count(*) FROM asset_reservations
			WHERE execution_account_id=$1 AND risk_policy_id <> ''
			  AND daily_risk_notional=50`, accountID).Scan(&authorized); err != nil || authorized != 1 {
			t.Fatalf("authorized daily reservations=%d err=%v, want 1", authorized, err)
		}
	})

	t.Run("concurrent buys cannot overbook market exposure", func(t *testing.T) {
		accountID := "account-live-market"
		insertAccount(t, db, accountID, "0xlivemarket", "1000", "1000", "0")
		provisionLiveRisk(t, db, liveRiskFixture{
			accountID: accountID, now: now, binding: true,
			maxOrder: "100", maxMarket: "75", maxStrategy: "1000",
			maxWallet: "1000", maxDaily: "1000",
		})
		orders := []domain.Order{
			liveIntegrationOrder("market-a", accountID, "token-market-a", "100", "0.5", now),
			liveIntegrationOrder("market-b", accountID, "token-market-b", "100", "0.5", now),
		}
		assertConcurrentRiskLimit(t, manager, orders, "MAX_MARKET_EXPOSURE_EXCEEDED")
		assertAccount(t, db, accountID, "1000", "950", "50")
	})

	t.Run("submit trigger rechecks kill switch after reservation", func(t *testing.T) {
		accountID := "account-live-submit-gate"
		insertAccount(t, db, accountID, "0xlivesubmitgate", "100", "100", "0")
		provisionLiveRisk(t, db, liveRiskFixture{
			accountID: accountID, now: now, binding: true,
			maxOrder: "100", maxMarket: "100", maxStrategy: "100",
			maxWallet: "100", maxDaily: "100",
		})
		repository, err := NewOrderRepository(db)
		if err != nil {
			t.Fatal(err)
		}
		order := liveIntegrationOrder("submit-gate", accountID, "token-submit-gate", "10", "0.5", now)
		order.Status = domain.OrderStatusReceived
		order.CreatedAt, order.UpdatedAt, order.Revision = now, now, 1
		stored, created, err := repository.Create(context.Background(), order)
		if err != nil || !created {
			t.Fatalf("create live order: created=%v err=%v", created, err)
		}
		stored, event := applyIntegrationTransition(t, stored, domain.OrderStatusValidating, domain.TransitionTriggerValidation, now, "")
		if err := repository.Transition(context.Background(), stored, event); err != nil {
			t.Fatal(err)
		}
		if _, err := manager.Reserve(context.Background(), stored); err != nil {
			t.Fatalf("reserve live order: %v", err)
		}
		stored, event = applyIntegrationTransition(t, stored, domain.OrderStatusReserved, domain.TransitionTriggerReservation, now, "")
		if err := repository.Transition(context.Background(), stored, event); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`
			UPDATE execution_risk_global_control
			SET kill_switch=TRUE, reason='TEST_KILL', version=version+1, updated_at=$1
			WHERE singleton=TRUE`, now); err != nil {
			t.Fatal(err)
		}
		attempt := domain.OrderAttempt{
			ID: "attempt:submit-gate:1", OrderID: stored.ID, Sequence: 1,
			Kind: domain.OrderAttemptSubmit, Outcome: domain.AttemptOutcomeStarted,
			StartedAt: now,
		}
		stored, event = applyIntegrationTransition(t, stored, domain.OrderStatusSubmitting, domain.TransitionTriggerSubmit, now, attempt.ID)
		if err := repository.StartAttempt(context.Background(), stored, event, attempt); err == nil ||
			!strings.Contains(err.Error(), "GLOBAL_KILL_SWITCH") {
			t.Fatalf("submit gate error=%v, want GLOBAL_KILL_SWITCH", err)
		}
	})
}

type liveRiskFixture struct {
	accountID                        string
	now                              time.Time
	binding                          bool
	maxOrder, maxMarket, maxStrategy string
	maxWallet, maxDaily              string
}

func provisionLiveRisk(t *testing.T, db *sql.DB, fixture liveRiskFixture) {
	t.Helper()
	// One model/strategy pair is intentionally bound to exactly one account.
	// Subtests reuse the pair, so release the previous fixture binding first.
	if _, err := db.Exec(`
		DELETE FROM execution_strategy_bindings
		WHERE model_id='model-a' AND strategy_id='multfactor_v1'`); err != nil {
		t.Fatal(err)
	}
	_, err := db.Exec(`
		INSERT INTO execution_risk_policies (
			execution_account_id, policy_id, enabled,
			max_order_notional, max_market_exposure, max_strategy_exposure,
			max_wallet_exposure, max_daily_traded_notional,
			max_price_age_ms, max_signal_age_ms, max_state_age_ms, daily_timezone
		) VALUES ($1,$2,TRUE,$3::numeric,$4::numeric,$5::numeric,$6::numeric,$7::numeric,
			60000,60000,60000,'UTC')`,
		fixture.accountID, "policy-"+fixture.accountID, fixture.maxOrder, fixture.maxMarket,
		fixture.maxStrategy, fixture.maxWallet, fixture.maxDaily)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		INSERT INTO execution_risk_controls (
			execution_account_id, control_scope, control_key, paused, reason
		) VALUES ($1,'ACCOUNT','',FALSE,'')`, fixture.accountID)
	if err != nil {
		t.Fatal(err)
	}
	if fixture.binding {
		_, err = db.Exec(`
			INSERT INTO execution_strategy_bindings (
				model_id, strategy_id, execution_account_id, enabled
			) VALUES ('model-a','multfactor_v1',$1,TRUE)`, fixture.accountID)
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err = db.Exec(`
		INSERT INTO reconciliation_runs (
			run_id, execution_account_id, trigger, status, summary, started_at, completed_at
		) VALUES ($1,$2,'SCHEDULED','COMPLETED','{}'::jsonb,$3,$4)`,
		"risk-run-"+fixture.accountID, fixture.accountID,
		fixture.now.Add(-2*time.Second), fixture.now.Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
}

func liveIntegrationOrder(orderID, accountID, tokenID, size, worstPrice string, now time.Time) domain.Order {
	order := integrationOrder(orderID, accountID, tokenID, domain.SideBuy, size, worstPrice)
	marketAt := now.Add(-time.Second)
	signalAt := now.Add(-time.Second)
	order.Intent.MarketSnapshotAt = &marketAt
	order.Intent.SignalAt = &signalAt
	order.MarketValidation = &domain.MarketValidation{
		Mode: liveMarketValidationMode, ValidatedAt: now, WorstPrice: order.Intent.WorstPrice,
	}
	return order
}

func assertConcurrentRiskLimit(t *testing.T, manager *ReservationManager, orders []domain.Order, rejectionCode string) {
	t.Helper()
	errorsChannel := make(chan error, len(orders))
	var waitGroup sync.WaitGroup
	for _, order := range orders {
		waitGroup.Add(1)
		go func(value domain.Order) {
			defer waitGroup.Done()
			_, err := manager.Reserve(context.Background(), value)
			errorsChannel <- err
		}(order)
	}
	waitGroup.Wait()
	close(errorsChannel)
	successes, rejections := 0, 0
	for err := range errorsChannel {
		if err == nil {
			successes++
			continue
		}
		var rejection *port.Rejection
		if errors.As(err, &rejection) && rejection.Code == rejectionCode {
			rejections++
			continue
		}
		t.Fatalf("unexpected concurrent live risk error: %v", err)
	}
	if successes != 1 || rejections != 1 {
		t.Fatalf("successes=%d rejections=%d, want one of each", successes, rejections)
	}
}

func assertRejectionCode(t *testing.T, err error, code string) {
	t.Helper()
	var rejection *port.Rejection
	if !errors.As(err, &rejection) || rejection.Code != code {
		t.Fatalf("error=%v, want rejection %s", err, code)
	}
}

// TestFillAndPositionLedgerPostgresIntegration 验证 Fill And Position Ledger Postgres Integration 场景下的行为。
func TestFillAndPositionLedgerPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	insertAccount(t, db, "account-fill-ledger", "0xfillledger", "100", "100", "0")
	repository, err := NewOrderRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	reservations, err := NewReservationManager(ReservationManagerParams{DB: db, MaxBuyFeeRateBPS: "0"})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := NewFillLedger(FillLedgerParams{
		DB: db, DustSharesThreshold: "0.01", DustNotionalThreshold: "1",
	})
	if err != nil {
		t.Fatal(err)
	}
	processor, err := fillprocessor.New(fillprocessor.Params{
		Orders: repository, Source: noFillsSource{}, Ledger: ledger,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	buy := integrationOrder("ledger-buy", "account-fill-ledger", "token-ledger", domain.SideBuy, "10", "0.6")
	buy.Intent.Venue = "paper"
	buy = createAcknowledgedIntegrationOrder(t, ctx, repository, reservations, buy, "0xbuy-ledger", now)

	matched := domain.Fill{
		VenueFillID: "trade-buy-1", LiquidityRole: domain.LiquidityRoleMaker,
		Status: domain.FillStatusMatched, Shares: "4", Price: "0.5",
		GrossNotional: "2", FeeRateBPS: "0", PlatformFeeRate: "0", FeeExponent: "0",
		PlatformFee: "0", BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0",
		FeeSource: "TEST_AUTHORITATIVE_SETTLEMENT",
		MatchedAt: now.Add(10 * time.Second), VenueUpdatedAt: now.Add(10 * time.Second),
		ObservedAt: now.Add(11 * time.Second),
	}
	observation, err := processor.Process(ctx, buy, matched)
	if err != nil {
		t.Fatal(err)
	}
	if observation.Applied || observation.Order.Revision != buy.Revision {
		t.Fatalf("MATCHED observation changed authority: %#v", observation)
	}
	assertAccount(t, db, "account-fill-ledger", "100", "94", "6")

	confirmedAt := now.Add(12 * time.Second)
	matched.Status = domain.FillStatusConfirmed
	matched.ConfirmedAt = &confirmedAt
	matched.ObservedAt = confirmedAt
	first, err := processor.Process(ctx, observation.Order, matched)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Applied || first.Order.Status != domain.OrderStatusPartiallyFilled ||
		!first.Order.FilledSize.Equal("4") || !first.Order.AverageFillPrice.Equal("0.5") {
		t.Fatalf("first fill application = %#v", first)
	}
	assertAccount(t, db, "account-fill-ledger", "98", "94.4", "3.6")
	assertPosition(t, db, "account-fill-ledger", "token-ledger", "4", "4", "0")

	duplicate, err := processor.Process(ctx, first.Order, matched)
	if err != nil || !duplicate.Duplicate || duplicate.Applied {
		t.Fatalf("duplicate application = %#v err=%v", duplicate, err)
	}
	assertAccount(t, db, "account-fill-ledger", "98", "94.4", "3.6")

	secondConfirmedAt := now.Add(14 * time.Second)
	second, err := processor.Process(ctx, duplicate.Order, domain.Fill{
		VenueFillID: "trade-buy-2", LiquidityRole: domain.LiquidityRoleMaker,
		Status: domain.FillStatusConfirmed, Shares: "6", Price: "0.4",
		GrossNotional: "2.4", FeeRateBPS: "0", PlatformFeeRate: "0", FeeExponent: "0",
		PlatformFee: "0", BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0",
		FeeSource: "TEST_AUTHORITATIVE_SETTLEMENT",
		MatchedAt: now.Add(13 * time.Second), VenueUpdatedAt: secondConfirmedAt,
		ObservedAt: secondConfirmedAt, ConfirmedAt: &secondConfirmedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Order.Status != domain.OrderStatusFilled || !second.Order.FilledNotional.Equal("4.4") ||
		!second.Order.AverageFillPrice.Equal("0.44") {
		t.Fatalf("completed BUY order = %#v", second.Order)
	}
	assertAccount(t, db, "account-fill-ledger", "95.6", "95.6", "0")
	position, err := ledger.GetPosition(ctx, "account-fill-ledger", "token-ledger")
	if err != nil || !position.TotalShares.Equal("10") || !position.CostBasis.Equal("4.4") ||
		!position.AverageCostPrice.Equal("0.44") {
		t.Fatalf("BUY position = %#v err=%v", position, err)
	}
	lots, err := ledger.ListLots(ctx, "account-fill-ledger", "token-ledger")
	if err != nil || len(lots) != 2 || lots[0].OpeningFillKey == lots[1].OpeningFillKey {
		t.Fatalf("BUY lots = %#v err=%v", lots, err)
	}

	sell := integrationOrder("ledger-sell", "account-fill-ledger", "token-ledger", domain.SideSell, "4", "0.3")
	sell.Intent.Venue = "paper"
	sell.Intent.TargetLotID = lots[0].LotID
	sell.Intent.TimeInForce = domain.TimeInForceFAK
	sell = createAcknowledgedIntegrationOrder(t, ctx, repository, reservations, sell, "0xsell-ledger", now.Add(20*time.Second))
	sellConfirmedAt := now.Add(31 * time.Second)
	sold, err := processor.Process(ctx, sell, domain.Fill{
		VenueFillID: "trade-sell-1", LiquidityRole: domain.LiquidityRoleMaker,
		Status: domain.FillStatusConfirmed, Shares: "3.999", Price: "0.7",
		GrossNotional: "2.7993", FeeRateBPS: "0", PlatformFeeRate: "0", FeeExponent: "0",
		PlatformFee: "0", BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0",
		FeeSource: "TEST_AUTHORITATIVE_SETTLEMENT",
		MatchedAt: now.Add(30 * time.Second), VenueUpdatedAt: sellConfirmedAt,
		ObservedAt: sellConfirmedAt, ConfirmedAt: &sellConfirmedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if sold.Order.Status != domain.OrderStatusCancelled || !sold.Order.FilledSize.Equal("3.999") {
		t.Fatalf("FAK partial SELL = %#v", sold.Order)
	}
	position = *sold.Position
	if !position.TotalShares.Equal("6.001") || !position.AvailableShares.Equal("6") || !position.ReservedShares.Equal("0.001") ||
		!position.CostBasis.Equal("2.4005") || !position.RealizedPnL.Equal("0.7998") || position.IsDust {
		t.Fatalf("partial SELL position = %#v", position)
	}
	assertAccount(t, db, "account-fill-ledger", "98.3993", "98.3993", "0")
	assertPosition(t, db, "account-fill-ledger", "token-ledger", "6.001", "6", "0.001")
	if _, err := reservations.Reconcile(ctx, sold.Order); err != nil {
		t.Fatalf("finalize cancelled SELL reservation: %v", err)
	}
	assertPosition(t, db, "account-fill-ledger", "token-ledger", "6.001", "6.001", "0")
	balance, err := ledger.GetBalance(ctx, "account-fill-ledger")
	if err != nil || !balance.TotalBalance.Equal("98.3993") || !balance.AvailableBalance.Equal("98.3993") ||
		!balance.ReservedBalance.Equal("0") {
		t.Fatalf("funds snapshot = %#v err=%v", balance, err)
	}
	accountEvents, err := ledger.ListAccountEvents(ctx, "account-fill-ledger")
	if err != nil || len(accountEvents) != 3 {
		t.Fatalf("account events = %#v err=%v", accountEvents, err)
	}

	lots, err = ledger.ListLots(ctx, "account-fill-ledger", "token-ledger")
	if err != nil || len(lots) != 2 || !lots[0].RemainingShares.Equal("0.001") ||
		!lots[1].RemainingShares.Equal("6") {
		t.Fatalf("lot-addressed remaining lots = %#v err=%v", lots, err)
	}
	marked, err := ledger.MarkPosition(ctx, domain.PositionMark{
		ExecutionAccountID: "account-fill-ledger", TokenID: "token-ledger",
		Price: "0.2", ObservedAt: now.Add(40 * time.Second),
	})
	if err != nil || !marked.TotalShares.Equal("6.001") || !marked.MarketValue.Equal("1.2002") ||
		!marked.UnrealizedPnL.Equal("-1.2003") || marked.IsDust {
		t.Fatalf("marked dust position = %#v err=%v", marked, err)
	}
	events, err := ledger.ListPositionEvents(ctx, "account-fill-ledger", "token-ledger")
	if err != nil || len(events) != 4 {
		t.Fatalf("position events = %#v err=%v", events, err)
	}
	var outboxCount int
	if err := db.QueryRow(`SELECT count(*) FROM execution_outbox WHERE status='PENDING'`).Scan(&outboxCount); err != nil || outboxCount != 5 {
		t.Fatalf("pending outbox count=%d err=%v", outboxCount, err)
	}

	settledAt := now.Add(50 * time.Second)
	settled, err := ledger.MarkPositionSettled(ctx, "account-fill-ledger", "token-ledger", "data-api:condition-1", settledAt)
	if err != nil || settled.LifecycleStatus != domain.PositionLifecycleSettledPendingRedeem ||
		!settled.TotalShares.Equal("6.001") || !settled.CostBasis.Equal("2.4005") {
		t.Fatalf("settled position = %#v err=%v", settled, err)
	}
	lots, err = ledger.ListLots(ctx, "account-fill-ledger", "token-ledger")
	if err != nil || len(lots) != 2 || lots[0].Status != domain.PositionLotSettledPendingRedeem ||
		lots[1].Status != domain.PositionLotSettledPendingRedeem {
		t.Fatalf("settled lots = %#v err=%v", lots, err)
	}
	if _, err := ledger.MarkPositionSettled(ctx, "account-fill-ledger", "token-ledger", "data-api:condition-1", settledAt); err != nil {
		t.Fatalf("idempotent settlement mark: %v", err)
	}
	events, err = ledger.ListPositionEvents(ctx, "account-fill-ledger", "token-ledger")
	if err != nil || len(events) != 5 || events[4].EventType != domain.PositionEventSettled ||
		!events[4].SharesAfter.Equal("6.001") {
		t.Fatalf("settlement events = %#v err=%v", events, err)
	}
}

// TestBuyFeeReservationPostgresIntegration 验证手续费上限被预占，且超上限 Fill 不会消费未预占资金。
func TestBuyFeeReservationPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	repository, err := NewOrderRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	reservations, err := NewReservationManager(ReservationManagerParams{
		DB: db, MaxBuyFeeRateBPS: "250",
	})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := NewFillLedger(FillLedgerParams{DB: db})
	if err != nil {
		t.Fatal(err)
	}
	processor, err := fillprocessor.New(fillprocessor.Params{
		Orders: repository, Source: noFillsSource{}, Ledger: ledger,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	insertAccount(t, db, "account-buy-fee-cap", "0xbuyfeecap", "6.15", "6.15", "0")
	atCap := integrationOrder("buy-fee-cap", "account-buy-fee-cap", "token-buy-fee-cap", domain.SideBuy, "10", "0.6")
	atCap.Intent.Venue = "paper"
	atCap = createAcknowledgedIntegrationOrder(t, ctx, repository, reservations, atCap, "0xbuy-fee-cap", now)
	assertAccount(t, db, "account-buy-fee-cap", "6.15", "0", "6.15")
	confirmedAt := now.Add(10 * time.Second)
	application, err := processor.Process(ctx, atCap, domain.Fill{
		VenueFillID: "trade-buy-fee-cap-partial", LiquidityRole: domain.LiquidityRoleMaker,
		Status: domain.FillStatusConfirmed, Shares: "4", Price: "0.6",
		GrossNotional: "2.4", FeeRateBPS: "0", PlatformFeeRate: "0", FeeExponent: "0",
		PlatformFee: "0", BuilderFeeRateBPS: "250", BuilderFee: "0.06", TotalFee: "0.06",
		FeeSource: "TEST_AUTHORITATIVE_SETTLEMENT", MatchedAt: confirmedAt, VenueUpdatedAt: confirmedAt,
		ObservedAt: confirmedAt, ConfirmedAt: &confirmedAt,
	})
	if err != nil || !application.Applied || application.Order.Status != domain.OrderStatusPartiallyFilled ||
		!application.Order.TotalFees.Equal("0.06") {
		t.Fatalf("partial fee-capped fill application=%#v err=%v", application, err)
	}
	assertAccount(t, db, "account-buy-fee-cap", "3.69", "0", "3.69")

	finalAt := confirmedAt.Add(time.Second)
	application, err = processor.Process(ctx, application.Order, domain.Fill{
		VenueFillID: "trade-buy-fee-cap-final", LiquidityRole: domain.LiquidityRoleMaker,
		Status: domain.FillStatusConfirmed, Shares: "6", Price: "0.6",
		GrossNotional: "3.6", FeeRateBPS: "0", PlatformFeeRate: "0", FeeExponent: "0",
		PlatformFee: "0", BuilderFeeRateBPS: "250", BuilderFee: "0.09", TotalFee: "0.09",
		FeeSource: "TEST_AUTHORITATIVE_SETTLEMENT", MatchedAt: finalAt, VenueUpdatedAt: finalAt,
		ObservedAt: finalAt, ConfirmedAt: &finalAt,
	})
	if err != nil || !application.Applied || application.Order.Status != domain.OrderStatusFilled ||
		!application.Order.TotalFees.Equal("0.15") {
		t.Fatalf("final fee-capped fill application=%#v err=%v", application, err)
	}
	assertAccount(t, db, "account-buy-fee-cap", "0", "0", "0")

	insertAccount(t, db, "account-buy-fee-over-cap", "0xbuyfeeovercap", "10", "10", "0")
	overCap := integrationOrder("buy-fee-over-cap", "account-buy-fee-over-cap", "token-buy-fee-over-cap", domain.SideBuy, "10", "0.6")
	overCap.Intent.Venue = "paper"
	overCap = createAcknowledgedIntegrationOrder(t, ctx, repository, reservations, overCap, "0xbuy-fee-over-cap", now.Add(20*time.Second))
	assertAccount(t, db, "account-buy-fee-over-cap", "10", "3.85", "6.15")
	overCapAt := now.Add(30 * time.Second)
	if _, err := processor.Process(ctx, overCap, domain.Fill{
		VenueFillID: "trade-buy-fee-over-cap", LiquidityRole: domain.LiquidityRoleMaker,
		Status: domain.FillStatusConfirmed, Shares: "10", Price: "0.6",
		GrossNotional: "6", FeeRateBPS: "0", PlatformFeeRate: "0", FeeExponent: "0",
		PlatformFee: "0", BuilderFeeRateBPS: "251", BuilderFee: "0.1506", TotalFee: "0.1506",
		FeeSource: "TEST_AUTHORITATIVE_SETTLEMENT", MatchedAt: overCapAt, VenueUpdatedAt: overCapAt,
		ObservedAt: overCapAt, ConfirmedAt: &overCapAt,
	}); err == nil {
		t.Fatal("over-cap fill error = nil, want reservation constraint rejection")
	}
	// The account mutation precedes the reservation update inside FillLedger,
	// so this assertion proves the database constraint rolled back the whole
	// transaction rather than consuming unrelated available cash.
	assertAccount(t, db, "account-buy-fee-over-cap", "10", "3.85", "6.15")
}

// TestPolygonSettlementEvidencePostgresIntegration proves that the finalized
// OrderFilled proof and V2 curve inputs survive a round trip, participate in
// fill idempotency, and cannot be replayed under another local fill key.
func TestPolygonSettlementEvidencePostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	insertAccount(t, db, "account-v2-evidence", "0x1111111111111111111111111111111111111111", "10", "10", "0")
	repository, err := NewOrderRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	reservations, err := NewReservationManager(ReservationManagerParams{DB: db, MaxBuyFeeRateBPS: "0"})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := NewFillLedger(FillLedgerParams{DB: db})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	orderHash := "0x" + strings.Repeat("22", 32)
	transactionHash := "0x" + strings.Repeat("cd", 32)
	order := integrationOrder("v2-evidence", "account-v2-evidence", "42", domain.SideBuy, "1", "0.5")
	order = createAcknowledgedIntegrationOrder(t, ctx, repository, reservations, order, orderHash, now)
	confirmedAt := now.Add(10 * time.Second)
	fill := domain.Fill{
		Key:   fillprocessor.FillKey("polymarket", "trade-v2-evidence", order.ID),
		Venue: "polymarket", VenueFillID: "trade-v2-evidence", OrderID: order.ID,
		VenueOrderID: orderHash, ExecutionAccountID: order.Intent.ExecutionAccountID,
		MarketID: order.Intent.MarketID, ConditionID: order.Intent.ConditionID,
		TokenID: order.Intent.TokenID, Side: domain.SideBuy,
		LiquidityRole: domain.LiquidityRoleMaker, Status: domain.FillStatusConfirmed,
		Shares: "1", Price: "0.5", GrossNotional: "0.5", FeeRateBPS: "0",
		PlatformFeeRate: "0.25", FeeExponent: "2", PlatformFee: "0",
		BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0", NetCashDelta: "-0.5",
		TransactionHash: transactionHash, MatchedAt: confirmedAt,
		VenueUpdatedAt: confirmedAt, ObservedAt: confirmedAt, ConfirmedAt: &confirmedAt,
		FeeSource:        domain.FeeSourcePolygonV2OrderFilled,
		RawPayloadSHA256: strings.Repeat("a", 64),
		SettlementEvidence: &domain.SettlementEvidence{
			SchemaVersion:   domain.SettlementEvidenceSchemaV1,
			Source:          domain.FeeSourcePolygonV2OrderFilled,
			ChainID:         domain.SettlementEvidencePolygonChainID,
			ExchangeAddress: "0x" + strings.Repeat("ab", 20),
			TransactionHash: transactionHash, BlockNumber: 123,
			BlockHash: "0x" + strings.Repeat("ef", 32), LogIndex: 7, Confirmations: 64,
			OrderHash: orderHash, MakerAddress: "0x" + strings.Repeat("11", 20),
			TokenID: order.Intent.TokenID, Side: domain.SideBuy,
			MakerAmountBaseUnits: "500000", TakerAmountBaseUnits: "1000000",
			TotalFeeBaseUnits: "0", BuilderCode: "0x" + strings.Repeat("00", 32),
			BuilderFeeKnown: true, BuilderFeeBaseUnits: "0",
			BuilderFeeSource:   domain.SettlementEvidenceZeroBuilder,
			CollateralDecimals: 6, OutcomeTokenDecimals: 6,
		},
	}
	application, err := ledger.Record(ctx, order, fill)
	if err != nil || !application.Applied {
		t.Fatalf("record Polygon settlement: application=%#v err=%v", application, err)
	}
	stored, err := ledger.GetFill(ctx, fill.Key)
	if err != nil {
		t.Fatal(err)
	}
	if !stored.PlatformFeeRate.Equal("0.25") || !stored.FeeExponent.Equal("2") ||
		stored.SettlementEvidence == nil || stored.SettlementEvidence.LogIndex != 7 {
		t.Fatalf("stored V2 settlement = %#v", stored)
	}
	if err := verifyFillIdentity(stored, fill); err != nil {
		t.Fatalf("round-trip identity mismatch: %v", err)
	}

	changedRate := cloneFillEvidence(fill)
	changedRate.PlatformFeeRate = "0.3"
	if _, err := ledger.Record(ctx, application.Order, changedRate); !errors.Is(err, port.ErrFillConflict) {
		t.Fatalf("changed platform rate error = %v, want fill conflict", err)
	}
	growingConfirmations := cloneFillEvidence(fill)
	growingConfirmations.SettlementEvidence.Confirmations++
	replay, err := ledger.Record(ctx, application.Order, growingConfirmations)
	if err != nil || replay.Applied {
		t.Fatalf("confirmation-only replay application=%#v err=%v, want idempotent no-op", replay, err)
	}
	changedEvidence := cloneFillEvidence(fill)
	changedEvidence.SettlementEvidence.BlockHash = "0x" + strings.Repeat("88", 32)
	if _, err := ledger.Record(ctx, application.Order, changedEvidence); !errors.Is(err, port.ErrFillConflict) {
		t.Fatalf("changed immutable evidence error = %v, want fill conflict", err)
	}

	_, err = db.Exec(`
		INSERT INTO execution_fills (
			fill_key, venue, venue_fill_id, order_id, venue_order_id, execution_account_id,
			market_id, condition_id, token_id, side, liquidity_role, status, shares, price,
			gross_notional, fee_rate_bps, platform_fee_rate, fee_exponent, platform_fee,
			builder_fee_rate_bps, builder_fee, total_fee, net_cash_delta, settlement_evidence,
			fee_source, transaction_hash, raw_payload_sha256, matched_at, venue_updated_at,
			first_observed_at, last_observed_at, confirmed_at
		)
		SELECT 'replayed-' || fill_key, venue, 'replayed-' || venue_fill_id, order_id,
			venue_order_id, execution_account_id, market_id, condition_id, token_id, side,
			liquidity_role, status, shares, price, gross_notional, fee_rate_bps,
			platform_fee_rate, fee_exponent, platform_fee, builder_fee_rate_bps,
			builder_fee, total_fee, net_cash_delta, settlement_evidence, fee_source,
			transaction_hash, raw_payload_sha256, matched_at, venue_updated_at,
			first_observed_at, last_observed_at, confirmed_at
		FROM execution_fills WHERE fill_key = $1`, fill.Key)
	if err == nil {
		t.Fatal("replayed on-chain event insert error = nil, want unique-index rejection")
	}
}

// noFillsSource 表示后端使用的 noFillsSource 类型。
type noFillsSource struct{}

// ListOrderFills 返回模拟数据源中的测试列表。
func (noFillsSource) ListOrderFills(context.Context, domain.Order) ([]domain.Fill, error) {
	return nil, nil
}

// createAcknowledgedIntegrationOrder 模拟创建并返回测试记录。
func createAcknowledgedIntegrationOrder(
	t *testing.T,
	ctx context.Context,
	repository *OrderRepository,
	reservations *ReservationManager,
	order domain.Order,
	venueOrderID string,
	now time.Time,
) domain.Order {
	t.Helper()
	order.Status = domain.OrderStatusReceived
	order.FilledSize = "0"
	order.FilledNotional = "0"
	order.TotalFees = "0"
	order.CreatedAt = now
	order.UpdatedAt = now
	order.Revision = 1
	current, created, err := repository.Create(ctx, order)
	if err != nil || !created {
		t.Fatalf("create order: created=%v err=%v", created, err)
	}
	current, event := applyIntegrationTransition(t, current, domain.OrderStatusValidating, domain.TransitionTriggerValidation, now.Add(time.Second), "")
	if err := repository.Transition(ctx, current, event); err != nil {
		t.Fatal(err)
	}
	reserved, event := applyIntegrationTransition(t, current, domain.OrderStatusReserved, domain.TransitionTriggerReservation, now.Add(2*time.Second), "")
	if _, err := reservations.Reserve(ctx, reserved); err != nil {
		t.Fatal(err)
	}
	if err := repository.Transition(ctx, reserved, event); err != nil {
		t.Fatal(err)
	}
	attempt := domain.OrderAttempt{
		ID: "attempt:" + order.ID + ":1", OrderID: order.ID, Sequence: 1,
		Kind: domain.OrderAttemptSubmit, Outcome: domain.AttemptOutcomeStarted,
		StartedAt: now.Add(3 * time.Second), RequestFingerprint: "integration-fill",
	}
	submitting, event := applyIntegrationTransition(t, reserved, domain.OrderStatusSubmitting, domain.TransitionTriggerSubmit, attempt.StartedAt, attempt.ID)
	if err := repository.StartAttempt(ctx, submitting, event, attempt); err != nil {
		t.Fatal(err)
	}
	completedAt := now.Add(4 * time.Second)
	attempt.Outcome = domain.AttemptOutcomeSucceeded
	attempt.CompletedAt = &completedAt
	attempt.VenueOrderID = venueOrderID
	attempt.VenueStatus = "live"
	acknowledged, event := applyIntegrationTransition(t, submitting, domain.OrderStatusAcknowledged, domain.TransitionTriggerVenueResponse, completedAt, attempt.ID)
	acknowledged.VenueOrderID = venueOrderID
	event.VenueOrderID = venueOrderID
	if err := repository.FinishAttempt(ctx, acknowledged, event, attempt); err != nil {
		t.Fatal(err)
	}
	return acknowledged
}

// newIntegrationDatabase 创建测试所需的模拟对象。
func newIntegrationDatabase(t *testing.T, databaseURL string) *sql.DB {
	t.Helper()
	adminConfig, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse PostgreSQL URL: %v", err)
	}
	adminDB := stdlib.OpenDB(*adminConfig)
	t.Cleanup(func() { _ = adminDB.Close() })
	schema := fmt.Sprintf("trading_execution_test_%d", time.Now().UnixNano())
	if _, err := adminDB.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatalf("create test schema: %v", err)
	}
	t.Cleanup(func() { _, _ = adminDB.Exec(`DROP SCHEMA ` + schema + ` CASCADE`) })

	testConfig := adminConfig.Copy()
	testConfig.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*testConfig)
	db.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = db.Close() })
	for _, name := range []string{"0001_asset_reservations.sql", "0002_order_lifecycle.sql", "0003_fills_positions_ledger.sql", "0004_lot_addressed_strategy_exits.sql", "0005_position_exit_cycles.sql", "0006_reconciliation.sql", "0007_trade_history_read_model.sql", "0008_buy_fee_reservation_guard.sql", "0009_atomic_live_risk.sql", "0010_v2_settlement_evidence.sql"} {
		migration, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", name))
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		if _, err := db.Exec(string(migration)); err != nil {
			t.Fatalf("apply migration %s: %v", name, err)
		}
	}
	return db
}

// applyIntegrationTransition 应用测试状态变更。
func applyIntegrationTransition(
	t *testing.T,
	current domain.Order,
	target domain.OrderStatus,
	trigger domain.OrderTransitionTrigger,
	at time.Time,
	attemptID string,
) (domain.Order, domain.OrderEvent) {
	t.Helper()
	next, event, err := orderstate.Apply(current, orderstate.Transition{
		EventID:   fmt.Sprintf("event:%s:%d", current.ID, current.Revision+1),
		To:        target,
		Trigger:   trigger,
		AttemptID: attemptID,
		At:        at,
	})
	if err != nil {
		t.Fatal(err)
	}
	return next, event
}

// insertAccount 实现当前测试场景所需的辅助行为。
func insertAccount(t *testing.T, db *sql.DB, accountID, wallet, total, available, reserved string) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO execution_accounts (
			execution_account_id, wallet_address, total_balance, available_balance, reserved_balance
		) VALUES ($1, $2, $3::numeric, $4::numeric, $5::numeric)`, accountID, wallet, total, available, reserved)
	if err != nil {
		t.Fatal(err)
	}
}

// insertOpenLotFixture 实现当前测试场景所需的辅助行为。
func insertOpenLotFixture(t *testing.T, db *sql.DB, accountID, tokenID, shares string) {
	insertOpenLotFixtureNamed(t, db, accountID, tokenID, "lot-"+tokenID, shares)
}

// insertOpenLotFixtureNamed 实现当前测试场景所需的辅助行为。
func insertOpenLotFixtureNamed(t *testing.T, db *sql.DB, accountID, tokenID, lotID, shares string) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	orderID := "opening-order-" + accountID + "-" + lotID
	fillKey := "opening-fill-" + accountID + "-" + lotID
	intent := integrationOrder("opening-"+accountID+"-"+lotID, accountID, tokenID, domain.SideBuy, shares, "0.5").Intent
	payload, err := json.Marshal(intent)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		INSERT INTO execution_orders (
			order_id, client_order_id, execution_account_id, venue, market_id, token_id,
			intent, status, filled_size, average_fill_price, revision, created_at, updated_at
		) VALUES ($1,$2,$3,'polymarket','market-1',$4,$5::jsonb,'FILLED',$6::numeric,0.5,1,$7,$7)`,
		orderID, "client-"+orderID, accountID, tokenID, payload, shares, now)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		INSERT INTO execution_fills (
			fill_key, venue, venue_fill_id, order_id, venue_order_id, execution_account_id,
			market_id, condition_id, token_id, side, liquidity_role, status, shares, price,
			gross_notional, fee_rate_bps, platform_fee, builder_fee_rate_bps, builder_fee,
			total_fee, net_cash_delta, fee_source, matched_at, first_observed_at,
			last_observed_at, confirmed_at, applied_at
		) VALUES ($1,'polymarket',$2,$3,$4,$5,'market-1','condition-1',$6,'BUY','MAKER','CONFIRMED',
			$7::numeric,0.5,$7::numeric*0.5,0,0,0,0,0,-($7::numeric*0.5),'fixture',$8,$8,$8,$8,$8)`,
		fillKey, "venue-"+fillKey, orderID, "venue-"+orderID, accountID, tokenID, shares, now)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		INSERT INTO position_lots (
			lot_id, execution_account_id, market_id, condition_id, token_id, outcome_index,
			outcome_name, neg_risk, model_id, strategy_id, opening_order_id, opening_fill_key,
			original_shares, remaining_shares, original_cost, remaining_cost,
			average_entry_price, status, opened_at
		) VALUES ($1,$2,'market-1','condition-1',$3,0,'Yes',false,'model-a','strategy-v1',$4,$5,
			$6::numeric,$6::numeric,$6::numeric*0.5,$6::numeric*0.5,0.5,'OPEN',$7)`,
		lotID, accountID, tokenID, orderID, fillKey, shares, now)
	if err != nil {
		t.Fatal(err)
	}
}

// assertAccount 执行对应的测试断言。
func assertAccount(t *testing.T, db *sql.DB, accountID, total, available, reserved string) {
	t.Helper()
	var actualTotal, actualAvailable, actualReserved string
	err := db.QueryRow(`
		SELECT total_balance::text, available_balance::text, reserved_balance::text
		FROM execution_accounts WHERE execution_account_id = $1`, accountID).
		Scan(&actualTotal, &actualAvailable, &actualReserved)
	if err != nil {
		t.Fatal(err)
	}
	if !sameNumeric(actualTotal, total) || !sameNumeric(actualAvailable, available) || !sameNumeric(actualReserved, reserved) {
		t.Fatalf("account=(%s,%s,%s), want (%s,%s,%s)", actualTotal, actualAvailable, actualReserved, total, available, reserved)
	}
}

// assertPosition 执行对应的测试断言。
func assertPosition(t *testing.T, db *sql.DB, accountID, tokenID, total, available, reserved string) {
	t.Helper()
	var actualTotal, actualAvailable, actualReserved string
	err := db.QueryRow(`
		SELECT total_shares::text, available_shares::text, reserved_shares::text
		FROM execution_positions WHERE execution_account_id = $1 AND token_id = $2`, accountID, tokenID).
		Scan(&actualTotal, &actualAvailable, &actualReserved)
	if err != nil {
		t.Fatal(err)
	}
	if !sameNumeric(actualTotal, total) || !sameNumeric(actualAvailable, available) || !sameNumeric(actualReserved, reserved) {
		t.Fatalf("position=(%s,%s,%s), want (%s,%s,%s)", actualTotal, actualAvailable, actualReserved, total, available, reserved)
	}
}

// sameNumeric 实现当前测试场景所需的辅助行为。
func sameNumeric(left, right string) bool {
	return domain.Decimal(left).Equal(domain.Decimal(right))
}

// integrationOrder 实现当前测试场景所需的辅助行为。
func integrationOrder(orderID, accountID, tokenID string, side domain.Side, size, worstPrice string) domain.Order {
	intent := domain.OrderIntent{
		ModelID:            "model-a",
		StrategyID:         "strategy-v1",
		ExecutionAccountID: accountID,
		SignalID:           "signal-" + orderID,
		ClientOrderID:      "client-" + orderID,
		Venue:              "polymarket",
		MarketID:           "market-1",
		ConditionID:        "condition-1",
		TokenID:            tokenID,
		Side:               side,
		Type:               domain.OrderTypeLimit,
		Price:              "0.5",
		WorstPrice:         domain.Decimal(worstPrice),
		Size:               domain.Decimal(size),
		TimeInForce:        domain.TimeInForceGTC,
	}
	negRisk := false
	outcomeIndex := 0
	intent.ExpectedNegRisk = &negRisk
	intent.OutcomeIndex = &outcomeIndex
	intent.OutcomeName = "Yes"
	if side == domain.SideSell {
		intent.TargetLotID = "lot-" + tokenID
	}
	return domain.Order{
		ID:         "order-" + orderID,
		Intent:     intent,
		Status:     domain.OrderStatusAccepted,
		FilledSize: "0",
		Revision:   1,
	}
}
