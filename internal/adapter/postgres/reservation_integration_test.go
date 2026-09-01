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

func TestExecutionStrategyBindingEnabledUniquenessPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	insertAccount(t, db, "wallet-retired", "0xretired", "0", "0", "0")
	insertAccount(t, db, "wallet-active", "0xactive", "0", "0", "0")
	insertAccount(t, db, "wallet-conflict", "0xconflict", "0", "0", "0")

	if _, err := db.Exec(`
		INSERT INTO execution_strategy_bindings (
			model_id,strategy_id,execution_account_id,enabled,version
		) VALUES
			('model-a','multfactor_v1','wallet-retired',FALSE,2),
			('model-a','multfactor_v1','wallet-active',TRUE,1)`); err != nil {
		t.Fatalf("retain disabled route and insert replacement: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_strategy_bindings (
			model_id,strategy_id,execution_account_id,enabled,version
		) VALUES ('model-a','multfactor_v1','wallet-conflict',FALSE,1)`); err != nil {
		t.Fatalf("insert second disabled audit route: %v", err)
	}
	if _, err := db.Exec(`
		UPDATE execution_strategy_bindings
		SET enabled=TRUE,version=version+1
		WHERE model_id='model-a' AND strategy_id='multfactor_v1'
		  AND execution_account_id='wallet-conflict'`); err == nil {
		t.Fatal("enabling a second model/strategy route succeeded, want unique-index rejection")
	}

	var disabledRows, enabledRows int
	if err := db.QueryRow(`
		SELECT count(*) FILTER (WHERE NOT enabled), count(*) FILTER (WHERE enabled)
		FROM execution_strategy_bindings
		WHERE model_id='model-a' AND strategy_id='multfactor_v1'`).Scan(
		&disabledRows, &enabledRows,
	); err != nil {
		t.Fatal(err)
	}
	if disabledRows != 2 || enabledRows != 1 {
		t.Fatalf("binding history = disabled %d, enabled %d", disabledRows, enabledRows)
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

func TestReconciliationRecorderClosesFillLagDriftAfterAuthoritativeFill(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	const accountID = "account-fill-lag-resolution"
	insertAccount(t, db, accountID, "0xfilllag", "10", "10", "0")
	recorder, err := NewReconciliationRecorder(db)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	firstRun, err := (domain.ReconciliationRunParams{
		RunID: "run-fill-lag-open", ExecutionAccountID: accountID,
		Trigger: domain.ReconciliationTriggerScheduled, StartedAt: now,
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.Start(context.Background(), firstRun); err != nil {
		t.Fatal(err)
	}
	issues := []domain.ReconciliationIssue{
		{
			IssueID: "issue-fill-lag-balance", RunID: firstRun.RunID, Fingerprint: "fill-lag-balance",
			ExecutionAccountID: accountID, Type: domain.ReconciliationIssueBalanceDrift,
			Resolution: domain.ReconciliationResolutionManual, Status: domain.ReconciliationIssueOpen,
			LocalValue: "10", RemoteValue: "8.5", Source: "EVM_ERC20_ETH_CALL",
			Details: "fill reached chain before the local ledger", ObservedAt: now,
		},
		{
			IssueID: "issue-fill-lag-position", RunID: firstRun.RunID, Fingerprint: "fill-lag-position",
			ExecutionAccountID: accountID, Type: domain.ReconciliationIssuePhantomPosition,
			Resolution: domain.ReconciliationResolutionManual, Status: domain.ReconciliationIssueOpen,
			TokenID: "token-fill-lag", RemoteValue: "5", Source: "POLYMARKET_DATA_API",
			Details: "fill reached the position source before the local ledger", ObservedAt: now,
		},
		{
			IssueID: "issue-unrelated-external-trade", RunID: firstRun.RunID, Fingerprint: "unrelated-external-trade",
			ExecutionAccountID: accountID, Type: domain.ReconciliationIssueExternalTrade,
			Resolution: domain.ReconciliationResolutionManual, Status: domain.ReconciliationIssueOpen,
			VenueTradeID: "external-trade", Source: "POLYMARKET_CLOB",
			Details: "unrelated manual issue must remain open", ObservedAt: now,
		},
	}
	for _, issue := range issues {
		if err := recorder.RecordIssue(context.Background(), issue); err != nil {
			t.Fatal(err)
		}
	}
	firstCompleted := now.Add(time.Second)
	firstRun.Status, firstRun.CompletedAt = domain.ReconciliationRunAttentionRequired, &firstCompleted
	if err := recorder.Complete(context.Background(), firstRun); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`
		UPDATE execution_accounts
		SET total_balance=8.5,available_balance=8.5,reserved_balance=0
		WHERE execution_account_id=$1`, accountID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_positions (
			execution_account_id,market_id,condition_id,outcome_index,outcome_name,
			token_id,total_shares,available_shares,reserved_shares,cost_basis
		) VALUES ($1,'market-fill-lag','condition-fill-lag',0,'Yes',$2,5,5,0,1.5)`,
		accountID, "token-fill-lag"); err != nil {
		t.Fatal(err)
	}
	fillAppliedAt := now.Add(2500 * time.Millisecond)
	if _, err := db.Exec(`
		INSERT INTO execution_orders (
			order_id,client_order_id,execution_account_id,venue,market_id,token_id,
			intent,venue_order_id,status,filled_size,filled_notional,total_fees,
			average_fill_price,revision,created_at,updated_at
		) VALUES (
			'order-fill-lag','client-fill-lag',$1,'polymarket','market-fill-lag',$2,
			'{}'::jsonb,'venue-order-fill-lag','FILLED',5,1.5,0,0.3,1,$3,$4
		)`, accountID, "token-fill-lag", now, fillAppliedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_fills (
			fill_key,venue,venue_fill_id,order_id,venue_order_id,execution_account_id,
			market_id,token_id,side,liquidity_role,status,shares,price,gross_notional,
			fee_rate_bps,platform_fee,builder_fee_rate_bps,builder_fee,total_fee,
			net_cash_delta,fee_source,matched_at,first_observed_at,last_observed_at,
			confirmed_at,applied_at
		) VALUES (
			'fill-fill-lag','polymarket','venue-fill-lag','order-fill-lag',
			'venue-order-fill-lag',$1,'market-fill-lag',$2,'BUY','TAKER','CONFIRMED',
			5,0.3,1.5,0,0,0,0,0,-1.5,'TEST_FINALIZED_FILL',$3,$3,$3,$3,$3
		)`, accountID, "token-fill-lag", fillAppliedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_account_events (
			account_event_id,execution_account_id,event_type,order_id,fill_key,
			total_balance_delta,available_balance_delta,reserved_balance_delta,
			total_balance_after,available_balance_after,reserved_balance_after,occurred_at
		) VALUES (
			'account-event-fill-lag',$1,'BUY_FILL','order-fill-lag','fill-fill-lag',
			-1.5,-1.5,0,8.5,8.5,0,$2
		)`, accountID, fillAppliedAt); err != nil {
		t.Fatal(err)
	}
	secondRun, err := (domain.ReconciliationRunParams{
		RunID: "run-fill-lag-resolved", ExecutionAccountID: accountID,
		Trigger: domain.ReconciliationTriggerScheduled, StartedAt: now.Add(2 * time.Second),
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.Start(context.Background(), secondRun); err != nil {
		t.Fatal(err)
	}
	secondCompleted := now.Add(3 * time.Second)
	secondRun.Status, secondRun.CompletedAt = domain.ReconciliationRunCompleted, &secondCompleted
	secondRun.Summary["fills_applied"] = 1
	if err := recorder.Complete(context.Background(), secondRun); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(`
		SELECT issue_type,status,resolution
		FROM reconciliation_issues
		WHERE execution_account_id=$1 ORDER BY issue_type`, accountID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	statuses := map[domain.ReconciliationIssueType][2]string{}
	for rows.Next() {
		var issueType domain.ReconciliationIssueType
		var status, resolution string
		if err := rows.Scan(&issueType, &status, &resolution); err != nil {
			t.Fatal(err)
		}
		statuses[issueType] = [2]string{status, resolution}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, issueType := range []domain.ReconciliationIssueType{
		domain.ReconciliationIssueBalanceDrift, domain.ReconciliationIssuePhantomPosition,
	} {
		if got := statuses[issueType]; got != [2]string{"RESOLVED", "AUTOMATIC"} {
			t.Fatalf("%s status = %v, want RESOLVED/AUTOMATIC", issueType, got)
		}
	}
	if got := statuses[domain.ReconciliationIssueExternalTrade]; got != [2]string{"OPEN", "MANUAL_REVIEW"} {
		t.Fatalf("unrelated issue status = %v, want OPEN/MANUAL_REVIEW", got)
	}
	var summaryJSON []byte
	if err := db.QueryRow(`SELECT summary FROM reconciliation_runs WHERE run_id=$1`, secondRun.RunID).Scan(&summaryJSON); err != nil {
		t.Fatal(err)
	}
	var summary map[string]int
	if err := json.Unmarshal(summaryJSON, &summary); err != nil {
		t.Fatal(err)
	}
	if summary["issues_resolved"] != 2 || summary["issues_automatic"] != 2 || summary["issues_total"] != 2 {
		t.Fatalf("resolution summary = %#v, want two automatic resolutions", summary)
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

	t.Run("legacy daily cap does not block concurrent strategy orders", func(t *testing.T) {
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
		assertConcurrentReservationsSucceed(t, manager, orders)
		assertAccount(t, db, accountID, "1000", "900", "100")
		var authorized int
		if err := db.QueryRow(`
			SELECT count(*) FROM asset_reservations
			WHERE execution_account_id=$1 AND risk_policy_id <> ''
			  AND daily_risk_notional=50`, accountID).Scan(&authorized); err != nil || authorized != 2 {
			t.Fatalf("authorized daily reservations=%d err=%v, want 2", authorized, err)
		}
	})

	t.Run("legacy market cap does not block concurrent strategy orders", func(t *testing.T) {
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
		assertConcurrentReservationsSucceed(t, manager, orders)
		assertAccount(t, db, accountID, "1000", "900", "100")
	})

	t.Run("old strategy snapshot uses latest validated book for reservation and submit", func(t *testing.T) {
		accountID := "account-live-latest-book"
		insertAccount(t, db, accountID, "0xlivelatestbook", "100", "100", "0")
		provisionLiveRisk(t, db, liveRiskFixture{
			accountID: accountID, now: now, binding: true,
			maxOrder: "100", maxMarket: "100", maxStrategy: "100",
			maxWallet: "100", maxDaily: "100",
		})
		repository, err := NewOrderRepository(db)
		if err != nil {
			t.Fatal(err)
		}
		order := liveIntegrationOrder("latest-book", accountID, "token-latest-book", "10", "0.5", now)
		oldStrategySnapshotAt := now.Add(-4 * time.Hour)
		order.Intent.MarketSnapshotAt = &oldStrategySnapshotAt
		order.MarketValidation.StrategySnapshotAt = oldStrategySnapshotAt
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
			t.Fatalf("reserve using latest validated book: %v", err)
		}
		stored, event = applyIntegrationTransition(t, stored, domain.OrderStatusReserved, domain.TransitionTriggerReservation, now, "")
		if err := repository.Transition(context.Background(), stored, event); err != nil {
			t.Fatal(err)
		}
		attempt := domain.OrderAttempt{
			ID: "attempt:latest-book:1", OrderID: stored.ID, Sequence: 1,
			Kind: domain.OrderAttemptSubmit, Outcome: domain.AttemptOutcomeStarted,
			StartedAt: now,
		}
		stored, event = applyIntegrationTransition(t, stored, domain.OrderStatusSubmitting, domain.TransitionTriggerSubmit, now, attempt.ID)
		if err := repository.StartAttempt(context.Background(), stored, event, attempt); err != nil {
			t.Fatalf("submit gate rejected old strategy snapshot despite fresh validated book: %v", err)
		}
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
	latestBookAt := now.Add(-time.Second)
	signalAt := now.Add(-time.Second)
	order.Intent.MarketSnapshotAt = &marketAt
	order.Intent.SignalAt = &signalAt
	order.MarketValidation = &domain.MarketValidation{
		Mode: liveMarketValidationMode, ValidatedAt: now,
		StrategySnapshotAt: marketAt, LatestBookSourceAt: latestBookAt,
		WorstPrice: order.Intent.WorstPrice,
	}
	return order
}

func assertConcurrentReservationsSucceed(t *testing.T, manager *ReservationManager, orders []domain.Order) {
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
	successes := 0
	for err := range errorsChannel {
		if err == nil {
			successes++
			continue
		}
		t.Fatalf("concurrent strategy reservation error: %v", err)
	}
	if successes != len(orders) {
		t.Fatalf("successes=%d, want %d", successes, len(orders))
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

func TestPolymarketBuyPriceImprovementCanSettleMoreSharesThanRequested(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	insertAccount(t, db, "account-wallet-6-price-improvement", "0xwallet6", "100", "100", "0")
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
	processor, err := fillprocessor.New(fillprocessor.Params{Orders: repository, Source: noFillsSource{}, Ledger: ledger})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	order := integrationOrder(
		"wallet-6-price-improvement", "account-wallet-6-price-improvement",
		"42", domain.SideBuy, "30", "0.34",
	)
	order.Intent.Price = "0.34"
	orderHash := "0x" + strings.Repeat("22", 32)
	transactionHash := "0x" + strings.Repeat("cd", 32)
	order = createAcknowledgedIntegrationOrder(t, ctx, repository, reservations, order, orderHash, now)
	confirmedAt := now.Add(10 * time.Second)
	application, err := processor.Process(ctx, order, domain.Fill{
		VenueFillID: "trade-wallet-6", LiquidityRole: domain.LiquidityRoleTaker,
		Status: domain.FillStatusConfirmed, Shares: "30.147057", Price: "0.3383414507",
		GrossNotional: "10.199999", FeeRateBPS: "0", PlatformFeeRate: "0", FeeExponent: "0",
		PlatformFee: "0", BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0",
		FeeSource: domain.FeeSourcePolygonV2OrderFilled, TransactionHash: transactionHash,
		RawPayloadSHA256: strings.Repeat("a", 64), MatchedAt: confirmedAt, VenueUpdatedAt: confirmedAt,
		ObservedAt: confirmedAt, ConfirmedAt: &confirmedAt,
		SettlementEvidence: &domain.SettlementEvidence{
			SchemaVersion: domain.SettlementEvidenceSchemaV1, Source: domain.FeeSourcePolygonV2OrderFilled,
			ChainID: domain.SettlementEvidencePolygonChainID, ExchangeAddress: "0x" + strings.Repeat("ab", 20),
			TransactionHash: transactionHash, BlockNumber: 123, BlockHash: "0x" + strings.Repeat("ef", 32),
			LogIndex: 7, Confirmations: 64, OrderHash: orderHash, MakerAddress: "0x" + strings.Repeat("11", 20),
			TokenID: order.Intent.TokenID, Side: domain.SideBuy, MakerAmountBaseUnits: "10199999",
			TakerAmountBaseUnits: "30147057", TotalFeeBaseUnits: "0", BuilderCode: "0x" + strings.Repeat("00", 32),
			BuilderFeeKnown: true, BuilderFeeBaseUnits: "0", BuilderFeeSource: domain.SettlementEvidenceZeroBuilder,
			CollateralDecimals: 6, OutcomeTokenDecimals: 6,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !application.Applied || application.Order.Status != domain.OrderStatusFilled ||
		!application.Order.FilledSize.Equal("30.147057") || !application.Order.FilledNotional.Equal("10.199999") {
		t.Fatalf("price-improved fill application = %#v", application)
	}
	assertAccount(t, db, "account-wallet-6-price-improvement", "89.800001", "89.800001", "0")
	assertPosition(t, db, "account-wallet-6-price-improvement", "42", "30.147057", "30.147057", "0")
	var settledShares, remainingBalance, reservationStatus string
	if err := db.QueryRow(`
		SELECT settled_shares::text, remaining_reserved_balance::text, status
		FROM asset_reservations WHERE order_id=$1`, order.ID,
	).Scan(&settledShares, &remainingBalance, &reservationStatus); err != nil {
		t.Fatal(err)
	}
	if !sameNumeric(settledShares, "30") || !sameNumeric(remainingBalance, "0") ||
		reservationStatus != string(domain.ReservationStatusSettled) {
		t.Fatalf("reservation settlement = %s/%s/%s", settledShares, remainingBalance, reservationStatus)
	}
	if _, err := reservations.Reconcile(ctx, application.Order); err != nil {
		t.Fatalf("reconcile price-improved reservation: %v", err)
	}
	assertAccount(t, db, "account-wallet-6-price-improvement", "89.800001", "89.800001", "0")
}

func TestIOCMultipleFillsCorrectCancelledOrderToFilledPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	insertAccount(t, db, "account-ioc-multi-fill", "0xiocmultifill", "100", "100", "0")
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
	processor, err := fillprocessor.New(fillprocessor.Params{Orders: repository, Source: noFillsSource{}, Ledger: ledger})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	order := integrationOrder("ioc-multi-fill", "account-ioc-multi-fill", "token-ioc-multi", domain.SideBuy, "2", "0.6")
	order.Intent.Venue = "paper"
	order.Intent.TimeInForce = domain.TimeInForceIOC
	order = createAcknowledgedIntegrationOrder(t, ctx, repository, reservations, order, "venue-ioc-multi", now)

	confirmedFill := func(id, shares string, at time.Time) domain.Fill {
		confirmedAt := at
		notional, err := domain.Decimal(shares).Multiply("0.5")
		if err != nil {
			t.Fatal(err)
		}
		return domain.Fill{
			VenueFillID: id, LiquidityRole: domain.LiquidityRoleTaker,
			Status: domain.FillStatusConfirmed, Shares: domain.Decimal(shares), Price: "0.5",
			GrossNotional: domain.Decimal(notional.FloatString(8)), FeeRateBPS: "0", PlatformFeeRate: "0", FeeExponent: "0",
			PlatformFee: "0", BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0", FeeSource: "TEST_KALSHI_IOC",
			MatchedAt: at, VenueUpdatedAt: at, ObservedAt: at, ConfirmedAt: &confirmedAt,
		}
	}
	firstFill := confirmedFill("ioc-fill-1", "1", now.Add(10*time.Second))
	first, err := processor.Process(ctx, order, firstFill)
	if err != nil {
		t.Fatal(err)
	}
	if first.Order.Status != domain.OrderStatusCancelled || !first.Order.FilledSize.Equal("1") {
		t.Fatalf("first IOC fill = %#v", first.Order)
	}
	duplicate, err := processor.Process(ctx, first.Order, firstFill)
	if err != nil || !duplicate.Duplicate || duplicate.Applied {
		t.Fatalf("duplicate IOC fill = %#v err=%v", duplicate, err)
	}
	second, err := processor.Process(ctx, duplicate.Order, confirmedFill("ioc-fill-2", "1", now.Add(11*time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	if second.Order.Status != domain.OrderStatusFilled || !second.Order.FilledSize.Equal("2") ||
		!second.Order.FilledNotional.Equal("1") {
		t.Fatalf("completed IOC order = %#v", second.Order)
	}
	assertAccount(t, db, "account-ioc-multi-fill", "99", "99", "0")
	assertPosition(t, db, "account-ioc-multi-fill", "token-ioc-multi", "2", "2", "0")
}

func TestExternalPositionBaselinePostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	const (
		accountID  = "account-external-position-baseline"
		baselineID = "baseline-external-position"
	)
	insertAccount(t, db, accountID, "0xexternalpositionbaseline", "20", "20", "0")

	if _, err := db.Exec(`
		INSERT INTO execution_external_position_baselines (
			baseline_id, execution_account_id, source, observed_at, evidence, actor, reason
		) VALUES ($1,$2,'POLYMARKET_DATA_API',clock_timestamp(),'{"snapshot_sha256":"empty"}'::jsonb,
		          'integration-test','empty seal must fail')`, baselineID, accountID); err == nil {
		t.Fatal("empty external position baseline header succeeded")
	}

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`
		INSERT INTO execution_external_position_baseline_items (
			baseline_id, execution_account_id, token_id, condition_id,
			outcome_index, outcome_name, neg_risk, shares
		) VALUES
			($1,$2,'baseline-token-yes','baseline-condition',0,'YES',FALSE,5),
			($1,$2,'baseline-token-no','baseline-condition',1,'NO',FALSE,3)`, baselineID, accountID); err != nil {
		t.Fatalf("insert items before deferred header: %v", err)
	}
	if _, err := tx.Exec(`
		INSERT INTO execution_external_position_baselines (
			baseline_id, execution_account_id, source, observed_at, evidence, actor, reason
		) VALUES ($1,$2,'POLYMARKET_DATA_API',clock_timestamp(),
		          '{"snapshot_sha256":"two-items"}'::jsonb,'integration-test','initial account cutover')`, baselineID, accountID); err != nil {
		t.Fatalf("seal populated external position baseline: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit deferred external position baseline FK: %v", err)
	}

	repository, err := NewExternalPositionBaselineRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	baselines, err := repository.ListExternalPositionBaselines(context.Background(), accountID)
	if err != nil || len(baselines) != 2 || baselines[0].BaselineID != baselineID || baselines[1].BaselineID != baselineID {
		t.Fatalf("external position baselines = %#v err=%v", baselines, err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_external_position_baseline_items (
			baseline_id, execution_account_id, token_id, condition_id,
			outcome_index, outcome_name, neg_risk, shares
		) VALUES ($1,$2,'baseline-token-late','baseline-condition',2,'LATE',FALSE,1)`, baselineID, accountID); err == nil {
		t.Fatal("late item extended a sealed external position baseline")
	}
	if _, err := db.Exec(`UPDATE execution_external_position_baselines SET reason='mutated' WHERE baseline_id=$1`, baselineID); err == nil {
		t.Fatal("append-only external position baseline UPDATE succeeded")
	}
	if _, err := db.Exec(`DELETE FROM execution_external_position_baseline_items WHERE baseline_id=$1`, baselineID); err == nil {
		t.Fatal("append-only external position baseline item DELETE succeeded")
	}
	if _, err := db.Exec(`TRUNCATE execution_external_position_baselines`); err == nil {
		t.Fatal("append-only external position baseline TRUNCATE succeeded")
	}
}

func TestExternalPositionBaselineRequiresExactWalletMigrationEvidence(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	const (
		accountID  = "account-wallet-migration-baseline"
		baselineID = "baseline-before-wallet-migration"
		oldWallet  = "0x1111111111111111111111111111111111111111"
		newWallet  = "0x2222222222222222222222222222222222222222"
	)
	insertAccount(t, db, accountID, oldWallet, "20", "20", "0")
	observedAt := time.Now().UTC().Add(-time.Hour)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`
		INSERT INTO execution_external_position_baseline_items (
			baseline_id, execution_account_id, token_id, condition_id,
			outcome_index, outcome_name, neg_risk, shares
		) VALUES ($1,$2,'wallet-migration-token','wallet-migration-condition',0,'YES',FALSE,5)`, baselineID, accountID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`
		INSERT INTO execution_external_position_baselines (
			baseline_id, execution_account_id, source, observed_at, evidence, actor, reason
		) VALUES ($1,$2,'POLYMARKET_DATA_API',$3,jsonb_build_object('wallet_address',$4),
		          'integration-test','pre-migration ownership')`, baselineID, accountID, observedAt, oldWallet); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	repository, err := NewExternalPositionBaselineRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	baselines, err := repository.ListExternalPositionBaselines(context.Background(), accountID)
	if err != nil || len(baselines) != 1 {
		t.Fatalf("pre-migration baselines = %#v err=%v", baselines, err)
	}
	if _, err := db.Exec(`UPDATE execution_accounts SET wallet_address=$2 WHERE execution_account_id=$1`, accountID, newWallet); err != nil {
		t.Fatal(err)
	}
	baselines, err = repository.ListExternalPositionBaselines(context.Background(), accountID)
	if err != nil || len(baselines) != 1 {
		t.Fatalf("wallet mismatch without evidence did not fail closed: %#v err=%v", baselines, err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_wallet_migrations (
			migration_id, execution_account_id, old_wallet_address, new_wallet_address,
			chain_id, collateral_asset, collateral_balance_before, collateral_balance_after,
			occurred_at, evidence, actor, reason
		) VALUES (
			'wallet-migration-test',$1,$2,$3,137,'pUSD',20,20,$4,
			'{"transaction_hash":"0xabc"}'::jsonb,'integration-test','deposit wallet migration'
		)`, accountID, oldWallet, newWallet, observedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	baselines, err = repository.ListExternalPositionBaselines(context.Background(), accountID)
	if err != nil || len(baselines) != 0 {
		t.Fatalf("exact wallet migration did not retire old baseline: %#v err=%v", baselines, err)
	}
	if _, err := db.Exec(`UPDATE execution_wallet_migrations SET reason='mutated' WHERE migration_id='wallet-migration-test'`); err == nil {
		t.Fatal("append-only wallet migration UPDATE succeeded")
	}
	if _, err := db.Exec(`
		INSERT INTO reconciliation_runs (
			run_id,execution_account_id,trigger,status,summary,error,started_at,completed_at
		) VALUES (
			'wallet-migration-old-run',$1,'STARTUP','ATTENTION_REQUIRED','{}'::jsonb,'',$2,$3
		)`, accountID, observedAt.Add(2*time.Minute), observedAt.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO reconciliation_issues (
			issue_id,run_id,fingerprint,execution_account_id,issue_type,resolution,status,
			token_id,source,details,observed_at
		) VALUES
			('wallet-migration-baseline-issue','wallet-migration-old-run','migration-baseline',$1,
			 'EXTERNAL_POSITION_BASELINE_DRIFT','MANUAL_REVIEW','OPEN','wallet-migration-token',
			 'POLYMARKET_DATA_API','old wallet baseline is absent',$2),
			('wallet-migration-unrelated-issue','wallet-migration-old-run','migration-unrelated',$1,
			 'EXTERNAL_TRADE','MANUAL_REVIEW','OPEN','',
			 'POLYMARKET_DATA_API','unrelated issue must remain open',$2)`, accountID, observedAt.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	completedAt := observedAt.Add(5 * time.Minute)
	resolveTx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := resolveWalletMigrationIssues(context.Background(), resolveTx, domain.ReconciliationRun{
		RunID:              "wallet-migration-clean-run",
		ExecutionAccountID: accountID,
		Status:             domain.ReconciliationRunCompleted,
		StartedAt:          observedAt.Add(4 * time.Minute),
		CompletedAt:        &completedAt,
	})
	if err != nil {
		resolveTx.Rollback()
		t.Fatal(err)
	}
	if resolved != 1 {
		resolveTx.Rollback()
		t.Fatalf("resolved wallet migration issues = %d, want 1", resolved)
	}
	if err := resolveTx.Commit(); err != nil {
		t.Fatal(err)
	}
	var baselineStatus, unrelatedStatus string
	if err := db.QueryRow(`
		SELECT max(status) FILTER (WHERE issue_id='wallet-migration-baseline-issue'),
		       max(status) FILTER (WHERE issue_id='wallet-migration-unrelated-issue')
		FROM reconciliation_issues WHERE execution_account_id=$1`, accountID).Scan(&baselineStatus, &unrelatedStatus); err != nil {
		t.Fatal(err)
	}
	if baselineStatus != "RESOLVED" || unrelatedStatus != "OPEN" {
		t.Fatalf("wallet migration issue statuses = %s/%s, want RESOLVED/OPEN", baselineStatus, unrelatedStatus)
	}
}

// TestExternalPositionAdoptionPostgresIntegration proves that an immutable
// unmanaged baseline can become one managed Python-visible lot without a fake
// execution order or fill, while its effective unmanaged remainder becomes
// zero.
func TestExternalPositionAdoptionPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	const (
		accountID     = "account-external-position-adoption"
		baselineID    = "baseline-external-position-adoption"
		batchID       = "batch-external-position-adoption"
		adoptionID    = "external-adoption-1"
		dispositionID = "external-adoption-disposition-1"
		lotID         = "lot-external-adoption-1"
		eventID       = "position-event-external-adoption-1"
		tokenID       = "token-external-adoption-1"
		conditionID   = "condition-external-adoption-1"
	)
	observedAt := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	openedAt := observedAt.Add(-72 * time.Hour)
	adoptedAt := observedAt.Add(time.Minute)
	insertAccount(t, db, accountID, "0xexternalpositionadoption", "20", "20", "0")
	if _, err := db.Exec(`
		INSERT INTO execution_strategy_bindings (
			model_id,strategy_id,execution_account_id,enabled
		) VALUES ('echo','multfactor_v1',$1,TRUE)`, accountID); err != nil {
		t.Fatal(err)
	}
	baselineTx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := baselineTx.Exec(`
		INSERT INTO execution_external_position_baseline_items (
			baseline_id,execution_account_id,token_id,condition_id,
			outcome_index,outcome_name,neg_risk,shares
		) VALUES ($1,$2,$3,$4,1,'No',TRUE,1.5)`, baselineID, accountID, tokenID, conditionID); err != nil {
		t.Fatal(err)
	}
	if _, err := baselineTx.Exec(`
		INSERT INTO execution_external_position_baselines (
			baseline_id,execution_account_id,source,observed_at,evidence,actor,reason
		) VALUES ($1,$2,'POLYMARKET_DATA_API',$3,'{"snapshot_sha256":"adoption"}',
		          'integration-test','initial ownership capture')`, baselineID, accountID, observedAt); err != nil {
		t.Fatal(err)
	}
	if err := baselineTx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`
		INSERT INTO execution_external_position_dispositions (
			disposition_id,adjustment_batch_id,baseline_id,execution_account_id,
			condition_id,token_id,disposition_kind,transition_sequence,
			shares_before,shares_delta,shares_after,occurred_at,evidence,actor,reason
		) VALUES ($1,$2,$3,$4,$5,$6,'ADOPTION',1,1.5,1.5,0,$7,
		          '{"remote_shares":"1.5"}','integration-test','authorized ownership transfer')`,
		dispositionID, batchID, baselineID, accountID, conditionID, tokenID, adoptedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`
		INSERT INTO execution_external_position_adoptions (
			external_adoption_id,adjustment_batch_id,disposition_id,baseline_id,
			execution_account_id,lot_id,position_event_id,market_id,condition_id,token_id,
			outcome_index,outcome_name,neg_risk,model_id,strategy_id,shares,remaining_cost,
			average_entry_price,opened_at,adopted_at,evidence,actor,reason
		) VALUES ($1,$2,$3,$4,$5,$6,$7,'market-adoption',$8,$9,1,'No',TRUE,
		          'echo','multfactor_v1',1.5,0.615,0.4,$10,$11,
		          '{"fifo":"proven"}','integration-test','authorized ownership transfer')`,
		adoptionID, batchID, dispositionID, baselineID, accountID, lotID, eventID,
		conditionID, tokenID, openedAt, adoptedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`
		INSERT INTO execution_positions (
			execution_account_id,market_id,condition_id,token_id,outcome_index,outcome_name,
			total_shares,available_shares,reserved_shares,cost_basis,average_cost_price,
			realized_pnl,market_value,unrealized_pnl,is_dust,lifecycle_status,created_at,updated_at
		) VALUES ($1,'market-adoption',$2,$3,1,'No',1.5,1.5,0,0.615,0.41,
		          0,0,0,FALSE,'OPEN',$4,$4)`, accountID, conditionID, tokenID, adoptedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`
		INSERT INTO position_lots (
			lot_id,execution_account_id,market_id,condition_id,token_id,outcome_index,
			outcome_name,neg_risk,model_id,strategy_id,opening_order_id,opening_fill_key,
			external_adoption_id,original_shares,remaining_shares,original_cost,
			remaining_cost,average_entry_price,status,opened_at
		) VALUES ($1,$2,'market-adoption',$3,$4,1,'No',TRUE,'echo','multfactor_v1',
		          NULL,NULL,$5,1.5,1.5,0.615,0.615,0.4,'OPEN',$6)`,
		lotID, accountID, conditionID, tokenID, adoptionID, openedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`
		INSERT INTO position_events (
			position_event_id,event_type,execution_account_id,market_id,token_id,
			order_id,fill_key,external_adoption_id,model_id,strategy_id,shares_delta,
			cash_delta,cost_basis_delta,realized_pnl_delta,shares_after,cost_basis_after,
			average_cost_after,realized_pnl_after,unrealized_pnl_after,occurred_at
		) VALUES ($1,'ADOPTED',$2,'market-adoption',$3,'','',$4,'echo','multfactor_v1',
		          1.5,0,0.615,0,1.5,0.615,0.41,0,0,$5)`,
		eventID, accountID, tokenID, adoptionID, adoptedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`
		INSERT INTO execution_external_position_adjustment_batches (
			adjustment_batch_id,schema_version,observed_at,evidence,evidence_sha256,actor,reason
		) VALUES ($1,'trading.external-position-adjustment.v1',$2,
		          '{"cutover":"integration"}',$3,'integration-test','authorized ownership transfer')`,
		batchID, adoptedAt, strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit external position adoption: %v", err)
	}

	baselineRepository, err := NewExternalPositionBaselineRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	baselines, err := baselineRepository.ListExternalPositionBaselines(context.Background(), accountID)
	if err != nil || len(baselines) != 0 {
		t.Fatalf("effective unmanaged baselines = %#v err=%v, want empty", baselines, err)
	}
	ledger, err := NewFillLedger(FillLedgerParams{DB: db})
	if err != nil {
		t.Fatal(err)
	}
	lots, err := ledger.ListOpenLots(context.Background(), accountID)
	if err != nil || len(lots) != 1 || lots[0].OpeningOrderID != "external-adoption:"+adoptionID ||
		lots[0].OpeningFillKey != "external-adoption:"+adoptionID || !lots[0].RemainingShares.Equal("1.5") {
		t.Fatalf("adopted open lots = %#v err=%v", lots, err)
	}
	exits, err := ledger.ListOpenPositionExitTrades(context.Background(), accountID)
	if err != nil || len(exits) != 1 || exits[0].VenueTradeID != "external-adoption:"+adoptionID ||
		exits[0].OpeningOrderID != "external-adoption:"+adoptionID {
		t.Fatalf("adopted exit trades = %#v err=%v", exits, err)
	}
	if _, err := db.Exec(`UPDATE position_lots SET external_adoption_id=NULL WHERE lot_id=$1`, lotID); err == nil {
		t.Fatal("adopted lot origin mutation succeeded")
	}
	if _, err := db.Exec(`UPDATE position_lots SET original_cost=0.5 WHERE lot_id=$1`, lotID); err == nil {
		t.Fatal("adopted lot original economics mutation succeeded")
	}
	if _, err := db.Exec(`UPDATE position_events SET shares_after=1 WHERE position_event_id=$1`, eventID); err == nil {
		t.Fatal("adopted position event mutation succeeded")
	}

	orderRepository, err := NewOrderRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	reservations, err := NewReservationManager(ReservationManagerParams{DB: db, MaxBuyFeeRateBPS: "0"})
	if err != nil {
		t.Fatal(err)
	}
	processor, err := fillprocessor.New(fillprocessor.Params{
		Orders: orderRepository, Source: noFillsSource{}, Ledger: ledger,
	})
	if err != nil {
		t.Fatal(err)
	}
	sellAt := adoptedAt.Add(time.Hour)
	sell := integrationOrder("external-adoption-sell", accountID, tokenID, domain.SideSell, "1.5", "0.3")
	sell.Intent.Venue = "paper"
	sell.Intent.ModelID = "echo"
	sell.Intent.StrategyID = "multfactor_v1"
	sell.Intent.MarketID = "market-adoption"
	sell.Intent.ConditionID = conditionID
	sell.Intent.TargetLotID = lotID
	sellOutcomeIndex := 1
	sellNegRisk := true
	sell.Intent.OutcomeIndex = &sellOutcomeIndex
	sell.Intent.OutcomeName = "No"
	sell.Intent.ExpectedNegRisk = &sellNegRisk
	sell.Intent.TimeInForce = domain.TimeInForceFOK
	sell = createAcknowledgedIntegrationOrder(
		t, context.Background(), orderRepository, reservations, sell,
		"0xexternal-adoption-sell", sellAt,
	)
	confirmedAt := sellAt.Add(10 * time.Second)
	sold, err := processor.Process(context.Background(), sell, domain.Fill{
		VenueFillID: "trade-external-adoption-sell", LiquidityRole: domain.LiquidityRoleMaker,
		Status: domain.FillStatusConfirmed, Shares: "1.5", Price: "0.5",
		GrossNotional: "0.75", FeeRateBPS: "0", PlatformFeeRate: "0", FeeExponent: "0",
		PlatformFee: "0", BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0",
		FeeSource: "TEST_AUTHORITATIVE_SETTLEMENT",
		MatchedAt: sellAt.Add(9 * time.Second), VenueUpdatedAt: confirmedAt,
		ObservedAt: confirmedAt, ConfirmedAt: &confirmedAt,
	})
	if err != nil {
		t.Fatalf("sell adopted lot: %v", err)
	}
	if sold.Position == nil || sold.Order.Status != domain.OrderStatusFilled ||
		!sold.Position.TotalShares.Equal("0") || !sold.Position.CostBasis.Equal("0") ||
		!sold.Position.RealizedPnL.Equal("0.135") {
		t.Fatalf("sold adopted position = %#v", sold)
	}
	assertAccount(t, db, accountID, "20.75", "20.75", "0")
	lots, err = ledger.ListLots(context.Background(), accountID, tokenID)
	if err != nil || len(lots) != 1 || lots[0].Status != domain.PositionLotClosed ||
		!lots[0].RemainingShares.Equal("0") || !lots[0].RemainingCost.Equal("0") ||
		lots[0].OpeningOrderID != "external-adoption:"+adoptionID {
		t.Fatalf("closed adopted lot = %#v err=%v", lots, err)
	}
}

// TestExternalCashAdjustmentChainPostgresIntegration proves that an external
// sell batch starts from the account's pre-adjustment balance and that every
// later transaction is appended in one strict, gap-free cash chain.
func TestExternalCashAdjustmentChainPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	baselineObservedAt := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)

	type fixture struct {
		accountID  string
		baselineID string
		batchID    string
		tokenID    string
		condition  string
	}
	setup := func(t *testing.T, suffix string) fixture {
		t.Helper()
		value := fixture{
			accountID:  "account-external-cash-" + suffix,
			baselineID: "baseline-external-cash-" + suffix,
			batchID:    "batch-external-cash-" + suffix,
			tokenID:    "token-external-cash-" + suffix,
			condition:  "condition-external-cash-" + suffix,
		}
		insertAccount(t, db, value.accountID, "0xexternalcash"+suffix, "20", "20", "0")
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err := tx.Exec(`
			INSERT INTO execution_external_position_baseline_items (
				baseline_id,execution_account_id,token_id,condition_id,
				outcome_index,outcome_name,neg_risk,shares
			) VALUES ($1,$2,$3,$4,0,'Yes',FALSE,10)`,
			value.baselineID, value.accountID, value.tokenID, value.condition); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(`
			INSERT INTO execution_external_position_baselines (
				baseline_id,execution_account_id,source,observed_at,evidence,actor,reason
			) VALUES ($1,$2,'POLYMARKET_DATA_API',$3,'{"snapshot_sha256":"cash-chain"}',
			          'integration-test','initial ownership capture')`,
			value.baselineID, value.accountID, baselineObservedAt); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		return value
	}
	insertDisposition := func(
		t *testing.T,
		tx *sql.Tx,
		value fixture,
		sequence int,
		before, delta, after string,
		occurredAt time.Time,
		transactionHash string,
	) {
		t.Helper()
		_, err := tx.Exec(`
			INSERT INTO execution_external_position_dispositions (
				disposition_id,adjustment_batch_id,baseline_id,execution_account_id,
				condition_id,token_id,disposition_kind,transition_sequence,
				shares_before,shares_delta,shares_after,venue_trade_id,venue_order_id,
				transaction_hash,occurred_at,evidence,actor,reason
			) VALUES ($1,$2,$3,$4,$5,$6,'EXTERNAL_SELL',$7,$8::numeric,$9::numeric,$10::numeric,
			          $11,$12,$13,$14,'{"receipt":"finalized"}',
			          'integration-test','account for external sell')`,
			fmt.Sprintf("disposition-%s-%d", value.accountID, sequence), value.batchID,
			value.baselineID, value.accountID, value.condition, value.tokenID, sequence,
			before, delta, after, fmt.Sprintf("venue-trade-%s-%d", value.accountID, sequence),
			fmt.Sprintf("venue-order-%s-%d", value.accountID, sequence), transactionHash, occurredAt)
		if err != nil {
			t.Fatal(err)
		}
	}
	insertCash := func(
		tx *sql.Tx,
		value fixture,
		cashID, eventID, before, delta, after string,
		occurredAt time.Time,
		transactionHash string,
	) error {
		_, err := tx.Exec(`
			INSERT INTO execution_external_cash_adjustments (
				cash_adjustment_id,adjustment_batch_id,execution_account_id,transaction_hash,
				asset,total_delta,available_delta,reserved_delta,balance_before,balance_after,
				account_event_id,occurred_at,evidence,actor,reason
			) VALUES ($1,$2,$3,$4,'pUSD',$5::numeric,$5::numeric,0,$6::numeric,$7::numeric,
			          $8,$9,'{"receipt":"finalized"}',
			          'integration-test','account for external sell')`,
			cashID, value.batchID, value.accountID, transactionHash, delta, before, after,
			eventID, occurredAt)
		return err
	}

	t.Run("forged opening balance is rejected", func(t *testing.T) {
		value := setup(t, "forged-opening")
		occurredAt := baselineObservedAt.Add(time.Minute)
		transactionHash := "0x" + strings.Repeat("11", 32)
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		insertDisposition(t, tx, value, 1, "10", "1", "9", occurredAt, transactionHash)
		err = insertCash(tx, value, "cash-forged-opening", "event-forged-opening", "19", "2", "21", occurredAt, transactionHash)
		if err == nil || !strings.Contains(err.Error(), "must start at the current available account balance") {
			t.Fatalf("forged opening cash balance error = %v", err)
		}
	})

	t.Run("broken chain is rejected", func(t *testing.T) {
		value := setup(t, "broken-chain")
		firstAt := baselineObservedAt.Add(time.Minute)
		secondAt := firstAt.Add(time.Minute)
		firstHash := "0x" + strings.Repeat("22", 32)
		secondHash := "0x" + strings.Repeat("33", 32)
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		insertDisposition(t, tx, value, 1, "10", "1", "9", firstAt, firstHash)
		insertDisposition(t, tx, value, 2, "9", "1", "8", secondAt, secondHash)
		if err := insertCash(tx, value, "cash-broken-1", "event-broken-1", "20", "2", "22", firstAt, firstHash); err != nil {
			t.Fatal(err)
		}
		err = insertCash(tx, value, "cash-broken-2", "event-broken-2", "21", "3", "24", secondAt, secondHash)
		if err == nil || !strings.Contains(err.Error(), "does not continue the batch balance chain") {
			t.Fatalf("broken cash chain error = %v", err)
		}
	})

	t.Run("out of order insert is rejected", func(t *testing.T) {
		value := setup(t, "out-of-order")
		firstAt := baselineObservedAt.Add(time.Minute)
		secondAt := firstAt.Add(time.Minute)
		firstHash := "0x" + strings.Repeat("44", 32)
		secondHash := "0x" + strings.Repeat("55", 32)
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		insertDisposition(t, tx, value, 1, "10", "1", "9", firstAt, firstHash)
		insertDisposition(t, tx, value, 2, "9", "1", "8", secondAt, secondHash)
		if err := insertCash(tx, value, "cash-order-later", "event-order-later", "20", "2", "22", secondAt, secondHash); err != nil {
			t.Fatal(err)
		}
		err = insertCash(tx, value, "cash-order-earlier", "event-order-earlier", "22", "3", "25", firstAt, firstHash)
		if err == nil || !strings.Contains(err.Error(), "strict occurred_at/id order") {
			t.Fatalf("out-of-order cash insert error = %v", err)
		}
	})

	t.Run("multiple transactions commit one exact chain", func(t *testing.T) {
		value := setup(t, "valid-chain")
		firstAt := baselineObservedAt.Add(time.Minute)
		secondAt := firstAt.Add(time.Minute)
		firstHash := "0x" + strings.Repeat("66", 32)
		secondHash := "0x" + strings.Repeat("77", 32)
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		insertDisposition(t, tx, value, 1, "10", "2", "8", firstAt, firstHash)
		insertDisposition(t, tx, value, 2, "8", "3", "5", secondAt, secondHash)
		if err := insertCash(tx, value, "cash-valid-1", "event-valid-1", "20", "2", "22", firstAt, firstHash); err != nil {
			t.Fatal(err)
		}
		if err := insertCash(tx, value, "cash-valid-2", "event-valid-2", "22", "3", "25", secondAt, secondHash); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(`
			INSERT INTO execution_account_events (
				account_event_id,execution_account_id,event_type,order_id,fill_key,
				total_balance_delta,available_balance_delta,reserved_balance_delta,
				total_balance_after,available_balance_after,reserved_balance_after,occurred_at
			) VALUES
				('event-valid-1',$1,'EXTERNAL_POSITION_DISPOSITION','','',2,2,0,22,22,0,$2),
				('event-valid-2',$1,'EXTERNAL_POSITION_DISPOSITION','','',3,3,0,25,25,0,$3)`,
			value.accountID, firstAt, secondAt); err != nil {
			t.Fatal(err)
		}
		accountResult, err := tx.Exec(`
			UPDATE execution_accounts
			SET total_balance=25,available_balance=25,reserved_balance=0,
			    version=version+1,updated_at=$2
			WHERE execution_account_id=$1 AND total_balance=20
			  AND available_balance=20 AND reserved_balance=0`, value.accountID, secondAt)
		if err != nil {
			t.Fatal(err)
		}
		if !oneRow(accountResult) {
			t.Fatal("external cash chain account compare-and-swap did not update exactly one row")
		}
		if _, err := tx.Exec(`
			INSERT INTO execution_external_position_adjustment_batches (
				adjustment_batch_id,schema_version,observed_at,evidence,evidence_sha256,actor,reason
			) VALUES ($1,'trading.external-position-adjustment.v1',$2,
			          '{"cutover":"cash-chain"}',$3,'integration-test','account for external sells')`,
			value.batchID, secondAt, strings.Repeat("b", 64)); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit valid external cash chain: %v", err)
		}

		assertAccount(t, db, value.accountID, "25", "25", "0")
		repository, err := NewExternalPositionBaselineRepository(db)
		if err != nil {
			t.Fatal(err)
		}
		baselines, err := repository.ListExternalPositionBaselines(context.Background(), value.accountID)
		if err != nil || len(baselines) != 1 || !baselines[0].Shares.Equal("5") {
			t.Fatalf("effective baseline after external sells = %#v err=%v", baselines, err)
		}
		dispositions, err := repository.ListExternalPositionDispositionTrades(context.Background(), value.accountID)
		if err != nil || len(dispositions) != 2 {
			t.Fatalf("external sell disposition identities = %#v err=%v", dispositions, err)
		}
	})
}

// TestExternalPositionAdjustmentBatchSealPostgresIntegration proves that the
// child-first/header-last protocol cannot be extended by a concurrent late
// child after the immutable batch header starts sealing.
func TestExternalPositionAdjustmentBatchSealPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	const (
		accountID = "account-external-batch-seal"
		batchID   = "batch-external-batch-seal"
	)
	observedAt := time.Date(2026, 8, 18, 11, 0, 0, 0, time.UTC)
	insertAccount(t, db, accountID, "0xexternalbatchseal", "20", "20", "0")

	sealing, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sealing.Rollback()
	if _, err := sealing.Exec(`
		INSERT INTO execution_external_position_dispositions (
			disposition_id,adjustment_batch_id,baseline_id,execution_account_id,
			condition_id,token_id,disposition_kind,transition_sequence,
			shares_before,shares_delta,shares_after,venue_trade_id,venue_order_id,
			transaction_hash,occurred_at,evidence,actor,reason
		) VALUES (
			'seed-false-attribution',$1,NULL,$2,'condition-seal','token-seal',
			'FALSE_ATTRIBUTION',NULL,0,0,0,'trade-seed','order-seed','',$3,
			'{"account_scope":"absent"}','integration-test','seed immutable audit'
		)`, batchID, accountID, observedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := sealing.Exec(`
		INSERT INTO execution_external_position_adjustment_batches (
			adjustment_batch_id,schema_version,observed_at,evidence,evidence_sha256,actor,reason
		) VALUES ($1,'trading.external-position-adjustment.v1',$2,
		          '{"cutover":"seal-race"}',$3,'integration-test','seal immutable audit')`,
		batchID, observedAt, strings.Repeat("c", 64)); err != nil {
		t.Fatal(err)
	}

	lateResult := make(chan error, 1)
	go func() {
		_, lateErr := db.Exec(`
			INSERT INTO execution_external_position_dispositions (
				disposition_id,adjustment_batch_id,baseline_id,execution_account_id,
				condition_id,token_id,disposition_kind,transition_sequence,
				shares_before,shares_delta,shares_after,venue_trade_id,venue_order_id,
				transaction_hash,occurred_at,evidence,actor,reason
			) VALUES (
				'late-false-attribution',$1,NULL,$2,'condition-late','token-late',
				'FALSE_ATTRIBUTION',NULL,0,0,0,'trade-late','order-late','',$3,
				'{"account_scope":"absent"}','integration-test','late immutable audit'
			)`, batchID, accountID, observedAt)
		lateResult <- lateErr
	}()

	// The late insert must be waiting on the shared advisory seal lock while
	// the header transaction remains uncommitted.
	select {
	case lateErr := <-lateResult:
		t.Fatalf("late child returned before batch seal committed: %v", lateErr)
	case <-time.After(150 * time.Millisecond):
	}
	if err := sealing.Commit(); err != nil {
		t.Fatalf("commit external adjustment batch seal: %v", err)
	}
	select {
	case lateErr := <-lateResult:
		if lateErr == nil || !strings.Contains(lateErr.Error(), "already sealed") {
			t.Fatalf("late child error = %v, want already sealed", lateErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("late child did not fail after immutable batch seal committed")
	}
	var childCount int
	if err := db.QueryRow(`
		SELECT count(*) FROM execution_external_position_dispositions
		WHERE adjustment_batch_id=$1`, batchID).Scan(&childCount); err != nil {
		t.Fatal(err)
	}
	if childCount != 1 {
		t.Fatalf("sealed batch child count = %d, want exactly 1", childCount)
	}

	// A historical false-attribution audit must not permanently block a later
	// stronger account-scoped proof for the same trade identity.
	const (
		baselineID     = "baseline-external-batch-correction"
		accountedBatch = "batch-external-batch-correction"
	)
	baselineTx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer baselineTx.Rollback()
	if _, err := baselineTx.Exec(`
		INSERT INTO execution_external_position_baseline_items (
			baseline_id,execution_account_id,token_id,condition_id,
			outcome_index,outcome_name,neg_risk,shares
		) VALUES ($1,$2,'token-positive','condition-positive',0,'Yes',FALSE,1)`,
		baselineID, accountID); err != nil {
		t.Fatal(err)
	}
	if _, err := baselineTx.Exec(`
		INSERT INTO execution_external_position_baselines (
			baseline_id,execution_account_id,source,observed_at,evidence,actor,reason
		) VALUES ($1,$2,'POLYMARKET_DATA_API',$3,
		          '{"snapshot_sha256":"correction"}','integration-test','seal ownership boundary')`,
		baselineID, accountID, observedAt.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := baselineTx.Commit(); err != nil {
		t.Fatal(err)
	}
	correction, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer correction.Rollback()
	if _, err := correction.Exec(`
		INSERT INTO execution_external_position_dispositions (
			disposition_id,adjustment_batch_id,baseline_id,execution_account_id,
			condition_id,token_id,disposition_kind,transition_sequence,
			shares_before,shares_delta,shares_after,venue_trade_id,venue_order_id,
			transaction_hash,occurred_at,evidence,actor,reason
		) VALUES (
			'accounted-after-false',$1,$2,$3,'condition-seal','token-seal',
			'BASELINE_ACCOUNTED',NULL,0,0,0,'trade-seed','order-seed','',$4,
			'{"account_scope":"proven"}','integration-test','correct prior attribution'
		)`, accountedBatch, baselineID, accountID, observedAt); err != nil {
		t.Fatalf("insert stronger accounting proof after false attribution: %v", err)
	}
	if _, err := correction.Exec(`
		INSERT INTO execution_external_position_adjustment_batches (
			adjustment_batch_id,schema_version,observed_at,evidence,evidence_sha256,actor,reason
		) VALUES ($1,'trading.external-position-adjustment.v1',$2,
		          '{"cutover":"correction"}',$3,'integration-test','seal corrected audit')`,
		accountedBatch, observedAt.Add(time.Hour), strings.Repeat("d", 64)); err != nil {
		t.Fatal(err)
	}
	if err := correction.Commit(); err != nil {
		t.Fatalf("commit stronger accounting proof after false attribution: %v", err)
	}
	repository, err := NewExternalPositionBaselineRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	accounted, err := repository.ListExternalPositionDispositionTrades(context.Background(), accountID)
	if err != nil || len(accounted) != 1 || accounted[0].VenueTradeID != "trade-seed" {
		t.Fatalf("corrected accounted trade identities = %#v err=%v", accounted, err)
	}
}

// TestPositionLotModelRoutePostgresIntegration proves that a logical model can
// close one explicitly routed legacy lot without rewriting its opening BUY.
func TestPositionLotModelRoutePostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	const (
		accountID    = "account-model-route"
		tokenID      = "token-model-route"
		lotID        = "lot-model-route"
		originModel  = "gemini-3.6-flash"
		logicalModel = "gemini_masked"
	)
	insertAccount(t, db, accountID, "0xmodelroute", "20", "20", "0")
	if _, err := db.Exec(`
		INSERT INTO execution_positions (
			execution_account_id, market_id, condition_id, token_id, outcome_index, outcome_name,
			total_shares, available_shares, reserved_shares, cost_basis, average_cost_price
		) VALUES ($1,'market-1','condition-1',$2,0,'Yes',5,5,0,2.5,0.5)`, accountID, tokenID); err != nil {
		t.Fatal(err)
	}
	insertOpenLotFixtureNamedWithModel(t, db, accountID, tokenID, lotID, "5", originModel)

	reservations, err := NewReservationManager(ReservationManagerParams{DB: db, MaxBuyFeeRateBPS: "0"})
	if err != nil {
		t.Fatal(err)
	}
	unrouted := integrationOrder("unrouted-model-sell", accountID, tokenID, domain.SideSell, "1", "0.4")
	unrouted.Intent.ModelID = logicalModel
	unrouted.Intent.TargetLotID = lotID
	if _, err := reservations.Reserve(context.Background(), unrouted); err == nil {
		t.Fatal("unrouted logical SELL reserve error = nil, want fail-closed identity rejection")
	} else {
		assertRejectionCode(t, err, "TARGET_LOT_IDENTITY_MISMATCH")
	}

	if _, err := db.Exec(`
		INSERT INTO position_lot_model_routes (
			lot_id, origin_model_id, logical_model_id, reason, actor
		) VALUES ($1,'wrong-origin',$2,'test cutover','integration-test')`, lotID, logicalModel); err == nil {
		t.Fatal("route with a mismatched origin model succeeded")
	}
	if _, err := db.Exec(`
		UPDATE execution_risk_global_control
		SET kill_switch=FALSE, reason='route guard test', version=version+1
		WHERE singleton=TRUE`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO position_lot_model_routes (
			lot_id, origin_model_id, logical_model_id, reason, actor
		) VALUES ($1,$2,$3,'test cutover','integration-test')`, lotID, originModel, logicalModel); err == nil {
		t.Fatal("route INSERT succeeded with the global kill switch disabled")
	}
	if _, err := db.Exec(`
		UPDATE execution_risk_global_control
		SET kill_switch=TRUE, reason='route guard test complete', version=version+1
		WHERE singleton=TRUE`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO asset_reservations (
			order_id, client_order_id, intent_fingerprint,
			execution_account_id, strategy_id, market_id, token_id, target_lot_id, side,
			requested_shares, reserve_unit_price,
			initial_reserved_balance, remaining_reserved_balance,
			initial_reserved_shares, remaining_reserved_shares,
			settled_shares, settled_notional, status
		) VALUES (
			'route-blocking-reservation','route-blocking-client','route-blocking-fingerprint',
			$1,'strategy-v1','market-1',$2,$3,'SELL',
			1,0,0,0,1,1,0,0,'ACTIVE'
		)`, accountID, tokenID, lotID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO position_lot_model_routes (
			lot_id, origin_model_id, logical_model_id, reason, actor
		) VALUES ($1,$2,$3,'test cutover','integration-test')`, lotID, originModel, logicalModel); err == nil {
		t.Fatal("route INSERT succeeded with an active lot reservation")
	}
	if _, err := db.Exec(`DELETE FROM asset_reservations WHERE order_id='route-blocking-reservation'`); err != nil {
		t.Fatal(err)
	}

	blockingOrder := integrationOrder("route-blocking-order", accountID, tokenID, domain.SideBuy, "1", "0.4")
	blockingPayload, err := json.Marshal(blockingOrder.Intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_orders (
			order_id, client_order_id, execution_account_id, venue, market_id, token_id,
			intent, status, filled_size, revision, created_at, updated_at
		) VALUES (
			'route-blocking-order','route-blocking-order-client',$1,'paper','market-1',$2,
			$3::jsonb,'RECEIVED',0,1,clock_timestamp(),clock_timestamp()
		)`, accountID, tokenID, blockingPayload); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO position_lot_model_routes (
			lot_id, origin_model_id, logical_model_id, reason, actor
		) VALUES ($1,$2,$3,'test cutover','integration-test')`, lotID, originModel, logicalModel); err == nil {
		t.Fatal("route INSERT succeeded with a non-terminal account order")
	}
	if _, err := db.Exec(`UPDATE execution_orders SET status='REJECTED' WHERE order_id='route-blocking-order'`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`
		INSERT INTO strategy_decision_runs (
			cycle_id, input_id, decision_at, model_id, strategy_id, execution_account_id,
			input_payload, output_payload, order_submission_enabled, decided_at
		) VALUES (
			'route-blocking-cycle','route-blocking-input',clock_timestamp(),$1,'strategy-v1',$2,
			'{}'::jsonb,'{}'::jsonb,TRUE,clock_timestamp()
		)`, logicalModel, accountID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO strategy_order_intent_deliveries (
			client_order_id, cycle_id, sequence_no, intent_payload, status
		) VALUES ('route-blocking-delivery','route-blocking-cycle',0,$1::jsonb,'PENDING')`, blockingPayload); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO position_lot_model_routes (
			lot_id, origin_model_id, logical_model_id, reason, actor
		) VALUES ($1,$2,$3,'test cutover','integration-test')`, lotID, originModel, logicalModel); err == nil {
		t.Fatal("route INSERT succeeded with a pending account intent")
	}
	if _, err := db.Exec(`DELETE FROM strategy_order_intent_deliveries WHERE client_order_id='route-blocking-delivery'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM strategy_decision_runs WHERE cycle_id='route-blocking-cycle'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO position_lot_model_routes (
			lot_id, origin_model_id, logical_model_id, reason, actor
		) VALUES ($1,$2,$3,'test cutover','integration-test')`, lotID, originModel, logicalModel); err != nil {
		t.Fatal(err)
	}

	ledger, err := NewFillLedger(FillLedgerParams{DB: db})
	if err != nil {
		t.Fatal(err)
	}
	openLots, err := ledger.ListOpenLots(context.Background(), accountID)
	if err != nil || len(openLots) != 1 || openLots[0].OriginModelID != originModel || openLots[0].ModelID != logicalModel {
		t.Fatalf("routed open lots = %#v err=%v", openLots, err)
	}
	exitTrades, err := ledger.ListOpenPositionExitTrades(context.Background(), accountID)
	if err != nil || len(exitTrades) != 1 || exitTrades[0].OriginModelID != originModel || exitTrades[0].ModelID != logicalModel {
		t.Fatalf("routed exit trades = %#v err=%v", exitTrades, err)
	}

	repository, err := NewOrderRepository(db)
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
	sell := integrationOrder("routed-model-sell", accountID, tokenID, domain.SideSell, "5", "0.4")
	sell.Intent.ModelID = logicalModel
	sell.Intent.Venue = "paper"
	sell.Intent.TargetLotID = lotID
	sell = createAcknowledgedIntegrationOrder(t, ctx, repository, reservations, sell, "0xrouted-model-sell", now)
	if _, err := db.Exec(`
		UPDATE asset_reservations
		SET risk_policy_id='route-test-policy', risk_policy_version=1,
		    risk_day=CURRENT_DATE, daily_risk_notional=2
		WHERE order_id=$1`, sell.ID); err != nil {
		t.Fatal(err)
	}
	account := ExpectedExecutionAccount{ExecutionAccountID: accountID, WalletAddress: "0xmodelroute"}
	if err := CheckLiveLedgerBootstrap(ctx, db, []ExpectedExecutionAccount{account}); err != nil {
		t.Fatalf("routed active SELL bootstrap: %v", err)
	}

	confirmedAt := now.Add(10 * time.Second)
	sold, err := processor.Process(ctx, sell, domain.Fill{
		VenueFillID: "trade-routed-model-sell", LiquidityRole: domain.LiquidityRoleMaker,
		Status: domain.FillStatusConfirmed, Shares: "5", Price: "0.6",
		GrossNotional: "3", FeeRateBPS: "0", PlatformFeeRate: "0", FeeExponent: "0",
		PlatformFee: "0", BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0",
		FeeSource: "TEST_AUTHORITATIVE_SETTLEMENT",
		MatchedAt: confirmedAt, VenueUpdatedAt: confirmedAt,
		ObservedAt: confirmedAt, ConfirmedAt: &confirmedAt,
	})
	if err != nil || !sold.Applied || sold.Order.Status != domain.OrderStatusFilled {
		t.Fatalf("routed SELL fill = %#v err=%v", sold, err)
	}
	lots, err := ledger.ListLots(ctx, accountID, tokenID)
	if err != nil || len(lots) != 1 || lots[0].Status != domain.PositionLotClosed ||
		lots[0].OriginModelID != originModel || lots[0].ModelID != logicalModel {
		t.Fatalf("closed routed lot = %#v err=%v", lots, err)
	}
	var storedOrigin string
	if err := db.QueryRow(`SELECT model_id FROM position_lots WHERE lot_id=$1`, lotID).Scan(&storedOrigin); err != nil || storedOrigin != originModel {
		t.Fatalf("stored origin model=%q err=%v", storedOrigin, err)
	}

	history, err := NewTradeHistoryRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	page, err := history.ListTradeHistory(ctx, domain.TradeHistoryFilter{ExecutionAccountID: accountID})
	if err != nil || len(page.Items) != 2 {
		t.Fatalf("routed trade history = %#v err=%v", page, err)
	}
	modelsBySide := make(map[domain.Side]string, len(page.Items))
	for _, item := range page.Items {
		modelsBySide[item.Side] = item.ModelID
	}
	if modelsBySide[domain.SideBuy] != originModel || modelsBySide[domain.SideSell] != logicalModel {
		t.Fatalf("historical models by side = %#v", modelsBySide)
	}
	if _, err := db.Exec(`UPDATE position_lot_model_routes SET reason='mutated' WHERE lot_id=$1`, lotID); err == nil {
		t.Fatal("append-only route UPDATE succeeded")
	}
	if _, err := db.Exec(`DELETE FROM position_lot_model_routes WHERE lot_id=$1`, lotID); err == nil {
		t.Fatal("append-only route DELETE succeeded")
	}
	if _, err := db.Exec(`TRUNCATE position_lot_model_routes`); err == nil {
		t.Fatal("append-only route TRUNCATE succeeded")
	}
	if _, err := db.Exec(`UPDATE position_lots SET model_id='mutated' WHERE lot_id=$1`, lotID); err == nil {
		t.Fatal("immutable origin model UPDATE succeeded")
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
	for _, name := range []string{"0001_asset_reservations.sql", "0002_order_lifecycle.sql", "0003_fills_positions_ledger.sql", "0004_lot_addressed_strategy_exits.sql", "0005_position_exit_cycles.sql", "0006_reconciliation.sql", "0007_trade_history_read_model.sql", "0008_buy_fee_reservation_guard.sql", "0009_atomic_live_risk.sql", "0010_v2_settlement_evidence.sql", "0011_live_operations.sql", "0012_strategy_decision_cycles.sql", "0013_strategy_intent_deliveries.sql", "0014_external_position_ownership_baselines.sql", "0015_position_lot_model_routes.sql", "0016_external_position_dispositions.sql", "0017_enabled_strategy_binding_uniqueness.sql", "0018_execution_wallet_migrations.sql", "0019_edge_distribution_read_index.sql", "0020_latest_validated_book_risk_freshness.sql"} {
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
	insertOpenLotFixtureNamedWithModel(t, db, accountID, tokenID, lotID, shares, "model-a")
}

// insertOpenLotFixtureNamedWithModel inserts an opening BUY and immutable lot
// with the requested historical model identity.
func insertOpenLotFixtureNamedWithModel(t *testing.T, db *sql.DB, accountID, tokenID, lotID, shares, modelID string) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	orderID := "opening-order-" + accountID + "-" + lotID
	fillKey := "opening-fill-" + accountID + "-" + lotID
	intent := integrationOrder("opening-"+accountID+"-"+lotID, accountID, tokenID, domain.SideBuy, shares, "0.5").Intent
	intent.ModelID = modelID
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
		) VALUES ($1,$2,'market-1','condition-1',$3,0,'Yes',false,$8,'strategy-v1',$4,$5,
			$6::numeric,$6::numeric,$6::numeric*0.5,$6::numeric*0.5,0.5,'OPEN',$7)`,
		lotID, accountID, tokenID, orderID, fillKey, shares, now, modelID)
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
