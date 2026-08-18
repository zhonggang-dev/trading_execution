package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	manager, err := NewReservationManager(ReservationManagerParams{DB: db})
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
		if reservation.RemainingReservedBalance != "56.0" && reservation.RemainingReservedBalance != "56" {
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
	reservations, err := NewReservationManager(ReservationManagerParams{DB: db})
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
	buy = createAcknowledgedIntegrationOrder(t, ctx, repository, reservations, buy, "0xbuy-ledger", now)

	matched := domain.Fill{
		VenueFillID: "trade-buy-1", LiquidityRole: domain.LiquidityRoleMaker,
		Status: domain.FillStatusMatched, Shares: "4", Price: "0.5",
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
	sell.Intent.TargetLotID = lots[0].LotID
	sell.Intent.TimeInForce = domain.TimeInForceFAK
	sell = createAcknowledgedIntegrationOrder(t, ctx, repository, reservations, sell, "0xsell-ledger", now.Add(20*time.Second))
	sellConfirmedAt := now.Add(31 * time.Second)
	sold, err := processor.Process(ctx, sell, domain.Fill{
		VenueFillID: "trade-sell-1", LiquidityRole: domain.LiquidityRoleMaker,
		Status: domain.FillStatusConfirmed, Shares: "3.999", Price: "0.7",
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
	for _, name := range []string{"0001_asset_reservations.sql", "0002_order_lifecycle.sql", "0003_fills_positions_ledger.sql", "0004_lot_addressed_strategy_exits.sql", "0005_position_exit_cycles.sql", "0006_reconciliation.sql"} {
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
