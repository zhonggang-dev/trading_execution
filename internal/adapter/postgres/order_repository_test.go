package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestLiveSubmitRiskRejectionMapsOnlyKnownLocalTriggerErrors(t *testing.T) {
	err := &pgconn.PgError{Code: "P0001", Message: "GLOBAL_KILL_SWITCH blocks order ord-1"}
	mapped := liveSubmitRiskRejection(err)
	var rejection *port.Rejection
	if !errors.As(mapped, &rejection) || rejection.Code != "GLOBAL_KILL_SWITCH" {
		t.Fatalf("mapped error = %#v", mapped)
	}
	for _, candidate := range []error{
		&pgconn.PgError{Code: "P0001", Message: "unrelated trigger failure"},
		&pgconn.PgError{Code: "23514", Message: "GLOBAL_KILL_SWITCH blocks order ord-1"},
		errors.New("GLOBAL_KILL_SWITCH blocks order ord-1"),
	} {
		if mapped := liveSubmitRiskRejection(candidate); mapped != nil {
			t.Fatalf("unexpected mapping for %T: %v", candidate, mapped)
		}
	}
}

func TestOrderRepositoryRecoverySelectionPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	const accountID = "account-reconciliation-selection"
	insertAccount(t, db, accountID, "0xreconciliationselection", "100", "100", "0")
	repository, err := NewOrderRepository(db)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	old := now.Add(-7 * 24 * time.Hour)
	recent := now.Add(-time.Hour)
	fixtures := []struct {
		name              string
		side              domain.Side
		status            domain.OrderStatus
		failureCode       string
		updatedAt         time.Time
		reservationStatus domain.ReservationStatus
		emptyReservation  bool
		wantPending       bool
		wantReconcile     bool
	}{
		{name: "fill-pending-clob", side: domain.SideBuy, status: domain.OrderStatusUnknown, failureCode: "CLOB_FILL_DETAILS_UNAVAILABLE", updatedAt: old, wantReconcile: true},
		{name: "fill-pending-durable", side: domain.SideBuy, status: domain.OrderStatusUnknown, failureCode: "VENUE_FILL_EVIDENCE_PENDING", updatedAt: old, wantReconcile: true},
		{name: "ordinary-unknown", side: domain.SideBuy, status: domain.OrderStatusUnknown, failureCode: "CLOB_ORDER_NOT_FOUND", updatedAt: old, wantPending: true, wantReconcile: true},
		{name: "old-manual-unprotected", side: domain.SideBuy, status: domain.OrderStatusManualReview, updatedAt: old},
		{name: "old-manual-active-buy", side: domain.SideBuy, status: domain.OrderStatusManualReview, failureCode: "CLOB_FILL_DETAILS_UNAVAILABLE", updatedAt: old, reservationStatus: domain.ReservationStatusActive, wantReconcile: true},
		{name: "old-manual-unrelated", side: domain.SideBuy, status: domain.OrderStatusManualReview, failureCode: "CLOB_ORDER_NOT_FOUND", updatedAt: old, reservationStatus: domain.ReservationStatusActive, wantReconcile: true},
		{name: "old-manual-uncertain-sell", side: domain.SideSell, status: domain.OrderStatusManualReview, failureCode: "HTTP_TIMEOUT", updatedAt: old, reservationStatus: domain.ReservationStatusReconciliationRequired, wantReconcile: true},
		{name: "old-uncertain-buy", side: domain.SideBuy, status: domain.OrderStatusRejected, updatedAt: old, reservationStatus: domain.ReservationStatusReconciliationRequired},
		{name: "old-active-sell", side: domain.SideSell, status: domain.OrderStatusCancelled, updatedAt: old, reservationStatus: domain.ReservationStatusActive, wantReconcile: true},
		{name: "old-uncertain-cancelled-buy", side: domain.SideBuy, status: domain.OrderStatusCancelled, updatedAt: old, reservationStatus: domain.ReservationStatusReconciliationRequired, wantReconcile: true},
		{name: "old-empty-cancelled-sell", side: domain.SideSell, status: domain.OrderStatusCancelled, updatedAt: old, reservationStatus: domain.ReservationStatusActive, emptyReservation: true},
		{name: "old-uncertain-sell", side: domain.SideSell, status: domain.OrderStatusRejected, updatedAt: old, reservationStatus: domain.ReservationStatusReconciliationRequired},
		{name: "old-unprotected-sell", side: domain.SideSell, status: domain.OrderStatusCancelled, updatedAt: old},
		{name: "old-terminal-buy", side: domain.SideBuy, status: domain.OrderStatusRejected, updatedAt: old},
		{name: "recent-terminal-buy", side: domain.SideBuy, status: domain.OrderStatusRejected, updatedAt: recent, wantReconcile: true},
	}
	fixtureIDs := make(map[string]struct{}, len(fixtures))
	for _, fixture := range fixtures {
		tokenID := "token-" + fixture.name
		order := integrationOrder("reconcile-"+fixture.name, accountID, tokenID, fixture.side, "1", "0.5")
		order.Status = fixture.status
		order.FailureCode = fixture.failureCode
		order.VenueOrderID = "venue-" + fixture.name
		order.CreatedAt = old
		order.UpdatedAt = fixture.updatedAt
		insertReconciliationSelectionOrder(t, db, order)
		fixtureIDs[order.ID] = struct{}{}
		if fixture.reservationStatus == "" {
			continue
		}
		if fixture.side == domain.SideSell {
			insertOpenLotFixtureNamed(t, db, accountID, tokenID, order.Intent.TargetLotID, "1")
		}
		insertReconciliationSelectionReservation(t, db, order, fixture.reservationStatus, fixture.emptyReservation)
	}
	insertAccount(t, db, "account-retired-selection", "0xretiredselection", "0", "0", "0")
	retiredOrder := integrationOrder(
		"reconcile-retired", "account-retired-selection", "token-retired",
		domain.SideBuy, "1", "0.5",
	)
	retiredOrder.Status = domain.OrderStatusUnknown
	retiredOrder.FailureCode = "CLOB_ORDER_NOT_FOUND"
	retiredOrder.CreatedAt = old
	retiredOrder.UpdatedAt = old
	insertReconciliationSelectionOrder(t, db, retiredOrder)

	pending, err := repository.ListPending(context.Background(), now.Add(-2*time.Second), 100)
	if err != nil {
		t.Fatal(err)
	}
	scopedPending, err := repository.ListPendingForAccounts(
		context.Background(), []string{accountID}, now.Add(-2*time.Second), 100,
	)
	if err != nil {
		t.Fatal(err)
	}
	reconciliationOrders, err := repository.ListForReconciliation(
		context.Background(), accountID, now.Add(-48*time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	pendingIDs := selectedFixtureOrderIDs(pending, fixtureIDs)
	scopedPendingIDs := selectedFixtureOrderIDs(scopedPending, fixtureIDs)
	if !containsOrderID(pending, retiredOrder.ID) || containsOrderID(scopedPending, retiredOrder.ID) {
		t.Fatalf("retired order global/scoped selection = %t/%t", containsOrderID(pending, retiredOrder.ID), containsOrderID(scopedPending, retiredOrder.ID))
	}
	reconciliationIDs := selectedFixtureOrderIDs(reconciliationOrders, fixtureIDs)
	for _, fixture := range fixtures {
		orderID := "order-reconcile-" + fixture.name
		if _, selected := pendingIDs[orderID]; selected != fixture.wantPending {
			t.Errorf("pending selection for %s = %t, want %t", fixture.name, selected, fixture.wantPending)
		}
		if _, selected := scopedPendingIDs[orderID]; selected != fixture.wantPending {
			t.Errorf("scoped pending selection for %s = %t, want %t", fixture.name, selected, fixture.wantPending)
		}
		if _, selected := reconciliationIDs[orderID]; selected != fixture.wantReconcile {
			t.Errorf("reconciliation selection for %s = %t, want %t", fixture.name, selected, fixture.wantReconcile)
		}
	}
}

func containsOrderID(orders []domain.Order, orderID string) bool {
	for _, order := range orders {
		if order.ID == orderID {
			return true
		}
	}
	return false
}

func insertReconciliationSelectionOrder(t *testing.T, db *sql.DB, order domain.Order) {
	t.Helper()
	payload, err := json.Marshal(order.Intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_orders (
			order_id, client_order_id, execution_account_id, venue, market_id, token_id,
			intent, venue_order_id, status, failure_code, revision, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,1,$11,$12)`,
		order.ID, order.Intent.ClientOrderID, order.Intent.ExecutionAccountID, order.Intent.Venue,
		order.Intent.MarketID, order.Intent.TokenID, payload, order.VenueOrderID,
		string(order.Status), order.FailureCode, order.CreatedAt, order.UpdatedAt); err != nil {
		t.Fatal(err)
	}
}

func insertReconciliationSelectionReservation(
	t *testing.T,
	db *sql.DB,
	order domain.Order,
	status domain.ReservationStatus,
	empty bool,
) {
	t.Helper()
	reserveUnitPrice := "0"
	initialReservedBalance := "0"
	remainingReservedBalance := "0"
	initialReservedShares := "1"
	remainingReservedShares := "1"
	if order.Intent.Side == domain.SideBuy {
		reserveUnitPrice = "0.5"
		initialReservedBalance = "0.5"
		remainingReservedBalance = "0.5"
		initialReservedShares = "0"
		remainingReservedShares = "0"
	}
	if empty {
		remainingReservedBalance = "0"
		remainingReservedShares = "0"
	}
	if _, err := db.Exec(`
		INSERT INTO asset_reservations (
			order_id, client_order_id, intent_fingerprint, execution_account_id,
			strategy_id, market_id, token_id, target_lot_id, side,
			requested_shares, reserve_unit_price,
			initial_reserved_balance, remaining_reserved_balance,
			initial_reserved_shares, remaining_reserved_shares,
			settled_shares, settled_notional, status, uncertain_reason
		) VALUES ($1,$2,$3,$4,$5,$6,$7,NULLIF($8,''),$9,1,$10,$11,$12,$13,$14,0,0,$15,$16)`,
		order.ID, order.Intent.ClientOrderID, "fixture-"+order.ID,
		order.Intent.ExecutionAccountID, order.Intent.StrategyID, order.Intent.MarketID,
		order.Intent.TokenID, order.Intent.TargetLotID, string(order.Intent.Side), reserveUnitPrice,
		initialReservedBalance, remainingReservedBalance, initialReservedShares, remainingReservedShares,
		string(status), "integration fixture"); err != nil {
		t.Fatal(err)
	}
}

func selectedFixtureOrderIDs(orders []domain.Order, fixtures map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{})
	for _, order := range orders {
		if _, fixture := fixtures[order.ID]; fixture {
			result[order.ID] = struct{}{}
		}
	}
	return result
}
