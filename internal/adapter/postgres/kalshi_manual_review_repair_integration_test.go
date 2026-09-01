package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/kalshirepair"
)

func TestKalshiManualReviewRepairPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	now := time.Now().UTC().Truncate(time.Microsecond)
	order := insertKalshiManualReviewFixture(t, db, "kalshi:wallet-kalshi-repair", "order-kalshi-repair", "", now)
	store, err := NewKalshiManualReviewRepairStore(db, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	evidence := kalshiRepairIntegrationEvidence(order, now)
	fingerprint, err := evidence.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(context.Background(), order.Intent.ExecutionAccountID, order.ID)
	if err != nil || loaded.Order.Status != domain.OrderStatusManualReview ||
		loaded.ReservationStatus != domain.ReservationStatusReconciliationRequired || loaded.RemainingReservedBalance != "10.09" {
		t.Fatalf("pre-repair state=%#v error=%v", loaded, err)
	}
	applied, err := store.ApplyCancelled(context.Background(), kalshirepair.ApplyParams{
		ExecutionAccountID: order.Intent.ExecutionAccountID, OrderID: order.ID,
		Evidence: evidence, EvidenceFingerprint: fingerprint,
	})
	if err != nil || !applied {
		t.Fatalf("ApplyCancelled() applied=%t error=%v", applied, err)
	}
	assertAccount(t, db, order.Intent.ExecutionAccountID, "100", "100", "0")
	var (
		venueOrderID, orderStatus, failureCode, failureReason string
		orderRevision                                         int64
	)
	if err := db.QueryRow(`
		SELECT venue_order_id,status,failure_code,failure_reason,revision
		FROM execution_orders WHERE order_id=$1`, order.ID).Scan(
		&venueOrderID, &orderStatus, &failureCode, &failureReason, &orderRevision,
	); err != nil {
		t.Fatal(err)
	}
	if venueOrderID != evidence.OrderID || orderStatus != "CANCELLED" || failureCode != kalshiLegacyCancelReasonCode ||
		!strings.Contains(failureReason, fingerprint) || orderRevision != 2 {
		t.Fatalf("order=%q/%q/%q/%q/%d", venueOrderID, orderStatus, failureCode, failureReason, orderRevision)
	}
	var reservationStatus, remainingBalance, remainingShares string
	if err := db.QueryRow(`
		SELECT status,remaining_reserved_balance::text,remaining_reserved_shares::text
		FROM asset_reservations WHERE order_id=$1`, order.ID).Scan(
		&reservationStatus, &remainingBalance, &remainingShares,
	); err != nil {
		t.Fatal(err)
	}
	if reservationStatus != "RELEASED" || !domain.Decimal(remainingBalance).Equal("0") || !domain.Decimal(remainingShares).Equal("0") {
		t.Fatalf("reservation=%q/%q/%q", reservationStatus, remainingBalance, remainingShares)
	}
	var eventTrigger, eventReason, attemptOutcome, attemptFingerprint, reservationEventType, issueStatus string
	if err := db.QueryRow(`SELECT trigger,reason FROM execution_order_events WHERE order_id=$1 AND revision=2`, order.ID).Scan(&eventTrigger, &eventReason); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT outcome,request_fingerprint FROM execution_order_attempts WHERE order_id=$1 AND sequence=1`, order.ID).Scan(&attemptOutcome, &attemptFingerprint); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT event_type FROM asset_reservation_events WHERE order_id=$1 AND event_key=$2`, order.ID, "kalshi-legacy-cancel:"+fingerprint).Scan(&reservationEventType); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM reconciliation_issues WHERE execution_account_id=$1 AND order_id=$2`, order.Intent.ExecutionAccountID, order.ID).Scan(&issueStatus); err != nil {
		t.Fatal(err)
	}
	if eventTrigger != "OPERATOR" || !strings.Contains(eventReason, fingerprint) || attemptOutcome != "SUCCEEDED" ||
		attemptFingerprint != fingerprint || reservationEventType != "RELEASED" || issueStatus != "RESOLVED" {
		t.Fatalf("audit=%q/%q/%q/%q/%q/%q", eventTrigger, eventReason, attemptOutcome, attemptFingerprint, reservationEventType, issueStatus)
	}

	loaded, err = store.Load(context.Background(), order.Intent.ExecutionAccountID, order.ID)
	if err != nil || loaded.RepairFingerprint != fingerprint || loaded.Order.Status != domain.OrderStatusCancelled {
		t.Fatalf("post-repair state=%#v error=%v", loaded, err)
	}
	applied, err = store.ApplyCancelled(context.Background(), kalshirepair.ApplyParams{
		ExecutionAccountID: order.Intent.ExecutionAccountID, OrderID: order.ID,
		Evidence: evidence, EvidenceFingerprint: fingerprint,
	})
	if err != nil || applied {
		t.Fatalf("idempotent ApplyCancelled() applied=%t error=%v", applied, err)
	}
	assertAccount(t, db, order.Intent.ExecutionAccountID, "100", "100", "0")
	assertRepairAuditCounts(t, db, order.ID, 1, 1, 1)
}

func TestKalshiManualReviewRepairRollsBackCollateralOnOrderIdentityConflict(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	now := time.Now().UTC().Truncate(time.Microsecond)
	order := insertKalshiManualReviewFixture(t, db, "kalshi:wallet-kalshi-repair-conflict", "order-kalshi-repair-conflict", "", now)
	evidence := kalshiRepairIntegrationEvidence(order, now)
	insertKalshiOrderIdentityOwner(t, db, order.Intent.ExecutionAccountID, evidence.OrderID, now)
	fingerprint, err := evidence.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewKalshiManualReviewRepairStore(db, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := store.ApplyCancelled(context.Background(), kalshirepair.ApplyParams{
		ExecutionAccountID: order.Intent.ExecutionAccountID, OrderID: order.ID,
		Evidence: evidence, EvidenceFingerprint: fingerprint,
	}); err == nil || applied {
		t.Fatalf("identity-conflict repair applied=%t error=%v", applied, err)
	}
	assertAccount(t, db, order.Intent.ExecutionAccountID, "100", "89.91", "10.09")
	var orderStatus, reservationStatus string
	if err := db.QueryRow(`SELECT status FROM execution_orders WHERE order_id=$1`, order.ID).Scan(&orderStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM asset_reservations WHERE order_id=$1`, order.ID).Scan(&reservationStatus); err != nil {
		t.Fatal(err)
	}
	if orderStatus != "MANUAL_REVIEW" || reservationStatus != "RECONCILIATION_REQUIRED" {
		t.Fatalf("rolled-back state=%q/%q", orderStatus, reservationStatus)
	}
	assertRepairAuditCounts(t, db, order.ID, 0, 0, 0)
}

func TestKalshiManualReviewRepairRejectsReservationIdentityDriftBeforeRelease(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	now := time.Now().UTC().Truncate(time.Microsecond)
	order := insertKalshiManualReviewFixture(t, db, "kalshi:wallet-kalshi-reservation-drift", "order-kalshi-reservation-drift", "", now)
	if _, err := db.Exec(`UPDATE asset_reservations SET strategy_id='wrong-strategy' WHERE order_id=$1`, order.ID); err != nil {
		t.Fatal(err)
	}
	evidence := kalshiRepairIntegrationEvidence(order, now)
	fingerprint, err := evidence.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewKalshiManualReviewRepairStore(db, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := store.ApplyCancelled(context.Background(), kalshirepair.ApplyParams{
		ExecutionAccountID: order.Intent.ExecutionAccountID, OrderID: order.ID,
		Evidence: evidence, EvidenceFingerprint: fingerprint,
	}); err == nil || applied {
		t.Fatalf("reservation-drift repair applied=%t error=%v", applied, err)
	}
	assertAccount(t, db, order.Intent.ExecutionAccountID, "100", "89.91", "10.09")
	var orderStatus, reservationStatus string
	if err := db.QueryRow(`SELECT status FROM execution_orders WHERE order_id=$1`, order.ID).Scan(&orderStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM asset_reservations WHERE order_id=$1`, order.ID).Scan(&reservationStatus); err != nil {
		t.Fatal(err)
	}
	if orderStatus != "MANUAL_REVIEW" || reservationStatus != "RECONCILIATION_REQUIRED" {
		t.Fatalf("identity-drift state=%q/%q", orderStatus, reservationStatus)
	}
	assertRepairAuditCounts(t, db, order.ID, 0, 0, 0)
}

func insertKalshiManualReviewFixture(t *testing.T, db *sql.DB, accountID, orderID, venueOrderID string, now time.Time) domain.Order {
	t.Helper()
	insertAccount(t, db, accountID, "kalshi-api-key:"+accountID, "100", "89.91", "10.09")
	order := domain.Order{
		ID: orderID, VenueOrderID: venueOrderID, Status: domain.OrderStatusManualReview,
		FilledSize: "0", FilledNotional: "0", TotalFees: "0", Revision: 1,
		CreatedAt: now.Add(-48 * time.Hour), UpdatedAt: now.Add(-24 * time.Hour),
		Intent: domain.OrderIntent{
			ModelID: "gemini_masked", StrategyID: "multfactor_v2", ExecutionAccountID: accountID,
			SignalID: "signal-" + orderID, ClientOrderID: "client-" + orderID, Venue: "kalshi",
			MarketSource: domain.MarketSourceKalshi, MarketID: "KXTEST-YES", ConditionID: "kalshi:KXTEST-YES",
			OutcomeID: "YES", TokenID: "kalshi:KXTEST-YES:YES", Side: domain.SideBuy,
			Type: domain.OrderTypeLimit, Price: "0.5", WorstPrice: "0.5", Size: "20", TimeInForce: domain.TimeInForceFOK,
		},
	}
	payload, err := json.Marshal(order.Intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_orders (
			order_id,client_order_id,execution_account_id,venue,market_id,token_id,intent,
			venue_order_id,status,filled_size,filled_notional,total_fees,failure_code,failure_reason,
			revision,created_at,updated_at
		) VALUES ($1,$2,$3,'kalshi',$4,$5,$6::jsonb,$7,'MANUAL_REVIEW',0,0,0,
			'RECONCILE_FAILED','legacy authoritative id mismatch',1,$8,$9)`,
		order.ID, order.Intent.ClientOrderID, accountID, order.Intent.MarketID, order.Intent.TokenID,
		payload, venueOrderID, order.CreatedAt, order.UpdatedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO asset_reservations (
			order_id,client_order_id,intent_fingerprint,execution_account_id,strategy_id,market_id,token_id,
			side,requested_shares,reserve_unit_price,initial_reserved_balance,remaining_reserved_balance,
			initial_reserved_shares,remaining_reserved_shares,settled_shares,settled_notional,status,uncertain_reason
		) VALUES ($1,$2,$3,$4,$5,$6,$7,'BUY',20,0.5045,10.09,10.09,0,0,0,0,
			'RECONCILIATION_REQUIRED','legacy authoritative id mismatch')`,
		order.ID, order.Intent.ClientOrderID, "fingerprint-"+order.ID, accountID,
		order.Intent.StrategyID, order.Intent.MarketID, order.Intent.TokenID); err != nil {
		t.Fatal(err)
	}
	runID := "run-" + order.ID
	if _, err := db.Exec(`
		INSERT INTO reconciliation_runs (
			run_id,execution_account_id,trigger,focus_order_id,status,started_at,completed_at
		) VALUES ($1,$2,'ORDER_UNKNOWN',$3,'ATTENTION_REQUIRED',$4,$4)`, runID, accountID, order.ID, order.UpdatedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO reconciliation_issues (
			issue_id,run_id,fingerprint,execution_account_id,issue_type,resolution,status,
			order_id,source,details,observed_at
		) VALUES ($1,$2,$3,$4,'SUBMIT_UNCONFIRMED','MANUAL_REVIEW','OPEN',$5,
			'LOCAL_ORDER_STATE','legacy authoritative id mismatch',$6)`,
		"issue-"+order.ID, runID, "issue-fingerprint-"+order.ID, accountID, order.ID, order.UpdatedAt); err != nil {
		t.Fatal(err)
	}
	return order
}

func kalshiRepairIntegrationEvidence(order domain.Order, now time.Time) kalshirepair.Evidence {
	cancelOnPause := true
	subaccount := 0
	return kalshirepair.Evidence{
		OrderID: "authoritative-" + order.ID, ClientOrderID: order.Intent.ClientOrderID,
		MarketID: order.Intent.MarketID, OutcomeSide: "YES", Action: "buy", BookSide: "bid",
		OrderType: "limit", OrderPrice: order.Intent.WorstPrice, SelfTradePolicy: "taker_at_cross",
		CancelOnPause: &cancelOnPause, SubaccountNumber: &subaccount,
		Status: "canceled", FillCount: "0", RemainingCount: "0",
		InitialCount: order.Intent.Size, FillIDs: []string{}, LastUpdatedAt: now.Add(-time.Hour), ObservedAt: now,
		OrderQuerySource: "KALSHI_ORDER_BY_CLIENT_THEN_ORDER_ID", FillQuerySource: "KALSHI_FILLS_BY_ORDER_ID",
	}
}

func insertKalshiOrderIdentityOwner(t *testing.T, db *sql.DB, accountID, venueOrderID string, now time.Time) {
	t.Helper()
	intent := domain.OrderIntent{
		ModelID: "model", StrategyID: "strategy", ExecutionAccountID: accountID,
		SignalID: "signal-owner", ClientOrderID: "client-owner", Venue: "kalshi",
		MarketSource: domain.MarketSourceKalshi, MarketID: "KXOTHER", ConditionID: "kalshi:KXOTHER",
		OutcomeID: "YES", TokenID: "kalshi:KXOTHER:YES", Side: domain.SideBuy,
		Type: domain.OrderTypeLimit, Price: "0.5", WorstPrice: "0.5", Size: "1", TimeInForce: domain.TimeInForceFOK,
	}
	payload, err := json.Marshal(intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_orders (
			order_id,client_order_id,execution_account_id,venue,market_id,token_id,intent,
			venue_order_id,status,revision,created_at,updated_at
		) VALUES ('identity-owner','client-owner',$1,'kalshi','KXOTHER','kalshi:KXOTHER:YES',
			$2::jsonb,$3,'CANCELLED',1,$4,$4)`, accountID, payload, venueOrderID, now); err != nil {
		t.Fatal(err)
	}
}

func assertRepairAuditCounts(t *testing.T, db *sql.DB, orderID string, events, attempts, reservationEvents int) {
	t.Helper()
	var actualEvents, actualAttempts, actualReservationEvents int
	if err := db.QueryRow(`SELECT count(*) FROM execution_order_events WHERE order_id=$1 AND reason_code=$2`, orderID, kalshiLegacyCancelReasonCode).Scan(&actualEvents); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM execution_order_attempts WHERE order_id=$1`, orderID).Scan(&actualAttempts); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM asset_reservation_events WHERE order_id=$1 AND event_key LIKE 'kalshi-legacy-cancel:%'`, orderID).Scan(&actualReservationEvents); err != nil {
		t.Fatal(err)
	}
	if actualEvents != events || actualAttempts != attempts || actualReservationEvents != reservationEvents {
		t.Fatalf("audit counts=%d/%d/%d, want %d/%d/%d", actualEvents, actualAttempts, actualReservationEvents, events, attempts, reservationEvents)
	}
}
