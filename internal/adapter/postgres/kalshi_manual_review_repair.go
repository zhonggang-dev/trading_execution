package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/kalshirepair"
)

const kalshiLegacyCancelReasonCode = "KALSHI_LEGACY_CANCEL_CONFIRMED"

var evidenceFingerprintPattern = regexp.MustCompile(`(?:^|[ ;])evidence_sha256=([0-9a-f]{64})(?:$|[ ;])`)

// KalshiManualReviewRepairStore is an operator-scoped adapter for legacy
// MANUAL_REVIEW orders. It is intentionally not wired into the server or any
// background scanner.
type KalshiManualReviewRepairStore struct {
	db  *sql.DB
	now func() time.Time
}

func NewKalshiManualReviewRepairStore(db *sql.DB, now func() time.Time) (*KalshiManualReviewRepairStore, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	if now == nil {
		now = time.Now
	}
	return &KalshiManualReviewRepairStore{db: db, now: now}, nil
}

func (store *KalshiManualReviewRepairStore) Load(ctx context.Context, accountID, orderID string) (kalshirepair.LocalState, error) {
	accountID, orderID = strings.TrimSpace(accountID), strings.TrimSpace(orderID)
	order, err := selectOrder(ctx, store.db, `WHERE order_id = $1`, orderID)
	if err != nil {
		return kalshirepair.LocalState{}, err
	}
	if order.Intent.ExecutionAccountID != accountID {
		return kalshirepair.LocalState{}, fmt.Errorf("order is not owned by the explicitly selected execution account")
	}
	var status string
	var balance, shares domain.Decimal
	err = store.db.QueryRowContext(ctx, `
		SELECT status, remaining_reserved_balance::text, remaining_reserved_shares::text
		FROM asset_reservations
		WHERE order_id=$1 AND execution_account_id=$2`, orderID, accountID).Scan(&status, &balance, &shares)
	if err == sql.ErrNoRows {
		return kalshirepair.LocalState{}, port.ErrReservationNotFound
	}
	if err != nil {
		return kalshirepair.LocalState{}, fmt.Errorf("load exact Kalshi repair reservation: %w", err)
	}
	return kalshirepair.LocalState{
		Order: order, ReservationStatus: domain.ReservationStatus(status),
		RemainingReservedBalance: balance, RemainingReservedShares: shares,
		RepairFingerprint: store.loadRepairFingerprint(ctx, store.db, orderID),
	}, nil
}

func (store *KalshiManualReviewRepairStore) ApplyCancelled(ctx context.Context, params kalshirepair.ApplyParams) (bool, error) {
	params.ExecutionAccountID = strings.TrimSpace(params.ExecutionAccountID)
	params.OrderID = strings.TrimSpace(params.OrderID)
	params.EvidenceFingerprint = strings.TrimSpace(params.EvidenceFingerprint)
	if params.ExecutionAccountID == "" || params.OrderID == "" || !evidenceFingerprintPattern.MatchString("evidence_sha256="+params.EvidenceFingerprint) {
		return false, fmt.Errorf("exact account, order, and evidence fingerprint are required")
	}
	recomputed, err := params.Evidence.Fingerprint()
	if err != nil || recomputed != params.EvidenceFingerprint {
		return false, fmt.Errorf("Kalshi repair evidence fingerprint mismatch")
	}
	if err := validateStoredRepairEvidence(params); err != nil {
		return false, err
	}
	now := store.now().UTC()
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, fmt.Errorf("begin Kalshi manual-review repair: %w", err)
	}
	defer tx.Rollback()
	var lockedAccount string
	if err := tx.QueryRowContext(ctx, `
		SELECT execution_account_id FROM execution_accounts
		WHERE execution_account_id=$1 FOR UPDATE`, params.ExecutionAccountID).Scan(&lockedAccount); err != nil {
		return false, fmt.Errorf("lock exact Kalshi repair account: %w", err)
	}
	order, err := selectOrder(ctx, tx, `WHERE order_id = $1 FOR UPDATE`, params.OrderID)
	if err != nil {
		return false, err
	}
	if order.Intent.ExecutionAccountID != params.ExecutionAccountID ||
		!strings.EqualFold(order.Intent.Venue, "kalshi") || order.Intent.MarketSource.Normalize() != domain.MarketSourceKalshi {
		return false, fmt.Errorf("locked order does not match the explicit Kalshi account/order scope")
	}
	storedReservation, err := selectReservationByOrderID(ctx, tx, params.OrderID, true)
	if err != nil {
		return false, err
	}
	reservation := storedReservation.AssetReservation
	if reservation.ExecutionAccountID != params.ExecutionAccountID {
		return false, fmt.Errorf("locked reservation does not match the explicit account")
	}
	if err := validateRepairReservationIdentity(reservation, order); err != nil {
		return false, err
	}
	if order.Status == domain.OrderStatusCancelled {
		fingerprint := store.loadRepairFingerprint(ctx, tx, params.OrderID)
		if reservation.Status == domain.ReservationStatusReleased && fingerprint == params.EvidenceFingerprint {
			if err := tx.Commit(); err != nil {
				return false, fmt.Errorf("commit idempotent Kalshi repair check: %w", err)
			}
			return false, nil
		}
		return false, fmt.Errorf("cancelled order is not the result of this exact repair evidence")
	}
	if order.Status != domain.OrderStatusManualReview || reservation.Status != domain.ReservationStatusReconciliationRequired {
		return false, fmt.Errorf("locked order/reservation is no longer MANUAL_REVIEW/RECONCILIATION_REQUIRED")
	}
	if order.Intent.ClientOrderID != params.Evidence.ClientOrderID || order.Intent.MarketID != params.Evidence.MarketID ||
		strings.TrimSpace(params.Evidence.OrderID) == "" || params.Evidence.OrderID == order.Intent.ClientOrderID ||
		!params.Evidence.InitialCount.Equal(order.Intent.Size) {
		return false, fmt.Errorf("locked local identity does not match Kalshi evidence")
	}
	if !repairEvidenceMatchesIntent(params.Evidence, order.Intent) {
		return false, fmt.Errorf("locked local order semantics do not match Kalshi evidence")
	}
	if sign, signErr := decimalSignForRepair(order.FilledSize); signErr != nil || sign != 0 {
		return false, fmt.Errorf("local order has fills and cannot use the no-fill repair")
	}
	var localFillCount int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM execution_fills WHERE order_id=$1`, order.ID).Scan(&localFillCount); err != nil {
		return false, fmt.Errorf("check local Kalshi fills: %w", err)
	}
	if localFillCount != 0 {
		return false, fmt.Errorf("local fill ledger is not empty; no-fill repair is forbidden")
	}
	if err := releaseLegacyReservation(ctx, tx, reservation, now); err != nil {
		return false, err
	}
	sequence, err := nextRepairAttemptSequence(ctx, tx, order.ID)
	if err != nil {
		return false, err
	}
	attemptID := fmt.Sprintf("attempt:%s:%d", order.ID, sequence)
	reason := fmt.Sprintf(
		"operator-scoped repair; source=KALSHI_API_ORDER_AND_FILLS; evidence_sha256=%s; client_order_id=%s; authoritative_order_id=%s; venue_status=%s; fills=0",
		params.EvidenceFingerprint, order.Intent.ClientOrderID, params.Evidence.OrderID, strings.ToLower(strings.TrimSpace(params.Evidence.Status)),
	)
	nextRevision := order.Revision + 1
	result, err := tx.ExecContext(ctx, `
		UPDATE execution_orders
		SET venue_order_id=$4, status='CANCELLED', failure_code=$5, failure_reason=$6,
		    venue_last_observed_at=$7, revision=$3, updated_at=$8
		WHERE order_id=$1 AND execution_account_id=$2 AND revision=$9 AND status='MANUAL_REVIEW'`,
		order.ID, params.ExecutionAccountID, nextRevision, params.Evidence.OrderID,
		kalshiLegacyCancelReasonCode, reason, params.Evidence.ObservedAt.UTC(), now, order.Revision)
	if err != nil {
		return false, fmt.Errorf("finalize legacy Kalshi order: %w", err)
	}
	if !oneRow(result) {
		return false, port.ErrOrderRevisionConflict
	}
	event := domain.OrderEvent{
		ID: fmt.Sprintf("event:%s:%d", order.ID, nextRevision), OrderID: order.ID, Revision: nextRevision,
		FromStatus: domain.OrderStatusManualReview, ToStatus: domain.OrderStatusCancelled,
		Trigger: domain.TransitionTriggerOperator, AttemptID: attemptID,
		ReasonCode: kalshiLegacyCancelReasonCode, Reason: reason,
		VenueStatus: strings.TrimSpace(params.Evidence.Status), VenueOrderID: params.Evidence.OrderID,
		FilledSize: "0", FilledNotional: order.FilledNotional, TotalFees: order.TotalFees,
		VenueObservedAt: utcTimePointer(params.Evidence.ObservedAt), OccurredAt: now,
	}
	if err := insertOrderEvent(ctx, tx, event); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO execution_order_attempts (
			attempt_id, order_id, sequence, kind, outcome, request_fingerprint,
			venue_order_id, venue_status, error_code, error_message, started_at, completed_at
		) VALUES ($1,$2,$3,'RECONCILE','SUCCEEDED',$4,$5,$6,'','',$7,$7)`,
		attemptID, order.ID, sequence, params.EvidenceFingerprint, params.Evidence.OrderID,
		strings.TrimSpace(params.Evidence.Status), now); err != nil {
		return false, fmt.Errorf("insert Kalshi repair attempt: %w", err)
	}
	reservation.RemainingReservedBalance = "0"
	reservation.RemainingReservedShares = "0"
	reservation.Status = domain.ReservationStatusReleased
	reservation.UncertainReason = ""
	reservation.UpdatedAt = now
	reservation.ReleasedAt = &now
	reservation.Revision++
	result, err = tx.ExecContext(ctx, `
		UPDATE asset_reservations
		SET remaining_reserved_balance=0, remaining_reserved_shares=0,
		    status='RELEASED', uncertain_reason='', last_venue_observed_at=$3,
		    revision=revision+1, updated_at=$4, released_at=$4
		WHERE order_id=$1 AND execution_account_id=$2 AND status='RECONCILIATION_REQUIRED'`,
		order.ID, params.ExecutionAccountID, params.Evidence.ObservedAt.UTC(), now)
	if err != nil {
		return false, fmt.Errorf("release legacy Kalshi reservation: %w", err)
	}
	if !oneRow(result) {
		return false, port.ErrReservationConflict
	}
	if err := insertEvent(ctx, tx, reservation, "kalshi-legacy-cancel:"+params.EvidenceFingerprint,
		"RELEASED", string(domain.OrderStatusCancelled), reason); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE reconciliation_issues
		SET status='RESOLVED', resolution='MANUAL_REVIEW',
		    source='KALSHI_API_ORDER_AND_FILLS',
		    details=details || CASE WHEN details='' THEN '' ELSE ' | ' END || $3,
		    observed_at=GREATEST(observed_at,$4), resolved_at=$4
		WHERE execution_account_id=$1 AND order_id=$2 AND status='OPEN'
		  AND issue_type IN ('SUBMIT_UNCONFIRMED','SOURCE_UNAVAILABLE','SOURCE_CONFLICT')`,
		params.ExecutionAccountID, order.ID, reason, now); err != nil {
		return false, fmt.Errorf("resolve exact Kalshi order reconciliation issues: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit Kalshi manual-review repair (outcome may be unknown; rerun idempotently): %w", err)
	}
	return true, nil
}

func validateStoredRepairEvidence(params kalshirepair.ApplyParams) error {
	evidence := params.Evidence
	status := strings.ToLower(strings.TrimSpace(evidence.Status))
	if status != "canceled" && status != "cancelled" {
		return fmt.Errorf("Kalshi evidence is not a confirmed cancellation")
	}
	if len(evidence.FillIDs) != 0 {
		return fmt.Errorf("Kalshi fills exist; no-fill repair is forbidden")
	}
	for _, value := range []domain.Decimal{evidence.FillCount, evidence.RemainingCount} {
		if value.IsEmpty() {
			return fmt.Errorf("Kalshi cancellation evidence omitted an explicit count")
		}
		if sign, err := value.Sign(); err != nil || sign != 0 {
			return fmt.Errorf("Kalshi cancellation evidence is not unfilled and terminal")
		}
	}
	if evidence.InitialCount.IsEmpty() || evidence.FillIDs == nil {
		return fmt.Errorf("Kalshi cancellation evidence is incomplete")
	}
	if evidence.ObservedAt.IsZero() || evidence.LastUpdatedAt.IsZero() || evidence.LastUpdatedAt.After(evidence.ObservedAt) ||
		evidence.OrderQuerySource != "KALSHI_ORDER_BY_CLIENT_THEN_ORDER_ID" || evidence.FillQuerySource != "KALSHI_FILLS_BY_ORDER_ID" {
		return fmt.Errorf("Kalshi evidence provenance is incomplete")
	}
	return nil
}

func validateRepairReservationIdentity(reservation domain.AssetReservation, order domain.Order) error {
	intent := order.Intent
	if reservation.OrderID != order.ID || reservation.ClientOrderID != intent.ClientOrderID ||
		reservation.ExecutionAccountID != intent.ExecutionAccountID || reservation.StrategyID != intent.StrategyID ||
		reservation.MarketID != intent.MarketID || reservation.TokenID != intent.TokenID ||
		reservation.TargetLotID != intent.TargetLotID || reservation.Side != intent.Side ||
		!reservation.RequestedShares.Equal(intent.Size) {
		return fmt.Errorf("locked reservation identity does not match the persisted order intent")
	}
	return nil
}

func repairEvidenceMatchesIntent(evidence kalshirepair.Evidence, intent domain.OrderIntent) bool {
	identity, err := domain.CanonicalKalshiOrderIdentity(intent.Side, intent.OutcomeID, intent.WorstPrice)
	if err != nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(evidence.OutcomeSide), identity.OutcomeSide) &&
		strings.EqualFold(strings.TrimSpace(evidence.BookSide), identity.BookSide) &&
		strings.EqualFold(strings.TrimSpace(evidence.OrderType), string(intent.Type)) &&
		intent.Type == domain.OrderTypeLimit && intent.TimeInForce == domain.TimeInForceFOK &&
		!evidence.OrderPrice.IsEmpty() && evidence.OrderPrice.Equal(identity.OrderPrice) &&
		evidence.SubaccountNumber != nil && *evidence.SubaccountNumber == 0
}

func releaseLegacyReservation(ctx context.Context, tx *sql.Tx, reservation domain.AssetReservation, now time.Time) error {
	switch reservation.Side {
	case domain.SideBuy:
		amount := reservation.RemainingReservedBalance
		if sign, err := decimalSignForRepair(amount); err != nil || sign <= 0 {
			return fmt.Errorf("legacy BUY reservation has no releasable balance")
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE execution_accounts
			SET available_balance=available_balance+$2::numeric,
			    reserved_balance=reserved_balance-$2::numeric,
			    version=version+1, updated_at=$3
			WHERE execution_account_id=$1 AND reserved_balance >= $2::numeric`,
			reservation.ExecutionAccountID, amount.String(), now)
		if err != nil {
			return fmt.Errorf("release legacy Kalshi BUY balance: %w", err)
		}
		if !oneRow(result) {
			return fmt.Errorf("legacy Kalshi BUY reservation exceeds account reserved balance")
		}
	case domain.SideSell:
		shares := reservation.RemainingReservedShares
		if sign, err := decimalSignForRepair(shares); err != nil || sign <= 0 {
			return fmt.Errorf("legacy SELL reservation has no releasable shares")
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE execution_positions
			SET available_shares=available_shares+$3::numeric,
			    reserved_shares=reserved_shares-$3::numeric,
			    version=version+1, updated_at=$4
			WHERE execution_account_id=$1 AND token_id=$2 AND market_id=$5
			  AND reserved_shares >= $3::numeric`,
			reservation.ExecutionAccountID, reservation.TokenID, shares.String(), now, reservation.MarketID)
		if err != nil {
			return fmt.Errorf("release legacy Kalshi SELL shares: %w", err)
		}
		if !oneRow(result) {
			return fmt.Errorf("legacy Kalshi SELL reservation exceeds position reserved shares")
		}
	default:
		return fmt.Errorf("unsupported legacy reservation side %q", reservation.Side)
	}
	return nil
}

func nextRepairAttemptSequence(ctx context.Context, tx *sql.Tx, orderID string) (int, error) {
	var sequence int
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(sequence),0)+1 FROM execution_order_attempts WHERE order_id=$1`, orderID).Scan(&sequence); err != nil {
		return 0, fmt.Errorf("select Kalshi repair attempt sequence: %w", err)
	}
	return sequence, nil
}

type repairFingerprintQuery interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (store *KalshiManualReviewRepairStore) loadRepairFingerprint(ctx context.Context, query repairFingerprintQuery, orderID string) string {
	var reason string
	err := query.QueryRowContext(ctx, `
		SELECT reason FROM execution_order_events
		WHERE order_id=$1 AND reason_code=$2
		ORDER BY revision DESC LIMIT 1`, orderID, kalshiLegacyCancelReasonCode).Scan(&reason)
	if err != nil {
		return ""
	}
	match := evidenceFingerprintPattern.FindStringSubmatch(reason)
	if len(match) != 2 {
		return ""
	}
	return match[1]
}

func utcTimePointer(value time.Time) *time.Time {
	result := value.UTC()
	return &result
}

func decimalSignForRepair(value domain.Decimal) (int, error) {
	if value.IsEmpty() {
		value = "0"
	}
	return value.Sign()
}

var _ kalshirepair.Store = (*KalshiManualReviewRepairStore)(nil)
