package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/domain/orderstate"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// No-fill release of unaccepted orders (migration 0046).
//
// Facts reads the local view of one order in a single read-only snapshot.
// Apply cancels an UNKNOWN order and releases its reservation in ONE
// transaction together with its events, the reconciliation attempt, the
// closure of the order's recovery issues and the audit row. Splitting it would
// let a failure leave a CANCELLED order whose reservation can never be
// released by the retired-cancellation path. RecordShadow only appends the
// audit row of a shadow evaluation.

// ErrNoFillReleaseStale reports that the order, its reservation or its issues
// changed after the evidence was collected. Nothing was written.
var ErrNoFillReleaseStale = errors.New("no-fill release evidence is stale")

// NoFillReleaseTablesPresent reports whether migration 0046 has been applied.
// A binary that finds it missing keeps running with automatic no-fill release
// and venue expiry recording turned off.
func NoFillReleaseTablesPresent(ctx context.Context, db *sql.DB) (bool, error) {
	if db == nil {
		return false, nil
	}
	column, err := OrderAttemptVenueExpiryPresent(ctx, db)
	if err != nil {
		return false, err
	}
	var table bool
	if err := db.QueryRowContext(ctx, `
		SELECT to_regclass('execution_order_no_fill_resolutions') IS NOT NULL`).Scan(&table); err != nil {
		return false, fmt.Errorf("inspect execution_order_no_fill_resolutions: %w", err)
	}
	return column && table, nil
}

// NoFillReleaseStore persists no-fill decisions.
type NoFillReleaseStore struct {
	db          *sql.DB
	now         func() time.Time
	maxAttempts int
}

// NewNoFillReleaseStore creates the store. Call NoFillReleaseTablesPresent first.
func NewNoFillReleaseStore(db *sql.DB, now func() time.Time) (*NoFillReleaseStore, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	if now == nil {
		now = time.Now
	}
	return &NoFillReleaseStore{db: db, now: now, maxAttempts: defaultTransactionAttempts}, nil
}

// Facts reads the order, attempts, event statuses, fill counts, reservation,
// OPEN order issues and recovery lease in one consistent read-only snapshot.
func (store *NoFillReleaseStore) Facts(ctx context.Context, orderID string) (domain.NoFillLocalFacts, error) {
	orderID = strings.TrimSpace(orderID)
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return domain.NoFillLocalFacts{}, fmt.Errorf("begin no-fill facts read: %w", err)
	}
	defer tx.Rollback()
	order, err := selectOrder(ctx, tx, `WHERE order_id = $1`, orderID)
	if err != nil {
		return domain.NoFillLocalFacts{}, err
	}
	facts := domain.NoFillLocalFacts{Order: order}
	if facts.Attempts, err = queryOrderAttempts(ctx, tx, orderID, true); err != nil {
		return domain.NoFillLocalFacts{}, err
	}
	if facts.EventStatuses, err = noFillEventStatuses(ctx, tx, orderID); err != nil {
		return domain.NoFillLocalFacts{}, err
	}
	if facts.FillCount, facts.FillEventCount, err = noFillFillCounts(ctx, tx, orderID); err != nil {
		return domain.NoFillLocalFacts{}, err
	}
	stored, err := scanReservation(tx.QueryRowContext(ctx,
		`SELECT `+reservationColumns+` FROM asset_reservations WHERE order_id = $1`, orderID))
	switch {
	case err == nil:
		reservation := stored.AssetReservation
		facts.Reservation = &reservation
	case !errors.Is(err, port.ErrReservationNotFound):
		return domain.NoFillLocalFacts{}, err
	}
	if facts.OpenIssues, err = noFillOpenIssues(ctx, tx, order); err != nil {
		return domain.NoFillLocalFacts{}, err
	}
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM order_recovery_leases
			WHERE order_id = $1 AND holder <> '' AND expires_at > clock_timestamp()
		)`, orderID).Scan(&facts.LeaseHeld); err != nil {
		return domain.NoFillLocalFacts{}, fmt.Errorf("read order recovery lease: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return domain.NoFillLocalFacts{}, fmt.Errorf("finish no-fill facts read: %w", err)
	}
	return facts, nil
}

// HasShadow reports whether a shadow evaluation of the order was recorded.
func (store *NoFillReleaseStore) HasShadow(ctx context.Context, orderID string) (bool, error) {
	var exists bool
	if err := store.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM execution_order_no_fill_resolutions
			WHERE order_id = $1 AND mode = 'SHADOW'
		)`, strings.TrimSpace(orderID)).Scan(&exists); err != nil {
		return false, fmt.Errorf("read no-fill shadow record: %w", err)
	}
	return exists, nil
}

// RecordShadow appends the audit row of a shadow evaluation. It changes
// nothing else. A second evaluation of the same order is not recorded.
func (store *NoFillReleaseStore) RecordShadow(ctx context.Context, resolution domain.NoFillResolution) (bool, error) {
	if resolution.Mode != domain.NoFillModeShadow {
		return false, fmt.Errorf("shadow record requires mode SHADOW")
	}
	if err := validateNoFillResolution(resolution); err != nil {
		return false, err
	}
	result, err := store.db.ExecContext(ctx, insertNoFillResolutionSQL+` ON CONFLICT (resolution_id) DO NOTHING`,
		noFillResolutionID(resolution), resolution.OrderID, resolution.ExecutionAccountID,
		string(resolution.Mode), resolution.Actor, resolution.Reason, resolution.ReasonCode,
		resolution.ExpectedOrderRevision, string(resolution.Evidence), resolution.EvidenceSHA256,
		"0", "0", resolution.ResolvedAt.UTC())
	if err != nil {
		return false, fmt.Errorf("insert no-fill shadow record: %w", err)
	}
	return oneRow(result), nil
}

// Apply cancels the UNKNOWN order, releases its reservation, closes its
// recovery issues and writes the audit row in one transaction. Any changed
// precondition rolls everything back.
func (store *NoFillReleaseStore) Apply(ctx context.Context, resolution domain.NoFillResolution) (domain.Order, error) {
	if resolution.Mode != domain.NoFillModeAuto && resolution.Mode != domain.NoFillModeOperator {
		return domain.Order{}, fmt.Errorf("no-fill release requires mode AUTO or OPERATOR")
	}
	if err := validateNoFillResolution(resolution); err != nil {
		return domain.Order{}, err
	}
	var lastErr error
	for attempt := 1; attempt <= store.maxAttempts; attempt++ {
		order, err := store.applyOnce(ctx, resolution)
		if err == nil {
			return order, nil
		}
		if !retryablePostgresError(err) || attempt == store.maxAttempts {
			return domain.Order{}, err
		}
		lastErr = err
	}
	return domain.Order{}, lastErr
}

func (store *NoFillReleaseStore) applyOnce(ctx context.Context, resolution domain.NoFillResolution) (domain.Order, error) {
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return domain.Order{}, fmt.Errorf("begin no-fill release: %w", err)
	}
	defer tx.Rollback()
	// Lock order: account, order, reservation, position (as Reconcile).
	if err := lockExecutionAccount(ctx, tx, resolution.ExecutionAccountID); err != nil {
		return domain.Order{}, err
	}
	order, err := selectOrder(ctx, tx, `WHERE order_id = $1 FOR UPDATE`, resolution.OrderID)
	if err != nil {
		return domain.Order{}, err
	}
	if order.Intent.ExecutionAccountID != resolution.ExecutionAccountID {
		return domain.Order{}, fmt.Errorf("%w: order belongs to another execution account", ErrNoFillReleaseStale)
	}
	revisionMoved := order.Revision != resolution.ExpectedOrderRevision
	if order.Status != domain.OrderStatusUnknown || order.Revision < resolution.ExpectedOrderRevision ||
		(revisionMoved && !resolution.AllowReadOnlyProgress) {
		return domain.Order{}, fmt.Errorf("%w: order is %s at revision %d, evidence was collected at revision %d",
			ErrNoFillReleaseStale, order.Status, order.Revision, resolution.ExpectedOrderRevision)
	}
	// Re-check the whole history under the order lock: whatever happened since
	// the evidence was collected must still be reads that never saw the order.
	attempts, err := queryOrderAttempts(ctx, tx, order.ID, true)
	if err != nil {
		return domain.Order{}, err
	}
	statuses, err := noFillEventStatuses(ctx, tx, order.ID)
	if err != nil {
		return domain.Order{}, err
	}
	if err := domain.NoFillHistoryError(attempts, statuses); err != nil {
		return domain.Order{}, fmt.Errorf("%w: %v", ErrNoFillReleaseStale, err)
	}
	if !decimalIsZero(order.FilledSize) || !decimalIsZero(order.FilledNotional) || !decimalIsZero(order.TotalFees) {
		return domain.Order{}, fmt.Errorf("%w: order has a recorded fill", ErrNoFillReleaseStale)
	}
	fills, fillEvents, err := noFillFillCounts(ctx, tx, order.ID)
	if err != nil {
		return domain.Order{}, err
	}
	if fills != 0 || fillEvents != 0 {
		return domain.Order{}, fmt.Errorf("%w: order has %d fill(s) and %d fill event(s)", ErrNoFillReleaseStale, fills, fillEvents)
	}
	stored, err := selectReservationByOrderID(ctx, tx, order.ID, true)
	if err != nil {
		return domain.Order{}, fmt.Errorf("lock order reservation: %w", err)
	}
	reservation := stored.AssetReservation
	remainderMatches := reservation.RemainingReservedBalance.Equal(decimalOrZeroValue(resolution.ExpectedReleaseBalance))
	if reservation.Side == domain.SideSell {
		remainderMatches = reservation.RemainingReservedShares.Equal(decimalOrZeroValue(resolution.ExpectedReleaseShares))
	}
	if (reservation.Revision != resolution.ExpectedReservationRevision && !resolution.AllowReadOnlyProgress) ||
		reservation.Revision < resolution.ExpectedReservationRevision || !remainderMatches ||
		(reservation.Status != domain.ReservationStatusActive && reservation.Status != domain.ReservationStatusReconciliationRequired) ||
		!decimalIsZero(reservation.SettledShares) || !decimalIsZero(reservation.SettledNotional) || !decimalIsZero(reservation.SettledFees) {
		return domain.Order{}, fmt.Errorf("%w: reservation is %s at revision %d", ErrNoFillReleaseStale, reservation.Status, reservation.Revision)
	}
	issues, err := noFillOpenIssues(ctx, tx, order)
	if err != nil {
		return domain.Order{}, err
	}
	for _, issue := range issues {
		if !domain.NoFillReleasableIssue(issue) {
			return domain.Order{}, fmt.Errorf("%w: order has an OPEN %s issue from %s", ErrNoFillReleaseStale, issue.Type, issue.Source)
		}
	}
	if resolution.RequireLeaseFree {
		var held bool
		if err := tx.QueryRowContext(ctx, `
			SELECT COALESCE(bool_or(holder <> '' AND expires_at > clock_timestamp()), false)
			FROM (SELECT holder, expires_at FROM order_recovery_leases WHERE order_id = $1 FOR UPDATE) lease`,
			order.ID).Scan(&held); err != nil {
			return domain.Order{}, fmt.Errorf("lock order recovery lease: %w", err)
		}
		if held {
			return domain.Order{}, fmt.Errorf("%w: another worker holds the order recovery lease; retry later", ErrNoFillReleaseStale)
		}
	}

	var sequence int
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(sequence), 0) + 1 FROM execution_order_attempts WHERE order_id = $1`, order.ID).Scan(&sequence); err != nil {
		return domain.Order{}, fmt.Errorf("allocate no-fill attempt sequence: %w", err)
	}
	at := store.now().UTC()
	if at.Before(order.UpdatedAt.UTC()) {
		at = order.UpdatedAt.UTC()
	}
	trigger := domain.TransitionTriggerReconciliation
	if resolution.Mode == domain.NoFillModeOperator {
		trigger = domain.TransitionTriggerOperator
	}
	attemptID := fmt.Sprintf("attempt:%s:%d", order.ID, sequence)
	reconciling, firstEvent, err := orderstate.Apply(order, orderstate.Transition{
		EventID: fmt.Sprintf("event:%s:%d", order.ID, order.Revision+1), To: domain.OrderStatusReconciling,
		Trigger: trigger, AttemptID: attemptID, ReasonCode: resolution.ReasonCode,
		Reason: "no-fill evidence complete; releasing the reservation", At: at,
	})
	if err != nil {
		return domain.Order{}, err
	}
	cancelled, secondEvent, err := orderstate.Apply(reconciling, orderstate.Transition{
		EventID: fmt.Sprintf("event:%s:%d", order.ID, reconciling.Revision+1), To: domain.OrderStatusCancelled,
		Trigger: trigger, AttemptID: attemptID, ReasonCode: resolution.ReasonCode, Reason: resolution.Summary,
		VenueStatus: domain.NoFillReleaseVenueStatus, VenueOrderID: order.VenueOrderID, At: at,
	})
	if err != nil {
		return domain.Order{}, err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE execution_orders
		SET status = $3, failure_code = $4, failure_reason = $5, revision = $6, updated_at = $7
		WHERE order_id = $1 AND revision = $2 AND status = 'UNKNOWN'`,
		order.ID, order.Revision, string(cancelled.Status), cancelled.FailureCode, cancelled.FailureReason,
		cancelled.Revision, cancelled.UpdatedAt)
	if err != nil {
		return domain.Order{}, fmt.Errorf("update released order: %w", err)
	}
	if !oneRow(result) {
		return domain.Order{}, fmt.Errorf("%w: order revision changed", ErrNoFillReleaseStale)
	}
	for _, event := range []domain.OrderEvent{firstEvent, secondEvent} {
		if err := insertOrderEvent(ctx, tx, event); err != nil {
			return domain.Order{}, err
		}
	}
	attempt := domain.OrderAttempt{
		ID: attemptID, OrderID: order.ID, Sequence: sequence, Kind: domain.OrderAttemptReconcile,
		Outcome: domain.AttemptOutcomeStarted, RequestFingerprint: resolution.EvidenceSHA256,
		VenueOrderID: order.VenueOrderID, StartedAt: at,
	}
	if err := insertOrderAttempt(ctx, tx, attempt); err != nil {
		return domain.Order{}, err
	}
	completedAt := at
	attempt.Outcome = domain.AttemptOutcomeSucceeded
	attempt.CompletedAt = &completedAt
	attempt.VenueStatus = domain.NoFillReleaseVenueStatus
	attempt.ErrorCode = resolution.ReasonCode
	if err := finishOrderAttempt(ctx, tx, attempt); err != nil {
		return domain.Order{}, err
	}

	fingerprint, _, err := validateSettlementOrder(cancelled)
	if err != nil {
		return domain.Order{}, err
	}
	releasedBalance, releasedShares := domain.Decimal("0"), domain.Decimal("0")
	if reservation.Side == domain.SideBuy {
		releasedBalance = reservation.RemainingReservedBalance
	} else {
		releasedShares = reservation.RemainingReservedShares
	}
	released, err := reconcileReservationTx(ctx, tx, cancelled, fingerprint, at,
		resolution.ReasonCode+": "+resolution.Summary)
	if err != nil {
		return domain.Order{}, fmt.Errorf("release order reservation: %w", err)
	}
	if released.Status != domain.ReservationStatusReleased {
		return domain.Order{}, fmt.Errorf("release order reservation ended in %s", released.Status)
	}

	issueResolution := domain.ReconciliationResolutionAutomatic
	if resolution.Mode == domain.NoFillModeOperator {
		issueResolution = domain.ReconciliationResolutionManual
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE reconciliation_issues
		SET status = 'RESOLVED', resolution = $3, resolved_at = $4,
		    details = details || '; ' || $5
		WHERE execution_account_id = $1 AND order_id = $2 AND status = 'OPEN'
		  AND (issue_type IN ('ORDER_RECOVERY_PENDING', 'ORDER_RECOVERY_STALLED')
		       OR (issue_type = 'SOURCE_UNAVAILABLE' AND source = $6))`,
		order.Intent.ExecutionAccountID, order.ID, string(issueResolution), at,
		resolution.ReasonCode+": "+resolution.Summary, domain.NoFillReleaseSourceOrder); err != nil {
		return domain.Order{}, fmt.Errorf("resolve released order issues: %w", err)
	}
	if _, err := tx.ExecContext(ctx, insertNoFillResolutionSQL,
		noFillResolutionID(resolution), order.ID, order.Intent.ExecutionAccountID,
		string(resolution.Mode), resolution.Actor, resolution.Reason, resolution.ReasonCode,
		order.Revision, string(resolution.Evidence), resolution.EvidenceSHA256,
		decimalOrZero(releasedBalance), decimalOrZero(releasedShares), at); err != nil {
		return domain.Order{}, fmt.Errorf("insert no-fill release audit: %w", err)
	}
	if err := tx.Commit(); err != nil {
		// A failed COMMIT may still have committed; the caller re-reads the order.
		return domain.Order{}, fmt.Errorf("commit no-fill release (outcome may be unknown): %w", err)
	}
	return cancelled, nil
}

const insertNoFillResolutionSQL = `
	INSERT INTO execution_order_no_fill_resolutions (
		resolution_id, order_id, execution_account_id, mode, actor, reason, reason_code,
		order_revision, evidence, evidence_sha256, released_balance, released_shares, created_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11::numeric, $12::numeric, $13)`

func noFillResolutionID(resolution domain.NoFillResolution) string {
	return "nofill:" + strings.ToLower(string(resolution.Mode)) + ":" + resolution.OrderID
}

func validateNoFillResolution(resolution domain.NoFillResolution) error {
	if strings.TrimSpace(resolution.OrderID) == "" || strings.TrimSpace(resolution.ExecutionAccountID) == "" ||
		strings.TrimSpace(resolution.Actor) == "" || strings.TrimSpace(resolution.ReasonCode) == "" {
		return fmt.Errorf("no-fill resolution requires order, account, actor and reason code")
	}
	if resolution.Mode == domain.NoFillModeOperator && strings.TrimSpace(resolution.Reason) == "" {
		return fmt.Errorf("operator no-fill release requires a reason")
	}
	if len(resolution.Evidence) == 0 || len(resolution.EvidenceSHA256) != 64 || resolution.ResolvedAt.IsZero() {
		return fmt.Errorf("no-fill resolution requires evidence, its sha256 and a time")
	}
	return nil
}

type noFillQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func noFillEventStatuses(ctx context.Context, query noFillQuerier, orderID string) ([]domain.OrderStatus, error) {
	rows, err := query.QueryContext(ctx, `
		SELECT to_status FROM execution_order_events WHERE order_id = $1 ORDER BY revision`, orderID)
	if err != nil {
		return nil, fmt.Errorf("query order event statuses: %w", err)
	}
	defer rows.Close()
	var statuses []domain.OrderStatus
	for rows.Next() {
		var status string
		if err := rows.Scan(&status); err != nil {
			return nil, fmt.Errorf("scan order event status: %w", err)
		}
		statuses = append(statuses, domain.OrderStatus(status))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate order event statuses: %w", err)
	}
	return statuses, nil
}

func noFillFillCounts(ctx context.Context, query noFillQuerier, orderID string) (int, int, error) {
	var fills, events int
	if err := query.QueryRowContext(ctx, `
		SELECT (SELECT count(*) FROM execution_fills WHERE order_id = $1),
		       (SELECT count(*) FROM execution_fill_events WHERE order_id = $1)`, orderID).Scan(&fills, &events); err != nil {
		return 0, 0, fmt.Errorf("count order fills: %w", err)
	}
	return fills, events, nil
}

func noFillOpenIssues(ctx context.Context, query noFillQuerier, order domain.Order) ([]domain.NoFillIssueRef, error) {
	rows, err := query.QueryContext(ctx, `
		SELECT issue_id, issue_type, source, resolution
		FROM reconciliation_issues
		WHERE execution_account_id = $1 AND order_id = $2 AND status = 'OPEN'
		ORDER BY observed_at, issue_id`, order.Intent.ExecutionAccountID, order.ID)
	if err != nil {
		return nil, fmt.Errorf("query open order issues: %w", err)
	}
	defer rows.Close()
	var issues []domain.NoFillIssueRef
	for rows.Next() {
		var issue domain.NoFillIssueRef
		var issueType, resolution string
		if err := rows.Scan(&issue.IssueID, &issueType, &issue.Source, &resolution); err != nil {
			return nil, fmt.Errorf("scan open order issue: %w", err)
		}
		issue.Type = domain.ReconciliationIssueType(issueType)
		issue.Resolution = domain.ReconciliationResolution(resolution)
		issues = append(issues, issue)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate open order issues: %w", err)
	}
	return issues, nil
}

func decimalOrZeroValue(value domain.Decimal) domain.Decimal {
	if value.IsEmpty() {
		return "0"
	}
	return value
}

func decimalIsZero(value domain.Decimal) bool {
	if value.IsEmpty() {
		return true
	}
	sign, err := value.Sign()
	return err == nil && sign == 0
}
