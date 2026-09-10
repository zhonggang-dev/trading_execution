package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// OrderRecoveryLeaseStore is the PostgreSQL implementation of the order-level
// recovery lease. The row is locked FOR UPDATE for the whole decision so two
// workers cannot both believe they acquired the same order.
type OrderRecoveryLeaseStore struct {
	db *sql.DB
}

var _ port.OrderRecoveryLeaseStore = (*OrderRecoveryLeaseStore)(nil)

const orderRecoveryLeaseColumns = `order_id, execution_account_id, holder, order_revision,
	acquired_at, expires_at, attempts, first_pending_at, last_attempt_at, next_retry_at,
	last_error, escalated_at, updated_at`

// NewOrderRecoveryLeaseStore 创建并初始化 Order Recovery Lease Store。
func NewOrderRecoveryLeaseStore(db *sql.DB) (*OrderRecoveryLeaseStore, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	return &OrderRecoveryLeaseStore{db: db}, nil
}

// AcquireOrderRecoveryLease 按订单粒度获取恢复租约并校验快照版本。
func (store *OrderRecoveryLeaseStore) AcquireOrderRecoveryLease(ctx context.Context, request domain.OrderRecoveryLeaseRequest) (domain.OrderRecoveryLease, error) {
	request.OrderID = strings.TrimSpace(request.OrderID)
	request.ExecutionAccountID = strings.TrimSpace(request.ExecutionAccountID)
	request.Holder = strings.TrimSpace(request.Holder)
	if request.OrderID == "" || request.ExecutionAccountID == "" || request.Holder == "" {
		return domain.OrderRecoveryLease{}, fmt.Errorf("order id, execution account id, and holder are required")
	}
	if request.TTL <= 0 {
		return domain.OrderRecoveryLease{}, fmt.Errorf("order recovery lease ttl must be positive")
	}
	now := request.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	expiresAt := now.Add(request.TTL)
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return domain.OrderRecoveryLease{}, fmt.Errorf("begin order recovery lease: %w", err)
	}
	defer tx.Rollback()
	current, err := scanOrderRecoveryLease(tx.QueryRowContext(ctx, `
		SELECT `+orderRecoveryLeaseColumns+` FROM order_recovery_leases WHERE order_id=$1 FOR UPDATE`, request.OrderID))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		lease, err := scanOrderRecoveryLease(tx.QueryRowContext(ctx, `
			INSERT INTO order_recovery_leases (
				order_id, execution_account_id, holder, order_revision, acquired_at, expires_at, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$5)
			RETURNING `+orderRecoveryLeaseColumns,
			request.OrderID, request.ExecutionAccountID, request.Holder, request.OrderRevision, now, expiresAt))
		if err != nil {
			return domain.OrderRecoveryLease{}, fmt.Errorf("insert order recovery lease: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return domain.OrderRecoveryLease{}, fmt.Errorf("commit order recovery lease: %w", err)
		}
		return lease, nil
	case err != nil:
		return domain.OrderRecoveryLease{}, fmt.Errorf("lock order recovery lease: %w", err)
	}
	if current.HeldAt(now, request.Holder) {
		return current, port.ErrOrderRecoveryLeaseHeld
	}
	if current.OrderRevision > request.OrderRevision {
		return current, port.ErrOrderRecoveryStaleView
	}
	if !request.BypassBackoff && !current.RetryDueAt(now) {
		return current, port.ErrOrderRecoveryBackoff
	}
	lease, err := scanOrderRecoveryLease(tx.QueryRowContext(ctx, `
		UPDATE order_recovery_leases
		SET holder=$2, order_revision=$3, acquired_at=$4, expires_at=$5, updated_at=$4
		WHERE order_id=$1
		RETURNING `+orderRecoveryLeaseColumns,
		request.OrderID, request.Holder, request.OrderRevision, now, expiresAt))
	if err != nil {
		return domain.OrderRecoveryLease{}, fmt.Errorf("acquire order recovery lease: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return domain.OrderRecoveryLease{}, fmt.Errorf("commit order recovery lease: %w", err)
	}
	return lease, nil
}

// ReleaseOrderRecoveryLease 记录一次恢复结果并释放或删除订单租约。
func (store *OrderRecoveryLeaseStore) ReleaseOrderRecoveryLease(ctx context.Context, release domain.OrderRecoveryRelease) (domain.OrderRecoveryLease, error) {
	release.OrderID = strings.TrimSpace(release.OrderID)
	release.Holder = strings.TrimSpace(release.Holder)
	if release.OrderID == "" || release.Holder == "" {
		return domain.OrderRecoveryLease{}, fmt.Errorf("order id and holder are required")
	}
	now := release.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return domain.OrderRecoveryLease{}, fmt.Errorf("begin order recovery release: %w", err)
	}
	defer tx.Rollback()
	current, err := scanOrderRecoveryLease(tx.QueryRowContext(ctx, `
		SELECT `+orderRecoveryLeaseColumns+` FROM order_recovery_leases WHERE order_id=$1 FOR UPDATE`, release.OrderID))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.OrderRecoveryLease{}, fmt.Errorf("order recovery lease %s: %w", release.OrderID, port.ErrOrderNotFound)
	}
	if err != nil {
		return domain.OrderRecoveryLease{}, fmt.Errorf("lock order recovery lease: %w", err)
	}
	if current.Holder != release.Holder {
		return current, port.ErrOrderRecoveryLeaseHeld
	}
	var lease domain.OrderRecoveryLease
	switch release.Kind {
	case domain.OrderRecoveryOutcomeResolved:
		if _, err := tx.ExecContext(ctx, `DELETE FROM order_recovery_leases WHERE order_id=$1`, release.OrderID); err != nil {
			return domain.OrderRecoveryLease{}, fmt.Errorf("delete resolved order recovery lease: %w", err)
		}
		lease = domain.OrderRecoveryLease{OrderID: current.OrderID, ExecutionAccountID: current.ExecutionAccountID, OrderRevision: current.OrderRevision, UpdatedAt: now}
	case domain.OrderRecoveryOutcomeWaiting, domain.OrderRecoveryOutcomeFailed:
		var nextRetry sql.NullTime
		if !release.NextRetryAt.IsZero() {
			nextRetry = sql.NullTime{Time: release.NextRetryAt.UTC(), Valid: true}
		}
		failed := 0
		if release.Kind == domain.OrderRecoveryOutcomeFailed {
			failed = 1
		}
		lease, err = scanOrderRecoveryLease(tx.QueryRowContext(ctx, `
			UPDATE order_recovery_leases
			SET holder='', acquired_at=NULL, expires_at=NULL,
			    attempts=attempts+$2,
			    first_pending_at=COALESCE(first_pending_at, $3),
			    last_attempt_at=$3, next_retry_at=$4, last_error=$5,
			    escalated_at=CASE WHEN $6 THEN COALESCE(escalated_at, $3) ELSE escalated_at END,
			    updated_at=$3
			WHERE order_id=$1
			RETURNING `+orderRecoveryLeaseColumns,
			release.OrderID, failed, now, nextRetry, strings.TrimSpace(release.Error), release.Escalate))
		if err != nil {
			return domain.OrderRecoveryLease{}, fmt.Errorf("release order recovery lease: %w", err)
		}
	default:
		return current, fmt.Errorf("unsupported order recovery outcome %q", release.Kind)
	}
	if err := tx.Commit(); err != nil {
		return domain.OrderRecoveryLease{}, fmt.Errorf("commit order recovery release: %w", err)
	}
	return lease, nil
}

func scanOrderRecoveryLease(row rowScanner) (domain.OrderRecoveryLease, error) {
	var lease domain.OrderRecoveryLease
	var acquiredAt, expiresAt, firstPendingAt, lastAttemptAt, nextRetryAt, escalatedAt sql.NullTime
	err := row.Scan(
		&lease.OrderID, &lease.ExecutionAccountID, &lease.Holder, &lease.OrderRevision,
		&acquiredAt, &expiresAt, &lease.Attempts, &firstPendingAt, &lastAttemptAt, &nextRetryAt,
		&lease.LastError, &escalatedAt, &lease.UpdatedAt,
	)
	if err != nil {
		return domain.OrderRecoveryLease{}, err
	}
	lease.AcquiredAt = nullTimePointer(acquiredAt)
	lease.ExpiresAt = nullTimePointer(expiresAt)
	lease.FirstPendingAt = nullTimePointer(firstPendingAt)
	lease.LastAttemptAt = nullTimePointer(lastAttemptAt)
	lease.NextRetryAt = nullTimePointer(nextRetryAt)
	lease.EscalatedAt = nullTimePointer(escalatedAt)
	lease.UpdatedAt = lease.UpdatedAt.UTC()
	return lease, nil
}

func nullTimePointer(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	utc := value.Time.UTC()
	return &utc
}

// GetOrderReservation 读取一张订单当前冻结的预占，只读不加锁。
func (manager *ReservationManager) GetOrderReservation(ctx context.Context, orderID string) (domain.AssetReservation, error) {
	orderID = strings.TrimSpace(orderID)
	if orderID == "" {
		return domain.AssetReservation{}, fmt.Errorf("order id is required")
	}
	stored, err := scanReservation(manager.db.QueryRowContext(ctx,
		`SELECT `+reservationColumns+` FROM asset_reservations WHERE order_id = $1`, orderID))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.AssetReservation{}, port.ErrReservationNotFound
	}
	if err != nil {
		return domain.AssetReservation{}, fmt.Errorf("read order reservation: %w", err)
	}
	return stored.AssetReservation, nil
}

var _ port.OrderReservationReader = (*ReservationManager)(nil)
