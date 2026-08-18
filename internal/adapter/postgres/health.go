package postgres

import (
	"context"
	"database/sql"
	"fmt"
)

// HealthChecker verifies both database connectivity and the schema required by
// the HTTP execution path. A successful Ping alone is not enough: serving with
// a partially migrated database would fail after accepting traffic.
type HealthChecker struct {
	db *sql.DB
}

// NewHealthChecker creates a read-only PostgreSQL readiness checker.
func NewHealthChecker(db *sql.DB) (*HealthChecker, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	return &HealthChecker{db: db}, nil
}

// Check returns nil only when PostgreSQL is reachable and every table used by
// the persistent execution, reservation, fill, outbox, reconciliation, exit,
// and trade-history paths exists.
func (checker *HealthChecker) Check(ctx context.Context) error {
	if err := checker.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	var missing int
	err := checker.db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM (VALUES
			('execution_accounts'),
			('execution_positions'),
			('asset_reservations'),
			('asset_reservation_events'),
			('execution_orders'),
			('execution_order_events'),
			('execution_order_attempts'),
			('execution_fills'),
			('execution_fill_events'),
			('position_lots'),
			('position_lot_closures'),
			('position_events'),
			('execution_account_events'),
			('execution_outbox'),
			('position_exit_runs'),
			('reconciliation_runs'),
			('reconciliation_issues')
		) AS required(name)
		WHERE to_regclass('public.' || required.name) IS NULL`).Scan(&missing)
	if err != nil {
		return fmt.Errorf("inspect postgres schema: %w", err)
	}
	if missing != 0 {
		return fmt.Errorf("postgres schema is incomplete: %d required relations are missing", missing)
	}
	return nil
}
