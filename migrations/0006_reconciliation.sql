BEGIN;

ALTER TABLE execution_positions
    ADD COLUMN lifecycle_status TEXT NOT NULL DEFAULT 'OPEN'
        CHECK (lifecycle_status IN ('OPEN', 'SETTLED_PENDING_REDEEM', 'CLOSED', 'MANUAL_REVIEW'));

UPDATE execution_positions
SET lifecycle_status = 'CLOSED'
WHERE total_shares = 0;

ALTER TABLE position_lots
    DROP CONSTRAINT position_lots_status_check,
    DROP CONSTRAINT position_lots_close_shape,
    ADD CONSTRAINT position_lots_status_check CHECK (
        status IN ('OPEN', 'SETTLED_PENDING_REDEEM', 'CLOSED')
    ),
    ADD CONSTRAINT position_lots_close_shape CHECK (
        (status IN ('OPEN', 'SETTLED_PENDING_REDEEM')
            AND remaining_shares > 0 AND closed_at IS NULL)
        OR (status = 'CLOSED'
            AND remaining_shares = 0 AND remaining_cost = 0 AND closed_at IS NOT NULL)
    );

ALTER TABLE position_events
    DROP CONSTRAINT position_events_event_type_check,
    ADD CONSTRAINT position_events_event_type_check CHECK (
        event_type IN ('BOUGHT', 'SOLD', 'MARKED', 'SETTLED')
    );

CREATE TABLE reconciliation_runs (
    run_id TEXT PRIMARY KEY,
    execution_account_id TEXT NOT NULL REFERENCES execution_accounts(execution_account_id),
    trigger TEXT NOT NULL CHECK (trigger IN (
        'STARTUP', 'SCHEDULED', 'ORDER_UNKNOWN', 'CANCEL_UNKNOWN', 'ASSET_DRIFT'
    )),
    focus_order_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK (status IN (
        'RUNNING', 'COMPLETED', 'ATTENTION_REQUIRED', 'FAILED'
    )),
    summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    error TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    CONSTRAINT reconciliation_runs_completion_shape CHECK (
        (status = 'RUNNING' AND completed_at IS NULL)
        OR (status <> 'RUNNING' AND completed_at IS NOT NULL)
    )
);

CREATE INDEX reconciliation_runs_account_time_idx
    ON reconciliation_runs (execution_account_id, started_at DESC);

-- Cross-process exclusion. ReconciliationRecorder marks an abandoned RUNNING
-- row FAILED after its lease window before inserting a replacement.
CREATE UNIQUE INDEX reconciliation_runs_one_active_account_uidx
    ON reconciliation_runs (execution_account_id)
    WHERE status = 'RUNNING';

CREATE TABLE reconciliation_issues (
    issue_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES reconciliation_runs(run_id),
    fingerprint TEXT NOT NULL,
    execution_account_id TEXT NOT NULL REFERENCES execution_accounts(execution_account_id),
    issue_type TEXT NOT NULL,
    resolution TEXT NOT NULL CHECK (resolution IN (
        'AUTOMATIC', 'MANUAL_REVIEW', 'OBSERVED_ONLY', 'RETRY_LATER'
    )),
    status TEXT NOT NULL CHECK (status IN ('OPEN', 'RESOLVED')),
    order_id TEXT NOT NULL DEFAULT '',
    venue_order_id TEXT NOT NULL DEFAULT '',
    venue_trade_id TEXT NOT NULL DEFAULT '',
    market_id TEXT NOT NULL DEFAULT '',
    condition_id TEXT NOT NULL DEFAULT '',
    token_id TEXT NOT NULL DEFAULT '',
    local_value NUMERIC,
    remote_value NUMERIC,
    source TEXT NOT NULL DEFAULT '',
    details TEXT NOT NULL DEFAULT '',
    observed_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    CONSTRAINT reconciliation_issues_resolution_shape CHECK (
        (status = 'OPEN' AND resolved_at IS NULL)
        OR (status = 'RESOLVED' AND resolved_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX reconciliation_issues_open_fingerprint_uidx
    ON reconciliation_issues (execution_account_id, fingerprint)
    WHERE status = 'OPEN';

CREATE INDEX reconciliation_issues_manual_idx
    ON reconciliation_issues (execution_account_id, observed_at DESC)
    WHERE status = 'OPEN' AND resolution = 'MANUAL_REVIEW';

COMMIT;
