BEGIN;

CREATE TABLE execution_orders (
    order_id TEXT PRIMARY KEY,
    client_order_id TEXT NOT NULL UNIQUE,
    execution_account_id TEXT NOT NULL REFERENCES execution_accounts(execution_account_id),
    venue TEXT NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    intent JSONB NOT NULL,
    market_validation JSONB,
    venue_order_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK (status IN (
        'RECEIVED', 'VALIDATING', 'RESERVED', 'SUBMITTING',
        'ACKNOWLEDGED', 'LIVE', 'PARTIALLY_FILLED', 'FILLED',
        'REJECTED', 'UNKNOWN', 'CANCEL_PENDING', 'CANCELLED',
        'RECONCILING', 'MANUAL_REVIEW'
    )),
    filled_size NUMERIC NOT NULL DEFAULT 0 CHECK (filled_size >= 0),
    average_fill_price NUMERIC,
    failure_code TEXT NOT NULL DEFAULT '',
    failure_reason TEXT NOT NULL DEFAULT '',
    venue_last_observed_at TIMESTAMPTZ,
    revision BIGINT NOT NULL CHECK (revision >= 1),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT execution_orders_fill_price_shape CHECK (
        average_fill_price IS NULL OR average_fill_price > 0
    )
);

CREATE UNIQUE INDEX execution_orders_venue_order_uidx
    ON execution_orders (venue, venue_order_id)
    WHERE venue_order_id <> '';

CREATE INDEX execution_orders_reconciliation_idx
    ON execution_orders (status, updated_at)
    WHERE status IN (
        'RECEIVED', 'VALIDATING', 'RESERVED', 'SUBMITTING', 'ACKNOWLEDGED',
        'LIVE', 'PARTIALLY_FILLED', 'UNKNOWN', 'CANCEL_PENDING', 'RECONCILING'
    );

CREATE TABLE execution_order_events (
    event_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL REFERENCES execution_orders(order_id),
    revision BIGINT NOT NULL,
    from_status TEXT NOT NULL DEFAULT '',
    to_status TEXT NOT NULL,
    trigger TEXT NOT NULL,
    attempt_id TEXT NOT NULL DEFAULT '',
    reason_code TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL DEFAULT '',
    venue_status TEXT NOT NULL DEFAULT '',
    venue_order_id TEXT NOT NULL DEFAULT '',
    filled_size NUMERIC NOT NULL DEFAULT 0,
    average_fill_price NUMERIC,
    venue_observed_at TIMESTAMPTZ,
    occurred_at TIMESTAMPTZ NOT NULL,
    UNIQUE (order_id, revision)
);

-- Attempts never store private keys, API credentials, signed payloads, or
-- signatures. request_fingerprint is a non-secret SHA-256 audit identity.
CREATE TABLE execution_order_attempts (
    attempt_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL REFERENCES execution_orders(order_id),
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (kind IN ('SUBMIT', 'CANCEL', 'RECONCILE')),
    outcome TEXT NOT NULL CHECK (outcome IN ('STARTED', 'SUCCEEDED', 'REJECTED', 'UNKNOWN', 'FAILED')),
    request_fingerprint TEXT NOT NULL DEFAULT '',
    venue_order_id TEXT NOT NULL DEFAULT '',
    venue_status TEXT NOT NULL DEFAULT '',
    http_status INTEGER,
    error_code TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    UNIQUE (order_id, sequence),
    CONSTRAINT execution_order_attempts_completion_shape CHECK (
        (outcome = 'STARTED' AND completed_at IS NULL)
        OR (outcome <> 'STARTED' AND completed_at IS NOT NULL)
    )
);

COMMIT;
