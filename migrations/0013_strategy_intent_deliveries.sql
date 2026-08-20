BEGIN;

-- A decision output must remember the state of the independent submission
-- gate at the moment the output was first accepted. Historical outputs are
-- fail-closed: they were recorded before durable delivery existed and must
-- never become live merely because a later deployment enables submission.
ALTER TABLE strategy_decision_runs
    ADD COLUMN order_submission_enabled BOOLEAN;

UPDATE strategy_decision_runs
SET order_submission_enabled = FALSE
WHERE output_payload IS NOT NULL;

ALTER TABLE strategy_decision_runs
    ADD CONSTRAINT strategy_decision_runs_submission_mode_shape CHECK (
        (output_payload IS NULL AND order_submission_enabled IS NULL)
        OR (output_payload IS NOT NULL AND order_submission_enabled IS NOT NULL)
    );

CREATE TABLE strategy_order_intent_deliveries (
    client_order_id TEXT PRIMARY KEY,
    cycle_id TEXT NOT NULL REFERENCES strategy_decision_runs(cycle_id) ON DELETE RESTRICT,
    sequence_no INTEGER NOT NULL,
    intent_payload JSONB NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    claimed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    order_id TEXT,
    order_status TEXT,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT strategy_order_intent_deliveries_cycle_sequence_unique
        UNIQUE (cycle_id, sequence_no),
    CONSTRAINT strategy_order_intent_deliveries_identity_nonempty CHECK (
        client_order_id <> '' AND cycle_id <> '' AND sequence_no >= 0
    ),
    CONSTRAINT strategy_order_intent_deliveries_payload_object CHECK (
        jsonb_typeof(intent_payload) = 'object'
    ),
    CONSTRAINT strategy_order_intent_deliveries_status CHECK (
        status IN ('PENDING', 'SUBMITTING', 'SUBMITTED', 'FAILED', 'UNKNOWN')
    ),
    CONSTRAINT strategy_order_intent_deliveries_attempt_nonnegative CHECK (
        attempt_count >= 0
    ),
    CONSTRAINT strategy_order_intent_deliveries_state_shape CHECK (
        (status = 'PENDING' AND claimed_at IS NULL AND completed_at IS NULL)
        OR (status = 'SUBMITTING' AND claimed_at IS NOT NULL AND completed_at IS NULL)
        OR (status IN ('SUBMITTED', 'FAILED', 'UNKNOWN')
            AND claimed_at IS NOT NULL AND completed_at IS NOT NULL)
    ),
    CONSTRAINT strategy_order_intent_deliveries_result_shape CHECK (
        (status IN ('PENDING', 'SUBMITTING')
            AND order_id IS NULL AND order_status IS NULL)
        OR (status IN ('SUBMITTED', 'FAILED', 'UNKNOWN')
            AND order_id IS NOT NULL AND order_id <> ''
            AND order_status IS NOT NULL AND order_status <> '')
    )
);

CREATE INDEX strategy_order_intent_deliveries_pending_idx
    ON strategy_order_intent_deliveries (created_at, cycle_id, sequence_no)
    WHERE status = 'PENDING';

CREATE INDEX strategy_order_intent_deliveries_stale_idx
    ON strategy_order_intent_deliveries (claimed_at, cycle_id, sequence_no)
    WHERE status = 'SUBMITTING';

COMMIT;
