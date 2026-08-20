BEGIN;

-- The complete algorithm input and output are immutable audit snapshots. A
-- retry may reuse a cycle only when its content-addressed input and the exact
-- recorded output are identical.
CREATE TABLE strategy_decision_runs (
    cycle_id TEXT PRIMARY KEY,
    input_id TEXT NOT NULL UNIQUE,
    decision_at TIMESTAMPTZ NOT NULL,
    model_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    execution_account_id TEXT NOT NULL,
    input_payload JSONB NOT NULL,
    output_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    decided_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT strategy_decision_runs_identity_nonempty CHECK (
        cycle_id <> '' AND input_id <> '' AND model_id <> ''
        AND strategy_id <> '' AND execution_account_id <> ''
    ),
    CONSTRAINT strategy_decision_runs_output_time CHECK (
        (output_payload IS NULL AND decided_at IS NULL)
        OR (output_payload IS NOT NULL AND decided_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX strategy_decision_runs_account_time_idx
    ON strategy_decision_runs (execution_account_id, decision_at DESC);

COMMIT;
