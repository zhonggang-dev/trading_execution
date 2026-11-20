-- Reverse migration 0053_account_orderbook_snapshots.sql, for rolling the
-- service back to a release that captures one shared orderbook batch per
-- decision boundary.
--
-- Run it as the migration owner (adminuser) BEFORE starting the old release,
-- with the new release already stopped:
--
--   psql "$ADMIN_DATABASE_URL" -v ON_ERROR_STOP=1 \
--     -f deploy/rollback_0053_account_orderbook_snapshots.sql
--
-- What it removes: only the per-account batches written by the new release
-- (execution_account_id <> ''). The strategy inputs of those cycles keep the
-- exact orderbooks in strategy_decision_runs.input_payload. Legacy shared
-- batches (execution_account_id = '') are kept unchanged.
--
-- The unique index execution_strategy_bindings_enabled_model_strategy_uidx is
-- recreated at the end. If two enabled authorization rows share
-- (model_id, strategy_id) (for example new wallets added after 0053), disable
-- those rows first with a version bump, or the whole script rolls back.

BEGIN;

ALTER TABLE strategy_orderbook_snapshots
    DISABLE TRIGGER strategy_orderbook_snapshots_append_only_trigger;
ALTER TABLE strategy_orderbook_snapshot_batches
    DISABLE TRIGGER strategy_orderbook_snapshot_batches_append_only_trigger;

DELETE FROM strategy_orderbook_snapshots WHERE execution_account_id <> '';
DELETE FROM strategy_orderbook_snapshot_batches WHERE execution_account_id <> '';

ALTER TABLE strategy_orderbook_snapshots
    ENABLE TRIGGER strategy_orderbook_snapshots_append_only_trigger;
ALTER TABLE strategy_orderbook_snapshot_batches
    ENABLE TRIGGER strategy_orderbook_snapshot_batches_append_only_trigger;

ALTER TABLE strategy_orderbook_snapshots
    DROP CONSTRAINT strategy_orderbook_snapshots_batch_fk;
ALTER TABLE strategy_orderbook_snapshots
    DROP CONSTRAINT strategy_orderbook_snapshots_pkey;
ALTER TABLE strategy_orderbook_snapshot_batches
    DROP CONSTRAINT strategy_orderbook_snapshot_batches_pkey;

ALTER TABLE strategy_orderbook_snapshot_batches
    ADD CONSTRAINT strategy_orderbook_snapshot_batches_pkey PRIMARY KEY (decision_at);
ALTER TABLE strategy_orderbook_snapshots
    ADD CONSTRAINT strategy_orderbook_snapshots_pkey
    PRIMARY KEY (decision_at, market_source, token_id);
ALTER TABLE strategy_orderbook_snapshots
    ADD CONSTRAINT strategy_orderbook_snapshots_batch_fk
    FOREIGN KEY (decision_at)
    REFERENCES strategy_orderbook_snapshot_batches (decision_at) ON DELETE RESTRICT;

-- Original 0026 body: one batch per boundary.
CREATE OR REPLACE FUNCTION execution_require_complete_strategy_orderbook_snapshot_batch()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    expected_count INTEGER;
    actual_count INTEGER;
    expected_ok INTEGER;
    expected_empty INTEGER;
    expected_missing INTEGER;
    expected_error INTEGER;
    actual_ok INTEGER;
    actual_empty INTEGER;
    actual_missing INTEGER;
    actual_error INTEGER;
BEGIN
    SELECT snapshot_count, ok_count, empty_count, missing_count, error_count
    INTO expected_count, expected_ok, expected_empty, expected_missing, expected_error
    FROM strategy_orderbook_snapshot_batches
    WHERE decision_at = NEW.decision_at;

    SELECT count(*),
           count(*) FILTER (WHERE status = 'OK'),
           count(*) FILTER (WHERE status = 'EMPTY'),
           count(*) FILTER (WHERE status = 'MISSING'),
           count(*) FILTER (WHERE status = 'ERROR')
    INTO actual_count, actual_ok, actual_empty, actual_missing, actual_error
    FROM strategy_orderbook_snapshots
    WHERE decision_at = NEW.decision_at;

    IF expected_count IS NULL
        OR actual_count <> expected_count
        OR actual_ok <> expected_ok
        OR actual_empty <> expected_empty
        OR actual_missing <> expected_missing
        OR actual_error <> expected_error
    THEN
        RAISE EXCEPTION 'strategy orderbook snapshot batch is incomplete';
    END IF;
    RETURN NULL;
END;
$$;

ALTER TABLE strategy_orderbook_snapshots DROP COLUMN execution_account_id;
ALTER TABLE strategy_orderbook_snapshot_batches DROP COLUMN execution_account_id;

CREATE UNIQUE INDEX execution_strategy_bindings_enabled_model_strategy_uidx
    ON execution_strategy_bindings (model_id, strategy_id)
    WHERE enabled;

COMMIT;
