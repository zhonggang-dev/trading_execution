BEGIN;

-- Every execution account captures its own orderbooks right before its
-- strategy call, so a decision boundary now has one immutable batch per
-- account. Existing rows were captured once per boundary and shared by every
-- account; they keep execution_account_id = '' and stay valid evidence.
ALTER TABLE strategy_orderbook_snapshot_batches
    ADD COLUMN execution_account_id TEXT NOT NULL DEFAULT '';
ALTER TABLE strategy_orderbook_snapshots
    ADD COLUMN execution_account_id TEXT NOT NULL DEFAULT '';

ALTER TABLE strategy_orderbook_snapshots
    DROP CONSTRAINT strategy_orderbook_snapshots_batch_fk;
ALTER TABLE strategy_orderbook_snapshots
    DROP CONSTRAINT strategy_orderbook_snapshots_pkey;
ALTER TABLE strategy_orderbook_snapshot_batches
    DROP CONSTRAINT strategy_orderbook_snapshot_batches_pkey;

ALTER TABLE strategy_orderbook_snapshot_batches
    ADD CONSTRAINT strategy_orderbook_snapshot_batches_pkey
    PRIMARY KEY (decision_at, execution_account_id);
ALTER TABLE strategy_orderbook_snapshots
    ADD CONSTRAINT strategy_orderbook_snapshots_pkey
    PRIMARY KEY (decision_at, execution_account_id, market_source, token_id);
-- Every existing child row has '' like its header, so the key validates.
ALTER TABLE strategy_orderbook_snapshots
    ADD CONSTRAINT strategy_orderbook_snapshots_batch_fk
    FOREIGN KEY (decision_at, execution_account_id)
    REFERENCES strategy_orderbook_snapshot_batches (decision_at, execution_account_id)
    ON DELETE RESTRICT;

-- The deferred completeness check must compare one batch with its own rows;
-- counting every row of the boundary would reject the second account's batch.
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
    WHERE decision_at = NEW.decision_at
      AND execution_account_id = NEW.execution_account_id;

    SELECT count(*),
           count(*) FILTER (WHERE status = 'OK'),
           count(*) FILTER (WHERE status = 'EMPTY'),
           count(*) FILTER (WHERE status = 'MISSING'),
           count(*) FILTER (WHERE status = 'ERROR')
    INTO actual_count, actual_ok, actual_empty, actual_missing, actual_error
    FROM strategy_orderbook_snapshots
    WHERE decision_at = NEW.decision_at
      AND execution_account_id = NEW.execution_account_id;

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

-- Accounts are no longer serialized behind one model label per strategy:
-- several enabled authorization rows may share (model_id, strategy_id).
DROP INDEX execution_strategy_bindings_enabled_model_strategy_uidx;

COMMIT;
