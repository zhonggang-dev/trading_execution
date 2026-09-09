BEGIN;

-- One immutable header seals the complete shared capture used by every
-- strategy binding at a global decision boundary.
CREATE TABLE strategy_orderbook_snapshot_batches (
    decision_at TIMESTAMPTZ NOT NULL,
    prediction_snapshot_id TEXT NOT NULL,
    snapshot_set_id TEXT NOT NULL,
    target_count INTEGER NOT NULL,
    snapshot_count INTEGER NOT NULL,
    ok_count INTEGER NOT NULL,
    empty_count INTEGER NOT NULL,
    missing_count INTEGER NOT NULL,
    error_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT strategy_orderbook_snapshot_batches_pkey PRIMARY KEY (decision_at),
    CONSTRAINT strategy_orderbook_snapshot_batches_set_unique UNIQUE (snapshot_set_id),
    CONSTRAINT strategy_orderbook_snapshot_batches_identity_nonempty CHECK (
        prediction_snapshot_id <> '' AND snapshot_set_id <> ''
    ),
    CONSTRAINT strategy_orderbook_snapshot_batches_count_shape CHECK (
        target_count >= 0 AND snapshot_count >= 0
        AND target_count = snapshot_count
        AND ok_count >= 0 AND empty_count >= 0
        AND missing_count >= 0 AND error_count >= 0
        AND ok_count + empty_count + missing_count + error_count = snapshot_count
    )
);

CREATE TABLE strategy_orderbook_snapshots (
    decision_at TIMESTAMPTZ NOT NULL,
    market_source TEXT NOT NULL,
    market_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome_index INTEGER NOT NULL,
    outcome_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    error_code TEXT NOT NULL DEFAULT '',
    source_at TIMESTAMPTZ,
    observed_at TIMESTAMPTZ NOT NULL,
    tick_size NUMERIC,
    min_order_size NUMERIC,
    best_bid NUMERIC,
    best_ask NUMERIC,
    depth_limit INTEGER NOT NULL,
    bids JSONB NOT NULL,
    asks JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT strategy_orderbook_snapshots_pkey
        PRIMARY KEY (decision_at, market_source, token_id),
    CONSTRAINT strategy_orderbook_snapshots_batch_fk
        FOREIGN KEY (decision_at)
        REFERENCES strategy_orderbook_snapshot_batches(decision_at) ON DELETE RESTRICT,
    CONSTRAINT strategy_orderbook_snapshots_identity_nonempty CHECK (
        market_source <> '' AND market_id <> '' AND condition_id <> '' AND token_id <> ''
    ),
    CONSTRAINT strategy_orderbook_snapshots_market_source CHECK (
        market_source IN ('POLYMARKET', 'KALSHI')
    ),
    CONSTRAINT strategy_orderbook_snapshots_outcome_index CHECK (outcome_index IN (0, 1)),
    CONSTRAINT strategy_orderbook_snapshots_status CHECK (
        status IN ('OK', 'EMPTY', 'MISSING', 'ERROR')
    ),
    CONSTRAINT strategy_orderbook_snapshots_depth CHECK (depth_limit BETWEEN 0 AND 15),
    CONSTRAINT strategy_orderbook_snapshots_levels_arrays CHECK (
        jsonb_typeof(bids) = 'array' AND jsonb_typeof(asks) = 'array'
        AND jsonb_array_length(bids) <= 15 AND jsonb_array_length(asks) <= 15
    ),
    CONSTRAINT strategy_orderbook_snapshots_positive_metadata CHECK (
        (tick_size IS NULL OR tick_size > 0)
        AND (min_order_size IS NULL OR min_order_size > 0)
        AND (best_bid IS NULL OR best_bid > 0)
        AND (best_ask IS NULL OR best_ask > 0)
    ),
    CONSTRAINT strategy_orderbook_snapshots_best_levels CHECK (
        (
            (jsonb_array_length(bids) = 0 AND best_bid IS NULL)
            OR (
                jsonb_array_length(bids) > 0
                AND jsonb_typeof(bids -> 0) = 'object'
                AND best_bid = ((bids -> 0) ->> 'price')::numeric
            )
        )
        AND (
            (jsonb_array_length(asks) = 0 AND best_ask IS NULL)
            OR (
                jsonb_array_length(asks) > 0
                AND jsonb_typeof(asks -> 0) = 'object'
                AND best_ask = ((asks -> 0) ->> 'price')::numeric
            )
        )
    ),
    CONSTRAINT strategy_orderbook_snapshots_book_shape CHECK (
        status <> 'OK'
        OR (
            source_at IS NOT NULL
            AND jsonb_array_length(bids) > 0
            AND jsonb_array_length(asks) > 0
            AND best_bid IS NOT NULL
            AND best_ask IS NOT NULL
        )
    ),
    CONSTRAINT strategy_orderbook_snapshots_failure_shape CHECK (
        status NOT IN ('MISSING', 'ERROR')
        OR (
            error_code <> ''
            AND jsonb_array_length(bids) = 0
            AND jsonb_array_length(asks) = 0
            AND best_bid IS NULL
            AND best_ask IS NULL
        )
    )
);

CREATE INDEX strategy_orderbook_snapshots_token_time_idx
    ON strategy_orderbook_snapshots (token_id, decision_at DESC);

CREATE INDEX strategy_orderbook_snapshots_condition_time_idx
    ON strategy_orderbook_snapshots (condition_id, decision_at DESC);

CREATE INDEX strategy_orderbook_snapshots_time_brin_idx
    ON strategy_orderbook_snapshots USING BRIN (decision_at);

CREATE FUNCTION execution_require_complete_strategy_orderbook_snapshot_batch()
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

CREATE CONSTRAINT TRIGGER strategy_orderbook_snapshot_batches_complete_trigger
AFTER INSERT ON strategy_orderbook_snapshot_batches
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION execution_require_complete_strategy_orderbook_snapshot_batch();

CREATE CONSTRAINT TRIGGER strategy_orderbook_snapshots_complete_trigger
AFTER INSERT ON strategy_orderbook_snapshots
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION execution_require_complete_strategy_orderbook_snapshot_batch();

CREATE FUNCTION execution_reject_strategy_orderbook_snapshot_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'strategy orderbook snapshots are append-only';
END;
$$;

CREATE TRIGGER strategy_orderbook_snapshot_batches_append_only_trigger
BEFORE UPDATE OR DELETE OR TRUNCATE ON strategy_orderbook_snapshot_batches
FOR EACH STATEMENT
EXECUTE FUNCTION execution_reject_strategy_orderbook_snapshot_mutation();

CREATE TRIGGER strategy_orderbook_snapshots_append_only_trigger
BEFORE UPDATE OR DELETE OR TRUNCATE ON strategy_orderbook_snapshots
FOR EACH STATEMENT
EXECUTE FUNCTION execution_reject_strategy_orderbook_snapshot_mutation();

COMMIT;
