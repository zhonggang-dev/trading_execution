BEGIN;
SET LOCAL search_path=public,pg_temp;

CREATE TABLE wallet_drawdown_order_operations (
    client_order_id TEXT PRIMARY KEY CHECK (client_order_id <> ''),
    execution_account_id TEXT NOT NULL,
    slot_at TIMESTAMPTZ NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    market_id TEXT NOT NULL CHECK (market_id <> ''),
    condition_id TEXT NOT NULL CHECK (condition_id <> ''),
    token_id TEXT NOT NULL CHECK (token_id <> ''),
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    time_in_force TEXT NOT NULL,
    price NUMERIC NOT NULL CHECK (price > 0),
    size NUMERIC NOT NULL CHECK (size > 0),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    submission_state TEXT NOT NULL DEFAULT 'PREPARED'
        CHECK (submission_state IN ('PREPARED', 'ORDER_RECORDED', 'SUBMIT_UNCONFIRMED')),
    submit_finished_at TIMESTAMPTZ,
    UNIQUE (execution_account_id, slot_at, attempt),
    FOREIGN KEY (execution_account_id, slot_at)
        REFERENCES wallet_drawdown_samples (execution_account_id, slot_at),
    CHECK ((submission_state = 'PREPARED') = (submit_finished_at IS NULL))
);

REVOKE ALL ON wallet_drawdown_order_operations FROM PUBLIC;
DO $$ BEGIN
    IF to_regrole('trading_execution_app') IS NOT NULL THEN
        GRANT SELECT, INSERT ON wallet_drawdown_order_operations TO trading_execution_app;
        GRANT UPDATE (submission_state, submit_finished_at) ON wallet_drawdown_order_operations TO trading_execution_app;
    END IF;
END $$;
COMMIT;
