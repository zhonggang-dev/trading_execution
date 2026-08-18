BEGIN;

-- CLOB V2 settles against Polymarket USD. Existing rows are deliberately not
-- rewritten; production migration must reconcile each wallet's actual asset.
ALTER TABLE execution_accounts
    ALTER COLUMN collateral_asset SET DEFAULT 'pUSD';

ALTER TABLE execution_orders
    ADD COLUMN filled_notional NUMERIC NOT NULL DEFAULT 0 CHECK (filled_notional >= 0),
    ADD COLUMN total_fees NUMERIC NOT NULL DEFAULT 0 CHECK (total_fees >= 0);

ALTER TABLE execution_order_events
    ADD COLUMN fill_key TEXT NOT NULL DEFAULT '',
    ADD COLUMN filled_notional NUMERIC NOT NULL DEFAULT 0 CHECK (filled_notional >= 0),
    ADD COLUMN total_fees NUMERIC NOT NULL DEFAULT 0 CHECK (total_fees >= 0);

ALTER TABLE asset_reservations
    ADD COLUMN settled_fees NUMERIC NOT NULL DEFAULT 0 CHECK (settled_fees >= 0);

ALTER TABLE asset_reservation_events
    ADD COLUMN cumulative_fees NUMERIC NOT NULL DEFAULT 0 CHECK (cumulative_fees >= 0);

ALTER TABLE execution_positions
    ADD COLUMN condition_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN outcome_index SMALLINT CHECK (outcome_index IN (0, 1)),
    ADD COLUMN outcome_name TEXT NOT NULL DEFAULT '',
    ADD COLUMN cost_basis NUMERIC NOT NULL DEFAULT 0 CHECK (cost_basis >= 0),
    ADD COLUMN average_cost_price NUMERIC NOT NULL DEFAULT 0 CHECK (average_cost_price >= 0),
    ADD COLUMN realized_pnl NUMERIC NOT NULL DEFAULT 0,
    ADD COLUMN mark_price NUMERIC CHECK (mark_price > 0),
    ADD COLUMN market_value NUMERIC NOT NULL DEFAULT 0 CHECK (market_value >= 0),
    ADD COLUMN unrealized_pnl NUMERIC NOT NULL DEFAULT 0,
    ADD COLUMN is_dust BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN last_marked_at TIMESTAMPTZ;

CREATE TABLE execution_fills (
    fill_key TEXT PRIMARY KEY,
    venue TEXT NOT NULL,
    venue_fill_id TEXT NOT NULL,
    order_id TEXT NOT NULL REFERENCES execution_orders(order_id),
    venue_order_id TEXT NOT NULL,
    execution_account_id TEXT NOT NULL REFERENCES execution_accounts(execution_account_id),
    market_id TEXT NOT NULL,
    condition_id TEXT NOT NULL DEFAULT '',
    token_id TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    liquidity_role TEXT NOT NULL CHECK (liquidity_role IN ('MAKER', 'TAKER')),
    status TEXT NOT NULL CHECK (status IN ('MATCHED', 'MINED', 'CONFIRMED', 'RETRYING', 'FAILED')),
    shares NUMERIC NOT NULL CHECK (shares > 0),
    price NUMERIC NOT NULL CHECK (price > 0 AND price < 1),
    gross_notional NUMERIC NOT NULL CHECK (gross_notional >= 0),
    fee_rate_bps NUMERIC NOT NULL CHECK (fee_rate_bps >= 0),
    platform_fee NUMERIC NOT NULL CHECK (platform_fee >= 0),
    builder_fee_rate_bps NUMERIC NOT NULL CHECK (builder_fee_rate_bps >= 0),
    builder_fee NUMERIC NOT NULL CHECK (builder_fee >= 0),
    total_fee NUMERIC NOT NULL CHECK (total_fee >= 0),
    net_cash_delta NUMERIC NOT NULL,
    fee_source TEXT NOT NULL,
    transaction_hash TEXT NOT NULL DEFAULT '',
    raw_payload_sha256 TEXT NOT NULL DEFAULT '',
    matched_at TIMESTAMPTZ NOT NULL,
    venue_updated_at TIMESTAMPTZ,
    first_observed_at TIMESTAMPTZ NOT NULL,
    last_observed_at TIMESTAMPTZ NOT NULL,
    confirmed_at TIMESTAMPTZ,
    applied_at TIMESTAMPTZ,
    UNIQUE (venue, venue_fill_id, order_id),
    CONSTRAINT execution_fills_confirmation_shape CHECK (
        (status = 'CONFIRMED' AND confirmed_at IS NOT NULL)
        OR status <> 'CONFIRMED'
    ),
    CONSTRAINT execution_fills_application_shape CHECK (
        applied_at IS NULL OR status = 'CONFIRMED'
    )
);

CREATE INDEX execution_fills_order_idx
    ON execution_fills (order_id, matched_at, venue_fill_id);
CREATE INDEX execution_fills_pending_idx
    ON execution_fills (status, last_observed_at)
    WHERE applied_at IS NULL AND status <> 'FAILED';

CREATE TABLE execution_fill_events (
    fill_event_id TEXT PRIMARY KEY,
    fill_key TEXT NOT NULL REFERENCES execution_fills(fill_key),
    order_id TEXT NOT NULL REFERENCES execution_orders(order_id),
    status TEXT NOT NULL,
    transaction_hash TEXT NOT NULL DEFAULT '',
    venue_updated_at TIMESTAMPTZ,
    observed_at TIMESTAMPTZ NOT NULL,
    payload_sha256 TEXT NOT NULL DEFAULT '',
    UNIQUE (fill_key, status, transaction_hash, observed_at)
);

CREATE TABLE position_lots (
    lot_id TEXT PRIMARY KEY,
    execution_account_id TEXT NOT NULL REFERENCES execution_accounts(execution_account_id),
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    opening_order_id TEXT NOT NULL REFERENCES execution_orders(order_id),
    opening_fill_key TEXT NOT NULL UNIQUE REFERENCES execution_fills(fill_key),
    original_shares NUMERIC NOT NULL CHECK (original_shares > 0),
    remaining_shares NUMERIC NOT NULL CHECK (remaining_shares >= 0),
    original_cost NUMERIC NOT NULL CHECK (original_cost > 0),
    remaining_cost NUMERIC NOT NULL CHECK (remaining_cost >= 0),
    average_entry_price NUMERIC NOT NULL CHECK (average_entry_price > 0),
    status TEXT NOT NULL CHECK (status IN ('OPEN', 'CLOSED')),
    opened_at TIMESTAMPTZ NOT NULL,
    closed_at TIMESTAMPTZ,
    CONSTRAINT position_lots_bounds CHECK (
        remaining_shares <= original_shares AND remaining_cost <= original_cost
    ),
    CONSTRAINT position_lots_close_shape CHECK (
        (status = 'OPEN' AND remaining_shares > 0 AND closed_at IS NULL)
        OR (status = 'CLOSED' AND remaining_shares = 0 AND remaining_cost = 0 AND closed_at IS NOT NULL)
    )
);

CREATE INDEX position_lots_open_idx
    ON position_lots (execution_account_id, token_id, opened_at, lot_id)
    WHERE status = 'OPEN';

-- A closure is tied to the exact source lot selected by the strategy. Aggregate
-- position cost basis remains the sum of all OPEN lot remaining_cost values.
CREATE TABLE position_lot_closures (
    closure_id TEXT PRIMARY KEY,
    closing_fill_key TEXT NOT NULL REFERENCES execution_fills(fill_key),
    lot_id TEXT NOT NULL REFERENCES position_lots(lot_id),
    closed_shares NUMERIC NOT NULL CHECK (closed_shares > 0),
    allocated_cost NUMERIC NOT NULL CHECK (allocated_cost >= 0),
    allocated_net_proceeds NUMERIC NOT NULL CHECK (allocated_net_proceeds >= 0),
    realized_pnl NUMERIC NOT NULL,
    closed_at TIMESTAMPTZ NOT NULL,
    UNIQUE (closing_fill_key, lot_id)
);

CREATE TABLE position_events (
    position_event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL CHECK (event_type IN ('BOUGHT', 'SOLD', 'MARKED')),
    execution_account_id TEXT NOT NULL REFERENCES execution_accounts(execution_account_id),
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    order_id TEXT NOT NULL DEFAULT '',
    fill_key TEXT NOT NULL DEFAULT '',
    model_id TEXT NOT NULL DEFAULT '',
    strategy_id TEXT NOT NULL DEFAULT '',
    shares_delta NUMERIC NOT NULL,
    cash_delta NUMERIC NOT NULL,
    cost_basis_delta NUMERIC NOT NULL,
    realized_pnl_delta NUMERIC NOT NULL,
    shares_after NUMERIC NOT NULL CHECK (shares_after >= 0),
    cost_basis_after NUMERIC NOT NULL CHECK (cost_basis_after >= 0),
    average_cost_after NUMERIC NOT NULL CHECK (average_cost_after >= 0),
    realized_pnl_after NUMERIC NOT NULL,
    mark_price NUMERIC,
    unrealized_pnl_after NUMERIC NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX position_events_position_idx
    ON position_events (execution_account_id, token_id, occurred_at, position_event_id);

CREATE TABLE execution_account_events (
    account_event_id TEXT PRIMARY KEY,
    execution_account_id TEXT NOT NULL REFERENCES execution_accounts(execution_account_id),
    event_type TEXT NOT NULL,
    order_id TEXT NOT NULL DEFAULT '',
    fill_key TEXT NOT NULL DEFAULT '',
    total_balance_delta NUMERIC NOT NULL,
    available_balance_delta NUMERIC NOT NULL,
    reserved_balance_delta NUMERIC NOT NULL,
    total_balance_after NUMERIC NOT NULL,
    available_balance_after NUMERIC NOT NULL,
    reserved_balance_after NUMERIC NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX execution_account_events_account_idx
    ON execution_account_events (execution_account_id, occurred_at, account_event_id);

-- Transactional outbox: fill + ledger changes and the event to publish commit
-- together. A dispatcher can retry publication without replaying accounting.
CREATE TABLE execution_outbox (
    outbox_event_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    event_key TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PUBLISHED')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    UNIQUE (topic, event_key)
);

CREATE INDEX execution_outbox_pending_idx
    ON execution_outbox (next_attempt_at, created_at)
    WHERE status = 'PENDING';

COMMIT;
