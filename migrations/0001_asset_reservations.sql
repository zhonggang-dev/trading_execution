BEGIN;

-- One row is the serialization point for every operation using an execution
-- account. All live reservation transactions lock this row first.
CREATE TABLE execution_accounts (
    execution_account_id TEXT PRIMARY KEY,
    wallet_address TEXT NOT NULL,
    collateral_asset TEXT NOT NULL DEFAULT 'USDC',
    total_balance NUMERIC NOT NULL DEFAULT 0,
    available_balance NUMERIC NOT NULL DEFAULT 0,
    reserved_balance NUMERIC NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 1,
    reconciled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT execution_accounts_non_negative CHECK (
        total_balance >= 0 AND available_balance >= 0 AND reserved_balance >= 0
    ),
    CONSTRAINT execution_accounts_balance_identity CHECK (
        total_balance = available_balance + reserved_balance
    )
);

-- Two logical execution accounts must not independently reserve the same
-- physical wallet. Wallet addresses are normalized case-insensitively here.
CREATE UNIQUE INDEX execution_accounts_wallet_asset_uidx
    ON execution_accounts (LOWER(wallet_address), collateral_asset);

-- Positions are account/token scoped. All mutations still lock the parent
-- execution_accounts row before locking this row, which gives every code path
-- the same deadlock-free lock order.
CREATE TABLE execution_positions (
    execution_account_id TEXT NOT NULL REFERENCES execution_accounts(execution_account_id),
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    total_shares NUMERIC NOT NULL DEFAULT 0,
    available_shares NUMERIC NOT NULL DEFAULT 0,
    reserved_shares NUMERIC NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 1,
    reconciled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (execution_account_id, token_id),
    CONSTRAINT execution_positions_identity_nonempty CHECK (market_id <> '' AND token_id <> ''),
    CONSTRAINT execution_positions_non_negative CHECK (
        total_shares >= 0 AND available_shares >= 0 AND reserved_shares >= 0
    ),
    CONSTRAINT execution_positions_share_identity CHECK (
        total_shares = available_shares + reserved_shares
    )
);

CREATE TABLE asset_reservations (
    order_id TEXT PRIMARY KEY,
    client_order_id TEXT NOT NULL UNIQUE,
    intent_fingerprint TEXT NOT NULL,
    execution_account_id TEXT NOT NULL REFERENCES execution_accounts(execution_account_id),
    strategy_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    requested_shares NUMERIC NOT NULL CHECK (requested_shares > 0),
    reserve_unit_price NUMERIC NOT NULL CHECK (reserve_unit_price >= 0),
    initial_reserved_balance NUMERIC NOT NULL DEFAULT 0,
    remaining_reserved_balance NUMERIC NOT NULL DEFAULT 0,
    initial_reserved_shares NUMERIC NOT NULL DEFAULT 0,
    remaining_reserved_shares NUMERIC NOT NULL DEFAULT 0,
    settled_shares NUMERIC NOT NULL DEFAULT 0,
    settled_notional NUMERIC NOT NULL DEFAULT 0,
    status TEXT NOT NULL CHECK (
        status IN ('ACTIVE', 'RELEASED', 'SETTLED', 'RECONCILIATION_REQUIRED')
    ),
    uncertain_reason TEXT NOT NULL DEFAULT '',
    last_venue_observed_at TIMESTAMPTZ,
    revision BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    released_at TIMESTAMPTZ,
    CONSTRAINT asset_reservations_non_negative CHECK (
        initial_reserved_balance >= 0 AND remaining_reserved_balance >= 0
        AND initial_reserved_shares >= 0 AND remaining_reserved_shares >= 0
        AND settled_shares >= 0 AND settled_notional >= 0
    ),
    CONSTRAINT asset_reservations_bounds CHECK (
        remaining_reserved_balance <= initial_reserved_balance
        AND remaining_reserved_shares <= initial_reserved_shares
        AND settled_shares <= requested_shares
    ),
    CONSTRAINT asset_reservations_side_shape CHECK (
        (side = 'BUY'
            AND reserve_unit_price > 0
            AND initial_reserved_balance > 0
            AND initial_reserved_shares = 0
            AND remaining_reserved_shares = 0)
        OR
        (side = 'SELL'
            AND initial_reserved_balance = 0
            AND remaining_reserved_balance = 0
            AND initial_reserved_shares = requested_shares)
    ),
    CONSTRAINT asset_reservations_terminal_empty CHECK (
        status IN ('ACTIVE', 'RECONCILIATION_REQUIRED')
        OR (remaining_reserved_balance = 0 AND remaining_reserved_shares = 0)
    )
);

CREATE INDEX asset_reservations_active_account_idx
    ON asset_reservations (execution_account_id, status)
    WHERE status IN ('ACTIVE', 'RECONCILIATION_REQUIRED');

CREATE UNIQUE INDEX asset_reservations_active_token_side_uidx
    ON asset_reservations (execution_account_id, token_id, side)
    WHERE status IN ('ACTIVE', 'RECONCILIATION_REQUIRED');

-- Append-only audit events make cumulative reconciliation explainable. The
-- unique key prevents retries from duplicating an externally visible event.
CREATE TABLE asset_reservation_events (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id TEXT NOT NULL REFERENCES asset_reservations(order_id),
    event_key TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (
        event_type IN ('RESERVED', 'RECONCILED', 'RELEASED', 'MARKED_UNCERTAIN')
    ),
    order_status TEXT NOT NULL,
    cumulative_filled_shares NUMERIC NOT NULL DEFAULT 0,
    cumulative_fill_notional NUMERIC NOT NULL DEFAULT 0,
    remaining_reserved_balance NUMERIC NOT NULL DEFAULT 0,
    remaining_reserved_shares NUMERIC NOT NULL DEFAULT 0,
    details TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (order_id, event_key)
);

COMMIT;
