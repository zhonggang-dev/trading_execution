BEGIN;

-- Kalshi settles by itself: the exchange credits the payout to cash and removes
-- the position. The local ledger has to follow with the exchange's own
-- settlement record, so one row here is one account/market whose settlement was
-- either applied to the ledger (APPLIED) or held for a person (MANUAL_REVIEW).
-- A row exists at most once per account/market, which makes applying idempotent.
CREATE TABLE kalshi_settlements (
    execution_account_id TEXT NOT NULL REFERENCES execution_accounts(execution_account_id),
    ticker TEXT NOT NULL CHECK (ticker <> ''),
    status TEXT NOT NULL CHECK (status IN ('APPLIED','MANUAL_REVIEW')),
    market_result TEXT NOT NULL,
    exchange_yes_count NUMERIC NOT NULL CHECK (exchange_yes_count >= 0),
    exchange_no_count NUMERIC NOT NULL CHECK (exchange_no_count >= 0),
    exchange_revenue NUMERIC NOT NULL CHECK (exchange_revenue >= 0),
    exchange_payout_per_yes NUMERIC,
    exchange_fee_cost NUMERIC,
    local_yes_shares NUMERIC NOT NULL CHECK (local_yes_shares >= 0),
    local_no_shares NUMERIC NOT NULL CHECK (local_no_shares >= 0),
    local_cost_basis NUMERIC NOT NULL CHECK (local_cost_basis >= 0),
    realized_pnl NUMERIC,
    review_reason TEXT NOT NULL DEFAULT '',
    settled_at TIMESTAMPTZ,
    applied_at TIMESTAMPTZ,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    exchange_record JSONB NOT NULL,
    PRIMARY KEY (execution_account_id, ticker),
    CONSTRAINT kalshi_settlements_shape CHECK (
        (status = 'APPLIED' AND applied_at IS NOT NULL AND realized_pnl IS NOT NULL AND review_reason = '')
        OR (status = 'MANUAL_REVIEW' AND applied_at IS NULL AND realized_pnl IS NULL AND review_reason <> '')
    )
);

-- The share of an applied settlement that each source lot received.
CREATE TABLE kalshi_settlement_lots (
    lot_id TEXT PRIMARY KEY REFERENCES position_lots(lot_id) ON DELETE RESTRICT,
    execution_account_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    token_id TEXT NOT NULL,
    settled_shares NUMERIC NOT NULL CHECK (settled_shares > 0),
    allocated_cost NUMERIC NOT NULL CHECK (allocated_cost >= 0),
    allocated_payout NUMERIC NOT NULL CHECK (allocated_payout >= 0),
    realized_pnl NUMERIC NOT NULL,
    settled_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT kalshi_settlement_lots_parent_fk
        FOREIGN KEY (execution_account_id, ticker)
        REFERENCES kalshi_settlements(execution_account_id, ticker)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX kalshi_settlement_lots_market_idx
    ON kalshi_settlement_lots (execution_account_id, ticker);

-- Running comparison of the local cash ledger with the exchange balance. A row
-- is OPEN while the two differ by more than the configured threshold and turns
-- RESOLVED when they agree again. The ledger is never overwritten from here.
CREATE TABLE kalshi_balance_drift (
    execution_account_id TEXT PRIMARY KEY REFERENCES execution_accounts(execution_account_id),
    status TEXT NOT NULL CHECK (status IN ('OPEN','RESOLVED')),
    local_balance NUMERIC NOT NULL,
    exchange_balance NUMERIC NOT NULL,
    difference NUMERIC NOT NULL,
    observations BIGINT NOT NULL CHECK (observations > 0),
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    CONSTRAINT kalshi_balance_drift_resolved_shape CHECK (
        (status = 'OPEN' AND resolved_at IS NULL) OR (status = 'RESOLVED' AND resolved_at IS NOT NULL)
    )
);

COMMIT;
