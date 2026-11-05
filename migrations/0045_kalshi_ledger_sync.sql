BEGIN;

-- Running Kalshi cash sync, read-only position check, and settlement review.
-- Only new tables: a binary that does not know them keeps working unchanged.
-- Grants are applied separately at release time (see the task's implement.md).

-- One row per Kalshi account: the last cash sync and position check, why the
-- last ones were skipped and how often, and the drift observation waiting for
-- its confirming repeat. Nothing here gates trading.
CREATE TABLE kalshi_ledger_checks (
    execution_account_id TEXT PRIMARY KEY REFERENCES execution_accounts(execution_account_id),
    balance_checked_at TIMESTAMPTZ,
    balance_outcome TEXT NOT NULL DEFAULT 'NEVER' CHECK (balance_outcome IN
        ('NEVER','IN_SYNC','PENDING_CONFIRMATION','SYNCED','SKIPPED')),
    balance_skip_reason TEXT NOT NULL DEFAULT '',
    balance_consecutive_skips BIGINT NOT NULL DEFAULT 0 CHECK (balance_consecutive_skips >= 0),
    balance_total_skips BIGINT NOT NULL DEFAULT 0 CHECK (balance_total_skips >= 0),
    balance_skipping_since TIMESTAMPTZ,
    pending_local_balance NUMERIC,
    pending_exchange_balance NUMERIC,
    pending_account_version BIGINT,
    pending_first_seen_at TIMESTAMPTZ,
    last_synced_at TIMESTAMPTZ,
    last_sync_event_id TEXT NOT NULL DEFAULT '',
    positions_checked_at TIMESTAMPTZ,
    positions_outcome TEXT NOT NULL DEFAULT 'NEVER' CHECK (positions_outcome IN
        ('NEVER','MATCHED','DRIFTED','SKIPPED','ERROR')),
    positions_skip_reason TEXT NOT NULL DEFAULT '',
    positions_open_drifts INTEGER NOT NULL DEFAULT 0 CHECK (positions_open_drifts >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT kalshi_ledger_checks_pending_shape CHECK (
        (pending_local_balance IS NULL AND pending_exchange_balance IS NULL
         AND pending_account_version IS NULL AND pending_first_seen_at IS NULL)
     OR (pending_local_balance IS NOT NULL AND pending_exchange_balance IS NOT NULL
         AND pending_account_version IS NOT NULL AND pending_first_seen_at IS NOT NULL))
);

-- One row per running cash sync, keyed by its KALSHI_BALANCE_SYNC account
-- event, with the two observations that justified it. Append-only.
CREATE TABLE kalshi_balance_syncs (
    account_event_id TEXT PRIMARY KEY
        REFERENCES execution_account_events(account_event_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    execution_account_id TEXT NOT NULL REFERENCES execution_accounts(execution_account_id),
    local_balance_before NUMERIC NOT NULL,
    exchange_balance NUMERIC NOT NULL CHECK (exchange_balance >= 0),
    difference NUMERIC NOT NULL CHECK (difference <> 0),
    account_version_before BIGINT NOT NULL,
    first_observed_at TIMESTAMPTZ NOT NULL,
    confirmed_at TIMESTAMPTZ NOT NULL,
    observations INTEGER NOT NULL CHECK (observations >= 2),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT kalshi_balance_syncs_math CHECK (difference = exchange_balance - local_balance_before),
    CONSTRAINT kalshi_balance_syncs_order CHECK (first_observed_at <= confirmed_at)
);
CREATE INDEX kalshi_balance_syncs_account_idx ON kalshi_balance_syncs (execution_account_id, confirmed_at);

-- Per-market difference between the local net position and the exchange's,
-- shaped like kalshi_balance_drift. Recorded only; the ledger is never changed
-- from here.
CREATE TABLE kalshi_position_drift (
    execution_account_id TEXT NOT NULL REFERENCES execution_accounts(execution_account_id),
    ticker TEXT NOT NULL CHECK (ticker <> '' AND strpos(ticker, ':') = 0),
    status TEXT NOT NULL CHECK (status IN ('OPEN','RESOLVED')),
    drift_kind TEXT NOT NULL CHECK (drift_kind IN ('NET_MISMATCH','LOCAL_BOTH_SIDES')),
    local_yes_shares NUMERIC NOT NULL CHECK (local_yes_shares >= 0),
    local_no_shares NUMERIC NOT NULL CHECK (local_no_shares >= 0),
    exchange_position NUMERIC NOT NULL,
    exchange_record JSONB,
    observations BIGINT NOT NULL CHECK (observations > 0),
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    PRIMARY KEY (execution_account_id, ticker),
    CONSTRAINT kalshi_position_drift_resolved_shape CHECK (
        (status = 'OPEN' AND resolved_at IS NULL) OR (status = 'RESOLVED' AND resolved_at IS NOT NULL))
);

-- One row each time the settlement review tool turns a MANUAL_REVIEW
-- settlement into APPLIED, keeping the original review reason. Append-only.
CREATE TABLE kalshi_settlement_reviews (
    execution_account_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    previous_reason TEXT NOT NULL CHECK (previous_reason <> ''),
    previous_exchange_record JSONB NOT NULL,
    exchange_record JSONB NOT NULL,
    evidence JSONB NOT NULL,
    cash_difference_after NUMERIC NOT NULL,
    expected_cash_diff NUMERIC NOT NULL,
    cash_tolerance NUMERIC NOT NULL CHECK (cash_tolerance >= 0 AND cash_tolerance <= 0.01),
    actor TEXT NOT NULL CHECK (actor <> ''),
    reason TEXT NOT NULL CHECK (reason <> ''),
    fingerprint TEXT NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
    applied_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (execution_account_id, ticker),
    CONSTRAINT kalshi_settlement_reviews_parent_fk FOREIGN KEY (execution_account_id, ticker)
        REFERENCES kalshi_settlements(execution_account_id, ticker) ON DELETE RESTRICT
);

CREATE FUNCTION kalshi_reject_audit_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END;
$$;

-- Statement triggers also reject TRUNCATE, which row triggers would not see.
CREATE TRIGGER kalshi_balance_syncs_append_only
    BEFORE UPDATE OR DELETE OR TRUNCATE ON kalshi_balance_syncs
    FOR EACH STATEMENT EXECUTE FUNCTION kalshi_reject_audit_change();
CREATE TRIGGER kalshi_settlement_reviews_append_only
    BEFORE UPDATE OR DELETE OR TRUNCATE ON kalshi_settlement_reviews
    FOR EACH STATEMENT EXECUTE FUNCTION kalshi_reject_audit_change();

COMMIT;
