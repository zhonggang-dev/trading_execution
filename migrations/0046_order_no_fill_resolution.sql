BEGIN;

-- PM orders whose placement outcome is unknown (POST 5xx or timeout) and that
-- the venue never shows again stay UNKNOWN with frozen collateral. Two
-- additive objects let the service prove "the venue never accepted it":
--
--   * execution_order_attempts.venue_expires_at: the venue-enforced expiry a
--     SUBMIT attempt carried (short-lived GTD). It is written in the same
--     transaction that records SUBMITTING, before any POST bytes leave the
--     process. NULL means the placement had no venue expiry (GTC, every row
--     written before this migration, and every row written by an older
--     binary); such orders are never released automatically.
--   * execution_order_no_fill_resolutions: append-only audit of every
--     evaluation that found complete no-fill evidence. SHADOW rows are
--     observations only (nothing else changed); AUTO and OPERATOR rows are
--     written in the same transaction that cancels the order and releases its
--     reservation, at most one per order.
--
-- Both objects are invisible to older binaries, so the migration is applied
-- before the new binary and is never rolled back. Grants to the application
-- role are applied separately (SELECT, INSERT on the audit table).

ALTER TABLE execution_order_attempts ADD COLUMN venue_expires_at TIMESTAMPTZ;

ALTER TABLE execution_order_attempts
    ADD CONSTRAINT execution_order_attempts_venue_expiry_shape
    CHECK (venue_expires_at IS NULL OR kind = 'SUBMIT');

CREATE TABLE execution_order_no_fill_resolutions (
    resolution_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL REFERENCES execution_orders(order_id),
    execution_account_id TEXT NOT NULL REFERENCES execution_accounts(execution_account_id),
    mode TEXT NOT NULL CHECK (mode IN ('SHADOW', 'AUTO', 'OPERATOR')),
    actor TEXT NOT NULL CHECK (btrim(actor) <> ''),
    reason TEXT NOT NULL DEFAULT '',
    reason_code TEXT NOT NULL CHECK (btrim(reason_code) <> ''),
    order_revision BIGINT NOT NULL CHECK (order_revision >= 1),
    evidence JSONB NOT NULL CHECK (jsonb_typeof(evidence) = 'object'),
    evidence_sha256 TEXT NOT NULL CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
    released_balance NUMERIC NOT NULL DEFAULT 0 CHECK (released_balance >= 0),
    released_shares NUMERIC NOT NULL DEFAULT 0 CHECK (released_shares >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT no_fill_resolutions_shadow_releases_nothing CHECK (
        mode <> 'SHADOW' OR (released_balance = 0 AND released_shares = 0)
    ),
    CONSTRAINT no_fill_resolutions_operator_reason CHECK (
        mode <> 'OPERATOR' OR btrim(reason) <> ''
    )
);

-- At most one real release and one shadow observation per order.
CREATE UNIQUE INDEX no_fill_resolutions_release_uidx
    ON execution_order_no_fill_resolutions (order_id)
    WHERE mode IN ('AUTO', 'OPERATOR');

CREATE UNIQUE INDEX no_fill_resolutions_shadow_uidx
    ON execution_order_no_fill_resolutions (order_id)
    WHERE mode = 'SHADOW';

CREATE FUNCTION no_fill_resolutions_append_only()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'execution_order_no_fill_resolutions is append-only';
END;
$$;

CREATE TRIGGER no_fill_resolutions_append_only_trigger
BEFORE UPDATE OR DELETE OR TRUNCATE ON execution_order_no_fill_resolutions
FOR EACH STATEMENT EXECUTE FUNCTION no_fill_resolutions_append_only();

COMMIT;
