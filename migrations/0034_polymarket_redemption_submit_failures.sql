BEGIN;

-- Durable, append-only record of failed Polymarket redemption submit calls
-- (task 10-19-redeem-per-wallet-serial).
--
-- polymarket_redemptions.last_error is overwritten by every retry, so the raw
-- relayer rejection of a failed submit used to be lost within minutes. One row
-- here is one failed submit call; the number of rows for an account/condition
-- is the failure count that bounds automatic resubmission. Rows are never
-- updated or deleted.
CREATE TABLE polymarket_redemption_submit_failures (
    id BIGSERIAL PRIMARY KEY,
    execution_account_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    submission_kind TEXT NOT NULL CHECK (submission_kind IN ('APPROVAL','REDEEM')),
    outcome TEXT NOT NULL CHECK (outcome IN ('NOT_SUBMITTED','REJECTED','AMBIGUOUS')),
    http_status INTEGER CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
    error_text TEXT NOT NULL CHECK (error_text <> ''),
    attempt_started_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT polymarket_redemption_submit_failures_redemption_fk
        FOREIGN KEY (execution_account_id, condition_id)
        REFERENCES polymarket_redemptions(execution_account_id, condition_id)
        ON DELETE RESTRICT
);

CREATE INDEX polymarket_redemption_submit_failures_redemption_idx
    ON polymarket_redemption_submit_failures (execution_account_id, condition_id, id);

CREATE FUNCTION reject_polymarket_redemption_submit_failure_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'polymarket redemption submit failures are append-only';
END;
$$;

CREATE TRIGGER polymarket_redemption_submit_failures_append_only_trigger
BEFORE UPDATE OR DELETE ON polymarket_redemption_submit_failures
FOR EACH ROW EXECUTE FUNCTION reject_polymarket_redemption_submit_failure_mutation();

CREATE TRIGGER polymarket_redemption_submit_failures_truncate_guard_trigger
BEFORE TRUNCATE ON polymarket_redemption_submit_failures
FOR EACH STATEMENT EXECUTE FUNCTION reject_polymarket_redemption_submit_failure_mutation();

COMMIT;
