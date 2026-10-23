BEGIN;

-- The cutover baseline (0014) is a single sealed set per account, and a
-- disposition (0016) can only consume it through an identified venue trade or
-- an adoption. Two real situations fit neither:
--
--   * ADD_RESIDUAL: shares that already sat in the wallet before the ledger was
--     rebased (dust of an old account) were not captured by the baseline. As soon as
--     the service buys the same token, the chain holds ledger + residual and
--     reconciliation reports a POSITION_DRIFT it can never close.
--   * RETIRE_CONSUMED: an unmanaged baseline position was burned on-chain (for
--     example a zero-payout redemption of a lost outcome), so the baseline
--     claims shares that no longer exist.
--
-- An amendment records either fact with its chain evidence. Rows are
-- append-only and never touch the baseline, positions, lots, fills or cash;
-- they only change the ownership equation reconciliation evaluates:
--   remote shares = unmanaged baseline remaining + managed ledger shares
-- where baseline remaining = item - dispositions + ADD_RESIDUAL - RETIRE_CONSUMED.
CREATE TABLE execution_external_position_baseline_amendments (
    amendment_id TEXT NOT NULL,
    execution_account_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    outcome_index INTEGER,
    outcome_name TEXT NOT NULL,
    neg_risk BOOLEAN NOT NULL,
    kind TEXT NOT NULL,
    shares NUMERIC NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    evidence JSONB NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT execution_external_position_baseline_amendments_pkey PRIMARY KEY (amendment_id),
    CONSTRAINT execution_external_position_baseline_amendments_account_fk
        FOREIGN KEY (execution_account_id)
        REFERENCES execution_accounts(execution_account_id) ON DELETE RESTRICT,
    CONSTRAINT execution_external_position_baseline_amendments_shape CHECK (
        btrim(amendment_id) <> '' AND btrim(token_id) <> '' AND btrim(condition_id) <> ''
        AND btrim(outcome_name) <> '' AND btrim(actor) <> '' AND btrim(reason) <> ''
        AND kind IN ('ADD_RESIDUAL', 'RETIRE_CONSUMED')
        AND shares > 0
        AND (outcome_index IS NULL OR outcome_index >= 0)
    ),
    CONSTRAINT execution_external_position_baseline_amendments_evidence_object CHECK (
        jsonb_typeof(evidence) = 'object' AND evidence <> '{}'::jsonb
    )
);

CREATE INDEX execution_external_position_baseline_amendments_token_idx
    ON execution_external_position_baseline_amendments (execution_account_id, token_id, created_at, amendment_id);

-- Remaining unmanaged shares per token, the single definition shared by the
-- reconciliation read model and the redemption safety checks. delta > 0 in a
-- disposition means shares were consumed (shares_before = delta + after).
CREATE VIEW execution_external_position_baseline_remaining AS
SELECT movement.execution_account_id,
       movement.token_id,
       movement.condition_id,
       SUM(movement.shares) AS shares
FROM (
    SELECT item.execution_account_id, item.token_id, item.condition_id, item.shares
    FROM execution_external_position_baseline_items item
    UNION ALL
    SELECT disposition.execution_account_id, disposition.token_id, disposition.condition_id,
           -disposition.shares_delta
    FROM execution_external_position_dispositions disposition
    WHERE disposition.baseline_id IS NOT NULL
      AND disposition.disposition_kind IN ('EXTERNAL_SELL', 'ADOPTION')
    UNION ALL
    SELECT amendment.execution_account_id, amendment.token_id, amendment.condition_id,
           CASE amendment.kind WHEN 'ADD_RESIDUAL' THEN amendment.shares ELSE -amendment.shares END
    FROM execution_external_position_baseline_amendments amendment
) movement
GROUP BY movement.execution_account_id, movement.token_id, movement.condition_id
HAVING SUM(movement.shares) > 0;

CREATE FUNCTION execution_validate_external_position_baseline_amendment()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    known RECORD;
    remaining NUMERIC;
BEGIN
    -- Serialize amendments of one token so the exactness checks below cannot
    -- be raced by a concurrent insert.
    PERFORM pg_advisory_xact_lock(
        hashtextextended('baseline-amendment:' || NEW.execution_account_id || ':' || NEW.token_id, 0));

    IF NOT (NEW.evidence ? 'block_number') OR NOT (NEW.evidence ? 'chain_shares')
       OR NOT (NEW.evidence ? 'wallet_address') THEN
        RAISE EXCEPTION 'baseline amendment evidence must record wallet_address, block_number and chain_shares';
    END IF;

    -- Identity must agree with every earlier record of the token.
    SELECT item.condition_id, item.outcome_index, item.outcome_name, item.neg_risk
      INTO known
      FROM execution_external_position_baseline_items item
     WHERE item.execution_account_id = NEW.execution_account_id AND item.token_id = NEW.token_id;
    IF FOUND AND (known.condition_id <> NEW.condition_id
                  OR known.outcome_index IS DISTINCT FROM NEW.outcome_index
                  OR known.outcome_name <> NEW.outcome_name
                  OR known.neg_risk <> NEW.neg_risk) THEN
        RAISE EXCEPTION 'baseline amendment identity does not match the immutable baseline item';
    END IF;
    SELECT prior.condition_id, prior.outcome_index, prior.outcome_name, prior.neg_risk
      INTO known
      FROM execution_external_position_baseline_amendments prior
     WHERE prior.execution_account_id = NEW.execution_account_id AND prior.token_id = NEW.token_id
     ORDER BY prior.created_at DESC, prior.amendment_id DESC
     LIMIT 1;
    IF FOUND AND (known.condition_id <> NEW.condition_id
                  OR known.outcome_index IS DISTINCT FROM NEW.outcome_index
                  OR known.outcome_name <> NEW.outcome_name
                  OR known.neg_risk <> NEW.neg_risk) THEN
        RAISE EXCEPTION 'baseline amendment identity does not match an earlier amendment of the token';
    END IF;

    IF NEW.kind = 'RETIRE_CONSUMED' THEN
        SELECT COALESCE(SUM(current_remaining.shares), 0) INTO remaining
          FROM execution_external_position_baseline_remaining current_remaining
         WHERE current_remaining.execution_account_id = NEW.execution_account_id
           AND current_remaining.token_id = NEW.token_id;
        IF remaining <= 0 OR NEW.shares <> remaining THEN
            RAISE EXCEPTION 'a retirement must consume exactly the whole remaining unmanaged baseline';
        END IF;
        IF (NEW.evidence->>'chain_shares')::NUMERIC <> 0 THEN
            RAISE EXCEPTION 'a retirement requires evidence that the wallet no longer holds the token';
        END IF;
    ELSIF (NEW.evidence->>'chain_shares')::NUMERIC <= 0 THEN
        RAISE EXCEPTION 'a residual requires evidence that the wallet holds the token';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER baseline_amendments_validate_trigger
BEFORE INSERT ON execution_external_position_baseline_amendments
FOR EACH ROW
EXECUTE FUNCTION execution_validate_external_position_baseline_amendment();

CREATE FUNCTION execution_reject_external_position_baseline_amendment_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'external position baseline amendments are append-only';
END;
$$;

-- Statement triggers also reject TRUNCATE, which row triggers would not see.
CREATE TRIGGER baseline_amendments_append_only_trigger
BEFORE UPDATE OR DELETE OR TRUNCATE ON execution_external_position_baseline_amendments
FOR EACH STATEMENT
EXECUTE FUNCTION execution_reject_external_position_baseline_amendment_mutation();

COMMIT;
