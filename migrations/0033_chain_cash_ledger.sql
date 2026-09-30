BEGIN;

-- On-chain pUSD cash ledger (task 09-30-recon-balance-ledger).
--
-- Balance reconciliation used to compare execution_accounts.total_balance with
-- balanceOf(wallet) and explain differences case by case. Platform rewards and
-- self-funded deposits have no fill/redeem event and therefore became permanent
-- account-wide BALANCE_DRIFT issues. This migration records every confirmed
-- pUSD Transfer of an opted-in wallet so that:
--   * REWARD and DEPOSIT transfers are credited to the ledger exactly once;
--   * TRADE and REDEMPTION transfers are classified only (their own flows
--     already account for the cash);
--   * UNATTRIBUTED_IN/OUT transfers remain audit evidence, never ledger events.
-- Only accounts with a cursor row use the new comparison. Accounts without a
-- cursor keep the previous behaviour exactly.

CREATE TABLE execution_chain_cash_cursors (
    execution_account_id TEXT PRIMARY KEY
        REFERENCES execution_accounts(execution_account_id) ON DELETE RESTRICT,
    wallet_address TEXT NOT NULL,
    token_address TEXT NOT NULL,
    start_block BIGINT NOT NULL,
    start_balance NUMERIC NOT NULL,
    processed_block BIGINT NOT NULL,
    initialized_by TEXT NOT NULL,
    initialized_reason TEXT NOT NULL,
    evidence JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT execution_chain_cash_cursors_shape CHECK (
        wallet_address ~ '^0x[0-9a-f]{40}$'
        AND token_address ~ '^0x[0-9a-f]{40}$'
        AND start_block > 0
        AND start_balance >= 0
        AND processed_block >= start_block
        AND btrim(initialized_by) <> ''
        AND btrim(initialized_reason) <> ''
        AND jsonb_typeof(evidence) = 'object'
    )
);

-- The start snapshot is immutable audit evidence and processed_block only moves
-- forward. Deleting the whole row is the documented rollback to the old logic.
CREATE FUNCTION execution_chain_cash_cursor_guard()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.execution_account_id IS DISTINCT FROM OLD.execution_account_id
       OR NEW.wallet_address IS DISTINCT FROM OLD.wallet_address
       OR NEW.token_address IS DISTINCT FROM OLD.token_address
       OR NEW.start_block IS DISTINCT FROM OLD.start_block
       OR NEW.start_balance IS DISTINCT FROM OLD.start_balance
       OR NEW.initialized_by IS DISTINCT FROM OLD.initialized_by
       OR NEW.initialized_reason IS DISTINCT FROM OLD.initialized_reason
       OR NEW.evidence IS DISTINCT FROM OLD.evidence
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'chain cash cursor start snapshot is immutable';
    END IF;
    IF NEW.processed_block < OLD.processed_block THEN
        RAISE EXCEPTION 'chain cash cursor processed_block must not move backwards';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER execution_chain_cash_cursors_guard_trigger
BEFORE UPDATE ON execution_chain_cash_cursors
FOR EACH ROW EXECUTE FUNCTION execution_chain_cash_cursor_guard();

-- One row per confirmed pUSD Transfer log that touches an opted-in wallet. The
-- account is part of the key: a transfer between two managed wallets is an OUT
-- row for one account and an IN row for the other.
CREATE TABLE execution_chain_cash_transfers (
    execution_account_id TEXT NOT NULL
        REFERENCES execution_accounts(execution_account_id) ON DELETE RESTRICT,
    transaction_hash TEXT NOT NULL,
    log_index BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash TEXT NOT NULL,
    direction TEXT NOT NULL,
    counterparty TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    classification TEXT NOT NULL,
    account_event_id TEXT UNIQUE
        REFERENCES execution_account_events(account_event_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    observed_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT execution_chain_cash_transfers_pkey
        PRIMARY KEY (execution_account_id, transaction_hash, log_index),
    CONSTRAINT execution_chain_cash_transfers_shape CHECK (
        transaction_hash ~ '^0x[0-9a-f]{64}$'
        AND block_hash ~ '^0x[0-9a-f]{64}$'
        AND counterparty ~ '^0x[0-9a-f]{40}$'
        AND log_index >= 0
        AND block_number > 0
        AND amount > 0
        AND scale(amount) <= 6
        AND direction IN ('IN', 'OUT')
        AND classification IN (
            'TRADE', 'REDEMPTION', 'REWARD', 'DEPOSIT',
            'UNATTRIBUTED_IN', 'UNATTRIBUTED_OUT'
        )
    ),
    CONSTRAINT execution_chain_cash_transfers_direction_shape CHECK (
        classification = 'TRADE'
        OR (classification IN ('REDEMPTION', 'REWARD', 'DEPOSIT', 'UNATTRIBUTED_IN') AND direction = 'IN')
        OR (classification = 'UNATTRIBUTED_OUT' AND direction = 'OUT')
    ),
    CONSTRAINT execution_chain_cash_transfers_credit_shape CHECK (
        account_event_id IS NULL
        OR (classification IN ('REWARD', 'DEPOSIT') AND direction = 'IN')
    )
);

CREATE INDEX execution_chain_cash_transfers_account_block_idx
    ON execution_chain_cash_transfers (execution_account_id, block_number, log_index);

-- Append-only. The only permitted updates are:
--   * crediting a REWARD/DEPOSIT row (account_event_id NULL -> value);
--   * re-attributing an uncredited UNATTRIBUTED_IN row to REDEMPTION once the
--     account's own redemption transaction hash has been recorded (the payout
--     can reach finality before autoredeem stores the relayer's hash).
CREATE FUNCTION execution_chain_cash_transfer_guard()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'chain cash transfers are append-only';
    END IF;
    IF NEW.execution_account_id IS DISTINCT FROM OLD.execution_account_id
       OR NEW.transaction_hash IS DISTINCT FROM OLD.transaction_hash
       OR NEW.log_index IS DISTINCT FROM OLD.log_index
       OR NEW.block_number IS DISTINCT FROM OLD.block_number
       OR NEW.block_hash IS DISTINCT FROM OLD.block_hash
       OR NEW.direction IS DISTINCT FROM OLD.direction
       OR NEW.counterparty IS DISTINCT FROM OLD.counterparty
       OR NEW.amount IS DISTINCT FROM OLD.amount
       OR NEW.observed_at IS DISTINCT FROM OLD.observed_at THEN
        RAISE EXCEPTION 'chain cash transfer evidence is immutable';
    END IF;
    IF NEW.account_event_id IS DISTINCT FROM OLD.account_event_id
       AND (OLD.account_event_id IS NOT NULL OR NEW.account_event_id IS NULL) THEN
        RAISE EXCEPTION 'chain cash transfer account event may only be set once';
    END IF;
    IF NEW.classification IS DISTINCT FROM OLD.classification
       AND NOT (
           OLD.classification = 'UNATTRIBUTED_IN'
           AND NEW.classification = 'REDEMPTION'
           AND OLD.account_event_id IS NULL
           AND NEW.account_event_id IS NULL
       ) THEN
        RAISE EXCEPTION 'chain cash transfer classification is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER execution_chain_cash_transfers_guard_trigger
BEFORE UPDATE OR DELETE ON execution_chain_cash_transfers
FOR EACH ROW EXECUTE FUNCTION execution_chain_cash_transfer_guard();

-- A credited transfer must point at the exact account event it produced.
CREATE FUNCTION execution_validate_chain_cash_credit()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    event_row execution_account_events%ROWTYPE;
BEGIN
    IF NEW.account_event_id IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT * INTO event_row FROM execution_account_events
     WHERE account_event_id = NEW.account_event_id;
    IF NOT FOUND
       OR event_row.execution_account_id <> NEW.execution_account_id
       OR event_row.event_type <> ('CHAIN_CASH_' || NEW.classification)
       OR event_row.order_id <> '' OR event_row.fill_key <> ''
       OR event_row.total_balance_delta <> NEW.amount
       OR event_row.available_balance_delta <> NEW.amount
       OR event_row.reserved_balance_delta <> 0 THEN
        RAISE EXCEPTION 'chain cash credit account event does not match the transfer exactly';
    END IF;
    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER execution_chain_cash_transfers_credit_trigger
AFTER INSERT OR UPDATE ON execution_chain_cash_transfers
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION execution_validate_chain_cash_credit();

-- Balance issues keep one OPEN row per account/type/source; observed_at is the
-- latest observation, first_observed_at the first discovery and never changes.
ALTER TABLE reconciliation_issues ADD COLUMN first_observed_at TIMESTAMPTZ;

UPDATE reconciliation_issues
SET first_observed_at = observed_at
WHERE first_observed_at IS NULL;

ALTER TABLE reconciliation_issues ALTER COLUMN first_observed_at SET NOT NULL;

-- Older releases (still running while this migration is applied) do not know
-- the column. Fill it on insert and keep it immutable on every update.
CREATE FUNCTION reconciliation_issues_first_observed_guard()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        NEW.first_observed_at := COALESCE(NEW.first_observed_at, NEW.observed_at);
    ELSE
        NEW.first_observed_at := OLD.first_observed_at;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER reconciliation_issues_first_observed_at_trigger
BEFORE INSERT OR UPDATE ON reconciliation_issues
FOR EACH ROW EXECUTE FUNCTION reconciliation_issues_first_observed_guard();

-- Each unattributed transfer has exactly one audit issue (any status), found by
-- the transfer reference stored in venue_trade_id.
CREATE INDEX reconciliation_issues_chain_cash_transfer_idx
    ON reconciliation_issues (execution_account_id, venue_trade_id)
    WHERE issue_type IN ('UNATTRIBUTED_CASH_IN', 'UNATTRIBUTED_CASH_OUT');

COMMIT;
