BEGIN;

-- Capture immutable binary resolution evidence before any chain mutation.
ALTER TABLE execution_positions
    ADD COLUMN settlement_price NUMERIC,
    ADD COLUMN settlement_source TEXT NOT NULL DEFAULT '',
    ADD COLUMN settled_at TIMESTAMPTZ,
    ADD CONSTRAINT execution_positions_settlement_price_check CHECK (
        settlement_price IS NULL OR settlement_price IN (0, 1)
    ),
    ADD CONSTRAINT execution_positions_settlement_evidence_shape CHECK (
        (settlement_price IS NULL AND settlement_source = '' AND settled_at IS NULL)
        OR
        (settlement_price IS NOT NULL AND settlement_source <> '' AND settled_at IS NOT NULL)
    );

ALTER TABLE position_events
    DROP CONSTRAINT position_events_event_type_check,
    ADD CONSTRAINT position_events_event_type_check CHECK (
        event_type IN ('BOUGHT','ADOPTED','SOLD','MARKED','SETTLED','REDEEMED')
    );

-- One row owns one exact account/condition workflow. The *_SUBMITTING states
-- are written before the first network byte leaves the process. Ambiguous
-- outcomes are recovered from the relayer/Data API and never blindly retried.
CREATE TABLE polymarket_redemptions (
    execution_account_id TEXT NOT NULL
        REFERENCES execution_accounts(execution_account_id) ON DELETE RESTRICT,
    condition_id TEXT NOT NULL,
    wallet_address TEXT NOT NULL,
    neg_risk BOOLEAN NOT NULL,
    status TEXT NOT NULL DEFAULT 'READY' CHECK (status IN (
        'READY','APPROVAL_SUBMITTING','APPROVAL_SUBMITTED',
        'REDEEM_SUBMITTING','REDEEM_SUBMITTED','CONFIRMED','APPLIED','MANUAL_REVIEW'
    )),
    submission_provider TEXT NOT NULL DEFAULT '',
    submission_reference TEXT NOT NULL DEFAULT '',
    transaction_hash TEXT NOT NULL DEFAULT '',
    event_type TEXT NOT NULL DEFAULT '',
    payout_base_units NUMERIC(78,0),
    receipt_block_number BIGINT,
    receipt_block_hash TEXT NOT NULL DEFAULT '',
    confirmations BIGINT NOT NULL DEFAULT 0 CHECK (confirmations >= 0),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT NOT NULL DEFAULT '',
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    submitting_at TIMESTAMPTZ,
    submitted_at TIMESTAMPTZ,
    confirmed_at TIMESTAMPTZ,
    applied_at TIMESTAMPTZ,
    PRIMARY KEY (execution_account_id, condition_id),
    CONSTRAINT polymarket_redemptions_identity_nonempty CHECK (
        execution_account_id <> '' AND condition_id ~ '^0x[0-9a-f]{64}$'
        AND wallet_address ~ '^0x[0-9a-f]{40}$'
    ),
    CONSTRAINT polymarket_redemptions_state_shape CHECK (
        (status = 'READY'
            AND submission_provider = '' AND submission_reference = ''
            AND transaction_hash = '' AND submitting_at IS NULL AND submitted_at IS NULL
            AND confirmed_at IS NULL AND applied_at IS NULL)
        OR
        (status IN ('APPROVAL_SUBMITTING','REDEEM_SUBMITTING')
            AND submission_provider = '' AND submission_reference = ''
            AND transaction_hash = '' AND submitting_at IS NOT NULL AND submitted_at IS NULL
            AND confirmed_at IS NULL AND applied_at IS NULL)
        OR
        (status IN ('APPROVAL_SUBMITTED','REDEEM_SUBMITTED')
            AND submission_provider <> '' AND submission_reference <> ''
            AND submitting_at IS NOT NULL AND submitted_at IS NOT NULL
            AND confirmed_at IS NULL AND applied_at IS NULL)
        OR
        (status = 'CONFIRMED'
            AND transaction_hash ~ '^0x[0-9a-f]{64}$' AND event_type = 'POSITIONS_REDEEMED'
            AND payout_base_units IS NOT NULL AND payout_base_units >= 0
            AND receipt_block_number IS NOT NULL AND receipt_block_number > 0
            AND receipt_block_hash ~ '^0x[0-9a-f]{64}$'
            AND confirmed_at IS NOT NULL AND applied_at IS NULL)
        OR
        (status = 'APPLIED'
            AND transaction_hash ~ '^0x[0-9a-f]{64}$' AND event_type = 'POSITIONS_REDEEMED'
            AND payout_base_units IS NOT NULL AND payout_base_units >= 0
            AND receipt_block_number IS NOT NULL AND receipt_block_number > 0
            AND receipt_block_hash ~ '^0x[0-9a-f]{64}$'
            AND confirmed_at IS NOT NULL AND applied_at IS NOT NULL)
        OR
        (status = 'MANUAL_REVIEW' AND last_error <> '')
    )
);

CREATE UNIQUE INDEX polymarket_redemptions_tx_condition_uidx
    ON polymarket_redemptions (transaction_hash, execution_account_id, condition_id)
    WHERE transaction_hash <> '';

CREATE INDEX polymarket_redemptions_due_idx
    ON polymarket_redemptions (next_attempt_at, created_at)
    WHERE status NOT IN ('APPLIED','MANUAL_REVIEW');

CREATE TABLE position_lot_redemptions (
    redemption_id TEXT PRIMARY KEY,
    execution_account_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    transaction_hash TEXT NOT NULL,
    lot_id TEXT NOT NULL UNIQUE REFERENCES position_lots(lot_id) ON DELETE RESTRICT,
    redeemed_shares NUMERIC NOT NULL CHECK (redeemed_shares > 0),
    allocated_cost NUMERIC NOT NULL CHECK (allocated_cost >= 0),
    allocated_payout NUMERIC NOT NULL CHECK (allocated_payout >= 0),
    realized_pnl NUMERIC NOT NULL,
    redeemed_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT position_lot_redemptions_parent_fk
        FOREIGN KEY (execution_account_id, condition_id)
        REFERENCES polymarket_redemptions(execution_account_id, condition_id)
        ON DELETE RESTRICT
);

COMMIT;
