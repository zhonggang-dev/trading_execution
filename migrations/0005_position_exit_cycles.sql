BEGIN;

-- A SELL reservation belongs to one opening lot, not to the whole token.
-- NOT VALID avoids making deployment depend on legacy rows while still
-- enforcing the shape for all new writes immediately.
ALTER TABLE asset_reservations
    ADD COLUMN target_lot_id TEXT;

UPDATE asset_reservations AS reservation
SET target_lot_id = NULLIF(order_row.intent->>'target_lot_id', '')
FROM execution_orders AS order_row
WHERE reservation.order_id = order_row.order_id
  AND reservation.side = 'SELL';

ALTER TABLE asset_reservations
    ADD CONSTRAINT asset_reservations_target_lot_fk
        FOREIGN KEY (target_lot_id) REFERENCES position_lots(lot_id) NOT VALID,
    ADD CONSTRAINT asset_reservations_target_lot_shape CHECK (
        (side = 'BUY' AND target_lot_id IS NULL)
        OR (side = 'SELL' AND target_lot_id IS NOT NULL AND target_lot_id <> '')
    ) NOT VALID;

DROP INDEX asset_reservations_active_token_side_uidx;

CREATE UNIQUE INDEX asset_reservations_active_buy_token_uidx
    ON asset_reservations (execution_account_id, token_id)
    WHERE side = 'BUY'
      AND status IN ('ACTIVE', 'RECONCILIATION_REQUIRED');

CREATE UNIQUE INDEX asset_reservations_active_sell_lot_uidx
    ON asset_reservations (execution_account_id, target_lot_id)
    WHERE side = 'SELL'
      AND status IN ('ACTIVE', 'RECONCILIATION_REQUIRED');

CREATE UNIQUE INDEX asset_reservations_active_legacy_sell_token_uidx
    ON asset_reservations (execution_account_id, token_id)
    WHERE side = 'SELL' AND target_lot_id IS NULL
      AND status IN ('ACTIVE', 'RECONCILIATION_REQUIRED');

-- Input and output are stored as immutable JSON snapshots. A retry uses the
-- same cycle_id and is allowed to replay only these exact bytes/identities.
CREATE TABLE position_exit_runs (
    cycle_id TEXT PRIMARY KEY,
    input_id TEXT NOT NULL UNIQUE,
    decision_at TIMESTAMPTZ NOT NULL,
    model_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    execution_account_id TEXT NOT NULL,
    input_payload JSONB NOT NULL,
    output_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    decided_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT position_exit_runs_identity_nonempty CHECK (
        cycle_id <> '' AND input_id <> '' AND model_id <> ''
        AND strategy_id <> '' AND execution_account_id <> ''
    ),
    CONSTRAINT position_exit_runs_output_time CHECK (
        (output_payload IS NULL AND decided_at IS NULL)
        OR (output_payload IS NOT NULL AND decided_at IS NOT NULL)
    )
);

CREATE INDEX position_exit_runs_account_time_idx
    ON position_exit_runs (execution_account_id, decision_at DESC);

COMMIT;
