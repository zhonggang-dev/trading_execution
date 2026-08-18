BEGIN;

-- Strategy exits are addressed to the exact opening lot. Market identity is
-- copied onto the immutable lot at BUY-fill time so Python can evaluate exits
-- even when that token has no currently effective prediction.
ALTER TABLE position_lots
    ADD COLUMN condition_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN outcome_index SMALLINT CHECK (outcome_index IN (0, 1)),
    ADD COLUMN outcome_name TEXT NOT NULL DEFAULT '',
    ADD COLUMN neg_risk BOOLEAN;

-- Best-effort backfill for rows created before this migration. Production
-- rollout must fail closed on any remaining blank identity before enabling
-- lot-addressed SELL decisions.
UPDATE position_lots AS lot
SET condition_id = position.condition_id,
    outcome_index = position.outcome_index,
    outcome_name = position.outcome_name
FROM execution_positions AS position
WHERE position.execution_account_id = lot.execution_account_id
  AND position.token_id = lot.token_id;

-- neg_risk was not stored by the old ledger. It must be backfilled from the
-- Market Universe before old lots can be exposed to Python or sold. New BUY
-- fills persist it directly from the validated order intent.

CREATE INDEX position_lots_strategy_open_idx
    ON position_lots (execution_account_id, model_id, strategy_id, opened_at, lot_id)
    WHERE status = 'OPEN';

COMMIT;
