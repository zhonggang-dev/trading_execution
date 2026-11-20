BEGIN;
SET LOCAL search_path=public,pg_temp;

DO $$
DECLARE
    sample_fk TEXT;
BEGIN
    SELECT conname INTO STRICT sample_fk
    FROM pg_constraint
    WHERE conrelid = 'wallet_drawdown_order_operations'::regclass
      AND confrelid = 'wallet_drawdown_samples'::regclass
      AND contype = 'f';
    EXECUTE format('ALTER TABLE wallet_drawdown_order_operations DROP CONSTRAINT %I', sample_fk);
END;
$$;

ALTER TABLE wallet_drawdown_order_operations
    ADD COLUMN source_sell_client_order_id TEXT REFERENCES wallet_drawdown_order_operations (client_order_id),
    ADD COLUMN sample_account_id TEXT GENERATED ALWAYS AS (
        CASE WHEN source_sell_client_order_id IS NULL THEN execution_account_id ELSE 'main' END
    ) STORED,
    ADD CONSTRAINT wallet_drawdown_order_sample_fk
        FOREIGN KEY (sample_account_id, slot_at)
        REFERENCES wallet_drawdown_samples (execution_account_id, slot_at),
    ADD CONSTRAINT wallet_drawdown_follow_buy_shape CHECK (
        source_sell_client_order_id IS NULL OR (side = 'BUY' AND execution_account_id <> 'main')
    );

CREATE UNIQUE INDEX wallet_drawdown_one_follow_buy_per_sell
    ON wallet_drawdown_order_operations (source_sell_client_order_id)
    WHERE source_sell_client_order_id IS NOT NULL;

COMMIT;
