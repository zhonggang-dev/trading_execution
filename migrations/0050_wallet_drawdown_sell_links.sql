BEGIN;
SET LOCAL search_path=public,pg_temp;

ALTER TABLE wallet_drawdown_order_operations
    ADD COLUMN opening_client_order_id TEXT REFERENCES wallet_drawdown_order_operations (client_order_id),
    ADD COLUMN target_lot_id TEXT REFERENCES position_lots (lot_id),
    ADD CONSTRAINT wallet_drawdown_sell_link_shape CHECK (
        (opening_client_order_id IS NULL AND target_lot_id IS NULL) OR
        (side = 'SELL' AND opening_client_order_id IS NOT NULL AND target_lot_id IS NOT NULL)
    );

CREATE INDEX wallet_drawdown_sell_lot_idx
    ON wallet_drawdown_order_operations (target_lot_id, attempt DESC)
    WHERE target_lot_id IS NOT NULL;

COMMIT;
