BEGIN;

-- A binary losing outcome settles at zero. Active positions must still use a
-- strictly positive mark, while a redeemed CLOSED position preserves its
-- authoritative zero settlement price in mark_price and immutable events.
ALTER TABLE execution_positions
    DROP CONSTRAINT execution_positions_mark_price_check,
    ADD CONSTRAINT execution_positions_mark_price_lifecycle_check CHECK (
        mark_price IS NULL
        OR mark_price > 0
        OR (mark_price = 0 AND lifecycle_status = 'CLOSED')
    );

COMMIT;
