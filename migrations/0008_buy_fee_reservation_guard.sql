BEGIN;

-- A BUY reserve_unit_price is the worst executable price plus the
-- execution-owned maximum fee buffer per share. Keep the initial amount tied
-- to that unit value so every remaining-share calculation uses the same cap.
-- NOT VALID avoids making deployment depend on unreviewed historical rows;
-- PostgreSQL still enforces both constraints for every new or updated row.
ALTER TABLE asset_reservations
    ADD CONSTRAINT asset_reservations_buy_initial_reserve_identity
    CHECK (
        side <> 'BUY'
        OR initial_reserved_balance = requested_shares * reserve_unit_price
    ) NOT VALID;

-- FillLedger records BUY cash as gross notional plus platform and builder
-- fees. This invariant makes an over-cap fill fail in the same transaction,
-- before it can consume unreserved account balance. Historical rows should be
-- reconciled and the constraint VALIDATEd separately after deployment.
ALTER TABLE asset_reservations
    ADD CONSTRAINT asset_reservations_buy_spend_within_reserve
    CHECK (
        side <> 'BUY'
        OR settled_notional + settled_fees + remaining_reserved_balance
           <= initial_reserved_balance
    ) NOT VALID;

COMMIT;
