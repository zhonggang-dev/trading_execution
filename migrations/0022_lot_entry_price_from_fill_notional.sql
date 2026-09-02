-- 0022_lot_entry_price_from_fill_notional.sql
--
-- position_lots.average_entry_price used to copy the CLOB display price of the
-- opening fill. That price is a tick bucket (two decimals for a 0.01 tick) and
-- can differ from the settled ratio after price improvement. The ledger now
-- writes gross_notional / shares of the opening fill at full numeric precision.
--
-- Backfill every managed (non-adopted) lot that still carries a bucketed entry
-- price and is still economically live. Closed lots are historical records and
-- are intentionally left untouched; realized PnL never depended on this column.
UPDATE position_lots AS lot
SET average_entry_price = fill.gross_notional / fill.shares
FROM execution_fills AS fill
WHERE fill.fill_key = lot.opening_fill_key
  AND lot.external_adoption_id IS NULL
  AND lot.status IN ('OPEN', 'SETTLED_PENDING_REDEEM')
  AND fill.shares > 0
  AND fill.gross_notional > 0
  AND lot.average_entry_price <> fill.gross_notional / fill.shares;
