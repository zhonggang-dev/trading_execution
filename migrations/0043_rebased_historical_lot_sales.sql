BEGIN;
SET LOCAL search_path=public,pg_temp;

-- Maintenance owner only. The long-running application must never own or
-- receive INSERT on this audit relation, even if it can CREATE in public.
DO $$ BEGIN
 IF current_user='trading_execution_app' THEN RAISE EXCEPTION 'historical sale migration requires the maintenance owner'; END IF;
END $$;

CREATE TABLE historical_rebased_lot_sales (
 sale_id TEXT PRIMARY KEY CHECK(sale_id ~ '^[0-9a-f]{64}$'),
 manifest_sha256 TEXT NOT NULL CHECK(manifest_sha256 ~ '^[0-9a-f]{64}$'),
 execution_account_id TEXT NOT NULL REFERENCES execution_accounts,
 lot_id TEXT NOT NULL UNIQUE REFERENCES position_lots,
 market_id TEXT NOT NULL, condition_id TEXT NOT NULL, token_id TEXT NOT NULL,
 transaction_hash TEXT NOT NULL CHECK(transaction_hash ~ '^0x[0-9a-f]{64}$'),
 order_hash TEXT NOT NULL CHECK(order_hash ~ '^0x[0-9a-f]{64}$'),
 chain_id BIGINT NOT NULL CHECK(chain_id=137),
 block_number BIGINT NOT NULL CHECK(block_number>0),
 block_hash TEXT NOT NULL CHECK(block_hash ~ '^0x[0-9a-f]{64}$'),
 fill_log_index BIGINT NOT NULL CHECK(fill_log_index>=0),
 trade_shares NUMERIC NOT NULL CHECK(trade_shares>0),
 trade_gross_proceeds NUMERIC NOT NULL CHECK(trade_gross_proceeds>=0),
 trade_fee NUMERIC NOT NULL CHECK(trade_fee>=0 AND trade_fee<=trade_gross_proceeds),
 allocated_shares NUMERIC NOT NULL CHECK(allocated_shares>0 AND allocated_shares<=trade_shares),
 allocated_gross_proceeds NUMERIC NOT NULL CHECK(allocated_gross_proceeds>=0),
 allocated_fee NUMERIC NOT NULL CHECK(allocated_fee>=0 AND allocated_fee<=allocated_gross_proceeds),
 allocated_principal NUMERIC NOT NULL CHECK(allocated_principal>0),
 sold_at TIMESTAMPTZ NOT NULL,
 rebase_account_event_id TEXT NOT NULL REFERENCES execution_account_events,
 cash_effect NUMERIC NOT NULL DEFAULT 0 CHECK(cash_effect=0),
 evidence JSONB NOT NULL CHECK(jsonb_typeof(evidence)='object'),
 actor TEXT NOT NULL CHECK(btrim(actor)<>''), reason TEXT NOT NULL CHECK(btrim(reason)<>''),
 recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(execution_account_id,transaction_hash,fill_log_index),
 CHECK(allocated_gross_proceeds*trade_shares=trade_gross_proceeds*allocated_shares),
 CHECK(allocated_fee*trade_shares=trade_fee*allocated_shares)
);

CREATE FUNCTION check_historical_rebased_lot_sale() RETURNS trigger LANGUAGE plpgsql SET search_path=public,pg_temp AS $$
DECLARE lot position_lots; rebase execution_account_events;
BEGIN
 SELECT * INTO STRICT lot FROM position_lots WHERE lot_id=NEW.lot_id FOR UPDATE;
 SELECT * INTO STRICT rebase FROM execution_account_events WHERE account_event_id=NEW.rebase_account_event_id;
 -- This narrowly covers a previously adopted lot already closed by an old
 -- cash rebase. It never closes a live lot or infers an unrecorded payout.
 IF lot.execution_account_id<>NEW.execution_account_id OR lot.market_id<>NEW.market_id
 OR lot.condition_id<>NEW.condition_id OR lot.token_id<>NEW.token_id
 OR lot.external_adoption_id IS NULL OR lot.opening_order_id IS NOT NULL OR lot.opening_fill_key IS NOT NULL
 OR lot.status<>'CLOSED' OR lot.remaining_shares<>0 OR lot.remaining_cost<>0
 OR lot.original_shares<>NEW.allocated_shares OR lot.original_cost<>NEW.allocated_principal
 OR lot.opened_at>NEW.sold_at OR lot.closed_at<NEW.sold_at
 OR rebase.execution_account_id<>NEW.execution_account_id OR rebase.event_type<>'MANUAL_REBASE'
 OR rebase.occurred_at IS DISTINCT FROM lot.closed_at OR rebase.occurred_at<NEW.sold_at
 OR rebase.order_id<>'' OR rebase.fill_key<>'' THEN
  RAISE EXCEPTION 'historical sale does not match the closed adopted lot and original cash rebase';
 END IF;
 IF EXISTS(SELECT 1 FROM position_lot_closures WHERE lot_id=NEW.lot_id)
 OR EXISTS(SELECT 1 FROM position_lot_redemptions WHERE lot_id=NEW.lot_id)
 OR EXISTS(SELECT 1 FROM kalshi_settlement_lots WHERE lot_id=NEW.lot_id)
 OR EXISTS(SELECT 1 FROM managed_external_sell_allocations WHERE lot_id=NEW.lot_id)
 OR EXISTS(SELECT 1 FROM execution_fills WHERE execution_account_id=NEW.execution_account_id AND transaction_hash=NEW.transaction_hash)
 OR EXISTS(SELECT 1 FROM managed_external_sells WHERE execution_account_id=NEW.execution_account_id AND transaction_hash=NEW.transaction_hash) THEN
  RAISE EXCEPTION 'historical sale already has accounting evidence';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER historical_rebased_lot_sale_insert_guard BEFORE INSERT ON historical_rebased_lot_sales
 FOR EACH ROW EXECUTE FUNCTION check_historical_rebased_lot_sale();
CREATE FUNCTION reject_historical_rebased_lot_sale_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'historical rebased lot sales are append-only'; END $$;
CREATE TRIGGER historical_rebased_lot_sales_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON historical_rebased_lot_sales
 FOR EACH STATEMENT EXECUTE FUNCTION reject_historical_rebased_lot_sale_mutation();

REVOKE ALL ON historical_rebased_lot_sales FROM PUBLIC;
DO $$ BEGIN
 IF to_regrole('trading_execution_app') IS NOT NULL THEN
  GRANT SELECT ON historical_rebased_lot_sales TO trading_execution_app;
  REVOKE INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON historical_rebased_lot_sales FROM trading_execution_app;
  IF has_table_privilege('trading_execution_app','historical_rebased_lot_sales','INSERT,UPDATE,DELETE,TRUNCATE') THEN
   RAISE EXCEPTION 'runtime application must have read-only historical-sale privileges';
  END IF;
 END IF;
END $$;
COMMIT;
