BEGIN;

-- Operator-only accounting of confirmed out-of-band SELLs of managed lots.
-- No local order, reservation or execution_fill is fabricated.
CREATE TABLE managed_external_sell_batches (
    batch_id TEXT PRIMARY KEY CHECK (batch_id ~ '^[0-9a-f]{64}$'),
    execution_account_id TEXT NOT NULL REFERENCES execution_accounts,
    actor TEXT NOT NULL CHECK (btrim(actor) <> ''),
    reason TEXT NOT NULL CHECK (btrim(reason) <> ''),
    evidence JSONB NOT NULL CHECK (jsonb_typeof(evidence)='object'),
    before_state JSONB NOT NULL CHECK (jsonb_typeof(before_state)='object'),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE managed_external_sells (
    sell_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES managed_external_sell_batches,
    execution_account_id TEXT NOT NULL REFERENCES execution_accounts,
    wallet_address TEXT NOT NULL,
    venue_trade_id TEXT NOT NULL CHECK (btrim(venue_trade_id)<>''),
    venue_order_id TEXT NOT NULL CHECK (venue_order_id ~ '^0x[0-9a-f]{64}$'),
    transaction_hash TEXT NOT NULL CHECK (transaction_hash ~ '^0x[0-9a-f]{64}$'),
    log_index BIGINT NOT NULL CHECK (log_index>=0),
    condition_id TEXT NOT NULL CHECK (condition_id ~ '^0x[0-9a-f]{64}$'),
    token_id TEXT NOT NULL CHECK (token_id ~ '^[0-9]+$'),
    model_id TEXT NOT NULL, strategy_id TEXT NOT NULL,
    shares NUMERIC NOT NULL CHECK (shares>0),
    gross_notional NUMERIC NOT NULL CHECK (gross_notional>0),
    total_fee NUMERIC NOT NULL CHECK (total_fee>=0 AND total_fee<gross_notional),
    net_cash NUMERIC NOT NULL CHECK (net_cash=gross_notional-total_fee),
    allocated_cost NUMERIC NOT NULL CHECK (allocated_cost>=0),
    shares_before NUMERIC NOT NULL CHECK (shares_before>=shares),
    shares_after NUMERIC NOT NULL CHECK (shares_after=shares_before-shares),
    occurred_at TIMESTAMPTZ NOT NULL,
    account_event_id TEXT NOT NULL UNIQUE REFERENCES execution_account_events DEFERRABLE INITIALLY DEFERRED,
    position_event_id TEXT NOT NULL UNIQUE REFERENCES position_events DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (execution_account_id,transaction_hash,log_index),
    UNIQUE (execution_account_id,venue_trade_id,venue_order_id,token_id),
    UNIQUE (batch_id,token_id)
);
CREATE TABLE managed_external_sell_allocations (
    sell_id TEXT NOT NULL REFERENCES managed_external_sells DEFERRABLE INITIALLY DEFERRED,
    lot_id TEXT NOT NULL REFERENCES position_lots,
    shares_before NUMERIC NOT NULL CHECK (shares_before>0),
    cost_before NUMERIC NOT NULL CHECK (cost_before>0),
    shares_after NUMERIC NOT NULL CHECK (shares_after>=0 AND shares_after<shares_before),
    cost_after NUMERIC NOT NULL CHECK (cost_after>=0 AND cost_after<=cost_before AND (shares_after>0 OR cost_after=0)),
    closed_shares NUMERIC NOT NULL CHECK (closed_shares=shares_before-shares_after),
    allocated_cost NUMERIC NOT NULL CHECK (allocated_cost=cost_before-cost_after),
    net_proceeds NUMERIC NOT NULL CHECK (net_proceeds>=0),
    PRIMARY KEY (sell_id,lot_id)
);
CREATE INDEX managed_external_sells_account_time_idx ON managed_external_sells(execution_account_id,occurred_at);

CREATE FUNCTION reject_managed_external_sell_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'managed external sell audit records are append-only'; END $$;
CREATE TRIGGER managed_external_batches_immutable BEFORE UPDATE OR DELETE ON managed_external_sell_batches
    FOR EACH ROW EXECUTE FUNCTION reject_managed_external_sell_mutation();
CREATE TRIGGER managed_external_sells_immutable BEFORE UPDATE OR DELETE ON managed_external_sells
    FOR EACH ROW EXECUTE FUNCTION reject_managed_external_sell_mutation();
CREATE TRIGGER managed_external_allocations_immutable BEFORE UPDATE OR DELETE ON managed_external_sell_allocations
    FOR EACH ROW EXECUTE FUNCTION reject_managed_external_sell_mutation();

CREATE FUNCTION check_managed_external_sell_accounting() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE s managed_external_sells; q NUMERIC; c NUMERIC; n NUMERIC;
BEGIN
    SELECT * INTO STRICT s FROM managed_external_sells WHERE sell_id=NEW.sell_id;
    SELECT sum(closed_shares),sum(allocated_cost),sum(net_proceeds) INTO q,c,n
      FROM managed_external_sell_allocations WHERE sell_id=s.sell_id;
    IF q IS DISTINCT FROM s.shares OR c IS DISTINCT FROM s.allocated_cost OR n IS DISTINCT FROM s.net_cash
       OR EXISTS (SELECT 1 FROM managed_external_sell_allocations x JOIN position_lots lot USING(lot_id)
           WHERE x.sell_id=s.sell_id AND (lot.execution_account_id<>s.execution_account_id OR lot.token_id<>s.token_id
             OR lot.model_id<>s.model_id OR lot.strategy_id<>s.strategy_id
             OR lot.remaining_shares<>x.shares_after OR lot.remaining_cost<>x.cost_after))
       OR NOT EXISTS (SELECT 1 FROM execution_account_events WHERE account_event_id=s.account_event_id
           AND execution_account_id=s.execution_account_id AND event_type='EXTERNAL_SELL'
           AND total_balance_delta=s.net_cash AND available_balance_delta=s.net_cash AND reserved_balance_delta=0
           AND order_id='' AND fill_key='')
       OR NOT EXISTS (SELECT 1 FROM position_events WHERE position_event_id=s.position_event_id
           AND execution_account_id=s.execution_account_id AND token_id=s.token_id AND event_type='SOLD'
           AND shares_delta=-s.shares AND cash_delta=s.net_cash AND cost_basis_delta=-s.allocated_cost
           AND realized_pnl_delta=s.net_cash-s.allocated_cost AND shares_after=s.shares_after AND order_id='' AND fill_key='') THEN
        RAISE EXCEPTION 'external sell accounting is incomplete or unbalanced';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER managed_external_sell_accounting AFTER INSERT ON managed_external_sells
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_managed_external_sell_accounting();
CREATE CONSTRAINT TRIGGER managed_external_allocation_accounting AFTER INSERT ON managed_external_sell_allocations
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_managed_external_sell_accounting();

-- The CLI verifies canonical receipts and current on-chain balances immediately
-- before invoking this function. SQL validates ownership, exact deltas and locks.
-- --database-dry-run runs this complete transaction and ROLLBACKs it.
CREATE FUNCTION apply_managed_external_sells(m JSONB, batch TEXT, operator_name TEXT, repair_reason TEXT)
RETURNS JSONB LANGUAGE plpgsql AS $$
DECLARE
    account_id TEXT := m->>'account'; a execution_accounts; p execution_positions;
    t JSONB; e JSONB; l RECORD; key TEXT; token TEXT;
    sold NUMERIC; gross NUMERIC; fee NUMERIC; net NUMERIC; remaining NUMERIC;
    old_cost NUMERIC; total_cost NUMERIC; cost_after NUMERIC; closed_qty NUMERIC;
    total_net NUMERIC; allocated_net NUMERIC; cash_delta NUMERIC := 0;
    model TEXT; strategy TEXT; trade_time TIMESTAMPTZ; snapshot JSONB; result JSONB;
BEGIN
    IF EXISTS (SELECT 1 FROM managed_external_sell_batches WHERE batch_id=batch AND execution_account_id=account_id) THEN
        RETURN jsonb_build_object('already_applied',true,'batch_id',batch);
    END IF;
    IF jsonb_typeof(m->'trades') IS DISTINCT FROM 'array' OR jsonb_array_length(m->'trades') NOT BETWEEN 1 AND 20
       OR m->>'observed_at' IS NULL
       OR (m->>'observed_at')::timestamptz < clock_timestamp()-interval '2 minutes'
       OR (m->>'observed_at')::timestamptz > clock_timestamp()+interval '5 seconds' THEN
        RAISE EXCEPTION 'invalid or stale external sell evidence';
    END IF;
    -- Prevent reconciliation from starting across the short repair transaction.
    LOCK TABLE reconciliation_runs IN SHARE ROW EXCLUSIVE MODE;
    SELECT * INTO STRICT a FROM execution_accounts WHERE execution_account_id=account_id FOR UPDATE;
    PERFORM 1 FROM execution_risk_controls WHERE execution_account_id=account_id FOR UPDATE;
    IF lower(a.wallet_address) IS DISTINCT FROM lower(m->>'wallet') OR a.collateral_asset<>'pUSD'
       OR a.reserved_balance<>0 OR NOT EXISTS (SELECT 1 FROM execution_risk_controls
           WHERE execution_account_id=account_id AND control_scope='ACCOUNT' AND control_key='' AND paused)
       OR EXISTS (SELECT 1 FROM reconciliation_runs WHERE execution_account_id=account_id AND status='RUNNING')
       OR EXISTS (SELECT 1 FROM execution_orders WHERE execution_account_id=account_id
           AND status NOT IN ('FILLED','CANCELLED','REJECTED','EXPIRED'))
       OR EXISTS (SELECT 1 FROM asset_reservations WHERE execution_account_id=account_id AND status IN ('ACTIVE','RECONCILIATION_REQUIRED'))
       OR EXISTS (SELECT 1 FROM execution_fills WHERE execution_account_id=account_id AND applied_at IS NULL AND status<>'FAILED')
       OR EXISTS (SELECT 1 FROM execution_external_position_baselines WHERE execution_account_id=account_id) THEN
        RAISE EXCEPTION 'account must be paused, quiescent, fully managed and have no pending fills';
    END IF;
    PERFORM 1 FROM execution_positions WHERE execution_account_id=account_id ORDER BY token_id FOR UPDATE;
    PERFORM 1 FROM position_lots WHERE execution_account_id=account_id ORDER BY lot_id FOR UPDATE;
    SELECT jsonb_build_object('account',to_jsonb(a),
        'positions',(SELECT coalesce(jsonb_agg(to_jsonb(x)),'[]') FROM execution_positions x WHERE execution_account_id=account_id),
        'lots',(SELECT coalesce(jsonb_agg(to_jsonb(x)),'[]') FROM position_lots x WHERE execution_account_id=account_id),
        'issues',(SELECT coalesce(jsonb_agg(to_jsonb(x)),'[]') FROM reconciliation_issues x WHERE execution_account_id=account_id AND status='OPEN')) INTO snapshot;
    INSERT INTO managed_external_sell_batches(batch_id,execution_account_id,actor,reason,evidence,before_state)
      VALUES (batch,account_id,operator_name,repair_reason,m,snapshot);
    FOR t IN SELECT value FROM jsonb_array_elements(m->'trades') LOOP
        e:=t->'evidence'; token:=e->>'TokenID'; trade_time:=(t->>'matched_at')::timestamptz;
        sold:=(e->>'MakerAmountBaseUnits')::numeric/1000000;
        gross:=(e->>'TakerAmountBaseUnits')::numeric/1000000;
        fee:=(e->>'FeeBaseUnits')::numeric/1000000; net:=gross-fee;
        remaining:=(t->>'remaining_shares')::numeric;
        key:='external-sell:'||account_id||':'||(e->>'TransactionHash')||':'||(e->>'LogIndex');
        IF (e->>'Side')::int IS DISTINCT FROM 1 OR lower(e->>'Maker') IS DISTINCT FROM lower(a.wallet_address)
           OR coalesce((e->>'Confirmations')::bigint,0)<128 OR coalesce((e->>'BlockNumber')::bigint,0)<=0
           OR coalesce(e->>'BlockHash','') !~ '^0x[0-9a-f]{64}$' OR sold IS NULL OR sold<=0 OR gross IS NULL OR gross<=0
           OR fee IS NULL OR fee<0 OR net<=0 OR remaining IS NULL OR remaining<0
           OR remaining*1000000<>trunc(remaining*1000000) OR trade_time IS NULL OR trade_time>clock_timestamp()-interval '5 minutes'
           OR EXISTS (SELECT 1 FROM execution_orders WHERE lower(venue_order_id)=lower(e->>'OrderHash'))
           OR EXISTS (SELECT 1 FROM execution_fills WHERE execution_account_id=account_id AND transaction_hash=e->>'TransactionHash')
           OR EXISTS (SELECT 1 FROM execution_external_position_dispositions WHERE execution_account_id=account_id AND venue_trade_id=t->>'venue_trade_id')
           OR NOT EXISTS (SELECT 1 FROM reconciliation_issues WHERE execution_account_id=account_id AND status='OPEN'
               AND issue_type='EXTERNAL_TRADE' AND venue_trade_id=t->>'venue_trade_id' AND venue_order_id=e->>'OrderHash'
               AND token_id=token AND condition_id=t->>'condition_id') THEN
            RAISE EXCEPTION 'unverified, duplicate or locally owned external trade %',t->>'venue_trade_id';
        END IF;
        SELECT * INTO STRICT p FROM execution_positions WHERE execution_account_id=account_id AND token_id=token;
        IF p.total_shares IS DISTINCT FROM sold+remaining OR p.reserved_shares<>0 OR p.available_shares<>p.total_shares
           OR p.condition_id IS DISTINCT FROM t->>'condition_id' OR p.lifecycle_status<>'OPEN'
           OR EXISTS (SELECT 1 FROM position_events WHERE execution_account_id=account_id AND token_id=token
               AND shares_delta<>0 AND occurred_at>trade_time) THEN
            RAISE EXCEPTION 'position does not exactly reconcile with chain evidence for %',token;
        END IF;
        SELECT min(model_id),min(strategy_id),sum(remaining_cost) INTO model,strategy,old_cost FROM position_lots
          WHERE execution_account_id=account_id AND token_id=token AND status='OPEN';
        IF old_cost IS DISTINCT FROM p.cost_basis OR (SELECT sum(remaining_shares) FROM position_lots
            WHERE execution_account_id=account_id AND token_id=token AND status='OPEN') IS DISTINCT FROM p.total_shares
           OR (SELECT count(DISTINCT (model_id,strategy_id)) FROM position_lots
            WHERE execution_account_id=account_id AND token_id=token AND status='OPEN')<>1
           OR EXISTS (SELECT 1 FROM position_lots WHERE execution_account_id=account_id AND token_id=token AND status='OPEN'
             AND (opened_at>trade_time OR remaining_cost<=0 OR remaining_shares*1000000<>trunc(remaining_shares*1000000))) THEN
            RAISE EXCEPTION 'ambiguous ownership or inconsistent lot totals for %',token;
        END IF;
        total_cost:=0; total_net:=0;
        -- Largest-remainder allocation of remaining ERC1155 base units. Exact
        -- aggregate, deterministic tie-break; proportional cost at 30 decimals.
        FOR l IN WITH weights AS (
            SELECT *, remaining*1000000*remaining_shares/p.total_shares AS units
              FROM position_lots WHERE execution_account_id=account_id AND token_id=token AND status='OPEN'
        ), ranked AS (
            SELECT *, row_number() OVER(ORDER BY units-floor(units) DESC,lot_id) AS rank,
              remaining*1000000-sum(floor(units)) OVER() AS extra FROM weights
        ) SELECT *, (floor(units)+CASE WHEN rank<=extra THEN 1 ELSE 0 END)/1000000 AS after_shares,
            row_number() OVER(ORDER BY lot_id) AS sequence, count(*) OVER() AS lot_count
            FROM ranked ORDER BY lot_id LOOP
            closed_qty:=l.remaining_shares-l.after_shares;
            IF closed_qty<=0 THEN RAISE EXCEPTION 'repair scope too small for proportional lot allocation'; END IF;
            cost_after:=CASE WHEN l.after_shares=0 THEN 0 ELSE round(l.remaining_cost*l.after_shares/l.remaining_shares,30) END;
            allocated_net:=CASE WHEN l.sequence=l.lot_count THEN net-total_net ELSE trunc(net*closed_qty/sold,30) END;
            INSERT INTO managed_external_sell_allocations VALUES(key,l.lot_id,l.remaining_shares,l.remaining_cost,
                l.after_shares,cost_after,closed_qty,l.remaining_cost-cost_after,allocated_net);
            UPDATE position_lots SET remaining_shares=l.after_shares,remaining_cost=cost_after,
                status=CASE WHEN l.after_shares=0 THEN 'CLOSED' ELSE 'OPEN' END,
                closed_at=CASE WHEN l.after_shares=0 THEN trade_time ELSE NULL END WHERE lot_id=l.lot_id;
            total_cost:=total_cost+l.remaining_cost-cost_after; total_net:=total_net+allocated_net;
        END LOOP;
        UPDATE execution_positions SET total_shares=remaining,available_shares=remaining,cost_basis=old_cost-total_cost,
            average_cost_price=CASE WHEN remaining=0 THEN 0 ELSE (old_cost-total_cost)/remaining END,
            realized_pnl=realized_pnl+net-total_cost,
            market_value=coalesce(mark_price*remaining,0),
            unrealized_pnl=CASE WHEN mark_price IS NULL THEN 0 ELSE mark_price*remaining-(old_cost-total_cost) END,
            is_dust=(remaining>0 AND remaining<0.01), lifecycle_status=CASE WHEN remaining=0 THEN 'CLOSED' ELSE 'OPEN' END,
            version=version+1,updated_at=clock_timestamp() WHERE execution_account_id=account_id AND token_id=token;
        UPDATE execution_accounts SET total_balance=total_balance+net,available_balance=available_balance+net,
            version=version+1,updated_at=clock_timestamp() WHERE execution_account_id=account_id;
        INSERT INTO execution_account_events(account_event_id,execution_account_id,event_type,total_balance_delta,
            available_balance_delta,reserved_balance_delta,total_balance_after,available_balance_after,reserved_balance_after,occurred_at)
            SELECT key,account_id,'EXTERNAL_SELL',net,net,0,total_balance,available_balance,reserved_balance,trade_time
              FROM execution_accounts WHERE execution_account_id=account_id;
        INSERT INTO position_events(position_event_id,event_type,execution_account_id,market_id,token_id,model_id,strategy_id,
            shares_delta,cash_delta,cost_basis_delta,realized_pnl_delta,shares_after,cost_basis_after,average_cost_after,
            realized_pnl_after,mark_price,unrealized_pnl_after,occurred_at)
            SELECT key,'SOLD',account_id,market_id,token_id,model,strategy,-sold,net,-total_cost,net-total_cost,
                total_shares,cost_basis,average_cost_price,realized_pnl,mark_price,unrealized_pnl,trade_time
                FROM execution_positions WHERE execution_account_id=account_id AND token_id=token;
        INSERT INTO managed_external_sells VALUES(key,batch,account_id,lower(a.wallet_address),t->>'venue_trade_id',e->>'OrderHash',
            e->>'TransactionHash',(e->>'LogIndex')::bigint,t->>'condition_id',token,model,strategy,sold,gross,fee,net,total_cost,
            p.total_shares,remaining,trade_time,key,key);
        UPDATE reconciliation_issues SET status='RESOLVED',resolved_at=clock_timestamp(),
            details=details||' [audited managed external sell repair: '||batch||']'
          WHERE execution_account_id=account_id AND status='OPEN' AND
            ((issue_type='EXTERNAL_TRADE' AND venue_trade_id=t->>'venue_trade_id' AND venue_order_id=e->>'OrderHash'
              AND token_id=token AND condition_id=t->>'condition_id') OR (issue_type='POSITION_DRIFT' AND token_id=token));
        cash_delta:=cash_delta+net;
    END LOOP;
    IF a.total_balance+cash_delta IS DISTINCT FROM (m->>'cash')::numeric THEN
        RAISE EXCEPTION 'cash delta does not exactly reconcile with current chain balance';
    END IF;
    UPDATE reconciliation_issues SET status='RESOLVED',resolved_at=clock_timestamp(),
        details=details||' [audited managed external sell repair: '||batch||']'
      WHERE execution_account_id=account_id AND status='OPEN' AND issue_type='BALANCE_DRIFT'
        AND local_value=a.total_balance AND remote_value=(m->>'cash')::numeric;
    SET CONSTRAINTS ALL IMMEDIATE;
    SELECT jsonb_build_object('already_applied',false,'batch_id',batch,'cash_delta',cash_delta::text,
      'cash_after',(a.total_balance+cash_delta)::text,'trades',count(*),'allocated_cost',sum(allocated_cost)::text,
      'realized_pnl',sum(net_cash-allocated_cost)::text,'remaining_shares',sum(shares_after)::text) INTO result
      FROM managed_external_sells WHERE batch_id=batch;
    INSERT INTO execution_outbox(outbox_event_id,topic,event_key,aggregate_type,aggregate_id,payload,created_at)
      VALUES ('managed-external-sells:'||batch,'trading.accounting.external_sell.v1',batch,'execution_account',account_id,
        result||jsonb_build_object('execution_account_id',account_id,'source','EXTERNAL_SELL'),clock_timestamp());
    RETURN result;
END $$;
REVOKE ALL ON FUNCTION apply_managed_external_sells(JSONB,TEXT,TEXT,TEXT) FROM PUBLIC;
COMMIT;
