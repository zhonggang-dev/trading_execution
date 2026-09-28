-- Run only after migration 0029, with trading-execution stopped, both decision
-- switches off, and the global kill switch on. This transaction does not
-- rewrite the historical Gemini -> qwen_masked lot routes or any old orders.
BEGIN;
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '120s';

DO $preflight$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM execution_risk_global_control
         WHERE singleton=TRUE AND kill_switch=TRUE
    ) THEN
        RAISE EXCEPTION 'v4.1 flash cutover requires the global kill switch';
    END IF;
    IF (SELECT count(*) FROM execution_strategy_bindings WHERE enabled=TRUE) <> 4
       OR (SELECT count(*) FROM execution_strategy_bindings
            WHERE enabled=TRUE AND model_id='qwen_masked'
              AND (strategy_id, execution_account_id) IN
                  (('multfactor_v1','wallet-6'),('multfactor_v2','wallet-7'))) <> 2 THEN
        RAISE EXCEPTION 'wallet-6/7 are not exactly bound to the temporary Qwen identity';
    END IF;
    IF EXISTS (
        SELECT 1 FROM position_lots AS lot
        LEFT JOIN position_lot_model_routes AS route USING (lot_id)
        WHERE lot.execution_account_id IN ('wallet-6','wallet-7')
          AND lot.status='OPEN'
          AND (lot.model_id <> 'gemini_masked'
               OR route.origin_model_id IS DISTINCT FROM 'gemini_masked'
               OR route.logical_model_id IS DISTINCT FROM 'qwen_masked')
    ) THEN
        RAISE EXCEPTION 'wallet-6/7 contain an unexpected OPEN lot identity';
    END IF;
    IF EXISTS (
        SELECT 1 FROM position_lot_model_route_successors AS successor
        JOIN position_lots AS lot USING (lot_id)
        WHERE lot.execution_account_id IN ('wallet-6','wallet-7')
          AND lot.status='OPEN'
    ) THEN
        RAISE EXCEPTION 'a wallet-6/7 OPEN lot already has a successor route';
    END IF;
    IF EXISTS (
        SELECT 1 FROM asset_reservations
        WHERE execution_account_id IN ('wallet-6','wallet-7') AND side='SELL'
          AND status IN ('ACTIVE','RECONCILIATION_REQUIRED')
    ) OR EXISTS (
        SELECT 1 FROM execution_orders
        WHERE execution_account_id IN ('wallet-6','wallet-7')
          AND status NOT IN ('FILLED','REJECTED','CANCELLED','MANUAL_REVIEW')
    ) OR EXISTS (
        SELECT 1 FROM strategy_order_intent_deliveries
        WHERE status IN ('PENDING','SUBMITTING')
          AND intent_payload->>'execution_account_id' IN ('wallet-6','wallet-7')
    ) THEN
        RAISE EXCEPTION 'wallet-6/7 have an in-flight SELL, order, or intent';
    END IF;
END
$preflight$;

INSERT INTO position_lot_model_route_successors (
    lot_id, prior_logical_model_id, logical_model_id, reason, actor
)
SELECT lot.lot_id, 'qwen_masked', 'deepseek_masked',
       'Replace temporary Qwen logical identity with the confirmed v4.1 flash model',
       'wallet67-v41-flash-cutover'
FROM position_lots AS lot
JOIN position_lot_model_routes AS route USING (lot_id)
WHERE lot.execution_account_id IN ('wallet-6','wallet-7')
  AND lot.status='OPEN' AND lot.model_id='gemini_masked'
  AND route.logical_model_id='qwen_masked'
ORDER BY lot.lot_id;

UPDATE execution_strategy_bindings
   SET enabled=FALSE, version=version+1, updated_at=clock_timestamp()
 WHERE enabled=TRUE AND model_id='qwen_masked'
   AND (strategy_id, execution_account_id) IN
       (('multfactor_v1','wallet-6'),('multfactor_v2','wallet-7'));

INSERT INTO execution_strategy_bindings (
    model_id, strategy_id, execution_account_id, enabled
) VALUES
    ('deepseek_masked','multfactor_v1','wallet-6',TRUE),
    ('deepseek_masked','multfactor_v2','wallet-7',TRUE);

DO $verify$
BEGIN
    IF (SELECT count(*) FROM execution_strategy_bindings WHERE enabled=TRUE) <> 4
       OR (SELECT count(*) FROM execution_strategy_bindings
            WHERE enabled=TRUE AND model_id='deepseek_masked'
              AND (strategy_id, execution_account_id) IN
                  (('multfactor_v1','wallet-6'),('multfactor_v2','wallet-7'))) <> 2
       OR EXISTS (
            SELECT 1 FROM position_lots AS lot
            LEFT JOIN position_lot_model_routes_effective AS route USING (lot_id)
            WHERE lot.execution_account_id IN ('wallet-6','wallet-7')
              AND lot.status='OPEN'
              AND route.logical_model_id IS DISTINCT FROM 'deepseek_masked'
       ) THEN
        RAISE EXCEPTION 'v4.1 flash wallet-6/7 cutover verification failed';
    END IF;
END
$verify$;

COMMIT;
