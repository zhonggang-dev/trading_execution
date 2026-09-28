BEGIN;

-- Preserve the original Gemini -> qwen_masked audit route.  A successor is a
-- second, append-only decision about the logical owner of that exact lot.
CREATE TABLE position_lot_model_route_successors (
    lot_id TEXT PRIMARY KEY REFERENCES position_lot_model_routes (lot_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    prior_logical_model_id TEXT NOT NULL,
    logical_model_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    actor TEXT NOT NULL,
    routed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT position_lot_model_route_successors_identity_nonempty CHECK (
        btrim(lot_id) <> '' AND btrim(prior_logical_model_id) <> ''
        AND btrim(logical_model_id) <> ''
        AND prior_logical_model_id <> logical_model_id
    ),
    CONSTRAINT position_lot_model_route_successors_audit_nonempty CHECK (
        btrim(reason) <> '' AND btrim(actor) <> ''
    )
);

CREATE FUNCTION enforce_position_lot_model_route_successor_insert_guard()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    global_kill BOOLEAN;
    lot_account_id TEXT;
    lot_token_id TEXT;
    lot_status TEXT;
    prior_model_id TEXT;
BEGIN
    LOCK TABLE execution_risk_global_control IN SHARE MODE;
    LOCK TABLE position_lot_model_routes IN SHARE MODE;
    LOCK TABLE position_lots IN SHARE MODE;
    LOCK TABLE asset_reservations IN SHARE MODE;
    LOCK TABLE execution_orders IN SHARE MODE;
    LOCK TABLE strategy_order_intent_deliveries IN SHARE MODE;

    SELECT kill_switch INTO global_kill
      FROM execution_risk_global_control WHERE singleton=TRUE;
    IF NOT FOUND OR NOT global_kill THEN
        RAISE EXCEPTION 'position lot model route successor requires global kill switch';
    END IF;

    SELECT lot.execution_account_id, lot.token_id, lot.status, route.logical_model_id
      INTO lot_account_id, lot_token_id, lot_status, prior_model_id
      FROM position_lot_model_routes AS route
      JOIN position_lots AS lot USING (lot_id)
     WHERE route.lot_id=NEW.lot_id;
    IF NOT FOUND OR prior_model_id IS DISTINCT FROM NEW.prior_logical_model_id THEN
        RAISE EXCEPTION 'position lot model route successor prior identity does not match';
    END IF;
    IF lot_status <> 'OPEN' THEN
        RAISE EXCEPTION 'position lot model route successor target must be OPEN';
    END IF;

    IF EXISTS (
        SELECT 1 FROM asset_reservations AS reservation
         WHERE reservation.execution_account_id=lot_account_id
           AND reservation.side='SELL'
           AND reservation.status IN ('ACTIVE','RECONCILIATION_REQUIRED')
           AND (reservation.target_lot_id=NEW.lot_id
                OR (reservation.target_lot_id IS NULL AND reservation.token_id=lot_token_id))
    ) THEN
        RAISE EXCEPTION 'position lot model route successor target has an active reservation';
    END IF;
    IF EXISTS (
        SELECT 1 FROM execution_orders AS order_row
         WHERE order_row.execution_account_id=lot_account_id
           AND order_row.status NOT IN ('FILLED','REJECTED','CANCELLED','MANUAL_REVIEW')
    ) THEN
        RAISE EXCEPTION 'position lot model route successor account has a non-terminal order';
    END IF;
    IF EXISTS (
        SELECT 1 FROM strategy_order_intent_deliveries AS delivery
         WHERE delivery.status IN ('PENDING','SUBMITTING')
           AND btrim(COALESCE(delivery.intent_payload->>'execution_account_id',''))=lot_account_id
    ) THEN
        RAISE EXCEPTION 'position lot model route successor account has a pending intent delivery';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER position_lot_model_route_successors_insert_guard_trigger
BEFORE INSERT ON position_lot_model_route_successors
FOR EACH ROW EXECUTE FUNCTION enforce_position_lot_model_route_successor_insert_guard();

CREATE TRIGGER position_lot_model_route_successors_append_only_trigger
BEFORE UPDATE OR DELETE ON position_lot_model_route_successors
FOR EACH ROW EXECUTE FUNCTION reject_position_lot_model_route_mutation();

CREATE TRIGGER position_lot_model_route_successors_truncate_guard_trigger
BEFORE TRUNCATE ON position_lot_model_route_successors
FOR EACH STATEMENT EXECUTE FUNCTION reject_position_lot_model_route_mutation();

CREATE VIEW position_lot_model_routes_effective AS
SELECT route.lot_id,
       COALESCE(successor.logical_model_id, route.logical_model_id) AS logical_model_id
  FROM position_lot_model_routes AS route
  LEFT JOIN position_lot_model_route_successors AS successor USING (lot_id);

COMMIT;
