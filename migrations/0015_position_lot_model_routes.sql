BEGIN;

-- A lot keeps the model that owned its opening BUY forever.  When model names
-- are changed at the strategy boundary, this append-only table authorizes one
-- exact legacy lot to be managed by the new logical model for future exits.
ALTER TABLE position_lots
    ADD CONSTRAINT position_lots_lot_origin_model_unique
    UNIQUE (lot_id, model_id);

CREATE TABLE position_lot_model_routes (
    lot_id TEXT PRIMARY KEY,
    origin_model_id TEXT NOT NULL,
    logical_model_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    actor TEXT NOT NULL,
    routed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT position_lot_model_routes_origin_fk
        FOREIGN KEY (lot_id, origin_model_id)
        REFERENCES position_lots (lot_id, model_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT position_lot_model_routes_identity_nonempty CHECK (
        btrim(lot_id) <> ''
        AND btrim(origin_model_id) <> ''
        AND btrim(logical_model_id) <> ''
        AND origin_model_id <> logical_model_id
    ),
    CONSTRAINT position_lot_model_routes_audit_nonempty CHECK (
        btrim(reason) <> '' AND btrim(actor) <> ''
    )
);

CREATE FUNCTION enforce_position_lot_model_route_insert_guard()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    global_kill BOOLEAN;
    lot_account_id TEXT;
    lot_token_id TEXT;
    lot_origin_model_id TEXT;
    lot_status TEXT;
BEGIN
    -- These table locks make the preconditions stable until the route INSERT
    -- commits. Production still performs this operation with the service
    -- stopped; the locks make an accidental concurrent writer fail by waiting
    -- rather than racing past the checks below.
    LOCK TABLE execution_risk_global_control IN SHARE MODE;
    LOCK TABLE position_lots IN SHARE MODE;
    LOCK TABLE asset_reservations IN SHARE MODE;
    LOCK TABLE execution_orders IN SHARE MODE;
    LOCK TABLE strategy_order_intent_deliveries IN SHARE MODE;

    SELECT kill_switch
      INTO global_kill
      FROM execution_risk_global_control
     WHERE singleton=TRUE;
    IF NOT FOUND OR NOT global_kill THEN
        RAISE EXCEPTION 'position lot model route requires global kill switch';
    END IF;

    SELECT execution_account_id, token_id, model_id, status
      INTO lot_account_id, lot_token_id, lot_origin_model_id, lot_status
      FROM position_lots
     WHERE lot_id=NEW.lot_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'position lot model route target does not exist';
    END IF;
    IF lot_origin_model_id IS DISTINCT FROM NEW.origin_model_id THEN
        RAISE EXCEPTION 'position lot model route origin does not match target lot';
    END IF;
    IF lot_status <> 'OPEN' THEN
        RAISE EXCEPTION 'position lot model route target must be OPEN';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM asset_reservations AS reservation
         WHERE reservation.execution_account_id=lot_account_id
           AND reservation.side='SELL'
           AND reservation.status IN ('ACTIVE','RECONCILIATION_REQUIRED')
           AND (
               reservation.target_lot_id=NEW.lot_id
               OR (reservation.target_lot_id IS NULL AND reservation.token_id=lot_token_id)
           )
    ) THEN
        RAISE EXCEPTION 'position lot model route target has an active reservation';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM execution_orders AS order_row
         WHERE order_row.execution_account_id=lot_account_id
           AND order_row.status NOT IN ('FILLED','REJECTED','CANCELLED','MANUAL_REVIEW')
    ) THEN
        RAISE EXCEPTION 'position lot model route account has a non-terminal order';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM strategy_order_intent_deliveries AS delivery
         WHERE delivery.status IN ('PENDING','SUBMITTING')
           AND btrim(COALESCE(delivery.intent_payload->>'execution_account_id',''))=lot_account_id
    ) THEN
        RAISE EXCEPTION 'position lot model route account has a pending intent delivery';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER position_lot_model_routes_insert_guard_trigger
BEFORE INSERT ON position_lot_model_routes
FOR EACH ROW EXECUTE FUNCTION enforce_position_lot_model_route_insert_guard();

CREATE FUNCTION reject_position_lot_model_route_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'position lot model routes are append-only';
END;
$$;

CREATE TRIGGER position_lot_model_routes_append_only_trigger
BEFORE UPDATE OR DELETE ON position_lot_model_routes
FOR EACH ROW EXECUTE FUNCTION reject_position_lot_model_route_mutation();

CREATE TRIGGER position_lot_model_routes_truncate_guard_trigger
BEFORE TRUNCATE ON position_lot_model_routes
FOR EACH STATEMENT EXECUTE FUNCTION reject_position_lot_model_route_mutation();

-- The opening model is historical BUY ownership, not mutable routing state.
CREATE FUNCTION enforce_position_lot_model_immutable()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.model_id IS DISTINCT FROM OLD.model_id THEN
        RAISE EXCEPTION 'position lot origin model is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER position_lots_model_immutable_trigger
BEFORE UPDATE OF model_id ON position_lots
FOR EACH ROW EXECUTE FUNCTION enforce_position_lot_model_immutable();

COMMIT;
