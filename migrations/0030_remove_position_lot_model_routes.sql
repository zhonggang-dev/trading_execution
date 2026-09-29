BEGIN;

-- Model ownership is the immutable identity recorded on the opening lot.
-- Strategy readers select only the current binding's lots; old-model dust is
-- retained for settlement/redemption and is never reassigned to a new model.
DROP VIEW position_lot_model_routes_effective;

DROP TABLE position_lot_model_route_successors;
DROP FUNCTION enforce_position_lot_model_route_successor_insert_guard();

DROP TABLE position_lot_model_routes;
DROP FUNCTION enforce_position_lot_model_route_insert_guard();
DROP FUNCTION reject_position_lot_model_route_mutation();

ALTER TABLE position_lots
    DROP CONSTRAINT position_lots_lot_origin_model_unique;

COMMIT;
