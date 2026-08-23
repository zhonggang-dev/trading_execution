BEGIN;

-- Keep retired account routes as immutable operational history. Authorization
-- uniqueness applies only to the row that is currently enabled; a cutover
-- disables the old route with a version bump and inserts a new account route
-- instead of rewriting the old primary-key identity.
ALTER TABLE execution_strategy_bindings
    DROP CONSTRAINT execution_strategy_bindings_model_id_strategy_id_key;

CREATE UNIQUE INDEX execution_strategy_bindings_enabled_model_strategy_uidx
    ON execution_strategy_bindings (model_id, strategy_id)
    WHERE enabled;

COMMIT;
