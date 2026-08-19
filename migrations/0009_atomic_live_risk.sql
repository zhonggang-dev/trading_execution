BEGIN;

-- Live risk is deliberately armed in the safest state. A production rollout
-- must provision an enabled account policy, an explicit model/strategy/account
-- binding, an unpaused account control, a fresh completed reconciliation and
-- only then turn this singleton kill switch off.
CREATE TABLE execution_risk_global_control (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    kill_switch BOOLEAN NOT NULL DEFAULT TRUE,
    reason TEXT NOT NULL DEFAULT 'LIVE_RISK_NOT_ARMED',
    version BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO execution_risk_global_control (singleton, kill_switch, reason)
VALUES (TRUE, TRUE, 'LIVE_RISK_NOT_ARMED');

-- Durations are stored as integer milliseconds so database/sql does not need
-- driver-specific interval decoding. All limits are required and positive;
-- there is no implicit "unlimited" production value.
CREATE TABLE execution_risk_policies (
    execution_account_id TEXT PRIMARY KEY
        REFERENCES execution_accounts(execution_account_id),
    policy_id TEXT NOT NULL,
    version BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    max_order_notional NUMERIC NOT NULL CHECK (max_order_notional > 0),
    max_market_exposure NUMERIC NOT NULL CHECK (max_market_exposure > 0),
    max_strategy_exposure NUMERIC NOT NULL CHECK (max_strategy_exposure > 0),
    max_wallet_exposure NUMERIC NOT NULL CHECK (max_wallet_exposure > 0),
    max_daily_traded_notional NUMERIC NOT NULL CHECK (max_daily_traded_notional > 0),
    max_price_age_ms BIGINT NOT NULL CHECK (max_price_age_ms > 0),
    max_signal_age_ms BIGINT NOT NULL CHECK (max_signal_age_ms > 0),
    max_state_age_ms BIGINT NOT NULL CHECK (max_state_age_ms > 0),
    daily_timezone TEXT NOT NULL DEFAULT 'UTC' CHECK (daily_timezone <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (policy_id, version)
);

-- ACCOUNT is mandatory for every live account and defaults to paused.
-- STRATEGY and MARKET rows are optional deny overrides: when present and
-- paused they stop that scope without affecting cancellation or reconciliation.
CREATE TABLE execution_risk_controls (
    execution_account_id TEXT NOT NULL
        REFERENCES execution_accounts(execution_account_id),
    control_scope TEXT NOT NULL CHECK (control_scope IN ('ACCOUNT', 'STRATEGY', 'MARKET')),
    control_key TEXT NOT NULL DEFAULT '',
    paused BOOLEAN NOT NULL DEFAULT TRUE,
    reason TEXT NOT NULL DEFAULT 'NOT_ARMED',
    version BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (execution_account_id, control_scope, control_key),
    CONSTRAINT execution_risk_controls_shape CHECK (
        (control_scope = 'ACCOUNT' AND control_key = '')
        OR (control_scope IN ('STRATEGY', 'MARKET') AND control_key <> '')
    )
);

-- The HTTP caller cannot select an arbitrary funded account merely by claiming
-- model_id/strategy_id in OrderIntent. A live reservation requires this exact
-- server-owned binding and it is disabled by default.
CREATE TABLE execution_strategy_bindings (
    model_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    execution_account_id TEXT NOT NULL
        REFERENCES execution_accounts(execution_account_id),
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    version BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (model_id, strategy_id, execution_account_id),
    UNIQUE (model_id, strategy_id),
    CONSTRAINT execution_strategy_bindings_identity CHECK (
        model_id <> '' AND strategy_id <> ''
    )
);

-- These fields are an authorization receipt, not a second cash ledger. Active
-- daily_risk_notional is combined with confirmed fills inside the same account
-- lock transaction, so multiple unfilled orders cannot all pass a daily cap.
ALTER TABLE asset_reservations
    ADD COLUMN risk_policy_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN risk_policy_version BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN risk_day DATE,
    ADD COLUMN daily_risk_notional NUMERIC NOT NULL DEFAULT 0,
    ADD CONSTRAINT asset_reservations_risk_metadata_shape CHECK (
        (risk_policy_id = '' AND risk_policy_version = 0
            AND risk_day IS NULL AND daily_risk_notional = 0)
        OR
        (risk_policy_id <> '' AND risk_policy_version >= 1
            AND risk_day IS NOT NULL AND daily_risk_notional > 0)
    );

CREATE INDEX asset_reservations_active_market_risk_idx
    ON asset_reservations (execution_account_id, market_id)
    WHERE status IN ('ACTIVE', 'RECONCILIATION_REQUIRED');

CREATE INDEX asset_reservations_active_strategy_risk_idx
    ON asset_reservations (execution_account_id, strategy_id)
    WHERE status IN ('ACTIVE', 'RECONCILIATION_REQUIRED');

-- Keep strategy aliases canonical at the storage boundary used by both the Go
-- transaction and the submit-time database trigger.
CREATE FUNCTION execution_canonical_strategy_id(value TEXT)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
STRICT
AS $$
    SELECT CASE lower(btrim(value))
        WHEN 'strategy-v1' THEN 'multfactor_v1'
        WHEN 'mult-factor-v1' THEN 'multfactor_v1'
        WHEN 'strategy-v2' THEN 'multfactor_v2'
        WHEN 'mult-factor-v2' THEN 'multfactor_v2'
        ELSE btrim(value)
    END
$$;

ALTER TABLE execution_strategy_bindings
    ADD CONSTRAINT execution_strategy_bindings_canonical_strategy CHECK (
        strategy_id = execution_canonical_strategy_id(strategy_id)
    );

ALTER TABLE execution_risk_controls
    ADD CONSTRAINT execution_risk_controls_canonical_strategy CHECK (
        control_scope <> 'STRATEGY'
        OR control_key = execution_canonical_strategy_id(control_key)
    );

-- Every operational change is auditable and invalidates reservations that
-- were authorized under an older policy version. Silent in-place changes are
-- rejected at the database boundary.
CREATE FUNCTION enforce_execution_control_version_bump()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.version < OLD.version THEN
        RAISE EXCEPTION 'risk control version must not decrease';
    END IF;
    IF (to_jsonb(NEW) - 'version' - 'updated_at')
       IS DISTINCT FROM (to_jsonb(OLD) - 'version' - 'updated_at')
       AND NEW.version <= OLD.version THEN
        RAISE EXCEPTION 'risk control changes require an increasing version';
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END
$$;

CREATE TRIGGER execution_risk_global_control_version_trigger
BEFORE UPDATE ON execution_risk_global_control
FOR EACH ROW EXECUTE FUNCTION enforce_execution_control_version_bump();

CREATE TRIGGER execution_risk_policies_version_trigger
BEFORE UPDATE ON execution_risk_policies
FOR EACH ROW EXECUTE FUNCTION enforce_execution_control_version_bump();

CREATE TRIGGER execution_risk_controls_version_trigger
BEFORE UPDATE ON execution_risk_controls
FOR EACH ROW EXECUTE FUNCTION enforce_execution_control_version_bump();

CREATE TRIGGER execution_strategy_bindings_version_trigger
BEFORE UPDATE ON execution_strategy_bindings
FOR EACH ROW EXECUTE FUNCTION enforce_execution_control_version_bump();

-- A RESERVED order can be resumed after a process crash without calling
-- Reserve again. This trigger is the final, durable placement gate before the
-- external request: policy/control/binding and reconciliation freshness are
-- checked again when the state machine enters SUBMITTING. Cancel/Get paths do
-- not use this transition and remain available under a kill switch.
CREATE FUNCTION enforce_live_order_submit_risk()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    reservation_policy_id TEXT;
    reservation_policy_version BIGINT;
    global_kill BOOLEAN;
    policy_enabled BOOLEAN;
    price_age_ms BIGINT;
    signal_age_ms BIGINT;
    state_age_ms BIGINT;
    account_paused BOOLEAN;
    scoped_paused BOOLEAN;
    binding_enabled BOOLEAN;
    latest_status TEXT;
    latest_completed_at TIMESTAMPTZ;
    price_at TIMESTAMPTZ;
    signal_at TIMESTAMPTZ;
BEGIN
    IF NEW.status <> 'SUBMITTING'
       OR COALESCE(NEW.market_validation->>'mode', '') <> 'LIVE_CHECK' THEN
        RETURN NEW;
    END IF;

    SELECT risk_policy_id, risk_policy_version
      INTO reservation_policy_id, reservation_policy_version
      FROM asset_reservations
     WHERE order_id = NEW.order_id
     FOR SHARE;
    IF NOT FOUND OR reservation_policy_id = '' OR reservation_policy_version < 1 THEN
        RAISE EXCEPTION 'LIVE_RISK_AUTHORIZATION_MISSING for order %', NEW.order_id;
    END IF;

    SELECT kill_switch INTO global_kill
      FROM execution_risk_global_control
     WHERE singleton = TRUE
     FOR SHARE;
    IF NOT FOUND OR global_kill THEN
        RAISE EXCEPTION 'GLOBAL_KILL_SWITCH blocks order %', NEW.order_id;
    END IF;

    SELECT enabled, max_price_age_ms, max_signal_age_ms, max_state_age_ms
      INTO policy_enabled, price_age_ms, signal_age_ms, state_age_ms
      FROM execution_risk_policies
     WHERE execution_account_id = NEW.execution_account_id
       AND policy_id = reservation_policy_id
       AND version = reservation_policy_version
     FOR SHARE;
    IF NOT FOUND OR NOT policy_enabled THEN
        RAISE EXCEPTION 'RISK_POLICY_DISABLED_OR_CHANGED for order %', NEW.order_id;
    END IF;

    SELECT paused INTO account_paused
      FROM execution_risk_controls
     WHERE execution_account_id = NEW.execution_account_id
       AND control_scope = 'ACCOUNT' AND control_key = ''
     FOR SHARE;
    IF NOT FOUND OR account_paused THEN
        RAISE EXCEPTION 'EXECUTION_ACCOUNT_PAUSED blocks order %', NEW.order_id;
    END IF;

    SELECT enabled INTO binding_enabled
      FROM execution_strategy_bindings
     WHERE model_id = btrim(NEW.intent->>'model_id')
       AND strategy_id = execution_canonical_strategy_id(NEW.intent->>'strategy_id')
       AND execution_account_id = NEW.execution_account_id
     FOR SHARE;
    IF NOT FOUND OR NOT binding_enabled THEN
        RAISE EXCEPTION 'STRATEGY_ACCOUNT_BINDING_DENIED for order %', NEW.order_id;
    END IF;

    SELECT TRUE INTO scoped_paused
      FROM execution_risk_controls
     WHERE execution_account_id = NEW.execution_account_id
       AND paused = TRUE
       AND ((control_scope = 'STRATEGY'
             AND control_key = execution_canonical_strategy_id(NEW.intent->>'strategy_id'))
         OR (control_scope = 'MARKET'
             AND control_key = btrim(NEW.intent->>'market_id')))
     LIMIT 1
     FOR SHARE;
    IF COALESCE(scoped_paused, FALSE) THEN
        RAISE EXCEPTION 'STRATEGY_OR_MARKET_PAUSED blocks order %', NEW.order_id;
    END IF;

    price_at := NULLIF(NEW.intent->>'market_snapshot_at', '')::timestamptz;
    signal_at := NULLIF(NEW.intent->>'signal_at', '')::timestamptz;
    IF price_at IS NULL
       OR price_at > clock_timestamp() + INTERVAL '2 seconds'
       OR clock_timestamp() - price_at
          > price_age_ms * INTERVAL '1 millisecond' THEN
        RAISE EXCEPTION 'PRICE_STALE blocks order %', NEW.order_id;
    END IF;
    IF signal_at IS NULL
       OR signal_at > clock_timestamp() + INTERVAL '2 seconds'
       OR clock_timestamp() - signal_at
          > signal_age_ms * INTERVAL '1 millisecond' THEN
        RAISE EXCEPTION 'SIGNAL_STALE blocks order %', NEW.order_id;
    END IF;

    SELECT status, completed_at
      INTO latest_status, latest_completed_at
      FROM reconciliation_runs
     WHERE execution_account_id = NEW.execution_account_id
     ORDER BY started_at DESC, run_id DESC
     LIMIT 1
     FOR SHARE;
    IF NOT FOUND OR latest_status <> 'COMPLETED' OR latest_completed_at IS NULL
       OR latest_completed_at > clock_timestamp() + INTERVAL '2 seconds'
       OR clock_timestamp() - latest_completed_at
          > state_age_ms * INTERVAL '1 millisecond' THEN
        RAISE EXCEPTION 'RISK_STATE_STALE blocks order %', NEW.order_id;
    END IF;

    IF EXISTS (
        SELECT 1 FROM reconciliation_issues
         WHERE execution_account_id = NEW.execution_account_id
           AND status = 'OPEN'
    ) THEN
        RAISE EXCEPTION 'RISK_STATE_HAS_OPEN_ISSUES blocks order %', NEW.order_id;
    END IF;

    RETURN NEW;
END
$$;

CREATE TRIGGER execution_orders_live_submit_risk_trigger
BEFORE UPDATE ON execution_orders
FOR EACH ROW
WHEN (NEW.status = 'SUBMITTING')
EXECUTE FUNCTION enforce_live_order_submit_risk();

COMMIT;
