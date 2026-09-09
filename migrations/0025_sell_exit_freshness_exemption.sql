BEGIN;

-- Strategy SELL exits are no longer gated by internal freshness windows.
--
-- The durable submit trigger repeated the Go-side live risk authorization so a
-- crash-resume could not bypass it. It rejected every LIVE_CHECK order whose
-- validated book capture (PRICE_STALE) or strategy decision (SIGNAL_STALE) was
-- older than the account policy window. For a SELL exit those windows only
-- measured how long the order waited inside this system; the venue still
-- executes the sell at or above the strategy worst_price or leaves it unfilled.
-- The Go authorization in internal/adapter/postgres/live_risk.go now applies
-- the two freshness gates to BUY entries only, and this trigger mirrors it.
--
-- Everything else stays mandatory for both sides: the live risk authorization
-- on the reservation, the global kill switch, the enabled account policy, the
-- account/strategy/market pause controls, the strategy-account binding, the
-- completed-reconciliation freshness window and the absence of OPEN
-- reconciliation issues.
CREATE OR REPLACE FUNCTION enforce_live_order_submit_risk()
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

    -- Price and signal freshness gate BUY entries only. A SELL exit is the
    -- strategy's instruction to sell and is submitted whenever the account
    -- gates above and the reconciliation state below allow it.
    IF btrim(NEW.intent->>'side') <> 'SELL' THEN
        price_at := NULLIF(NEW.market_validation->>'latest_book_observed_at', '')::timestamptz;
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
    END IF;

    SELECT completed_at
      INTO latest_completed_at
      FROM reconciliation_runs
     WHERE execution_account_id = NEW.execution_account_id
       AND status = 'COMPLETED'
       AND completed_at IS NOT NULL
     ORDER BY completed_at DESC, run_id DESC
     LIMIT 1
     FOR SHARE;
    IF NOT FOUND OR latest_completed_at IS NULL
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

COMMIT;
