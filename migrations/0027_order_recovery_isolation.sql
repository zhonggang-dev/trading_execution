BEGIN;

-- One UNKNOWN order must not stop every other market of the same account.
--
-- 1. order_recovery_leases serializes recovery work on a single order between
--    the fast order coordinator and the scheduled reconciliation, and keeps the
--    per-order retry schedule (attempts, backoff, escalation) durable.
-- 2. reconciliation_issues.impact_scope persists which trading an OPEN issue
--    blocks: ACCOUNT (everything), ORDER/TOKEN (the same token, condition or
--    market only), NONE (observation only). The Go service classifies new
--    issues; existing OPEN rows are backfilled with the same rule.
-- 3. The live submit trigger now blocks only account-wide issues or scoped
--    issues that share the order's token/condition/market, and it measures
--    risk-state freshness from the latest *completed scan* (COMPLETED or
--    ATTENTION_REQUIRED). A scan that finished with scoped problems is still a
--    fresh view of the account; the scoped issue gate handles the problems.
--
-- Nothing here releases a reservation or changes an order status.

CREATE TABLE order_recovery_leases (
    order_id TEXT PRIMARY KEY REFERENCES execution_orders(order_id),
    execution_account_id TEXT NOT NULL REFERENCES execution_accounts(execution_account_id),
    holder TEXT NOT NULL DEFAULT '',
    order_revision BIGINT NOT NULL DEFAULT 0,
    acquired_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    first_pending_at TIMESTAMPTZ,
    last_attempt_at TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    escalated_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT order_recovery_leases_holder_shape CHECK (
        (holder = '' AND acquired_at IS NULL AND expires_at IS NULL)
        OR (holder <> '' AND acquired_at IS NOT NULL AND expires_at IS NOT NULL)
    )
);

CREATE INDEX order_recovery_leases_account_retry_idx
    ON order_recovery_leases (execution_account_id, next_retry_at);

CREATE INDEX order_recovery_leases_escalated_idx
    ON order_recovery_leases (execution_account_id, escalated_at)
    WHERE escalated_at IS NOT NULL;

ALTER TABLE reconciliation_issues
    ADD COLUMN impact_scope TEXT NOT NULL DEFAULT 'ACCOUNT'
        CHECK (impact_scope IN ('NONE', 'ORDER', 'TOKEN', 'ACCOUNT'));

-- Mirror of domain.ClassifyReconciliationImpact for rows recorded before the
-- column existed. RESOLVED rows block nothing regardless of the value.
UPDATE reconciliation_issues
SET impact_scope = CASE
    WHEN resolution = 'OBSERVED_ONLY' THEN 'NONE'
    WHEN issue_type = 'BALANCE_DRIFT' AND resolution = 'RETRY_LATER' THEN 'NONE'
    WHEN issue_type IN ('BALANCE_DRIFT', 'SOURCE_CONFLICT') THEN 'ACCOUNT'
    WHEN issue_type IN ('SOURCE_UNAVAILABLE', 'SUBMIT_UNCONFIRMED', 'FILL_FINALITY_STALLED',
                        'LOCAL_ORDER_CANCELLED', 'MISSED_BUY_FILL', 'MISSED_SELL_FILL',
                        'EXTERNAL_TRADE', 'ORDER_RECOVERY_PENDING', 'ORDER_RECOVERY_STALLED') THEN
        CASE
            WHEN token_id = '' AND condition_id = '' AND market_id = '' THEN 'ACCOUNT'
            WHEN order_id <> '' THEN 'ORDER'
            ELSE 'TOKEN'
        END
    WHEN issue_type IN ('POSITION_DRIFT', 'PHANTOM_POSITION',
                        'EXTERNAL_POSITION_BASELINE_DRIFT', 'POSITION_SETTLED') THEN
        CASE
            WHEN token_id = '' AND condition_id = '' AND market_id = '' THEN 'ACCOUNT'
            ELSE 'TOKEN'
        END
    ELSE 'ACCOUNT'
END
WHERE status = 'OPEN';

CREATE INDEX reconciliation_issues_open_scope_idx
    ON reconciliation_issues (execution_account_id, impact_scope, token_id, condition_id, market_id)
    WHERE status = 'OPEN';

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
    order_condition_id TEXT;
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

    -- Freshness comes from the latest finished scan. ATTENTION_REQUIRED is a
    -- finished scan whose problems are represented by OPEN issues below;
    -- RUNNING and FAILED runs never count.
    SELECT completed_at
      INTO latest_completed_at
      FROM reconciliation_runs
     WHERE execution_account_id = NEW.execution_account_id
       AND status IN ('COMPLETED', 'ATTENTION_REQUIRED')
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

    order_condition_id := btrim(COALESCE(NEW.intent->>'condition_id', ''));
    IF EXISTS (
        SELECT 1 FROM reconciliation_issues issue
         WHERE issue.execution_account_id = NEW.execution_account_id
           AND issue.status = 'OPEN'
           AND (
                issue.impact_scope = 'ACCOUNT'
             OR (issue.impact_scope IN ('ORDER', 'TOKEN') AND (
                    (issue.token_id <> '' AND issue.token_id = NEW.token_id)
                 OR (issue.condition_id <> '' AND order_condition_id <> ''
                     AND lower(issue.condition_id) = lower(order_condition_id))
                 OR (issue.market_id <> '' AND issue.market_id = NEW.market_id)))
           )
    ) THEN
        RAISE EXCEPTION 'RISK_STATE_HAS_OPEN_ISSUES blocks order %', NEW.order_id;
    END IF;

    RETURN NEW;
END
$$;

COMMIT;
