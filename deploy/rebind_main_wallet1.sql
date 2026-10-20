-- Point main and wallet-1 at their (new) strategy IDs in one transaction.
--
-- Run it with the same two strategy IDs that DECISION_CYCLE_BINDINGS_JSON
-- carries, then restart the service. It changes nothing but
-- execution_strategy_bindings: every currently enabled binding of the two
-- accounts is disabled (version + 1, never deleted) and the target
-- (model, strategy, account) row is enabled or inserted.
--
--   psql "$TRADING_EXECUTION_DATABASE_URL" -v ON_ERROR_STOP=1 \
--     -v main_strategy=alpha_new -v wallet1_strategy=beta_new \
--     -f deploy/rebind_main_wallet1.sql
--
-- Optional variables:
--   model_id=deepseek_masked   logical model; must equal wallet-6's enabled model
--   allow_open_lots=on         proceed even though OPEN lots belong to a binding
--                              that is being retired (they stay in the ledger but
--                              no strategy will manage them any more)
--
-- Lots are owned by the exact model_id + strategy_id, so swap the placeholder
-- names for the formal ones BEFORE the BUY gate is lifted for these accounts.

\if :{?model_id}
\else
  \set model_id deepseek_masked
\endif
\if :{?allow_open_lots}
\else
  \set allow_open_lots off
\endif

BEGIN;

SELECT set_config('rebind.main', :'main_strategy', true),
       set_config('rebind.wallet1', :'wallet1_strategy', true),
       set_config('rebind.model', :'model_id', true),
       set_config('rebind.allow_open_lots', :'allow_open_lots', true);

DO $$
DECLARE
    main_strategy TEXT := current_setting('rebind.main');
    wallet1_strategy TEXT := current_setting('rebind.wallet1');
    target_model TEXT := current_setting('rebind.model');
    fixed_model TEXT;
    orphaned BIGINT;
    target RECORD;
BEGIN
    IF main_strategy !~ '^[A-Za-z][A-Za-z0-9._-]{0,63}$'
       OR wallet1_strategy !~ '^[A-Za-z][A-Za-z0-9._-]{0,63}$' THEN
        RAISE EXCEPTION 'strategy IDs must match ^[A-Za-z][A-Za-z0-9._-]{0,63}$';
    END IF;
    IF lower(main_strategy) IN ('multfactor_v1','multfactor_v2','strategy-v1','strategy-v2','mult-factor-v1','mult-factor-v2')
       OR lower(wallet1_strategy) IN ('multfactor_v1','multfactor_v2','strategy-v1','strategy-v2','mult-factor-v1','mult-factor-v2') THEN
        RAISE EXCEPTION 'main/wallet-1 cannot take the wallet-6/wallet-7 strategies (or their aliases)';
    END IF;
    IF main_strategy = wallet1_strategy THEN
        RAISE EXCEPTION 'main and wallet-1 must use distinct strategy IDs';
    END IF;

    SELECT model_id INTO fixed_model
      FROM execution_strategy_bindings
     WHERE execution_account_id = 'wallet-6' AND enabled;
    IF fixed_model IS DISTINCT FROM target_model THEN
        RAISE EXCEPTION 'model % differs from wallet-6''s enabled model %', target_model, fixed_model;
    END IF;

    SELECT count(*) INTO orphaned
      FROM position_lots l
     WHERE l.execution_account_id IN ('main','wallet-1')
       AND l.status IN ('OPEN','SETTLED_PENDING_REDEEM')
       AND NOT (l.model_id = target_model AND (
              (l.execution_account_id = 'main' AND l.strategy_id = main_strategy)
           OR (l.execution_account_id = 'wallet-1' AND l.strategy_id = wallet1_strategy)));
    IF orphaned > 0 AND current_setting('rebind.allow_open_lots') <> 'on' THEN
        RAISE EXCEPTION '% OPEN lot(s) on main/wallet-1 belong to a binding being retired; close them first or pass -v allow_open_lots=on', orphaned;
    END IF;

    FOR target IN
        SELECT * FROM (VALUES ('main', main_strategy), ('wallet-1', wallet1_strategy)) AS t(account_id, strategy_id)
    LOOP
        PERFORM 1 FROM execution_accounts WHERE execution_account_id = target.account_id FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'execution account % does not exist', target.account_id;
        END IF;

        UPDATE execution_strategy_bindings
           SET enabled = false, version = version + 1
         WHERE execution_account_id = target.account_id
           AND enabled
           AND NOT (model_id = target_model AND strategy_id = target.strategy_id);

        IF EXISTS (SELECT 1 FROM execution_strategy_bindings
                    WHERE model_id = target_model AND strategy_id = target.strategy_id
                      AND execution_account_id = target.account_id) THEN
            UPDATE execution_strategy_bindings
               SET enabled = true, version = version + 1
             WHERE model_id = target_model AND strategy_id = target.strategy_id
               AND execution_account_id = target.account_id AND NOT enabled;
        ELSE
            INSERT INTO execution_strategy_bindings (model_id, strategy_id, execution_account_id, enabled)
            VALUES (target_model, target.strategy_id, target.account_id, true);
        END IF;
    END LOOP;
END
$$;

SELECT model_id, strategy_id, execution_account_id, enabled, version
  FROM execution_strategy_bindings
 WHERE execution_account_id IN ('main','wallet-1')
 ORDER BY execution_account_id, enabled DESC, model_id, strategy_id;

COMMIT;
