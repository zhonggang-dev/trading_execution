BEGIN;

-- A wallet-reuse cutover is an accounting boundary, not a trade, fill,
-- adoption, or redemption.  It records the exact finalized asset snapshot and
-- (when the old ledger is higher than the chain) one explicit debit.  Existing
-- positions, lots, issues, fills and disabled bindings remain untouched.
CREATE TABLE execution_wallet_cutover_batches (
    batch_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    evidence_block_number BIGINT NOT NULL,
    evidence_block_hash TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    evidence JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT execution_wallet_cutover_batches_identity CHECK (
        btrim(batch_id) <> ''
        AND schema_version = 'trading.wallet_reuse_cutover.v1'
        AND manifest_sha256 ~ '^[0-9a-f]{64}$'
        AND evidence_block_number > 0
        AND evidence_block_hash ~ '^0x[0-9a-f]{64}$'
        AND expires_at > observed_at
        AND btrim(actor) <> ''
        AND btrim(reason) <> ''
    ),
    CONSTRAINT execution_wallet_cutover_batches_evidence_object CHECK (
        jsonb_typeof(evidence) = 'object' AND evidence <> '{}'::jsonb
    )
);

CREATE TABLE execution_wallet_cutover_accounts (
    batch_id TEXT NOT NULL REFERENCES execution_wallet_cutover_batches(batch_id) ON DELETE RESTRICT,
    execution_account_id TEXT NOT NULL REFERENCES execution_accounts(execution_account_id) ON DELETE RESTRICT,
    wallet_address TEXT NOT NULL,
    expected_version BIGINT NOT NULL,
    ledger_before NUMERIC NOT NULL,
    available_before NUMERIC NOT NULL,
    reserved_before NUMERIC NOT NULL,
    chain_balance NUMERIC NOT NULL,
    adjustment_delta NUMERIC NOT NULL,
    ledger_after NUMERIC NOT NULL,
    account_event_id TEXT NOT NULL UNIQUE REFERENCES execution_account_events(account_event_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    evidence JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (batch_id, execution_account_id),
    CONSTRAINT execution_wallet_cutover_accounts_wallet_shape CHECK (
        wallet_address ~ '^0x[0-9a-f]{40}$'
    ),
    CONSTRAINT execution_wallet_cutover_accounts_version_shape CHECK (expected_version > 0),
    CONSTRAINT execution_wallet_cutover_accounts_balance_shape CHECK (
        ledger_before >= 0 AND available_before >= 0 AND reserved_before = 0
        AND chain_balance >= 0 AND ledger_after >= 0
        AND adjustment_delta = ledger_after - ledger_before
        AND ledger_after = chain_balance
        AND available_before = ledger_before
        AND adjustment_delta <= 0
    ),
    CONSTRAINT execution_wallet_cutover_accounts_evidence_object CHECK (
        jsonb_typeof(evidence) = 'object' AND evidence <> '{}'::jsonb
    )
);

-- One row is retained for every token in the selected snapshot.  The row is
-- evidence only; OLD_MANAGED is kept with its old lot, while RETAINED_OLD and
-- EXTERNAL are exposed to the reconciliation baseline read model by the Go
-- adapter.  No row can be used as a reservation or strategy lot.
CREATE TABLE execution_wallet_cutover_assets (
    batch_id TEXT NOT NULL,
    execution_account_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    outcome_index SMALLINT,
    outcome_name TEXT NOT NULL,
    neg_risk BOOLEAN NOT NULL,
    chain_shares NUMERIC NOT NULL,
    managed_shares NUMERIC NOT NULL,
    managed_cost NUMERIC NOT NULL DEFAULT 0,
    after_cost NUMERIC NOT NULL DEFAULT 0,
    ownership TEXT NOT NULL CHECK (ownership IN ('OLD_MANAGED','RETAINED_OLD','EXTERNAL')),
    residual_reason TEXT NOT NULL DEFAULT '',
    evidence JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (batch_id, execution_account_id, token_id),
    FOREIGN KEY (batch_id, execution_account_id)
        REFERENCES execution_wallet_cutover_accounts(batch_id, execution_account_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT execution_wallet_cutover_assets_identity CHECK (
        token_id ~ '^[0-9]+$' AND btrim(condition_id) <> ''
        AND btrim(market_id) <> '' AND btrim(outcome_name) <> ''
        AND chain_shares >= 0 AND managed_shares >= 0
        AND (ownership = 'OLD_MANAGED' OR managed_shares <= chain_shares)
        AND managed_cost >= 0
        AND after_cost >= 0
        AND (outcome_index IS NULL OR outcome_index IN (0,1))
        AND (ownership = 'OLD_MANAGED' OR btrim(residual_reason) <> '')
    ),
    CONSTRAINT execution_wallet_cutover_assets_evidence_object CHECK (
        jsonb_typeof(evidence) = 'object' AND evidence <> '{}'::jsonb
    )
);

CREATE TABLE execution_wallet_cutover_lot_adjustments (
    batch_id TEXT NOT NULL,
    execution_account_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    lot_id TEXT NOT NULL REFERENCES position_lots(lot_id) ON DELETE RESTRICT,
    before_shares NUMERIC NOT NULL,
    before_cost NUMERIC NOT NULL,
    after_shares NUMERIC NOT NULL,
    after_cost NUMERIC NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (batch_id, lot_id),
    FOREIGN KEY (batch_id, execution_account_id, token_id)
        REFERENCES execution_wallet_cutover_assets(batch_id, execution_account_id, token_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT execution_wallet_cutover_lot_adjustments_shape CHECK (
        before_shares > 0 AND before_cost >= 0
        AND after_shares >= 0 AND after_cost >= 0
        AND after_shares <= before_shares AND after_cost <= before_cost
        AND btrim(reason) <> ''
    )
);

CREATE INDEX execution_wallet_cutover_assets_retained_idx
    ON execution_wallet_cutover_assets (execution_account_id, token_id)
    WHERE ownership IN ('RETAINED_OLD','EXTERNAL');

CREATE FUNCTION execution_reject_wallet_cutover_mutation()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'wallet reuse cutover evidence is append-only';
END;
$$;

CREATE TRIGGER execution_wallet_cutover_batches_append_only
BEFORE UPDATE OR DELETE OR TRUNCATE ON execution_wallet_cutover_batches
FOR EACH STATEMENT EXECUTE FUNCTION execution_reject_wallet_cutover_mutation();
CREATE TRIGGER execution_wallet_cutover_accounts_append_only
BEFORE UPDATE OR DELETE OR TRUNCATE ON execution_wallet_cutover_accounts
FOR EACH STATEMENT EXECUTE FUNCTION execution_reject_wallet_cutover_mutation();
CREATE TRIGGER execution_wallet_cutover_assets_append_only
BEFORE UPDATE OR DELETE OR TRUNCATE ON execution_wallet_cutover_assets
FOR EACH STATEMENT EXECUTE FUNCTION execution_reject_wallet_cutover_mutation();
CREATE TRIGGER execution_wallet_cutover_lot_adjustments_append_only
BEFORE UPDATE OR DELETE OR TRUNCATE ON execution_wallet_cutover_lot_adjustments
FOR EACH STATEMENT EXECUTE FUNCTION execution_reject_wallet_cutover_mutation();

-- A cutover reduction is evidence-only. It must never enter the normal SOLD,
-- REDEEMED or realized-PnL streams consumed by trade history.
ALTER TABLE position_events
    DROP CONSTRAINT IF EXISTS position_events_event_type_check,
    ADD CONSTRAINT position_events_event_type_check CHECK (
        event_type IN ('BOUGHT','ADOPTED','SOLD','MARKED','SETTLED','REDEEMED','CUTOVER_ADJUSTED')
    );

CREATE FUNCTION execution_wallet_cutover_position_event_guard()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.event_type='CUTOVER_ADJUSTED'
       OR (TG_OP = 'UPDATE' AND NEW.event_type='CUTOVER_ADJUSTED') THEN
        RAISE EXCEPTION 'wallet cutover position events are append-only';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER execution_wallet_cutover_position_event_guard
BEFORE UPDATE OR DELETE ON position_events
FOR EACH ROW EXECUTE FUNCTION execution_wallet_cutover_position_event_guard();

-- The running reconciliation read model needs residual cutover evidence, while
-- ordinary application connections must not be able to append or mutate it.
-- Do not guess the deployment's runtime role here.  The rollout grants SELECT
-- explicitly to the named read role; PUBLIC receives no audit-table access.
REVOKE ALL ON execution_wallet_cutover_batches,
    execution_wallet_cutover_accounts,
    execution_wallet_cutover_assets,
    execution_wallet_cutover_lot_adjustments FROM PUBLIC;

CREATE FUNCTION execution_apply_wallet_reuse_cutover(
    payload JSONB,
    batch TEXT,
    manifest_hash TEXT,
    operator_name TEXT,
    cutover_reason TEXT
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    manifest JSONB := payload->'manifest';
    block JSONB := manifest->'evidence_block';
    account JSONB;
    asset JSONB;
    account_id TEXT;
    wallet TEXT;
    account_event TEXT;
    expected_version BIGINT;
    old_total NUMERIC;
    old_available NUMERIC;
    old_reserved NUMERIC;
    chain_total NUMERIC;
    new_total NUMERIC;
    delta NUMERIC;
    after_total NUMERIC;
    after_cost NUMERIC;
    position_before_shares NUMERIC;
    position_before_cost NUMERIC;
    owner_model TEXT;
    owner_strategy TEXT;
    owner_count INTEGER;
    lot RECORD;
    lot_adjustment JSONB;
    position_event TEXT;
    issue_id_value TEXT;
    issue_status TEXT;
    issue_type TEXT;
    issue_token TEXT;
    resolved_issue_ids TEXT[] := ARRAY[]::TEXT[];
    account_count INTEGER := 0;
    asset_count INTEGER := 0;
    existing_sha TEXT;
    result JSONB;
BEGIN
    -- The function is revoked from PUBLIC below.  Only the database owner or
    -- an explicitly granted operator role can invoke this SECURITY DEFINER
    -- boundary; ordinary application roles cannot call it or write these rows.
    IF session_user <> current_user
       AND (NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trading_execution_cutover_operator')
            OR NOT pg_has_role(session_user, 'trading_execution_cutover_operator', 'member')) THEN
        RAISE EXCEPTION 'wallet cutover requires the controlled operator role';
    END IF;
    IF btrim(batch) = '' OR btrim(manifest_hash) !~ '^[0-9a-f]{64}$'
       OR btrim(operator_name) = '' OR btrim(cutover_reason) = '' THEN
        RAISE EXCEPTION 'wallet cutover identity is incomplete';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(batch, 160035));
    IF jsonb_typeof(manifest) IS DISTINCT FROM 'object'
       OR manifest->>'schema_version' <> 'trading.wallet_reuse_cutover.v1'
       OR manifest->>'batch_id' <> batch
       OR manifest->>'actor' <> operator_name
       OR manifest->>'reason' <> cutover_reason
       OR jsonb_typeof(manifest->'accounts') IS DISTINCT FROM 'array'
       OR jsonb_array_length(manifest->'accounts') <> 2 THEN
        RAISE EXCEPTION 'wallet cutover manifest shape is invalid';
    END IF;

    SELECT b.manifest_sha256 INTO existing_sha
      FROM execution_wallet_cutover_batches b WHERE b.batch_id = batch;
    IF existing_sha IS NOT NULL THEN
        IF existing_sha <> manifest_hash THEN
            RAISE EXCEPTION 'wallet cutover batch already exists with a different manifest';
        END IF;
        RETURN jsonb_build_object('already_applied', true, 'batch_id', batch,
            'manifest_sha256', manifest_hash);
    END IF;

    IF block->>'chain_id' <> '137'
       OR (block->>'number')::bigint <= 0
       OR block->>'hash' !~ '^0x[0-9a-f]{64}$'
       OR (block->>'confirmations')::bigint <= 0 THEN
        RAISE EXCEPTION 'wallet cutover evidence block is not finalized Polygon evidence';
    END IF;
    IF (manifest->>'expires_at')::timestamptz <= clock_timestamp()
       OR (manifest->>'expires_at')::timestamptz <= (manifest->>'observed_at')::timestamptz THEN
        RAISE EXCEPTION 'wallet cutover manifest is expired or has invalid validity';
    END IF;

    -- Every non-zero local token must be explicitly represented in the
    -- reviewed snapshot. This includes token id 0 and effective residuals
    -- carried by an older immutable baseline or cutover batch; omission must
    -- never make an old baseline disappear from the read model.
    IF EXISTS (
        WITH required AS (
            SELECT p.execution_account_id, p.token_id
              FROM execution_positions p
             WHERE p.execution_account_id IN ('main','wallet-1')
               AND p.total_shares > 0
            UNION
            SELECT l.execution_account_id, l.token_id
              FROM position_lots l
             WHERE l.execution_account_id IN ('main','wallet-1')
               AND l.status IN ('OPEN','SETTLED_PENDING_REDEEM')
               AND l.remaining_shares > 0
            UNION
            SELECT item.execution_account_id, item.token_id
              FROM execution_external_position_baseline_items item
              LEFT JOIN execution_external_position_dispositions disposition
                ON disposition.baseline_id=item.baseline_id
               AND disposition.execution_account_id=item.execution_account_id
               AND disposition.token_id=item.token_id
               AND disposition.disposition_kind IN ('EXTERNAL_SELL','ADOPTION')
             WHERE item.execution_account_id IN ('main','wallet-1')
             GROUP BY item.execution_account_id, item.token_id, item.shares
            HAVING item.shares - COALESCE(sum(disposition.shares_delta),0) > 0
            UNION
            SELECT latest.execution_account_id, latest.token_id
              FROM (
                    SELECT DISTINCT ON (a.execution_account_id, a.token_id)
                           a.execution_account_id, a.token_id,
                           a.chain_shares, a.managed_shares
                      FROM execution_wallet_cutover_assets a
                      JOIN execution_wallet_cutover_batches b USING (batch_id)
                     WHERE a.execution_account_id IN ('main','wallet-1')
                     ORDER BY a.execution_account_id, a.token_id, b.created_at DESC, a.batch_id DESC
                   ) latest
             WHERE latest.chain_shares > latest.managed_shares
        )
        SELECT 1
          FROM required r
         WHERE NOT EXISTS (
             SELECT 1
               FROM jsonb_array_elements(manifest->'accounts') account_value
               CROSS JOIN LATERAL jsonb_array_elements(COALESCE(account_value->'assets','[]'::jsonb)) asset_value
              WHERE account_value->>'execution_account_id'=r.execution_account_id
                AND asset_value->>'token_id'=r.token_id
         )
    ) THEN
        RAISE EXCEPTION 'wallet cutover manifest omits a non-zero local asset';
    END IF;

    INSERT INTO execution_wallet_cutover_batches(
        batch_id, schema_version, manifest_sha256, evidence_block_number,
        evidence_block_hash, observed_at, expires_at, actor, reason, evidence
    ) VALUES (
        batch, manifest->>'schema_version', manifest_hash,
        (block->>'number')::bigint, block->>'hash',
        (manifest->>'observed_at')::timestamptz, (manifest->>'expires_at')::timestamptz,
        operator_name, cutover_reason,
        jsonb_build_object('manifest', manifest, 'chain_report', payload->'chain_report')
    );

    -- Stable account lock order prevents deadlocks when an operator retries a
    -- two-wallet batch while an account-scoped reconciliation is finishing.
    FOR account IN
        SELECT value FROM jsonb_array_elements(manifest->'accounts') value
        ORDER BY value->>'execution_account_id'
    LOOP
        account_id := btrim(account->>'execution_account_id');
        IF account_id NOT IN ('main','wallet-1') THEN
            RAISE EXCEPTION 'wallet cutover account % is outside allowlist', account_id;
        END IF;
        SELECT lower(a.wallet_address), a.version, a.total_balance,
               a.available_balance, a.reserved_balance
          INTO wallet, expected_version, old_total, old_available, old_reserved
          FROM execution_accounts a
         WHERE a.execution_account_id = account_id
         FOR UPDATE;
        IF NOT FOUND THEN RAISE EXCEPTION 'wallet cutover account % does not exist', account_id; END IF;
        IF lower(wallet) <> lower(account->>'wallet_address')
           OR expected_version <> (account->>'before_version')::bigint
           OR old_total IS DISTINCT FROM (account->'ledger'->>'total_balance')::numeric
           OR old_available IS DISTINCT FROM (account->'ledger'->>'available_balance')::numeric
           OR old_reserved IS DISTINCT FROM (account->'ledger'->>'reserved_balance')::numeric
           OR old_reserved <> 0 THEN
            RAISE EXCEPTION 'wallet cutover account % failed version or ledger CAS', account_id;
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM execution_risk_controls c
             WHERE c.execution_account_id=account_id
               AND c.control_scope='ACCOUNT' AND c.control_key='' AND c.paused
        ) THEN RAISE EXCEPTION 'wallet cutover account % must be paused', account_id; END IF;
        IF EXISTS (SELECT 1 FROM reconciliation_runs r WHERE r.execution_account_id=account_id AND r.status='RUNNING')
           OR EXISTS (SELECT 1 FROM execution_orders o WHERE o.execution_account_id=account_id
                     AND o.status NOT IN ('FILLED','REJECTED','CANCELLED','EXPIRED','MANUAL_REVIEW'))
           OR EXISTS (SELECT 1 FROM asset_reservations r WHERE r.execution_account_id=account_id
                     AND r.status IN ('ACTIVE','RECONCILIATION_REQUIRED'))
           OR EXISTS (SELECT 1 FROM execution_fills f WHERE f.execution_account_id=account_id
                     AND f.applied_at IS NULL AND f.status <> 'FAILED')
           OR EXISTS (SELECT 1 FROM strategy_order_intent_deliveries d
                     WHERE d.status IN ('PENDING','SUBMITTING')
                       AND d.intent_payload->>'execution_account_id'=account_id)
           OR EXISTS (SELECT 1 FROM polymarket_redemptions r WHERE r.execution_account_id=account_id
                     AND r.status NOT IN ('APPLIED','MANUAL_REVIEW')) THEN
            RAISE EXCEPTION 'wallet cutover account % is not quiescent', account_id;
        END IF;

        -- Only issue ids explicitly reviewed in this account's manifest may
        -- be closed. No account-wide sweep is allowed, and token-scoped issues
        -- must be covered by the same asset snapshot. The original issue row
        -- and details remain intact; the appended batch reference is the audit
        -- link for the manual resolution.
        FOR issue_id_value IN
            SELECT value FROM jsonb_array_elements_text(COALESCE(account->'open_issue_ids','[]'::jsonb)) value
        LOOP
            IF btrim(issue_id_value) = '' OR issue_id_value = ANY(resolved_issue_ids) THEN
                RAISE EXCEPTION 'wallet cutover issue id is empty or duplicated for account %', account_id;
            END IF;
            SELECT i.status, i.issue_type, i.token_id
              INTO issue_status, issue_type, issue_token
              FROM reconciliation_issues i
             WHERE i.issue_id=issue_id_value
               AND i.execution_account_id=account_id
             FOR UPDATE;
            IF NOT FOUND OR issue_status <> 'OPEN' THEN
                RAISE EXCEPTION 'wallet cutover issue % is not an open issue owned by account %', issue_id_value, account_id;
            END IF;
            IF issue_type NOT IN ('BALANCE_DRIFT','PHANTOM_POSITION','POSITION_DRIFT',
                                  'EXTERNAL_POSITION_BASELINE_DRIFT','EXTERNAL_TRADE') THEN
                RAISE EXCEPTION 'wallet cutover issue % has unsupported type %', issue_id_value, issue_type;
            END IF;
            IF btrim(COALESCE(issue_token,'')) <> '' AND NOT EXISTS (
                SELECT 1
                  FROM jsonb_array_elements(COALESCE(account->'assets','[]'::jsonb)) asset_value
                 WHERE asset_value->>'token_id'=issue_token
            ) THEN
                RAISE EXCEPTION 'wallet cutover issue % token % is outside the reviewed asset snapshot', issue_id_value, issue_token;
            END IF;
            UPDATE reconciliation_issues AS issue_row
               SET status='RESOLVED', resolution='MANUAL_REVIEW',
                   resolved_at=GREATEST((manifest->>'observed_at')::timestamptz, issue_row.observed_at),
                   details=details || '; manually resolved by wallet cutover batch ' || batch
             WHERE issue_row.issue_id=issue_id_value
               AND issue_row.execution_account_id=account_id
               AND issue_row.status='OPEN';
            IF NOT FOUND THEN
                RAISE EXCEPTION 'wallet cutover issue % changed during apply', issue_id_value;
            END IF;
            resolved_issue_ids := array_append(resolved_issue_ids, issue_id_value);
        END LOOP;

        chain_total := (account->>'chain_balance')::numeric;
        new_total := chain_total;
        delta := new_total - old_total;
        IF chain_total < 0 OR delta > 0 OR new_total < 0 THEN
            RAISE EXCEPTION 'wallet cutover account % requires a non-positive debit to a non-negative balance', account_id;
        END IF;
        account_event := 'wallet-cutover:' || batch || ':' || account_id;
        INSERT INTO execution_account_events(
            account_event_id, execution_account_id, event_type,
            total_balance_delta, available_balance_delta, reserved_balance_delta,
            total_balance_after, available_balance_after, reserved_balance_after, occurred_at
        ) VALUES (
            account_event, account_id, 'CUTOVER_ADJUSTMENT', delta, delta, 0,
            new_total, new_total, 0, (manifest->>'observed_at')::timestamptz
        );
        UPDATE execution_accounts
           SET total_balance=new_total, available_balance=new_total,
               reserved_balance=0, version=version+1,
               reconciled_at=(manifest->>'observed_at')::timestamptz,
               updated_at=clock_timestamp()
         WHERE execution_account_id=account_id;
        INSERT INTO execution_wallet_cutover_accounts(
            batch_id, execution_account_id, wallet_address, expected_version,
            ledger_before, available_before, reserved_before, chain_balance,
            adjustment_delta, ledger_after, account_event_id, evidence
        ) VALUES (
            batch, account_id, wallet, expected_version, old_total, old_available,
            old_reserved, chain_total, delta, new_total, account_event,
            jsonb_build_object('manifest_account', account, 'chain_report', payload->'chain_report')
        );

        FOR asset IN SELECT value FROM jsonb_array_elements(COALESCE(account->'assets','[]'::jsonb)) value LOOP
            INSERT INTO execution_wallet_cutover_assets(
                batch_id, execution_account_id, token_id, condition_id, market_id,
                outcome_index, outcome_name, neg_risk, chain_shares, managed_shares,
                managed_cost, after_cost, ownership, residual_reason, evidence
            ) VALUES (
                batch, account_id, asset->>'token_id', asset->>'condition_id', asset->>'market_id',
                NULLIF(asset->>'outcome_index','')::smallint, asset->>'outcome_name',
                COALESCE((asset->>'neg_risk')::boolean,false),
                (asset->>'chain_shares')::numeric, (asset->>'managed_shares')::numeric,
                (asset->>'managed_cost')::numeric, (asset->>'after_cost')::numeric,
                asset->>'ownership', COALESCE(asset->>'residual_reason',''), asset
            );

            IF asset->>'ownership' = 'OLD_MANAGED' THEN
                SELECT p.total_shares, p.cost_basis, p.market_id
                  INTO position_before_shares, position_before_cost
                  FROM execution_positions p
                 WHERE p.execution_account_id=account_id AND p.token_id=asset->>'token_id'
                 FOR UPDATE;
                IF NOT FOUND THEN
                    IF (asset->>'managed_shares')::numeric <> 0 OR (asset->>'chain_shares')::numeric <> 0 THEN
                        RAISE EXCEPTION 'OLD_MANAGED asset % has no local position', asset->>'token_id';
                    END IF;
                ELSE
                    IF position_before_shares IS DISTINCT FROM (asset->>'managed_shares')::numeric
                       OR position_before_cost IS DISTINCT FROM (asset->>'managed_cost')::numeric THEN
                        RAISE EXCEPTION 'OLD_MANAGED asset % does not match local position', asset->>'token_id';
                    END IF;
                    IF EXISTS (SELECT 1 FROM execution_positions p
                                WHERE p.execution_account_id=account_id AND p.token_id=asset->>'token_id'
                                  AND p.reserved_shares <> 0) THEN
                        RAISE EXCEPTION 'OLD_MANAGED asset % has reserved shares', asset->>'token_id';
                    END IF;
                END IF;
                SELECT min(l.model_id), min(l.strategy_id), count(DISTINCT (l.model_id,l.strategy_id))
                  INTO owner_model, owner_strategy, owner_count
                  FROM position_lots l
                 WHERE l.execution_account_id=account_id AND l.token_id=asset->>'token_id'
                   AND l.status IN ('OPEN','SETTLED_PENDING_REDEEM');
                IF owner_count > 1 THEN
                    RAISE EXCEPTION 'OLD_MANAGED asset % has mixed strategy ownership', asset->>'token_id';
                END IF;
                FOR lot_adjustment IN SELECT value FROM jsonb_array_elements(COALESCE(asset->'lot_adjustments','[]'::jsonb)) value LOOP
                    SELECT l.* INTO lot FROM position_lots l
                     WHERE l.lot_id=lot_adjustment->>'lot_id'
                     FOR UPDATE;
                    IF NOT FOUND OR lot.execution_account_id<>account_id OR lot.token_id<>asset->>'token_id'
                       OR lot.status <> 'OPEN'
                       OR lot.remaining_shares IS DISTINCT FROM (lot_adjustment->>'before_shares')::numeric
                       OR lot.remaining_cost IS DISTINCT FROM (lot_adjustment->>'before_cost')::numeric THEN
                        RAISE EXCEPTION 'lot adjustment % failed identity or version check', lot_adjustment->>'lot_id';
                    END IF;
                    IF (lot_adjustment->>'after_shares')::numeric > lot.remaining_shares
                       OR (lot_adjustment->>'after_cost')::numeric > lot.remaining_cost
                       OR ((lot_adjustment->>'after_shares')::numeric = 0
                           AND (lot_adjustment->>'after_cost')::numeric <> 0) THEN
                        RAISE EXCEPTION 'lot adjustment % is not a reduction', lot_adjustment->>'lot_id';
                    END IF;
                    UPDATE position_lots
                       SET remaining_shares=(lot_adjustment->>'after_shares')::numeric,
                           remaining_cost=(lot_adjustment->>'after_cost')::numeric,
                           status=CASE WHEN (lot_adjustment->>'after_shares')::numeric=0 THEN 'CLOSED' ELSE 'OPEN' END,
                           closed_at=CASE WHEN (lot_adjustment->>'after_shares')::numeric=0
                                          THEN (manifest->>'observed_at')::timestamptz ELSE NULL END
                     WHERE lot_id=lot_adjustment->>'lot_id';
                    INSERT INTO execution_wallet_cutover_lot_adjustments(
                        batch_id, execution_account_id, token_id, lot_id,
                        before_shares, before_cost, after_shares, after_cost, reason
                    ) VALUES (
                        batch, account_id, asset->>'token_id', lot_adjustment->>'lot_id',
                        (lot_adjustment->>'before_shares')::numeric,
                        (lot_adjustment->>'before_cost')::numeric,
                        (lot_adjustment->>'after_shares')::numeric,
                        (lot_adjustment->>'after_cost')::numeric,
                        lot_adjustment->>'reason'
                    );
                END LOOP;
                SELECT COALESCE(sum(l.remaining_shares),0), COALESCE(sum(l.remaining_cost),0)
                  INTO after_total, after_cost
                  FROM position_lots l
                 WHERE l.execution_account_id=account_id AND l.token_id=asset->>'token_id'
                   AND l.status IN ('OPEN','SETTLED_PENDING_REDEEM');
                IF after_total IS DISTINCT FROM (asset->>'chain_shares')::numeric
                   OR after_cost IS DISTINCT FROM (asset->>'after_cost')::numeric THEN
                    RAISE EXCEPTION 'OLD_MANAGED asset % lot totals do not match chain baseline', asset->>'token_id';
                END IF;
                IF position_before_shares IS NOT NULL AND
                   (position_before_shares IS DISTINCT FROM after_total OR position_before_cost IS DISTINCT FROM after_cost) THEN
                    UPDATE execution_positions
                       SET total_shares=after_total, available_shares=after_total, reserved_shares=0,
                           cost_basis=after_cost,
                           average_cost_price=CASE WHEN after_total=0 THEN 0 ELSE after_cost/after_total END,
                           realized_pnl=realized_pnl,
                           market_value=COALESCE(mark_price*after_total,0),
                           unrealized_pnl=CASE WHEN mark_price IS NULL THEN 0 ELSE mark_price*after_total-after_cost END,
                           lifecycle_status=CASE WHEN after_total=0 THEN 'CLOSED' ELSE lifecycle_status END,
                           version=version+1, updated_at=clock_timestamp()
                     WHERE execution_account_id=account_id AND token_id=asset->>'token_id';
                    position_event := 'wallet-cutover-position:'||batch||':'||account_id||':'||(asset->>'token_id');
                    INSERT INTO position_events(
                        position_event_id, event_type, execution_account_id, market_id, token_id,
                        order_id, fill_key, model_id, strategy_id, shares_delta, cash_delta,
                        cost_basis_delta, realized_pnl_delta, shares_after, cost_basis_after,
                        average_cost_after, realized_pnl_after, mark_price, unrealized_pnl_after, occurred_at
                    )
                    SELECT position_event, 'CUTOVER_ADJUSTED', p.execution_account_id, p.market_id, p.token_id,
                           '', '', COALESCE(owner_model,''), COALESCE(owner_strategy,''), after_total-position_before_shares, 0,
                           after_cost-position_before_cost, 0, after_total, after_cost,
                           CASE WHEN after_total=0 THEN 0 ELSE after_cost/after_total END,
                           p.realized_pnl, p.mark_price, p.unrealized_pnl, (manifest->>'observed_at')::timestamptz
                      FROM execution_positions p
                     WHERE p.execution_account_id=account_id AND p.token_id=asset->>'token_id';
                END IF;
            END IF;
            asset_count := asset_count + 1;
        END LOOP;
        account_count := account_count + 1;
    END LOOP;
    IF account_count <> 2 THEN RAISE EXCEPTION 'wallet cutover must contain main and wallet-1'; END IF;

    RETURN jsonb_build_object('already_applied', false, 'batch_id', batch,
        'manifest_sha256', manifest_hash, 'account_count', account_count, 'asset_count', asset_count);
END;
$$;

-- SECURITY DEFINER must not inherit a caller-controlled search_path. Resolve
-- the schema that owns this migration once, then pin the function to that
-- literal schema (the integration harness uses an isolated schema instead of
-- public, so this is intentionally applied dynamically at migration time).
DO $$
BEGIN
    EXECUTE format(
        'ALTER FUNCTION %I.execution_apply_wallet_reuse_cutover(JSONB,TEXT,TEXT,TEXT,TEXT) SET search_path = pg_catalog, %I',
        current_schema(), current_schema()
    );
END;
$$;

REVOKE ALL ON FUNCTION execution_apply_wallet_reuse_cutover(JSONB,TEXT,TEXT,TEXT,TEXT) FROM PUBLIC;

-- Production provisions this NOLOGIN role and grants it EXECUTE explicitly.
-- Keeping the conditional grant makes the migration safe on installations that
-- create roles outside schema migrations; without that role only the function
-- owner can invoke the SECURITY DEFINER boundary.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trading_execution_cutover_operator') THEN
        EXECUTE 'GRANT EXECUTE ON FUNCTION execution_apply_wallet_reuse_cutover(JSONB,TEXT,TEXT,TEXT,TEXT) TO trading_execution_cutover_operator';
    END IF;
END;
$$;

COMMIT;
