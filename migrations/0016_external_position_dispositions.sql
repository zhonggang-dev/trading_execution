BEGIN;

-- This migration is the only supported boundary for taking ownership of an
-- immutable 0014 baseline.  It deliberately preserves the original snapshot:
-- every later change is represented by append-only evidence instead of an
-- UPDATE to the baseline or by a fabricated order/fill.
CREATE TABLE execution_external_position_adjustment_batches (
    adjustment_batch_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    evidence JSONB NOT NULL,
    evidence_sha256 TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT execution_external_position_adjustment_batches_identity CHECK (
        btrim(adjustment_batch_id) <> ''
        AND schema_version = 'trading.external-position-adjustment.v1'
        AND btrim(actor) <> '' AND btrim(reason) <> ''
    ),
    CONSTRAINT execution_external_position_adjustment_batches_evidence_object CHECK (
        jsonb_typeof(evidence) = 'object' AND evidence <> '{}'::jsonb
    ),
    CONSTRAINT execution_external_position_adjustment_batches_evidence_sha256 CHECK (
        evidence_sha256 ~ '^[0-9a-f]{64}$'
    )
);

-- EXTERNAL_SELL and ADOPTION are the only kinds which consume baseline
-- shares. BASELINE_ACCOUNTED records a real pre-capture trade already
-- reflected by the immutable snapshot. FALSE_ATTRIBUTION is resolution audit
-- for a trade returned by an incorrectly scoped historical venue query; it is
-- never allowed to suppress a future venue result or change any balance.
CREATE TABLE execution_external_position_dispositions (
    disposition_id TEXT PRIMARY KEY,
    adjustment_batch_id TEXT NOT NULL,
    baseline_id TEXT,
    execution_account_id TEXT NOT NULL
        REFERENCES execution_accounts(execution_account_id) ON DELETE RESTRICT,
    condition_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    disposition_kind TEXT NOT NULL CHECK (disposition_kind IN (
        'EXTERNAL_SELL', 'ADOPTION', 'BASELINE_ACCOUNTED', 'FALSE_ATTRIBUTION'
    )),
    transition_sequence INTEGER,
    shares_before NUMERIC NOT NULL,
    shares_delta NUMERIC NOT NULL,
    shares_after NUMERIC NOT NULL,
    venue_trade_id TEXT NOT NULL DEFAULT '',
    venue_order_id TEXT NOT NULL DEFAULT '',
    transaction_hash TEXT NOT NULL DEFAULT '',
    occurred_at TIMESTAMPTZ NOT NULL,
    evidence JSONB NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT execution_external_position_dispositions_batch_fk
        FOREIGN KEY (adjustment_batch_id)
        REFERENCES execution_external_position_adjustment_batches(adjustment_batch_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT execution_external_position_dispositions_baseline_fk
        FOREIGN KEY (baseline_id, execution_account_id)
        REFERENCES execution_external_position_baselines(baseline_id, execution_account_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT execution_external_position_dispositions_identity_nonempty CHECK (
        btrim(disposition_id) <> '' AND btrim(adjustment_batch_id) <> ''
        AND btrim(execution_account_id) <> '' AND btrim(condition_id) <> ''
        AND btrim(token_id) <> '' AND btrim(actor) <> '' AND btrim(reason) <> ''
    ),
    CONSTRAINT execution_external_position_dispositions_evidence_object CHECK (
        jsonb_typeof(evidence) = 'object' AND evidence <> '{}'::jsonb
    ),
    CONSTRAINT execution_external_position_dispositions_shape CHECK (
        (disposition_kind IN ('EXTERNAL_SELL','ADOPTION')
            AND baseline_id IS NOT NULL
            AND transition_sequence IS NOT NULL AND transition_sequence > 0
            AND shares_before > 0 AND shares_delta > 0 AND shares_after >= 0
            AND shares_before = shares_delta + shares_after)
        OR
        (disposition_kind IN ('BASELINE_ACCOUNTED','FALSE_ATTRIBUTION')
            AND transition_sequence IS NULL
            AND shares_before = 0 AND shares_delta = 0 AND shares_after = 0)
    ),
    CONSTRAINT execution_external_position_dispositions_venue_shape CHECK (
        (disposition_kind = 'ADOPTION'
            AND venue_trade_id = '' AND venue_order_id = '' AND transaction_hash = '')
        OR
        (disposition_kind = 'EXTERNAL_SELL'
            AND btrim(venue_trade_id) <> '' AND btrim(venue_order_id) <> ''
            AND transaction_hash ~ '^0x[0-9a-f]{64}$')
        OR
        (disposition_kind IN ('BASELINE_ACCOUNTED','FALSE_ATTRIBUTION')
            AND btrim(venue_trade_id) <> '' AND btrim(venue_order_id) <> ''
            AND (transaction_hash = '' OR transaction_hash ~ '^0x[0-9a-f]{64}$'))
    ),
    CONSTRAINT execution_external_position_dispositions_accounted_shape CHECK (
        (disposition_kind = 'BASELINE_ACCOUNTED' AND baseline_id IS NOT NULL)
        OR (disposition_kind = 'FALSE_ATTRIBUTION' AND baseline_id IS NULL)
        OR disposition_kind IN ('EXTERNAL_SELL','ADOPTION')
    ),
    CONSTRAINT execution_external_position_dispositions_transition_unique
        UNIQUE (baseline_id, token_id, transition_sequence)
);

-- A false-attribution audit must never make a later, stronger ownership proof
-- impossible to record. Keep accounting identities unique among accounting
-- rows, while retaining a separate idempotency domain for false observations.
CREATE UNIQUE INDEX execution_external_position_dispositions_accounted_trade_uidx
    ON execution_external_position_dispositions (
        execution_account_id, venue_trade_id, venue_order_id, condition_id, token_id
    ) WHERE disposition_kind IN ('EXTERNAL_SELL','BASELINE_ACCOUNTED');

CREATE UNIQUE INDEX execution_external_position_dispositions_false_attribution_uidx
    ON execution_external_position_dispositions (
        execution_account_id, venue_trade_id, venue_order_id, condition_id, token_id
    ) WHERE disposition_kind='FALSE_ATTRIBUTION';

CREATE UNIQUE INDEX execution_external_position_dispositions_adoption_uidx
    ON execution_external_position_dispositions (baseline_id, token_id)
    WHERE disposition_kind='ADOPTION';

CREATE INDEX execution_external_position_dispositions_baseline_idx
    ON execution_external_position_dispositions (
        baseline_id, token_id, transition_sequence
    ) WHERE disposition_kind IN ('EXTERNAL_SELL','ADOPTION');

-- A Polygon transaction can contain more than one CLOB trade component.  Cash
-- evidence is consequently transaction-addressed, not forced into an
-- artificial one-cash-row-per-trade relationship.
CREATE TABLE execution_external_cash_adjustments (
    cash_adjustment_id TEXT PRIMARY KEY,
    adjustment_batch_id TEXT NOT NULL,
    execution_account_id TEXT NOT NULL
        REFERENCES execution_accounts(execution_account_id) ON DELETE RESTRICT,
    transaction_hash TEXT NOT NULL,
    asset TEXT NOT NULL,
    total_delta NUMERIC NOT NULL,
    available_delta NUMERIC NOT NULL,
    reserved_delta NUMERIC NOT NULL,
    balance_before NUMERIC NOT NULL,
    balance_after NUMERIC NOT NULL,
    account_event_id TEXT NOT NULL UNIQUE
        REFERENCES execution_account_events(account_event_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    occurred_at TIMESTAMPTZ NOT NULL,
    evidence JSONB NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT execution_external_cash_adjustments_batch_fk
        FOREIGN KEY (adjustment_batch_id)
        REFERENCES execution_external_position_adjustment_batches(adjustment_batch_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT execution_external_cash_adjustments_identity_nonempty CHECK (
        btrim(cash_adjustment_id) <> '' AND btrim(adjustment_batch_id) <> ''
        AND btrim(execution_account_id) <> ''
        AND transaction_hash ~ '^0x[0-9a-f]{64}$'
        AND asset = 'pUSD' AND btrim(actor) <> '' AND btrim(reason) <> ''
    ),
    CONSTRAINT execution_external_cash_adjustments_amount_shape CHECK (
        total_delta > 0 AND available_delta = total_delta
        AND reserved_delta = 0 AND balance_before >= 0
        AND balance_after = balance_before + total_delta
    ),
    CONSTRAINT execution_external_cash_adjustments_evidence_object CHECK (
        jsonb_typeof(evidence) = 'object' AND evidence <> '{}'::jsonb
    ),
    CONSTRAINT execution_external_cash_adjustments_transaction_unique
        UNIQUE (execution_account_id, transaction_hash)
);

-- Make imported ownership an explicit alternative origin for a lot.  NULL is
-- accepted by the old FKs and unique constraint, while the XOR below prevents
-- a historical adoption from masquerading as a real BUY fill.
ALTER TABLE position_lots
    ALTER COLUMN opening_order_id DROP NOT NULL,
    ALTER COLUMN opening_fill_key DROP NOT NULL,
    ADD COLUMN external_adoption_id TEXT,
    ADD CONSTRAINT position_lots_origin_xor CHECK (
        (external_adoption_id IS NULL
            AND opening_order_id IS NOT NULL AND opening_fill_key IS NOT NULL)
        OR
        (external_adoption_id IS NOT NULL
            AND opening_order_id IS NULL AND opening_fill_key IS NULL)
    ),
    ADD CONSTRAINT position_lots_external_adoption_unique UNIQUE (external_adoption_id);

-- ADOPTED is a real ledger import event, but never a venue execution event.
ALTER TABLE position_events
    DROP CONSTRAINT position_events_event_type_check,
    ADD COLUMN external_adoption_id TEXT,
    ADD CONSTRAINT position_events_event_type_check CHECK (
        event_type IN ('BOUGHT','ADOPTED','SOLD','MARKED','SETTLED')
    ),
    ADD CONSTRAINT position_events_external_adoption_unique UNIQUE (external_adoption_id),
    ADD CONSTRAINT position_events_external_adoption_shape CHECK (
        (event_type = 'ADOPTED'
            AND external_adoption_id IS NOT NULL
            AND order_id = '' AND fill_key = ''
            AND shares_delta > 0 AND cash_delta = 0
            AND cost_basis_delta > 0 AND realized_pnl_delta = 0)
        OR
        (event_type <> 'ADOPTED' AND external_adoption_id IS NULL)
    );

CREATE TABLE execution_external_position_adoptions (
    external_adoption_id TEXT PRIMARY KEY,
    adjustment_batch_id TEXT NOT NULL,
    disposition_id TEXT NOT NULL UNIQUE,
    baseline_id TEXT NOT NULL,
    execution_account_id TEXT NOT NULL
        REFERENCES execution_accounts(execution_account_id) ON DELETE RESTRICT,
    lot_id TEXT NOT NULL UNIQUE,
    position_event_id TEXT NOT NULL UNIQUE,
    market_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome_index SMALLINT NOT NULL CHECK (outcome_index IN (0,1)),
    outcome_name TEXT NOT NULL,
    neg_risk BOOLEAN NOT NULL,
    model_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    shares NUMERIC NOT NULL,
    remaining_cost NUMERIC NOT NULL,
    average_entry_price NUMERIC NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL,
    adopted_at TIMESTAMPTZ NOT NULL,
    evidence JSONB NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT execution_external_position_adoptions_batch_fk
        FOREIGN KEY (adjustment_batch_id)
        REFERENCES execution_external_position_adjustment_batches(adjustment_batch_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT execution_external_position_adoptions_disposition_fk
        FOREIGN KEY (disposition_id)
        REFERENCES execution_external_position_dispositions(disposition_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT execution_external_position_adoptions_baseline_fk
        FOREIGN KEY (baseline_id, execution_account_id)
        REFERENCES execution_external_position_baselines(baseline_id, execution_account_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT execution_external_position_adoptions_lot_fk
        FOREIGN KEY (lot_id) REFERENCES position_lots(lot_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT execution_external_position_adoptions_event_fk
        FOREIGN KEY (position_event_id) REFERENCES position_events(position_event_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT execution_external_position_adoptions_identity_nonempty CHECK (
        btrim(external_adoption_id) <> '' AND btrim(adjustment_batch_id) <> ''
        AND btrim(disposition_id) <> '' AND btrim(baseline_id) <> ''
        AND btrim(execution_account_id) <> '' AND btrim(lot_id) <> ''
        AND btrim(position_event_id) <> '' AND btrim(market_id) <> ''
        AND btrim(condition_id) <> '' AND btrim(token_id) <> ''
        AND btrim(outcome_name) <> '' AND btrim(model_id) <> ''
        AND btrim(strategy_id) <> '' AND btrim(actor) <> '' AND btrim(reason) <> ''
    ),
    CONSTRAINT execution_external_position_adoptions_amount_shape CHECK (
        shares > 0 AND remaining_cost > 0
        AND average_entry_price > 0 AND average_entry_price <= 1
        AND opened_at <= adopted_at
    ),
    CONSTRAINT execution_external_position_adoptions_evidence_object CHECK (
        jsonb_typeof(evidence) = 'object' AND evidence <> '{}'::jsonb
    ),
    CONSTRAINT execution_external_position_adoptions_account_token_unique
        UNIQUE (execution_account_id, token_id)
);

ALTER TABLE position_lots
    ADD CONSTRAINT position_lots_external_adoption_fk
    FOREIGN KEY (external_adoption_id)
    REFERENCES execution_external_position_adoptions(external_adoption_id)
    ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE position_events
    ADD CONSTRAINT position_events_external_adoption_fk
    FOREIGN KEY (external_adoption_id)
    REFERENCES execution_external_position_adoptions(external_adoption_id)
    ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

-- A child row can only be written before its batch header is sealed.  The
-- operational table locks make all idle checks stable through commit.  This
-- workflow is intentionally an offline, kill-switched cutover transaction.
CREATE FUNCTION execution_require_external_position_adjustment_safety(
    target_batch_id TEXT,
    target_account_id TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    global_kill BOOLEAN;
    account_asset TEXT;
    account_total NUMERIC;
    account_available NUMERIC;
    account_reserved NUMERIC;
BEGIN
    -- Serialize every child/header decision for one batch across transactions.
    -- Advisory locks are transaction-scoped and re-entrant, so the supported
    -- child-first/header-last workflow keeps one lock until commit while a
    -- concurrent late child cannot race an uncommitted seal.
    PERFORM pg_advisory_xact_lock(hashtextextended(target_batch_id, 160016));
    LOCK TABLE execution_risk_global_control IN SHARE MODE;
    LOCK TABLE execution_accounts IN SHARE ROW EXCLUSIVE MODE;
    LOCK TABLE execution_positions IN SHARE MODE;
    LOCK TABLE position_lots IN SHARE MODE;
    LOCK TABLE asset_reservations IN SHARE MODE;
    LOCK TABLE execution_orders IN SHARE MODE;
    LOCK TABLE strategy_order_intent_deliveries IN SHARE MODE;
    LOCK TABLE reconciliation_runs IN SHARE MODE;

    IF EXISTS (
        SELECT 1 FROM execution_external_position_adjustment_batches
        WHERE adjustment_batch_id=target_batch_id
    ) THEN
        RAISE EXCEPTION 'external position adjustment batch is already sealed';
    END IF;

    SELECT kill_switch INTO global_kill
      FROM execution_risk_global_control WHERE singleton=TRUE;
    IF NOT FOUND OR NOT global_kill THEN
        RAISE EXCEPTION 'external position adjustment requires global kill switch';
    END IF;

    SELECT collateral_asset, total_balance, available_balance, reserved_balance
      INTO account_asset, account_total, account_available, account_reserved
      FROM execution_accounts
     WHERE execution_account_id=target_account_id
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'external position adjustment account does not exist';
    END IF;
    IF account_asset <> 'pUSD' OR account_reserved <> 0 OR account_total <> account_available THEN
        RAISE EXCEPTION 'external position adjustment account ledger is not idle pUSD';
    END IF;
    IF EXISTS (
        SELECT 1 FROM execution_orders
         WHERE execution_account_id=target_account_id
           AND status NOT IN ('FILLED','REJECTED','CANCELLED','MANUAL_REVIEW')
    ) THEN
        RAISE EXCEPTION 'external position adjustment account has a non-terminal order';
    END IF;
    IF EXISTS (
        SELECT 1 FROM asset_reservations
         WHERE execution_account_id=target_account_id
           AND status IN ('ACTIVE','RECONCILIATION_REQUIRED')
    ) THEN
        RAISE EXCEPTION 'external position adjustment account has an active reservation';
    END IF;
    IF EXISTS (
        SELECT 1 FROM strategy_order_intent_deliveries
         WHERE status IN ('PENDING','SUBMITTING')
           AND btrim(COALESCE(intent_payload->>'execution_account_id',''))=target_account_id
    ) THEN
        RAISE EXCEPTION 'external position adjustment account has a pending intent delivery';
    END IF;
    IF EXISTS (
        SELECT 1 FROM reconciliation_runs
         WHERE execution_account_id=target_account_id AND status='RUNNING'
    ) THEN
        RAISE EXCEPTION 'external position adjustment account has a running reconciliation';
    END IF;
END;
$$;

CREATE FUNCTION execution_guard_external_position_disposition_insert()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    baseline_condition TEXT;
    baseline_shares NUMERIC;
    baseline_observed_at TIMESTAMPTZ;
    previous_sequence INTEGER;
    previous_after NUMERIC;
    previous_occurred_at TIMESTAMPTZ;
BEGIN
    PERFORM execution_require_external_position_adjustment_safety(
        NEW.adjustment_batch_id, NEW.execution_account_id
    );
    LOCK TABLE execution_external_position_dispositions IN SHARE ROW EXCLUSIVE MODE;

    IF NEW.baseline_id IS NOT NULL THEN
        SELECT header.observed_at INTO baseline_observed_at
          FROM execution_external_position_baselines AS header
         WHERE header.baseline_id=NEW.baseline_id
           AND header.execution_account_id=NEW.execution_account_id;
    END IF;

    IF NEW.disposition_kind IN ('EXTERNAL_SELL','ADOPTION') THEN
        SELECT item.condition_id, item.shares
          INTO baseline_condition, baseline_shares
          FROM execution_external_position_baseline_items AS item
         WHERE item.baseline_id=NEW.baseline_id
           AND item.execution_account_id=NEW.execution_account_id
           AND item.token_id=NEW.token_id;
        IF baseline_condition IS NULL OR baseline_condition <> NEW.condition_id THEN
            RAISE EXCEPTION 'baseline transition does not match an immutable baseline item';
        END IF;
        SELECT transition_sequence, shares_after, occurred_at
          INTO previous_sequence, previous_after, previous_occurred_at
          FROM execution_external_position_dispositions
         WHERE baseline_id=NEW.baseline_id AND token_id=NEW.token_id
           AND disposition_kind IN ('EXTERNAL_SELL','ADOPTION')
         ORDER BY transition_sequence DESC LIMIT 1;
        IF previous_sequence IS NULL THEN
            IF NEW.transition_sequence <> 1 OR NEW.shares_before <> baseline_shares THEN
                RAISE EXCEPTION 'first baseline transition must start at immutable baseline shares';
            END IF;
        ELSIF NEW.transition_sequence <> previous_sequence + 1
           OR NEW.shares_before <> previous_after
           OR NEW.occurred_at < previous_occurred_at THEN
            RAISE EXCEPTION 'baseline transition chain is not contiguous';
        END IF;
        IF NEW.occurred_at < baseline_observed_at THEN
            RAISE EXCEPTION 'baseline transition predates the ownership baseline';
        END IF;
        IF NEW.disposition_kind='ADOPTION' AND NEW.shares_after <> 0 THEN
            RAISE EXCEPTION 'an adoption must consume the exact effective baseline remainder';
        END IF;
        IF NEW.disposition_kind='ADOPTION' AND EXISTS (
            SELECT 1 FROM execution_positions
             WHERE execution_account_id=NEW.execution_account_id AND token_id=NEW.token_id
               AND (total_shares<>0 OR reserved_shares<>0 OR cost_basis<>0)
        ) THEN
            RAISE EXCEPTION 'adoption target already has a managed position';
        END IF;
        IF NEW.disposition_kind='ADOPTION' AND EXISTS (
            SELECT 1 FROM position_lots
             WHERE execution_account_id=NEW.execution_account_id AND token_id=NEW.token_id
               AND status IN ('OPEN','SETTLED_PENDING_REDEEM')
        ) THEN
            RAISE EXCEPTION 'adoption target already has a managed lot';
        END IF;
    ELSIF NEW.disposition_kind='BASELINE_ACCOUNTED' THEN
        IF baseline_observed_at IS NULL OR NEW.occurred_at > baseline_observed_at THEN
            RAISE EXCEPTION 'baseline-accounted trade must predate its ownership capture';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER execution_external_position_dispositions_insert_guard_trigger
BEFORE INSERT ON execution_external_position_dispositions
FOR EACH ROW EXECUTE FUNCTION execution_guard_external_position_disposition_insert();

CREATE FUNCTION execution_guard_external_cash_adjustment_insert()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    account_total NUMERIC;
    account_available NUMERIC;
    previous_adjustment_id TEXT;
    previous_occurred_at TIMESTAMPTZ;
    previous_balance_after NUMERIC;
BEGIN
    PERFORM execution_require_external_position_adjustment_safety(
        NEW.adjustment_batch_id, NEW.execution_account_id
    );
    LOCK TABLE execution_external_cash_adjustments IN SHARE ROW EXCLUSIVE MODE;

    SELECT cash.cash_adjustment_id, cash.occurred_at, cash.balance_after
      INTO previous_adjustment_id, previous_occurred_at, previous_balance_after
      FROM execution_external_cash_adjustments AS cash
     WHERE cash.adjustment_batch_id=NEW.adjustment_batch_id
       AND cash.execution_account_id=NEW.execution_account_id
     ORDER BY cash.occurred_at DESC, cash.cash_adjustment_id DESC
     LIMIT 1;
    IF FOUND THEN
        IF ROW(NEW.occurred_at,NEW.cash_adjustment_id)
              <= ROW(previous_occurred_at,previous_adjustment_id) THEN
            RAISE EXCEPTION 'external cash adjustments must be inserted in strict occurred_at/id order';
        END IF;
        IF NEW.balance_before IS DISTINCT FROM previous_balance_after THEN
            RAISE EXCEPTION 'external cash adjustment does not continue the batch balance chain';
        END IF;
    ELSE
        SELECT total_balance, available_balance
          INTO account_total, account_available
          FROM execution_accounts
         WHERE execution_account_id=NEW.execution_account_id;
        IF NOT FOUND OR account_total IS DISTINCT FROM account_available
           OR NEW.balance_before IS DISTINCT FROM account_total THEN
            RAISE EXCEPTION 'first external cash adjustment must start at the current available account balance';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER execution_external_cash_adjustments_insert_guard_trigger
BEFORE INSERT ON execution_external_cash_adjustments
FOR EACH ROW EXECUTE FUNCTION execution_guard_external_cash_adjustment_insert();

CREATE FUNCTION execution_guard_external_position_adoption_insert()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM execution_require_external_position_adjustment_safety(
        NEW.adjustment_batch_id, NEW.execution_account_id
    );
    IF NOT EXISTS (
        SELECT 1 FROM execution_strategy_bindings
         WHERE execution_account_id=NEW.execution_account_id
           AND model_id=NEW.model_id
           AND strategy_id=execution_canonical_strategy_id(NEW.strategy_id)
           AND enabled=TRUE
    ) THEN
        RAISE EXCEPTION 'external position adoption has no enabled exact strategy binding';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER execution_external_position_adoptions_insert_guard_trigger
BEFORE INSERT ON execution_external_position_adoptions
FOR EACH ROW EXECUTE FUNCTION execution_guard_external_position_adoption_insert();

-- Deferred checks see the complete transaction (including the batch header,
-- account event, position, lot, and ADOPTED event) and make a partial import
-- impossible to commit.
CREATE FUNCTION execution_validate_external_position_disposition()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    batch_observed_at TIMESTAMPTZ;
BEGIN
    SELECT observed_at INTO batch_observed_at
      FROM execution_external_position_adjustment_batches
     WHERE adjustment_batch_id=NEW.adjustment_batch_id;
    IF NOT FOUND OR NEW.occurred_at > batch_observed_at THEN
        RAISE EXCEPTION 'external position disposition is outside its sealed batch';
    END IF;
    IF NEW.disposition_kind='EXTERNAL_SELL' AND NOT EXISTS (
        SELECT 1 FROM execution_external_cash_adjustments AS cash
         WHERE cash.adjustment_batch_id=NEW.adjustment_batch_id
           AND cash.execution_account_id=NEW.execution_account_id
           AND cash.transaction_hash=NEW.transaction_hash
    ) THEN
        RAISE EXCEPTION 'external sell disposition is missing exact transaction cash evidence';
    END IF;
    IF NEW.disposition_kind='ADOPTION' AND NOT EXISTS (
        SELECT 1 FROM execution_external_position_adoptions AS adoption
         WHERE adoption.disposition_id=NEW.disposition_id
    ) THEN
        RAISE EXCEPTION 'adoption disposition is missing its managed adoption';
    END IF;
    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER execution_external_position_dispositions_validate_trigger
AFTER INSERT ON execution_external_position_dispositions
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION execution_validate_external_position_disposition();

CREATE FUNCTION execution_validate_external_cash_adjustment()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    event_row execution_account_events%ROWTYPE;
    account_row execution_accounts%ROWTYPE;
    first_before NUMERIC;
    last_after NUMERIC;
    cash_total NUMERIC;
    broken_links INTEGER;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM execution_external_position_dispositions AS disposition
         WHERE disposition.adjustment_batch_id=NEW.adjustment_batch_id
           AND disposition.execution_account_id=NEW.execution_account_id
           AND disposition.transaction_hash=NEW.transaction_hash
           AND disposition.disposition_kind='EXTERNAL_SELL'
    ) THEN
        RAISE EXCEPTION 'external cash adjustment has no exact external sell transaction';
    END IF;
    SELECT * INTO event_row FROM execution_account_events
     WHERE account_event_id=NEW.account_event_id;
    IF NOT FOUND OR event_row.execution_account_id<>NEW.execution_account_id
       OR event_row.event_type<>'EXTERNAL_POSITION_DISPOSITION'
       OR event_row.order_id<>'' OR event_row.fill_key<>''
       OR event_row.total_balance_delta<>NEW.total_delta
       OR event_row.available_balance_delta<>NEW.available_delta
       OR event_row.reserved_balance_delta<>NEW.reserved_delta
       OR event_row.total_balance_after<>NEW.balance_after
       OR event_row.available_balance_after<>NEW.balance_after
       OR event_row.reserved_balance_after<>0
       OR event_row.occurred_at<>NEW.occurred_at THEN
        RAISE EXCEPTION 'external cash adjustment account event does not match exactly';
    END IF;

    SELECT min(balance_before) FILTER (WHERE sequence_number=1),
           max(balance_after) FILTER (WHERE reverse_sequence=1),
           sum(total_delta),
           count(*) FILTER (WHERE sequence_number>1 AND balance_before<>previous_after)
      INTO first_before, last_after, cash_total, broken_links
      FROM (
        SELECT cash.*,
               row_number() OVER (ORDER BY occurred_at,cash_adjustment_id) AS sequence_number,
               row_number() OVER (ORDER BY occurred_at DESC,cash_adjustment_id DESC) AS reverse_sequence,
               lag(balance_after) OVER (ORDER BY occurred_at,cash_adjustment_id) AS previous_after
          FROM execution_external_cash_adjustments AS cash
         WHERE adjustment_batch_id=NEW.adjustment_batch_id
           AND execution_account_id=NEW.execution_account_id
      ) AS ordered_cash;
    SELECT * INTO account_row FROM execution_accounts
     WHERE execution_account_id=NEW.execution_account_id;
    IF broken_links<>0 OR last_after<>first_before+cash_total
       OR account_row.total_balance<>last_after
       OR account_row.available_balance<>last_after
       OR account_row.reserved_balance<>0 THEN
        RAISE EXCEPTION 'external cash adjustment chain does not match account ledger';
    END IF;
    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER execution_external_cash_adjustments_validate_trigger
AFTER INSERT ON execution_external_cash_adjustments
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION execution_validate_external_cash_adjustment();

CREATE FUNCTION execution_validate_external_position_adoption()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    disposition_row execution_external_position_dispositions%ROWTYPE;
    baseline_condition_id TEXT;
    baseline_outcome_index INTEGER;
    baseline_outcome_name TEXT;
    baseline_neg_risk BOOLEAN;
    baseline_observed_at TIMESTAMPTZ;
    lot_row position_lots%ROWTYPE;
    position_row execution_positions%ROWTYPE;
    event_row position_events%ROWTYPE;
BEGIN
    SELECT * INTO disposition_row FROM execution_external_position_dispositions
     WHERE disposition_id=NEW.disposition_id;
    IF NOT FOUND OR disposition_row.disposition_kind<>'ADOPTION'
       OR disposition_row.adjustment_batch_id<>NEW.adjustment_batch_id
       OR disposition_row.baseline_id<>NEW.baseline_id
       OR disposition_row.execution_account_id<>NEW.execution_account_id
       OR disposition_row.condition_id<>NEW.condition_id
       OR disposition_row.token_id<>NEW.token_id
       OR disposition_row.shares_delta<>NEW.shares
       OR disposition_row.shares_after<>0
       OR disposition_row.occurred_at<>NEW.adopted_at THEN
        RAISE EXCEPTION 'external adoption does not match its baseline disposition';
    END IF;

    SELECT item.condition_id, item.outcome_index, item.outcome_name,
           item.neg_risk, header.observed_at
      INTO baseline_condition_id, baseline_outcome_index,
           baseline_outcome_name, baseline_neg_risk, baseline_observed_at
      FROM execution_external_position_baseline_items AS item
      JOIN execution_external_position_baselines AS header
        ON header.baseline_id=item.baseline_id
       AND header.execution_account_id=item.execution_account_id
     WHERE item.baseline_id=NEW.baseline_id
       AND item.execution_account_id=NEW.execution_account_id
       AND item.token_id=NEW.token_id;
    IF NOT FOUND OR baseline_condition_id IS DISTINCT FROM NEW.condition_id
       OR baseline_outcome_index IS DISTINCT FROM NEW.outcome_index
       OR baseline_outcome_name IS DISTINCT FROM NEW.outcome_name
       OR baseline_neg_risk IS DISTINCT FROM NEW.neg_risk
       OR NEW.opened_at>baseline_observed_at OR NEW.adopted_at<baseline_observed_at THEN
        RAISE EXCEPTION 'external adoption does not match immutable baseline identity and timing';
    END IF;

    SELECT * INTO lot_row FROM position_lots WHERE lot_id=NEW.lot_id;
    IF NOT FOUND OR lot_row.external_adoption_id IS DISTINCT FROM NEW.external_adoption_id
       OR lot_row.execution_account_id IS DISTINCT FROM NEW.execution_account_id
       OR lot_row.market_id IS DISTINCT FROM NEW.market_id OR lot_row.condition_id IS DISTINCT FROM NEW.condition_id
       OR lot_row.token_id IS DISTINCT FROM NEW.token_id OR lot_row.outcome_index IS DISTINCT FROM NEW.outcome_index
       OR lot_row.outcome_name IS DISTINCT FROM NEW.outcome_name OR lot_row.neg_risk IS DISTINCT FROM NEW.neg_risk
       OR lot_row.model_id IS DISTINCT FROM NEW.model_id
       OR execution_canonical_strategy_id(lot_row.strategy_id) IS DISTINCT FROM execution_canonical_strategy_id(NEW.strategy_id)
       OR lot_row.opening_order_id IS NOT NULL OR lot_row.opening_fill_key IS NOT NULL
       OR lot_row.original_shares<>NEW.shares OR lot_row.remaining_shares<>NEW.shares
       OR lot_row.original_cost<>NEW.remaining_cost OR lot_row.remaining_cost<>NEW.remaining_cost
       OR lot_row.average_entry_price<>NEW.average_entry_price
       OR lot_row.status<>'OPEN' OR lot_row.opened_at<>NEW.opened_at THEN
        RAISE EXCEPTION 'external adoption managed lot does not match exactly';
    END IF;

    SELECT * INTO position_row FROM execution_positions
     WHERE execution_account_id=NEW.execution_account_id AND token_id=NEW.token_id;
    IF NOT FOUND OR position_row.market_id IS DISTINCT FROM NEW.market_id
       OR position_row.condition_id IS DISTINCT FROM NEW.condition_id
       OR position_row.outcome_index IS DISTINCT FROM NEW.outcome_index
       OR position_row.outcome_name IS DISTINCT FROM NEW.outcome_name
       OR position_row.total_shares<>NEW.shares
       OR position_row.available_shares<>NEW.shares
       OR position_row.reserved_shares<>0
       OR position_row.cost_basis<>NEW.remaining_cost
       OR position_row.average_cost_price<>(NEW.remaining_cost/NEW.shares)
       OR position_row.realized_pnl<>0
       OR position_row.lifecycle_status<>'OPEN' THEN
        RAISE EXCEPTION 'external adoption managed position does not match exactly';
    END IF;

    SELECT * INTO event_row FROM position_events
     WHERE position_event_id=NEW.position_event_id;
    IF NOT FOUND OR event_row.external_adoption_id<>NEW.external_adoption_id
       OR event_row.event_type<>'ADOPTED'
       OR event_row.execution_account_id<>NEW.execution_account_id
       OR event_row.market_id<>NEW.market_id OR event_row.token_id<>NEW.token_id
       OR event_row.order_id<>'' OR event_row.fill_key<>''
       OR event_row.model_id<>NEW.model_id
       OR execution_canonical_strategy_id(event_row.strategy_id)<>execution_canonical_strategy_id(NEW.strategy_id)
       OR event_row.shares_delta<>NEW.shares OR event_row.cash_delta<>0
       OR event_row.cost_basis_delta<>NEW.remaining_cost OR event_row.realized_pnl_delta<>0
       OR event_row.shares_after<>NEW.shares
       OR event_row.cost_basis_after<>NEW.remaining_cost
       OR event_row.average_cost_after<>(NEW.remaining_cost/NEW.shares)
       OR event_row.realized_pnl_after<>0
       OR event_row.occurred_at<>NEW.adopted_at THEN
        RAISE EXCEPTION 'external adoption position event does not match exactly';
    END IF;
    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER execution_external_position_adoptions_validate_trigger
AFTER INSERT ON execution_external_position_adoptions
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION execution_validate_external_position_adoption();

-- Opening execution identity is immutable for both real-fill and adoption
-- lots. Normal SELL settlement may still reduce remaining_shares/cost and
-- close the row.
CREATE FUNCTION execution_enforce_position_lot_origin_immutable()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.opening_order_id IS DISTINCT FROM OLD.opening_order_id
       OR NEW.opening_fill_key IS DISTINCT FROM OLD.opening_fill_key
       OR NEW.external_adoption_id IS DISTINCT FROM OLD.external_adoption_id THEN
        RAISE EXCEPTION 'position lot opening origin is immutable';
    END IF;
    IF OLD.external_adoption_id IS NOT NULL OR NEW.external_adoption_id IS NOT NULL THEN
        IF NEW.execution_account_id IS DISTINCT FROM OLD.execution_account_id
           OR NEW.market_id IS DISTINCT FROM OLD.market_id
           OR NEW.condition_id IS DISTINCT FROM OLD.condition_id
           OR NEW.token_id IS DISTINCT FROM OLD.token_id
           OR NEW.outcome_index IS DISTINCT FROM OLD.outcome_index
           OR NEW.outcome_name IS DISTINCT FROM OLD.outcome_name
           OR NEW.neg_risk IS DISTINCT FROM OLD.neg_risk
           OR NEW.model_id IS DISTINCT FROM OLD.model_id
           OR NEW.strategy_id IS DISTINCT FROM OLD.strategy_id
           OR NEW.original_shares IS DISTINCT FROM OLD.original_shares
           OR NEW.original_cost IS DISTINCT FROM OLD.original_cost
           OR NEW.average_entry_price IS DISTINCT FROM OLD.average_entry_price
           OR NEW.opened_at IS DISTINCT FROM OLD.opened_at THEN
            RAISE EXCEPTION 'external adoption lot identity and original economics are immutable';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER position_lots_origin_immutable_trigger
BEFORE UPDATE ON position_lots
FOR EACH ROW EXECUTE FUNCTION execution_enforce_position_lot_origin_immutable();

-- Existing position/account events predate append-only table guards. Protect
-- the new externally sourced rows in full without changing normal mark/fill
-- behavior for legacy event types.
CREATE FUNCTION execution_enforce_external_position_event_immutable()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.event_type='ADOPTED' OR NEW.event_type='ADOPTED'
       OR OLD.external_adoption_id IS NOT NULL OR NEW.external_adoption_id IS NOT NULL THEN
        IF to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
            RAISE EXCEPTION 'external adoption position event is immutable';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER position_events_external_adoption_immutable_trigger
BEFORE UPDATE ON position_events
FOR EACH ROW EXECUTE FUNCTION execution_enforce_external_position_event_immutable();

CREATE FUNCTION execution_enforce_external_account_event_immutable()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.event_type='EXTERNAL_POSITION_DISPOSITION'
       OR NEW.event_type='EXTERNAL_POSITION_DISPOSITION' THEN
        IF to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
            RAISE EXCEPTION 'external position disposition account event is immutable';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER execution_account_events_external_adjustment_immutable_trigger
BEFORE UPDATE ON execution_account_events
FOR EACH ROW EXECUTE FUNCTION execution_enforce_external_account_event_immutable();

-- Serialize the 0014 item-before-header seal protocol across transactions.
-- Without the advisory lock, a concurrent late item could race an uncommitted
-- header and extend a snapshot after the operator believed it was sealed.
CREATE OR REPLACE FUNCTION execution_reject_sealed_external_position_baseline_item()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(NEW.baseline_id||E'\n'||NEW.execution_account_id, 0));
    IF EXISTS (
        SELECT 1 FROM execution_external_position_baselines AS baseline
         WHERE baseline.baseline_id=NEW.baseline_id
           AND baseline.execution_account_id=NEW.execution_account_id
    ) THEN
        RAISE EXCEPTION 'external position ownership baseline is already sealed';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION execution_require_external_position_baseline_items()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(NEW.baseline_id||E'\n'||NEW.execution_account_id, 0));
    IF NOT EXISTS (
        SELECT 1 FROM execution_external_position_baseline_items AS item
         WHERE item.baseline_id=NEW.baseline_id
           AND item.execution_account_id=NEW.execution_account_id
    ) THEN
        RAISE EXCEPTION 'external position ownership baseline requires at least one item before sealing';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION execution_require_external_position_adjustment_batch_items()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(NEW.adjustment_batch_id, 160016));
    IF NOT EXISTS (
        SELECT 1 FROM execution_external_position_dispositions
         WHERE adjustment_batch_id=NEW.adjustment_batch_id
    ) THEN
        RAISE EXCEPTION 'external position adjustment batch requires at least one disposition';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER execution_external_position_batches_items_required_trigger
BEFORE INSERT ON execution_external_position_adjustment_batches
FOR EACH ROW EXECUTE FUNCTION execution_require_external_position_adjustment_batch_items();

CREATE FUNCTION execution_reject_external_position_adjustment_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'external position adjustments are append-only';
END;
$$;

CREATE TRIGGER execution_external_position_batches_append_only_trigger
BEFORE UPDATE OR DELETE OR TRUNCATE ON execution_external_position_adjustment_batches
FOR EACH STATEMENT EXECUTE FUNCTION execution_reject_external_position_adjustment_mutation();

CREATE TRIGGER execution_external_position_dispositions_append_only_trigger
BEFORE UPDATE OR DELETE OR TRUNCATE ON execution_external_position_dispositions
FOR EACH STATEMENT EXECUTE FUNCTION execution_reject_external_position_adjustment_mutation();

CREATE TRIGGER execution_external_cash_adjustments_append_only_trigger
BEFORE UPDATE OR DELETE OR TRUNCATE ON execution_external_cash_adjustments
FOR EACH STATEMENT EXECUTE FUNCTION execution_reject_external_position_adjustment_mutation();

CREATE TRIGGER execution_external_position_adoptions_append_only_trigger
BEFORE UPDATE OR DELETE OR TRUNCATE ON execution_external_position_adoptions
FOR EACH STATEMENT EXECUTE FUNCTION execution_reject_external_position_adjustment_mutation();

COMMIT;
