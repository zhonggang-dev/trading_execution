BEGIN;

-- A cutover baseline identifies positions that already belonged to a wallet
-- before trading_execution took ownership of that execution account. These
-- rows are evidence only: they are deliberately separate from
-- execution_positions and position_lots, so unmanaged shares can never be
-- exposed to a strategy or reserved by the SELL path.
CREATE TABLE execution_external_position_baselines (
    baseline_id TEXT NOT NULL,
    execution_account_id TEXT NOT NULL,
    source TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    evidence JSONB NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT execution_external_position_baselines_pkey PRIMARY KEY (baseline_id),
    CONSTRAINT execution_external_position_baselines_execution_account_fk
        FOREIGN KEY (execution_account_id)
        REFERENCES execution_accounts(execution_account_id) ON DELETE RESTRICT,
    CONSTRAINT execution_external_position_baselines_account_unique
        UNIQUE (execution_account_id),
    CONSTRAINT execution_external_position_baselines_identity_nonempty CHECK (
        baseline_id <> '' AND execution_account_id <> '' AND source <> ''
        AND actor <> '' AND reason <> ''
    ),
    CONSTRAINT execution_external_position_baselines_evidence_object CHECK (
        jsonb_typeof(evidence) = 'object' AND evidence <> '{}'::jsonb
    ),
    CONSTRAINT execution_external_position_baselines_account_identity_unique
        UNIQUE (baseline_id, execution_account_id)
);

CREATE TABLE execution_external_position_baseline_items (
    baseline_id TEXT NOT NULL,
    execution_account_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    outcome_index INTEGER,
    outcome_name TEXT NOT NULL,
    neg_risk BOOLEAN NOT NULL,
    shares NUMERIC NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT execution_external_position_baseline_items_pkey
        PRIMARY KEY (baseline_id, token_id),
    CONSTRAINT execution_external_position_baseline_items_header_fk
        FOREIGN KEY (baseline_id, execution_account_id)
        REFERENCES execution_external_position_baselines (baseline_id, execution_account_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT execution_external_position_baseline_items_account_token_unique
        UNIQUE (execution_account_id, token_id),
    CONSTRAINT execution_external_position_baseline_items_identity_nonempty CHECK (
        baseline_id <> '' AND execution_account_id <> '' AND token_id <> ''
        AND condition_id <> '' AND outcome_name <> ''
    ),
    CONSTRAINT execution_external_position_baseline_items_outcome_index CHECK (
        outcome_index IS NULL OR outcome_index >= 0
    ),
    CONSTRAINT execution_external_position_baseline_items_positive_shares CHECK (shares > 0)
);

CREATE INDEX execution_external_position_baseline_items_account_idx
    ON execution_external_position_baseline_items (execution_account_id, condition_id, token_id);

-- Items must be inserted before their header in the same transaction. The
-- deferred foreign key makes that possible; once the header is present, the
-- baseline is sealed and no later item can extend its ownership scope.
CREATE FUNCTION execution_reject_sealed_external_position_baseline_item()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM execution_external_position_baselines baseline
        WHERE baseline.baseline_id = NEW.baseline_id
          AND baseline.execution_account_id = NEW.execution_account_id
    ) THEN
        RAISE EXCEPTION 'external position ownership baseline is already sealed';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER execution_external_position_baseline_items_sealed_trigger
BEFORE INSERT ON execution_external_position_baseline_items
FOR EACH ROW
EXECUTE FUNCTION execution_reject_sealed_external_position_baseline_item();

-- A header is the irreversible seal. Reject an empty seal so an operator
-- cannot permanently consume the one account baseline without recording any
-- item. The deferred composite FK still permits any number of items to be
-- inserted first and the matching header to be inserted last in one transaction.
CREATE FUNCTION execution_require_external_position_baseline_items()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM execution_external_position_baseline_items item
        WHERE item.baseline_id = NEW.baseline_id
          AND item.execution_account_id = NEW.execution_account_id
    ) THEN
        RAISE EXCEPTION 'external position ownership baseline requires at least one item before sealing';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER execution_external_position_baselines_items_required_trigger
BEFORE INSERT ON execution_external_position_baselines
FOR EACH ROW
EXECUTE FUNCTION execution_require_external_position_baseline_items();

-- Baseline evidence is append-only. Statement triggers also reject TRUNCATE,
-- which row-level UPDATE/DELETE triggers would not see.
CREATE FUNCTION execution_reject_external_position_baseline_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'external position ownership baselines are append-only';
END;
$$;

CREATE TRIGGER execution_external_position_baselines_append_only_trigger
BEFORE UPDATE OR DELETE OR TRUNCATE ON execution_external_position_baselines
FOR EACH STATEMENT
EXECUTE FUNCTION execution_reject_external_position_baseline_mutation();

CREATE TRIGGER execution_external_position_baseline_items_append_only_trigger
BEFORE UPDATE OR DELETE OR TRUNCATE ON execution_external_position_baseline_items
FOR EACH STATEMENT
EXECUTE FUNCTION execution_reject_external_position_baseline_mutation();

COMMIT;
