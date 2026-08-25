BEGIN;

-- Wallet identity changes are security-sensitive and must be explicit.  This
-- append-only evidence lets readers distinguish a genuine account migration
-- from an accidental wallet-address mismatch without mutating old snapshots.
CREATE TABLE execution_wallet_migrations (
    migration_id TEXT NOT NULL,
    execution_account_id TEXT NOT NULL,
    old_wallet_address TEXT NOT NULL,
    new_wallet_address TEXT NOT NULL,
    chain_id BIGINT NOT NULL,
    collateral_asset TEXT NOT NULL,
    collateral_balance_before NUMERIC NOT NULL,
    collateral_balance_after NUMERIC NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    evidence JSONB NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT execution_wallet_migrations_pkey PRIMARY KEY (migration_id),
    CONSTRAINT execution_wallet_migrations_execution_account_fk
        FOREIGN KEY (execution_account_id)
        REFERENCES execution_accounts(execution_account_id) ON DELETE RESTRICT,
    CONSTRAINT execution_wallet_migrations_account_wallets_unique
        UNIQUE (execution_account_id, old_wallet_address, new_wallet_address),
    CONSTRAINT execution_wallet_migrations_identity_nonempty CHECK (
        migration_id <> '' AND execution_account_id <> ''
        AND collateral_asset <> '' AND actor <> '' AND reason <> ''
    ),
    CONSTRAINT execution_wallet_migrations_wallet_shape CHECK (
        old_wallet_address ~ '^0x[0-9a-f]{40}$'
        AND new_wallet_address ~ '^0x[0-9a-f]{40}$'
        AND old_wallet_address <> new_wallet_address
    ),
    CONSTRAINT execution_wallet_migrations_chain_shape CHECK (chain_id > 0),
    CONSTRAINT execution_wallet_migrations_balance_shape CHECK (
        collateral_balance_before >= 0 AND collateral_balance_after >= 0
    ),
    CONSTRAINT execution_wallet_migrations_evidence_object CHECK (
        jsonb_typeof(evidence) = 'object' AND evidence <> '{}'::jsonb
    )
);

CREATE INDEX execution_wallet_migrations_account_new_wallet_idx
    ON execution_wallet_migrations (
        execution_account_id, new_wallet_address, occurred_at DESC
    );

CREATE FUNCTION execution_reject_wallet_migration_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'execution wallet migrations are append-only';
END;
$$;

CREATE TRIGGER execution_wallet_migrations_append_only_trigger
BEFORE UPDATE OR DELETE OR TRUNCATE ON execution_wallet_migrations
FOR EACH STATEMENT
EXECUTE FUNCTION execution_reject_wallet_migration_mutation();

COMMIT;
