BEGIN;

-- Removes the objects installed by the retired 0035_wallet_reuse_cutover
-- migration: four append-only audit tables, the SECURITY DEFINER apply
-- function, their guard functions/trigger, and the extra position_events
-- event type. None of them is used any more; the main/wallet-1 reuse is a
-- plain strategy rebinding (deploy/rebind_main_wallet1.sql). Everything is
-- IF EXISTS so it is a no-op on databases that never had 0035.
--
-- It refuses to run if any audit row or CUTOVER_ADJUSTED event exists, so it
-- can never discard evidence of an applied cutover.
DO $$
DECLARE
    audit_table TEXT;
    row_count BIGINT;
BEGIN
    FOREACH audit_table IN ARRAY ARRAY[
        'execution_wallet_cutover_batches',
        'execution_wallet_cutover_accounts',
        'execution_wallet_cutover_assets',
        'execution_wallet_cutover_lot_adjustments'
    ] LOOP
        IF to_regclass(audit_table) IS NOT NULL THEN
            EXECUTE format('SELECT count(*) FROM %I', audit_table) INTO row_count;
            IF row_count <> 0 THEN
                RAISE EXCEPTION '% holds % row(s); refusing to drop applied cutover evidence', audit_table, row_count;
            END IF;
        END IF;
    END LOOP;
    SELECT count(*) INTO row_count FROM position_events WHERE event_type = 'CUTOVER_ADJUSTED';
    IF row_count <> 0 THEN
        RAISE EXCEPTION 'position_events holds % CUTOVER_ADJUSTED row(s); refusing to drop them', row_count;
    END IF;
END
$$;

DROP TRIGGER IF EXISTS execution_wallet_cutover_position_event_guard ON position_events;
DROP FUNCTION IF EXISTS execution_wallet_cutover_position_event_guard();
DROP FUNCTION IF EXISTS execution_apply_wallet_reuse_cutover(JSONB, TEXT, TEXT, TEXT, TEXT);
DROP TABLE IF EXISTS
    execution_wallet_cutover_lot_adjustments,
    execution_wallet_cutover_assets,
    execution_wallet_cutover_accounts,
    execution_wallet_cutover_batches;
DROP FUNCTION IF EXISTS execution_reject_wallet_cutover_mutation();

ALTER TABLE position_events
    DROP CONSTRAINT IF EXISTS position_events_event_type_check,
    ADD CONSTRAINT position_events_event_type_check CHECK (
        event_type IN ('BOUGHT','ADOPTED','SOLD','MARKED','SETTLED','REDEEMED')
    );

COMMIT;
