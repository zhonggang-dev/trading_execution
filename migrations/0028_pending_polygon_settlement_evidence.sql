BEGIN;

-- A validated canonical receipt may still be below the configured confirmation
-- depth. Persist its evidence as MINED, without booking cash, shares or releasing
-- reservations. The existing application_shape constraint still requires
-- CONFIRMED before applied_at can be set; all exact proof checks remain intact.
ALTER TABLE execution_fills
    DROP CONSTRAINT execution_fills_polygon_settlement_evidence_shape,
    ADD CONSTRAINT execution_fills_polygon_settlement_evidence_shape
        CHECK ((
            (
                fee_source <> 'POLYGON_V2_ORDER_FILLED'
                AND settlement_evidence = '{}'::jsonb
            )
            OR
            (
                fee_source = 'POLYGON_V2_ORDER_FILLED'
                AND status IN ('MINED', 'CONFIRMED')
                AND (status = 'CONFIRMED' OR (confirmed_at IS NULL AND applied_at IS NULL))
                AND settlement_evidence ?& ARRAY[
                    'schema_version', 'source', 'chain_id',
                    'exchange_address', 'transaction_hash', 'block_number',
                    'block_hash', 'log_index', 'confirmations', 'order_hash',
                    'maker_address', 'token_id', 'side',
                    'maker_amount_base_units', 'taker_amount_base_units',
                    'total_fee_base_units', 'builder_code',
                    'builder_fee_known', 'builder_fee_base_units',
                    'builder_fee_source', 'collateral_decimals',
                    'outcome_token_decimals'
                ]
                AND settlement_evidence - ARRAY[
                    'schema_version', 'source', 'chain_id',
                    'exchange_address', 'transaction_hash', 'block_number',
                    'block_hash', 'log_index', 'confirmations', 'order_hash',
                    'maker_address', 'token_id', 'side',
                    'maker_amount_base_units', 'taker_amount_base_units',
                    'total_fee_base_units', 'builder_code',
                    'builder_fee_known', 'builder_fee_base_units',
                    'builder_fee_source', 'collateral_decimals',
                    'outcome_token_decimals'
                ] = '{}'::jsonb
                AND settlement_evidence->>'schema_version' = 'trading.settlement_evidence.v1'
                AND settlement_evidence->>'source' = 'POLYGON_V2_ORDER_FILLED'
                AND settlement_evidence->>'chain_id' = '137'
                AND settlement_evidence->>'exchange_address' ~ '^0x[0-9a-f]{40}$'
                AND settlement_evidence->>'transaction_hash' ~ '^0x[0-9a-f]{64}$'
                AND settlement_evidence->>'block_hash' ~ '^0x[0-9a-f]{64}$'
                AND settlement_evidence->>'order_hash' ~ '^0x[0-9a-f]{64}$'
                AND settlement_evidence->>'maker_address' ~ '^0x[0-9a-f]{40}$'
                AND settlement_evidence->>'builder_code' ~ '^0x[0-9a-f]{64}$'
                AND settlement_evidence->>'transaction_hash' = lower(transaction_hash)
                AND settlement_evidence->>'order_hash' = lower(venue_order_id)
                AND settlement_evidence->>'token_id' = token_id
                AND settlement_evidence->>'side' = side
                AND jsonb_typeof(settlement_evidence->'block_number') = 'number'
                AND (settlement_evidence->>'block_number')::numeric > 0
                AND (settlement_evidence->>'block_number')::numeric
                    = trunc((settlement_evidence->>'block_number')::numeric)
                AND jsonb_typeof(settlement_evidence->'log_index') = 'number'
                AND (settlement_evidence->>'log_index')::numeric >= 0
                AND (settlement_evidence->>'log_index')::numeric
                    = trunc((settlement_evidence->>'log_index')::numeric)
                AND jsonb_typeof(settlement_evidence->'confirmations') = 'number'
                AND (settlement_evidence->>'confirmations')::numeric > 0
                AND (settlement_evidence->>'confirmations')::numeric
                    = trunc((settlement_evidence->>'confirmations')::numeric)
                AND settlement_evidence->>'maker_amount_base_units' ~ '^(0|[1-9][0-9]*)$'
                AND settlement_evidence->>'taker_amount_base_units' ~ '^(0|[1-9][0-9]*)$'
                AND settlement_evidence->>'total_fee_base_units' ~ '^(0|[1-9][0-9]*)$'
                AND settlement_evidence->>'builder_fee_base_units' ~ '^(0|[1-9][0-9]*)$'
                AND jsonb_typeof(settlement_evidence->'builder_fee_known') = 'boolean'
                AND settlement_evidence->>'builder_fee_known' = 'true'
                AND settlement_evidence->>'collateral_decimals' = '6'
                AND settlement_evidence->>'outcome_token_decimals' = '6'
            )
        ) IS TRUE),
    ADD CONSTRAINT execution_fills_pending_polygon_evidence_unapplied
        CHECK (fee_source <> 'POLYGON_V2_ORDER_FILLED' OR status <> 'MINED'
            OR (confirmed_at IS NULL AND applied_at IS NULL));

COMMIT;
