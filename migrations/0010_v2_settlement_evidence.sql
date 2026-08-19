BEGIN;

-- Preserve the CLOB V2 curve inputs independently from the legacy
-- fee_rate_bps metadata. The empty JSON object is reserved for historical and
-- non-Polygon fills; an authoritative Polygon fill must carry the complete
-- finalized OrderFilled proof.
ALTER TABLE execution_fills
    ADD COLUMN platform_fee_rate NUMERIC NOT NULL DEFAULT 0,
    ADD COLUMN fee_exponent NUMERIC NOT NULL DEFAULT 0,
    ADD COLUMN settlement_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD CONSTRAINT execution_fills_platform_fee_rate_nonnegative
        CHECK (platform_fee_rate >= 0),
    ADD CONSTRAINT execution_fills_fee_exponent_shape
        CHECK (fee_exponent >= 0 AND fee_exponent = trunc(fee_exponent)),
    ADD CONSTRAINT execution_fills_settlement_evidence_object
        CHECK (jsonb_typeof(settlement_evidence) = 'object'),
    ADD CONSTRAINT execution_fills_polygon_settlement_evidence_shape
        CHECK ((
            (
                fee_source <> 'POLYGON_V2_ORDER_FILLED'
                AND settlement_evidence = '{}'::jsonb
            )
            OR
            (
                fee_source = 'POLYGON_V2_ORDER_FILLED'
                AND status = 'CONFIRMED'
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
        ) IS TRUE);

-- One on-chain log can settle exactly one local fill. This prevents replaying
-- an otherwise valid receipt under a different fill or order identity.
CREATE UNIQUE INDEX execution_fills_polygon_settlement_event_uidx
    ON execution_fills (
        ((settlement_evidence->>'chain_id')::numeric),
        ((settlement_evidence->>'exchange_address')),
        ((settlement_evidence->>'transaction_hash')),
        ((settlement_evidence->>'log_index')::numeric)
    )
    WHERE fee_source = 'POLYGON_V2_ORDER_FILLED';

-- Migrations 0005 and 0008 deliberately installed these as NOT VALID so the
-- service could first reconcile legacy rows. A live-capable schema must no
-- longer report ready while historical reservation identity or fee accounting
-- remains outside the invariant.
ALTER TABLE asset_reservations
    VALIDATE CONSTRAINT asset_reservations_target_lot_fk;
ALTER TABLE asset_reservations
    VALIDATE CONSTRAINT asset_reservations_target_lot_shape;
ALTER TABLE asset_reservations
    VALIDATE CONSTRAINT asset_reservations_buy_initial_reserve_identity;
ALTER TABLE asset_reservations
    VALIDATE CONSTRAINT asset_reservations_buy_spend_within_reserve;

COMMIT;
