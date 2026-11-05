BEGIN;
SET LOCAL search_path=public,pg_temp;
-- These are market observations, not position, order, or cash ledger rows.
CREATE TABLE account_price_history_windows (
 history_id BIGSERIAL PRIMARY KEY,
 asset_key TEXT NOT NULL CHECK(asset_key ~ '^[0-9a-f]{64}$'),
 asset JSONB NOT NULL CHECK(jsonb_typeof(asset)='object'),
 window_from TIMESTAMPTZ NOT NULL,
 window_to TIMESTAMPTZ NOT NULL CHECK(window_to>=window_from),
 evidence_sha256 TEXT NOT NULL CHECK(evidence_sha256 ~ '^[0-9a-f]{64}$'),
 series JSONB NOT NULL CHECK(jsonb_typeof(series)='object'),
 recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(asset_key,window_from,window_to,evidence_sha256)
);
CREATE INDEX account_price_history_asset_window_idx ON account_price_history_windows(asset_key,window_from,window_to,history_id);
-- Rebuildable bounded read projections; immutable windows above retain audit.
CREATE TABLE account_price_history_points (
 asset_key TEXT NOT NULL, point_at TIMESTAMPTZ NOT NULL, valid_until TIMESTAMPTZ,
 fetched_at TIMESTAMPTZ NOT NULL, history_id BIGINT NOT NULL REFERENCES account_price_history_windows,
 point JSONB NOT NULL CHECK(jsonb_typeof(point)='object'), PRIMARY KEY(asset_key,point_at)
);
CREATE INDEX account_price_history_point_validity_idx ON account_price_history_points(asset_key,valid_until) WHERE valid_until IS NOT NULL;
CREATE TABLE account_price_history_settlements (
 asset_key TEXT NOT NULL,effective_at TIMESTAMPTZ NOT NULL,fetched_at TIMESTAMPTZ NOT NULL,
 history_id BIGINT NOT NULL REFERENCES account_price_history_windows,
 settlement JSONB NOT NULL CHECK(jsonb_typeof(settlement)='object'),PRIMARY KEY(asset_key,effective_at)
);
CREATE TABLE account_price_history_coverage (
 asset_key TEXT NOT NULL,coverage_key TEXT NOT NULL,window_from TIMESTAMPTZ NOT NULL,
 window_to TIMESTAMPTZ NOT NULL CHECK(window_to>=window_from),state TEXT NOT NULL CHECK(state IN ('complete','missing','error')),
 reason TEXT NOT NULL,fetched_at TIMESTAMPTZ NOT NULL,
 history_id BIGINT NOT NULL REFERENCES account_price_history_windows,PRIMARY KEY(asset_key,coverage_key)
);
CREATE INDEX account_price_history_coverage_window_idx ON account_price_history_coverage(asset_key,window_from,window_to);
CREATE FUNCTION reject_account_price_history_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'account price history is append-only'; END $$;
CREATE TRIGGER account_price_history_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON account_price_history_windows
 FOR EACH STATEMENT EXECUTE FUNCTION reject_account_price_history_mutation();
REVOKE ALL ON account_price_history_windows,account_price_history_points,account_price_history_settlements,account_price_history_coverage FROM PUBLIC;
DO $$ BEGIN
 IF to_regrole('trading_execution_app') IS NOT NULL THEN
  GRANT SELECT,INSERT ON account_price_history_windows TO trading_execution_app;
  GRANT SELECT,INSERT,UPDATE,DELETE ON account_price_history_points,account_price_history_settlements,account_price_history_coverage TO trading_execution_app;
  GRANT USAGE,SELECT ON SEQUENCE account_price_history_windows_history_id_seq TO trading_execution_app;
  REVOKE UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON account_price_history_windows FROM trading_execution_app;
 END IF;
END $$;
COMMIT;
