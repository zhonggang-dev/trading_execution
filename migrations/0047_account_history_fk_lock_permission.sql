BEGIN;
SET LOCAL search_path=public,pg_temp;

-- PostgreSQL's FK check locks the referenced history row FOR KEY SHARE.
-- That lock needs UPDATE permission on at least one column even when the
-- application owns this append-only table. Migration 0044 revoked its own
-- UPDATE ACL; superuser-only tests did not exercise the runtime restriction.
-- A column grant is sufficient for the FK lock. The statement-level immutable
-- trigger still rejects every actual UPDATE, DELETE, and TRUNCATE operation.
DO $$ BEGIN
 IF to_regrole('trading_execution_app') IS NOT NULL THEN
  GRANT UPDATE(history_id) ON account_price_history_windows TO trading_execution_app;
 END IF;
END $$;
COMMIT;
