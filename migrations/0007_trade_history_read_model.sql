BEGIN;

-- The console reads only authoritative fills that have already been applied to
-- cash and positions. These partial indexes keep time-range and account views
-- bounded without adding write overhead for transient MATCHED/MINED rows.
CREATE INDEX execution_fills_history_time_idx
    ON execution_fills (matched_at DESC, fill_key DESC)
    WHERE status = 'CONFIRMED' AND applied_at IS NOT NULL;

CREATE INDEX execution_fills_history_account_idx
    ON execution_fills (execution_account_id, matched_at DESC, fill_key DESC)
    WHERE status = 'CONFIRMED' AND applied_at IS NOT NULL;

COMMIT;
