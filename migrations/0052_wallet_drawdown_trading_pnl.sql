BEGIN;
SET LOCAL search_path=public,pg_temp;

DO $$
DECLARE
    legacy_peak_check TEXT;
BEGIN
    SELECT conname INTO STRICT legacy_peak_check
    FROM pg_constraint
    WHERE conrelid = 'wallet_drawdown_samples'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%window_peak_equity%';
    EXECUTE format('ALTER TABLE wallet_drawdown_samples DROP CONSTRAINT %I', legacy_peak_check);
END;
$$;

ALTER TABLE wallet_drawdown_samples
    ALTER COLUMN cash_pusd DROP NOT NULL,
    ALTER COLUMN cash_block DROP NOT NULL,
    ALTER COLUMN equity DROP NOT NULL,
    ADD COLUMN realized_pnl NUMERIC,
    ADD COLUMN unrealized_pnl NUMERIC,
    ADD COLUMN total_pnl NUMERIC,
    ADD COLUMN window_peak_pnl NUMERIC,
    ADD CONSTRAINT wallet_drawdown_sample_basis CHECK (
        (total_pnl IS NULL AND realized_pnl IS NULL AND unrealized_pnl IS NULL AND window_peak_pnl IS NULL
          AND cash_pusd IS NOT NULL AND cash_block IS NOT NULL AND equity IS NOT NULL
          AND (drawdown IS NULL) = (window_peak_equity IS NULL))
        OR
        (total_pnl IS NOT NULL AND realized_pnl IS NOT NULL AND unrealized_pnl IS NOT NULL
          AND cash_pusd IS NULL AND cash_block IS NULL AND equity IS NULL AND window_peak_equity IS NULL
          AND (drawdown IS NULL) = (window_peak_pnl IS NULL))
    ),
    ADD CONSTRAINT wallet_drawdown_peak_time_shape CHECK ((drawdown IS NULL) = (window_peak_at IS NULL));

COMMIT;
