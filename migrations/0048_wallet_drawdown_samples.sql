BEGIN;
SET LOCAL search_path=public,pg_temp;
-- Read-only risk monitoring samples: one row per account per sampling slot.
-- Equity = on-chain pUSD + Data API position marks. Nothing here feeds the
-- cash ledger, positions or order flow.
CREATE TABLE wallet_drawdown_samples (
 execution_account_id TEXT NOT NULL CHECK(execution_account_id<>''),
 slot_at TIMESTAMPTZ NOT NULL,
 sampled_at TIMESTAMPTZ NOT NULL CHECK(sampled_at>=slot_at),
 cash_pusd NUMERIC NOT NULL CHECK(cash_pusd>=0),
 cash_block BIGINT NOT NULL CHECK(cash_block>0),
 positions_value NUMERIC NOT NULL CHECK(positions_value>=0),
 equity NUMERIC NOT NULL,
 window_minutes INTEGER NOT NULL CHECK(window_minutes>0),
 window_peak_equity NUMERIC,
 window_peak_at TIMESTAMPTZ,
 drawdown NUMERIC CHECK(drawdown IS NULL OR drawdown>=0),
 threshold_usd NUMERIC NOT NULL CHECK(threshold_usd>0),
 exceeded BOOLEAN NOT NULL,
 notified BOOLEAN NOT NULL DEFAULT FALSE,
 notify_error TEXT,
 positions JSONB NOT NULL CHECK(jsonb_typeof(positions)='array'),
 recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(execution_account_id,slot_at),
 CHECK((drawdown IS NULL)=(window_peak_equity IS NULL) AND (drawdown IS NULL)=(window_peak_at IS NULL)),
 CHECK(NOT exceeded OR drawdown IS NOT NULL)
);
CREATE INDEX wallet_drawdown_samples_sampled_idx ON wallet_drawdown_samples(execution_account_id,sampled_at);
REVOKE ALL ON wallet_drawdown_samples FROM PUBLIC;
DO $$ BEGIN
 IF to_regrole('trading_execution_app') IS NOT NULL THEN
  GRANT SELECT,INSERT ON wallet_drawdown_samples TO trading_execution_app;
  GRANT UPDATE(notified,notify_error) ON wallet_drawdown_samples TO trading_execution_app;
 END IF;
END $$;
COMMIT;
