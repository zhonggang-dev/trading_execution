BEGIN;

-- 聚合器按账户、Token 和时间读取最近订单及市场展示元数据，避免后台刷新扫描全部历史订单。
CREATE INDEX execution_orders_live_operations_idx
	ON execution_orders (execution_account_id, updated_at DESC, order_id DESC);

CREATE INDEX execution_orders_live_position_label_idx
	ON execution_orders (execution_account_id, token_id, created_at DESC, order_id DESC);

CREATE INDEX execution_order_events_live_operations_idx
    ON execution_order_events (occurred_at DESC, order_id, event_id);

CREATE INDEX reconciliation_issues_live_operations_idx
    ON reconciliation_issues (execution_account_id, observed_at DESC, issue_id);

-- 外部调度器和本进程通过这一小张表发布非交易性 heartbeat。表中禁止保存配置、凭证或策略输入。
CREATE TABLE live_runtime_status (
    thread_id TEXT NOT NULL CHECK (thread_id IN ('cycle', 'monitor', 'prediction')),
    run_id TEXT NOT NULL,
    name TEXT NOT NULL,
    purpose TEXT NOT NULL,
    cadence TEXT NOT NULL,
    max_heartbeat_age_ms BIGINT NOT NULL CHECK (max_heartbeat_age_ms > 0),
    last_heartbeat_at TIMESTAMPTZ NOT NULL,
    current_task TEXT NOT NULL DEFAULT '',
    current_cycle_id TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT '',
    metric_label TEXT NOT NULL DEFAULT '',
    metric_value TEXT NOT NULL DEFAULT '',
    stopped BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, thread_id),
    CONSTRAINT live_runtime_status_identity_nonempty CHECK (
        run_id <> '' AND name <> '' AND purpose <> '' AND cadence <> ''
    )
);

CREATE INDEX live_runtime_status_updated_idx
    ON live_runtime_status (updated_at DESC, run_id, thread_id);

-- 一轮漏斗必须由同一个 run_id/cycle_id 写入，聚合接口只读取最新一轮，禁止跨轮相加。
CREATE TABLE live_cycle_funnel (
    run_id TEXT NOT NULL,
    cycle_id TEXT NOT NULL,
    stage_id TEXT NOT NULL CHECK (stage_id IN ('scan', 'filter', 'predict', 'risk', 'route', 'ledger')),
    stage_index SMALLINT NOT NULL CHECK (stage_index BETWEEN 1 AND 6),
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    count BIGINT NOT NULL DEFAULT 0 CHECK (count >= 0),
    throughput_label TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL CHECK (state IN ('done', 'active', 'warning', 'idle')),
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (run_id, cycle_id, stage_id),
    UNIQUE (run_id, cycle_id, stage_index),
    CONSTRAINT live_cycle_funnel_identity_nonempty CHECK (run_id <> '' AND cycle_id <> '')
);

CREATE INDEX live_cycle_funnel_latest_idx
    ON live_cycle_funnel (run_id, observed_at DESC, cycle_id, stage_index);

COMMIT;
