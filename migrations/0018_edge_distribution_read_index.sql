BEGIN;

-- 控制台只读取最新全局决策边界，并可按逻辑模型缩小范围；该索引保证审计历史增长后查询仍有界。
CREATE INDEX strategy_decision_runs_decision_model_idx
    ON strategy_decision_runs (decision_at DESC, model_id);

COMMIT;
