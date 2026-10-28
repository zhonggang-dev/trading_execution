BEGIN;

-- 买入单在创建时记录本轮决策使用的盘口（YES/NO 买一、卖一）和模型给出的 YES
-- 概率，便于逐笔回看下单依据。值来自同一轮冻结的 strategy_orderbook_snapshots 与
-- 预测快照；卖出单、人工下单以及缺失的一侧都为 NULL。列只在 INSERT 时写入一次，
-- 不参与订单身份、风控或状态流转。
ALTER TABLE execution_orders
    ADD COLUMN decision_yes_bid NUMERIC,
    ADD COLUMN decision_yes_ask NUMERIC,
    ADD COLUMN decision_no_bid NUMERIC,
    ADD COLUMN decision_no_ask NUMERIC,
    ADD COLUMN decision_yes_probability NUMERIC;

ALTER TABLE execution_orders
    ADD CONSTRAINT execution_orders_decision_quotes_range CHECK (
        (decision_yes_bid IS NULL OR decision_yes_bid BETWEEN 0 AND 1)
        AND (decision_yes_ask IS NULL OR decision_yes_ask BETWEEN 0 AND 1)
        AND (decision_no_bid IS NULL OR decision_no_bid BETWEEN 0 AND 1)
        AND (decision_no_ask IS NULL OR decision_no_ask BETWEEN 0 AND 1)
        AND (decision_yes_probability IS NULL OR decision_yes_probability BETWEEN 0 AND 1)
    ) NOT VALID;

ALTER TABLE execution_orders VALIDATE CONSTRAINT execution_orders_decision_quotes_range;

COMMIT;
