BEGIN;

-- 买入单在创建时记录下单依据的补充信息，供逐笔复盘：当时官方标注的市场到期日
-- （market_end_at，UTC）和市场所属的全部 domain（domains，字符串数组）。用 JSONB
-- 保存，后续要补充别的复盘字段时不需要再迁移。值来自同一轮冻结的预测快照；卖出单、
-- 人工下单以及预测里两项都缺失时为 NULL。列只在 INSERT 时写入一次，不参与订单身份、
-- 风控或状态流转。
ALTER TABLE execution_orders
    ADD COLUMN decision_meta JSONB;

ALTER TABLE execution_orders
    ADD CONSTRAINT execution_orders_decision_meta_object CHECK (
        decision_meta IS NULL OR jsonb_typeof(decision_meta) = 'object'
    ) NOT VALID;

ALTER TABLE execution_orders VALIDATE CONSTRAINT execution_orders_decision_meta_object;

COMMIT;
