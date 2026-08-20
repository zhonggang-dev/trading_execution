# 10 分钟策略决策周期

`Trading Execution` 同时包含两个明确分层的应用能力：

1. `decisioncycle` 只负责编排外部数据和外部策略；
2. `execution` 只负责幂等下单、交易所硬限制、状态和对账。

策略规则、edge、概率聚合、MOM/MACD 和动态仓位仍全部属于外部策略服务。

Go 启动时注入绑定表，例如：

```text
model-a / strategy-v1 / account-model-a-strategy-v1
model-a / strategy-v2 / account-model-a-strategy-v2
model-b / strategy-v1 / account-model-b-strategy-v1
model-b / strategy-v2 / account-model-b-strategy-v2
model-c / strategy-v1 / account-model-c-strategy-v1
model-c / strategy-v2 / account-model-c-strategy-v2
```

框架拒绝重复的 model/strategy 组合，也拒绝把同一个 execution account 绑定两次。

## 周期顺序

```text
10-minute UTC boundary T
  -> GET prediction_infra snapshot(as_of=T)
  -> select predictions belonging to configured model IDs
  -> load every binding's OPEN position lots
  -> capture CLOB books and [T-48h,T] midpoint histories once for the prediction/position token union
  -> normalize bids DESC / asks ASC / top 15 each side
  -> expand configured (model, strategy, execution account) bindings
  -> one isolated trading.strategy_input.v4 per binding
  -> POST strategy /api/v4/decisions (Idempotency-Key: cycle_id)
  -> validate context and durably record trading.strategy_output.v4
  -> submit each OrderIntent through execution.Service
  -> Go Hard Risk rejects unsafe intents without changing strategy fields
  -> Market Validation re-checks authoritative market state and fresh venue price
  -> execution.Service places the validated order
```

当前快照在边界后采集，等价于旧 `mult_factor_v2` 的逻辑时间 T + 当前盘口模式。
如果以后要求与 `poly_parity` 完全一致，可以只替换 `OrderBookSource`，在 T-2 秒冻结
盘口；策略输入协议和执行协议都不需要改变。

## 策略输入

```json
{
  "schema_version": "trading.strategy_input.v4",
  "cycle_id": "account-model-a-strategy-v1:20260818T042000Z",
  "input_id": "strategy-input-...",
  "context": {
    "model_id": "model-a",
    "strategy_id": "strategy-v1",
    "execution_account_id": "account-model-a-strategy-v1"
  },
  "decision_at": "2026-08-18T04:20:00Z",
  "generated_at": "2026-08-18T04:20:04Z",
  "prediction_snapshot_id": "predsnap-...",
  "prediction_scope": "ALL_EFFECTIVE_AT_DECISION_AT",
  "predictions": [],
  "positions": [],
  "orderbooks": [],
  "mid_price_histories": [],
  "execution_constraints": {
    "size_unit": "SHARES",
    "size_decimal_places": 2,
    "buy_notional_decimal_places": 2,
    "minimum_buy_notional": "1",
    "allowed_time_in_force": ["FOK"],
    "price_protection_policy": "EXACT_TOP_OF_BOOK"
  }
}
```

`predictions` 每个周期发送 `model.name == context.model_id` 的全部当前有效 producer 结果，不是增量；每条结果包含两个
按原始 Outcome 顺序对齐的概率和 token。不同策略处理同一模型时复用完全相同的冻结
prob 和盘口，但使用不同 `strategy_id`、`execution_account_id`、`cycle_id` 和 `input_id`。
`positions` 按开仓批次发送当前 binding 的全部 OPEN lot，不按 token 聚合。`orderbooks` 覆盖
prediction 和 position 的 token 并固定请求 top 15，每侧返回实际存在的最多 15 档；价格和数量必须使用 JSON string decimal。
`mid_price_histories` 也按 token 一一对应，`mid_prices[].p` 直接来自 Polymarket
`/prices-history` 的 `p`，不进行二次平均；默认窗口为 48 小时、源粒度为一分钟、原始点不重采样不补值。
时间戳使用唯一字段 `interval_end_at`，由上游 `t` 按 UTC `ceil('min')` 归一化。

单个盘口失败不会伪装成空数据：`status` 使用 `OK`、`EMPTY`、`MISSING` 或 `ERROR`；
历史价格额外支持 `PARTIAL`，
策略必须 fail closed 或记录拒绝原因。

## 策略输出

策略必须原样回显可信 `context`，并为每个 `(prediction_id, token_id)` 返回一条买入 `SKIP` 或 `SUBMIT`
evaluation；按手卖出通过 `exits[].lot_id` 返回。entry 和 exit 都只能使用 `LIMIT + FOK`。
完整 HTTP 请求、响应字段和错误码见
[`strategy-http-api.md`](strategy-http-api.md)。Trading Execution 根据 `SUBMIT` 的订单
参数生成内部 `OrderIntent`；venue 和 `client_order_id` 不由策略服务指定。
Python 响应的 `decided_at` 会成为 `OrderIntent.signal_at`，供 Go 硬风控执行信号时效检查。

## 审计和失败语义

- 概率快照失败：整个周期失败，不使用上一次内存数据；
- 单个模型/策略绑定失败：保留该绑定错误，继续执行其他独立账户；
- 某个盘口失败：在输入中明确标记，由策略拒绝该 token；
- 周期输入通过 `ClaimInput` 原子持久化；重试只能复用原快照；
- 策略输出通过 `ClaimOutput` 原子持久化；未成功持久化时不执行订单；
- 进程启动时会在发布下一周期前接管并清空旧进程留下的全部 intent lease；任一恢复失败都会阻止调度器就绪，
  不会把旧信号与新周期静默叠加；
- 提交开关打开时，策略输出和全部 OrderIntent 在同一事务中写入
  `strategy_order_intent_deliveries`；worker 使用 `PENDING -> SUBMITTING -> terminal` 状态、
  有界租约和 attempt fencing 投递，进程崩溃后仍以稳定 `client_order_id` 恢复；
- 执行层已经返回 `UNKNOWN/MANUAL_REVIEW` 的 intent 不会自动重投，必须由订单对账继续处理；
- 单个订单失败：继续处理其余独立 intent，并返回组合错误；
- 周期重试：账户级 `cycle_id`、策略幂等键和 `client_order_id` 共同避免重复交易；
- 生产 Runner 只在精确的十分钟 UTC 边界运行，并可在边界后等待配置的启动延迟；进程
  启动过晚、线程暂停超过 `DECISION_CYCLE_MAX_START_LATENESS` 或上一轮运行过长时直接跳过
  已经错过的边界，不补跑陈旧交易；
- Runner 串行执行且有单轮 timeout；单轮失败写日志并使 readiness 降级，但循环继续等待
  下一边界，后续成功会自动恢复；
- readiness 在首个完整周期成功前保持失败；调度器错过允许启动窗口也会降级，而不会保持假绿；
- `DECISION_CYCLE_ENABLED=true` 只打开快照、行情、算法调用及 PostgreSQL 输入/输出审计。
  `DECISION_CYCLE_ORDER_SUBMISSION_ENABLED` 默认 `false`，关闭时仍构建并记录合法 OrderIntent，
  但明确标记 `submission_disabled` 且绝不调用 `execution.Submit`；
- Go 绑定表保证一个 `(model_id, strategy_id)` 只对应一个唯一 `execution_account_id`，同一
  execution account 不能重复绑定。

生产装配使用 migrations `0012_strategy_decision_cycles.sql`、
`0013_strategy_intent_deliveries.sql` 和 PostgreSQL `DecisionRecorder` 持久化完整输入、输出与
订单意图投递状态。配置通过 `DECISION_CYCLE_BINDINGS_JSON` 注入，并要求绑定账户存在于
受限钱包文件。只有 live 模式且两个显式开关都打开，OrderIntent 才会进入执行层；之后仍受
数据库 Kill Switch、账户/策略暂停、binding、余额和 reconciliation freshness 等硬风控阻断。
最新 BBO 和价格保护由独立 Market Validation 层负责，详见
[`market-validation.md`](market-validation.md)；余额、仓位、敞口与暂停检查由 Go Hard Risk
负责，详见 [`risk-control.md`](risk-control.md)。

`multfactor_v1` 不依赖小时因子，因此调度器不会为仅由 v1 使用的 Token 请求
48 小时历史数据；冻结输入仍保留一条 `MISSING` 占位记录，并使用
`error_code=NOT_REQUIRED_FOR_MULTFACTOR_V1` 明确表示“未请求”而不是上游故障。
混合 v1/v2 周期中，历史源故障只会让 v2 绑定失败关闭，v1 仍可基于同一冻结盘口完成审计决策。
