# 10 分钟策略决策周期

`Trading Execution` 同时包含两个明确分层的应用能力：

1. `decisioncycle` 只负责编排外部数据和外部策略；
2. `execution` 只负责幂等下单、交易所硬限制、状态和对账。

策略规则、edge、概率聚合、MOM/MACD 和动态仓位仍全部属于外部策略服务。

Go 启动时注入绑定表，例如：

```text
echo / multfactor_v2 / main
echo / multfactor_v1 / wallet-1
gemini_masked / multfactor_v1 / wallet-6
gemini_masked / multfactor_v2 / wallet-7
```

框架拒绝重复的 model/strategy 组合，也拒绝把同一个 execution account 绑定两次。
这里的 `model_id` 是策略、订单和风控使用的稳定业务身份。可选的
`prediction_model_id` 是 prediction snapshot 里 `model.name` 的精确值；两者不同时，
Trading 先按 `prediction_model_id` 选择 Market，再在发送副本中把 `prediction.model.name`
投影为 `model_id`。Python 协议不增加字段，它仍会看到
`prediction.model.name == context.model_id`。

当前四钱包配置中，`#0` 是现有 literal account ID `main`，不是隐式的
`wallet-0` 别名。上线前必须从当前 PIT snapshot 确认 `prediction_model_id`；
例如已验证过的 masked producer 可能是 `gemini-3.6-flash`，但该值不能由
`gemini_masked` 业务名猜测。

## 周期顺序

```text
10-minute UTC boundary T
  -> GET prediction_infra snapshot(as_of=T)
  -> filter each source model by its configured DIRECT/SANDBOX provenance
  -> select the newest fresh PIT result per Market/source model
  -> load every binding's OPEN position lots
  -> capture CLOB books and [T-48h,T] midpoint histories once for the prediction/position token union
  -> normalize bids DESC / asks ASC / top 15 each side
  -> project source identity to logical model_id
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
  "cycle_id": "main:20260818T042000Z",
  "input_id": "strategy-input-...",
  "context": {
    "model_id": "echo",
    "strategy_id": "multfactor_v1",
    "execution_account_id": "main"
  },
  "decision_at": "2026-08-18T04:20:00Z",
  "generated_at": "2026-08-18T04:20:04Z",
  "prediction_snapshot_id": "predsnap-...",
  "prediction_scope": "ALL_EFFECTIVE_AT_DECISION_AT",
  "predictions": [],
  "positions": [],
  "orderbooks": [],
  "execution_constraints": {
    "size_unit": "SHARES",
    "size_decimal_places": 2,
    "buy_notional_decimal_places": 4,
    "minimum_buy_notional": "1",
    "allowed_time_in_force": ["FOK"],
    "price_protection_policy": "DEPTH_AWARE_LIMIT",
    "max_price_slippage_ticks": 2
  }
}
```

策略返回的 BUY `size` 可保留最多 2 位小数；Trading 在构建订单意图前按四舍五入
转为整数 shares，然后使用转换后的数量执行最小下单量、保护价卖盘流动性、BUY notional 精度和风控校验。
如果数量发生变化，原始值会记录在 intent metadata 的 `strategy_requested_size`。

`DEPTH_AWARE_LIMIT` 允许策略把 `worst_price` 设在同轮盘口最优价到更差最多
`max_price_slippage_ticks`（当前为 2）个 tick 的范围内。BUY 必须满足
`best_ask <= worst_price <= best_ask + 2*tick_size`，并由所有
`ask.price <= worst_price` 的可见档位累计覆盖 FOK shares；SELL 必须满足
`best_bid - 2*tick_size <= worst_price <= best_bid`，并由所有
`bid.price >= worst_price` 的可见档位累计覆盖 FOK shares。保护价必须是
`tick_size` 的整数倍；执行时仍以 `worst_price` 作为限价，不允许无限追价。

`predictions` 每个周期发送当前 binding 所属模型的全部当前有效 Market。一条 Market/Model 预测仍包含两个按原始
Outcome 顺序对齐且和为 1 的概率和 token，而不是只传一个脱离 Outcome 的 `prob`。
Prediction snapshot 保留 lookback 内的 immutable 结果，同时携带由 Direct Prediction task
生成的 `expected_predictions[]`。Trading 的运行时来源契约由
`DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON` 固定：`DIRECT` 模型只接受空
`sandbox_id`，`SANDBOX` 模型只接受非空 `sandbox_id`；配置必须精确覆盖所有 binding 的
上游 `prediction_model_id`。来源过滤在 freshness 与 effective selection 之前执行，因此错误来源中更新的
revision 不会覆盖正确来源。候选结果要求 `prediction_as_of` 和 `completed_at` 均不早于
`decision_at - DECISION_CYCLE_PREDICTION_LOOKBACK`，再按 `prediction_as_of`、
`available_at`、`completed_at` 选择每个 `(market_id, prediction_model_id)` 的最新结果；三类时间戳
完全相同但 payload 不同的 revision 会因歧义直接失败，等价 delivery duplicate 才按 prediction ID
确定性择一。不同模型对同一 Market 的 Condition、Outcome 顺序和 token 必须完全一致。
四钱包部署 preflight 另外对当前 dry-run snapshot 做更严格的来源证据检查：Direct echo 必须有最新
`COMPLETED` manifest task 及其精确、非 Sandbox 结果；每个 active Sandbox 模型必须至少有一条 fresh、
PIT-visible 且 `sandbox_id` 非空的 effective 结果，Sandbox 模型不得伪装成 Direct expectation。
`DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE=true` 时，每个 active binding 的上游模型至少要有一条
符合来源与 freshness 契约的结果；缺失模型只关闭该 binding 的 BUY entry，并继续允许其 OPEN lots 走合法
SELL exit。该门禁不要求不同模型覆盖完全相同的 Market 集合。
Order submission 打开时该覆盖开关是强制项；四钱包生产拓扑必须开启它，dry-run 也建议开启以便先观察完整矩阵。
发往 Python 前已确保 `prediction.model.name == context.model_id`。不同策略处理同一模型时复用完全相同的冻结
prob 和盘口，但使用不同 `strategy_id`、`execution_account_id`、`cycle_id` 和 `input_id`。
某个模型当轮没有 Market 时仍调用对应 binding，因为该钱包可能还有需要退出的 OPEN lots；
严格覆盖模式会关闭整个周期的 entry intent 构建并把该轮标记为失败，不会把这种不完整矩阵视为健康。
运行日志会逐 binding 记录上游模型和 prediction count，空预测不再与“未调用”混淆。
`positions` 按开仓批次发送当前 binding 的全部 OPEN lot，不按 token 聚合。`orderbooks` 覆盖
prediction 和 position 的 token 并固定请求 top 15，每侧返回实际存在的最多 15 档；价格和数量必须使用 JSON string decimal。
Trading 不再抓取或发送 `mid_price_histories`。`multfactor_v2` 计算 MOM/MACD 所需的历史由策略服务
直接读取对应 venue 的官方 prices-history。

单个盘口失败不会伪装成空数据：`status` 使用 `OK`、`EMPTY`、`MISSING` 或 `ERROR`；

## 策略输出

策略必须原样回显可信 `context`，并为每个 `(prediction_id, token_id)` 返回一条买入 `SKIP` 或 `SUBMIT`
evaluation；按手卖出通过 `exits[].lot_id` 返回。entry 和 exit 都只能使用 `LIMIT + FOK`。
完整 HTTP 请求、响应字段、业务校验和错误码见
[`python-algorithm-http-api.md`](python-algorithm-http-api.md)。Trading Execution 根据 `SUBMIT` 的订单
参数生成内部 `OrderIntent`；venue 和 `client_order_id` 不由策略服务指定。
Python 响应的 `decided_at` 会成为 `OrderIntent.signal_at`，供 Go 硬风控执行信号时效检查。
当 model coverage 不完整时，Go 会在收到 Python 响应后追加持久化专用字段
`entry_policy={"enabled":false,"block_reason":"INCOMPLETE_MODEL_COVERAGE"}`。Python 不得发送或决定
该字段；正常周期不写该字段，以兼容升级前的幂等输出。被阻断的 SUBMIT evaluation 仍留在审计输出中，但不会
生成 BUY intent；同一响应里的合法 SELL exit 继续生成、持久化和提交。Runner 的每个 binding 摘要也会记录
`entry_submission_enabled=false` 和同一 block reason，避免把 4/4 Python 调用误判为 4/4 入场健康。

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
- `DECISION_CYCLE_ORDER_SUBMISSION_ENABLED=true` 还要求
  `DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE=true`，否则配置校验和服务装配都会拒绝启动；
- `DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED=true` 是独立的 sell-only 闸门：即使订单提交已开启，
  Trading 仍会在 Go 侧拒绝所有 BUY，只允许当前 managed lot 的合法 SELL exit；
- Go 绑定表保证一个 `(model_id, strategy_id)` 只对应一个唯一 `execution_account_id`，同一
  execution account 不能重复绑定；同一逻辑 `model_id` 不能来自多个 `prediction_model_id`，
  同一 `prediction_model_id` 也不能路由到多个逻辑模型。

生产装配使用 migrations `0012_strategy_decision_cycles.sql`、
`0013_strategy_intent_deliveries.sql` 和 PostgreSQL `DecisionRecorder` 持久化完整输入、输出与
订单意图投递状态。配置通过 `DECISION_CYCLE_BINDINGS_JSON` 注入，并要求绑定账户存在于
受限钱包文件。只有 live 模式且两个显式开关都打开，OrderIntent 才会进入执行层；之后仍受
数据库 Kill Switch、账户/策略暂停、binding、余额和 reconciliation freshness 等硬风控阻断。

四钱包示例配置（`echo` 的上游精确名需在部署前核对）：

```json
[
  {"prediction_model_id":"echo","model_id":"echo","strategy_id":"multfactor_v2","execution_account_id":"main"},
  {"prediction_model_id":"echo","model_id":"echo","strategy_id":"multfactor_v1","execution_account_id":"wallet-1"},
  {"prediction_model_id":"gemini-3.6-flash","model_id":"gemini_masked","strategy_id":"multfactor_v1","execution_account_id":"wallet-6"},
  {"prediction_model_id":"gemini-3.6-flash","model_id":"gemini_masked","strategy_id":"multfactor_v2","execution_account_id":"wallet-7"}
]
```

`execution_strategy_bindings.model_id` 必须使用逻辑名 `echo` / `gemini_masked`，并与
`strategy_id + execution_account_id` 组成相同的四条绑定；上游真实名只存在运行配置中。
因为 SELL intent 和目标 lot 会端到端严格比对 `model_id`，从上游原始名切换到
逻辑名前，四个账户必须没有仍归属旧模型名的 OPEN lot。不能只改 env 或
`execution_strategy_bindings` 就带仓切换；历史已关闭订单也不应被改写。
最新 BBO 和价格保护由独立 Market Validation 层负责，详见
[`market-validation.md`](market-validation.md)；余额、仓位、敞口与暂停检查由 Go Hard Risk
负责，详见 [`risk-control.md`](risk-control.md)。

调度器不会为任何入场决策 Token 请求历史价格，也不会在 v1/v2 冻结输入中保留历史占位记录。
混合 v1/v2 周期只共享冻结盘口；v2 的历史数据可用性由策略服务自己的官方 prices-history 读取负责，
不会再触发 Trading 侧 `STALE_DATA` 映射要求。
