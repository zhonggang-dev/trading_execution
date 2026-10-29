# 10 分钟策略决策周期

`Trading Execution` 同时包含两个明确分层的应用能力：

1. `decisioncycle` 只负责编排外部数据和外部策略；
2. `execution` 只负责幂等下单、交易所硬限制、状态和对账。

策略规则、edge、概率聚合、MOM/MACD 和动态仓位仍全部属于外部策略服务。

一个执行账户就是一个组合：平台 + 资金 + 预测模型 + 策略 + 运行模式。配置只有两部分：

```text
DECISION_CYCLE_PREDICTION_MODEL_ID = v4.1 flash          # 全局唯一的预测模型名
DECISION_CYCLE_ACCOUNTS_JSON（每账户一行）:
  main         POLYMARKET  brier_mid_edge_once  ACTIVE
  wallet-1     POLYMARKET  strategy_pending_b   PAUSED
  wallet-6     POLYMARKET  multfactor_v1        ACTIVE
  wallet-7     POLYMARKET  multfactor_v2        ACTIVE
  kalshi:main  KALSHI      brier_mid_edge_once  ACTIVE
```

- 预测模型名就是发给策略、写进订单和 lot 的 `model_id`，没有另一个内部名；
  `prediction.model.name == context.model_id` 由 Trading 保证。换模型只改这一项。
- 每个账户只交易一个平台，只收到该平台的预测、该账户的 OPEN lot 和对应盘口；
  Kalshi 账户（`kalshi:<name>`）与 Polymarket 钱包平级，有自己的策略和模式，
  不挂在任何钱包下面。
- 持仓只按账户归属：lot 上的 `model_id`/`strategy_id` 是开仓时的审计记录，换模型或换策略后，
  账户里的 lot 由当前策略继续管理。
- `mode`：`ACTIVE` 正常；`SELL_ONLY` 照常调用策略但拦截所有 BUY；`PAUSED` 不再调用策略、
  拦截 BUY，对账与已持久化 intent 的恢复投递照常。
- 账户数量、名字、策略都不固定；同一模型或同一策略可以跑在多个账户上。

## 周期顺序

```text
10-minute UTC boundary T
  -> GET prediction_infra snapshot(as_of=T)
  -> filter each source model by its configured DIRECT/SANDBOX provenance
  -> select the newest fresh PIT result per Market/source model
  -> load every routed account's OPEN position lots (by account only)
  -> capture CLOB books and [T-48h,T] midpoint histories once for the prediction/position token union
  -> normalize bids DESC / asks ASC / top 15 each side
  -> atomically freeze the complete shared orderbook batch in PostgreSQL
  -> one isolated trading.strategy_input.v4 per account, holding only
     that account's venue predictions, its own lots and their books
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
    "model_id": "v4.1 flash",
    "strategy_id": "brier_mid_edge_once",
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
    "allowed_time_in_force": ["IOC", "FOK"],
    "price_protection_policy": "DEPTH_AWARE_LIMIT",
    "max_price_slippage_ticks": 2
  }
}
```

策略返回的 BUY `size` 可保留最多 2 位小数；Trading 在构建订单意图前按四舍五入
转为整数 shares，然后使用转换后的数量执行最小下单量、保护价卖盘流动性、BUY notional 精度和风控校验。
如果数量发生变化，原始值会记录在 intent metadata 的 `strategy_requested_size`。

`size` 是股数上限，`worst_price` 是能接受的最差价，两者都不是资金预算。BUY 从 best ask 往上吃，
吃满 `size` 股停，价格不超过 `worst_price`，花多少算多少；`worst_price` 高于 best ask 的差值是给价格留容错，
不是给钱留额度。例如 `size=42`、`worst_price=0.29`、`best_ask=0.24` 应买到 42 股、花约 10.5 pUSD，
而不是拿 `42×0.29=12.18` 当预算扫成 50.75 股。

对于 Polymarket，`DEPTH_AWARE_LIMIT` 允许策略把 `worst_price` 设在同轮盘口最优价到更差最多
`max_price_slippage_ticks`（当前为 2）个 tick 的范围内。BUY 必须满足 `best_ask <= worst_price`，
且所有 `ask.price <= worst_price` 的可见档位至少有正数深度；保护价内深度不足 `size` 时不拒单，
按能买到的量成交，其余立即取消（见下文 IOC）。SELL 退出不套用这些门禁：Trading 不比较快照 best bid，
也不要求保护价内深度覆盖，见下文“SELL 退出只认策略信号”。保护价必须是
`tick_size` 的整数倍；执行时仍以 `worst_price` 作为限价，不允许无限追价。

Kalshi 盘口可能跳过中间 tick，因此不套用固定的两 tick 距离上限。策略仍必须基于冻结快照给出明确的
`worst_price`，价格方向正确、位于 tick 上，且保护价范围内至少有可见的可成交 shares。
下单前 Go 还会用最新官方盘口重复校验最优价没有越过保护线且保护价内仍有正数深度（仅 BUY）。

`predictions` 每个周期发送配置模型在该账户平台上的全部当前有效 Market。一条 Market/Model 预测仍包含两个按原始
Outcome 顺序对齐且和为 1 的概率和 token，而不是只传一个脱离 Outcome 的 `prob`。
Prediction snapshot 保留 lookback 内的 immutable 结果，同时携带由 Direct Prediction task
生成的 `expected_predictions[]`。Trading 的运行时来源契约由
`DECISION_CYCLE_PREDICTION_SOURCE_MODE`（默认 `SANDBOX`）固定：`DIRECT` 只接受空
`sandbox_id`，`SANDBOX` 只接受非空 `sandbox_id`。来源过滤在 freshness 与 effective selection 之前执行，因此错误来源中更新的
revision 不会覆盖正确来源。候选结果要求 `prediction_as_of` 和 `completed_at` 均不早于
`decision_at - DECISION_CYCLE_PREDICTION_LOOKBACK`，再按 `prediction_as_of`、
`available_at`、`completed_at` 选择每个 `(market_id, prediction_model_id)` 的最新结果；三类时间戳
完全相同但 payload 不同的 revision 会因歧义直接失败，等价 delivery duplicate 才按 prediction ID
确定性择一。不同模型对同一 Market 的 Condition、Outcome 顺序和 token 必须完全一致。
`DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE=true` 时，每个账户在自己的平台上至少要有一条
符合来源与 freshness 契约的结果；缺失时只关闭该账户的 BUY entry，并继续允许其 OPEN lots 走合法
SELL exit（例如某轮没有 Kalshi 预测，只有 `kalshi:main` 进入 sell-only）。
Order submission 打开时该覆盖开关是强制项。
发往 Python 前已确保 `prediction.model.name == context.model_id`。不同策略处理同一模型时复用完全相同的冻结
prob 和盘口，但使用不同 `strategy_id`、`execution_account_id`、`cycle_id` 和 `input_id`。
某个模型当轮没有 Market 时仍调用对应 binding，因为该钱包可能还有需要退出的 OPEN lots；
严格覆盖模式会关闭整个周期的 entry intent 构建并把该轮标记为失败，不会把这种不完整矩阵视为健康。
运行日志会逐 binding 记录上游模型和 prediction count，空预测不再与“未调用”混淆。
`positions` 按开仓批次发送当前账户的全部 OPEN lot，不按 token 聚合；`entry_price` 是开仓成交
`gross_notional ÷ shares` 的完整精度，不是两位小数的 CLOB 展示价。`orderbooks` 覆盖
prediction 和 position 的 token 并固定请求 top 15，每侧返回实际存在的最多 15 档；价格和数量必须使用 JSON string decimal。
Trading 不再抓取或发送 `mid_price_histories`。`multfactor_v2` 计算 MOM/MACD 所需的历史由策略服务
直接读取对应 venue 的官方 prices-history。

单个盘口失败不会伪装成空数据：`status` 使用 `OK`、`EMPTY`、`MISSING` 或 `ERROR`；

## 策略输出

策略必须原样回显可信 `context`，并为每个 `(prediction_id, token_id)` 返回一条买入 `SKIP` 或 `SUBMIT`
evaluation；按手卖出通过 `exits[].lot_id` 返回。entry 和 exit 只能使用 `LIMIT`，
`time_in_force` 必须在 `execution_constraints.allowed_time_in_force`（当前 `["IOC","FOK"]`）内。
Trading 构建执行 intent 时把 Kalshi 订单和所有 Polymarket BUY 转成 `LIMIT + IOC`：在 `worst_price`
内立即成交当时可获得的数量，剩余数量立即取消；原始值记录在 metadata `strategy_time_in_force` /
`execution_time_in_force`。Kalshi 原生支持 IOC。Polymarket CLOB 没有 IOC，而它的 FOK/FAK BUY 是
按 pUSD 预算成交的（预算会在限价内全部花完，盘口好于保护价时买到的 shares 多于 `size`），因此
Polymarket 的 IOC 由执行层模拟：签一张按股数计的 GTC 限价单（限价 `worst_price`，数量为最新盘口在
保护价内可成交的量且不超过 `size`），下单响应返回后立刻撤掉未成交部分。这保证最多买 `size` 股、
价差体现为少花钱；剩余部分在盘口上只停留一次请求往返。Polymarket SELL 保持策略返回的 FOK。
已经冻结持久化的历史 FOK delivery 保留原语义恢复，不会在重启或重放时被改成 IOC。
完整 HTTP 请求、响应字段、业务校验和错误码见
[`python-algorithm-http-api.md`](python-algorithm-http-api.md)。Trading Execution 根据 `SUBMIT` 的订单
参数生成内部 `OrderIntent`；venue 和 `client_order_id` 不由策略服务指定。
Python 响应的 `decided_at` 会成为 `OrderIntent.signal_at`，供 Go 硬风控执行信号时效检查。
当 model coverage 不完整时，Go 会在收到 Python 响应后追加持久化专用字段
`entry_policy={"enabled":false,"block_reason":"INCOMPLETE_MODEL_COVERAGE"}`。Python 不得发送或决定
该字段；正常周期不写该字段，以兼容升级前的幂等输出。被阻断的 SUBMIT evaluation 仍留在审计输出中，但不会
生成 BUY intent；同一响应里的合法 SELL exit 继续生成、持久化和提交。Runner 的每个 binding 摘要也会记录
`entry_submission_enabled=false` 和同一 block reason，避免把 4/4 Python 调用误判为 4/4 入场健康。

## SELL 退出只认策略信号

策略通过 `exits[]` 返回的 SELL 就是卖出指令。Trading 对退出只做身份和格式核对：`lot_id`/`token_id`
必须属于本轮输入的 OPEN lot；`reason_code` 非空即可，不再限定为 `HOLD_48H`；订单必须是
`SELL + LIMIT + FOK/IOC`、无 `expires_at`、数量为正、精度合法且不超过该 lot 的剩余 shares；
`worst_price` 在 `(0,1]` 内。以下检查已从 SELL 路径移除：

- 持有 48 小时门槛和 reason_code 枚举；
- 快照 best bid 与 `worst_price` 的比较、保护价内深度覆盖和 `min_order_size`；
- 冻结盘口必须为 `OK`：该 token 盘口 MISSING/ERROR 时退出仍然生成，`market_snapshot_at`
  退回 `decision_at`，metadata 不记录 `strategy_reference_price`；
- 执行前市场校验里的市场交易状态（resolved/closed/paused/not accepting）、元数据时效、`neg_risk`
  变化、盘口抓取时效、`PRICE_DRIFT` 和 IOC 保护价内深度（SELL IOC 不记录 `executable_size`，
  按策略数量全额提交，模拟 IOC 再撤掉剩余）；Kalshi 侧同样不再检查参考价、价格越界、盘口
  时效和深度；
- 原子预占里的 `PRICE_STALE` / `SIGNAL_STALE` 时效窗；migration `0025` 同步让数据库 SUBMITTING
  trigger 对 SELL 跳过这两项。

SELL 仍然受以下门禁约束：重复卖单（同 lot 或同 token 已有活动 SELL 预留，`DUPLICATE_SELL_ORDER`）；
账户门禁（全局 Kill Switch、账户/策略/市场暂停、binding 未启用、risk policy 未启用、最近一次
COMPLETED 对账超出时效窗或存在 OPEN reconciliation issue）；账本一致性（目标 lot 属于同一账户且
market/token 一致，数量不超过 lot 剩余与 position 可用 shares；lot 的开仓模型/策略不参与比较）；以及 Polymarket 下单前
的 outcome token 余额/授权检查。市场身份、outcome/token 映射、tick 对齐和成功读取的最新盘口证据
仍然必须满足。官方单边或空盘口允许 SELL 提交，记录 `book_status=EMPTY`，不伪造缺失报价；
官方读取失败（ERROR/MISSING）仍拒绝。Polymarket 签名前也不再用 `min_order_size` 拦截 SELL，
由交易所确认是否接受小额退出；shares、金额精度与正数要求不变。单个退出订单在市场校验、预占或下单阶段失败只拒绝该订单，不影响同一响应中的其他
intent；`exits[]` 的身份/格式错误仍会使整个策略响应被拒绝。

## 订单回报（策略占位释放）

`brier_mid_edge_once` 在应答时就给市场“占位”，只有收到终态回报才释放（累计成交为 0 的终态 = 可重试，
有成交 = 永久已交易，UNKNOWN 或无回报 = 一直占着）。因此每个 SUBMIT 的 `decision_id`
都必须被回报，接口为 `POST {DECISION_CYCLE_STRATEGY_URL}/api/v1/strategy-order-reports`
（同一个 Bearer token，`trading.strategy_order_report.v1`，≤500 条/请求）。

- 开关：`DECISION_CYCLE_ORDER_REPORT_STRATEGY_IDS_JSON='["brier_mid_edge_once"]'`；留空 = 不回报。
  列出的策略必须有绑定，否则启动失败。`DECISION_CYCLE_ORDER_REPORT_LOOKBACK`（默认 24h）是每次回读订单的窗口。
- 有订单的决策：每个决策周期**结束后**（无论成功、失败、没有预测）扫一次这些策略账户在窗口内更新过的 BUY 订单，
  对尚未确认过当前 `revision` 的订单发回报。无状态：`report_seq` = 订单 `revision`（严格递增、不要求连续），
  `decision_id` = `OrderIntent.SignalID`，内容由订单行决定，所以重发是幂等的（对方按 `(decision_id, report_seq)` 去重）；
  进程内只记“已确认的 revision”，重启后整窗重发一遍即可。整批失败（5xx/超时）下个周期自动重发；
  单条被拒（`UNKNOWN_DECISION`/`REPORT_CONFLICT`/`INVALID_ORDER_REPORT`）只打一条 WARN，不再重试。
  该钩子有独立的 30 秒超时，错误只记日志，不影响周期结果和 readiness。
- 状态映射：未到交易所的 RECEIVED/VALIDATING/RESERVED/SUBMITTING 不报；ACKNOWLEDGED→SUBMITTED，LIVE/CANCEL_PENDING→OPEN，
  有成交→PARTIALLY_FILLED，FILLED/CANCELLED 原样；REJECTED 且无 `venue_order_id`（余额不足、价格漂移、kill switch 等）→
  NOT_SUBMITTED，交易所拒绝→REJECTED；UNKNOWN/RECONCILING/MANUAL_REVIEW→UNKNOWN（保持占位）。
  有成交但缺均价时用 `filled_notional / filled_size` 推算，推不出来就不报。
- 没有订单的决策：`runBinding` 里同步报 NOT_SUBMITTED（`report_seq=1`，重试 3 次、总超时 20 秒，失败只打 ERROR）：
  策略响应整体被 Go 校验拒绝（`STRATEGY_RESPONSE_REJECTED`，响应里每个 SUBMIT 都报）、下单开关关闭
  （`ORDER_SUBMISSION_DISABLED`）、入口被封（`ENTRY_BLOCKED`）、路由不接单（`VENUE_SUBMISSION_DISABLED`）。
- 不覆盖：`Decide` 本身超时/出错（策略可能已占位但我们没拿到响应）和 `ClaimOutput` 出错，仍需人工重置占位。

## 审计和失败语义

- 概率快照失败：整个周期失败，不使用上一次内存数据；
- 单个模型/策略绑定失败：保留该绑定错误，继续执行其他独立账户；
- 某个盘口失败：在输入中明确标记，由策略拒绝该 token；
- 共享盘口批次保存失败：整个周期在任何策略调用前失败，不产生新 OrderIntent；
- 同一 `decision_at` 的等价共享盘口重试只复用首次保存结果；身份、数量或内容不同则幂等冲突；
- 周期输入通过 `ClaimInput` 原子持久化；重试只能复用原快照；
- 策略输出通过 `ClaimOutput` 原子持久化；未成功持久化时不执行订单；
- 进程启动时会在发布下一周期前接管并清空旧进程留下的全部 intent lease；任一恢复失败都会阻止调度器就绪，
  不会把旧信号与新周期静默叠加；
- 提交开关打开时，策略输出和全部 OrderIntent 在同一事务中写入
  `strategy_order_intent_deliveries`；worker 使用 `PENDING -> SUBMITTING -> terminal` 状态、
  有界租约和 attempt fencing 投递，进程崩溃后仍以稳定 `client_order_id` 恢复；
- 执行层已经返回 `UNKNOWN/MANUAL_REVIEW` 的 intent 不会自动重投，必须由订单对账继续处理；
- 单个订单失败：继续处理其余独立 intent，并返回组合错误；
- 同一 execution account 的一批 durable intent 在提交期间持有账户执行门；定时、即时和手工对账必须
  等待整批提交结束后才能读取该账户。门在成功、拒绝、UNKNOWN、调用错误和 context 取消时都释放，
  不同 execution account 使用独立门，互不阻塞；
- 同一 execution account 一批 durable intent 的投递并发数由 `DECISION_CYCLE_DELIVERY_CONCURRENCY`
  （1–8，默认 1 = 严格串行）决定。大于 1 时按领取顺序（`created_at, cycle_id, sequence_no`，即策略响应数组顺序，
  edge 从大到小）依次启动，风控检查、官方盘口校验、签名和发单并行；**资金/份额预占仍严格按该顺序进行**：
  第 i 张等第 i-1 张的预占结束（成功或被拒）才预占，预占一结束立刻放行下一张。因此余额不足时拿到钱的
  仍是策略排在前面的订单。`IntentResult` 与错误聚合保持领取顺序，每张意图的
  `execute -> CompleteIntent` 恰好各一次，钱包执行门仍覆盖整批。每批投递结束打一条
  `decision intent batch delivered` 日志（批大小、并发数、耗时、成交/拒绝/UNKNOWN 数）。
  并发会按比例消耗共享的 CLOB 令牌桶（`POLYMARKET_REQUESTS_PER_SECOND`），一张买单约 5–7 次请求加一次成交读取，
  带撤单的再加 2 次，并发数应与令牌桶一起评估；
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
- 账户行的 `mode` 是唯一的账户级开关：`SELL_ONLY` 等价于账户只卖，`PAUSED` 不加载 lot、
  不调用策略，并在执行层拦截 BUY，结果中标记 `account_strategy_disabled=true`；不能把全部账户设为
  `PAUSED`。旧的 submission/entry/strategy disabled 三个名单与 Kalshi 输入开关已删除，
  任何旧变量出现都会拒绝启动；要彻底停一个平台，就把该平台的账户设为 `PAUSED`；
- Kalshi 账户启动预检失败（维护态）时，本轮不为它调用策略（`route_unavailable=true`），也不抓
  Kalshi 盘口；对账、结算和历史订单恢复照常；
- 同一 execution account 只能出现一次；账户之间可以共享模型或策略。

生产装配使用 migrations `0012_strategy_decision_cycles.sql`、
`0013_strategy_intent_deliveries.sql`、`0026_strategy_orderbook_snapshots.sql`，并通过 PostgreSQL
recorders 持久化共享盘口、完整 binding 输入、输出与订单意图投递状态。共享盘口的表结构、查询和
容量观察见 [`orderbook-snapshot-storage.md`](orderbook-snapshot-storage.md)。配置通过
`DECISION_CYCLE_ACCOUNTS_JSON` 注入，Polymarket 账户集合必须与受限钱包文件完全一致，
Kalshi 账户的凭据由 `KALSHI_LIVE_ACCOUNTS_JSON` 提供（每账户一把独立 API key）。只有 live 模式且两个显式开关都打开，OrderIntent 才会进入执行层；之后仍受
数据库 Kill Switch、账户/策略暂停、（账户, 策略）binding、余额和 reconciliation freshness 等硬风控阻断。

当前生产等价配置：

```text
DECISION_CYCLE_PREDICTION_MODEL_ID=v4.1 flash
DECISION_CYCLE_PREDICTION_SOURCE_MODE=SANDBOX
DECISION_CYCLE_ACCOUNTS_JSON=[
  {"execution_account_id":"main","market_source":"POLYMARKET","strategy_id":"brier_mid_edge_once","mode":"ACTIVE"},
  {"execution_account_id":"wallet-1","market_source":"POLYMARKET","strategy_id":"strategy_pending_b","mode":"PAUSED"},
  {"execution_account_id":"wallet-6","market_source":"POLYMARKET","strategy_id":"multfactor_v1","mode":"ACTIVE"},
  {"execution_account_id":"wallet-7","market_source":"POLYMARKET","strategy_id":"multfactor_v2","mode":"ACTIVE"},
  {"execution_account_id":"kalshi:main","market_source":"KALSHI","strategy_id":"brier_mid_edge_once","mode":"ACTIVE"}
]
KALSHI_LIVE_ACCOUNTS_JSON=[{"execution_account_id":"kalshi:main","api_key_id":"...","private_key_path":"/opt/trading-execution/secrets/kalshi-main-20261028.pem"}]
```

数据库授权按（账户, 策略）：每个 Polymarket 账户必须恰有一条启用的
`execution_strategy_bindings`（`strategy_id + execution_account_id`），行上的 `model_id`
只是审计字段，不必等于配置模型名，换模型不需要改库；全局启用的（策略, 账户）集合必须与
配置完全一致。Go 预占校验与 migration `0041` 的提交触发器使用同一规则。Kalshi 账户不走这张表，
其授权是 `KALSHI_LIVE_ACCOUNTS_JSON` 中的路由：Kalshi intent 的策略必须等于该账户配置的策略。
换策略时同步修改账户行与数据库绑定（见 [`deploy/rebind_main_wallet1.sql`](../deploy/rebind_main_wallet1.sql) 的做法）。

OPEN lot 保留开仓时的 `model_id` 和 `strategy_id` 作审计。每个策略周期查询该账户全部至少
0.01 shares 的 lot（Kalshi lot 不受此下限），与开仓模型/策略无关，因此换模型或换策略不会留下
无人管理的持仓。小于交易所两位小数 SELL 精度的真实链上 dust 留在账本等待结算/赎回，不会被舍入为零。
最新 BBO 和价格保护由独立 Market Validation 层负责，详见
[`market-validation.md`](market-validation.md)；余额、仓位、敞口与暂停检查由 Go Hard Risk
负责，详见 [`risk-control.md`](risk-control.md)。

调度器不会为任何入场决策 Token 请求历史价格，也不会在 v1/v2 冻结输入中保留历史占位记录。
混合 v1/v2 周期只共享冻结盘口；v2 的历史数据可用性由策略服务自己的官方 prices-history 读取负责，
不会再触发 Trading 侧 `STALE_DATA` 映射要求。
