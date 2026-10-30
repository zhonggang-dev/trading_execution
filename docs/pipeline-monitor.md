# 实盘交易链路监控

监控是独立于下单热路径的后台只读采样，不进入交易 readiness，不执行策略、下单、撤单、对账或订单重投。默认关闭。配置、状态文件、采样或飞书发送失败只影响监控诊断，不改变已有交易行为。

## 配置

```dotenv
PIPELINE_MONITOR_ENABLED=true
PIPELINE_MONITOR_INTERVAL=1m
PIPELINE_MONITOR_COLLECT_TIMEOUT=10s
PIPELINE_MONITOR_HTTP_TIMEOUT=3s
PIPELINE_MONITOR_ALERT_COOLDOWN=15m
PIPELINE_MONITOR_STATE_PATH=/var/lib/trading-execution/pipeline-monitor-state.json
PIPELINE_MONITOR_FEISHU_WEBHOOK_URL=<独立监控群的机器人地址>
PIPELINE_MONITOR_FEISHU_SECRET=<可选签名密钥>
```

发送通道与 Redis 容量告警独立。也可不用 Webhook，改为完整配置
`PIPELINE_MONITOR_FEISHU_APP_ID`、`PIPELINE_MONITOR_FEISHU_APP_SECRET`、
`PIPELINE_MONITOR_FEISHU_CHAT_ID` 三项，使用飞书应用机器人；两种传输不可混配。
凭证只放在服务器受限配置，不写仓库。机器人必须在目标群内。

无发送通道时仍采样，并报告 `monitor.delivery=unknown`。告警状态文件原子写入、权限为 0600；不可读或损坏时显示 `monitor.state=unknown` 并暂停发送，避免丢失去重记录后刷屏；修复文件后采样会重新读取。

## 只读诊断接口

```http
GET /api/v1/pipeline-monitor
Authorization: Bearer <LIVE_OPERATIONS_READ_ONLY_TOKEN>
```

返回 `code` 和 `data`；`data.schema_version=trading.pipeline_monitor.v1`，包含
`component=trading-execution`、UTC `sampled_at`、`stale`、`checks[]`。
单项字段为稳定 `id`、稳定维度 `scope`、`status`、`summary`、可选 `details` 和
`evidence`（string map）。状态为 `healthy/warning/critical/unknown/suppressed`。

接口只读取缓存，不触发采样或通知。未配置监控、首次采样前、装配失败或快照超过两个采样周期时返回 503；HTTP 200 只表示观测新鲜可读，不能把它解释为所有业务 check 都健康。原执行 Token 不具备该接口的只读权限。

## 观测口径

- `decision.schedule`：直接读取当前 `DecisionRunner.Snapshot`，检查循环停止、超过实际最大迟到时间、运行超过单轮预算和最近周期错误。不使用旧 `live_runtime_status` 代替实际运行事实。当前进程尚未完成首轮，但循环正常、正在等待已发布的首个启动窗口（含配置允许迟到宽限）或首轮正在预算内运行时标记 `suppressed`，不发送启动噪声；停止、迟到、超时和已知周期错误仍优先判断。没有首轮完成记录，也没有运行中或下一次计划证据时才报告 `unknown`。
- `binding.processing` / `binding.entry`：配置停用策略、提交或新开仓的路由如实标记 `suppressed`，不把 wallet-1 等计划停用报成故障。
- `prediction.observation` / `prediction.coverage`：每轮最多读取一次已有中台概率快照，使用生产同一来源/时效/版本筛选函数；按活动路由的精确上游模型、SANDBOX/DIRECT 和实际 Kalshi 输入路由判断。有效概率为空只说明 BUY 输入不足，不说明 SELL/Cancel 失效。最后一批有效输入距离全部到期不足 `min(30m, lookback/4)` 时预警。
- `prediction.venue`：展示来源级数量及最新 Direct manifest 的 pending 数；初期为处理中，等待超过 `min(30m, lookback/4)` 提醒，且该来源没有有效结果时升级为 critical，不让另一来源的有效结果掩盖已知缺失。该预算仅用于观测，不改变预测资格。Sandbox 的来源级期望应从中台 dispatch 观察；没有期望证据时不从结果并集反推“零任务成功”，也不单独制造来源级缺盘告警。
- `account.heartbeat` / `account.reconciliation`：使用账户局部的运行时检查，不把不相关钱包的全局 readiness 错误当成所有账户故障。该运行时接口用于 Polymarket；Kalshi 不伪装成具有 CLOB dead-man heartbeat。
- `account.authorization` / `account.cash` / `account.risk_state`：有界只读查询活动账户及已启用的 Kalshi 内部账户，核对数据库绑定、风险政策、账户/策略/全局开关、可用与预占现金、最近完成对账及政策 `max_state_age`。账户风险状态超过政策有效期前 20% 开始预警。零可用余额只作为 BUY 资金提醒，不自动释放预占。
- `account.reconciliation_issues` / `market.reconciliation_issues`：分别展示账户级与市场/token 级 OPEN issue。市场级问题只影响相同市场，不一概宣布钱包全部交易被阻断。
- `strategy.progress`：读取该路由最近的持久化策略周期；输入已写但输出超出周期预算时告警；盘口 `ERROR/MISSING` 与合法 `EMPTY` 分开。合法 SKIP 或无订单本身不触发告警。
- `intent.delivery` / `order.recovery` / `fill.application`：每账户每类最多读取 101 条未决记录，监控超出单轮周期预算的 PENDING/SUBMITTING/UNKNOWN 意图、长期无更新的 UNKNOWN/MANUAL_REVIEW/CANCEL_PENDING 订单及已确认但未入账的 Fill。PENDING/SUBMITTING 查询使用对应部分索引，不混入全部历史状态；UNKNOWN delivery 限当前账户最新 100 个周期，MANUAL_REVIEW order 限当前账户最近更新的 1,000 单，超出该窗口的历史问题通过 OPEN reconciliation issue 观察。范围明确记录在 evidence 中，不称作全量历史扫描。Kalshi durable 意图仍属于逻辑账户，在执行阶段才投影为内部账户，因此不伪造独立的空 Kalshi 意图队列。超过预算一倍为 warning、两倍为 critical；超过采样上限明确记录计数不完整。监控从不重发 UNKNOWN，不改订单或账本。

`strategy_order_intent_deliveries.status=UNKNOWN` 保存的是首次执行结果，对账后来确认订单成交或取消时不会改写这条历史交付记录。因此不能仅凭该状态一直报投递卡住：监控会按全局唯一 `client_order_id` 查询当前权威订单，只在该订单为 `FILLED/CANCELLED/REJECTED`，且执行账户与原意图匹配时排除历史 UNKNOWN。普通意图必须匹配原逻辑账户；明确 `market_source=KALSHI` 的意图必须匹配生产路由实际投影出的 `kalshi:<逻辑账户>`。错账户、错 client ID、没有订单、非终态订单仍保留告警；`MANUAL_REVIEW` 需要处理，不视为已解决终态。该判断只读取事实，不改写 delivery、订单、成交或预占。
- `venue.execution_errors`：每账户最多读取 101 条最近 15 分钟具名基础错误。认证、签名、allowance/余额与交易所暂停直接提醒，限流/服务错误累计三条提醒；正常 FOK/IOC 未成交、市场关闭等不列入系统错误。此项是实际执行失败的补充证据，不宣称在无请求时已证明交易所认证健康。

SQL 使用只读事务、每句 2 秒 statement timeout，并受本轮采集总预算限制。读取失败返回 `unknown`，不伪装成空数据。通知中不包含上游原始错误、URL、Token、钱包私钥或 API 响应正文。

## 通知与边界

首次问题提醒，升级立即提醒，同级成功发送后冷却；仅曾成功发送过且当前确认为 healthy 的事件发送恢复。每轮最多选择 12 个待通知 check，独立保存每项去重状态。通知错误不推进成功冷却；发送有总预算与 HTTP 超时，不拖住交易。

飞书通知使用 Card 2.0：严重问题为红色、预警为橙色、恢复为绿色。首屏优先展示加粗的问题和运行影响，再给范围及少量中文证据；英文技术字段、完整解释和定位 ID 放在诊断折叠区。首屏最多四类问题，更多已选择的问题仍保留在折叠区，不能静默丢弃。未知状态说明观测不足，不宣称交易已经中断。

所有告警 family 的首屏都尽量使用现有证据中的中文数量、金额和时间：现金、对账有效期、采样问题数、决策起止及累计跳过、盘口缺失份数、采样投递/恢复/入账超时数、交易所近期异常次数与最新原因。无证据不补造指标，采样数不写成全量。覆盖告警写“可用预测：0 个市场”或“无可用预测结果”，不能写成易与模型输出 0% 混淆的“可用概率为 0”；合法的单个 outcome 概率为 0 仍是有效预测。恢复也保留具体口径：低于告警阈值不等于错误归零，本次采样未再发现超时不等于历史所有 UNKNOWN 已解决。

同源模型的钱包概率告警仅在来源模式、级别、问题及完整原因/非账户证据一致时合并展示；独立 Check 和去重键保持不变。旧版 Check 没有可选 `source_model` 时，只能从结构明确的账户/模型/策略 Scope 中解析，并把解析出的模型加入分组身份；Scope 无法证明模型或与显式证据冲突时不合并。不同模型、venue、原因或状态不能误合并，逻辑账户与 Kalshi 内部账户的授权问题也不能混为一个钱包。故障与恢复分卡发送；部分卡片失败只重试未成功项，不能将另一张卡上的 Check 误记为已通知。

影响文案遵循执行边界：活动路由的 Critical 零有效概率是已确认的开仓门禁，可明确写出“这些账户无法新开仓，平仓/撤单按原规则”；即将过期的 Warning 使用预警语义，Unknown 则说明观测不足。概率不足、即将过期或现金不足影响 BUY 新开仓，不据此宣称 SELL/撤单停止；账户授权、对账和 Polymarket 心跳门禁可能阻断对应账户的新订单，但不关闭 Cancel/Get；市场级 issue 仅影响对应 token/condition/market。UNKNOWN 订单提示核实和恢复，不解释成“确定失败/未成交”，也不建议自动重投；监控恢复不等于订单已成交或历史订单被自动重试。所有动态文本均转义，不能由字段注入提及、链接或颜色标记。

此模块不会额外调用模型、CLOB、链上 RPC 或探测性真实下单。Allowance、地理资格等变化在没有新权威证据时不能由此保证；现有启动预检、真实执行/对账的结果仍是证据。需要更深预警时应增加有明确预算的只读观测，而不是以旧启动成功冒充当前认证健康。

主进程停止由宿主定时探针兜底，整台服务器失联仍超出同机方案的通知能力。QA/dataset 旁路不混入本交易链路健康度。

## 验证

运行 `make check`；`TestPipelineMonitorReadOnlyBusinessEvidencePostgresIntegration` 在隔离本地 PostgreSQL schema 上验证真实迁移、正常零订单、策略停滞、账户授权和局部 issue 语义。飞书测试使用本地 HTTP fixture；不向真实群试发、不调用模型、不下单。
