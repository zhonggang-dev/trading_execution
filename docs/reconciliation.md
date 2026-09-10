# 对账与安全修复

实现位置：

- `internal/service/reconciliation`：账户级对账、自动修复白名单、启动/周期/异常触发 Runner；
- `internal/adapter/polymarket/trading_client.go`：CLOB Open Orders、Trades 和单订单 Fill；
- `internal/adapter/polymarketdata/positions.go`：Data API 钱包持仓快照；
- `internal/adapter/evmrpc/erc20_balance.go`：ERC20 `balanceOf` 链上余额；
- `internal/adapter/postgres/reconciliation_recorder.go`：run/issue 审计和跨进程单账户互斥；
- `migrations/0006_reconciliation.sql`：对账表与仓位结算生命周期。

## 运行顺序

一次对账以 `execution_account_id` 为隔离边界，钱包只来自 Go 的 execution account 配置，不能由
Python 指定：

```text
读取 PostgreSQL account + 本系统 orders
  -> CLOB Open Orders / Trades（查询失败 != 空集合）
  -> 对每张本地 venue order 拉真实 /data/trades
  -> CONFIRMED Fill 通过 FillLedger 幂等补账
     （receipt 确认数不足 POLYGON_ORDER_FILLED_CONFIRMATIONS 时记为 MINED finality-pending 观察）
  -> 通过原订单状态机 Refresh（不直接 UPDATE orders）
  -> 读取 finality-pending Fill 视图（链上已生效、本地尚未入账的精确增量）
  -> 重新读取本地 positions
  -> 对比 Data API/未来链上 position source（本地 + finality-pending 增量）
  -> 对比 PostgreSQL total_balance/链上 ERC20 balance（本地 + finality-pending 净现金）
  -> 记录 reconciliation_run + issues
```

必须先补真实 Fill、再比较仓位和余额。否则“本地漏了一笔正常 BUY Fill”会被误报成 Phantom
Position，并诱发错误的人工补账。

### 配对成交与已下架订单的回填

单订单 `/data/trades` 查询保留执行钱包 `maker_address` 与市场 `market` 范围，不能用该订单的
token 作为顶层 `asset_id` 过滤：YES/NO 配对成交的顶层 token 属于 taker，maker 分量可能属于
另一 outcome。回填必须按精确 order hash 匹配 `maker_orders`，分别检查钱包、condition、token、
side，再用该分量的数量和价格核验 finalized Polygon OrderFilled 回执；不能将顶层 taker 的
数量、价格或另一账户的 maker 分量入账。同一 trade ID 在不同订单上的分量分别幂等记账。

订单查询 HTTP 200/null 是缺失证据，返回可重试 `CLOB_ORDER_NOT_FOUND`，不是成功的零成交
观察，更不是已经撤单。对账仍先通过独立成交回填路径找回真实 Fill，保留未确认部分的预占。

旧余额异常只有在后续完整对账通过、当前余额匹配、并且已入账成交现金事件的净变化完整解释
原差额时才能自动关闭。仓位减少交给精确 SELL 证据分支；通用 fill-lag 分支不得绕过其来源、
订单身份和完整数量差额检查。不能通过删除 OPEN issue 或手改 UNKNOWN 恢复交易。

CLOB 已报告 `CONFIRMED`、Polygon receipt 也已稳定但确认数还没到阈值的成交，是预期中的传播
状态，不是数据源故障。它会带着完整 OrderFilled 证据以 `MINED` 状态写入 `execution_fills`
（`applied_at IS NULL`），订单进入 `UNKNOWN + VENUE_FILL_EVIDENCE_PENDING`，预占保持冻结；
链上此时已经减少仓位、增加余额，本地账本仍是成交前的值。对账把每一笔待终局成交的精确
signed shares 和 `net_cash_delta` 加到本地视图后再与链上比较，逐笔归因而不是解释一个总差额：
差额恰好等于待终局成交之和才不记异常，其它差额仍按下文人工核查处理。达到阈值后同一证据
再次读取为 `CONFIRMED`，账本原子入账并释放预占。摘要字段 `finality_pending_fills`、
`positions_explained_by_finality_pending_fills`、`balance_explained_by_finality_pending_fills`
记录这一轮吸收了多少。

## 触发方式

- Go 服务启动时：`STARTUP` 对每个钱包使用常规 `RECONCILIATION_TRADE_LOOKBACK`
  的有界交易窗口，同时无条件选中所有非终态订单、未知订单和仍有活动/不确定预占的终态订单；
  因此启动恢复不会漏掉风险订单，也不会为重放全部历史终态订单和 trades 而阻塞 HTTP；
- Runner 默认每 5 分钟：`SCHEDULED`；
- 下单结果进入 `UNKNOWN`：`ORDER_UNKNOWN`，带 `focus_order_id` 优先处理；
- 撤单结果不确定：`CANCEL_UNKNOWN`；
- 余额或持仓监控发现异常：`ASSET_DRIFT`；
- 运维接口：`POST /internal/jobs/reconciliation/run`。

Runner 的异常队列不阻塞下单线程。即使进程在入队前崩溃，数据库中的 `UNKNOWN`、
`CANCEL_PENDING`、预占和 STARTED attempt 仍会被下次启动扫描发现。PostgreSQL 对每个 account 只
允许一条 `RUNNING` run；启动或周期扫描收到 shutdown context 后，服务会在独立的短时数据库
上下文中把已创建的 run 终结为 `FAILED`，避免留下阻塞下一次启动的账户租约。进程被强制终止等
无法运行清理代码的情形，遗留 run 超过 30 分钟租期后仍会被标记 `FAILED`，防止多实例同时
修复同一钱包。`RUNNING` 或 `FAILED` run 不会让实盘风控判定状态过期：下单时的
`RISK_STATE_STALE` 只看最近一次完成的扫描（`COMPLETED` 或 `ATTENTION_REQUIRED`）的完成时间是否在
`max_state_age_ms` 内；有问题的扫描由 OPEN issue 的 `impact_scope` 决定拦截范围，见下文。

手工触发示例：

```http
POST /internal/jobs/reconciliation/run
Authorization: Bearer <JOB_TOKEN>
Content-Type: application/json

{
  "execution_account_id": "account-model-a-multfactor-v1",
  "trigger": "ASSET_DRIFT",
  "focus_order_id": "ord-123"
}
```

## 单笔异常订单的隔离

一张 `UNKNOWN/RECONCILING/CANCEL_PENDING/SUBMITTING` 订单的恢复失败，不应让同一账户的其它市场
停止交易，也不应把正常的账本比较误判成人工漂移。实现位置：

- `internal/service/orderrecovery`：订单级恢复守卫（租约、单笔超时、退避、升级）；
- `internal/adapter/postgres/order_recovery_lease.go` + `migrations/0027_order_recovery_isolation.sql`：
  `order_recovery_leases` 持久化租约与重试计划；
- `internal/service/reconciliation/recovery.go`：未解决订单视图，隔离资产比较；
- `reconciliation_issues.impact_scope`：每条 OPEN issue 持久化其交易影响范围。

### 订单级租约与版本校验

快速订单扫描（`ordercoordinator`，秒级）与定时对账（分钟级）都会恢复同一张不确定订单。除了已有的
revision compare-and-swap，两者现在先按 `order_id` 获取同一条 `order_recovery_leases` 租约：

- 另一持有者的租约未过期 → 本轮跳过该订单（`orders_recovery_deferred`），不重复调用交易所；
- 租约里记录的 `order_revision` 比调用方看到的更新 → 视为过期快照，跳过；
- `next_retry_at` 未到 → 跳过；只有 `ORDER_UNKNOWN` 即时触发的 focus 订单可以越过退避；
- 工作完成后释放：脱离恢复状态 → 删除租约；仍在等待（成交明细传播中、revision 冲突）→ 不消耗重试
  预算；数据源失败或单笔超时 → `attempts+1`，`next_retry_at = now + min(30s × 2^(attempts-1), 10m)`。

### 单笔超时、退避与人工队列

每张订单的恢复调用受 `ORDER_RECOVERY_TIMEOUT`（默认 30s）约束；超时按失败记录并以 ERROR 日志告警，
其余订单和其它账户继续扫描。自 `first_pending_at` 起超过 `ORDER_RECOVERY_ESCALATE_AFTER`（默认 30m）
仍未解决的订单记 `ORDER_RECOVERY_STALLED`（`MANUAL_REVIEW`），进入人工队列；自动重试仍以上限退避继续，
但任何路径都不会自动释放该订单可能已成交的预占。

| 环境变量 | 默认 | 含义 |
| --- | --- | --- |
| `ORDER_RECOVERY_TIMEOUT` | 30s | 单笔恢复调用超时 |
| `ORDER_RECOVERY_LEASE_TTL` | 2m | 租约有效期（必须大于超时） |
| `ORDER_RECOVERY_RETRY_BACKOFF` | 30s | 失败后首个退避 |
| `ORDER_RECOVERY_MAX_BACKOFF` | 10m | 退避上限 |
| `ORDER_RECOVERY_ESCALATE_AFTER` | 30m | 进入人工队列的挂起时长 |
| `ORDER_RECOVERY_PENDING_GRACE` | 2m | 成交明细传播期内不记 `ORDER_RECOVERY_PENDING` 的宽限 |

### 未解决订单不污染资产比较

本轮恢复没有完成的订单（被跳过、失败、超时、仍在等待且账本里没有对应 MINED 成交）构成“未解决订单”
视图。它们的预占仍然冻结，因此链上任何由它们造成的变动都有界：

- 持仓：这些订单的 token 本轮不参与持仓比较（`positions_deferred_for_unresolved_orders`），不会记
  `POSITION_DRIFT`；其它 token 照常比较。
- 余额：`外部余额 − (本地余额 + 待终局成交净现金)` 若落在
  `[−Σ BUY 剩余预占金额, +Σ SELL 剩余预占份额 × 1]` 之内，记 `BALANCE_DRIFT + RETRY_LATER`（并把差额归因
  到这些订单），而不是账户级 `MANUAL_REVIEW`；超出区间仍按原逻辑记人工漂移。
- 已经有 `MINED` 成交、只在等待确认深度的订单，由既有的 finality-pending 视图解释，不算未解决。
- 未解决订单记 `ORDER_RECOVERY_PENDING`（`RETRY_LATER`），使后续干净对账不会在订单尚未收敛时自动
  关闭它的 issue；成交明细传播的前 `ORDER_RECOVERY_PENDING_GRACE` 内保持静默但仍排除其 token。

### “对账完成”与“允许交易”分离

run 状态不变：无 OPEN issue 为 `COMPLETED`，有 OPEN issue 为 `ATTENTION_REQUIRED`，无法建立本地权威为
`FAILED`。新增的 `impact_scope` 说明每条 OPEN issue 阻止什么：

| impact_scope | 何时 | 效果 |
| --- | --- | --- |
| `ACCOUNT` | `BALANCE_DRIFT`(MANUAL)、`SOURCE_CONFLICT`、账户级 `SOURCE_UNAVAILABLE`、缺少市场身份的 issue、未知类型 | 该账户所有新下单被拒 |
| `ORDER` / `TOKEN` | 订单级 `SOURCE_UNAVAILABLE`、`SUBMIT_UNCONFIRMED`、`FILL_FINALITY_STALLED`、`ORDER_RECOVERY_*`、`POSITION_DRIFT`、`PHANTOM_POSITION`、baseline drift、`EXTERNAL_TRADE` | 只拒绝同 token / 同 condition / 同 market 的新下单 |
| `NONE` | `OBSERVED_ONLY`、由未解决订单解释的 `BALANCE_DRIFT`(RETRY_LATER) | 不阻止交易 |

三处门控使用同一份范围：Go 侧 `Runner.CheckPlacement(order)`（`placementReadinessVenue`）、
`ReservationManager` 的 live risk 授权、以及 `enforce_live_order_submit_risk` 提交触发器。
`RISK_STATE_STALE` 的新鲜度现在取最近一次 **完成的扫描**（`COMPLETED` 或 `ATTENTION_REQUIRED`），
`RUNNING/FAILED` 仍不计。`Result.Impact` 和 run summary 中的 `impact_account_wide / impact_scoped_orders /
impact_scoped_tokens` 给出影响范围；`/health/ready` 只在账户级问题或扫描过期时报不就绪。

## 自动修复白名单

| 事实 | 证据要求 | 自动动作 |
| --- | --- | --- |
| 漏 BUY Fill | `/data/trades` 中属于本地 `venue_order_id` 的 `CONFIRMED` trade component | FillLedger 原子扣现金、增仓位/lot、更新订单和预占、写 outbox |
| 漏 SELL Fill | 同上 | FillLedger 原子加现金、减少目标 lot、计算 PnL、更新订单和预占 |
| 已补 SELL Fill 后遗留旧 `POSITION_DRIFT` | 后续对账全部数据源成功、精确 account/market/condition/token 当前本地值等于旧 remote value，且 issue 后已确认 BUY/SELL fills 的净 shares 精确解释全部差额 | 仅把旧 issue 幂等改为 `RESOLVED + AUTOMATIC`；不创建第二笔 Fill，也不直接改仓位 |
| 本地仍 LIVE、远端已取消 | CLOB 单订单查询明确返回 cancelled，且真实 Fill 已先同步 | 走订单状态机到 `CANCELLED`；保留预占，经过 Fill grace 并再查 Trades 后释放 |
| Market 已结算 | 外部持仓源明确 `redeemable=true`，且多个已配置来源一致 | 仓位/lot 改为 `SETTLED_PENDING_REDEEM`；不清 shares、不提前记 payout |
| 待赎回 condition | settlement price 已冻结为精确 `0/1`、所有 managed lot 的 `neg_risk` 一致、该 condition 无剩余 external baseline、adapter 授权已确认 | 先持久化 `*_SUBMITTING`，再提交精确 `redeemPositions`；只有 canonical `PositionsRedeemed` 回执达到确认深度后，才关闭 lot/position 并增加现金、实现 PnL |
| 赎回已落链、账本未入账 | `polymarket_redemptions` 处于 REDEEM_SUBMITTING/REDEEM_SUBMITTED/CONFIRMED | 已结算仓位从外部快照消失、链上余额恰好多出 payout 不记漂移；OPEN 仓位消失或余额差额不等于 payout 仍记 `MANUAL_REVIEW` |
| 赎回已入账、Data API 尚未索引 | 本地仓位 `CLOSED` 且 shares 为 0，同 condition 的赎回在 30 分钟内 `APPLIED`，远端仍 `redeemable=true` 且数量精确等于该 token 已赎回份额 | 记 `POSITION_DRIFT + RETRY_LATER`，下一次干净对账自动 `RESOLVED`；超出宽限期或数量不符仍是 `MANUAL_REVIEW` |

所有自动修改都复用现有的 PostgreSQL 事务边界和状态机。对账服务本身不能绕过 FillLedger 直接
改余额、仓位数量或成交记录。

Auto redeem 不使用“持有 48 小时”条件，也不使用 CLOB SELL。一次 condition 赎回会消耗钱包在该
condition 下的完整 ERC-1155 余额，因此发现任何未消耗 ownership baseline、开放/预占 lot 或不一致的
adapter identity 时都会进入 `MANUAL_REVIEW`。网络提交前先落 durable intent；若提交响应丢失，只能用
Data API 找候选交易并用 Polygon 回执验证，禁止盲目重发。

赎回状态机的每个等待都有上界，避免一条记录永远占用对账豁免：

- `CONFIRMED` 入账失败时区分原因。回执 payout 与 `shares × settlement_price` 不精确相等、settled lot 合计不等于
  仓位、baseline 残留等证据类失败重试也不会改变，立即转 `MANUAL_REVIEW`；数据库等瞬时失败按 retry interval
  重试，confirmed_at 超过 `POLYMARKET_AUTO_REDEEM_AMBIGUITY_TIMEOUT` 后同样转 `MANUAL_REVIEW`。
- `APPROVAL_SUBMITTED` / `REDEEM_SUBMITTED` 在 relayer 持续返回 pending、没有 tx hash、或回执迟迟不最终化时，
  自 submitted_at 起超过 ambiguity timeout 即转 `MANUAL_REVIEW`。
- 进入 `MANUAL_REVIEW` 后该 condition 不再被视为 in-flight，对账会如实报出仓位缺失与余额多出，由人工处理。

## 必须人工核查

以下 issue 保持 `OPEN + MANUAL_REVIEW`，不会猜测修复：

- `SUBMIT_UNCONFIRMED`：无法证明 POST 是否到达 CLOB，且没有 venue order id；禁止自动重发；
- `POSITION_DRIFT`：补完可证明 Fill 后，数量仍不一致；
- `PHANTOM_POSITION`：钱包有 shares，但本地没有可归因的 order/fill/position；
- `EXTERNAL_TRADE`：成交无法映射到本系统订单，无法区分人工或其他程序交易；
- `BALANCE_DRIFT`：链上余额和本地 total balance 不同，但没有可归因的 Fill/redeem/cash event；
- `SOURCE_CONFLICT`：Data API、链上或多个 RPC 相互矛盾；本轮关闭所有相关自动修复；
- `FILL_FINALITY_STALLED`：CLOB 已 `CONFIRMED` 的成交在 `match_time` 之后超过
  `POLYGON_FILL_FINALITY_MAX_AGE`（默认 30m）仍未达到确认阈值。确认数只会因为交易被丢弃、
  被 reorg 重新打包或 RPC head 卡住而停止增长，必须人工核对 receipt 是否 canonical；
- `EXTERNAL_ORDER` 使用 `OBSERVED_ONLY`：只展示，不撤销、不修改、不导入成本系统。

API/RPC 失败记录为 `SOURCE_UNAVAILABLE + RETRY_LATER`。查询接口可以有界重试，但失败结果绝不
转换成 `[]` 或余额 `0`。

Data API `/positions` 使用 `sizeThreshold=0&includeArchived=true` 分页读取，避免默认阈值或 archived
过滤把真实小仓位隐藏。官方上限是 150 次/10 秒；adapter 在所有钱包间共享一个默认 10 QPS 的
节流器，对账周期不为每个 model/strategy 建立独立 client。

历史 Kalshi `MANUAL_REVIEW + RECONCILIATION_REQUIRED` 只能使用 `cmd/kalshirepair` 做精确订单
修复。工具默认 dry-run，强制指定 `kalshi:<logical-account>` 和本地 order ID；`--apply` 还必须
提供完全相同的 `account/order` 确认串。只有 Kalshi 按 `client_order_id` 找到权威 order ID、
再由单订单详情和该 order ID 的 fills 接口共同证明订单已取消、零成交且已过 finality grace 时，
才在一个 PostgreSQL
事务中更新订单审计、释放预留并关闭该订单的问题。远端证据还必须和本地 intent 的 canonical
`outcome_side/book_side`、LIMIT 类型、YES-book 价格及 subaccount 完全一致。已弃用的
`action/side` 不作为身份依据；当前 GET Orders 不一定
返回 `time_in_force`，因此远端仅在该字段存在时核对 FOK。该手工工具只处理上线 IOC 之前已冻结的
LIMIT/FOK 订单；新的 Kalshi IOC 订单由常规 order coordinator、官方 order/fills 查询和 finality grace 自动收敛，
不进入这个 legacy no-fill repair。
`self_trade_prevention_type` 与 `cancel_order_on_pause` 可能不回显，只保留为观测字段，不参与
terminal cancelled + zero-fill 的身份判断或幂等指纹。
工具禁止广扫，也不会从远端余额推断订单结果。

Kalshi 启动时读到的 `balance` 是交易所当前可用现金，不是可以无条件覆盖本地累计账的总账基线。
只要该账户存在 `ACTIVE/RECONCILIATION_REQUIRED` reservation 或非零本地预留，启动就保留本地
`total/available/reserved` 不变，先由 order/fills 恢复链路应用真实成交，避免“外部余额已经反映成交，本地 fill 又记一次”。
只有本地没有未决交易资产时才更新外部现金基线。

若已存在的 Kalshi 账户在启动时遇到短暂远端 preflight 失败，进程保留 maintenance-only route：
禁止新的策略 intent，但该账户仍在 coordinator 扫描范围内，历史订单可继续 `Refresh/FinalizeCancellation`。
尚未发到交易所的历史订单不会在 maintenance-only 状态下自动提交；每次延后都会写入 `MAINTENANCE`
审计事件并刷新重试时间。取消终局所需的 order/fills 证据暂不可用时也会持久化退避，避免坏订单长期占满恢复批次。
如果本地凭据/配置错误导致连 maintenance route 都无法构建，且账户仍有未决 reservation，服务启动失败关闭，
不会带着失管订单假健康上线。

## 异常处理覆盖

| 异常 | 当前处理 |
| --- | --- |
| token 缺失、size <= 0 | `OrderIntent.Validate` 直接拒绝，不调用 CLOB、不重试 |
| 策略价格过期 | Market validator 下单前读最新 top-of-book，BUY 超过 `worst_price` 返回 `PRICE_DRIFT`；SELL 不做该检查 |
| 无订单簿/无买盘 | BUY 拒绝；SELL 在官方成功返回单边/空盘口时可提交；读取失败仍拒绝，不伪造成交 |
| tick、金额、shares 精度错误 | Polymarket adapter 使用十进制定点换算，下单前校验 tick/精度和 BUY 最小量 |
| 不足最小 SELL shares | 满足精度的策略 SELL 可提交，由交易所决定是否接受；无真实成交则保留原有 shares |
| 余额不足或余额滞后 | PostgreSQL 先原子预占；CLOB 拒绝后重新读链上余额并触发对账，不能用 Redis 锁补救 |
| CLOB 查询不可用 | 只对读请求有界重试；保留原订单和预占，不能解释为“没有订单” |
| `/trades` 延迟 | 保持 `UNKNOWN/RECONCILING` 并使用重叠窗口重查；不从 `size_matched` 伪造 Fill |
| 单笔订单恢复持续失败 | 订单级租约 + 退避重试，超时告警；其它订单继续扫描；超过升级窗口记 `ORDER_RECOVERY_STALLED` 进入人工队列，预占不自动释放 |
| placement status=`delayed` | 只映射为 `ACKNOWLEDGED`，不入仓位 |
| Cancel Race | `CANCEL_PENDING` 后先同步 Fill，再查订单；成交优先按真实 Fill 入账 |
| 部分成交 | 累计 shares/notional/fees，保存未成交预占和剩余 lot，不清零 |
| 人工挂单 | `EXTERNAL_ORDER` 只读展示；只有数据库中本系统拥有的 order 才能撤销 |

## 结算与 Redeem 边界

对账模块能确认“Market 已结算”，但不能把它当作“赎回已成功”。它只写：

```text
execution_positions.lifecycle_status = SETTLED_PENDING_REDEEM
position_lots.status                  = SETTLED_PENDING_REDEEM
shares / cost_basis                   = 原值保留
```

链上提交由 `internal/service/autoredeem` 负责，状态持久化在 `polymarket_redemptions`：广播结果未知时只能按
Data API 活动与 Polygon receipt 恢复，禁止盲目重发；确认成功后再用一个 PostgreSQL 事务关闭 shares、记录
实际 payout 和 realized PnL。没有 receipt 时绝不能靠 Data API 仓位消失来猜赎回成功。

## 待确认 Polygon 凭证与数据库迁移

### 订单数量的实际响应兼容

官方订单文档示例采用6位base units，但实测 `/data/order/{id}` 也会返回
`original_size="48"`、`size_matched="19.01"` 的人类份额表示。仅凭有无小数点区分单位会
把原始数量错读成0.000048，导致部分成交查询失败并跳过IOC撤余量。

适配器保留原始值；只有原始数量恰好等于持久化签名订单份额，且订单、市场、token、方向
全部一致时，才选用人类份额解释。其他情况保持原有base-unit解析，不以数值大小猜单位。
这同时用于状态与成交均价核验，不改变权威成交入账来源。形状不合法的订单响应为可重试
外部证据错误，不应因普通重试次数耗尽而自动推入不可继续恢复的人工终态。

参考：https://docs.polymarket.com/api-reference/trade/get-single-order-by-id

### 浅确认凭证

迁移 `0028_pending_polygon_settlement_evidence.sql` 必须先于依赖它的服务版本上线。
经严格校验但尚未达到配置确认数的 receipt 会保存为 `MINED`，保留完整结算凭证；此阶段
`confirmed_at`、`applied_at` 均为空，不更新现金或仓位，也不释放预占。达到确认深度后，同一
fill identity 升级为 `CONFIRMED` 并只入账一次。不能把待确认数据强改为 CONFIRMED 来绕过约束。

迁移保留原有链、合约、交易、订单、token、方向、金额字段形状和链上日志唯一性约束；
新增具名的待确认不得入账约束。数据库就绪检查要求该约束存在，防止遗漏迁移却报告可用。
PostgreSQL 完整路径测试同时覆盖浅确认不入账、最终确认入账、重复回放、MINED 提前入账拒绝，
以及遗漏新约束时就绪检查失败。

## 生产装配

`cmd/server` 的 live composition 已把以下组件连接到同一个 shutdown context：

```text
Postgres OrderRepository + ReservationManager + FillLedger + ReconciliationRecorder
Polymarket TradingClient（同时实现 Venue/Fill/Reconciliation source）
Polymarket Data API PositionClient
EVM RPC ERC20BalanceClient（token/asset/decimals 必须使用当前实盘 collateral 配置，不能硬编码旧 USDC）
fillprocessor.Service + execution.Service + reconciliation.Service + reconciliation.Runner
```

`execution.Params.Reconciliation` 通过 trigger bridge 指向 Runner，HTTP 的 `Reconciliation` 指向
同步 service。启动 reconciliation 必须全部成功，heartbeat 和后台 loop 才会启动；任何账户结果
不新鲜、loop 停止或 heartbeat 失效都会只阻止新 Place，不阻止 Cancel/Get。

上线前还要为实际 Polygon RPC/Data API 做故障注入：超时、429、分页重复、API 相互矛盾、
`/trades` 延迟、Cancel Race、进程在网络调用后/数据库提交前崩溃。PostgreSQL migration 集成测试
需要通过 `TRADING_EXECUTION_TEST_DATABASE_URL` 在目标版本上运行。

官方对照：

- <https://docs.polymarket.com/api-reference/core/get-current-positions-for-a-user>
- <https://docs.polymarket.com/api-reference/trade/get-trades>
- <https://docs.polymarket.com/api-reference/rate-limits>
- <https://docs.polymarket.com/resources/contracts>
