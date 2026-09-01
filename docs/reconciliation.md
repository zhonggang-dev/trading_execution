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
  -> 通过原订单状态机 Refresh（不直接 UPDATE orders）
  -> 重新读取本地 positions
  -> 对比 Data API/未来链上 position source
  -> 对比 PostgreSQL total_balance/链上 ERC20 balance
  -> 记录 reconciliation_run + issues
```

必须先补真实 Fill、再比较仓位和余额。否则“本地漏了一笔正常 BUY Fill”会被误报成 Phantom
Position，并诱发错误的人工补账。

## 触发方式

- Go 服务启动时：`STARTUP` 全钱包扫描；
- Runner 默认每 5 分钟：`SCHEDULED`；
- 下单结果进入 `UNKNOWN`：`ORDER_UNKNOWN`，带 `focus_order_id` 优先处理；
- 撤单结果不确定：`CANCEL_UNKNOWN`；
- 余额或持仓监控发现异常：`ASSET_DRIFT`；
- 运维接口：`POST /internal/jobs/reconciliation/run`。

Runner 的异常队列不阻塞下单线程。即使进程在入队前崩溃，数据库中的 `UNKNOWN`、
`CANCEL_PENDING`、预占和 STARTED attempt 仍会被下次启动扫描发现。PostgreSQL 对每个 account 只
允许一条 `RUNNING` run；崩溃遗留的 run 超过 30 分钟租期后会被标记 `FAILED`，防止多实例同时
修复同一钱包。

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

## 自动修复白名单

| 事实 | 证据要求 | 自动动作 |
| --- | --- | --- |
| 漏 BUY Fill | `/data/trades` 中属于本地 `venue_order_id` 的 `CONFIRMED` trade component | FillLedger 原子扣现金、增仓位/lot、更新订单和预占、写 outbox |
| 漏 SELL Fill | 同上 | FillLedger 原子加现金、减少目标 lot、计算 PnL、更新订单和预占 |
| 已补 SELL Fill 后遗留旧 `POSITION_DRIFT` | 后续对账全部数据源成功、精确 account/market/condition/token 当前本地值等于旧 remote value，且 issue 后已确认 BUY/SELL fills 的净 shares 精确解释全部差额 | 仅把旧 issue 幂等改为 `RESOLVED + AUTOMATIC`；不创建第二笔 Fill，也不直接改仓位 |
| 本地仍 LIVE、远端已取消 | CLOB 单订单查询明确返回 cancelled，且真实 Fill 已先同步 | 走订单状态机到 `CANCELLED`；保留预占，经过 Fill grace 并再查 Trades 后释放 |
| Market 已结算 | 外部持仓源明确 `redeemable=true`，且多个已配置来源一致 | 仓位/lot 改为 `SETTLED_PENDING_REDEEM`；不清 shares、不提前记 payout |

所有自动修改都复用现有的 PostgreSQL 事务边界和状态机。对账服务本身不能绕过 FillLedger 直接
改余额、仓位数量或成交记录。

## 必须人工核查

以下 issue 保持 `OPEN + MANUAL_REVIEW`，不会猜测修复：

- `SUBMIT_UNCONFIRMED`：无法证明 POST 是否到达 CLOB，且没有 venue order id；禁止自动重发；
- `POSITION_DRIFT`：补完可证明 Fill 后，数量仍不一致；
- `PHANTOM_POSITION`：钱包有 shares，但本地没有可归因的 order/fill/position；
- `EXTERNAL_TRADE`：成交无法映射到本系统订单，无法区分人工或其他程序交易；
- `BALANCE_DRIFT`：链上余额和本地 total balance 不同，但没有可归因的 Fill/redeem/cash event；
- `SOURCE_CONFLICT`：Data API、链上或多个 RPC 相互矛盾；本轮关闭所有相关自动修复；
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
| 策略价格过期 | Market validator 下单前读最新 top-of-book，超过 `worst_price` 返回 `PRICE_DRIFT` |
| 无订单簿/无买盘 | 开仓拒绝；lot 退出任务保留仓位并等待后续周期，不伪造成交 |
| tick、金额、shares 精度错误 | Polymarket adapter 使用十进制定点换算，下单前校验 tick/最小量/舍入 |
| 不足最小 SELL shares | 标记 dust 并保留真实 shares；等待合并、结算或人工策略处理 |
| 余额不足或余额滞后 | PostgreSQL 先原子预占；CLOB 拒绝后重新读链上余额并触发对账，不能用 Redis 锁补救 |
| CLOB 查询不可用 | 只对读请求有界重试；保留原订单和预占，不能解释为“没有订单” |
| `/trades` 延迟 | 保持 `UNKNOWN/RECONCILING` 并使用重叠窗口重查；不从 `size_matched` 伪造 Fill |
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

Redeem 的链上提交器目前尚未实现，不能声称已经覆盖 RPC timeout/revert。后续模块必须单独持久化
`chain_id + wallet + nonce + tx_hash + calldata_hash + receipt_status`：广播结果未知时只能按 nonce/hash
查 receipt；只有 receipt 明确 reverted/dropped 后才能以相同业务幂等键重试。确认成功后再用一个
PostgreSQL 事务关闭 shares、记录实际 payout 和 realized PnL。没有 receipt 时绝不能靠 Data API
仓位消失来猜赎回成功。

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
