# Trading Execution

`Trading Execution` 是独立的 Go 交易执行服务骨架。Go module 使用合法标识
`github.com/UniPat-AI/trading_execution`；代码包按职责命名为 `domain`、`execution`、
`port` 和 adapter。

它只接受策略已经完成决策后的 `OrderIntent`，负责：

- 按 `client_order_id` 幂等受理，避免至少一次交付导致重复下单；
- 执行订单字段校验和执行侧硬限制；
- 按 `condition_id` 重新确认 Market、outcome/token、状态、`neg_risk` 与 tick size；
- 下单前读取最新盘口并执行 Python `worst_price` 保护；
- 调用交易所 adapter 下单、查单和撤单；
- 只按真实 confirmed Trades/Fills 原子更新订单、资金、仓位与独立开仓批次；
- 保存已实现/未实现盈亏，dust 只分类、不清零真实 shares；
- 通过 transactional outbox 可靠发布成交与仓位事件；
- 提供 HTTP API、Bearer Token 鉴权、健康检查与优雅退出。

概率、edge、买卖方向选择、动态仓位计算和入场/离场规则不属于本服务。

## 策略周期编排

新框架通过独立的 `decisioncycle` 应用服务编排 10 分钟周期，但不包含策略规则：

```text
prediction_infra PIT 概率快照 + PostgreSQL OPEN lots + Polymarket top-15 订单簿 + v2 所需的 48 小时 mid price history
  -> 按 (model_id, strategy_id, execution_account_id) 展开
  -> trading.strategy_input.v4
  -> 外部策略服务
  -> trading.strategy_output.v4 / lot-addressed OrderIntent
  -> execution.Service
```

它不使用旧的 Redis live consumer。概率通过 `prediction_infra` 的只读 HTTP API 拉取，
盘口和历史 midpoint 由 Trading Execution 自己采集；每个执行上下文的冻结输入必须在调用策略前持久化，策略输出
也必须在下单前持久化。完整协议见
[`docs/decision-cycle.md`](docs/decision-cycle.md)。
外部策略团队需要实现的入场与逐笔持仓退出 HTTP 接口、完整请求/响应类型和业务校验见
[`docs/python-algorithm-http-api.md`](docs/python-algorithm-http-api.md)。
独立十分钟持仓退出任务的运行和持久化说明见
[`docs/position-exit-job.md`](docs/position-exit-job.md)。
执行前的市场身份、状态、tick 与最新价格校验见
[`docs/market-validation.md`](docs/market-validation.md)。
Python/Go 风控边界、硬风控规则和实盘原子预占要求见
[`docs/risk-control.md`](docs/risk-control.md)。
资金/仓位表结构、PostgreSQL 行锁、部分成交与不确定下单处理见
[`docs/asset-reservations.md`](docs/asset-reservations.md)。
Polymarket CLOB V2 的签名、精度、凭证与错误边界见
[`docs/polymarket-execution.md`](docs/polymarket-execution.md)。
旧实盘 `wallets.json` 兼容格式、多钱包加载和无下单连通性检查见
[`docs/wallet-connection.md`](docs/wallet-connection.md)。
完整订单状态图、UNKNOWN 对账和 Cancel Race 见
[`docs/order-state-machine.md`](docs/order-state-machine.md)。
真实 Fill 去重、手续费、部分成交、仓位批次、盈亏与 dust 处理见
[`docs/fills-and-position-ledger.md`](docs/fills-and-position-ledger.md)。
启动/定时/UNKNOWN 对账、自动修复白名单、外部订单隔离和链上余额核对见
[`docs/reconciliation.md`](docs/reconciliation.md)。

## 边界

```text
Prediction / Strategy service
  -> OrderIntent（已决定 token、方向、价格、数量）
  -> Trading Execution HTTP API
       -> idempotency history lookup
       -> Go hard risk（余额/敞口/重复订单/暂停/Kill Switch/时效）
       -> Market validation（Universe 元数据 + 最新 CLOB 盘口）
       -> atomic client_order_id claim
       -> PostgreSQL cash/share reservation
       -> Venue port
            -> paper adapter（默认）
            -> Polymarket CLOB V2 adapter（仅显式启用 live 时装配）
       -> OrderRepository port
            -> memory adapter（仅 local 且未配置数据库）
            -> PostgreSQL order/event/attempt adapter（非 local 当前实现）
```

`internal/domain.OrderIntent` 是策略与执行之间的契约。它故意不包含
`probability`、`edge` 或策略阈值；HTTP 解码也会拒绝未知字段，防止边界重新耦合。
每个 OrderIntent 都携带可信 `model_id + strategy_id + execution_account_id`，未来 live
Venue adapter 使用 execution account 选择对应钱包和签名器，Python 不能选择钱包。

## 当前安全状态

默认仍是 `EXECUTION_MODE=paper`，paper adapter 不发起任何交易网络请求。非 local 环境强制
配置 PostgreSQL，并把订单、事件、外部操作尝试、幂等键和资金预占持久化；服务启动时会恢复
未完成订单，`/health/ready` 会实际检查数据库连接和必需 schema。paper 撤单可同步释放预占；
live 必须等待真实成交终局和对账，不能复用这个快速路径。

`cmd/server` 已有 fail-closed 的 Polymarket CLOB V2 live composition：多钱包 secret、EOA/legacy
proxy/safe 签名、L2 认证、账户级 closed-only、pUSD 余额和 allowance、Gamma Market 校验、
PostgreSQL 订单/预占/Fill/仓位账本、启动与持续 reconciliation、heartbeat、Polygon
`OrderFilled` 结算证据和 placement-only readiness gate。live 必须同时显式设置
`EXECUTION_MODE=live`、`EXECUTION_VENUE=polymarket` 和 `POLYMARKET_LIVE_TRADING_ENABLED=true`；
缺少任何依赖都会拒绝启动或拒绝新下单，Cancel/Get 仍保持可用。

这不代表拿任意旧钱包即可直接开实盘。正式解除数据库全局 Kill Switch 前仍必须：

1. 用进程外 secret 文件/HSM 安全配置钱包，并确认旧机器人已停机；同一账户不能由两个 heartbeat
   owner 并行控制。`POLY_1271`/Deposit Wallet 当前仍 fail closed；
2. 执行并审计 migrations `0001..0015`，把钱包、账面 pUSD、已有仓位 cost basis/lots、
   外部持仓 ownership baseline、旧 lot 的逻辑模型路由、risk policy、strategy binding 和
   reconciliation 基线对齐；
3. 确认 CLOB V2 私有认证成功、`closed_only=false`、Polygon pUSD 及两个 V2 Exchange allowance
   正确，并让每个确认 Fill 都取得足够确认数的链上 `OrderFilled` 证据；
4. 使用专用小额空钱包完成一次人工批准的 BUY/SELL/Cancel canary 后再逐步放量。

transactional outbox 会与账本同事务写入，但生产消息 publisher 仍需按部署环境注入；独立 Position Exit
runner 和链上 redeem 仍未在 `cmd/server` 装配（OPEN-lot 退出当前由 decision cycle 处理）。策略周期已经使用 prediction_infra HTTP snapshot、
Python `/api/v4/decisions`、PostgreSQL 输入/输出审计和精确 UTC Runner 完成 live 装配，但默认
`DECISION_CYCLE_ENABLED=false`，且独立的订单提交开关也默认关闭。订单提交还强制要求
`DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE=true` 和精确的 2 模型 × 2 策略 × 4 钱包拓扑；
缺少任一门禁都不会由周期创建 BUY 订单。提交开关打开时，经过校验的 OrderIntent 会先与策略输出在同一事务中写入
PostgreSQL 投递表，再由带租约和 fencing attempt 的 worker 调用幂等执行服务；进程崩溃后只会按
稳定 `client_order_id` 恢复，不会把一次算法响应直接当成一次不可靠的内存下单。

内存 Repository 只用于 local 且未配置数据库的开发模式；重启会丢失订单和幂等记录，不能用于
共享测试或实盘。非 local 环境缺少 `TRADING_EXECUTION_DATABASE_URL` 时会直接拒绝启动。

## 本地运行

需要 Go 1.26 或更新版本：

```bash
cp .env.example .env
set -a
source .env
set +a
make run
```

默认监听 `:8090`。`local` 环境可不设置 Token，其他环境要求
`EXECUTION_API_TOKEN` 至少 32 字节。

只读检查旧实盘钱包：

```bash
export POLYMARKET_ACCOUNTS_FILE=/run/secrets/trading_execution/wallets.json
make wallet-check
```

该命令只检查 CLOB V2 版本并读取每个 execution account 的 Open Orders，不提交或撤销订单。

## API

实盘只读监控使用独立接口 `GET /api/v1/live-operations`，由后台每 5～15 秒聚合钱包、Data API、CLOB 和 Ledger 后原子更新，不在前端请求路径串行访问交易所。完整字段口径、错误结构和部署配置见 [实盘监控聚合接口](docs/live-operations.md)。

```text
GET  /health/live
GET  /health/ready
POST /api/v1/orders
GET  /api/v1/orders/{order_id}
POST /api/v1/orders/{order_id}/refresh
POST /api/v1/orders/{order_id}/cancel
GET  /api/v1/orders/{order_id}/events
GET  /api/v1/orders/{order_id}/attempts
GET  /api/v1/trades                              # 已确认且已入账的真实 Fill
GET  /api/v1/daily-pnl                           # UTC 日 × 执行账户 × 策略的净已实现盈亏
POST /internal/jobs/position-exit-evaluation/run  # 仅在注入 PositionExitJob 后注册，当前 cmd/server 未装配
POST /internal/jobs/reconciliation/run             # live 模式注册；paper 模式不注册
```

查询交易记录：

```bash
curl 'http://127.0.0.1:8090/api/v1/trades?from=2026-08-01T00:00:00Z&side=SELL&model_id=forecast-v2&strategy_id=multfactor_v2&limit=20&offset=0' \
  -H "Authorization: Bearer ${EXECUTION_API_TOKEN}"

curl 'http://127.0.0.1:8090/api/v1/daily-pnl?days=14' \
  -H "Authorization: Bearer ${EXECUTION_API_TOKEN}"
```

该接口只读取 `execution_fills.status=CONFIRMED AND applied_at IS NOT NULL` 的记录，
并从 `position_lot_closures` 汇总 SELL 的已实现盈亏。返回价格和金额均为 JSON
字符串小数；不返回钱包地址、CLOB 凭证、签名或原始响应。配置
`TRADING_EXECUTION_DATABASE_URL` 后读取 PostgreSQL；未配置时 paper 模式返回空列表，
不会把 paper 订单状态伪装成真实成交。生产库需按顺序执行至
`migrations/0015_position_lot_model_routes.sql`。

每日盈亏接口最多查询 90 天，固定使用 UTC 自然日。收益按
`position_lot_closures -> position_lots` 的开仓批次来源归因到准确的
`execution_account_id + model_id + strategy_id`；当前启用的策略绑定即使某天没有平仓，
也会返回 `realized_pnl="0"`，供前端区分零收益与数据缺失。该接口不返回真实钱包地址。

创建限价单：

```bash
curl -X POST http://127.0.0.1:8090/api/v1/orders \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer ${EXECUTION_API_TOKEN}" \
  -d '{
    "model_id":"model-a",
    "strategy_id":"strategy-v1",
    "execution_account_id":"account-model-a-strategy-v1",
    "signal_id":"live-prediction:pred-001",
    "client_order_id":"account-model-a-strategy-v1:pred-001:token-yes:entry-1",
    "venue":"polymarket-paper",
    "market_id":"1122565",
    "condition_id":"0x05297f...",
    "token_id":"731744596559930708",
    "market_snapshot_at":"2026-08-18T04:20:02Z",
    "signal_at":"2026-08-18T04:20:05Z",
    "side":"BUY",
    "type":"LIMIT",
    "price":"0.51",
    "worst_price":"0.53",
    "size":"20",
    "time_in_force":"GTC",
    "metadata":{"prediction_id":"pred-001"}
  }'
```

价格、数量和成交字段必须使用 JSON 字符串小数，不能传 JSON number。相同
`client_order_id` 和相同语义的请求返回原订单；若复用该 ID 但更改任一订单字段，
返回 `409 IDEMPOTENCY_CONFLICT`。

## 校验

```bash
make check
```
