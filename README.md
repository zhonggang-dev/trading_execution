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
prediction_infra PIT 概率快照 + PostgreSQL OPEN lots + Polymarket top-15 订单簿 + 48 小时 mid price history
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
策略团队需要实现的 HTTP v3 契约见
[`docs/strategy-http-api.md`](docs/strategy-http-api.md)。
独立的十分钟持仓退出任务、逐笔 trade 的 Python 请求/响应和 SELL 规则见
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
            -> paper adapter（当前）
            -> Polymarket CLOB V2 adapter（已实现，live 尚未装配）
       -> OrderRepository port
            -> memory adapter（当前）
            -> PostgreSQL order/event/attempt adapter（已实现）
```

`internal/domain.OrderIntent` 是策略与执行之间的契约。它故意不包含
`probability`、`edge` 或策略阈值；HTTP 解码也会拒绝未知字段，防止边界重新耦合。
每个 OrderIntent 都携带可信 `model_id + strategy_id + execution_account_id`，未来 live
Venue adapter 使用 execution account 选择对应钱包和签名器，Python 不能选择钱包。

## 当前安全状态

基础框架只支持 `EXECUTION_MODE=paper`，paper adapter 不发起任何网络请求。多钱包 secret
加载、EOA signer、L1 create/derive、L2 HMAC 和只读 `cmd/walletcheck` 已经实现；它可以验证真实
钱包凭证但不能下单。Polymarket CLOB V2 adapter、PostgreSQL 订单/Fill/资金/仓位账本和 outbox
也已经实现，但尚未在 `cmd/server` 中整体装配，因此 `EXECUTION_MODE=live` 仍会拒绝启动。
启用实盘前还需要：

1. 接入生产 secrets/HSM，并为实际钱包类型完成签名验收；`POLY_1271` 当前 fail closed；
2. 运行 migrations，装配 PostgreSQL order/reservation/fill/outbox adapters 和 coordinator；
3. 将已实现的 `riskcontrol.Service` 接到真实余额、仓位和订单数据源，并补齐同事务敞口检查、
   pending 对账、kill switch 控制面和可观测性；
4. 在 live composition 中启用已实现的 cancel Fill finality/grace worker，并增加 BUY 最大手续费
   预占 buffer；禁止旧累计 Reconcile 与新 Fill ledger 同时消费同一成交。

内存 Repository 只用于本地开发；重启会丢失订单和幂等记录，不能用于实盘。

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
POST /internal/jobs/position-exit-evaluation/run  # 仅在注入 PositionExitJob 后注册，当前 cmd/server 未装配
POST /internal/jobs/reconciliation/run             # 仅在注入 Reconciliation 后注册，当前 cmd/server 未装配
```

查询交易记录：

```bash
curl 'http://127.0.0.1:8090/api/v1/trades?from=2026-08-01T00:00:00Z&side=SELL&model_id=forecast-v2&strategy_id=multfactor_v2&limit=20&offset=0' \
  -H "Authorization: Bearer ${EXECUTION_API_TOKEN}"
```

该接口只读取 `execution_fills.status=CONFIRMED AND applied_at IS NOT NULL` 的记录，
并从 `position_lot_closures` 汇总 SELL 的已实现盈亏。返回价格和金额均为 JSON
字符串小数；不返回钱包地址、CLOB 凭证、签名或原始响应。配置
`TRADING_EXECUTION_DATABASE_URL` 后读取 PostgreSQL；未配置时 paper 模式返回空列表，
不会把 paper 订单状态伪装成真实成交。生产库需按顺序执行至
`migrations/0007_trade_history_read_model.sql`。

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
