# 实盘监控聚合接口

## 接口

```http
GET /api/v1/live-operations
Authorization: Bearer <LIVE_OPERATIONS_READ_ONLY_TOKEN>
Accept: application/json
```

成功响应：

```json
{
  "data": {
    "observedAt": "2026-08-19T08:00:48.123Z",
    "dataFreshnessSeconds": 4,
    "engine": {},
    "capital": {},
    "workers": [],
    "funnel": [],
    "risks": [],
    "orders": [],
    "positions": [],
    "events": [],
    "dataQuality": []
  }
}
```

该接口只读取内存中的不可变快照，不会在前端请求路径上调用 CLOB、Data API、链上 RPC，也不会触发下单、撤单或修改配置。后台默认每 10 秒聚合一次，HTTP 路径只做鉴权、快照新鲜度检查和 JSON 编码。

## 鉴权和错误

- `LIVE_OPERATIONS_READ_ONLY_TOKEN` 是独立的只读令牌；实盘模式至少 32 字节，且不能与执行令牌或任务令牌相同。
- Token 缺失或无效返回 `401`。
- 用交易执行 Token 访问只读接口返回 `403`。
- 从未生成完整快照，或最旧核心数据超过 `LIVE_OPERATIONS_MAX_SNAPSHOT_AGE`，返回 `503`。
- 其他内部读取错误返回 `500`。

错误结构固定为：

```json
{
  "code": "LIVE_SNAPSHOT_UNAVAILABLE",
  "message": "无法获取完整实盘快照",
  "requestId": "req-xxx"
}
```

## 数据口径

| 字段 | 权威来源与计算 |
| --- | --- |
| `capital.availableCash` | 钱包链上 pUSD 余额，不读取 allowance，不使用本地可用余额代替 |
| `positions[].shares` | Polymarket Data API/链上仓位 |
| `positions[].markPrice` | CLOB 最优买卖价算术中点；无完整双边盘口时回退 Data API `curPrice` 并降级 |
| `positions[].cost` | 外部真实 `shares × Ledger averagePrice` |
| `positions[].marketValue` | 外部真实 `shares × markPrice` |
| `capital.grossExposure` | 所有开放仓位 `cost` 之和 |
| `orders` | Ledger 订单与 `order_events`，并使用 CLOB Open Orders 验证开放状态 |
| `filledShares`、`MATCHED` | 仅来自已经通过 `/trades` 验真且写入 Ledger 的 Fill |
| `realizedPnlToday` | UTC 自然日内已确认、已入账 Fill 对应的 `position_lot_closures.realized_pnl` |
| `feeToday` | UTC 自然日内已确认、已入账 Fill 的手续费 |
| `dataFreshnessSeconds` | 钱包余额、外部持仓、CLOB 和 PostgreSQL 中最旧一次观察时间 |

`market_id`、`condition_id` 和 `token_id` 同时保留。仓位对齐键为 `execution_account_id + token_id`；YES/NO 通过相同 token 的 `outcome_index` 和 `outcome_name` 交叉检查，不依赖 token 数组顺序。

任何钱包余额或外部仓位核心读取失败都不会用零代替：本轮刷新失败并保留上一份成功快照；旧快照过期后接口返回 `503`。CLOB Open Orders、`/trades` 或盘口部分失败时，快照会标记 `degraded`，不能显示为 `healthy`。

## 状态与一致性

- `LIVE`：订单仍存在于 CLOB Open Orders。
- `PARTIAL`：已验真部分成交且余单仍开放。
- `MATCHED`：本地订单已经由真实 Fill 驱动到 `FILLED`。
- `CANCEL_PENDING`：已发起撤单，仍在处理 Cancel Race。
- `UNKNOWN`、`RECONCILING` 和 `MANUAL_REVIEW` 保留原始状态，不会为了页面展示强行映射成成功。

每轮至少检查：

- CLOB 开放订单是否属于本系统 Ledger；外部人工订单只报告，不修改。
- Ledger 开放订单是否仍在 CLOB。
- CLOB 已确认 Trade 是否已经验真入账。
- Data API/链上 shares 是否与 Ledger 一致。
- 同一 token 的 condition、outcome index 和 outcome name 是否一致。
- 最近对账是否成功、是否超过账户硬风控 `max_state_age`、是否存在未关闭问题。

## Worker heartbeat 和漏斗写入契约

迁移 `0011_live_operations.sql` 新增：

- `live_runtime_status`：固定记录 `cycle`、`monitor`、`prediction` 三个线程，并带当前 `run_id`。聚合器不读取上一次进程遗留的 heartbeat；当前 run 没有 heartbeat 时返回 `stopped`。
- `live_cycle_funnel`：以 `(run_id, cycle_id, stage_id)` 为主键。写入一轮时应在同一 PostgreSQL 事务中一次写全六步，聚合器只读取当前引擎 `run_id` 的最新 `cycle_id`，不会跨进程或跨轮累加。

线程写入 `max_heartbeat_age_ms` 后，接口按该真实阈值判断延迟；没有自定义值时建议 Cycle 使用计划周期两倍、Monitor 使用 6 分钟、Prediction 使用 60 秒。

当前交易执行进程还没有装配 Cycle、Monitor 和 PredictionScheduler 时，这三个线程会如实显示为 `stopped`。对应组件接入后只需持续更新上述 heartbeat/funnel 表，无需修改聚合 HTTP 协议。

## 风险字段说明

接口只展示当前 Go 硬风控实际执行的限额：总敞口、单市场敞口、按各账户 `daily_timezone` 统计的当日已验真交易金额加活动订单预占，以及按 `max_signal_age` 统计的过期预测数。已实现盈亏和手续费仍固定按 UTC 自然日展示。

当前硬风控表没有“当日亏损熔断金额”字段，因此 V1 不伪造 `loss` 限额。后续如果风控事务真正增加并执行该配置，再把同一口径加入响应；不能只为前端展示添加一个不参与交易拒绝逻辑的数字。

## 部署配置

后端：

```dotenv
LIVE_OPERATIONS_READ_ONLY_TOKEN=<至少32字节随机令牌>
LIVE_OPERATIONS_INTERVAL=10s
LIVE_OPERATIONS_REFRESH_TIMEOUT=8s
LIVE_OPERATIONS_MAX_SNAPSHOT_AGE=30s
LIVE_OPERATIONS_EVENT_LIMIT=50
```

前端服务端：

```dotenv
TRADING_EXECUTION_BASE_URL=http://127.0.0.1:8090
TRADING_EXECUTION_API_TOKEN=<与后端只读令牌相同>
```

令牌只能放在前端服务端环境变量中，不能打包进浏览器静态资源。
