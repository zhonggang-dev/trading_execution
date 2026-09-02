# Market Validation

Market Validation 是策略和交易所下单之间的执行侧硬门禁。Python 决定是否交易、方向、
数量和最差可接受价格；Go 重新确认市场身份、交易状态和当前价格，不重新计算策略信号。

## 下单顺序

```text
Python Strategy / SUBMIT
  -> 生成 OrderIntent
  -> client_order_id 历史幂等查询
  -> 静态风险限制
  -> Market Universe：按 condition_id 查询权威市场
  -> 校验 market/outcome/token/状态/neg_risk/tick size/快照时效
  -> Market Data 或 Polymarket CLOB：读取该权威 token 的最新盘口
  -> 校验最新价格没有越过 worst_price
  -> 原子占用 client_order_id 并保存 MarketValidation 证据
  -> Venue.Place
```

幂等历史查询在实时校验之前。已经受理过的完全相同请求直接返回原订单，不会因为市场之后
关闭而改变历史结果，也不会再次调用交易所。

## 策略返回的价格语义

策略返回两个不同概念：

- `evidence.reference_price`：策略输入快照中的 best ask（BUY）或 best bid（SELL），用于证明
  策略基于哪一个价格决策；
- `order.worst_price`：BUY 的最高可接受价格或 SELL 的最低可接受价格，是执行保护线。

执行前读取最新盘口：

- BUY：`latest_best_ask <= worst_price` 才可下单；
- SELL：`latest_best_bid >= worst_price` 才可下单；
- 价格必须是当前 `tick_size` 的精确整数倍，全程使用 decimal string，不转成 float；
- LIMIT 的实际 `price` 使用 `worst_price`，避免订单在校验与发送之间失去价格保护；
- MARKET 订单也必须带 `worst_price`，供后续真实 Venue adapter 做保护型 FOK/FAK 转换；
- `size` 是股数上限，`worst_price` 是价格上限，二者都不是资金预算。IOC intent（Kalshi 全部订单、
  Polymarket 全部 BUY）在校验时用最新盘口测量保护价内可成交的量：`executable_size = min(size, 保护价内可见深度)`，
  记入 `LIVE_CHECK` 证据并作为 adapter 提交的数量；保护价内深度不足 `size` 不拒单，按能成交的量下单，
  剩余立即取消。唯一下限是 venue `min_order_size`：可成交量低于它时以
  `PROTECTED_LIQUIDITY_BELOW_MIN_ORDER_SIZE` fail closed。Polymarket 的 IOC 由执行层用 GTC 限价单加
  立即撤单模拟，因为 CLOB 的 FOK/FAK BUY 是按 pUSD 预算成交的，会在盘口好于保护价时买超 `size` 股。

Kalshi 使用 `DEPTH_AWARE_LIMIT + IOC`：策略快照的最优价作为
`strategy_reference_price`，`worst_price` 必须处于可成交方向，且冻结盘口在该保护价内至少有正数可见深度。
Kalshi 盘口可能跳过中间 tick，因此不额外套用固定两 tick 距离上限。实际提交前 Go 会重新读取不超过
10 秒的官方盘口，允许价格向有利方向移动，也允许仍在 `worst_price` 内的不利移动。对新的 IOC intent，保护价内有正数深度
即可提交；当时能成交多少就成交多少，剩余由 venue 取消。对恢复中的历史 FOK intent，仍要求可见深度覆盖全部 shares。
最新最优价越过保护线、保护价内零深度、价格不在 tick 上或参考价缺失时均 fail closed，
Go 不会自行扩大策略给出的限价。

## OrderIntent 的市场上下文

这些字段由 Trading Execution 根据冻结的策略输入补齐，Python 不需要伪造执行身份：

```json
{
  "model_id": "model-a",
  "strategy_id": "strategy-v1",
  "execution_account_id": "account-model-a-strategy-v1",
  "market_id": "1122565",
  "condition_id": "0x05297f...",
  "outcome_index": 0,
  "outcome_name": "Yes",
  "token_id": "token-yes",
  "expected_neg_risk": false,
  "market_snapshot_at": "2026-08-18T04:20:02Z",
  "signal_at": "2026-08-18T04:20:05Z",
  "side": "BUY",
  "type": "LIMIT",
  "price": "0.53",
  "worst_price": "0.53"
}
```

`outcome_index` 是 YES/NO 与 A/B 市场的稳定桥梁。执行层用该 index 从 Market Universe 的
`outcomes` 找到权威 `token_id`，然后要求名称和 token 与意图完全对应；不能依赖 “Yes 永远
是数组第一个” 之外的猜测，也不能直接信任 Python 回传的 token。

`market_snapshot_at` 标识策略实际引用的冻结盘口，仅作为不可变决策审计证据；它的年龄不再
充当执行价格门禁。提交前 Trading Execution 会重新抓取官方订单簿，并使用
`latest_book_source_at` 校验执行价格的新鲜度、盘口深度和策略给出的 `worst_price` 保护边界。

## Market Universe Service 契约

Trading Execution 的 adapter 使用：

```http
GET /api/v1/markets/by-condition/{condition_id}
Authorization: Bearer ${MARKET_UNIVERSE_API_TOKEN}
Accept: application/json
```

成功响应：

```json
{
  "data": {
    "market_id": "1122565",
    "condition_id": "0x05297f...",
    "active": true,
    "closed": false,
    "resolved": false,
    "paused": false,
    "accepting_orders": true,
    "neg_risk": false,
    "tick_size": "0.01",
    "outcomes": [
      {"index": 0, "name": "Yes", "token_id": "token-yes"},
      {"index": 1, "name": "No", "token_id": "token-no"}
    ],
    "observed_at": "2026-08-18T04:20:05Z"
  }
}
```

找不到返回 `404`。其他非 2xx 或网络错误视为基础设施失败，整个订单 fail closed，不调用
Venue。`observed_at` 是这份市场元数据的观察时间，不应拿业务创建时间代替。

## 校验规则与拒绝码

| code | 条件 |
| --- | --- |
| `MARKET_CONTEXT_REQUIRED` | 执行上下文字段缺失 |
| `MARKET_SNAPSHOT_FUTURE` | 策略审计快照时间超过允许的未来时钟偏差 |
| `MARKET_NOT_FOUND` | condition_id 不存在 |
| `MARKET_IDENTITY_MISMATCH` | condition_id 对应的 market_id 已不一致 |
| `MARKET_METADATA_INVALID` | 权威二元 outcome/token 映射不完整或重复 |
| `MARKET_RESOLVED` | 市场已经结算 |
| `MARKET_CLOSED` | 市场关闭或 inactive |
| `MARKET_PAUSED` | 市场暂停 |
| `MARKET_NOT_ACCEPTING_ORDERS` | 市场拒绝新订单 |
| `OUTCOME_TOKEN_MISMATCH` | outcome index/name 与权威 token 映射不一致 |
| `NEG_RISK_MISMATCH` | neg_risk 在策略快照后变化 |
| `INVALID_TICK_SIZE` | 权威 tick size 无效 |
| `LIMIT_PRICE_MISMATCH` | LIMIT price 不等于策略 worst_price |
| `PRICE_TICK_MISMATCH` | 价格不是当前 tick size 的整数倍 |
| `LATEST_BOOK_UNAVAILABLE` | 最新盘口缺失、为空或状态非 OK |
| `LATEST_BOOK_IDENTITY_MISMATCH` | 最新盘口不是权威 outcome/token 对应的盘口 |
| `LATEST_BOOK_INVALID` | 最新盘口格式、排序或买卖价关系无效 |
| `LATEST_BOOK_SOURCE_STALE` | 最新盘口源时间过期 |
| `PRICE_DRIFT` | 最新可成交价越过 Python 的 worst_price |
| `NO_PROTECTED_LIQUIDITY` | IOC intent 在最新盘口保护价内没有可见深度 |
| `PROTECTED_LIQUIDITY_BELOW_MIN_ORDER_SIZE` | IOC intent 保护价内可成交量低于 venue `min_order_size` |

默认时效阈值为：策略快照 2 分钟、Market Universe 元数据 5 分钟、最新盘口 10 秒、未来
时钟偏差 2 秒。构造 `marketvalidation.Service` 时都可配置；实盘配置应结合服务 SLA 调整。

paper 使用显式 `PAPER_BYPASS` validator。live composition 会注入 Gamma Market Universe、
CLOB OrderBookSource 和 `marketvalidation.Service`，并生成 `LIVE_CHECK` 证据；缺少、过期或冲突的
Market 身份、状态、tick/盘口会 fail closed，且只有 `LIVE_CHECK` 才会进入数据库原子动态风控。
