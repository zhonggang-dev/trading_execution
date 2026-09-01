# 成交处理与权威仓位账本

实现位置：

- `internal/service/fillprocessor`：读取、归一化和计算真实 Fill；
- `internal/adapter/polymarket`：把 `/data/trades` 的 taker/maker 分量映射为内部 Fill；
- `internal/adapter/postgres/fill_ledger*.go`：在一个事务内完成去重、资金、仓位、批次、订单和 outbox；
- `internal/service/outboxdispatcher`：至少一次发布已提交的成交/仓位事件；
- `migrations/0003_fills_positions_ledger.sql`：Fill、事件、批次、快照和 outbox 表。

## 成交真相来源

下单响应的 `matched/delayed` 和订单查询的 `size_matched` 不是权威资金记账凭证。它们可以推动
订单观察与对账，但不能生成仓位。只有 `/data/trades` 返回的真实记录才会生成内部 Fill，并且：

| Trade status | 处理 |
| --- | --- |
| `MATCHED` / `MINED` / `RETRYING` | 保存观察和审计事件，不改资金与仓位 |
| `CONFIRMED` | 原子记账 |
| `FAILED` | 保存失败观察，不记账 |

旧服务因为 `/trades` 有传播延迟，会在没有 trade 时用下单响应的 `makingAmount/takingAmount`
补造成交。新实现明确禁止该 fallback：宁可继续保留预占并轮询，也不产生无法去重和核对的仓位。

Polymarket 一个 trade 可以同时引用一个 taker order 和多个 maker orders，因此内部幂等身份是：

```text
(venue, venue_fill_id, internal_order_id)
```

而不是只使用 `venue_fill_id`。`execution_fills` 同时有确定性 `fill_key` 主键和上述唯一约束；
重复轮询、进程重启和多个 worker 同时处理相同 Fill 都只会入账一次。

REST `/data/trades` 走与订单接口共享的 token-bucket 限流和超时；分页结果会按 trade ID 合并，
同一 ID 同时出现旧状态和 `CONFIRMED` 时保留更先进状态。生产建议以 authenticated user WebSocket
作为低延迟入口，REST 使用带重叠窗口的补洞/重启对账，二者都调用同一个幂等 processor，不能
为每个 model/strategy 单独重复拉全量 Market trades。

## 原子事务

每个确认 Fill 使用 PostgreSQL `SERIALIZABLE` 事务和固定锁顺序：

```text
execution_accounts FOR UPDATE
  -> execution_orders FOR UPDATE
  -> execution_fills / asset_reservations FOR UPDATE
  -> execution_positions / position_lots FOR UPDATE
  -> 更新资金、预占、仓位快照、批次、订单累计值
  -> 写 fill / position / account / order 审计事件
  -> 写 transactional outbox
COMMIT
```

因此不会出现“订单显示成交但仓位未增加”、或“仓位已增加但事件没发布”的半提交状态。数据库死锁和
序列化冲突只做有界事务重试；`COMMIT` 返回错误时结果可能未知，调用方必须按 `fill_key` 查询，
不能反向补偿或盲目再扣一次。

累计订单字段：

```text
filled_size       = sum(fill.shares)
filled_notional   = sum(fill.shares * fill.price)
average_fill_price = filled_notional / filled_size
total_fees        = sum(platform_fee + builder_fee)
```

全部金额使用 JSON decimal string、Go `big.Rat` 和 PostgreSQL `NUMERIC`。普通 Fill 要求
`gross = shares × price`；Polygon V2 Fill 以链上 6-decimal integer gross 为权威，并要求它与
`shares × API price` 的差小于一个 pUSD base unit。ledger 还会校验
`total_fee = platform_fee + builder_fee`、BUY/SELL 净现金方向和完整结算证据，即使绕过标准
processor 也不能写入一组自相矛盾的金额。

## 手续费与真实现金

BUY 的 `net_cash_delta = -(gross_notional + total_fee)`；SELL 的
`net_cash_delta = gross_notional - total_fee`。最终 `gross_notional/total_fee` 来自已确认的 Polygon
V2 `OrderFilled` 日志；CLOB fee schedule 只用于独立交叉检查和预占上限，不能替代实际扣款。
zero builder 时 `builder_fee=0`、`platform_fee=total_fee`；非零 builder 只有在权威拆分来源能与
链上 total 精确对账时才允许入账，否则进入人工处理。

下单前 BUY 预占是
`worst_price × (1 + max_buy_fee_rate_bps/10000) × shares`。手续费上限由 execution 配置，
覆盖平台费、builder fee 和逐 Fill 舍入余量，不能由 Python 策略指定。数据库同时约束
`settled_notional + settled_fees + remaining_reserved_balance <= initial_reserved_balance`；超过
配置上限的异常 Fill 会整体回滚而不会消耗未预占现金，调用方必须告警并转人工处理。

## 仓位快照、独立批次和盈亏

`positions`（数据库表名 `execution_positions`）保存当前快照；`position_events` 保存每次变化；
`position_lots` 为每一个 BUY Fill 建立独立开仓批次，并保存 `model_id + strategy_id` 来源。

当前成本法是加权平均成本法：

- BUY：批次成本包含实际 BUY fee，增加 `shares` 和 `cost_basis`；
- SELL：按卖出前总仓位的比例，从所有开放批次等比例减少 shares 与成本；
- 已实现盈亏：`SELL 净收入 - 分摊成本`；
- 未实现盈亏：有新鲜 mark 时为 `mark_price × remaining_shares - remaining_cost_basis`；
- 没有 mark 时未实现盈亏记为 0，而不是假设 token 价值为 0。

等比例减少批次可以让迟到或乱序到达的多个 BUY Fill 不改变整体已实现盈亏，同时仍保留剩余
仓位来自哪些 model/strategy 的可审计信息。每个 SELL Fill 的批次分摊另写入
`position_lot_closures`。

## 部分平仓与 dust

`is_dust` 只是派生分类：shares 或 mark value 低于配置阈值时为 true。它绝不修改真实数量。

例如持有 10 shares，卖出 8.999 shares 后：

```text
total_shares     = 1.001
available_shares = 1.001（订单已终止时）
reserved_shares  = 0
is_dust          = true/false（取决于配置和 mark）
```

不会为了“方便关闭仓位”把 `1.001` 改成 0。只有真实 SELL Fill、赎回或经审计的外部 reconciliation
事件才有权减少 shares。历史仓位若已有 shares 但没有 cost basis/lots，SELL 会 fail closed 并返回
`POSITION_COST_BASIS_MISSING` 或 `POSITION_LOTS_MISSING`；上线前必须先做一次批次和成本回填。

## 订单状态和 Cancel Race

一个订单可以有多次 Fill，每次确认后更新累计值：未完全成交为 `PARTIALLY_FILLED`，完全成交为
`FILLED`。IOC/FAK 的部分成交会先记录 `PARTIALLY_FILLED`，再记录 `CANCELLED`；未成交预占保持
`RECONCILIATION_REQUIRED`，等 Fill finality/grace 后才释放。
对 Kalshi IOC，POST 的 `fill_count` 只用于确定初始订单终态，真正的成交 shares、金额和费用仍必须来自官方
fills API；官方 order 累计值与本地 fill ledger 不一致时不释放剩余预留。
已取消订单后来出现确认 Fill 时，允许修正累计成交；如果累计达到全部数量，状态修正为 `FILLED`。
`MANUAL_REVIEW` 后出现部分确认 Fill 时仍保持 `MANUAL_REVIEW` 和预占；累计全部成交时可修正为
`FILLED`，保证“自动对账放弃”不会阻止真实成交入账。

撤单成功不会立刻释放预占：`/data/trades` 可能仍有传播延迟。execution 先把 reservation 保持为
`RECONCILIATION_REQUIRED`；对账服务再次同步真实 Fill，经过默认 30 秒（可配置）的 finality/grace
窗口后才调用 `FinalizeCancellation` 释放剩余部分。live composition 已启用 reconciliation Runner
与 authoritative Fill synchronizer，并明确禁止旧的累计 `Reconcile` 与新 Fill ledger 同时记账。

## 事件发布

确认 Fill 与 `trading.fill.confirmed.v1` outbox 行在同一事务提交。非最终观察使用
`trading.fill.observed.v1` / `trading.fill.failed.v1`，mark 使用
`trading.position.marked.v1`。dispatcher 用 `FOR UPDATE SKIP LOCKED` 租约领取、指数退避重试，
语义为 at-least-once。消费者必须以 `topic + event_key` 幂等；不能假设消息只到一次。

## 测试

普通单元测试覆盖精确手续费、taker/maker 映射、非最终状态、Fill key 和 outbox 重试。PostgreSQL
集成测试额外覆盖：非确认 trade 不入账、确认升级、重复 Fill、多次 BUY、FAK 部分 SELL、批次
等比例减少、残余 dust 不清零、mark/PnL 和 outbox 原子写入。

```bash
TRADING_EXECUTION_TEST_DATABASE_URL='postgres://...' make test-postgres
```
