# 资金与仓位预占

该模块是下单前最后一道并发资金边界。`riskcontrol.Service` 的只读检查可以尽早拒绝订单，
但只有 PostgreSQL 事务内的 `Reserve` 才能决定资金或 shares 是否真正可用。Redis、进程锁和
普通的“先查余额再更新”都不能作为实盘权威状态。

## 数据模型

### execution_accounts

```text
total_balance = available_balance + reserved_balance
```

- `total_balance`：钱包当前未消耗的全部 collateral，包括已被活动订单预占的部分；
- `available_balance`：新 BUY 可以继续使用的余额；
- `reserved_balance`：所有本地 pending、交易所 open 和结果未知 BUY 的占用总和。

`LOWER(wallet_address) + collateral_asset` 唯一，禁止两个逻辑 execution account 分别计算
同一个真实钱包的余额。如果未来确实需要多个策略共享一个钱包，必须改为独立 `wallets` 行作为
统一锁和账本，不能删除该约束后各算各的。

### execution_positions

```text
total_shares = available_shares + reserved_shares
```

- `total_shares`：当前仍持有的 token shares；
- `available_shares`：新 SELL 可以继续预占的 shares；
- `reserved_shares`：活动或结果未知 SELL 已预占的 shares。

主键是 `(execution_account_id, token_id)`。同一个 token 不能被重复关联到不同 Market。

### asset_reservations

每个 `order_id` 和 `client_order_id` 只有一条记录。`intent_fingerprint` 防止同一个幂等键被
更换 token、方向、价格、数量或账户后再次使用。

状态：

| 状态 | 含义 |
| --- | --- |
| `ACTIVE` | pending、submitted、open 或 partial fill，剩余部分继续预占 |
| `RELEASED` | 明确 rejected，或 cancelled 已经过 Fill finality/grace，未成交部分已释放 |
| `SETTLED` | 全部成交，预占已全部消耗/释放 |
| `RECONCILIATION_REQUIRED` | 下单或撤单结果未知，保留预占等待交易所对账 |

`asset_reservation_events` 是 append-only 审计记录；累计成交量和累计成交金额组成幂等事件键。

## 锁和事务顺序

所有 live 写路径使用完全相同的顺序：

```text
BEGIN
  -> SELECT execution_accounts ... FOR UPDATE
  -> SELECT asset_reservations ... FOR UPDATE（存在时）
  -> SELECT execution_positions ... FOR UPDATE（SELL 或仓位结算时）
  -> 条件校验和余额/仓位更新
  -> 写 reservation 和 audit event
COMMIT
```

账户行是同一真实钱包的 serialization point。假设余额为 100，两个 Worker 同时各提交 80：

1. Worker A 先获得账户行锁，将 `available=20, reserved=80` 后提交；
2. Worker B 随后获得锁，条件 `available_balance >= 80` 不成立，整笔事务回滚；
3. Venue 只会收到 Worker A 的订单。

SELL 使用同一个账户锁并额外锁 token position，因此两笔 SELL 也不能同时消费同一份
`available_shares`。数据库还有 `(execution_account_id, token_id, side)` 活动记录唯一索引，
原子执行“已有同方向订单”和“防止重复 SELL”规则。固定锁顺序同时减少账户/仓位交叉更新
造成的数据库死锁；序列化失败或死锁错误会有限重试。

## 预占金额

- BUY：`worst_price * (1 + max_buy_fee_rate_bps / 10000) * size`，不是策略快照价，也不是下单时 best ask；
- SELL：`size` shares；
- BUY 缺少正数 `worst_price` 时 fail closed；
- 全部使用 PostgreSQL `NUMERIC` 和 Go decimal string，不经过 `float64`。

`worst_price` 是 Python 允许的最差成交价，但其合法性仍由 Go Market 校验。`MaxBuyFeeRateBPS`
由 Go execution 配置所有平台费、builder fee 和逐 Fill 舍入余量的合计上界，不能采用 Python
策略传入的 fee。该配置必须显式提供；即使已确认零费，也要显式配置 `0`，防止 live 因漏配而
静默回到无手续费保护。

`reserve_unit_price` 保存的是最差价格加每 share 手续费 buffer 后的单位现金上限。迁移 0008
强制以下不变量（已有历史行先 `NOT VALID`，新写入和更新仍立即受约束）：

```text
initial_reserved_balance = requested_shares * reserve_unit_price
settled_notional + settled_fees + remaining_reserved_balance
  <= initial_reserved_balance
```

因此 Fill ledger 即使收到超过配置上限的 reported fee，也会在同一个数据库事务中 fail closed；
账户扣款、Fill、仓位和 reservation 会整体回滚，不会使用该订单之外的 available balance。

## 部分成交和释放

旧兼容路径使用累计 `filled_size + average_fill_price`；新的实盘路径只消费真实 confirmed Fill，
数据库保存累计 `settled_shares + settled_notional + settled_fees`。`fill_key` 去重保证重复轮询
不会重复扣款或增加仓位。两个路径不能同时启用。

BUY 示例：余额 100，按 `worst_price=0.80, size=100, max_buy_fee_rate_bps=250` 预占 82；
随后成交 30 shares，累计均价 0.70、累计手续费 0.525：

```text
成交消耗             21 + 0.525 = 21.525
剩余订单继续预占     0.82 * 70 = 57.4
total_balance         100 - 21.525 = 78.475
reserved_balance      57.4
available_balance     21.075
```

如果之后取消，只释放未成交的 57.4；已成交的 `gross + fee` 不会退回。BUY 成交 shares 会在同一事务增加
对应 position。SELL 则从 `total_shares/reserved_shares` 消耗成交 delta，并把成交现金增加到
`total_balance/available_balance`。

## 失败分类

- 明确 `REJECTED`：释放全部未成交预占；
- 明确 `CANCELLED`：先保留未成交预占并继续查 Fill，finality/grace 结束后才释放；
- `FILLED`：结算全部成交并结束预占；
- Place/Cancel 网络超时、响应字段非法、数据库提交结果未知：不得自动释放，标记
  `RECONCILIATION_REQUIRED`；
- 服务重启后，对账 Worker 必须优先扫描 `RECONCILIATION_REQUIRED` 和长期 `ACTIVE` 记录，
  使用 `client_order_id / venue_order_id` 查询交易所后再结算或释放。

不能只靠固定 TTL 自动释放结果未知订单。交易所可能已经接受该订单，TTL 到期直接释放会造成
超买或超卖。TTL 只能触发高优先级对账和告警。

## 与执行服务的接入

当前 `execution.Service` 顺序为：

```text
幂等查单
  -> Go hard risk
  -> Market 最新状态/盘口校验
  -> 创建本地 RECEIVED order
  -> PostgreSQL Reserve
  -> Venue Place
  -> 轮询真实 trades/fills 并由 FillLedger 原子结算
  -> 更新本地 order
```

Venue 不会在 Reserve 失败后被调用。Place 返回不确定错误时，本地订单记录为失败，但预占保留
并要求对账。

目前 live 模式仍被配置层禁用。正式启用前还必须完成：

1. 在 live composition 启用交易所订单/成交启动对账和持续对账，并配置 cancel finality/grace；
2. 钱包余额、仓位初始导入及定期 reconciliation，所有修正也必须先锁账户行；
3. 为历史仓位回填 cost basis 和 position lots；
4. 将单 Market/策略/钱包敞口与每日额度的最终判断合并到同一 check-and-reserve 事务，避免
   这些上限仍受只读快照竞态影响；
5. 完成链上 settlement 延迟、negative-risk conversion 和 allowance 的账务口径。

迁移文件：[`migrations/0001_asset_reservations.sql`](../migrations/0001_asset_reservations.sql) 和
[`migrations/0003_fills_positions_ledger.sql`](../migrations/0003_fills_positions_ledger.sql)、
[`migrations/0008_buy_fee_reservation_guard.sql`](../migrations/0008_buy_fee_reservation_guard.sql)。
PostgreSQL 实现：[`internal/adapter/postgres/reservation.go`](../internal/adapter/postgres/reservation.go)。

真实 PostgreSQL 并发测试：

```bash
TRADING_EXECUTION_TEST_DATABASE_URL='postgres://...' make test-postgres
```

测试在唯一临时 schema 中验证并发 BUY、并发 SELL、部分成交、重复累计回调和取消释放，完成后
删除该测试 schema。
