# Go 交易硬风控

`riskcontrol.Service` 是 Python 策略与 Venue 下单之间的执行侧硬门禁。它只对原始
`OrderIntent` 返回放行或拒绝，不修改 market、token、BUY/SELL、价格、数量或账户。

## Python 风控与 Go 风控的边界

| 所属 | 规则示例 | 结果 |
| --- | --- | --- |
| Python 策略 | probability、edge、longshot、止盈止损、入场/离场、仓位算法 | 产生 `SKIP` 或完整 `SUBMIT` |
| Go 硬风控 | 余额、金额与敞口上限、重复订单、防重复卖出、暂停、Kill Switch、时效 | 原样放行或拒绝 `SUBMIT` |

Go 不会为了让订单满足限额而缩小 size、调整 price、把 BUY 改为 SELL，或替 Python
重新计算策略。若策略提交的订单不安全，整笔订单直接拒绝。

## 检查输入

每次检查必须从 `HardRiskSource` 读取同一个 `execution_account_id` 的一致性快照：

- `total_balance / available_balance / reserved_balance`，并满足
  `total = available + reserved`；
- 当前仓位及其 `strategy_id / market_id / token_id` 归属，以及
  `total_shares / available_shares / reserved_shares`；
- `RESERVED/SUBMITTING/ACKNOWLEDGED/LIVE/PARTIALLY_FILLED/UNKNOWN` 等非终态订单的剩余占用；
- 当日累计成交金额；
- 当前钱包、策略、Market 和全局控制状态；
- 命中的风险策略与各项上限。

金额全部使用 decimal string 和精确有理数运算，不经过 `float64`。BUY/SELL 都优先使用
`worst_price * size` 计算候选金额；只为旧的 LIMIT 内部调用保留 `price * size` fallback。
实盘 BUY 预占必须有 `worst_price`。

## 检查规则

执行顺序是 fail closed 的：快照读取失败、快照身份不匹配或策略配置无效都不会调用 Venue。

| code | 条件 |
| --- | --- |
| `GLOBAL_KILL_SWITCH` | 全局停止交易 |
| `EXECUTION_ACCOUNT_PAUSED` | 当前执行钱包暂停 |
| `STRATEGY_PAUSED` | 当前策略暂停 |
| `MARKET_RISK_PAUSED` | 当前 Market 被风控暂停 |
| `PRICE_TIMESTAMP_REQUIRED` / `PRICE_STALE` | 缺少价格时间或价格快照过期 |
| `SIGNAL_TIMESTAMP_REQUIRED` / `SIGNAL_STALE` | 缺少信号时间或 Python 决策过期 |
| `RISK_STATE_STALE` | 余额、仓位、订单等风险状态过期 |
| `MAX_ORDER_NOTIONAL_EXCEEDED` | 超过单笔最大金额 |
| `DAILY_TRADED_NOTIONAL_EXCEEDED` | 加上本单后超过钱包当日最大交易金额 |
| `SAME_DIRECTION_ORDER_EXISTS` | 同钱包、同 token 已有同方向 BUY |
| `DUPLICATE_SELL_ORDER` | 同钱包、同 token 已有活动 SELL |
| `INSUFFICIENT_SELL_POSITION` | 卖出数量超过持仓减去活动 SELL 占用 |
| `INSUFFICIENT_WALLET_BALANCE` | BUY 保护金额超过可用余额 |
| `MAX_MARKET_EXPOSURE_EXCEEDED` | BUY 后超过单 Market 最大资金敞口 |
| `MAX_STRATEGY_EXPOSURE_EXCEEDED` | BUY 后超过单策略最大资金占用 |
| `MAX_WALLET_EXPOSURE_EXCEEDED` | BUY 后超过钱包总风险敞口 |

时间戳还会拒绝超过允许时钟偏差的未来时间。策略响应的 `decided_at` 会写入
`OrderIntent.signal_at`；`market_snapshot_at` 来自该策略实际引用的冻结盘口。

当前实现允许有足够可卖持仓、且没有活动 SELL 占用的风险降低型 SELL，即使钱包原有敞口
已经超过 BUY 上限；Kill Switch、暂停、时效、单笔金额和当日交易金额仍然生效。

## 敞口口径

- 钱包总敞口：全部持仓的保守 `risk_value` + 全部活动 BUY 的 `worst_price * remaining_size`；
- Market 敞口：相同 `market_id` 的上述金额；
- 策略资金占用：相同 `strategy_id` 的上述金额；
- 可卖数量：相同 `token_id` 的 `available_shares`；活动 SELL 已经体现在
  `reserved_shares`，不能再次从 available 中扣减；
- 当日交易金额：建议以执行账户配置时区的自然日统计 BUY 与 SELL 的实际成交金额；未成交
  委托通过活动订单占用处理，不提前计入已成交金额。

仓位 `risk_value` 的估值方法由风险数据适配器统一提供并版本化，不能由 Python 在订单中指定。

## 实盘并发与持久化要求

`HardRiskSource.Snapshot` 是领域适配接口，不等于原子资金预占。真实接入时必须在持久化层按
`execution_account_id` 完成以下一个不可分割的 check-and-reserve 流程：

```text
锁定执行账户 / 开启事务
  -> 读取余额、仓位、活动订单、当日成交和控制项的一致视图
  -> 执行本文件中的硬风控判断
  -> 按 client_order_id 幂等写入 pending 风险占用
  -> 提交事务并释放账户锁
```

后续订单成功受理时把 pending 占用转为订单占用；Market 校验或下单失败时释放，进程崩溃时
由 durable reservation 和对账任务恢复。结果未知的订单不能只靠 TTL 自动释放；TTL 只负责
触发对账和告警。只做无锁的“读快照再检查”会让两个并发 BUY 或 SELL 同时通过，因此禁止用于
live。表结构、锁顺序、部分成交和失败处理见 [`asset-reservations.md`](asset-reservations.md)。

当前 server 仍是 paper 模式，使用静态 guard；`riskcontrol.Service` 已实现并满足 `port.Guard`
接口，PostgreSQL 资金/仓位预占 adapter 也已实现，但真实 PostgreSQL OrderRepository、数据库
连接、余额/仓位同步与 Venue 对账尚未接入。接入完成前配置层继续拒绝 live 启动。
