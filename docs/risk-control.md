# Go 交易硬风控

`riskcontrol.Service` 是 Python 策略与 Venue 下单之间的执行侧硬门禁。它只对原始
`OrderIntent` 返回放行或拒绝，不修改 market、token、BUY/SELL、价格、数量或账户。

## Python 风控与 Go 风控的边界

| 所属 | 规则示例 | 结果 |
| --- | --- | --- |
| Python 策略 | probability、edge、longshot、止盈止损、入场/离场、仓位算法 | 产生 `SKIP` 或完整 `SUBMIT` |
| Go 执行安全 | 余额/可卖持仓、金额硬上限、重复订单、暂停、Kill Switch、身份授权、时效与对账 | 原样放行或拒绝 `SUBMIT` |

Go 不会缩小 size、调整 price、把 BUY 改为 SELL，或替 AI/Python 重新计算
仓位。策略返回完整 `SUBMIT/BUY` 后，Go 使用账户策略中的单笔、每日、Market、
strategy 和 wallet 金额硬上限做最后一道原子门禁。硬上限只拒绝新增 BUY 风险，
已经超限的账户仍允许 SELL 减仓、撤单和对账。

## 检查输入

每次检查必须从 `HardRiskSource` 读取同一个 `execution_account_id` 的一致性快照：

- `total_balance / available_balance / reserved_balance`，并满足
  `total = available + reserved`；
- 当前仓位及其 `strategy_id / market_id / token_id` 归属，以及
  `total_shares / available_shares / reserved_shares`；
- `RESERVED/SUBMITTING/ACKNOWLEDGED/LIVE/PARTIALLY_FILLED/UNKNOWN` 等非终态订单的剩余占用；
- 当前钱包、策略、Market 和全局控制状态；
- `max_order_notional / max_daily_traded_notional / max_market_exposure /
  max_strategy_exposure / max_wallet_exposure` 金额硬上限；
- 策略绑定、状态新鲜度与对账结果。

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
| `SAME_DIRECTION_ORDER_EXISTS` | 同钱包、同 token 已有同方向 BUY |
| `DUPLICATE_SELL_ORDER` | 同钱包、同 token 已有活动 SELL |
| `INSUFFICIENT_SELL_POSITION` | 卖出数量超过持仓减去活动 SELL 占用 |
| `INSUFFICIENT_WALLET_BALANCE` | BUY 保护金额超过可用余额 |
| `MAX_ORDER_NOTIONAL_EXCEEDED` | BUY 单笔金额超过账户硬上限 |
| `DAILY_TRADED_NOTIONAL_EXCEEDED` | 当日已确认成交、活动预占和候选 BUY 合计超过硬上限 |
| `MAX_MARKET_EXPOSURE_EXCEEDED` | 当前 Market 持仓、活动 BUY 和候选 BUY 合计超过硬上限 |
| `MAX_STRATEGY_EXPOSURE_EXCEEDED` | 当前策略持仓、活动 BUY 和候选 BUY 合计超过硬上限 |
| `MAX_WALLET_EXPOSURE_EXCEEDED` | 当前钱包持仓、活动 BUY 和候选 BUY 合计超过硬上限 |

时间戳还会拒绝超过允许时钟偏差的未来时间。策略响应的 `decided_at` 会写入
`OrderIntent.signal_at`；`market_snapshot_at` 来自该策略实际引用的冻结盘口。

可卖数量仍以相同 `token_id` 的 `available_shares` 为准；活动 SELL 已经体现在
`reserved_shares`，不会再次从 available 中扣减。

## 实盘并发与持久化要求

`HardRiskSource.Snapshot` 是领域适配接口，不等于原子资金预占。真实接入时必须在持久化层按
`execution_account_id` 完成以下一个不可分割的 check-and-reserve 流程：

```text
锁定执行账户 / 开启事务
  -> 读取余额、仓位、活动订单和控制项的一致视图
  -> 执行本文件中的执行安全判断
  -> 按 client_order_id 幂等写入 pending 风险占用
  -> 提交事务并释放账户锁
```

后续订单成功受理时把 pending 占用转为订单占用；Market 校验或下单失败时释放，进程崩溃时
由 durable reservation 和对账任务恢复。结果未知的订单不能只靠 TTL 自动释放；TTL 只负责
触发对账和告警。只做无锁的“读快照再检查”会让两个并发 BUY 或 SELL 同时通过，因此禁止用于
live。表结构、锁顺序、部分成交和失败处理见 [`asset-reservations.md`](asset-reservations.md)。

paper 继续使用静态 guard。live 把账户余额/持仓、pause/Kill Switch、binding 和
reconciliation freshness 以及金额硬上限放进 ReservationManager 的同一个账户行锁事务内检查并预占，
避免“先查再扣”的 TOCTOU；数据库 trigger 还会在进入 SUBMITTING 前
二次阻断 crash-resume 绕过。全局 Kill Switch 在 migration 中默认开启，必须完成账户、policy、
binding 和新鲜对账审计后才能显式解除。
