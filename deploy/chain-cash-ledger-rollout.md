# 链上 pUSD 流水账本上线手册（任务 09-30-recon-balance-ledger）

本手册覆盖迁移 `0033_chain_cash_ledger.sql`、新版本发布、`chaincashinit` 初始化钱包 6/7、上线观察与回滚。
全程保持 `DECISION_CYCLE_ORDER_SUBMISSION_ENABLED=true`，不需要关闭下单开关。

## 行为概要

- 只有在 `execution_chain_cash_cursors` 里有游标行的账户才启用新逻辑；main、wallet-1 不初始化，行为与旧版完全一致。
- 余额比较：`账本 total_balance（本轮入账之后）+ 已知流水偏移 = 快照区块链上余额`。
  - 奖励（`CHAIN_CASH_REWARD_SENDERS`）与自有充值（`CHAIN_CASH_DEPOSIT_SOURCES`）达到确认深度后自动入账，总余额与可用余额同时增加。
  - 交易所结算（`TRADE`）与本账户赎回铸币（`REDEMPTION`）只分类不入账。
    `TRADE` 包括两种形态：对手方是 V2 交易所合约；或所在交易里有两个 V2 交易所之一发出、maker/taker 为本钱包的 `OrderFilled`
    （点对点结算：钱直接付给对手方 maker 与手续费合约 `0x115f48dc2a731aa16251c6d6e1befc42f92accc9`，SELL 款直接由对手方付来）。
    真实历史里约一半的成交是点对点结算，所以每段区块除转账外还要额外查两次 `OrderFilled`。
  - 来源不明的转入：`UNATTRIBUTED_CASH_IN`，`OBSERVED_ONLY`，不拦单，永不自动关闭。
  - 来源不明的转出：`UNATTRIBUTED_CASH_OUT`，`MANUAL_REVIEW`，账户级拦截，永不自动关闭。
    不明转出（无论是否达到确认深度）**都不计入**预期余额：账本没有扣减这笔钱，所以余额比较会一直报 `BALANCE_DRIFT / MANUAL_REVIEW`，与 `UNATTRIBUTED_CASH_OUT` 同时拦截，直到账本被人工修正。
  - 仍对不上：链上钱少 → `BALANCE_DRIFT / MANUAL_REVIEW`（账户级拦截）；链上钱多 → `BALANCE_DRIFT / OBSERVED_ONLY`（不拦）。
  - 流水读取（包括 `OrderFilled` 查询）失败或尚未追平 → `SOURCE_UNAVAILABLE`（来源 `EVM_ERC20_TRANSFER_LOGS`，账户级拦截），游标不推进。
- `BALANCE_DRIFT` 的指纹不再包含金额（所有账户），同一账户同一来源只保留一条 OPEN；新增 `first_observed_at` 记录首次发现时间。
  升级前按旧指纹记录的 OPEN 行不会被合并；对已初始化账户，余额核平后由 `resolveVerifiedChainCashBalanceIssues` 一并关闭。

## 0. 前置检查

1. 确认 `trading_execution` release 已包含本任务提交，且 release 目录内同时有 `trading-execution` 与 `chaincashinit` 两个二进制
   （在 release 构建时 `go build -o chaincashinit ./cmd/chaincashinit`；不要放到 release 之外的固定路径）。
2. 确认 `09-30-recon-source-stability` 已上线观察完毕。
3. 读取当前钱包 6 的问题，作为验收基线：

```sql
SELECT issue_id, issue_type, resolution, status, local_value, remote_value, remote_block_number, observed_at
FROM reconciliation_issues
WHERE execution_account_id IN ('wallet-6','wallet-7') AND status='OPEN'
ORDER BY observed_at;
```

## 1. 定向备份

```bash
pg_dump -h <DB_HOST> -U adminuser -d <DB_NAME> \
  --data-only --table=reconciliation_issues \
  -f /var/backups/trading-execution/reconciliation_issues_before_0033_$(date -u +%Y%m%dT%H%M%SZ).sql
```

## 2. 迁移 0033（adminuser）

应用账号没有 DDL 权限（0031 时已验证），迁移必须由 `adminuser` 执行：

```bash
psql -X -v ON_ERROR_STOP=1 -h <DB_HOST> -U adminuser -d <DB_NAME> \
  -f /opt/trading-execution/releases/<RELEASE>/migrations/0033_chain_cash_ledger.sql
```

迁移只新增两张表、一个列与若干触发器。旧版本进程在迁移后仍可运行：
`first_observed_at` 由触发器在插入时补为 `observed_at`，且任何更新都不会改变它。

**迁移后必须立即给应用账号授权**（新表由 adminuser 创建，迁移文件本身不含 GRANT，与 0016 的做法一致：授权在迁移之后单独执行）：

```sql
GRANT SELECT, INSERT, UPDATE ON execution_chain_cash_cursors, execution_chain_cash_transfers TO <APP_ROLE>;
```

然后确认六项权限全部为 `true`：

```sql
SELECT relation_name, privilege,
       has_table_privilege('<APP_ROLE>', relation_name, privilege) AS granted
FROM (VALUES
  ('execution_chain_cash_cursors','SELECT'),('execution_chain_cash_cursors','INSERT'),
  ('execution_chain_cash_cursors','UPDATE'),('execution_chain_cash_transfers','SELECT'),
  ('execution_chain_cash_transfers','INSERT'),('execution_chain_cash_transfers','UPDATE')
) AS required(relation_name, privilege);
```

授权必须在切换新 release **之前**完成：新版本每轮对账都会读取所有账户（包括 main、wallet-1）的游标表，
新版本的就绪检查会校验这六项权限，缺任何一项服务都拒绝启动（而不是运行后让所有账户 `SOURCE_UNAVAILABLE`）。

## 3. 环境变量

在 `/etc/trading-execution/env` 中新增（地址统一小写，也接受大写，程序会规范化）：

```text
CHAIN_CASH_REWARD_SENDERS=0x607c8c9866ef3b4665c5a384188706be738d8bf8
CHAIN_CASH_DEPOSIT_SOURCES=wallet-6=<钱包6资金EOA>,wallet-7=<钱包7资金EOA>
# 可选，默认值如下
CHAIN_CASH_MAX_BLOCKS_PER_RUN=1500
CHAIN_CASH_LOG_CHUNK_BLOCKS=100
```

- 资金 EOA 取 `cmd/walletdepositfund` / `cmd/wallet67fund` 实际使用的发送地址（钱包 6 `0x0aefd80d…593f`，钱包 7 `0xc9ba3537…e6d9`，以完整地址为准）。
- `CHAIN_CASH_DEPOSIT_SOURCES` 中的账户名必须是钱包文件里的账户，否则服务拒绝启动。
- `CHAIN_CASH_LOG_CHUNK_BLOCKS` 大于 100 会被拒绝（dRPC 免费版单次上限 101 块）。
- 每段 4 次 `eth_getLogs`（pUSD 转入、转出，`OrderFilled` 作为 maker、作为 taker）。日常每轮约 3 段 12 次调用；
  `CHAIN_CASH_MAX_BLOCKS_PER_RUN=1500` 时追赶轮最多 15 段 60 次调用（约 12 秒），远小于每账户 2 分钟的对账超时。不建议调高；
  单轮超过 30 段（例如大于 3000 块，或把段长调小后段数超过 30）时服务拒绝启动。
- 流水抓取复用 `POLYGON_RPC_URL` 与 `POLYGON_ORDER_FILLED_CONFIRMATIONS`（生产 64），不需要新增 RPC 配置。

## 4. 发布新版本

1. 切换 `/opt/trading-execution/current` 到新 release，`systemctl restart trading-execution`。
2. 等待日志出现 `Polymarket live startup reconciliation passed` 且 HTTP 监听端口就绪（不只看 systemd `active`）。
3. 此时还没有任何游标，行为与旧版一致。确认：
   - `DECISION_CYCLE_ORDER_SUBMISSION_ENABLED` 与数据库下单开关仍为开启状态；
   - `SELECT count(*) FROM execution_chain_cash_cursors;` 为 0。
4. 钱包 6 在这一阶段会多出一条新指纹的 `BALANCE_DRIFT`（旧那条仍 OPEN），属于预期，初始化后两条会一起关闭。

## 5. 初始化游标（先试运行，再 --execute）

在服务机上用服务同一份环境执行（保证奖励地址、资金地址与服务一致；缺 `CHAIN_CASH_REWARD_SENDERS` 时命令直接拒绝）：

```bash
set -a; . /etc/trading-execution/env 2>/dev/null; set +a
# The env file is not shell-quoted: a value containing `&` (the database URL) is
# truncated by `.`. Re-read such values verbatim.
export TRADING_EXECUTION_DATABASE_URL="$(awk -F= '$1=="TRADING_EXECUTION_DATABASE_URL" {print substr($0,index($0,"=")+1); exit}' /etc/trading-execution/env)"
export POLYGON_RPC_URL="$(awk -F= '$1=="POLYGON_RPC_URL" {print substr($0,index($0,"=")+1); exit}' /etc/trading-execution/env)"
cd /opt/trading-execution/current
```

### 5.1 钱包 6（起点 94681377，奖励前一个块）

```bash
./chaincashinit --account wallet-6 --wallet <钱包6地址> --start-block 94681377 \
  --actor <操作人> --reason "09-30-recon-balance-ledger: enable chain cash ledger for wallet-6"
```

试运行会读取全部 RPC 证据，并在一个事务里完成全部写入后回滚，输出 JSON。逐项检查：

- `balance_of_at_start_block` = `1.700022`，`ledger_checks.ledger_total` = `1.700022`，`ledger_matches_on_chain=true`；
- `ledger_checks.ledger_events_after_start_block`、`pending_fills`、`uncertain_orders`、`in_flight_redemptions` 均为 0；
- `classifications` 中 `REWARD` ≥ 1（区块 94681378 的 0.0015，以及之后各日奖励）；
- `balance_of_at_catch_up_block` 等于 `balance_of_at_start_block + net_transfers_after_start_block`（否则命令已拒绝：日志不完整）；
- `ledger_checks.ledger_total_after_catch_up` 等于 `1.700022 + 已入账奖励/充值`；`credited` 列出每笔入账；
- 出现 `UNATTRIBUTED_OUT` 时停止，先人工确认去向。成交转出（包括付给对手方 maker 和手续费合约的点对点结算）应全部落在 `TRADE`；
  如果成交时段出现 `UNATTRIBUTED_OUT`，先核对该交易是否有本钱包的 `OrderFilled`，以及交易所地址是否变更。

证据无误后执行：

```bash
./chaincashinit --account wallet-6 --wallet <钱包6地址> --start-block 94681377 \
  --actor <操作人> --reason "09-30-recon-balance-ledger: enable chain cash ledger for wallet-6" --execute
```

`--execute` 在同一个事务里：锁账户行 → 复核全部条件 → 写入游标（`processed_block` 直接等于追平区块）→ 写入流水 → 奖励/充值入账 → 提交。
服务在提交前看不到这个账户的游标（继续走旧逻辑），提交后只处理追平区块之后的增量，不会出现“看到游标但积压超过 `CHAIN_CASH_MAX_BLOCKS_PER_RUN`（默认 1500 块）”的状态。
如果追平期间正好有成交或赎回入账，复核会发现“起点之后账本有变动”并整体回滚，重跑即可（最好选在没有成交的时段执行）。

默认超时 10 分钟（`--timeout` 可调）。起点到当前约数万个块，每 100 块四次 `eth_getLogs`（2 万块约 800 次调用），预计 3–5 分钟；超时可用 `--timeout 20m` 放宽。

### 5.2 钱包 7

选择一个“已核平”的近期区块作为起点：最近一次钱包 7 对账 `verified_balance:` 为 1 且此后没有新的账户资金事件。不传 `--start-block` 时默认取 `latest - 64`。

```bash
./chaincashinit --account wallet-7 --wallet <钱包7地址> --actor <操作人> \
  --reason "09-30-recon-balance-ledger: enable chain cash ledger for wallet-7"
# 核对输出后加 --start-block <同一区块> --execute
```

若 `ledger_matches_on_chain=false` 或存在起点之后的资金事件，换一个没有成交的时段重新选起点。

## 6. 上线观察

1. 下一轮对账（≤ 5 分钟）后：

```sql
SELECT execution_account_id, processed_block, updated_at FROM execution_chain_cash_cursors;

SELECT issue_id, issue_type, status, resolution, resolved_at, details
FROM reconciliation_issues
WHERE execution_account_id='wallet-6' AND issue_type='BALANCE_DRIFT'
ORDER BY observed_at DESC LIMIT 5;
```

   预期：钱包 6 的两条 `BALANCE_DRIFT` 均为 `RESOLVED / AUTOMATIC`，details 以
   `automatically resolved after the chain cash ledger reconciled ...` 结尾；`RISK_STATE_HAS_OPEN_ISSUES` 拒单停止。
2. 账本证据：

```sql
SELECT account_event_id, event_type, total_balance_delta, total_balance_after, occurred_at
FROM execution_account_events WHERE event_type LIKE 'CHAIN_CASH_%' ORDER BY occurred_at;

SELECT execution_account_id, classification, count(*), sum(amount)
FROM execution_chain_cash_transfers GROUP BY 1,2 ORDER BY 1,2;
```

3. 每日奖励（约 00:09 UTC 发放，约 64 块后达到确认深度）：确认出现新的 `CHAIN_CASH_REWARD` 事件、可用余额同步增加，且没有新的账户级问题。
4. 连续 24 小时关注 `SOURCE_UNAVAILABLE` 来源 `EVM_ERC20_TRANSFER_LOGS` 的次数；它会账户级拦截一轮，下一轮读取成功后自动关闭。频繁出现说明 dRPC 免费版不稳定，需要评估付费套餐或备用 RPC。
5. 运行摘要中的 `chain_cash_transfers_persisted / credited / reattributed` 可用于核对每轮处理量。

## 7. 回滚

- **初始化之前**：直接切回旧 release。迁移只新增表/列/触发器，旧版本不受影响。
- **初始化之后，保留新版本**：删除游标行，账户即回到旧逻辑：

```sql
DELETE FROM execution_chain_cash_cursors WHERE execution_account_id='wallet-6';
```

  流水行与已入账的奖励/充值事件保留（它们是真实到账的钱，不撤销）。注意此后旧逻辑的余额比较会使用已包含奖励的账本，
  之后新的奖励会重新变成 `BALANCE_DRIFT`。
- **需要回到旧 release**：先按上一步删除游标（可选），再照常切换 release。旧版本不读取新表，`first_observed_at` 由触发器维护。
- 不要 `UPDATE`/`DELETE` `execution_chain_cash_transfers`：触发器只允许一次性回填 `account_event_id` 与赎回重新归类，其余修改一律拒绝。
- `UNATTRIBUTED_CASH_OUT` 只能在人工确认去向后，由运维以带原因的 SQL 关闭；它不会被任何自动规则关闭。
  账本 `total_balance`/`available_balance` 不会因这笔转出自动扣减，所以即使关闭了它，`BALANCE_DRIFT` 仍会继续拦截；
  要恢复交易，必须先人工修正账本（目前没有配套的扣减工具，需要单独设计并审计），修正后余额核平，`BALANCE_DRIFT` 自动关闭。
