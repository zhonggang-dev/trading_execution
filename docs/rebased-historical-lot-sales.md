# 已由现金对齐覆盖的历史卖出补录

用于此前人工卖出后已通过 `MANUAL_REBASE` 对齐现金、同时直接关闭接管 lot、却没有保存逐 lot 卖出分配的历史事实。0043 新增 `historical_rebased_lot_sales`；维护工具只向该表追加证据，`cash_effect` 固定为零。当前范围为一份经过核对的两笔成交清单，每账户一笔，不提供 HTTP 写接口。

## 数据与取证

清单为 UTF-8 JSON，顶层只有 `sales`（恰好两条、账户和 lot 均不重复）。每条包含：

- `execution_account_id, lot_id, market_id, condition_id, token_id`
- `transaction_hash, order_hash, block_number, block_hash, fill_log_index`
- `trade_shares, trade_gross_proceeds, trade_fee`：整笔官方成交，十进制规范字符串。
- `allocated_shares, allocated_gross_proceeds, allocated_fee, allocated_principal`：仅归属纳管 lot 的份额、回款、费用和登记本金。整单回款/费用按份额比例精确分配，不能把钱包中外部份额的回款计入纳管。
- `sold_at`：链上成交区块时间，RFC3339 整秒。
- `rebase_account_event_id`：当初覆盖这次卖出的原始 `MANUAL_REBASE` 事件，发生时间必须等于原 lot 的 `closed_at`。
- `official_activity_sha256, official_activity_timestamp`：人工审阅的官方 Data API v2 活动安全字段规范 JSON 的摘要和成交时间。活动需逐项匹配账户查询、条件/token、SELL、交易 hash、份额、毛回款及时间。CLI 保存其审阅来源，**不会将清单摘要冒充为自动重新抓取活动的结果**。

真实清单及官方活动证据保存在受限维护目录或本地验收证据目录，不提交实际账户的清单到仓库。清单不含资金地址、RPC URL、密钥或数据库凭据。

CLI 会在数据库加锁前，使用 `POLYGON_RPC_URL` 重新检查 Polygon 137 的精确 V2 OrderFilled：订单 hash、账户 maker、SELL、token、份额、毛回款、总费、日志编号、区块 hash/编号和至少 128 个确认；同一最终 receipt 必须同时证明 pUSD 净转入等于毛回款减费用、CTF 净转出等于整笔份额。通过按区块编号查询 canonical header 核对真实成交时间。原始 maker 地址只在内存中参与比较，输出与持久化证据均不包含它。

## 迁移与权限

由维护 owner 执行 `migrations/0043_rebased_historical_lot_sales.sql`，使用受限进程内的 PG 环境变量。迁移显式使用 `public,pg_temp`；应用角色 `trading_execution_app` 不能充当迁移 owner。新表不授予 PUBLIC 权限，对应用角色只授予 SELECT 并拒绝 DML 权限；owner 的 UPDATE/DELETE/TRUNCATE 也由 append-only trigger 拒绝。

完成迁移后核对：

```sql
SELECT pg_get_userbyid(c.relowner) <> 'trading_execution_app' AS owner_is_maintenance,
       has_table_privilege('trading_execution_app',c.oid,'SELECT') AS app_can_read,
       has_table_privilege('trading_execution_app',c.oid,'INSERT,UPDATE,DELETE,TRUNCATE') AS app_can_write
FROM pg_class c WHERE c.oid='public.historical_rebased_lot_sales'::regclass;
```

预期 true / true / false。新版本 readiness 检查此表，先完成 owner 迁移再切换服务。旧版本兼容新增空表。

## 默认演练与明确提交

只从已合并主干精确 SHA 构建 `go build -o rebasedsales ./cmd/rebasedsales`，保留工具 hash。数据库连接默认读取 `REBASED_SALES_MAINTENANCE_DATABASE_URL`；由受限 owner broker 将 owner 连接放在其他环境键时，可使用 `--database-env TRADING_EXECUTION_DATABASE_URL`，不在参数或文件里传明文 DSN。

完整默认数据库演练（重新核链、锁定账户和 lot、运行全部插入约束、比较原始事实指纹、最后 ROLLBACK）：

```bash
./rebasedsales --manifest /secure/repair-manifest.json \
  --database-env TRADING_EXECUTION_DATABASE_URL \
  --actor operator --reason '补录已经现金对齐的历史卖出证据'
```

只有明确提交模式会 COMMIT：

```bash
./rebasedsales --manifest /secure/repair-manifest.json \
  --database-env TRADING_EXECUTION_DATABASE_URL \
  --actor operator --reason '补录已经现金对齐的历史卖出证据' \
  --apply --confirm-sha256 <逐字节核对过的清单SHA256>
```

两行在同一事务内完成，任何一行失败都回滚全部。锁等待最多两秒，SQL 十秒，含取证的总操作最多九十秒。检查同 token 无非终态订单；lot 必须是原接管仓位、已 CLOSED/剩余零、原始份额和本金严格匹配，四类原有关闭分配/原成交及外部卖出均不能重复。相同清单重跑返回 `already_applied=true`、新增行零；同 lot 的不同来源清单或部分批次均拒绝。

报告给出清单摘要、是否提交、行数、`cash_effect="0"` 和原始事实前后 SHA256。指纹覆盖受影响账户余额/版本、原 lot、原 position、已有账户事件和持仓事件；必须完全一致。工具不重写余额、现金事件、持仓事件、lot、position 或旧 rebase。

## 读模型验收

新事实只接入开仓区间 `CohortMetricFacts` 的关闭分配。毛回款减登记本金进入未扣费已实现盈亏，卖出费另列；最新持仓估值仍只读当前剩余。补录发生在现在，归属使用真实 `sold_at`；观察时间早于成交时不提前计入。重复的后续关闭证据造成份额超分配仍返回不可用，不会被历史缺证据警示吞掉。

隔离 PG 测试覆盖默认演练零持久行、提交后缺证据警示消失、主单包含外部份额的精确分配、原始事实不变、二行原子失败、相同清单幂等、不同来源拒绝、应用角色只读及 owner 不可修改。可用 `REBASED_SALES_REHEARSAL_MANIFEST` 指定受限真实清单重放隔离数据库，并以 `REBASED_SALES_REHEARSAL_REPORT` 保存演练报告；数据库测试使用 fixture 取证器，真实收据须由独立 RPC 验证与维护 CLI 再核。
