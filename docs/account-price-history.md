# 账户历史价格与小时回撤输入

该功能只维护新增行情事实和读投影，不调用交易、资金或仓位写接口。策略原有48小时mid输入不受影响。

## 数据与读取

迁移 `0044_account_price_history.sql` 新增四张表：

- `account_price_history_windows`：不可更新/删除/截断的原始窗口证据。相同资产、窗口、规范化证据幂等；价格修订保留新证据。
- `account_price_history_points`：按资产及有效时刻唯一的最新点投影。
- `account_price_history_settlements`：按资产及真实结算生效时间的终价投影。
- `account_price_history_coverage`：成功覆盖的连续窗口合并；失败/缺口只保留最新状态投影。

`postgres.NewAccountPriceHistoryStore(db)` 实现 `accountpricehistory.Reader.Load(ctx, []Window)`。读取投影而不加载所有原始审计窗口；有效输入有点数、字节、覆盖行预算。版本指纹由有效输入内容生成，价格回到此前出现过的值仍能使缓存失效。

`Series` 按 `SortSeries` 排序后使用 `PriceAt`。普通点只向前最多一小时，源粒度不能粗于一小时；较新有效点过期后不会恢复更早旧价。Kalshi完整查询成交流水证实无新成交后，才写入有限 `ValidUntil`；新成交之间各自分段延续。结算自可信 `EffectiveAt` 起有效，0兑付是合法价格。

PM窗口 `resolution_seconds=0` 终点可能是按请求end回压的合成结算点，不作为普通历史价格保存。真实边界通过 `as_of` 读取；最终结算独立核验condition、token、二元终价、resolved状态及可信终态/交易时间。合法JSON科学记数精确转为普通Decimal字符串，不经float舍入。

Kalshi市场K线按 `market_settled_ts` 路由live/archive；成交流水独立按 `trades_created_ts` 分区。两种cutoff不能互换，即使市场仍活跃，其早期成交也可能归档。

成交流水证明前后均重新读取cutoff；长回填跨过归档切换时，按新分区重查完整区间。连续三次分界仍不稳定则本窗口失败，不能保存为无成交延续。

## CLI

先从合并master的精确源码构建两个二进制（主服务及工具），将工具置于同一release目录：

```sh
go build -o accountpricehistory ./cmd/accountpricehistory
accountpricehistory plan --max-windows 250
accountpricehistory backfill --max-windows 250 --timeout 45m
accountpricehistory incremental --max-windows 250 --timeout 45m
accountpricehistory coverage
```

连接从 `TRADING_EXECUTION_DATABASE_URL` 读取，或用 `--database-env NAME` 指向已有受限环境变量；不把URL放命令行，不记录凭据。源URL默认官方地址，测试/专用部署可用 `ACCOUNT_HISTORY_PM_BASE_URL`、`ACCOUNT_HISTORY_GAMMA_BASE_URL`、`ACCOUNT_HISTORY_KALSHI_BASE_URL`。

`plan` 与 `--dry-run` 只读目标和既有覆盖，不访问行情、不保存事实。backfill/incremental使用相同恢复计划：跳过完整覆盖，补失败和漏窗，重查最近两小时以接纳迟到修订。首次执行按需分批重复，关注 `planned/saved/missing/failed/deferred`；`deferred>0`表示预算内仅完成一部分。`missing/failed>0`退出非零，失败详情有界。未知或失败不会作为成功checkpoint。

`coverage` 在最近已完成UTC小时内检查持有段起止边界和整点，边界恰逢整点只计一次；取消或超时返回失败，不把未完成检查当作零缺口。

目标来自完整lot真实持有段，外部接管与historical_rebased_lot_sales正确结束时间保留；按资产跨账户合并重叠时段。暂停账户仍持仓时继续同步。已退出且历史完整，或已有可信终价覆盖之后，不再无限更新。

## 调度

`deploy/account-price-history.service` 使用现有 `/etc/trading-execution/env`，调用 `/opt/trading-execution/current/accountpricehistory`。首次安装保持timer禁用；完成受限回填与五账户覆盖验证后启用 `account-price-history.timer`，每小时UTC :17运行，Persistent=true。独立oneshot失败不影响交易主服务；并发最多2，总速率最多2req/s，单HTTP请求12秒，429尊重Retry-After且有限重试。

部署后必须记录至少两次自然小时调度。异常先停止timer/回退读取开关，保留行情证据；不删除表、重写资金或替换其他会话部署。迁移只涉及新增对象，可由具备public CREATE及新对象权限的受限应用配置执行；生产执行由发布者统一进行。
