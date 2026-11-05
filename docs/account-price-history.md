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

从PR合并后的master完整SHA源码构建工具，在已验证release中保留二进制及来源记录；通过独立受保护指针 `/opt/trading-execution/account-price-history/current` 引用，不依赖主服务的current切换：

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

## 权限迁移

已应用的0044不修改。增量迁移 `0047_account_history_fk_lock_permission.sql` 只向 `trading_execution_app` 授予原始历史表 `UPDATE(history_id)`，供PostgreSQL外键内部 `SELECT FOR KEY SHARE` 的行锁权限检查；不授予整表UPDATE或其他列写权限，不移除外键。原追加式语句触发器仍拒绝任何真实UPDATE/DELETE/TRUNCATE。

部署必须使用真实非超级用户角色验证Save，而非仅用超级用户跑迁移：0044状态下可出现42501且整笔事务回滚；0047后Save/Load及重复执行应成功，孤儿外键仍23503，直接变更原始事实仍被拒绝。同步报告的storage failure只保留经过校验的五位SQLSTATE，不输出原错误文本、DSN或SQL参数。 角色回归只在专用隔离PostgreSQL上运行，测试连接须能创建和清理临时LOGIN角色与schema；生产应用连接不得用作测试连接。

## XXL-JOB调度

统一复用prediction_infra现有XXL-JOB BEAN执行器，handler为 `syncAccountPriceHistoryJob`。它无shell直接调用固定CLI：

```text
/opt/trading-execution/account-price-history/current/accountpricehistory incremental --max-windows 250 --timeout 45m
```

Infra配置 `ACCOUNT_PRICE_HISTORY_ENABLED/BINARY/DATABASE_URL/MAX_WINDOWS/TIMEOUT`；默认关闭；小时任务配置与XXL参数硬上限均为250窗口/45m，参数只能进一步收紧配置。BINARY为上述受保护绝对路径，DATABASE_URL来自服务器专用受限环境，子进程仅获得PATH和专用数据库连接配置，不继承整份Infra凭据。XXL参数仅允许 `{"max_windows":250,"timeout":"45m"}`，只能收紧配置预算。

首次回填及五账户覆盖验证完成后，在XXL Admin创建BEAN任务，使用每小时:17调度（例如cron `0 17 * * * ?`），配置失败重试与防重叠策略。CLI自身仍有数据库防重入锁、并发最多2、总速率最多2req/s、单HTTP12秒及有限重试。busy、failed、missing、deferred或无效报告必须失败回调，不能把未执行或未完成当成功；取消先SIGTERM，再有界结束，断点可续。XXL日志只保留job/log ID及结构化计数，不原样透传stderr或失败详情。

仓库不再提供该任务的systemd service/timer模板，避免双调度入口。部署后观察至少两次真实XXL小时触发及回调；不得为验收手动触发交易周期。

## 独立制品与回滚

工具与主服务遵守同一master来源要求：PR合并后按完整SHA导出构建，记录 `source_branch=master`、完整commit、源码归档SHA256、CLI二进制SHA256和release路径。独立指针只能指向含已验真工具的release，不能随主服务更新到缺少工具的旧二进制集合。

发布者持保护锁并核对预期worker指针后原子切换 `/opt/trading-execution/account-price-history/current`；不切主服务current，release清理须同时保护worker当前及回滚引用。

异常时在XXL禁用本任务、等待或有界取消其自身执行，保留历史数据；仅将独立工具指针回滚至已核验release，核对handler配置和工具hash后再恢复调度。不删历史表、不改业务账本、不回退其他会话服务。
