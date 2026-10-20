# 复用 main / wallet-1 的 DeepSeek 策略切换

本方案复用现有 EOA `main` 和 `wallet-1`，不创建新的真实钱包。两套新策略继续共用现有策略服务和 v4 协议；正常交易、资金预占与对账规则仍由原服务执行。

## 目标路由与分阶段开关

显式配置 `DECISION_CYCLE_TOPOLOGY=deepseek-reuse-four-wallet`：

```text
deepseek_masked / strategy_pending_a / main
deepseek_masked / strategy_pending_b / wallet-1
deepseek_masked / multfactor_v1 / wallet-6
deepseek_masked / multfactor_v2 / wallet-7
```

四条路由使用完全相同的 `prediction_model_id`，`DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON` 只声明该来源为 `SANDBOX`。精确源名称以当前 PIT snapshot 为准，不从业务别名推导。上游 Prediction 服务可以继续为其他消费者生产 Echo 结果；本模式不消费它们，不要求全局停掉 Echo。

默认 `four-wallet` 保持旧 Echo + masked 四钱包合同，不会因为部署新二进制就解锁 `main/wallet-1`。新模式初次验收仍保留 `DECISION_CYCLE_ENTRY_DISABLED_ACCOUNTS_JSON=["main","wallet-1"]`；策略调用使用新绑定，但 BUY 被账户门禁阻止。后续可以逐个移出这两个账户，不能意外把 wallet-6/7 加入该列表。

`strategy_pending_a/b` 是可直接替换的临时示例标识。正式名称确定后，只需在同一份
`DECISION_CYCLE_BINDINGS_JSON` 中替换 main 和 wallet-1 的 `strategy_id`，然后重新执行
启动/预检；名称必须以字母开头、只含 ASCII 字母数字、`.`、`_`、`-` 且不超过 64 个字符，两个
ID 必须互不相同，也不能占用 wallet-6/7 的 `multfactor_v1/v2`。不需要替换源码或新增算法
白名单。旧 Echo 授权行以 `enabled=false` 保留；历史订单、成交、财务事件、lot 的原
`model_id/strategy_id` 不改，新策略不能接管旧 lot。

## 旧仓处置原则

退出清单应来自完整真实资产盘点，而不是只读当前 managed lots。一个账户可能同时包含 managed、外部剩余、已结算和精度零头；它们的归属、成本与处置证据分别记录。

- 可卖仓位只按审核过的 `SELL + LIMIT` 数量、限价及有效期执行，不新增 BUY 凑最小量。
- 盘口声明的最小量不等于本地必须拒绝的 SELL；小额是否被接受、是否成交，以交易所结果为准。
- 不可成交仓位和精度零头保留旧归属及真实数量，不转给新策略，不直接置零。Polymarket 小于 0.01 股的旧 lot 不进入常规策略输入，但继续核账。
- 已结算、显示价值为零的仓位仍需核验 payout、所有权和链上余额；没有确认凭证不能声称已赎回或已关闭。
- 赎回只使用已核验的目标适配合约和既有授权；本轮维护工具不自动授权其他合约。

## 三条独立路径

正常交易服务：仅使用其运行绑定及受控开关。不能为了清理旧钱包而关闭全局风控、放宽对账阈值或让新策略卖旧仓。

精确退出维护工具：默认只读，只有明确的执行入口和审核资产清单才能签名下单或广播赎回。交易未知、超时或重启后先恢复既有身份，不重新生成另一张订单。拒绝、部分成交和剩余数量都必须保留。

审计基线维护工具：默认只读，数据库演练完整回滚；执行基线只改变经审核的账户账本与证据，不触发交易、赎回或授权。基线 apply 与退出执行不能隐式相互调用。

### 基线 CLI

构建入口为 `go build ./cmd/walletcutover`。在已安装正确版本、受保护环境变量和私有 manifest 后，先执行：

```bash
./walletcutover --manifest /private/operator/reuse-cutover.json
./walletcutover --manifest /private/operator/reuse-cutover.json --database-dry-run
```

第一条只读核验，第二条演练完整数据库事务并回滚；两者都不提交资产退出或基线写入。实际 apply 必须使用精确审核过的文件和受控操作角色：

```bash
./walletcutover \
  --manifest /private/operator/reuse-cutover.json \
  --apply-baseline \
  --confirm-sha256 REVIEWED_MANIFEST_SHA256 \
  --actor ACCOUNTABLE_OPERATOR \
  --reason REVIEWED_REASON
```

正常服务数据库身份不能执行受控基线函数。真实部署时按 migration 和运行角色安装所需的只读权限，不能把操作角色凭据写入普通服务环境。

迁移不会猜测服务器上的运行角色。部署完成后由数据库管理员按实际 schema 和角色显式收窄并授予读取权限；示例中的 `trading_execution` schema、`trading_execution_app` 角色必须按实际值替换。生产 owner 可能通过默认权限自动给新表写权限，因此必须在同一事务先对四张切换审计表 `REVOKE ALL`，再授予应用 `SELECT`；不能只追加 `GRANT SELECT`，也不要修改全库默认权限或其他表。以下是运行对账/控制台读取路径所需的权限清单：

```sql
BEGIN;
REVOKE ALL ON
  trading_execution.execution_wallet_cutover_batches,
  trading_execution.execution_wallet_cutover_accounts,
  trading_execution.execution_wallet_cutover_assets,
  trading_execution.execution_wallet_cutover_lot_adjustments
FROM trading_execution_app;

GRANT USAGE ON SCHEMA trading_execution TO trading_execution_app;
GRANT SELECT ON
  trading_execution.execution_wallet_cutover_batches,
  trading_execution.execution_wallet_cutover_accounts,
  trading_execution.execution_wallet_cutover_assets,
  trading_execution.execution_wallet_cutover_lot_adjustments,
  trading_execution.execution_external_position_baselines,
  trading_execution.execution_external_position_baseline_items,
  trading_execution.execution_external_position_dispositions,
  trading_execution.execution_accounts,
  trading_execution.execution_wallet_migrations
TO trading_execution_app;

COMMIT;

GRANT USAGE ON SCHEMA trading_execution TO trading_execution_cutover_operator;
GRANT EXECUTE ON FUNCTION trading_execution.execution_apply_wallet_reuse_cutover(JSONB,TEXT,TEXT,TEXT,TEXT)
TO trading_execution_cutover_operator;
```

复核时应逐表确认 `trading_execution_app` 只有 `SELECT`（`INSERT/UPDATE/DELETE/TRUNCATE/REFERENCES/TRIGGER` 均为 false），且它没有受控 apply 函数的 `EXECUTE`。`0035` 必须由独立的迁移/数据库所有者安装，不能让普通服务角色成为函数所有者；撤销 PUBLIC 权限不会隔离所有者。

`trading_execution_cutover_operator` 权限组由管理员预先创建，只允许专用维护登录身份加入，不能加入普通服务角色；维护凭据不进入正常服务环境。若权限组在迁移后创建，管理员仍须执行上例的 `GRANT EXECUTE`。

apply 成功输出是后续预检的基线证明，须以 mode `0600` 保存在受保护文件中；不能手工拼接 `applied=true`。同一批次、同一原始文件哈希的已完成结果可以只读重放；不同内容不能复用同一批次。

CLI 重放会先按 batch 与原始文件 SHA 查询已完成批次，再决定是否需要新的链上审计。这样即使审核文件已过期，已成功提交的结果仍可只读恢复；过期只阻止新的 audit/apply。manifest 的资产清单必须覆盖 main/wallet-1 本地账本及有效外部基线的全部非零 token，包括 token id `0`。

### 退出 CLI

构建入口为 `go build ./cmd/walletexit`。私有退出 manifest 使用独立 schema `trading.wallet_reuse_exit.v1`，与财务基线文件分开；交易限价和数量只能来自本轮审核清单。

```bash
./walletexit --manifest /private/operator/reuse-exit.json
```

默认命令不签署资产动作、不提交订单或链上交易。真正执行必须显式选择，并保留整个执行过程使用的同一 journal：
默认退出命令只做本地清单校验，不能被当作最新链上余额、授权或盘口已经通过的证明；实际执行入口会重新验证这些前置条件。

```bash
./walletexit \
  --manifest /private/operator/reuse-exit.json \
  --journal-file /private/operator/reuse-exit.journal.json \
  --execute \
  --confirm-sha256 REVIEWED_MANIFEST_SHA256 \
  --actor ACCOUNTABLE_OPERATOR \
  --reason REVIEWED_REASON
```

这些是代码入口说明，不是当前生产执行指令；示例路径和批准标记不代表真实批准。退出 journal 记录已提交动作的稳定身份，不能删除后盲目重跑。结果未知时只恢复原订单或原链上提交；不因响应丢失重新签一笔新动作。
真实新退出动作必须在目标服务已停止的维护主机执行，不能仅依靠全局下单 kill switch：自动赎回是独立链上写入路径。只读清单校验和查询既有 journal 不等于新提交。

维护身份使用自己拥有的 `0600` 凭据文件和私有目录；不能为了让工具读取而放宽正常服务密钥文件的权限。维护数据库角色与普通服务角色分开，凭据不进入仓库或命令输出。

## 审计基线和收益口径

用户已选择以核验后的真实资产建立切换起点，历史差额独立保留审计，而非逐笔还原全部旧账才切换。

基线必须记录最终性区块及 hash、精确资产、原账本版本、前后余额和持仓、旧所有权、调整分类、操作者、原因和审核文件 SHA。只有限定账户、正确身份、无在途状态、受控暂停和版本未漂移时才允许 apply；重复相同基线是幂等操作。

历史差额不能伪造成普通 SELL、费用、已查明亏损或新策略收益。旧 closed lots、订单、fills、成本和审计记录继续保留。真实退出后需要同步旧 managed lot/position 与剩余资产，不能仅写一条现金调整而留下错误 OPEN lot。未由 managed lot 覆盖的残余必须有独立所有权记录。

钱包页面的历史累计收益不因本次切换重置，也不能被称为新策略收益。新策略使用新绑定的交易和切换边界；维护活动与一次性审计调整不进入普通策略成交或新策略 PnL。

## 部署前顺序

1. 从已评审并合入 `master` 的精确 SHA 构建，备份现有发布、环境与受影响数据库记录，核对迁移和回退限制。
2. 在同一最终性区块重新盘点资产、授权和订单，复核私有 manifest 的数量、限价、有效期、残余方式及拟议基线金额。历史盘点和示例值不是生产执行批准。
3. 保持 main/wallet-1 新 BUY 与自动策略调用关闭，在受控维护状态下执行批准的退出；复核交易、赎回和剩余数量。
4. 对最终真实现金及剩余资产完成审计基线 dry-run，复核后 apply；只在 manifest 的 `open_issue_ids` 中逐条列出已审核、仍 OPEN 且被本次资产/余额证据覆盖的 issue，原 issue 行保留并追加切换批次链接，未列出的告警保持 OPEN。
5. 账链一致后独立执行 `chaincashinit` 的 dry-run 和初始化。它只负责未来流水，不能用于修复过去差额。
6. 同步数据库新 enabled 绑定和服务器环境，以新模式启动，保留 main/wallet-1 BUY 门禁，完成四钱包对账及新策略验收。
7. 独立批准后逐个恢复 main/wallet-1 新买入，检查 wallet-6/7 正常运行。

生产 manifest、journal、密钥与环境文件不得进入 Git。凭据只放受保护文件，日志不输出 HMAC、签名、私钥或带凭据的 URL。

预检入口增加 `--topology deepseek-reuse-four-wallet` 和 `--cutover-baseline-evidence-json /private/operator/reuse-cutover.applied.json`；其余发布身份、环境文件、风险批准和禁单证据参数仍须完整提供。预检核对已 apply 输出与数据库批次、文件 hash、账户和钱包身份，不仅检查 JSON 标记。配置示例见 `deploy/deepseek-reuse-four-wallet-decision-cycle.env.example`。

## 维护窗口与回退

wallet-6/7 的路由和资产保持不变；单体服务的账本维护与发布需要受控维护窗口，期间其新下单和调度可能短暂停止。窗口前检查在途订单、成交和对账，不强行打断未知业务。

链上成交不能通过恢复数据库备份撤销。已提交的财务调整不能删除；需要后续审计补偿。新策略产生订单或持仓后，也不能直接换回旧 Echo 绑定并遗留新资产。回退和继续修复都必须保留现存订单恢复、旧残仓对账与结算路径。
