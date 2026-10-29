# 账户即组合（account-portfolio routing）上线与回滚手册

> 只是手册，未执行。上线需要：PR 合并到 master → 从 master 构建 → 按本手册切换。
> 生产 release 在切换前必须确认是 master 的祖先，且只从合并后的 master 构建。

## 0. 这次发布改变了什么（线上可见）

| 项 | 之前 | 之后 |
| --- | --- | --- |
| 配置 | `DECISION_CYCLE_BINDINGS_JSON` + 来源模式 + 三个账户名单 + Kalshi 输入开关 + `KALSHI_LIVE_BINDINGS_JSON` | `DECISION_CYCLE_PREDICTION_MODEL_ID` + `DECISION_CYCLE_PREDICTION_SOURCE_MODE` + `DECISION_CYCLE_ACCOUNTS_JSON` + `KALSHI_LIVE_ACCOUNTS_JSON`；旧变量出现即拒绝启动 |
| 新订单/新 lot 的 `model_id` | `deepseek_masked` | `v4.1 flash`（预测模型名本身） |
| Kalshi | 挂在 `main` 的策略请求里，`main` 的策略/模型 | `kalshi:main` 独立一次策略请求，cycle_id `kalshi:main:<T>` |
| `main` 的策略请求 | 含 PM + Kalshi 市场、PM + Kalshi lot | 只含 PM |
| lot 归属 | 账户 + 模型 + 策略 | 只看账户：wallet-6 的 2 个、wallet-7 的 13 个 `gemini_masked` 旧 lot 会进入现策略请求，可能被卖出（已接受） |
| 授权 | 按（模型, 策略, 账户） | 按（策略, 账户）；迁移 0041 放宽提交触发器 |

策略服务影响（已核实 10-29）：

- `brier_mid_edge_once` 线上 `bindings: []`，接受任意（模型, 账户）。
- 它的"单次建仓"按（账户, 策略, 市场）去重。`main` 名下只交易过 1 个 Kalshi 市场，拆成 `kalshi:main` 后这 1 个市场可能被再买一次。
- multfactor（wallet-6/7）只回显 `model_id`。

## 1. 切换前只读检查

在服务器上用应用库只读执行（`URL=$(sudo grep -m1 '^TRADING_EXECUTION_DATABASE_URL=' /etc/trading-execution/env | cut -d= -f2-)`）：

```sql
-- 1) 没有未完成的策略投递（有也能恢复，但最好为 0）
SELECT intent_payload->>'execution_account_id' AS account, intent_payload->>'market_source' AS source, status, count(*)
FROM strategy_order_intent_deliveries WHERE status IN ('PENDING','SUBMITTING') GROUP BY 1,2,3;

-- 2) 启用的绑定恰为 4 个 Polymarket 账户各一条（模型名不必改）
SELECT model_id, strategy_id, execution_account_id FROM execution_strategy_bindings WHERE enabled ORDER BY 3;

-- 3) Kalshi 账户状态
SELECT execution_account_id, total_balance, available_balance, reserved_balance
FROM execution_accounts WHERE execution_account_id LIKE 'kalshi:%';
```

另外确认：

- 线上 release 是 master 的祖先。
- 当前时间在 :x1~:x2（避开 :x3:30 / :x8:30 定时对账；停服务后先查 `reconciliation_runs` 有无孤儿 `RUNNING`）。
- 策略服务 `127.0.0.1:8787/health` 正常。

## 2. 迁移 0041（先做，向后兼容）

只替换 `enforce_live_order_submit_risk()` 的函数体，绑定检查从（模型, 策略, 账户）改为（策略, 账户）存在启用行。不改表、不改索引、不改数据。旧二进制在新函数下行为不变：对同一批启用行，新函数只会更宽松。

```bash
psql -X -w -v ON_ERROR_STOP=1 -h <私网主机> -U adminuser -d trading_execution \
  -f /opt/trading-execution/releases/<新release>/migrations/0041_binding_authorization_without_model.sql
```

验证：

```sql
SELECT prosrc LIKE '%AND enabled%' AND prosrc NOT LIKE '%model_id = btrim%'
FROM pg_proc WHERE proname='enforce_live_order_submit_risk';   -- 期望 true
```

函数属主不变，应用账号无需新增授权。

## 3. env 转换（切换 release 前一刻做，先备份）

```bash
sudo cp -a /etc/trading-execution/env /var/tmp/te-env-backup-<ts>/env
sudo cp -a /etc/trading-execution/kalshi-live.env /var/tmp/te-env-backup-<ts>/kalshi-live.env
sudo cp -a /etc/trading-execution/kalshi-strategy-input.env /var/tmp/te-env-backup-<ts>/kalshi-strategy-input.env
```

对照表（按 10-29 线上值换算）：

| 删除 | 换成 |
| --- | --- |
| `DECISION_CYCLE_BINDINGS_JSON=[...4 条, prediction_model_id="v4.1 flash", model_id="deepseek_masked"...]` | `DECISION_CYCLE_PREDICTION_MODEL_ID=v4.1 flash` |
| `DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON='{"v4.1 flash":"SANDBOX"}'` | `DECISION_CYCLE_PREDICTION_SOURCE_MODE=SANDBOX` |
| `DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON='[]'` | （删除） |
| `DECISION_CYCLE_ENTRY_DISABLED_ACCOUNTS_JSON='["wallet-1"]'` 与 `DECISION_CYCLE_STRATEGY_DISABLED_ACCOUNTS_JSON='["wallet-1"]'` | wallet-1 行 `mode=PAUSED` |
| `kalshi-strategy-input.env` 里的 `DECISION_CYCLE_KALSHI_STRATEGY_INPUT_ENABLED=true` | （删除；文件可以留空） |
| `kalshi-live.env` 里的 `KALSHI_LIVE_BINDINGS_JSON=[{model_id, strategy_id, execution_account_id:"main", api_key_id, private_key_path}]` | `KALSHI_LIVE_ACCOUNTS_JSON=[{"execution_account_id":"kalshi:main","api_key_id":"<原值>","private_key_path":"<原值>"}]` |

新增：

```text
DECISION_CYCLE_ACCOUNTS_JSON='[{"execution_account_id":"main","market_source":"POLYMARKET","strategy_id":"brier_mid_edge_once","mode":"ACTIVE"},{"execution_account_id":"wallet-1","market_source":"POLYMARKET","strategy_id":"strategy_pending_b","mode":"PAUSED"},{"execution_account_id":"wallet-6","market_source":"POLYMARKET","strategy_id":"multfactor_v1","mode":"ACTIVE"},{"execution_account_id":"wallet-7","market_source":"POLYMARKET","strategy_id":"multfactor_v2","mode":"ACTIVE"},{"execution_account_id":"kalshi:main","market_source":"KALSHI","strategy_id":"brier_mid_edge_once","mode":"ACTIVE"}]'
```

注意：

- 旧变量只要还在任何 EnvironmentFile 里（哪怕 `VAR=` 空值）都会拒绝启动。
- `DECISION_CYCLE_ORDER_REPORT_STRATEGY_IDS_JSON='["brier_mid_edge_once"]'` 保持不变。之后 `main` 和 `kalshi:main` 两个账户的订单都会回报给策略服务。
- 数据库不需要改：现有 4 条启用绑定行（`model_id=deepseek_masked`）按（策略, 账户）匹配新配置。

## 4. 切换 release

按 deploy-service 的 trading-execution 流程：

1. 持部署锁。
2. 停服务后查孤儿 `RUNNING` 对账。
3. 原子切换 symlink。
4. 启动。

## 5. 切换后验证

- 启动日志：
  - `Kalshi live account preflight passed ... execution_account_id=kalshi:main`。
  - 没有 `replaced decision-cycle variables`。
  - 没有 `startup reconciliation failed closed`。
- 第一轮 `decision cycle completed` 的 `binding_runs`：
  - 有 5 条，含 `kalshi:main`，且后者没有 `route_unavailable`。
  - `wallet-1` 为 `account_strategy_disabled=true`。
  - 没有 `failed`（缺 Kalshi 预测导致的 `INCOMPLETE_MODEL_COVERAGE` 只封 `kalshi:main` 入场，属预期）。
- 数据库：

  ```sql
  SELECT cycle_id, execution_account_id FROM strategy_decision_runs
  WHERE decision_at = (SELECT max(decision_at) FROM strategy_decision_runs) ORDER BY 2;

  SELECT intent->>'model_id', intent->>'market_source', execution_account_id, count(*)
  FROM execution_orders WHERE created_at > now() - interval '30 minutes' GROUP BY 1,2,3;
  ```

  期望：新订单 `model_id='v4.1 flash'`；Kalshi 订单账户为 `kalshi:main`；没有 `STRATEGY_ACCOUNT_BINDING_DENIED` 拒单。
- `main` 的策略请求里没有 `market_source=KALSHI` 的预测或 lot（`strategy_decision_runs.input_payload`）。
- 对账：各账户下一轮 `COMPLETED`，没有新的 `POSITION_DRIFT`。

## 6. 回滚

回滚 = 恢复 env 备份 + 切回旧 release；迁移 0041 保留（旧二进制兼容）。回滚前必须先确认：

```sql
-- a) 没有新版本下的未终态 SELL：旧二进制入账时会因 lot 的模型/策略与订单不一致而拒绝该成交
SELECT order_id, status, intent->>'model_id', intent->>'target_lot_id' FROM execution_orders
WHERE intent->>'side'='SELL' AND intent->>'model_id' <> 'deepseek_masked'
  AND status NOT IN ('FILLED','CANCELLED','REJECTED');

-- b) 没有挂在 kalshi:* 账户名下的未完成投递：旧二进制只认 main 名下的 Kalshi 投递
SELECT count(*) FROM strategy_order_intent_deliveries
WHERE intent_payload->>'execution_account_id' LIKE 'kalshi:%' AND status IN ('PENDING','SUBMITTING');
```

回滚后的已知限制：新版本期间开出的 lot（`model_id='v4.1 flash'`），旧二进制不会把它们发给策略（旧版按模型筛选 lot），直到再次前滚。它们仍在账本、对账、结算与赎回里。
