# 官方钱包盈亏只读接口

两个接口使用现有 `LIVE_OPERATIONS_READ_ONLY_TOKEN`，不会下单、改账户授权或触发对账。浏览器通过控制台BFF访问，令牌只留在服务端。

## 支持账户

`GET /api/v1/wallet-pnl/accounts`，无查询参数。

返回：

```json
{"data":{"accounts":[{"execution_account_id":"main","market_source":"POLYMARKET"}],"observed_at":"2026-10-30T09:00:00Z"}}
```

以上仅为协议示例。实际集合来自服务启动时已校验的Polymarket账户，按账户ID排序；逐个核对 `execution_accounts.wallet_address` 的真实注册，不返回地址，不调用外部盈亏服务。Kalshi和未知账户不属于此能力；前端不得硬编码四个钱包。

注册缺失、地址无效或数据库错误使整个请求返回503，不返回部分成功。任意查询参数返回400，超时504；认证及HTTP方法沿用现有只读边界。成功响应禁止缓存。

## 盈亏曲线

`GET /api/v1/wallet-pnl?execution_account_id=main&interval=1w&fidelity=3h`。

`interval` 支持 `1d/1w/1m/all`，`fidelity` 支持 `1h/3h/12h/18h/1d`。服务从权威注册读取资金地址，再查询Polymarket官方累计 `trade_pnl`，返回账户、地址、指标、采样参数、读取时间及采样点。成功序列有5分钟有界缓存；空、错误和未知金额不补零。

官方曲线是钱包地址级盈亏，与系统成交/赎回账本的已实现盈亏范围不同，不能改标题后视为同一指标。账户列表查询不会预取任何曲线。

## 已知错账修正（`WALLET_PNL_CORRECTIONS_JSON`）

Polymarket 官方盈亏序列漏记过个别成交，导致它以为钱包还持有已经卖掉的份额（幽灵持仓），曲线会跟着这些市场的价格变动。可选环境变量 `WALLET_PNL_CORRECTIONS_JSON` 列出这些持仓，服务端在返回前扣掉，前端无感知。未配置时行为不变。

```json
[{"wallet_address":"0x…","token_id":"…","effective_at":"2026-08-07T09:51:10Z","phantom_shares":"51.13","missing_cash":"1.73842","note":"说明"}]
```

- 同一 token 可以配多条事件（如先卖出、后买回，`phantom_shares`/`missing_cash` 可为负），按各自 `effective_at` 叠加；钱包+token+生效时间不能重复。
- 对取值时刻不早于 `effective_at` 的点：`trade_pnl += missing_cash − phantom_shares × 该时刻价格`。`missing_cash` 是漏记成交的净现金（卖出收入减买入支出），`phantom_shares` 是官方多算的份额。
- 取值时刻用点的 `source_block` 出块时间，按固定锚点计算、与查询周期无关：区块号每 100 万（约 17 天）一段，已出完的段用两端锚点插值，当前段直接读真实出块时间；同一区块在任何周期里修正额相同。价格来自 `/v2/prices-history`：55 天内用 5 分钟桶、85 天内用 1 小时桶、更早用日桶，取不晚于取值时刻的最近一个桶。
- 历史价格分段永久缓存、并发拉取，最新分段缓存 5 分钟；出块时间读 `POLYGON_RPC_URL`，永久缓存。
- 读不到价格或出块时间时整个请求返回 502，不回退成未修正的官方值。仍受 8 秒预算约束；超时后已拉到的价格分段保留，下次请求复用。
- 只能修正仍未结算的幽灵持仓。已经结算的幽灵持仓在官方已实现盈亏里留下的固定偏差不在此修正范围内，「全部」等长周期的累计值仍可能偏离真实值。
- 配置格式错误时交易服务照常启动，但官方盈亏曲线整体停用（接口返回 503 未配置）并记错误日志 `wallet PnL disabled: invalid corrections`，不会显示未修正的数。
