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
