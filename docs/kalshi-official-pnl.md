# Kalshi 官方账户盈亏只读接口

该曲线直接读取 Kalshi 官网使用的账户累计盈亏序列，与本地纳管账本、开仓区间和未扣手续费指标独立；不自行重建成交、费用或历史估值，不改变交易、结算或对账行为。

## 配置

在对应 `KALSHI_LIVE_ACCOUNTS_JSON` 账户行中增加可选 `official_user_id`：

```json
[{"execution_account_id":"kalshi:main","api_key_id":"<existing-key-id>","private_key_path":"/secure/kalshi.pem","official_user_id":"11111111-2222-3333-4444-555555555555"}]
```

示例 UUID 必须替换为该 API Key 所属的真实 Kalshi 用户 ID；不能填写 API Key ID、执行账户 ID 或从这些值推断。账户集合只包含已成功装配的 Kalshi 账户与配置官方身份的交集。省略该字段时账户交易配置保持兼容，只不提供该账户的官方曲线。此变更不自动修改线上配置。

官方 host 固定为 `https://api.elections.kalshi.com`，仅签名 GET `/v1/users/{official_user_id}/portfolio/pnl_history?range=...&subaccount_number=0`；使用独立 HTTP client、拒绝重定向，不接受浏览器传入官方用户 ID、地址、URL 或子账户编号。

## 内部 HTTP

- GET `/api/v1/kalshi-pnl/accounts` 无 query：`data.accounts[{execution_account_id,market_source:"KALSHI"}],observed_at`。
- GET `/api/v1/kalshi-pnl?execution_account_id=kalshi%3Amain&range=1w`。
- 仅接受 `1d/1w/1m/1y/all`；不透传自定义起止或精度参数。
- 复用 live read-only token，执行 token 无权访问。全部响应 `Cache-Control:no-store`。

序列 `data`：`execution_account_id,market_source:"KALSHI",metric:"cumulative_pnl",range,period_interval_minutes,fetched_at,processed_through,points[{timestamp,cumulative_pnl,portfolio_value}]`。timestamp 为 Unix 秒；金额为精确十进制字符串；points 合法空数组保持空，不能当零收益。

上游实测 `1d` 主要10分钟、`1w/1m` 主要小时、`1y/all` 主要每日。末尾可能附带接近请求时刻的实时点；`processed_through` 是历史处理进度，与末点、读取时刻分别保留。前端应依据实际精度判断自定义边界；年初至今可截取1y，旧日期不足一天不能用日线伪造小时收益。

成功缓存60秒、最多128项、同账户/周期合并、全局最多2个外部读取；8秒总预算、5秒 HTTP timeout。取消最后一个 waiter 时取消上游，单 waiter 取消不影响其他读取者。失败不缓存为空值，响应不含官方用户 ID、密钥、上游 body 或错误原文。

非法/未知/重复参数400 `INVALID_KALSHI_PNL_FILTER`；未知/未启用账户404 `KALSHI_PNL_ACCOUNT_NOT_FOUND`；未装配503；上游失败502 `KALSHI_PNL_UNAVAILABLE`；超时504 `KALSHI_PNL_TIMEOUT`；取消503 `KALSHI_PNL_CANCELED`。
