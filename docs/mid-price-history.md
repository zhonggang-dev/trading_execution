# Polymarket 48 小时 Mid Price History

> 此适配器只保留给尚未在 `cmd/server` 装配的独立 position-exit 协议。入场 decision cycle
> 不再抓取或发送历史价格；multfactor_v2 由策略服务直取官方 prices-history。

Trading Execution 通过 Polymarket CLOB `POST /batch-prices-history` 获取每个 outcome token 在
`[decision_at-48h, decision_at]` 内的历史 midpoint。

PM 响应：

```json
{
  "history": {
    "token-yes": [
      {"t": 1787041630, "p": 0.135}
    ]
  }
}
```

Go 映射：

```text
t -> mid_prices[].observed_at（Unix 秒转 UTC）
p -> mid_prices[].mid_price（直接转 decimal string）
```

`p` 已经是 PM 在该时间点计算的 midpoint，不再执行 `(p + other_price) / 2`。当前 midpoint
的定义是 `(best_bid + best_ask) / 2`。

## 采集规则

- 请求参数 `market`/`markets` 使用 outcome `token_id`，不是 condition_id；
- 每批最多 20 个 token；默认最多 4 个 worker、全局 10 QPS、最多 3 次有界重试；
- 默认 `fidelity=1` 分钟、lookback=48 小时；
- 当前订单簿和历史 midpoint 并行采集；
- 相同 token 在一个周期只采一次，不因模型或策略数量增加而重复请求；
- 按 `token_id + t` 去重，重复时间戳保留最后一个 `p`；
- 丢弃窗口外点，按时间升序输出，不 forward fill；
- 不重采样、不补点，wire contract 明确标记 `UPSTREAM_RAW + NO_FILL + INTERVAL_END_UTC`；
- 请求尽量返回 48 小时；末端新鲜且原始覆盖至少 2 小时才标记 `OK`，否则标记 `PARTIAL`；
- 价格必须位于 `[0,1]`，并转换为 JSON string decimal；
- PM 超时、429 或 5xx 以 token 级 `ERROR` 进入策略输入，不阻塞其他 Market。

生产环境建议让 `MidPriceHistorySource` 读取提前滚动维护的共享缓存；当前 PM adapter 也可作为
冷启动/补洞数据源。这样以后替换为 Market Data Service 时，Python HTTP 协议不需要变化。
