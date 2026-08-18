# Trading Strategy HTTP API v4

Python 实现一个同步决策接口。Trading Execution 每 10 分钟按
`(model_id, strategy_id, execution_account_id)` 发起一次独立请求。Python 只能决定策略动作，
不能选择钱包、venue 或 execution account。

## Endpoint

```http
POST /api/v4/decisions HTTP/1.1
Authorization: Bearer ${STRATEGY_API_TOKEN}
Content-Type: application/json
Accept: application/json
Idempotency-Key: account-model-a-v1:20260818T042000Z
X-Strategy-Input-ID: strategy-input-012345...
X-Model-ID: model-a
X-Strategy-ID: multfactor_v1
X-Execution-Account-ID: account-model-a-v1
```

- `cycle_id` 是 execution account 加 UTC 10 分钟边界；
- `input_id` 是完整冻结输入的 SHA-256 身份；
- 相同 `cycle_id + input_id` 必须返回完全相同的响应；
- 同一 `cycle_id` 携带不同 `input_id` 必须返回 `409 IDEMPOTENCY_CONFLICT`；
- Header 和 JSON `context` 必须一致。

## Request

下面是完整字段形状。示例只缩短了重复的盘口档位和分钟点；生产请求不会截断：每侧最多
15 档，`mid_prices` 包含 PM 在 48 小时请求窗口内实际返回的全部原始点。

```json
{
  "schema_version": "trading.strategy_input.v4",
  "cycle_id": "account-model-a-v1:20260818T042000Z",
  "input_id": "strategy-input-012345...",
  "context": {
    "model_id": "model-a",
    "strategy_id": "multfactor_v1",
    "execution_account_id": "account-model-a-v1"
  },
  "decision_at": "2026-08-18T04:20:00Z",
  "generated_at": "2026-08-18T04:20:04Z",
  "prediction_snapshot_id": "predsnap-abcdef...",
  "prediction_scope": "ALL_EFFECTIVE_AT_DECISION_AT",
  "predictions": [
    {
      "prediction_id": "pred-model-a-001",
      "source_job_id": "pm-live:10:2001:pm-sandbox-live-v1",
      "sandbox_id": "pm-abc123",
      "market_id": "1122565",
      "condition_id": "0x05297f...",
      "event_id": "event-123",
      "question": "Will ...?",
      "event_slug": "event-slug",
      "market_slug": "market-slug",
      "domains": ["World/Geopolitics"],
      "end_at": "2026-12-31T00:00:00Z",
      "neg_risk": false,
      "outcomes": [
        {"index": 0, "name": "Yes", "token_id": "token-yes", "probability": 0.73},
        {"index": 1, "name": "No", "token_id": "token-no", "probability": 0.27}
      ],
      "prediction_as_of": "2026-08-18T04:00:00Z",
      "completed_at": "2026-08-18T04:10:00Z",
      "available_at": "2026-08-18T04:10:01Z",
      "model": {
        "name": "model-a",
        "predictor_version": "prediction-runner/1.0.0",
        "prompt_version": "pm-predict-v1"
      }
    }
  ],
  "positions": [
    {
      "lot_id": "lot:poly:trade-0007",
      "market_id": "1122565",
      "condition_id": "0x05297f...",
      "outcome_index": 0,
      "outcome_name": "Yes",
      "token_id": "token-yes",
      "neg_risk": false,
      "entered_at": "2026-08-16T03:50:12Z",
      "shares": "12.50",
      "entry_price": "0.40"
    }
  ],
  "orderbooks": [
    {
      "market_id": "1122565",
      "condition_id": "0x05297f...",
      "outcome_index": 0,
      "token_id": "token-yes",
      "status": "OK",
      "source_at": "2026-08-18T04:20:02Z",
      "observed_at": "2026-08-18T04:20:03Z",
      "tick_size": "0.01",
      "min_order_size": "1",
      "depth_limit": 15,
      "best_bid": "0.49",
      "best_ask": "0.50",
      "bids": [
        {"price": "0.49", "size": "120"},
        {"price": "0.48", "size": "80"},
        {"price": "0.47", "size": "210"}
      ],
      "asks": [
        {"price": "0.50", "size": "80"},
        {"price": "0.51", "size": "110"},
        {"price": "0.52", "size": "95"}
      ]
    },
    {
      "market_id": "1122565",
      "condition_id": "0x05297f...",
      "outcome_index": 1,
      "token_id": "token-no",
      "status": "OK",
      "source_at": "2026-08-18T04:20:02Z",
      "observed_at": "2026-08-18T04:20:03Z",
      "tick_size": "0.01",
      "min_order_size": "1",
      "depth_limit": 15,
      "best_bid": "0.48",
      "best_ask": "0.49",
      "bids": [{"price": "0.48", "size": "75"}],
      "asks": [{"price": "0.49", "size": "90"}]
    }
  ],
  "mid_price_histories": [
    {
      "market_id": "1122565",
      "condition_id": "0x05297f...",
      "outcome_index": 0,
      "token_id": "token-yes",
      "status": "OK",
      "window_start": "2026-08-16T04:20:00Z",
      "window_end": "2026-08-18T04:20:00Z",
      "fidelity_seconds": 60,
      "sampling": "UPSTREAM_RAW",
      "missing_value_policy": "NO_FILL",
      "timestamp_semantics": "INTERVAL_END_UTC",
      "fetched_at": "2026-08-18T04:20:04Z",
      "coverage_start": "2026-08-16T04:21:00Z",
      "coverage_end": "2026-08-18T04:20:00Z",
      "mid_prices": [
        {"interval_end_at": "2026-08-16T04:21:00Z", "p": "0.44"},
        {"interval_end_at": "2026-08-16T04:22:00Z", "p": "0.45"},
        {"interval_end_at": "2026-08-18T04:20:00Z", "p": "0.50"}
      ]
    },
    {
      "market_id": "1122565",
      "condition_id": "0x05297f...",
      "outcome_index": 1,
      "token_id": "token-no",
      "status": "OK",
      "window_start": "2026-08-16T04:20:00Z",
      "window_end": "2026-08-18T04:20:00Z",
      "fidelity_seconds": 60,
      "sampling": "UPSTREAM_RAW",
      "missing_value_policy": "NO_FILL",
      "timestamp_semantics": "INTERVAL_END_UTC",
      "fetched_at": "2026-08-18T04:20:04Z",
      "coverage_start": "2026-08-16T04:21:00Z",
      "coverage_end": "2026-08-18T04:20:00Z",
      "mid_prices": [
        {"interval_end_at": "2026-08-16T04:21:00Z", "p": "0.56"},
        {"interval_end_at": "2026-08-18T04:20:00Z", "p": "0.50"}
      ]
    }
  ],
  "execution_constraints": {
    "size_unit": "SHARES",
    "size_decimal_places": 2,
    "buy_notional_decimal_places": 2,
    "minimum_buy_notional": "1",
    "allowed_time_in_force": ["FOK"],
    "price_protection_policy": "EXACT_TOP_OF_BOOK"
  }
}
```

## Request semantics

- `prediction_scope=ALL_EFFECTIVE_AT_DECISION_AT`：每个 tick 都给该 model 当前全部有效预测，
  不是只给新增 prediction；某 token 没有出现在任何 prediction outcome 中，就表示该 tick
  没有该 model 的有效预测。
- `positions` 是当前 account、model、strategy 的全部 OPEN 批次，一手一行，不做 token
  净额合并。`entered_at` 是 UTC 绝对时间，`shares` 是该手剩余真实 shares。
- `market_id/condition_id/outcome/neg_risk` 是 Go 保存的执行身份，供无当前 prediction 的老仓位
  退出使用；Python 不需要推导或修改。
- `orderbooks` 覆盖 predictions 和 positions 中 token 的并集。`depth_limit` 固定为 15；
  bids 按价格降序、asks 按价格升序，每侧返回实际存在的前 15 档，少于 15 档时不补假档。
  `best_bid == bids[0].price`、`best_ask == asks[0].price`，两者都不是加权价或中点。
- `mid_price_histories` 覆盖相同 token 并请求 `[decision_at-48h, decision_at]`。
  `mid_prices[].p` 直接映射 PM `history[].p`，不计算 `(bid+ask)/2`。
- 分钟频、原始 `p`、不重采样、不 forward-fill；上游 `t` 只执行 UTC `ceil('min')`，并且
  只通过 `interval_end_at` 暴露，不再同时使用含义模糊的 `observed_at`。
- `OK` 要求序列末端新鲜且至少覆盖最近 2 小时；请求仍尽量提供完整 48 小时，所以断档恢复后
  Python 能直接重新 warm up。
- 所有价格、shares、edge 和 metrics 都是 JSON string decimal；probability 是 JSON number。

## Success response

`evaluations` 只负责逐 prediction outcome 的买入判断。Python 必须为每个
`(prediction_id, token_id)` 返回且只返回一条；`SKIP` 也是必须审计的结果。

`exits` 只放真正要卖出的批次。没有出现在 `exits` 中的 lot 表示继续持有；每个 SELL 必须
引用一个请求里的 `lot_id`。

```json
{
  "data": {
    "schema_version": "trading.strategy_output.v4",
    "cycle_id": "account-model-a-v1:20260818T042000Z",
    "input_id": "strategy-input-012345...",
    "context": {
      "model_id": "model-a",
      "strategy_id": "multfactor_v1",
      "execution_account_id": "account-model-a-v1"
    },
    "decided_at": "2026-08-18T04:20:05Z",
    "evaluations": [
      {
        "decision_id": "multfactor_v1:pred-model-a-001:token-yes:entry",
        "prediction_id": "pred-model-a-001",
        "market_id": "1122565",
        "condition_id": "0x05297f...",
        "outcome_index": 0,
        "token_id": "token-yes",
        "action": "SUBMIT",
        "reason_code": "ENTRY_SIGNAL",
        "evidence": {
          "probability": 0.73,
          "edge": "0.22",
          "metrics": {
            "best_ask": "0.50",
            "near_logdiff_usd": "0.418",
            "rel_spread": "0.0198",
            "MOM": "0.031",
            "MACD_SIGNAL": "0.008"
          }
        },
        "order": {
          "side": "BUY",
          "type": "LIMIT",
          "worst_price": "0.50",
          "size": "10.00",
          "time_in_force": "FOK"
        }
      },
      {
        "decision_id": "multfactor_v1:pred-model-a-001:token-no:skip",
        "prediction_id": "pred-model-a-001",
        "market_id": "1122565",
        "condition_id": "0x05297f...",
        "outcome_index": 1,
        "token_id": "token-no",
        "action": "SKIP",
        "reason_code": "EDGE_TOO_LOW",
        "reason": "edge did not pass the configured threshold",
        "evidence": {"probability": 0.27, "edge": "-0.22", "metrics": {"best_ask": "0.49"}}
      }
    ],
    "exits": [
      {
        "decision_id": "multfactor_v1:lot:poly:trade-0007:exit",
        "lot_id": "lot:poly:trade-0007",
        "token_id": "token-yes",
        "reason_code": "HOLD_48H",
        "reason": "lot reached its 48-hour holding period",
        "order": {
          "side": "SELL",
          "type": "LIMIT",
          "worst_price": "0.49",
          "size": "12.50",
          "time_in_force": "FOK"
        }
      }
    ]
  }
}
```

## Response validation

- `context/cycle_id/input_id` 必须原样回显；`decision_id` 在 evaluations 和 exits 的并集内唯一；
- evaluation `probability` 必须等于输入；SKIP 不得带 order；
- BUY entry 只能是 `LIMIT + FOK`，`worst_price` 必须严格等于输入 `best_ask`，不允许垫价；
- SELL exit 只能是 `LIMIT + FOK`，`worst_price` 必须严格等于输入 `best_bid`；
- SELL 必须引用 OPEN 且已满 48 小时的 lot，`size` 不得超过该手剩余 shares；正常情况卖完整手，
  若 venue 精度或最小量要求必须向下取整，剩余 dust 继续留在同一个 lot，Go 不会清零；
- `size` 的单位明确为 shares。Python 根据 v1 `$5`、v2 `$10` 和限价计算并向下取整；
  结果还必须同时满足 `execution_constraints` 和该 token 的 `min_order_size`；
- Go 不静默改方向、价格或 size。不满足 tick、最小金额或精度时整笔拒绝；
- SUBMIT evidence metrics 的封闭 key 是
  `best_ask / near_logdiff_usd / rel_spread / MOM / MACD_SIGNAL`；
- entry SUBMIT 必须提供全部五个 metrics，`best_ask` 必须与输入盘口相等；SKIP 可只提供能计算的子集；
- exits 不依赖当前 prediction，也不要求 48 小时价格因子，只依赖 lot 持有时间和当前盘口；
- 任何漏评、重复评估、未知 lot、上下文变化或非法枚举会拒绝整个响应，响应不会部分执行。

## Closed enums and status mapping

允许的 `reason_code`：

```text
ENTRY_SIGNAL
EDGE_TOO_LOW
SPREAD_TOO_WIDE
LIQUIDITY_TOO_LOW
PRICE_OUT_OF_RANGE
HOURLY_VETO
FACTOR_WARMUP
STALE_DATA
INVALID_BOOK
HOLD_48H
```

其中 `ENTRY_SIGNAL` 只用于 BUY SUBMIT，`HOLD_48H` 只用于 lot SELL。其余用于 SKIP。

| 输入字段 | status | Python 必须如何处理 |
| --- | --- | --- |
| orderbook | `OK` | 可以计算；仍需自行通过策略门槛 |
| orderbook | `EMPTY` / `MISSING` / `ERROR` | `SKIP + INVALID_BOOK` |
| mid-price history | `OK` | 可以计算 |
| mid-price history | `PARTIAL` / `EMPTY` / `MISSING` / `ERROR` | `SKIP + STALE_DATA` |

若 book 和 history 同时失败，优先返回 `INVALID_BOOK`。Go 会再次执行这张映射；错误状态下的
SUBMIT 会被整批拒绝。

## Strategy ID mapping

wire contract 只使用 canonical ID：

| 旧配置别名 | canonical `strategy_id` |
| --- | --- |
| `strategy-v1` / `mult-factor-v1` | `multfactor_v1` |
| `strategy-v2` / `mult-factor-v2` | `multfactor_v2` |

新增策略直接使用新的稳定 canonical ID；Go 的绑定表决定
`model_id + strategy_id -> execution_account_id`，不需要修改 HTTP schema。新增 model 也只新增
绑定和 account，不改变协议。

## Error response

```json
{
  "error": {
    "code": "INVALID_STRATEGY_INPUT",
    "message": "...",
    "retryable": false
  }
}
```

| HTTP | code | Trading Execution 行为 |
| --- | --- | --- |
| 400 | `INVALID_STRATEGY_INPUT` | 不重试，保留周期输入并告警 |
| 401/403 | `UNAUTHORIZED` | 不重试，配置告警 |
| 409 | `IDEMPOTENCY_CONFLICT` | 停止该 account 的周期并高优先级告警 |
| 422 | `UNSUPPORTED_SCHEMA` | 不重试，版本配置告警 |
| 429 | `RATE_LIMITED` | 在周期有效期内用完全相同输入有界退避重试 |
| 500/503/504 | 服务错误 | 用相同 cycle/input 有界重试 |

策略服务应在 30 秒内返回。重试不能重新读取市场数据后复用旧 `cycle_id`。
