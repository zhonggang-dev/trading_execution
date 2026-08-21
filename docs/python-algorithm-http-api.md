# Python 算法服务 HTTP 接口契约

本文档是 Trading Execution 与 Python 算法服务之间的统一接口契约，字段和校验规则以当前 Go
代码为准。Python 算法服务需要提供两个同步 HTTP 接口：

| 用途 | 方法与路径 | 请求版本 | 响应版本 |
| --- | --- | --- | --- |
| 预测入场决策 | `POST /api/v4/decisions` | `trading.strategy_input.v4` | `trading.strategy_output.v4` |
| 逐笔持仓退出决策 | `POST /api/v2/position-exits/evaluate` | `trading.position_exit_input.v2` | `trading.position_exit_output.v2` |

两个接口都按照 `(model_id, strategy_id, execution_account_id)` 隔离调用。同一个模型运行两个策略，
会收到两次独立请求；新增模型、策略或钱包只新增绑定，不改变接口结构。
Trading 可在内部用 `prediction_model_id` 将上游具体 producer 名投影为稳定的
`context.model_id`；该路由字段不进入 HTTP 协议。Python 仍只需要校验每条
`prediction.model.name == context.model_id`，不需要修改代码。

## 1. 通用 HTTP 规则

### 1.1 请求头

两个接口都包含：

```http
Content-Type: application/json
Accept: application/json
Authorization: Bearer <STRATEGY_API_TOKEN>
Idempotency-Key: <cycle_id>
X-Model-ID: <context.model_id>
X-Strategy-ID: <context.strategy_id>
X-Execution-Account-ID: <context.execution_account_id>
```

入场决策额外包含：

```http
X-Strategy-Input-ID: <input_id>
```

持仓退出额外包含：

```http
X-Position-Exit-Input-ID: <input_id>
```

Header 中的身份必须与 JSON `context` 一致。

### 1.2 幂等规则

- `cycle_id` 是钱包在一个10分钟UTC边界上的业务周期标识；
- `input_id` 是 Go 对冻结请求内容计算的 SHA-256 标识，Python 只能回显，不能重新生成或修改；
- 相同 `cycle_id + input_id` 重试时必须返回完全相同的业务响应；
- 同一 `cycle_id` 收到不同 `input_id` 时应返回 `409 IDEMPOTENCY_CONFLICT`；
- Python 不应在重试时重新抓取行情并改变已经形成的决策。

### 1.3 成功响应外层

请求 Body 直接是请求对象，没有 `data` 外层。成功响应必须使用下面的外层结构：

```json
{
  "data": {
    "schema_version": "..."
  }
}
```

Go 客户端要求 HTTP 状态严格为 `200`，响应最多 `8 MiB`，并拒绝未知字段和一个 Body 中的多个
JSON 对象。默认 HTTP 超时为30秒。

### 1.4 JSON 与 Python 类型映射

| 协议类型 | JSON 类型 | Python/Pydantic 建议类型 | 规则 |
| --- | --- | --- | --- |
| `Decimal` | `string` | `Decimal`，序列化为字符串 | 价格、shares、金额、edge、metrics 禁止使用 JSON 浮点数 |
| `Probability` | `number` | `float` | 范围 `[0,1]`，二元结果之和约等于1 |
| `UTCDateTime` | `string` | `datetime` | RFC3339 UTC，例如 `2026-08-18T12:00:00Z` |
| `Integer` | `number` | `int` | 不允许小数 |
| `Boolean` | `boolean` | `bool` | `true/false` |
| `StringMap` | `object` | `dict[str, str]` | value 必须是十进制字符串时会单独注明 |
| `T[]` | `array` | `list[T]` | 空集合也发送 `[]`，不要发送 `null` |

下面的类型定义使用接近 TypeScript 的 JSON 类型表达法：`?` 表示字段可以省略，`| null` 表示
字段必须存在但值可以为 `null`。

## 2. 两个接口共用的数据类型

```ts
type Decimal = string;
type UTCDateTime = string;

type ExecutionContext = {
  model_id: string;
  strategy_id: string;
  execution_account_id: string;
};

type PredictionOutcome = {
  index: 0 | 1;
  name: string;
  token_id: string;
  probability: number;
};

type PredictionModel = {
  name: string;
  predictor_version?: string;
  prompt_version?: string;
};

type Prediction = {
  prediction_id: string;
  source_job_id: string;
  sandbox_id: string;
  market_id: string;
  condition_id: string;
  event_id?: string;
  question: string;
  event_slug?: string;
  market_slug?: string;
  domains: string[];
  end_at?: UTCDateTime;
  neg_risk: boolean;
  outcomes: [PredictionOutcome, PredictionOutcome];
  prediction_as_of: UTCDateTime;
  completed_at: UTCDateTime;
  available_at: UTCDateTime;
  model: PredictionModel;
};

type PriceLevel = {
  price: Decimal;
  size: Decimal;
};

type OrderBookSnapshot = {
  market_id: string;
  condition_id: string;
  outcome_index: 0 | 1;
  token_id: string;
  status: "OK" | "EMPTY" | "MISSING" | "ERROR";
  source_at?: UTCDateTime;
  observed_at: UTCDateTime;
  tick_size?: Decimal;
  min_order_size?: Decimal;
  depth_limit: 15;
  best_bid?: Decimal;
  best_ask?: Decimal;
  bids: PriceLevel[];
  asks: PriceLevel[];
  error_code?: string;
};

type MidPricePoint = {
  interval_end_at: UTCDateTime;
  p: Decimal;
};

type MidPriceHistory = {
  market_id: string;
  condition_id: string;
  outcome_index: 0 | 1;
  token_id: string;
  status: "OK" | "PARTIAL" | "EMPTY" | "MISSING" | "ERROR";
  window_start: UTCDateTime;
  window_end: UTCDateTime;
  fidelity_seconds: 60;
  sampling: "UPSTREAM_RAW";
  missing_value_policy: "NO_FILL";
  timestamp_semantics: "INTERVAL_END_UTC";
  fetched_at: UTCDateTime;
  coverage_start?: UTCDateTime;
  coverage_end?: UTCDateTime;
  mid_prices: MidPricePoint[];
  error_code?: string;
};

type StrategyOrder = {
  side: "BUY" | "SELL";
  type: "LIMIT";
  worst_price: Decimal;
  size: Decimal;
  time_in_force: "FOK";
  expires_at?: UTCDateTime;
};
```

共用数据口径：

- `prediction_as_of` 是预测实际形成时间，且 `prediction_as_of/completed_at/available_at` 都不能晚于
  `decision_at`；
- `prediction_scope` 固定为 `ALL_EFFECTIVE_AT_DECISION_AT`，每个周期发送当前全部有效预测，不是只发新增；
- `orderbook.depth_limit` 固定为15，`bids` 按价格降序，`asks` 按价格升序；每侧最多15个真实档位，
  不足15档不补假数据；
- `best_bid == bids[0].price`，`best_ask == asks[0].price`，不是加权价，也不是买卖盘中点；
- `mid_prices[].p` 是 Polymarket `/prices-history` 返回的原始 `p`，Go 不计算 `(bid+ask)/2`；
- mid price 为分钟频原始数据，不重采样、不插值、不前向填充；时间戳统一表示为
  `interval_end_at`；
- `OK` 订单簿必须同时有 bids 和 asks；不可用的数据通过 `status + error_code` 表达，不能伪造行情。

## 3. 接口一：预测入场决策

### 3.1 路径

```http
POST /api/v4/decisions
```

Trading Execution 通常每10分钟为每个模型、策略、钱包绑定调用一次。当前生产装配同时通过本接口处理
预测入场和该 binding 的 OPEN lot 退出；策略必须按实际 `positions` 返回所需 `exits`。仓库中的独立
Position Exit endpoint/service 目前未在 `cmd/server` 注册，不能把它视为已启用的生产退出 runner。未来若
显式装配独立退出任务，才应让本接口返回 `exits: []`，并用切换门禁避免同一持仓双重卖出。

### 3.2 Request 类型

```ts
type StrategyPositionLot = {
  lot_id: string;
  market_id: string;
  condition_id: string;
  outcome_index: 0 | 1;
  outcome_name: string;
  token_id: string;
  neg_risk: boolean;
  entered_at: UTCDateTime;
  shares: Decimal;
  entry_price: Decimal;
};

type StrategyExecutionConstraints = {
  size_unit: "SHARES";
  size_decimal_places: 2;
  buy_notional_decimal_places: 4;
  minimum_buy_notional: "1";
  allowed_time_in_force: ["FOK"];
  price_protection_policy: "EXACT_TOP_OF_BOOK";
};

type StrategyDecisionRequest = {
  schema_version: "trading.strategy_input.v4";
  cycle_id: string;
  input_id: string;
  context: ExecutionContext;
  decision_at: UTCDateTime;
  generated_at: UTCDateTime;
  prediction_snapshot_id: string;
  prediction_scope: "ALL_EFFECTIVE_AT_DECISION_AT";
  predictions: Prediction[];
  positions: StrategyPositionLot[];
  orderbooks: OrderBookSnapshot[];
  mid_price_histories: MidPriceHistory[];
  execution_constraints: StrategyExecutionConstraints;
};
```

`positions` 是当前绑定下的全部开放持仓批次，一手一行，不按token合并。`shares` 是该手当前真实
剩余shares。`orderbooks` 和 `mid_price_histories` 覆盖预测与持仓token的并集。

### 3.3 Response 类型

```ts
type EntryReasonCode =
  | "ENTRY_SIGNAL"
  | "EDGE_TOO_LOW"
  | "SPREAD_TOO_WIDE"
  | "LIQUIDITY_TOO_LOW"
  | "PRICE_OUT_OF_RANGE"
  | "HOURLY_VETO"
  | "FACTOR_WARMUP"
  | "STALE_DATA"
  | "INVALID_BOOK"
  | "OUTSIDE_STRATEGY_UNIVERSE";

type StrategyEvidence = {
  probability: number;
  edge?: Decimal;
  metrics?: Record<string, Decimal>;
};

type StrategyEvaluation = {
  decision_id: string;
  prediction_id: string;
  market_id: string;
  condition_id: string;
  outcome_index: 0 | 1;
  token_id: string;
  action: "SKIP" | "SUBMIT";
  reason_code: EntryReasonCode;
  reason?: string;
  evidence: StrategyEvidence;
  order?: StrategyOrder;
};

type StrategyExit = {
  decision_id: string;
  lot_id: string;
  token_id: string;
  reason_code: "HOLD_48H";
  reason?: string;
  order: StrategyOrder;
};

type StrategyDecisionResponse = {
  schema_version: "trading.strategy_output.v4";
  cycle_id: string;
  input_id: string;
  context: ExecutionContext;
  decided_at: UTCDateTime;
  evaluations: StrategyEvaluation[];
  exits: StrategyExit[];
};

// 仅为解释 Trading 持久化输出；Python 响应不得发送该字段。
type GoOwnedEntryPolicy = {
  enabled: false;
  block_reason: "INCOMPLETE_MODEL_COVERAGE";
};

type StrategyDecisionSuccess = {
  data: StrategyDecisionResponse;
};
```

### 3.4 Response 规则

- 每个输入 `(prediction_id, token_id)` 必须且只能返回一条 `evaluation`；
- `cycle_id/input_id/context` 必须原样回显；所有 `decision_id` 在整个响应中唯一；
- `evidence.probability` 必须与输入对应 outcome 的 probability 完全一致；
- `SKIP` 不能带 `order`；
- `SUBMIT` 必须是 `BUY + LIMIT + FOK`，`reason_code` 必须为 `ENTRY_SIGNAL`；
- BUY `worst_price` 必须严格等于输入订单簿的 `best_ask`，不允许加价格垫；
- `size` 单位为 shares，输入最多 2 位小数；Trading 会在 BUY 下单前四舍五入为整数 shares，再校验不超过保护价的可见卖盘数量、`worst_price * size` 最多 4 位小数且不少于 1 美元；
- `multfactor_v1` SUBMIT 的 `evidence.metrics` 必须包含
  `best_ask/near_logdiff_usd/rel_spread`，MOM/MACD 可选；`multfactor_v2` 必须完整包含五项；
- `metrics.best_ask` 必须等于输入 `best_ask`，metrics value 全部为十进制字符串；
- 订单簿不可用必须 `SKIP + INVALID_BOOK`；只有 `multfactor_v2` 在 mid price 不可用时必须
  `SKIP + STALE_DATA`；策略范围外预测使用 `SKIP + OUTSIDE_STRATEGY_UNIVERSE`；
- 任何漏评、重复评价、非法字段都会导致整个响应被拒绝，不会部分执行。

`entry_policy` 不是 Python 响应契约的一部分。Trading 在 model task manifest 不完整时，会在 HTTP
响应通过身份/基础结构校验后由 Go 覆盖并追加
`entry_policy={"enabled":false,"block_reason":"INCOMPLETE_MODEL_COVERAGE"}`，然后把它作为
`trading.strategy_output.v4` 的 additive、Go-owned 审计字段持久化。Python 不应设置它；正常健康周期不持久化
该字段。阻断周期中的 SUBMIT evaluations 只供审计，不会形成 BUY OrderIntent，但同一响应中独立合法的
`exits` 仍会形成 SELL OrderIntent。该门禁独立于 Python 策略，所以 Python 仍应始终按正常 Response 规则返回。

### 3.5 最小示例

请求：

```json
{
  "schema_version": "trading.strategy_input.v4",
  "cycle_id": "wallet-model-a-v1:20260818T120000Z",
  "input_id": "strategy-input-<sha256>",
  "context": {
    "model_id": "model-a",
    "strategy_id": "multfactor_v1",
    "execution_account_id": "wallet-model-a-v1"
  },
  "decision_at": "2026-08-18T12:00:00Z",
  "generated_at": "2026-08-18T12:00:01Z",
  "prediction_snapshot_id": "pred-snapshot-01",
  "prediction_scope": "ALL_EFFECTIVE_AT_DECISION_AT",
  "predictions": [],
  "positions": [],
  "orderbooks": [],
  "mid_price_histories": [],
  "execution_constraints": {
    "size_unit": "SHARES",
    "size_decimal_places": 2,
    "buy_notional_decimal_places": 4,
    "minimum_buy_notional": "1",
    "allowed_time_in_force": ["FOK"],
    "price_protection_policy": "EXACT_TOP_OF_BOOK"
  }
}
```

响应：

```json
{
  "data": {
    "schema_version": "trading.strategy_output.v4",
    "cycle_id": "wallet-model-a-v1:20260818T120000Z",
    "input_id": "strategy-input-<sha256>",
    "context": {
      "model_id": "model-a",
      "strategy_id": "multfactor_v1",
      "execution_account_id": "wallet-model-a-v1"
    },
    "decided_at": "2026-08-18T12:00:02Z",
    "evaluations": [],
    "exits": []
  }
}
```

## 4. 接口二：逐笔持仓退出决策

### 4.1 路径

```http
POST /api/v2/position-exits/evaluate
```

该接口由10分钟持仓退出任务调用。每一条 `trade` 对应一个独立开仓批次，Python 必须逐手返回
`HOLD` 或 `SELL`，不能只返回token净仓位层面的决定。

### 4.2 Request 类型

```ts
type PositionExitTrade = {
  lot_id: string;
  venue_trade_id: string;
  opening_order_id: string;
  market_id: string;
  condition_id: string;
  outcome_index: 0 | 1;
  outcome_name: string;
  token_id: string;
  neg_risk: boolean;
  entered_at: UTCDateTime;
  original_shares: Decimal;
  remaining_shares: Decimal;
  available_shares: Decimal;
  reserved_shares: Decimal;
  entry_price: Decimal;
  remaining_cost: Decimal;
};

type PositionExitMarketStatus =
  | "OPEN"
  | "PAUSED"
  | "INACTIVE"
  | "NOT_ACCEPTING_ORDERS"
  | "CLOSED"
  | "RESOLVED";

type PositionExitMarketData = {
  market_id: string;
  condition_id: string;
  outcome_index: 0 | 1;
  token_id: string;
  market_status: PositionExitMarketStatus;
  closed_at: UTCDateTime | null;
  market_observed_at: UTCDateTime;
  orderbook: OrderBookSnapshot;
  mid_price_history: MidPriceHistory;
};

type PositionExitExecutionConstraints = {
  sell_size_unit: "SHARES";
  sell_size_decimal_places: 2;
  allowed_time_in_force: ["FOK"];
  price_protection_policy: "PYTHON_SUPPLIED_EXACT_BEST_BID";
};

type PositionExitRequest = {
  schema_version: "trading.position_exit_input.v2";
  cycle_id: string;
  input_id: string;
  decision_at: UTCDateTime;
  generated_at: UTCDateTime;
  context: ExecutionContext;
  prediction_snapshot_id: string;
  prediction_scope: "ALL_EFFECTIVE_AT_DECISION_AT";
  predictions: Prediction[];
  trades: PositionExitTrade[];
  market_data: PositionExitMarketData[];
  execution_constraints: PositionExitExecutionConstraints;
};
```

关键资金与持仓不变量：

```text
remaining_shares = available_shares + reserved_shares
SELL size <= available_shares
```

`market_data` 按token提供，一个token只有一份行情。`OPEN/PAUSED/INACTIVE/NOT_ACCEPTING_ORDERS`
的 `closed_at` 必须为 `null`；`CLOSED/RESOLVED` 必须提供真实 `closed_at`。

### 4.3 Response 类型

```ts
type PositionExitAction = "HOLD" | "SELL";

type PositionExitHoldReason =
  | "HOLD_NOT_DUE"
  | "HOLD_SIGNAL"
  | "LIQUIDITY_TOO_LOW"
  | "PRICE_OUT_OF_RANGE"
  | "STALE_DATA"
  | "INVALID_BOOK"
  | "MARKET_NOT_TRADABLE";

type PositionExitSellReason =
  | "TIME_EXIT_48H"
  | "TAKE_PROFIT"
  | "STOP_LOSS";

type PositionExitEvidence = {
  held_seconds: number;
  best_bid?: Decimal;
  metrics?: Record<string, Decimal>;
};

type PositionExitEvaluation = {
  decision_id: string;
  lot_id: string;
  action: PositionExitAction;
  reason_code: PositionExitHoldReason | PositionExitSellReason;
  reason?: string;
  evidence: PositionExitEvidence;
  order?: StrategyOrder;
};

type PositionExitResponse = {
  schema_version: "trading.position_exit_output.v2";
  cycle_id: string;
  input_id: string;
  context: ExecutionContext;
  decided_at: UTCDateTime;
  evaluations: PositionExitEvaluation[];
};

type PositionExitSuccess = {
  data: PositionExitResponse;
};
```

### 4.4 Response 规则

- 每个输入 `lot_id` 必须且只能返回一条 evaluation；
- `held_seconds = floor((decision_at - entered_at) / 1 second)`，必须精确一致；
- `HOLD` 不得带 `order`，只能使用 `PositionExitHoldReason`；
- `SELL` 只能使用 `TIME_EXIT_48H/TAKE_PROFIT/STOP_LOSS`，并且必须带 `order`；
- SELL order 必须是 `SELL + LIMIT + FOK`，不能设置 `expires_at`；
- `order.worst_price` 必须由 Python 填写，并严格等于冻结订单簿的 `best_bid`；Go不会覆盖价格；
- SELL evidence 必须提供相同的 `best_bid`；
- `order.size` 必须为正数、最多2位小数且不能超过该手 `available_shares`；
- `order.size` 还必须满足订单簿的 `min_order_size`；
- Market不是 `OPEN` 时必须返回 `HOLD + MARKET_NOT_TRADABLE`；
- 订单簿不可用时必须返回 `HOLD + INVALID_BOOK`；
- mid price不可用时必须返回 `HOLD + STALE_DATA`；
- CLOSED或RESOLVED持仓走结算/赎回，不允许通过SELL退出；
- Go只会拒绝不安全的SELL，不会反转方向、修改size或替换Python价格。

### 4.5 最小有效示例

请求中的15档盘口和48小时分钟点在此缩短展示；生产请求会传真实可用的全部数据。

```json
{
  "schema_version": "trading.position_exit_input.v2",
  "cycle_id": "position-exit:wallet-model-a-v1:20260818T120000Z",
  "input_id": "exit-input-<sha256>",
  "decision_at": "2026-08-18T12:00:00Z",
  "generated_at": "2026-08-18T12:00:01Z",
  "context": {
    "model_id": "model-a",
    "strategy_id": "multfactor_v1",
    "execution_account_id": "wallet-model-a-v1"
  },
  "prediction_snapshot_id": "pred-snapshot-01",
  "prediction_scope": "ALL_EFFECTIVE_AT_DECISION_AT",
  "predictions": [],
  "trades": [
    {
      "lot_id": "lot-01",
      "venue_trade_id": "pm-opening-trade-01",
      "opening_order_id": "opening-order-01",
      "market_id": "market-01",
      "condition_id": "condition-01",
      "outcome_index": 0,
      "outcome_name": "YES",
      "token_id": "token-yes-01",
      "neg_risk": false,
      "entered_at": "2026-08-16T12:00:00Z",
      "original_shares": "10.00",
      "remaining_shares": "7.25",
      "available_shares": "7.25",
      "reserved_shares": "0",
      "entry_price": "0.48",
      "remaining_cost": "3.48"
    }
  ],
  "market_data": [
    {
      "market_id": "market-01",
      "condition_id": "condition-01",
      "outcome_index": 0,
      "token_id": "token-yes-01",
      "market_status": "OPEN",
      "closed_at": null,
      "market_observed_at": "2026-08-18T12:00:00Z",
      "orderbook": {
        "market_id": "market-01",
        "condition_id": "condition-01",
        "outcome_index": 0,
        "token_id": "token-yes-01",
        "status": "OK",
        "source_at": "2026-08-18T11:59:59Z",
        "observed_at": "2026-08-18T12:00:00Z",
        "tick_size": "0.01",
        "min_order_size": "1",
        "depth_limit": 15,
        "best_bid": "0.52",
        "best_ask": "0.53",
        "bids": [{"price": "0.52", "size": "100"}],
        "asks": [{"price": "0.53", "size": "90"}]
      },
      "mid_price_history": {
        "market_id": "market-01",
        "condition_id": "condition-01",
        "outcome_index": 0,
        "token_id": "token-yes-01",
        "status": "OK",
        "window_start": "2026-08-16T12:00:00Z",
        "window_end": "2026-08-18T12:00:00Z",
        "fidelity_seconds": 60,
        "sampling": "UPSTREAM_RAW",
        "missing_value_policy": "NO_FILL",
        "timestamp_semantics": "INTERVAL_END_UTC",
        "fetched_at": "2026-08-18T12:00:00Z",
        "coverage_start": "2026-08-16T12:00:00Z",
        "coverage_end": "2026-08-18T12:00:00Z",
        "mid_prices": [
          {"interval_end_at": "2026-08-16T12:00:00Z", "p": "0.49"},
          {"interval_end_at": "2026-08-18T12:00:00Z", "p": "0.52"}
        ]
      }
    }
  ],
  "execution_constraints": {
    "sell_size_unit": "SHARES",
    "sell_size_decimal_places": 2,
    "allowed_time_in_force": ["FOK"],
    "price_protection_policy": "PYTHON_SUPPLIED_EXACT_BEST_BID"
  }
}
```

```json
{
  "data": {
    "schema_version": "trading.position_exit_output.v2",
    "cycle_id": "position-exit:wallet-model-a-v1:20260818T120000Z",
    "input_id": "exit-input-<sha256>",
    "context": {
      "model_id": "model-a",
      "strategy_id": "multfactor_v1",
      "execution_account_id": "wallet-model-a-v1"
    },
    "decided_at": "2026-08-18T12:00:02Z",
    "evaluations": [
      {
        "decision_id": "exit-lot-01-20260818T120000Z",
        "lot_id": "lot-01",
        "action": "SELL",
        "reason_code": "TIME_EXIT_48H",
        "reason": "position lot has been held for 48 hours",
        "evidence": {
          "held_seconds": 172800,
          "best_bid": "0.52",
          "metrics": {
            "MOM": "0.014",
            "MACD_SIGNAL": "-0.003"
          }
        },
        "order": {
          "side": "SELL",
          "type": "LIMIT",
          "worst_price": "0.52",
          "size": "7.25",
          "time_in_force": "FOK"
        }
      }
    ]
  }
}
```

## 5. 错误响应建议

非200响应不会进入业务响应解析。算法服务应返回稳定的JSON错误，方便日志和告警：

```ts
type AlgorithmErrorResponse = {
  error: {
    code: string;
    message: string;
    retryable: boolean;
  };
};
```

```json
{
  "error": {
    "code": "INVALID_STRATEGY_INPUT",
    "message": "input schema is invalid",
    "retryable": false
  }
}
```

建议状态码：

| HTTP | code | Go侧行为 |
| --- | --- | --- |
| `400` | `INVALID_STRATEGY_INPUT` | 不用新数据重算；记录错误并告警 |
| `401/403` | `UNAUTHORIZED` | 配置告警，不做业务重试 |
| `409` | `IDEMPOTENCY_CONFLICT` | 停止该周期并高优先级告警 |
| `422` | `UNSUPPORTED_SCHEMA` | 协议版本告警 |
| `429` | `RATE_LIMITED` | 只能使用相同 `cycle_id/input_id` 有界重试 |
| `500/503/504` | 服务错误 | 只能使用相同冻结输入有界重试 |

## 6. 算法侧实现检查清单

- 同时实现两个根路径，不要依赖额外URL前缀；
- 成功响应使用 `{ "data": ... }`，不能直接返回业务对象；
- 严格回显 `schema_version/cycle_id/input_id/context`；
- 所有金额、价格、shares、edge和metrics使用字符串小数；
- 入场接口逐prediction outcome返回评价，退出接口逐 `lot_id` 返回评价；
- 只允许 `LIMIT + FOK`，不返回GTC、IOC或MARKET；
- Python负责填写 `worst_price`，BUY等于冻结best ask，SELL等于冻结best bid；
- 算法做Edge、因子、止盈止损和持有期规则；Go做余额、预占、最大仓位、重复订单、Kill Switch、
  Market状态和最新价格漂移等硬风控；
- 同一幂等键重试必须返回同一决策，不得产生新的 `decision_id`。
