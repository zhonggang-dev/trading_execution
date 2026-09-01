# Position Exit Job

> Runtime status: this service and endpoint contract are implemented but are not currently wired by
> `cmd/server`. Production OPEN-lot exits continue through the ten-minute `decisioncycle` strategy call.
> Do not schedule `/internal/jobs/position-exit-evaluation/run` until server wiring and an atomic cutover
> have disabled decision-cycle exits; running both paths would create duplicate exit decisions.

This is the independent ten-minute exit cycle. It freezes the point-in-time
prediction snapshot, every open position lot, current Market status, top-15
book and raw 48-hour Polymarket `p` history, then requires exactly one `HOLD`
or `SELL` answer per `lot_id`.

## XXL-Job trigger

```http
POST /internal/jobs/position-exit-evaluation/run
Authorization: Bearer <POSITION_EXIT_JOB_TOKEN>
Content-Type: application/json

{
  "xxl_log_id": 891234,
  "scheduled_at": "2026-08-18T12:00:00Z"
}
```

`scheduled_at` must be an exact ten-minute UTC boundary. It is the snapshot's
business time; receipt time is never substituted. Configure XXL-Job with
`0 0/10 * * * ?`, one active executor plus failover, and a timeout longer than
the Python request plus market-data capture. Duplicate delivery is safe.

The route is registered when `httpapi.Params.PositionExitJob` is injected.
`JobToken` is a dedicated secret separate from the public execution and
read-only tokens. Live configuration rejects a missing, short, or shared token;
internal job routes never fall back to the execution API token.

Production assembly uses the existing live adapters:

```go
exitJob, _ := positionexit.New(positionexit.Params{
    PredictionSource: predictionInfraClient, // PIT snapshot at decision_at
    TradeSource: fillLedger,             // PostgreSQL position_lots
    MarketUniverse: marketUniverseClient,// status + closed_at
    OrderBookSource: marketDataClient,   // top 15
    MidPriceSource: midPriceClient,      // cached/raw Polymarket 48h
    Strategy: positionExitPythonClient,
    Recorder: positionExitRecorder,      // PostgreSQL position_exit_runs
    Executor: executionService,
    Bindings: []domain.StrategyExecutionBinding{
        {
            PredictionModelID: "gemini-3.6-flash", // name in the PIT snapshot
            ModelID: "model-a",                    // stable Python/order identity
            StrategyID: "multfactor_v1",
            ExecutionAccountID: "wallet-model-a-v1",
        },
    },
    Venue: "polymarket",
})

httpapi.New(httpapi.Params{
    Service: executionService,
    PositionExitJob: exitJob,
    JobToken: os.Getenv("POSITION_EXIT_JOB_TOKEN"),
})
```

The current `cmd/server` has a fail-closed live execution composition, but it
does not yet construct or register this Position Exit job. Add this assembly
only to the live runtime and never wire it to the paper reservation ledger.

## Python endpoint

```http
POST /api/v2/position-exits/evaluate
Idempotency-Key: position-exit:wallet-model-a-v1:20260818T120000Z
X-Position-Exit-Input-ID: exit-input-...
X-Model-ID: model-a
X-Strategy-ID: multfactor_v1
X-Execution-Account-ID: wallet-model-a-v1
Authorization: Bearer <STRATEGY_TOKEN>
```

One request belongs to one `(model_id, strategy_id,
execution_account_id)`. Adding a model or strategy adds another binding and
wallet, without changing the protocol. A binding also carries the internal
`prediction_model_id` route: Position Exit selects PIT rows whose original
`model.name` equals that value, then projects their `model.name` to the stable
`model_id` before calling Python. The producer name therefore never leaks into
the strategy/order identity. When `prediction_model_id` is omitted it defaults
to `model_id` for backward-compatible configurations.

### Backtest parity invariants

| Field | Enforced meaning |
| --- | --- |
| binding `prediction_model_id` | Exact upstream `predictions[].model.name` used only for snapshot selection |
| request `context.model_id` | Stable logical model used by Python, positions, orders, and risk bindings |
| `predictions[].sandbox_id` | Must be empty in production; Sandbox rows are discarded before PIT selection and cannot replace a Direct result |
| `predictions[].prediction_as_of` | Producer forecast time; it must be within the configured lookback, and it, `completed_at`, and `available_at` must all be no later than `decision_at` |
| `orderbook.best_ask` | Exactly `asks[0].price`; never weighted or midpoint |
| `orderbook.bids/asks` | Real nearest levels, sorted, up to 15 per side; no synthetic padding |
| `mid_prices[].p` | Exact decimal value returned as Polymarket `/prices-history` `p` |
| `mid_prices[].interval_end_at` | The only point timestamp, normalized from upstream `t` with UTC `ceil('min')` |
| `market_status/closed_at` | Explicit settlement guard; non-OPEN markets cannot produce SELL |
| response `order.worst_price` | Owned by Python, required to equal frozen `best_bid`, forwarded unchanged by Go |

### Request

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
  "prediction_snapshot_id": "predsnap-20260818T120000Z",
  "prediction_scope": "ALL_EFFECTIVE_AT_DECISION_AT",
  "predictions": [
    {
      "prediction_id": "prediction-01",
      "source_job_id": "predict-job-01",
      "sandbox_id": "",
      "market_id": "market-01",
      "condition_id": "condition-01",
      "question": "Will the event occur?",
      "domains": ["example.com"],
      "neg_risk": false,
      "outcomes": [
        {"index": 0, "name": "YES", "token_id": "token-yes-01", "probability": 0.61},
        {"index": 1, "name": "NO", "token_id": "token-no-01", "probability": 0.39}
      ],
      "prediction_as_of": "2026-08-18T11:50:00Z",
      "completed_at": "2026-08-18T11:51:00Z",
      "available_at": "2026-08-18T11:51:01Z",
      "model": {"name": "model-a", "predictor_version": "2026-08-18"}
    }
  ],
  "trades": [
    {
      "lot_id": "lot-01",
      "venue_trade_id": "pm-trade-opening-01",
      "opening_order_id": "order-opening-01",
      "market_id": "market-01",
      "condition_id": "condition-01",
      "outcome_index": 0,
      "outcome_name": "YES",
      "token_id": "token-yes-01",
      "neg_risk": false,
      "entered_at": "2026-08-16T11:50:00Z",
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
        "bids": [
          {"price":"0.52","size":"100"}, {"price":"0.51","size":"110"},
          {"price":"0.50","size":"120"}, {"price":"0.49","size":"130"},
          {"price":"0.48","size":"140"}, {"price":"0.47","size":"150"},
          {"price":"0.46","size":"160"}, {"price":"0.45","size":"170"},
          {"price":"0.44","size":"180"}, {"price":"0.43","size":"190"},
          {"price":"0.42","size":"200"}, {"price":"0.41","size":"210"},
          {"price":"0.40","size":"220"}, {"price":"0.39","size":"230"},
          {"price":"0.38","size":"240"}
        ],
        "asks": [
          {"price":"0.53","size":"90"}, {"price":"0.54","size":"100"},
          {"price":"0.55","size":"110"}, {"price":"0.56","size":"120"},
          {"price":"0.57","size":"130"}, {"price":"0.58","size":"140"},
          {"price":"0.59","size":"150"}, {"price":"0.60","size":"160"},
          {"price":"0.61","size":"170"}, {"price":"0.62","size":"180"},
          {"price":"0.63","size":"190"}, {"price":"0.64","size":"200"},
          {"price":"0.65","size":"210"}, {"price":"0.66","size":"220"},
          {"price":"0.67","size":"230"}
        ]
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
          {"interval_end_at": "2026-08-16T12:01:00Z", "p": "0.50"}
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

`bids` is descending and `asks` is ascending. `best_bid == bids[0].price` and
`best_ask == asks[0].price`; neither is a weighted price nor a midpoint. Up to
the nearest 15 real levels are sent per side—if the venue has fewer levels, Go
does not fabricate liquidity. Mid-price `p` is copied byte-for-byte in decimal
meaning from Polymarket `/prices-history`; timestamps alone are normalized with
UTC `ceil('min')` and named `interval_end_at`. There is no resampling, midpoint
recalculation, interpolation, or gap fill.

`remaining_shares = available_shares + reserved_shares`. Python may sell no
more than `available_shares`. A fully reserved lot remains visible but cannot
produce another SELL.

### Response

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
        "reason": "lot reached the strategy holding period",
        "evidence": {
          "held_seconds": 173400,
          "best_bid": "0.52",
          "metrics": {"MOM": "0.014", "MACD_SIGNAL": "-0.003"}
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

Python must return one evaluation for every input lot:

- `HOLD` omits `order`.
- `SELL` is `SELL LIMIT FOK`, has no `expires_at`, uses `size <=
  available_shares`, and Python sets `worst_price` to the snapshot best bid
  exactly. Go validates and forwards that exact value; it never overwrites or
  computes `worst_price`.
- For a newly created Kalshi execution intent, Go preserves the exact
  `worst_price` and requested shares but maps the protocol FOK to venue IOC.
  The immediately available protected quantity may fill and Kalshi cancels the
  remainder. Polymarket and frozen legacy Kalshi FOK deliveries are unchanged.
- `held_seconds` equals `decision_at - entered_at` exactly.
- `best_bid` is mandatory for SELL and equals the snapshot best bid.
- HOLD reasons: `HOLD_NOT_DUE`, `HOLD_SIGNAL`, `LIQUIDITY_TOO_LOW`,
  `PRICE_OUT_OF_RANGE`, `STALE_DATA`, `INVALID_BOOK`, `MARKET_NOT_TRADABLE`.
- SELL reasons: `TIME_EXIT_48H`, `TAKE_PROFIT`, `STOP_LOSS`.

`market_status != OPEN` requires `HOLD/MARKET_NOT_TRADABLE` and Go rejects any
SELL before it reaches execution. `CLOSED/RESOLVED` lots stay in the ledger and
are handled by settlement/reconciliation; they are never converted into a
sell. An unusable book requires `HOLD/INVALID_BOOK`; a non-`OK` history requires
`HOLD/STALE_DATA`. Go does not impose a 48-hour strategy rule, reverse the
direction, resize an order, or modify Python's price.

## Persistence, reservation and retries

1. Read the all-effective PIT prediction snapshot; every prediction carries
   `prediction_as_of/completed_at/available_at <= decision_at`.
2. Read open `position_lots` and subtract active reservations by exact
   `target_lot_id`.
3. Resolve Market status/`closed_at`, then capture all unique token books and
   48-hour histories.
4. Persist the immutable request in `position_exit_runs`.
5. Call Python, validate every lot, then persist its immutable output.
6. Convert SELL to an `OrderIntent` with `target_lot_id` and a stable
   `client_order_id`.
7. Reuse normal execution: fresh market/BBO validation, hard risk, PostgreSQL
   lot reservation, signing, CLOB submission, state machine and Fill ledger.

Retries read the frozen input/output and do not call Python or market data
again. They may replay the same intent after a crash; the stable client order
ID makes this an order lookup rather than a second order. An `UNKNOWN` venue
result keeps its reservation and is reconciled, never resubmitted with a new
ID.

Apply `migrations/0005_position_exit_cycles.sql` after 0001–0004. It adds
`target_lot_id`, changes SELL uniqueness from token to lot, conservatively
blocks unresolved legacy token-level SELLs, and creates `position_exit_runs`.
The new shape check is `NOT VALID`, so it protects new writes without making
rollout fail on historical rows that first need reconciliation.
