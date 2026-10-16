# Trading Console V2 read API

This document describes the read-only projection implemented in this checkout.
The routes are registered by the main HTTP server and use the existing
`LIVE_OPERATIONS_READ_ONLY_TOKEN` bearer token. They do not accept the
execution API token as a read permission. When that existing read-only token
is intentionally empty, the service keeps the existing local-development
authentication behavior.

## Common rules

All list routes return:

```json
{
  "data": {
    "items": [],
    "limit": 20,
    "offset": 0,
    "has_more": false,
    "observed_at": "2026-10-16T08:00:00Z",
    "summary": {}
  }
}
```

`limit` defaults to 20 and is bounded to 1–100. `offset` is bounded to
0–10000. The reader fetches one extra row to compute `has_more`; it never runs
a full-table `COUNT`. `q`, identity filters, and status values are single
values no longer than 128 characters and reject NUL, CR, and LF. Optional
`from`/`to` values are RFC3339 and `from` must be within one year of the
server clock. The default list view is recent (30 days) plus current attention
states described below. A database or schema failure returns 503 instead of an
empty successful page.

For frontend filter compatibility on resources that expose the corresponding
long filter, `model`, `strategy`, `account`, and `source` are accepted short
aliases for `model_id`, `strategy_id`, `execution_account_id`, and
`market_source` respectively. Supplying both a short and long spelling for
one filter is a 400 error.

Every v2 HTTP route has a five-second context deadline. A shorter caller
deadline remains effective, and cancellation propagates through the Reader
to PostgreSQL. Legacy HTTP route timeouts are unchanged.

Only explicitly projected identity, state, amount, and timestamp fields are
returned. Strategy input/output JSON, venue payloads, signing material,
credentials, wallet addresses, reconciliation `details`, and internal error
text are excluded. Every request is a database read or an in-process runtime
measurement; no venue call, job trigger, consumer, scheduler, or write path
is invoked.

## Runtime and process metrics

- `GET /api/v1/console/v2/runtime` returns `observed_at`, the actual
  `DecisionRunner.Snapshot()` projection (`enabled`, `running`, `loop_started`,
  `in_flight`, schedule timestamps, `has_last_error`, a stable
  `last_error_code`, and skipped boundaries), the configured 10-minute interval,
  and the configured binding identities
  (`prediction_model_id`, logical `model_id`, `strategy_id`,
  `execution_account_id`, `source_mode`). Disabled runners report `state` as
  `disabled`; an enabled runner that has not started reports `not_started`.
  The legacy `live_runtime_status` cycle/monitor/prediction rows are not used
  to infer runner health.
  `last_error_code` is one of `CYCLE_FAILED`, `STARTUP_RECOVERY_FAILED`, or
  `BOUNDARY_SKIPPED` when `has_last_error` is true.
  Raw runner error text is retained in server logs and is never returned to the browser.
- `GET /api/v1/console/v2/service-metrics` returns real Go runtime and memory
  measurements, uptime, and `observed_at`. CPU, request rate, and container
  limits are unavailable in this process and are represented in
  `availability`; they are never fabricated as zero.

## Decision cycles

- `GET /api/v1/console/v2/decision-cycles` returns one persisted
  `strategy_decision_runs` binding row. Fields are `cycle_id`, `decision_at`,
  `model_id`, `prediction_model_id` (null because the persisted strategy input
  contains the Python-facing logical model projection and does not preserve
  the upstream producer identity), `strategy_id`, `execution_account_id`, `input_id`,
  `prediction_snapshot_id`, `orderbook_snapshot_set_id`, input/output times,
  historical `order_submission_enabled` when an output was recorded,
  `entry_submission_enabled` and `entry_block_reason` from the Go-owned entry
  gate (blocked outputs report `false`/a stable reason; outputs without a
  persisted gate report `null`),
  prediction/position counts, persisted delivery intent count, and delivery
  counts, and a durable status. A cycle that has only recorded input, or an
  output from before the Go-owned entry gate was persisted, returns
  `entry_submission_enabled: null` and `entry_block_reason: null`; input-only
  rows also return `order_submission_enabled: null`. A non-null
  block reason such as
  `INCOMPLETE_MODEL_COVERAGE` explains a SELL-only cycle without exposing raw
  strategy output:
  `INPUT_RECORDED`, `OUTPUT_RECORDED`, `DELIVERY_PENDING`, or
  `DELIVERY_ATTENTION`. The status is derived only from persisted input/output
  and delivery rows; it never claims a missing row was running or failed.
- Supported filters are `q`, `model_id`, `strategy_id`,
  `execution_account_id`, `status`, `from`, `to`, `limit`, and `offset`.
- `GET /api/v1/console/v2/decision-cycles/{cycle_id}` returns the same safe
  summary plus finite arrays of persisted prediction identities
  (`prediction_id`, `source_job_id`, optional `sandbox_id`, market/condition/
  token identity), orderbook snapshot identities/statuses, and delivery
  summaries. Probabilities, orderbook levels, and strategy payloads are not
  returned. A missing cycle is 404.

## Orders and trace

- `GET /api/v1/console/v2/orders` reads `execution_orders`. It returns
  `order_id`, client/venue order identity, lifecycle status, account and
  binding identity, venue/market/condition/token identity, side, requested
  shares/price, filled shares/average fill price, and created/updated times.
  The default view covers recent orders and includes all non-terminal statuses
  (`UNKNOWN`, `RECONCILING`, and `MANUAL_REVIEW` remain visible when present).
  Filters are `q`, `model_id`, `strategy_id`, `execution_account_id`,
  `market_source`, `side`, `status`, `from`, `to`, `limit`, and `offset`.
- `GET /api/v1/console/v2/orders/{order_id}/trace` returns one safe order and
  finite `deliveries`, `events`, `attempts`, confirmed/observed `fills`,
  related `position_lots`, and related reconciliation issues. Attempt error
  messages and raw request/venue payloads are omitted. Accepted order,
  confirmed fill, and applied fill remain separate fields/statuses.

## Lots, reconciliation, and redemptions

- `GET /api/v1/console/v2/position-lots` reads the durable lot identity and
  ownership fields: `lot_id`, model/strategy/account, market identity,
  opening order/fill, status, original/remaining shares and cost,
  average entry price, and open/close times. The default includes recent lots
  plus `OPEN` and `SETTLED_PENDING_REDEEM` lots. Filters are `q`, `model_id`,
  `strategy_id`, `execution_account_id`, `market_source`, `status`, `from`,
  `to`, `limit`, and `offset`.
- `GET /api/v1/console/v2/reconciliation-runs` returns run/account/trigger/
  focus order/status/timestamps. The default includes recent runs plus
  `RUNNING` and `ATTENTION_REQUIRED`. It does not return the raw summary JSON.
- `GET /api/v1/console/v2/reconciliation-issues` returns issue/run/account,
  issue and a nullable reason code (the current table has no dedicated reason
  code column), resolution/status, stable order/venue/market
  identities, safe local/remote numeric values, source, and observed/resolved
  times. Open issues are always included regardless of age; resolved issues
  are bounded to the recent window. Raw `details` are never returned.
- `GET /api/v1/console/v2/redemptions` returns the natural composite
  `redemption_id` (`execution_account_id:condition_id`), account/condition,
  status, transaction hash, payout base units, receipt block, confirmations,
  timestamps, and confirmation/application times. Unconfirmed or unapplied
  rows are not represented as credited. Pending statuses are always included;
  pending and `MANUAL_REVIEW` rows are always included; completed `APPLIED`
  rows are bounded to the recent window.

Reconciliation list filters are `q`, `execution_account_id`, `status`,
`from`, `to`, `limit`, and `offset`. Redemption list filters are the same.

## Errors and compatibility

- 400 `INVALID_CONSOLE_V2_FILTER` means an unsupported/repeated parameter,
  invalid bound, invalid status/side, or invalid RFC3339 window.
- 404 `CONSOLE_V2_NOT_FOUND` means a requested cycle or order identity is not
  present.
- 401/403 use the existing read-only bearer-token permission responses.
- 503 `CONSOLE_V2_UNAVAILABLE` means the database/read model is unconfigured,
  unavailable, or failed a bounded query. The response does not expose SQL or
  internal error text.
- POST/PUT/PATCH/DELETE are not registered under this namespace and are
  rejected by the HTTP mux. Existing `/api/v1/*` routes and their response
  contracts remain unchanged.

No migration was added for this API. It reads the existing schema through
`strategy_decision_runs`, `strategy_order_intent_deliveries`, execution order
and fill tables, position lots, reconciliation tables, and
`polymarket_redemptions`.
