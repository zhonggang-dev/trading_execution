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
    "total": 0,
    "has_more": false,
    "observed_at": "2026-10-16T08:00:00Z",
    "summary": {}
  }
}
```

`limit` defaults to 20 and is bounded to 1–100. `offset` is bounded to
0–9007199254740891. `total` is the exact number of rows matching the applied filters
and default time window. The count and page query run in one read-only
repeatable-read transaction, and `has_more` is derived from `total` and the
returned page. Empty and out-of-range pages still return the exact `total`.
`q`, identity filters, and status values are single
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
  `account_data_state` is `available` or `unavailable`; when available,
  `accounts[]` keeps configured binding identity beside nullable database
  binding/policy/control facts, latest durable cycle/reconciliation status,
  and bounded open issue/order counts. Database failure preserves runner and
  config fields while marking this account projection unavailable. The
  `entry_gate_state` values are `enabled`, `sell_only`, or `unknown`; a
  historical cycle without a Go-owned entry policy remains `unknown` even if
  the current process configuration has changed, and an account with no
  persisted cycle remains `unknown`. Current configuration switches stay in
  `submission` and are not folded into historical gate state.
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
  shares/price, filled shares/average fill price, the nullable
  order-owned `market_question` and `outcome_name` frozen in `intent`, and
  created/updated times. These labels are never looked up from current market
  data; missing historical fields remain JSON `null`.
  The default view covers recent orders and includes all non-terminal statuses
  (`UNKNOWN`, `RECONCILING`, and `MANUAL_REVIEW` remain visible when present).
  Filters are `q`, `model_id`, `strategy_id`, `execution_account_id`,
  `market_source`, `side`, `status`, `from`, `to`, `limit`, and `offset`.
  Each order also contains an `evidence_summary` computed from all durable
  fills, reservations, and open reconciliation issues for that order. It
  separates `none`/`observed`/`finality_pending`/`confirmed` fill evidence
  from `not_applied`/`partially_applied`/`applied` accounting and never uses
  the finite trace array length as a global count.
- `GET /api/v1/console/v2/orders/{order_id}/trace` returns one safe order and
  finite `deliveries`, `events`, `attempts`, confirmed/observed `fills`,
  related `position_lots`, and related reconciliation issues. Attempt error
  messages and raw request/venue payloads are omitted. Accepted order,
  confirmed fill, and applied fill remain separate fields/statuses.

## Lots, reconciliation, and redemptions

- `GET /api/v1/console/v2/holdings` returns the current positive holdings
  projection, grouped by account, historical model/strategy ownership, market,
  condition, token, and stored outcome identity. It includes `OPEN` and
  `SETTLED_PENDING_REDEEM` lots regardless of age and sums their remaining
  shares/cost with an exact `lot_count`. A group containing both lifecycle
  states is returned as `MIXED`; a status filter applies to this complete group,
  never to a partial lot sum. `market_source` and `market_question` are read
  only from an opening order matching the lot account, market, condition and
  token. The returned outcome identity remains the lot identity. `mark_price`, `market_value`, and
  `unrealized_pnl` are nullable and are populated only when the matching
  account/market/condition/token row in `execution_positions` has a mark; a
  missing mark is not returned as zero. This current projection does not accept
  `from` or `to`; use `position-lots` for an explicit opened-at history query.
  Supported filters are `q`, `model_id`, `strategy_id`,
  `execution_account_id`, `market_source`, `status`, `limit`, and `offset`.

- `GET /api/v1/console/v2/holdings/lots` returns the exact current lot members
  for one Holdings row. Pass `holding_identity` as a URL-encoded JSON array in
  the order `[execution_account_id, model_id, strategy_id, market_source,
  market_id, condition_id, token_id, outcome_index, outcome_name]`; source
  `null`/empty and outcome name `null`/empty use the same normalized grouping
  semantics as `holdings`, and `outcome_index` may be `null`. The endpoint only
  includes positive `OPEN` and `SETTLED_PENDING_REDEEM` lots, uses the same
  strict opening-order identity checks and exact count/page snapshot, and does
  not accept `q`, `from`, or `to`. It returns the existing `PositionLot` page
  shape and an unmatched identity returns `total: 0`.

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
  identities, safe local/remote numeric values, source, observed time,
  immutable first-observed time, and persisted impact scope. Open issues are
  always included regardless of age; resolved issues are bounded to the recent
  window. Raw `details` are never returned.
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

## Managed accounts, six-type ledger, and settlement progress

- `GET /api/v1/console/v2/accounts` accepts no query and returns
  `data.accounts[{execution_account_id,market_source}]` plus `observed_at`.
  Venue membership comes from the validated live composition, intersected with
  registered accounts. No wallet address or Kalshi API key identity is selected.
- `GET /api/v1/console/v2/ledger-activities` accepts `execution_account_id`
  (`account` alias), `activity_type`, `q`, `from`, `to`, `limit`, `offset`.
  Its standard Page envelope has the legacy ledger row field names, plus
  `market_source` and `quantity_unit` (`shares` or `pairs`). All amounts remain
  decimal strings. Both time boundaries are inclusive; without `from`/`to`, the
  ledger reads all history, matching the legacy ledger's All time control.
  Explicit `from` retains the common one-year bound. The five-second query
  budget still applies to all-history counts and summaries. `confirmed_at` and `applied_at` are
  nullable evidence fields.

  The six activity types are `BUY`, `SELL`, `EXTERNAL_SELL`, `REDEEM`,
  `KALSHI_NETTING`, `KALSHI_SETTLEMENT`. Netting has one row per durable
  `KALSHI_NETTED` account event, with an exact prefixed fill/order/account join.
  Quantity and payout are the account event's cash delta, one dollar per pair;
  both closure legs are checked against that payout and twice that quantity.
  An inconsistent event makes the bounded read unavailable rather than silently
  omitting cash. BUY retains its own debit and fee with zero realized PnL;
  netting alone carries the two-leg allocated cost and realized PnL. Its
  confirmation time and execution price are unavailable. Application time is
  the committed accounting event time.

  Applied Kalshi settlements have one row per stored lot allocation. Payout,
  cost and PnL come directly from that allocation, including zero payouts;
  cumulative exchange settlement fees are never charged again. Manual-review
  rows do not enter this cash ledger. The summary retains the original eleven
  fields and adds `netting_count`, `settlement_count`, `netting_payout`,
  `settlement_payout`, `netting_realized_pnl`, `settlement_realized_pnl`.
  Counts are numbers; amount summaries are decimal strings. The full filtered
  summary, exact total and page share one read-only repeatable-read snapshot.
- `GET /api/v1/console/v2/settlements` accepts `execution_account_id`,
  `market_source`, `status`, `q`, `from`, `to`, `limit`, `offset` (usual aliases).
  It returns a standard Page of `settlement_id`, `market_source`, account,
  `market_id`, `condition_id`, `status`, optional `transaction_hash`, nullable
  decimal-string `payout`, `created_at`, `updated_at`, nullable `settled_at`,
  `confirmed_at`, `applied_at`. PM retains its eight real redemption states and
  chain evidence, with payout converted from base units to dollars. Kalshi
  retains only `APPLIED` or `MANUAL_REVIEW`, uses its ticker as market identity,
  and has no chain confirmation timestamp or hash. A recorded exchange payout
  in review is evidence, not a credited payout. The default includes all
  unfinished rows plus recently created applied rows.
- `reconciliation-issues` also includes `KALSHI_BALANCE_DRIFT` and
  `KALSHI_SETTLEMENT_REVIEW`, with source-qualified stable IDs and `source` set
  to `KALSHI`. Their `run_id` is empty because no sweep audit exists. Balance
  differences retain durable first/last observations and resolution state;
  settlement review exposes the recorded local cost and exchange payout, not
  an invented balance difference. Raw review reasons and exchange JSON remain
  private. Existing run audits and order trace contracts remain unchanged.

The new resources use the same read-only permission, five-second deadline,
query validation and safe 400/503 error codes. Legacy `/api/v1/ledger-activities`
and `/api/v1/daily-pnl` retain their existing four-type protocols and metrics.

## Account performance

`GET /api/v1/console/v2/account-performance` accepts only an optional single,
non-empty `execution_account_id`. There are no aliases, dates, pagination or
model/strategy filters. Unknown, unregistered or unmanaged accounts return
400 `INVALID_CONSOLE_V2_FILTER`. Without the parameter, `items` contains the
same registered managed account set as `accounts`, sorted by account ID; no
cross-account return is calculated. Authentication, five-second budget and
safe 503 errors are unchanged.

The response is `{ "data": { "observed_at": "...", "items": [...] } }`.
Each item contains:

- `execution_account_id`, explicit `market_source` (`POLYMARKET` or `KALSHI`),
  `asset_count`, `priced_asset_count`;
- decimal strings `current_cost`, `peak_holding_cost`,
  `cumulative_invested_cost`, `realized_pnl`, `realized_drawdown_14d`;
- nullable decimal strings `market_value`, `unrealized_pnl`, `total_pnl`,
  `return_rate` (a ratio, not a percentage);
- `drawdown_from`, `drawdown_to` (UTC, today included, 14 calendar dates);
- `valuation_state` (`complete` or `incomplete`), `valuation_reasons`;
- nullable `snapshot_decision_at` and `snapshot_observed_at` (the earliest
  strictly matched quote observation for that account).

Registration, event accounting, all current portfolio assets and quotes are
read in one READ ONLY REPEATABLE READ transaction. Current assets reuse the
portfolio-holdings identity and provenance, including old strategies, real
adoptions and positive `SETTLED_PENDING_REDEEM` lots. Counts are asset groups,
not lots or shares. Current model/strategy bindings do not filter economics.

`peak_holding_cost` replays all historical `cost_basis_delta` values in
`occurred_at, position_event_id` order and takes the maximum running cost,
with a zero floor. `cumulative_invested_cost` adds positive BOUGHT, ADOPTED and
EXTERNAL_BUY_IMPORTED cost deltas. `realized_pnl` sums all
`realized_pnl_delta` values, including both Kalshi netting legs and the single
non-zero REDEEMED event from expiration settlement. Account cash payouts and
fees are not added/deducted again. This is system-managed ledger performance,
not venue-wide address PnL, net-deposit ROI or an annualized return.

The positive `realized_drawdown_14d` is the maximum high-to-low drop of the
cumulative daily realized events, starting at zero, grouped by
`occurred_at AT TIME ZONE 'UTC'`. It includes today's partial day through
`observed_at`, does not capture intraday peaks, and is independent of the
official chart's date range. Empty days do not change the cumulative curve.

Valuation uses only one latest sealed decision batch whose decision and
creation time do not exceed `observed_at`. Members must match source, market,
condition, token and outcome index. Only status OK with finite uncrossed
two-sided prices in [0,1] and `observed_at` between 15 minutes ago and the
response observation time supplies a midpoint; venue `source_at` alone is
not a freshness test. There are no venue requests, default-zero position
marks, cost-as-price fallbacks, older-batch fallbacks, new background jobs,
write paths or migrations.

Any missing/invalid/stale/future member makes the entire account's
`market_value`, `unrealized_pnl`, `total_pnl`, and `return_rate` null;
`priced_asset_count` counts only fully valid fresh members. Reason enums are
`missing_batch`, `missing_quote`, `invalid_quote`, `stale_quote`, and
`future_quote`. Costs, realized amounts and asset count remain independently
available. Zero assets legitimately produce zero value and unrealized PnL,
with total equal to realized. Return is total divided by peak holding cost
and stays null whenever that denominator is zero. A missing quote is never
reported as a zero-valued asset.

## Account core metrics and independent valuation

`GET /api/v1/console/v2/account-core-metrics` accepts only an optional single
`execution_account_id`. It uses the registered managed-account set, the existing
read-only credential, a five-second request budget, and the existing safe V2
400/503 errors. The older `account-performance` snapshot endpoint is unchanged.

The envelope is `data.observed_at/items`. Each item keeps `asset_count`,
`priced_asset_count`, `current_cost`, `peak_holding_cost`,
`cumulative_invested_cost`, `realized_pnl`, nullable `market_value`,
`unrealized_pnl`, `total_pnl`, `return_rate`, `realized_drawdown_14d`,
`drawdown_from/to`, and `valuation_state`. It adds `ledger_observed_at`, nullable
`quote_observed_from/to`, `has_reference_prices`, and the complete
`valuation_assets` list. Money and rates are decimal strings; `return_rate` is a
ratio, not a percentage. The local ledger is frozen in one read-only RR
transaction, then the connection is released before fetching market data.
Quotes have separate observation times, not a fictitious common atomic time.

Each valuation asset has the portfolio identity (`execution_account_id`,
`market_source`, `market_id`, `condition_id`, `token_id`, nullable `outcome_index`
and `outcome_name`), nullable `market_question`, exact `remaining_shares` and
`remaining_cost`, nullable `price/market_value`, `state=priced|unpriced`, nullable
`price_source/source_at/observed_at/reason`. Reasons are stable enums:
`missing_price`, `one_sided`, `invalid_price`, `identity_mismatch`, `stale`,
`timeout`, `source_unavailable`, `unconfirmed_settlement`.

Confirmed official settlement prices take precedence, including valid zero.
Otherwise Polymarket uses official `current_price` first, then a valid two-sided
CLOB midpoint. External quantities and average costs never replace local lots;
the price-specific Data API reader retains zero external quantities. Kalshi
uses the matching YES/NO midpoint with positive two-sided liquidity. NO bid/ask
liquidity derives from YES ask/bid liquidity. This is a reference valuation,
not a promise to match the website. Only `finalized`, a consistent binary result
and settlement value, and a valid settlement time establish Kalshi finality;
closed/determined/disputed/amended are not final settlement. PM redeemable 0/1
prices carry official-settlement provenance; a missing source timestamp remains
null rather than being fabricated.

Any unpriced asset makes all four account valuation-dependent fields null.
Local costs, realized income, and the UTC daily 14-day realized drawdown remain
available. Zero assets have zero market/unrealized value, total equal to
realized income, and a null rate when the peak-cost denominator is zero. The
asset limit is 2,000 and the response limit 8 MiB; exceeding a limit fails rather
than returning a truncated successful total.

Dedicated read clients globally limit active HTTP requests to two and individual
calls to two seconds. Prices use a 30-second success / three-second failure
cache, never cached quantities or account values. Identical requests coalesce;
cancelling one waiter does not cancel another. Completed prices are published
incrementally, so another platform or fallback timing out does not erase them.
There is no new background collection, order, balance synchronization, strategy
call, or database write. Secrets and funding addresses are never in this DTO.

## Official wallet PnL precision metadata

The existing `/api/v1/wallet-pnl` result adds nullable `source_fidelity` and each
point adds nullable `source_block` as an exact decimal integer string. The
upstream wallet/interval/fidelity identity is checked. A one-hour output grid
can still report daily original fidelity; neither that flag nor repeated source
blocks prohibit using the official returned hourly values, and neither proves
an independent valuation every hour. No values are interpolated by the backend.
The 10,000-point / 8-MiB source bounds remain. Shared fetches have independent
bounded lifetimes, so a cancelled leader does not poison another live waiter.
Older consumers can ignore the additive metadata.

Polymarket terminal prices also use public Gamma batched identifier lookups
(`condition_ids` repeated, at most 100, `closed=any`). A result is a final price
only when `closed=true`, `umaResolutionStatus=resolved`, market/condition IDs
match, two unique token/outcome mappings match the local outcome index, and the
two outcome prices are exactly one zero and one one. These final prices override
both `current_price` and CLOB midpoints, including assets no longer returned by
account positions. A closed/proposed/disputed market alone is insufficient.
Gamma does not provide an unambiguous resolution timestamp here, so `source_at`
remains null; observation time is the actual read time. The lookup runs in the
same bounded read-only price layer and failures do not erase successful current
prices from other reads. No ledger settlement or redemption is performed.
