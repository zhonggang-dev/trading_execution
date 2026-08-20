# Go v4 contract compatibility

The adapter intentionally preserves the frozen strategy semantics. The Go v4
validator and scheduler implement the following contract requirements. They
remain documented here as release assertions that must stay covered by the Go
and Python test suites.

## 1. Auditable universe filtering

The closed reason code is:

```go
StrategyReasonOutsideUniverse StrategyReasonCode = "OUTSIDE_STRATEGY_UNIVERSE"
```

Accept it in `validSkipReason`. Add service tests proving that it is accepted
for `SKIP`, rejected for `SUBMIT`, and still loses to the mandatory
`INVALID_BOOK` status mapping when a book is not `OK`.

Why: the snapshot scope contains every effective prediction for a model, while
both multifactor strategies are restricted to `World/Geopolitics`. Omitting
those evaluations violates the all-outcome rule, and mapping them to an edge or
liquidity failure would falsify the audit record.

## 2. v1 must not acquire v2's hourly dependency

Entry status, evidence checks, and upstream data acquisition are strategy-specific:

- `multfactor_v1`: require `orderbook=OK`; do not require
  `mid_price_history=OK`; require SUBMIT metrics
  `best_ask/near_logdiff_usd/rel_spread`; permit, but do not require,
  `MOM/MACD_SIGNAL`. Trading Execution does not fetch history for v1-only
  tokens and supplies `MISSING/NOT_REQUIRED_FOR_MULTFACTOR_V1` placeholders.
- `multfactor_v2`: retain the current requirements: `orderbook=OK`,
  `mid_price_history=OK`, and all five metrics.

Concretely, pass `request.Context.StrategyID` into
`inputFailureReason`/`validateEvidenceMetrics`, and make the history lookup in
`buildEntryIntent` conditional on v2. Unknown strategy IDs remain fail-closed.

Add these Go tests:

1. v1 SUBMIT with an `ERROR` history and only the three common metrics is
   accepted.
2. the same response under v2 is rejected.
3. v2 with `OK` history but missing either hourly metric is rejected.
4. v1 still cannot submit when its book is not `OK`.
5. response evaluation count remains exactly `2 * predictions`, including
   outside-universe SKIPs.

Without this change, otherwise valid v1 signals become false negatives during
history outages or MACD warm-up, which changes the strategy rather than merely
hardening execution.
