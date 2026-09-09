# Shared orderbook snapshot storage

`trading_execution` owns the immutable orderbook evidence actually used by a
live decision cycle. Migration `0026_strategy_orderbook_snapshots.sql` adds two
ordinary PostgreSQL tables:

- `strategy_orderbook_snapshot_batches`: one sealed batch per `decision_at`;
- `strategy_orderbook_snapshots`: one normalized row per
  `(decision_at, market_source, token_id)`.

The cycle saves the prediction/position token union after `Capture` and
`alignBooks`, before any strategy call. Binding-specific requests are projected
from that same in-memory collection. The recorder never fetches or recomputes a
book, and `strategy_decision_runs.input_payload` remains the immutable evidence
for each individual binding.

For storage identity, a legacy empty Polymarket `market_source` is normalized
to `POLYMARKET`. The strategy request retains its existing omitted wire field,
so this database projection does not change the Strategy HTTP contract or its
input hash.

## Failure and replay

The header and every token row commit in one `READ COMMITTED` transaction. A
write failure rolls back the complete batch and ends the current cycle before
new strategy calls or new order intents. Recovery of older durable intents still
runs before the new market-data path. The next scheduled boundary is unaffected.

The first successful write for a `decision_at` wins. A retry with the same
prediction snapshot identity, target count, status summary, and canonical
snapshot-set hash reuses the stored batch without rewriting it. Any difference
returns an idempotency conflict. Database triggers reject later updates,
deletes, and truncation of either evidence table.

## Query shape

The detailed rows keep top-15 `bids` and `asks` as JSONB arrays. `best_bid`,
`best_ask`, `tick_size`, and `min_order_size` are separate numeric columns for
common analysis. The schema intentionally has no general-purpose JSONB GIN
index.

Token history:

```sql
SELECT decision_at, status, best_bid, best_ask, bids, asks
FROM strategy_orderbook_snapshots
WHERE token_id = $1
  AND decision_at >= $2
  AND decision_at < $3
ORDER BY decision_at DESC;
```

Condition history:

```sql
SELECT decision_at, token_id, outcome_index, status, best_bid, best_ask
FROM strategy_orderbook_snapshots
WHERE condition_id = $1
  AND decision_at >= $2
  AND decision_at < $3
ORDER BY decision_at DESC;
```

Batch completeness and status distribution:

```sql
SELECT decision_at, prediction_snapshot_id, target_count, snapshot_count,
       ok_count, empty_count, missing_count, error_count, created_at
FROM strategy_orderbook_snapshot_batches
WHERE decision_at >= $1
ORDER BY decision_at DESC;
```

## Capacity and rollout

A normal 300-market binary cycle is approximately 600 snapshot rows every 10
minutes, or 86,400 rows/day and 2.59 million rows per 30 days. The target union
can be smaller or larger, so neither application nor schema fixes the count at
600.

Apply migration `0026` before deploying the binary. PostgreSQL readiness checks
fail when either table, the critical constraints/indexes, or append-only
triggers are missing. Observe table/index size, row width, status distribution,
and decision-cycle duration before designing retention or partitioning. The
initial release does not delete, archive, backfill, or partition snapshots. A
binary rollback may leave the additive tables in place.
