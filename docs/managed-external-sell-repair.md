# Audited managed external SELL repair

This is an operator-approved accounting repair, not an order submission path or
an automatic external activity importer. It does not sign, cancel, buy, sell,
redeem, reset risk freshness, or mark a reconciliation run COMPLETED.

## Supported scope

- Confirmed Polygon CLOB V2 maker SELL events with an exact known order hash,
  transaction hash, token and already observed external-trade issue.
- At least 128 confirmations; actual pUSD transfers must exactly equal gross
  proceeds minus on-chain fees. A displayed trade price is not cash evidence.
- One sale per token per batch; at most 20 sales; one historical model/strategy
  owner per token; no unmanaged ownership baseline, open orders, reservations,
  pending fills or running reconciliation. The account must be explicitly paused.
- No later share-changing ledger event on the affected token; old sales cannot
  be retroactively attributed across intervening managed trades.
- The current chain cash and each remaining token balance must exactly match the
  ledger after all proposed deltas. Anything ambiguous is refused.

Keep the trading wallet dedicated to the service. Manual wallet activity remains
an anomaly and can pause trading until reviewed; this feature does not silently
accept third-party activity or make non-attributable cash adjustments.

## Allocation and audit

Remaining ERC1155 base units are allocated by each lot's share of the pre-repair
position, using largest remainder with a deterministic lot-id tie break. Cost
follows the same retained fraction, rounded to 30 decimal places; a fully closed
lot has exactly zero remaining cost. Net proceeds are proportional to closed
shares, with the final lot receiving the exact arithmetic remainder. No token
dust is deleted. Allocation, original state, complete verified evidence, operator
and reason are append-only, with deferred cross-table accounting constraints.

Cash/position events and an explicit `trading.accounting.external_sell.v1` outbox
event commit atomically. There are no fabricated `execution_orders` or fills.
`/ledger-activities` exposes `activity_type=EXTERNAL_SELL` without a local order
ID. Daily PnL and wallet accounting include the externally realized result;
`/trades` stays a compatibility view of local strategy fills only. Frontends
should label this additional activity type “External sell”, not a strategy order.

Reconciliation accepts only the exact accounted external identities. For known
managed dust of at most 0.01 shares omitted by the Data API, a fresh ERC1155 call
must exactly match the ledger; errors and even one-base-unit differences still
fail closed. The check never infers settlement or changes ledger balances.

## Operator procedure

1. Build from reviewed, merged `master`; execute migration `0024` before restarting
   the service. Back up the affected account, lots, positions, issues, environment
   and previous binary. Never commit production manifests or secrets to Git.
   Use the database migration-owner role, not the restricted application role.
   Grant the existing application role SELECT on the three new audit tables;
   leave repair-function EXECUTE revoked from that role and PUBLIC.
2. Prepare a private JSON manifest: `account`, `wallet`, `trades`. Each trade has
   `venue_trade_id`, `condition_id`, `matched_at` (UTC), and `request` containing
   `TransactionHash`, `OrderHash`, `TokenID`. Independently match trade identity
   and timestamp to venue activity before approval.
3. Set `POLYGON_RPC_URL`; run `externalsellrepair --manifest manifest.json` to read
   canonical receipts and a common-block asset snapshot. No private key needed.
4. Rehearse against a disposable database using `--database-dry-run` and
   `TRADING_EXECUTION_DATABASE_URL`. It exercises the entire transaction then
   rolls back, including its temporary versioned account pause. Evidence is
   re-read live for each invocation. Run private snapshot integration tests for
   production-specific data (see environment flags in the shadow test).
5. Pause the exact account with a versioned risk-control update, wait for running
   reconciliation and in-flight orders to finish, then stop the service cleanly.
   Do not resolve or delete an in-flight order to make this check pass.
6. Review the exact manifest SHA256. Execute `--apply --manifest manifest.json
   --confirm-sha256 HASH --actor OPERATOR --reason APPROVED_REASON`. The database
   function is revoked from PUBLIC; execute with the controlled migration-owner
   operator role, never expose it through HTTP. A repeat of the identical batch
   is a no-op. A changed batch cannot reuse an accounted chain event.
7. The current wallet-6/7 release refuses startup when an active account's
   **ACCOUNT** control is paused. After repair, atomically install a temporary
   **STRATEGY** pause for every enabled strategy of the affected account and
   restore the original ACCOUNT control (with increasing versions). This keeps
   both BUY and SELL blocked by Go and the database while allowing startup
   reconciliation. Preserve any pre-existing strategy pauses separately.
8. Start the merged binary with the original environment; run normal
   reconciliation and verify cash, lots, PnL, exact external identity accounting,
   and remaining dust. Only after a normal COMPLETED run, restore the previous
   strategy-control semantics with versioned updates. Do not clear unrelated
   operator pauses or force a test trade. Retain an inactive newly created
   maintenance-control row for audit rather than deleting its history.

Before commit any failure rolls back the complete batch. After commit, financial
audit rows are not deleted; any correction requires separately reviewed,
attributable compensating entries. The previous server binary is compatible with
the new tables, but cannot recognize accounted external sells, so rolling back
the binary requires leaving the affected account paused.
