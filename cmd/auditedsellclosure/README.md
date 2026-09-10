# Audited partial SELL closure

Operator-only maintenance for the single order pinned in `main.go`. This command
is not part of the HTTP server, cron, order coordinator, or automatic recovery.
It never places an order or sends a cancellation request; its CLOB transport
permits only authenticated GET requests. RPC is limited to the existing receipt
reader, without transaction signing.

Default execution is PostgreSQL read-only. The explicit
`--apply-authorized-partial-sell-closure` flag is for an operator who has obtained
authorization to finalize this exact cancelled order and release its remainder.
It must run with the service's existing protected environment; never place
credentials in command arguments, source, or logs.

Before release the command requires:

- The exact persisted account, order, wallet, market, token, lot and SELL amounts.
- One already-applied, finalized fill with its exact transaction and receipt log,
  plus exactly one matching cash event. Equal aggregate balances are insufficient.
- Repeated fresh official cancellation reads spanning the configured 30-second
  finality grace, with a complete matching fill enumeration and receipt checks.
- An exclusive order-recovery lease and a successful duplicate replay through
  the existing FillProcessor/FillLedger (zero additional applications).

It records an explicit OPERATOR event with a deterministic evidence fingerprint,
then uses the existing ReservationManager for the atomic reservation/position
update. A failure between those steps leaves the remainder frozen. A retry only
accepts the original reviewed state or this exact audited cancellation state;
unrelated state changes require investigation. A completed replay is a no-op.
Ordinary MANUAL_REVIEW transitions and trading/risk gates are not relaxed.

Validation: `go test -race ./internal/adapter/postgres -run
TestAuditedPartialSellClosurePostgresIntegration` with an isolated
`TRADING_EXECUTION_TEST_DATABASE_URL`. Tests cover exact partial release, duplicate
execution, dry run, changed fills, non-cancelled orders, insufficient finality,
wrong wallet/cash evidence, competing leases, revision races, and recovery after
an interrupted release.

`fill_evidence.go` is copied unchanged from the verified `3025049` production
composition. This is a bounded historical maintenance tool, not a generalized
replacement for future manual-order workflows.
