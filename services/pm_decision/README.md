# pm-decision

`pm-decision` is the loopback-only Python implementation of
`POST /api/v4/decisions`. It receives one immutable decision snapshot from
Trading Execution, evaluates every `(prediction_id, token_id)`, and returns
strategy proposals. It never reads wallet files, selects an account, applies
wallet balances, calls Polymarket, or submits an order.

The ownership boundary is deliberate:

```text
prediction_infra snapshot
        -> Trading Execution captures books/positions/history
        -> pm-decision calculates strategy proposals
        -> Trading Execution persists, risk-checks, reserves, and may submit
```

Keep `DECISION_CYCLE_ORDER_SUBMISSION_ENABLED=false` during dry-run. A valid
`SUBMIT` response is only an intent; Go's global kill switch, account policy,
wallet balance, reservation rules, order maximums, and venue validation remain
authoritative.

## Frozen strategy semantics

Both strategies evaluate YES and NO independently, allow multi-lot stacking on
adjacent ticks, use no confirmation bars, and use no per-market debounce.

| strategy | universe | entry gates | hourly dependency |
| --- | --- | --- | --- |
| `multfactor_v1` | `World/Geopolitics` | edge `> 0.30`; near log-diff `>= 2.36`; relative spread `< 0.10`; ask in `[0.10,0.99)` | none |
| `multfactor_v2` | `World/Geopolitics` | edge `> 0.10`; near log-diff `>= 1.50`; relative spread `< 0.10`; ask in `[0.10,0.99)`; MOM and MACD signal both `<= 0` | raw 48-hour minute history; right-closed UTC hours; no fill; missing hour resets warm-up; factor age `<= 1h` |

The dz0p1 15-level order-book factor and TA-Lib-seeded MOM(12)/MACD(12,26,9)
are pure ports of the reviewed old implementation. The old whole entry gates
are not imported: that code used superseded v1 confirm-3/gap-30 and v2
edge-0.18 settings. The current fixed-threshold parity run and handoff spec use
the table above. [PARITY_AUDIT.md](PARITY_AUDIT.md) records the exact source
files and the execution behavior deliberately excluded from this service.

Target notional is explicit configuration, not a hidden default:

- `PM_DECISION_V1_TARGET_NOTIONAL`
- `PM_DECISION_V2_TARGET_NOTIONAL`

Both must be positive, non-exponent decimal strings or the service refuses to
start. The frozen reference values are 5 and 10 USD respectively, but they
must be aligned with Go's `EXECUTION_MAX_ORDER_NOTIONAL`, account policy, and
the approved canary amount. Python never overrides or silently clips Go risk.

Every BUY proposal is `LIMIT + FOK` at the exact input best ask. Size is in
shares, capped to displayed top-level size, rounded down to at most two share
decimals, and chosen with exact decimal arithmetic so `price * size` has at
most two decimals. If no positive size satisfies the token minimum and the
request constraints, the outcome is a `SKIP`. Lots held for at least 48 hours
produce a full-lot (precision-rounded) `LIMIT + FOK` SELL at the exact best bid.

## Contract precondition

Before enabling the scheduler, merge the Go validator changes specified in
[GO_CONTRACT_PREREQUISITES.md](GO_CONTRACT_PREREQUISITES.md). They add the
truthful `OUTSIDE_STRATEGY_UNIVERSE` SKIP and prevent v1 from accidentally
depending on v2 hourly data.

## Authentication and idempotency

The endpoint requires exactly one of every identity header plus a Bearer token
of at least 32 characters. Header identities must exactly match the body.
Unknown JSON fields, duplicate keys, non-finite numbers, non-string decimals,
wrong UTC boundaries, incomplete token coverage, and unsupported schemas fail
closed.

SQLite stores only the canonical request hash and the final response, not the
large strategy input (Go already stores the input in PostgreSQL). A
`BEGIN IMMEDIATE` transaction serializes a cycle through computation and
response persistence:

- same cycle/input/body: byte-identical response, including after restart;
- same cycle with changed input, or reused input ID: HTTP 409;
- database unavailable or locked beyond the bounded timeout: HTTP 503, with no
  unpersisted intent returned.

Run one Uvicorn worker. The database uses WAL, `synchronous=FULL`, a 30-second
busy timeout, file mode 0600, and a systemd-owned 0700 state directory.

## Local test

Python 3.11 or newer is required.

```bash
python3.12 -m venv .venv
.venv/bin/pip install -e '.[test]'
.venv/bin/pytest
```

The suite covers v1/v2 gates and hourly separation, all-outcome evaluation,
outside-universe auditing, exact Decimal sizing, 48-hour exits, strict
authentication/schema/header behavior, concurrent retry, cross-restart replay,
conflicts, and a static prohibition on wallet/venue/outbound-client imports.

## Production layout

Suggested paths:

```text
/opt/trading-execution/services/pm_decision/    application + .venv
/etc/pm-decision/pm-decision.env                root:root 0600, non-secret config
/etc/pm-decision/api-token                      root:root 0600, >=32 random chars
/var/lib/pm-decision/                           pm-decision:pm-decision 0700
```

Install the package into its own venv, copy
`deploy/pm-decision.env.example` to the root-only environment file, fill both
notionals explicitly, and install `deploy/pm-decision.service`. The unit binds
the application to `127.0.0.1:8787`, supplies the token as a systemd
credential, blocks non-loopback IP traffic in both directions, applies
`UMask=0077`, and grants write access only to its state directory.

Verify without exposing the token:

```bash
systemctl is-active pm-decision.service
curl --fail --silent http://127.0.0.1:8787/health
curl --fail --silent http://127.0.0.1:8787/ready
stat -c '%a %U:%G %n' /var/lib/pm-decision \
  /var/lib/pm-decision/idempotency.sqlite3
```

Expected permissions are 700 for the directory and 600 for the database.
Do not log or paste the Bearer token. Trading Execution must use the same token
through its own protected environment configuration.

## Dry-run release gate

1. Keep Go's global kill switch on and
   `DECISION_CYCLE_ORDER_SUBMISSION_ENABLED=false`.
2. Start `pm-decision`, then Trading Execution with the snapshot and strategy
   tokens configured separately.
3. Observe at least two exact UTC ten-minute cycles.
4. Confirm complete inputs and outputs exist in PostgreSQL, every prediction
   has two evaluations, replay is stable, and orders/reservations/fills remain
   unchanged.
5. Load-test the real maximum selected-market payload before enabling
   submission; a 300-market, two-token, 48-hour raw-history request is the
   production memory/latency case.
6. Only after all gates pass may an explicitly approved tiny canary be
   considered. Scheduled order submission remains a separate decision.
