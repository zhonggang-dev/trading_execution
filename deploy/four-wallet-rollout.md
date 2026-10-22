# Four-wallet decision-cycle rollout

The four routes are one configuration unit: one upstream prediction model
(`deepseek_masked`, Sandbox source), four strategies and four distinct execution
accounts. wallet-6/wallet-7 are fixed to `multfactor_v1/v2`; main/wallet-1 take
two configurable, distinct strategy IDs (`strategy_pending_a/b` until the formal
names exist).
Do not reuse the legacy wallet-2 canary deployment or activation scripts for
this topology; those scripts intentionally contain a one-binding subset.

Use [`four-wallet-decision-cycle.env.example`](four-wallet-decision-cycle.env.example)
as the non-secret fragment. Replace the prediction model with the exact
`model.name` returned by the current Prediction PIT snapshot, install a wallet
file containing exactly `main`, `wallet-1`, `wallet-6`, and `wallet-7`, and keep
both durable gates closed (`ENTRY_DISABLED` may be any subset of
`["main","wallet-1"]`; start with both listed, then shrink it after the new
strategies have been observed, re-running the preflight each time):

```text
DECISION_CYCLE_ORDER_SUBMISSION_ENABLED=false
DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED=false
DECISION_CYCLE_ENTRY_DISABLED_ACCOUNTS_JSON=["main","wallet-1"]
DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON=[]
DECISION_CYCLE_STRATEGY_DISABLED_ACCOUNTS_JSON=[]
DECISION_CYCLE_KALSHI_STRATEGY_INPUT_ENABLED=true
DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE=true
POSITION_EXIT_JOB_TOKEN=DEDICATED_32_BYTE_OR_LONGER_SECRET
execution_risk_global_control.kill_switch=true
```

For the wallet-6/wallet-7 route, `deepseek_masked` is the final logical trading
identity. The upstream `model.name` is `v4.1 flash` and must be confirmed in a
fresh Sandbox prediction snapshot before enabling strategy requests. Previous
Gemini rollout evidence does not approve this new route. Keep order submission
disabled while preparing and validating the new binding.
The new binary can start while the decision cycle and order submission remain
disabled. OPEN lots retain their opening `model_id`; a new model does not take
ownership of an old model's lots.

The same reviewed environment must explicitly pin execution-format safety and
the v2 history window; monetary allocation belongs to the upstream AI strategy:

```text
EXECUTION_ALLOW_MARKET_ORDERS=false
POLYMARKET_MAX_BUY_FEE_RATE_BPS=EXACT_REVIEWED_NON_NEGATIVE_DECIMAL
POLYGON_ORDER_FILLED_CONFIRMATIONS=EXACT_REVIEWED_POSITIVE_INTEGER
DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON={"EXACT_SNAPSHOT_MODEL_NAME":"SANDBOX"}
```

The preflight binds these values into the configuration SHA and verifies the
running process has the same values. The account entry list is also hashed and
must be exactly `main,wallet-1`. The Go runtime also enforces the
source-mode map on every cycle: `DIRECT` accepts only an empty `sandbox_id`,
while `SANDBOX` requires a non-empty `sandbox_id`. The map must exactly cover
all configured upstream models; the current release pins `deepseek_masked` to
`SANDBOX`. Market orders must remain disabled for the
four-wallet activation; changing any other value invalidates disabled-pass
evidence and requires a new review.

This release requires the process-wide entry gate to remain `false`. The exact
account gate makes main/wallet-1 sell-only: they keep position-bearing strategy
requests and may submit SELL exits, but receive no entry predictions.
wallet-6/wallet-7 receive the reviewed Sandbox predictions. The execution
service enforces main/wallet-1 BUY rejection for direct HTTP Submit, reserved
Resume, decision delivery, and crash recovery.

The `DECISION_CYCLE_BINDINGS_JSON` array must be replaced atomically as one
environment-file value. The routes are `deepseek_masked/<main strategy> -> main`,
`deepseek_masked/<wallet-1 strategy> -> wallet-1`,
`deepseek_masked/multfactor_v1 -> wallet-6`, and
`deepseek_masked/multfactor_v2 -> wallet-7`. Binding the formal strategy names
later is a configuration change only (no code change). Do it while
`ENTRY_DISABLED` still lists the account, because lots are owned by the exact
`model_id + strategy_id`:

1. Edit the main/wallet-1 `strategy_id` values in `DECISION_CYCLE_BINDINGS_JSON`.
2. In `/etc/trading-execution/kalshi-live.env`, change the `main` entry of
   `KALSHI_LIVE_BINDINGS_JSON` to the same `model_id`/`strategy_id` (only those
   two fields; leave the key id and key path alone). Startup fails with
   `Kalshi live route is not an exact decision binding` otherwise.
3. Run [`rebind_main_wallet1.sql`](rebind_main_wallet1.sql) with the same two
   names (`-v main_strategy=... -v wallet1_strategy=...`).
4. Restart the service. Make sure the release directory is readable by the
   `trading-execution` user (mode 755), otherwise systemd fails with
   `status=200/CHDIR`.
5. Enable in two steps: first remove the account from
   `DECISION_CYCLE_STRATEGY_DISABLED_ACCOUNTS_JSON` (strategy is called, BUY is
   still blocked by `ENTRY_DISABLED`), check a cycle, then remove it from
   `DECISION_CYCLE_ENTRY_DISABLED_ACCOUNTS_JSON`. The global
   `DECISION_CYCLE_ORDER_SUBMISSION_ENABLED` stays `true` and is not changed.

Current state: main is bound to `brier_mid_edge_once` (strategy_version `1.0.0`,
display name "Brier 概率差单次建仓策略"), with main removed from both
`STRATEGY_DISABLED` and `ENTRY_DISABLED`; wallet-1 stays on `strategy_pending_b`
and is still listed in both. The algorithm service must accept the new ID on
`/api/v4/decisions` and `/api/v2/position-exits/evaluate`; a SUBMIT response
must carry `evidence.metrics` `best_ask`, `near_logdiff_usd` and `rel_spread`
(only `multfactor_v2` additionally needs `MOM`/`MACD_SIGNAL`).

Out-of-band exits and redemptions change the chain balance and holdings; record
the cash with a ledger rebase before the next reconciliation (an unexplained
difference is an account-wide `BALANCE_DRIFT` that blocks every order for the
account). The database must contain the
same four `(model_id, strategy_id, execution_account_id)` authorization rows.
All four authorization rows are enabled in shadow. main/wallet-1 remain active
so their existing OPEN lots can be reconciled and exited; new BUY entries are
blocked by the account gate. Source
`prediction_model_id` values never belong in the database authorization table.
Strategy and position-exit readers select OPEN lots by the exact
`model_id + strategy_id + execution_account_id` binding. Old-model lots remain
in the ledger for settlement or redemption and never transfer ownership to the
new model. Sub-centish Polymarket dust is retained in the ledger but omitted
from strategy requests because SELL sizes support at most two decimals.

Apply `migrations/0017_enabled_strategy_binding_uniqueness.sql` while the
service is stopped and the global kill switch is on. The migration replaces
the old global model/strategy uniqueness constraint with uniqueness over
enabled rows only. During cutover, preserve the retired route rows for audit:
in one reviewed transaction, set the wallet-2/wallet-3 rows to
`enabled=false` with `version=version+1`, then insert new wallet-6/wallet-7
rows. Do not `UPDATE execution_account_id` on an old row, and do not delete it.
The transaction must leave wallet-2/wallet-3 disabled and all four current
wallet bindings enabled.

Every configured account must also have one enabled execution-safety policy
and one unique ACCOUNT control. Preflight pins only
`90000/30000/600000` milliseconds for price/signal/state age and timezone
`UTC`; legacy monetary columns remain readable for audit but do not authorize
or reject an order. All four policies and bindings are enabled and all four ACCOUNT
controls are unpaused in shadow. The submission quarantine is exactly empty.
These wallet-6/wallet-7 template values are not approval to activate either
wallet. An active wallet-6 or wallet-7 requires a separate mode-`0600`
`four_wallet.activation_risk_approval.v1` artifact and its externally reviewed
SHA-256 on both preflight passes. The artifact pins the release commit, exact
active wallet set, policy identity/version and freshness contract; the disabled-phase
evidence schema cannot be reused as this approval. Activation also requires
fresh funding/allowance/placement and strict startup reconciliation. The
rollout must not modify the existing main/wallet-1 policies.
Retired wallet rows stay in PostgreSQL for audit, but their strategy bindings
must not remain enabled and they must not appear in the runtime wallet file.

The quarantine list is pinned to the exact empty array. Never quarantine
main/wallet-1: both have managed OPEN lots and must remain eligible for
reconciliation and SELL exits. wallet-6/wallet-7 activation is a reviewed
stopped-service CAS change while the global kill switch stays closed.

Prediction must publish the Sandbox source model named in the bindings, not the
logical alias. The following values are mandatory in `/etc/prediction-infra/env`
and must be loaded by the actual `prediction-infra.service` process:

```text
PREDICTION_RESULT_ENABLED=true
TRADING_INPUT_ENABLED=true
DATABASE_URL=postgresql://...
TRADING_INPUT_TOKEN=...
PREDICTION_RESULT_TOKEN=...
```

All four wallets consume the Sandbox source only, so there is no Direct Redis
model: the preflight uses an empty Direct group mapping, performs no Redis
`XINFO`, and requires zero Direct manifest markets. Prediction may keep
producing Echo through `DIRECT_PREDICTION_MODEL_IDS_JSON` for other consumers;
that inventory is not part of this contract. (If a Direct source model is ever
added back to the source-mode map, the preflight again requires the Redis
settings, an exact `DIRECT_PREDICTION_MODEL_IDS_JSON`, and a consumer group per
Direct model.) Prediction result callbacks and Trading input must both be
enabled. `TRADING_INPUT_TOKEN` must equal Trading's
`DECISION_CYCLE_PREDICTION_INFRA_TOKEN`; both Prediction tokens and Trading's
strategy token must be non-empty, and the result/input tokens must be distinct.

Run the read-only preflight while submission is still disabled:

```bash
python3 deploy/four_wallet_preflight.py \
  --env-file /etc/trading-execution/env \
  --prediction-env-file /etc/prediction-infra/env \
  --submission-state disabled \
  --expected-trading-commit APPROVED_FULL_TRADING_COMMIT \
  --expected-prediction-version APPROVED_IMMUTABLE_PREDICTION_BUILD_ID \
  --direct-model-groups-json '{}' \
  --write-disabled-evidence /var/lib/trading-execution/four-wallet-disabled-evidence.json
```

When either onboarding wallet is active, add both of the following to this
rollout command. The decision in the protected artifact must be exactly
`APPROVED_FOR_LIVE_ACTIVATION`. The SHA must come from the reviewed change
record, not be calculated ad hoc during deployment:

```text
--activation-risk-approval-json /var/lib/trading-execution/wallet67-risk-approval.json
--activation-risk-approval-sha256 REVIEWED_64_LOWERCASE_HEX_SHA256
```

If the reviewed Prediction binary exposes only an abbreviated Git prefix in
`/health/live`, keep `--expected-prediction-version` set to the full 40-byte
commit identity and additionally provide both
`--expected-prediction-health-version` and
`--expected-prediction-executable-sha256`. The abbreviated value must be a
7..39 character prefix of the full commit, and the executable SHA must come
from a byte-identical reproducible build of that exact commit. The preflight
hashes `/proc/<pid>/exe` itself and binds both identities into the two-phase
configuration digest; a short health value alone is never accepted.

The preflight exits non-zero if any of these invariants is false:

- binding JSON is malformed, duplicated, contains a placeholder, or is not the
  complete 2-by-2 matrix;
- strict same-market model coverage is not explicitly enabled;
- the wallet secret is not a protected regular file containing exactly the
  four configured account IDs;
- any configured execution account or database authorization row is missing,
  or enabled binding state differs from the exact four-active-wallet state;
- any configured account lacks its reviewed current-rollout risk policy or
  a unique ACCOUNT control, or policy/control state disagrees with account
  activation state;
- the actual systemd processes do not expose the approved health identity or
  do not contain the audited non-secret environment. This includes Trading's
  fee/finality safety, disabled market-order policy, PIT
  and midpoint lookbacks, prediction source-mode contract, and Prediction's
  Redis address/database, so the
  preflight cannot inspect one configuration while the processes use another;
- Prediction's health version is not an immutable lowercase 40-character Git
  SHA or `sha256:<64 lowercase hex>` image digest. Reusable values such as
  `dev`, a branch, or a semantic version are rejected. The Prediction build
  must set `internal/version.Value` through its documented linker flag to that
  immutable identity;
- the running Trading database URL, strategy/snapshot tokens, or Prediction
  database URL, Redis password, input/result tokens differ from the audited
  files. Trading's execution, read-only, and dedicated job tokens are pinned
  the same way. These values are compared and persisted only as one-way
  SHA-256 fingerprints and are never printed. Internal reconciliation and
  position-exit job endpoints accept only `POSITION_EXIT_JOB_TOKEN`; they
  never fall back to the execution API token;
- there is no recent, completed `submission=false` decision boundary from the
  current disabled Trading process with exactly one output row for each of the
  four active bindings and the same Prediction snapshot;
- main/wallet-1 do not have exactly zero predictions with the explicit
  `ENTRY_SUBMISSION_DISABLED` account policy, or wallet-6/wallet-7 do not have
  at least one prediction with no entry block;
- the PIT snapshot does not contain at least one fresh, PIT-visible effective
  new-model Sandbox result with a non-empty `sandbox_id`, or contains an
  ambiguous equally-timed result. The Direct manifest market count must be
  zero for this Sandbox-only entry cohort;
- any Direct consumer group is supplied for the Sandbox-only entry cohort;
- any managed-account decision intent delivery remains `PENDING`, `SUBMITTING`, or
  `UNKNOWN`, any delivery's order status is `UNKNOWN`/`MANUAL_REVIEW`, or any
  managed execution order remains `UNKNOWN`/`MANUAL_REVIEW`. Retired
  wallet-2/wallet-3 history remains available for audit but is outside these
  runtime recovery checks. Startup recovery and the order coordinator are
  scoped to active accounts, and even the manual reconciliation endpoint
  rejects retired accounts before recording a run or reading external state;
  the preflight never cancels or edits retired history;
- the database global kill switch is not still on.

By default these checks are collected directly and read-only: `systemctl` plus
`/proc` for the actual process and environment, both liveness endpoints for
release identity, `psql` for authorization and dry-run rows, the exact PIT
snapshot HTTP endpoint for the recorded decision boundary, and Redis `XINFO`
only when the entry-enabled cohort contains a Direct model. This Sandbox-only
cohort skips Redis consumer liveness. No credential is printed; PostgreSQL uses a protected
temporary passfile and Redis authentication remains in process memory.

If the deployment host deliberately prevents one of those reads, the operator
must provide the corresponding fresh evidence file with
`--runtime-state-json`, `--database-state-json`,
`--prediction-snapshot-json`, or `--consumer-state-json`. These flags are not
skip switches: the same invariants and freshness limits are applied, and a
missing or stale artifact fails closed. The default runtime/consumer evidence
age is 120 seconds; the default accepted dry-run age is 30 minutes.

Normal production operation should omit evidence JSON flags so the script
re-reads systemd, `/proc`, both health endpoints, and PostgreSQL live.

The disabled pass creates a new mode-`0600` evidence file without overwriting an
existing path. It binds the immutable releases, audited configuration,
one-way credential fingerprints, protected wallet
file content digest, four database wallet-address identities, exact dry-run
cycle/snapshot, manifest Market count, model-to-group mapping, and the global
delivery count/status/high-watermark. Terminal delivery history is aggregated
inside PostgreSQL; it is not copied into a growing JSON artifact. Preserve this
file unchanged. The evidence excludes only the submission flag so the same
immutable configuration can be verified in two phases.

First run with `DECISION_CYCLE_ORDER_SUBMISSION_ENABLED=false` and the database
global kill switch `true`. Then restart the same release with submission
`true`, keep the kill switch `true`, and run `--submission-state enabled`
against the exact disabled evidence and the independent LIVE approval. Only an
enabled PASS permits the separate CAS that opens the global kill switch. Never
reuse a shadow approval or auto-cancel historical intents.

Rollback to a binary that does not understand the account entry gate is a
stopped-service operation. First keep/set the database kill switch to `true`,
set order submission to `false`, and set the legacy process-wide entry gate to
`true`. Then atomically restore the legacy wallet-6/wallet-7 disabled policies,
bindings and paused ACCOUNT controls together with their legacy quarantine
before starting the old binary. Never start an old binary with all four
accounts active and only the new per-account gate in its environment; that
would reopen main/wallet-1 BUY entries.

Apart from creating the explicitly requested disabled-phase evidence file, the
preflight never edits configuration, restarts a service, acknowledges a Redis
message, or updates PostgreSQL. It does not open the kill switch. A PASS is
evidence for the operator's separate reviewed activation step, not an
activation by itself.
