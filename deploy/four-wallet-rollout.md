# Four-wallet decision-cycle rollout

The four routes are one configuration unit: two upstream prediction models,
two logical strategies for each model, and four distinct execution accounts.
Do not reuse the legacy wallet-2 canary deployment or activation scripts for
this topology; those scripts intentionally contain a one-binding subset.

Use [`four-wallet-decision-cycle.env.example`](four-wallet-decision-cycle.env.example)
as the non-secret fragment. Replace every placeholder with the exact
`model.name` returned by the current Prediction PIT snapshot, install a wallet
file containing exactly `main`, `wallet-1`, `wallet-6`, and `wallet-7`, and keep
both durable gates closed:

```text
DECISION_CYCLE_ORDER_SUBMISSION_ENABLED=false
DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED=false
DECISION_CYCLE_ENTRY_DISABLED_ACCOUNTS_JSON=["main","wallet-1"]
DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON=[]
DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE=true
POSITION_EXIT_JOB_TOKEN=DEDICATED_32_BYTE_OR_LONGER_SECRET
execution_risk_global_control.kill_switch=true
```

The same reviewed environment must explicitly pin every process-local order
limit and v2 history window; no Go default is accepted by this rollout:

```text
EXECUTION_ALLOW_MARKET_ORDERS=false
EXECUTION_MAX_ORDER_SIZE=EXACT_REVIEWED_POSITIVE_DECIMAL
EXECUTION_MAX_ORDER_NOTIONAL=EXACT_REVIEWED_POSITIVE_DECIMAL
POLYMARKET_MAX_BUY_FEE_RATE_BPS=EXACT_REVIEWED_NON_NEGATIVE_DECIMAL
POLYGON_ORDER_FILLED_CONFIRMATIONS=EXACT_REVIEWED_POSITIVE_INTEGER
DECISION_CYCLE_MID_PRICE_LOOKBACK=EXACT_REVIEWED_DURATION
DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON={"EXACT_ECHO_SNAPSHOT_MODEL_NAME":"DIRECT","gemini-3.6-flash":"SANDBOX"}
```

The preflight binds these values into the configuration SHA and verifies the
running process has the same values. The account entry list is also hashed and
must be exactly `main,wallet-1`. The Go runtime also enforces the
source-mode map on every cycle: `DIRECT` accepts only an empty `sandbox_id`,
while `SANDBOX` requires a non-empty `sandbox_id`. The map must exactly cover
all configured upstream models; the current release pins echo to `DIRECT` and
`gemini_masked` to `SANDBOX`. Market orders must remain disabled for the
four-wallet activation; changing any other value invalidates disabled-pass
evidence and requires a new review.

This release requires the process-wide entry gate to remain `false`. The exact
account gate makes main/wallet-1 sell-only: they keep position-bearing strategy
requests and may submit SELL exits, but receive no entry predictions.
wallet-6/wallet-7 receive the reviewed Sandbox predictions. The execution
service enforces main/wallet-1 BUY rejection for direct HTTP Submit, reserved
Resume, decision delivery, and crash recovery.

The `DECISION_CYCLE_BINDINGS_JSON` array must be replaced atomically as one
environment-file value. The routes are exact: `echo/multfactor_v2 -> main`,
`echo/multfactor_v1 -> wallet-1`, `gemini_masked/multfactor_v1 -> wallet-6`,
and `gemini_masked/multfactor_v2 -> wallet-7`. The database must contain the
same four `(model_id, strategy_id, execution_account_id)` authorization rows.
All four authorization rows are enabled in shadow. main/wallet-1 remain active
so their existing OPEN lots can be reconciled and exited; new BUY entries are
blocked by the account gate. Source
`prediction_model_id` values never belong in the database authorization table.

Apply `migrations/0017_enabled_strategy_binding_uniqueness.sql` while the
service is stopped and the global kill switch is on. The migration replaces
the old global model/strategy uniqueness constraint with uniqueness over
enabled rows only. During cutover, preserve the retired route rows for audit:
in one reviewed transaction, set the wallet-2/wallet-3 rows to
`enabled=false` with `version=version+1`, then insert new wallet-6/wallet-7
rows. Do not `UPDATE execution_account_id` on an old row, and do not delete it.
The transaction must leave wallet-2/wallet-3 disabled and all four current
wallet bindings enabled.

Every configured account must also have one live-risk policy and one unique
ACCOUNT control. The current-rollout policy caps are pinned per account in the
preflight code:
`1.10/2.10/2.10/2.10/1.10` for order/market/strategy/wallet/daily notional,
`90000/30000/600000` milliseconds for price/signal/state age, and timezone
`UTC` for the retained accounts; wallet-6/wallet-7 use the separately approved
v2 limits. All four policies and bindings are enabled and all four ACCOUNT
controls are unpaused in shadow. The submission quarantine is exactly empty.
These wallet-6/wallet-7 template values are not approval to activate either
wallet. An active wallet-6 or wallet-7 requires a separate mode-`0600`
`four_wallet.activation_risk_approval.v1` artifact and its externally reviewed
SHA-256 on both preflight passes. The artifact pins the release commit, exact
active wallet set, policy identity/version and every limit; the disabled-phase
evidence schema cannot be reused as this approval. Activation also requires
fresh funding/allowance/placement and strict startup reconciliation. The
rollout must not modify the existing main/wallet-1 policies.
Retired wallet rows stay in PostgreSQL for audit, but their strategy bindings
must not remain enabled and they must not appear in the runtime wallet file.

The quarantine list is pinned to the exact empty array. Never quarantine
main/wallet-1: both have managed OPEN lots and must remain eligible for
reconciliation and SELL exits. wallet-6/wallet-7 activation is a reviewed
stopped-service CAS change while the global kill switch stays closed.

Prediction must publish the same source-model set, not the logical aliases.
The following values are mandatory in `/etc/prediction-infra/env` and must be
loaded by the actual `prediction-infra.service` process:

```text
REDIS_ENABLED=true
REDIS_ADDRESS=APPROVED_REDIS_HOST:6379
REDIS_DATABASE=APPROVED_REDIS_DATABASE
DIRECT_PREDICTION_ENABLED=true
REDIS_DIRECT_PREDICTION_STREAM_KEY=prediction_pm_directprediction
DIRECT_PREDICTION_MODEL_IDS_JSON=["EXACT_ECHO_SNAPSHOT_MODEL_NAME"]
PREDICTION_RESULT_ENABLED=true
TRADING_INPUT_ENABLED=true
DATABASE_URL=postgresql://...
REDIS_PASSWORD=...
TRADING_INPUT_TOKEN=...
PREDICTION_RESULT_TOKEN=...
```

`DIRECT_PREDICTION_MODEL_IDS_JSON` must equal the `DIRECT` subset in Trading's
source-mode map, so it contains the echo source model and must not contain the
Sandbox Gemini source. Unknown, blank, duplicated, missing, or extra models
fail preflight. Prediction result callbacks and Trading input must both be
enabled. `TRADING_INPUT_TOKEN` must equal Trading's
`DECISION_CYCLE_PREDICTION_INFRA_TOKEN`; both Prediction tokens and Trading's
strategy token must be non-empty, and the result/input tokens must be distinct.

Direct model identity remains statically pinned in Prediction configuration,
but the entry-enabled cohort is wallet-6/wallet-7 and is Sandbox-only.
Therefore shadow preflight uses an empty Direct group mapping, performs no
Redis `XINFO`, and requires zero Direct manifest markets. main/wallet-1 do not
receive Echo entry predictions while their account BUY gate is active.

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
  static size/notional/fee/finality limits, disabled market-order policy, PIT
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
  Gemini Sandbox result with a non-empty `sandbox_id`, or contains an
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
