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
DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE=true
POSITION_EXIT_JOB_TOKEN=DEDICATED_32_BYTE_OR_LONGER_SECRET
execution_risk_global_control.kill_switch=true
```

For an explicitly approved exit-only maintenance window, set
`DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED=true` before enabling order
submission. Run the preflight with `--entry-submission-state blocked`; it also
requires zero non-terminal BUY orders. The execution service enforces this
gate for HTTP submissions, decision delivery, and crash recovery, not only in
the Python decision-cycle path.

The `DECISION_CYCLE_BINDINGS_JSON` array must be replaced atomically as one
environment-file value. The routes are exact: `echo/multfactor_v2 -> main`,
`echo/multfactor_v1 -> wallet-1`, `gemini_masked/multfactor_v1 -> wallet-6`,
and `gemini_masked/multfactor_v2 -> wallet-7`. The database must contain the
same four `(model_id, strategy_id, execution_account_id)` authorization rows.
Initially only the `main` and `wallet-1` rows are enabled; the `wallet-6` and
`wallet-7` rows are present but disabled because those accounts are listed in
`DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON`. Source
`prediction_model_id` values never belong in the database authorization table.

Apply `migrations/0017_enabled_strategy_binding_uniqueness.sql` while the
service is stopped and the global kill switch is on. The migration replaces
the old global model/strategy uniqueness constraint with uniqueness over
enabled rows only. During cutover, preserve the retired route rows for audit:
in one reviewed transaction, set the wallet-2/wallet-3 rows to
`enabled=false` with `version=version+1`, then insert new wallet-6/wallet-7
rows. Do not `UPDATE execution_account_id` on an old row, and do not delete it.
The transaction must leave wallet-2/wallet-3 disabled, main/wallet-1 enabled,
and wallet-6/wallet-7 present but disabled for the initial quarantine.

Every configured account must also have one live-risk policy and one unique
ACCOUNT control. The current-rollout policy caps are pinned per account in the
preflight code:
`1.10/2.10/2.10/2.10/1.10` for order/market/strategy/wallet/daily notional,
`90000/30000/600000` milliseconds for price/signal/state age, and timezone
`UTC`. Initially main/wallet-1 policies are enabled and controls unpaused;
wallet-6/wallet-7 policies and bindings are disabled and controls paused.
That state must exactly match
`DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON` in both preflight phases.
These wallet-6/wallet-7 template values are not approval to activate either
wallet; activation requires fresh funding/allowance/position evidence and an
explicitly reviewed risk decision. The rollout must not modify the existing
main/wallet-1 policies.
Retired wallet rows stay in PostgreSQL for audit, but their strategy bindings
must not remain enabled and they must not appear in the runtime wallet file.

This release cannot remove wallet-6 or wallet-7 from quarantine: both the Go
runtime and the preflight enforce that exact pair. A future, separate release
may do so only after fresh funding/allowance/position evidence and explicit
approval; its reviewed stopped-service cutover must atomically enable the
policy/binding, unpause the ACCOUNT control with an increasing version, and
change the runtime quarantine list while the global kill switch stays closed.

Prediction must publish the same source-model set, not the logical aliases.
The following values are mandatory in `/etc/prediction-infra/env` and must be
loaded by the actual `prediction-infra.service` process:

```text
REDIS_ENABLED=true
REDIS_ADDRESS=APPROVED_REDIS_HOST:6379
REDIS_DATABASE=APPROVED_REDIS_DATABASE
DIRECT_PREDICTION_ENABLED=true
REDIS_DIRECT_PREDICTION_STREAM_KEY=prediction_pm_directprediction
DIRECT_PREDICTION_MODEL_IDS_JSON=["EXACT_ECHO_SNAPSHOT_MODEL_NAME","gemini-3.6-flash"]
PREDICTION_RESULT_ENABLED=true
TRADING_INPUT_ENABLED=true
DATABASE_URL=postgresql://...
REDIS_PASSWORD=...
TRADING_INPUT_TOKEN=...
PREDICTION_RESULT_TOKEN=...
```

`DIRECT_PREDICTION_MODEL_IDS_JSON` must contain the active echo source model.
It may also retain the configured quarantined Gemini source, but unknown,
blank, or duplicated models fail preflight. Gemini availability is not a hard
gate in this release. Prediction result callbacks and Trading input must both
be enabled. `TRADING_INPUT_TOKEN` must equal Trading's
`DECISION_CYCLE_PREDICTION_INFRA_TOKEN`; both Prediction tokens and Trading's
strategy token must be non-empty, and the result/input tokens must be distinct.

Use one distinct Redis group per active source model. The current hard gate is
only the echo group used by main/wallet-1. A Gemini mapping may be supplied as
non-blocking onboarding metadata, but this release ignores its consumer health.
Preflight runs `XINFO` for active echo and requires a recent consumer, zero
pending consumer messages, zero group pending, and zero lag.

Run the read-only preflight while submission is still disabled:

```bash
python3 deploy/four_wallet_preflight.py \
  --env-file /etc/trading-execution/env \
  --prediction-env-file /etc/prediction-infra/env \
  --submission-state disabled \
  --expected-trading-commit APPROVED_FULL_TRADING_COMMIT \
  --expected-prediction-version APPROVED_IMMUTABLE_PREDICTION_BUILD_ID \
  --direct-model-groups-json \
  '{"EXACT_ECHO_SNAPSHOT_MODEL_NAME":"predict-echo-v1"}' \
  --write-disabled-evidence /var/lib/trading-execution/four-wallet-disabled-evidence.json
```

The preflight exits non-zero if any of these invariants is false:

- binding JSON is malformed, duplicated, contains a placeholder, or is not the
  complete 2-by-2 matrix;
- strict same-market model coverage is not explicitly enabled;
- the wallet secret is not a protected regular file containing exactly the
  four configured account IDs;
- any configured execution account or database authorization row is missing,
  or enabled binding state differs from the active/quarantined partition;
- any configured account lacks its reviewed current-rollout risk policy or
  a unique ACCOUNT control, or policy/control state disagrees with account
  quarantine;
- the actual systemd processes do not expose the approved health identity or
  do not contain the audited non-secret environment. This includes Trading's
  PIT lookback and Prediction's Redis address/database, so the preflight cannot
  inspect Redis A while the running publisher uses Redis B;
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
  current disabled Trading process with exactly one output row for each active
  (non-quarantined) binding and the same Prediction snapshot;
- any active binding received zero predictions, failed before persisting output, or
  carries `entry_policy.enabled=false` or a non-empty model-coverage block
  reason. Older compatible healthy output may omit `entry_policy`; omission is
  accepted, while explicit `false` is never treated as omission;
- the PIT snapshot used by that dry run does not contain an effective Direct
  task manifest where every Market has the active echo source model in
  `COMPLETED` state and each task has its exact matching result;
- the active echo Redis consumer group is missing, inactive, pending, or lagging;
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
for the Direct consumer. No credential is printed; PostgreSQL uses a protected
temporary passfile and Redis authentication remains in process memory.

If the deployment host deliberately prevents one of those reads, the operator
must provide the corresponding fresh evidence file with
`--runtime-state-json`, `--database-state-json`,
`--prediction-snapshot-json`, or `--consumer-state-json`. These flags are not
skip switches: the same invariants and freshness limits are applied, and a
missing or stale artifact fails closed. The default runtime/consumer evidence
age is 120 seconds; the default accepted dry-run age is 30 minutes.

Enabled offline mode additionally requires a separate
`--final-runtime-state-json` and `--final-database-state-json`. They must be
different files from the initial artifacts. Final runtime evidence must follow
the consumer evidence and prove both service PIDs/start times are unchanged;
final DB evidence must be collected after that and must still contain the exact
active-binding dry run. Reusing initial JSON as a fake final reread is rejected. Normal
production operation should omit these JSON flags so the script re-reads
systemd, `/proc`, both health endpoints, and then PostgreSQL live.

The disabled pass creates a new mode-`0600` evidence file without overwriting an
existing path. It binds the immutable releases, all audited configuration
except the submission flag, one-way credential fingerprints, protected wallet
file content digest, four database wallet-address identities, exact dry-run
cycle/snapshot, manifest Market count, model-to-group mapping, and the global
delivery count/status/high-watermark. Terminal delivery history is aggregated
inside PostgreSQL; it is not copied into a growing JSON artifact. Preserve this
file unchanged.

Choose an explicit no-trading maintenance window immediately after a decision
boundary. Only after the disabled pass should the environment submission flag
be staged as `true` and the service restarted. Keep the database kill switch
on, then run the full preflight again, replacing the write option with the
saved evidence:

```bash
python3 deploy/four_wallet_preflight.py \
  --env-file /etc/trading-execution/env \
  --prediction-env-file /etc/prediction-infra/env \
  --submission-state enabled \
  --expected-trading-commit APPROVED_FULL_TRADING_COMMIT \
  --expected-prediction-version APPROVED_IMMUTABLE_PREDICTION_BUILD_ID \
  --direct-model-groups-json \
  '{"EXACT_ECHO_SNAPSHOT_MODEL_NAME":"predict-echo-v1"}' \
  --disabled-evidence-json /var/lib/trading-execution/four-wallet-disabled-evidence.json
```

The enabled pass requires the newly running process to have the same approved
release identity, wallet/database identities, credential fingerprints, and
audited configuration hash; only `DECISION_CYCLE_ORDER_SUBMISSION_ENABLED` may
differ. It then revalidates the saved `submission=false` cycle and
content-addressed PIT snapshot. It does not
require that row to be newer than the enabled process PID, which would be
impossible after restart. If any `submission=true` decision cycle has occurred
after the saved evidence, activation fails: keep the kill switch closed,
discard that attempt, return to `submission=false`, and produce a new dry-run
artifact. Never use a `submission=true` cycle as a dry run because its durable
intents are real and retryable even while downstream risk rejects placement.

At enabled-preflight entry, the script captures the next scheduler due time
once; it never rolls that value forward if checks cross the boundary. The
running Trading process's first possible due is also derived from its actual
start time; if that due could already have begun, activation fails. The enabled
pass rechecks both unchanged processes, then rereads PostgreSQL as its final
evidence step and rejects any
delivery watermark change or any cycle after the disabled evidence. It also
prints `activation_deadline`, which is the next scheduler due time minus the
configured safety margin (60 seconds by default). Open the database kill switch
only after this PASS and strictly before that deadline. Do **not** wait for
decision-cycle readiness first: readiness requires a successful cycle, while a
BUY/SELL cycle with the kill switch still closed can create a rejected,
recoverable delivery and make readiness fail. Before opening the switch, check
only liveness/release identity and the enabled preflight. After opening it,
wait for the first successful cycle and normal readiness.

If the deadline is missed, a cycle appears, or any delivery watermark changes,
keep the kill switch closed, restore submission to `false`, and investigate the
delivery rows manually before producing new disabled evidence. Never simply
retry enabled mode and never auto-cancel historical intents.

Apart from creating the explicitly requested disabled-phase evidence file, the
preflight never edits configuration, restarts a service, acknowledges a Redis
message, or updates PostgreSQL. It does not open the kill switch. A PASS is
evidence for the operator's separate reviewed activation step, not an
activation by itself.
