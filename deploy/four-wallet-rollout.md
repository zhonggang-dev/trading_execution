# Four-wallet decision-cycle rollout

The four routes are one configuration unit: two upstream prediction models,
two logical strategies for each model, and four distinct execution accounts.
Do not reuse the legacy wallet-2 canary deployment or activation scripts for
this topology; those scripts intentionally contain a one-binding subset.

Use [`four-wallet-decision-cycle.env.example`](four-wallet-decision-cycle.env.example)
as the non-secret fragment. Replace every placeholder with the exact
`model.name` returned by the current Prediction PIT snapshot, install a wallet
file containing exactly `main`, `wallet-1`, `wallet-2`, and `wallet-3`, and keep
both durable gates closed:

```text
DECISION_CYCLE_ORDER_SUBMISSION_ENABLED=false
DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE=true
execution_risk_global_control.kill_switch=true
```

The `DECISION_CYCLE_BINDINGS_JSON` array must be replaced atomically as one
environment-file value. Each logical model must have both `multfactor_v1` and
`multfactor_v2`; each route must use a different wallet. The database must
contain the same four enabled `(model_id, strategy_id, execution_account_id)`
authorizations. Source `prediction_model_id` values never belong in the
database authorization table.

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

`DIRECT_PREDICTION_MODEL_IDS_JSON` must be exactly the set of the two Trading
`prediction_model_id` values. Order is irrelevant; missing, additional, blank,
or duplicated models fail preflight. Prediction result callbacks and Trading
input must both be enabled. `TRADING_INPUT_TOKEN` must equal Trading's
`DECISION_CYCLE_PREDICTION_INFRA_TOKEN`; both Prediction tokens and Trading's
strategy token must be non-empty, and the result/input tokens must be distinct.

Use one distinct Redis group per source model. A single group containing two
model-specific consumers is invalid because Redis will let them compete for
messages. The JSON mapping passed below must have exactly the two source-model
keys. Preflight runs `XINFO` for both groups and requires a recent consumer,
zero pending consumer messages, zero group pending, and zero lag for each one.

Run the read-only preflight while submission is still disabled:

```bash
python3 deploy/four_wallet_preflight.py \
  --env-file /etc/trading-execution/env \
  --prediction-env-file /etc/prediction-infra/env \
  --submission-state disabled \
  --expected-trading-commit APPROVED_FULL_TRADING_COMMIT \
  --expected-prediction-version APPROVED_IMMUTABLE_PREDICTION_BUILD_ID \
  --direct-model-groups-json \
  '{"EXACT_ECHO_SNAPSHOT_MODEL_NAME":"predict-echo-v1","gemini-3.6-flash":"predict-gemini-v1"}' \
  --write-disabled-evidence /var/lib/trading-execution/four-wallet-disabled-evidence.json
```

The preflight exits non-zero if any of these invariants is false:

- binding JSON is malformed, duplicated, contains a placeholder, or is not the
  complete 2-by-2 matrix;
- strict same-market model coverage is not explicitly enabled;
- the wallet secret is not a protected regular file containing exactly the
  four configured account IDs;
- any execution account or enabled database authorization is missing, or an
  unexpected database authorization remains enabled;
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
  files. These values are compared and persisted only as one-way SHA-256
  fingerprints and are never printed;
- there is no recent, completed `submission=false` decision boundary from the
  current disabled Trading process with exactly four output rows, one for each
  configured binding and the same Prediction snapshot;
- any binding received zero predictions, failed before persisting output, or
  carries `entry_policy.enabled=false` or a non-empty model-coverage block
  reason. Older compatible healthy output may omit `entry_policy`; omission is
  accepted, while explicit `false` is never treated as omission;
- the PIT snapshot used by that dry run does not contain an effective Direct
  task manifest where every Market has both configured source models in
  `COMPLETED` state and each task has its exact matching result;
- either approved per-model Redis consumer group is missing, inactive, shared
  by the two model-specific routes, pending, or lagging;
- any global decision intent delivery remains `PENDING`, `SUBMITTING`, or
  `UNKNOWN`, any delivery's order status is `UNKNOWN`/`MANUAL_REVIEW`, or any
  execution order remains `UNKNOWN`/`MANUAL_REVIEW`. `RecoverStartup` runs
  before coverage checks and can retry this history, so operators must resolve
  it explicitly; the preflight never cancels or edits it;
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
4-row dry run. Reusing initial JSON as a fake final reread is rejected. Normal
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
  '{"EXACT_ECHO_SNAPSHOT_MODEL_NAME":"predict-echo-v1","gemini-3.6-flash":"predict-gemini-v1"}' \
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
