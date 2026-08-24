from __future__ import annotations

import contextlib
import datetime as dt
import io
import json
import pathlib
import tempfile
import unittest

import four_wallet_preflight as preflight


VALID_BINDINGS = [
    {
        "prediction_model_id": "echo-producer-v7",
        "model_id": "echo",
        "strategy_id": "multfactor_v2",
        "execution_account_id": "main",
    },
    {
        "prediction_model_id": "echo-producer-v7",
        "model_id": "echo",
        "strategy_id": "multfactor_v1",
        "execution_account_id": "wallet-1",
    },
    {
        "prediction_model_id": "gemini-3.6-flash",
        "model_id": "gemini_masked",
        "strategy_id": "multfactor_v1",
        "execution_account_id": "wallet-6",
    },
    {
        "prediction_model_id": "gemini-3.6-flash",
        "model_id": "gemini_masked",
        "strategy_id": "multfactor_v2",
        "execution_account_id": "wallet-7",
    },
]
SOURCE_MODES = {
    "echo-producer-v7": "DIRECT",
    "gemini-3.6-flash": "SANDBOX",
}
MODEL_GROUPS = {"echo-producer-v7": "predict-echo-v1"}
TRADING_COMMIT = "a" * 40
PREDICTION_COMMIT = "b" * 40
STATIC_TRADING_ENVIRONMENT = {
    "EXECUTION_ALLOW_MARKET_ORDERS": "false",
    "EXECUTION_MAX_ORDER_SIZE": "20",
    "EXECUTION_MAX_ORDER_NOTIONAL": "40",
    "POLYMARKET_MAX_BUY_FEE_RATE_BPS": "10000",
    "POLYGON_ORDER_FILLED_CONFIRMATIONS": "64",
    "DECISION_CYCLE_MID_PRICE_LOOKBACK": "48h",
    "DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON": json.dumps(
        SOURCE_MODES, separators=(",", ":")
    ),
}
STATIC_TRADING_DRIFT = {
    "EXECUTION_ALLOW_MARKET_ORDERS": "true",
    "EXECUTION_MAX_ORDER_SIZE": "21",
    "EXECUTION_MAX_ORDER_NOTIONAL": "41",
    "POLYMARKET_MAX_BUY_FEE_RATE_BPS": "9999",
    "POLYGON_ORDER_FILLED_CONFIRMATIONS": "65",
    "DECISION_CYCLE_MID_PRICE_LOOKBACK": "47h",
    "DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON": (
        '{"echo-producer-v7":"SANDBOX","gemini-3.6-flash":"DIRECT"}'
    ),
}


def iso(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def dry_run_rows(entry_policy_enabled: object = None, block_reason: str = "") -> list[dict[str, object]]:
    return [
        {
            "model_id": item["model_id"],
            "strategy_id": item["strategy_id"],
            "execution_account_id": item["execution_account_id"],
            "order_submission_enabled": False,
            "has_output": True,
            "prediction_count": 1,
            "entry_policy_enabled": entry_policy_enabled,
            "entry_block_reason": block_reason,
        }
        for item in VALID_BINDINGS
    ]


def complete_snapshot(decision_at: dt.datetime, snapshot_id: str) -> dict[str, object]:
    outcomes = [
        {"index": 0, "name": "Yes", "token_id": "yes-token", "probability": 0.7},
        {"index": 1, "name": "No", "token_id": "no-token", "probability": 0.3},
    ]
    prediction_as_of = iso(decision_at - dt.timedelta(minutes=12))
    expectations = [
        {
            "prediction_id": "pred-direct-1",
            "source_job_id": "pm-direct:1",
            "prediction_model_id": "echo-producer-v7",
            "selection_id": 101,
            "selection_run_id": 41,
            "market_id": "market-1",
            "condition_id": "condition-1",
            "outcomes": outcomes,
            "prediction_as_of": prediction_as_of,
            "task_available_at": iso(decision_at - dt.timedelta(minutes=11)),
            "status": "COMPLETED",
            "result_available_at": iso(decision_at - dt.timedelta(minutes=1)),
        }
    ]
    predictions = [
        prediction_result(
            decision_at,
            prediction_id="pred-direct-1",
            model_id="echo-producer-v7",
            sandbox_id="",
        ),
        prediction_result(
            decision_at,
            prediction_id="pred-sandbox-2",
            model_id="gemini-3.6-flash",
            sandbox_id="sandbox-gemini",
        ),
    ]
    predictions[0]["source_job_id"] = "pm-direct:1"
    return {
        "data": {
            "snapshot_id": snapshot_id,
            "decision_at": iso(decision_at),
            "expected_predictions": expectations,
            "predictions": predictions,
        }
    }


def prediction_result(
    decision_at: dt.datetime,
    *,
    prediction_id: str,
    model_id: str,
    sandbox_id: str,
    market_id: str = "market-1",
    condition_id: str = "condition-1",
) -> dict[str, object]:
    return {
        "prediction_id": prediction_id,
        "source_job_id": f"job-{prediction_id}",
        "sandbox_id": sandbox_id,
        "market_id": market_id,
        "condition_id": condition_id,
        "question": "Will it happen?",
        "domains": ["Other"],
        "neg_risk": False,
        "outcomes": [
            {"index": 0, "name": "Yes", "token_id": "yes-token", "probability": 0.7},
            {"index": 1, "name": "No", "token_id": "no-token", "probability": 0.3},
        ],
        "prediction_as_of": iso(decision_at - dt.timedelta(minutes=12)),
        "completed_at": iso(decision_at - dt.timedelta(minutes=2)),
        "available_at": iso(decision_at - dt.timedelta(minutes=1)),
        "model": {"name": model_id, "predictor_version": "v1"},
    }


def database_state(bindings: list[dict[str, object]] | None = None) -> dict[str, object]:
    rows = bindings if bindings is not None else [
        {
            "model_id": item["model_id"],
            "strategy_id": item["strategy_id"],
            "execution_account_id": item["execution_account_id"],
            "enabled": True,
        }
        for item in VALID_BINDINGS
    ]
    state = {
        "observed_at": iso(dt.datetime.now(dt.timezone.utc)),
        "global_kill_switch": True,
        "accounts": [
            {"execution_account_id": account, "wallet_address": "0x00"}
            for account in sorted(preflight.EXPECTED_ACCOUNTS)
        ],
        "bindings": rows,
        "risk_policies": [
            {
                "execution_account_id": account,
                "policy_id": f"policy-{account}",
                "version": 1,
                "enabled": True,
                **preflight.CURRENT_ROLLOUT_RISK_CONTRACT_BY_ACCOUNT[account],
            }
            for account in sorted(preflight.EXPECTED_ACCOUNTS)
        ],
        "account_controls": [
            {
                "execution_account_id": account,
                "control_scope": "ACCOUNT",
                "control_key": "",
                "paused": False,
                "reason": "TEST_READY",
                "version": 1,
            }
            for account in sorted(preflight.EXPECTED_ACCOUNTS)
        ],
        "decision_deliveries": [],
        "manual_review_orders": [],
        "nonterminal_buy_orders": [],
    }
    quarantine_accounts(state, "wallet-6", "wallet-7")
    return state


def quarantine_accounts(
    state: dict[str, object], *execution_account_ids: str
) -> None:
    quarantined = set(execution_account_ids)
    for row in state["bindings"]:
        if row["execution_account_id"] in quarantined:
            row["enabled"] = False
    for row in state["risk_policies"]:
        if row["execution_account_id"] in quarantined:
            row["enabled"] = False
    for row in state["account_controls"]:
        if row["execution_account_id"] in quarantined:
            row["paused"] = True
            row["reason"] = "TEST_QUARANTINE"
            row["version"] += 1


def activate_accounts(
    state: dict[str, object], *execution_account_ids: str
) -> None:
    activated = set(execution_account_ids)
    for row in state["bindings"]:
        if row["execution_account_id"] in activated:
            row["enabled"] = True
    for row in state["risk_policies"]:
        if row["execution_account_id"] in activated:
            row["enabled"] = True
    for row in state["account_controls"]:
        if row["execution_account_id"] in activated:
            row["paused"] = False
            row["reason"] = "TEST_ACTIVATION_APPROVED"
            row["version"] += 1


def approved_risk_contracts(
    state: dict[str, object], *execution_account_ids: str
) -> dict[str, dict[str, object]]:
    wanted = set(execution_account_ids)
    return {
        row["execution_account_id"]: {
            "policy_id": row["policy_id"],
            "version": row["version"],
            **{field: row[field] for field in preflight.POLICY_LIMIT_FIELDS},
            "daily_timezone": row["daily_timezone"],
        }
        for row in state["risk_policies"]
        if row["execution_account_id"] in wanted
    }


def activation_risk_approval(
    now: dt.datetime,
    state: dict[str, object],
    *execution_account_ids: str,
) -> dict[str, object]:
    contracts = approved_risk_contracts(state, *execution_account_ids)
    return {
        "schema_version": preflight.ACTIVATION_RISK_APPROVAL_SCHEMA,
        "decision": preflight.ACTIVATION_RISK_DECISION,
        "approval_id": "risk-review-ticket-123",
        "approved_by": "risk-reviewer",
        "approved_at": iso(now - dt.timedelta(minutes=5)),
        "expires_at": iso(now + dt.timedelta(hours=1)),
        "approved_trading_commit": TRADING_COMMIT,
        "accounts": [
            {"execution_account_id": account_id, **contracts[account_id]}
            for account_id in sorted(contracts)
        ],
    }


def consumer_state(now: dt.datetime) -> dict[str, object]:
    return {
        "observed_at": iso(now),
        "stream_key": "prediction_pm_directprediction",
        "model_groups": MODEL_GROUPS,
        "groups": [
            {
                "model_id": model_id,
                "group": group,
                "consumers": 1,
                "pending": 0,
                "lag": 0,
                "consumer_details": [
                    {
                        "name": f"worker-{index}",
                        "pending": 0,
                        "idle_ms": 1000,
                    }
                ],
            }
            for index, (model_id, group) in enumerate(MODEL_GROUPS.items(), start=1)
        ],
    }


def delivery_watermark() -> dict[str, object]:
    result: dict[str, object] = {
        "count": 0,
        "status_counts": {
            "PENDING": 0,
            "SUBMITTING": 0,
            "SUBMITTED": 0,
            "FAILED": 0,
            "UNKNOWN": 0,
        },
        "non_terminal_counts": {
            "PENDING": 0,
            "SUBMITTING": 0,
            "UNKNOWN": 0,
            "DELIVERY_ORDER_UNKNOWN_OR_MANUAL_REVIEW": 0,
            "EXECUTION_ORDER_UNKNOWN_OR_MANUAL_REVIEW": 0,
        },
        "max_updated_at": None,
    }
    result["sha256"] = preflight._canonical_sha256(result)
    return result


class BindingTopologyTests(unittest.TestCase):
    def test_accepts_complete_two_by_two_matrix(self) -> None:
        bindings = preflight.decode_bindings(json.dumps(VALID_BINDINGS))
        self.assertEqual(len(bindings), 4)
        self.assertEqual(
            {binding.execution_account_id for binding in bindings},
            preflight.EXPECTED_ACCOUNTS,
        )

    def test_accepts_current_prediction_source_names_without_hardcoding(self) -> None:
        current = [dict(item) for item in VALID_BINDINGS]
        current[0]["prediction_model_id"] = "echo-producer-current"
        current[1]["prediction_model_id"] = "echo-producer-current"
        current[2]["prediction_model_id"] = "masked-producer-current"
        current[3]["prediction_model_id"] = "masked-producer-current"
        self.assertEqual(len(preflight.decode_bindings(json.dumps(current))), 4)

    def test_rejects_missing_wallet(self) -> None:
        broken = [dict(item) for item in VALID_BINDINGS]
        broken[-1]["execution_account_id"] = "wallet-6"
        with self.assertRaisesRegex(preflight.PreflightError, "binding accounts must be exactly"):
            preflight.decode_bindings(json.dumps(broken))

    def test_rejects_incomplete_cartesian_product(self) -> None:
        broken = [dict(item) for item in VALID_BINDINGS]
        broken[-1]["strategy_id"] = "multfactor_v1"
        with self.assertRaisesRegex(preflight.PreflightError, "duplicate logical model/strategy"):
            preflight.decode_bindings(json.dumps(broken))

    def test_rejects_swapped_execution_account_routes(self) -> None:
        broken = [dict(item) for item in VALID_BINDINGS]
        broken[2]["execution_account_id"] = "wallet-7"
        broken[3]["execution_account_id"] = "wallet-6"
        with self.assertRaisesRegex(preflight.PreflightError, "must use execution account"):
            preflight.decode_bindings(json.dumps(broken))

    def test_rejects_swapped_retained_account_routes(self) -> None:
        broken = [dict(item) for item in VALID_BINDINGS]
        broken[0]["execution_account_id"] = "wallet-1"
        broken[1]["execution_account_id"] = "main"
        with self.assertRaisesRegex(preflight.PreflightError, "must use execution account"):
            preflight.decode_bindings(json.dumps(broken))

    def test_rejects_prediction_model_alias_collision(self) -> None:
        broken = [dict(item) for item in VALID_BINDINGS]
        broken[2]["prediction_model_id"] = "echo-producer-v7"
        broken[3]["prediction_model_id"] = "echo-producer-v7"
        with self.assertRaisesRegex(preflight.PreflightError, "exactly 2 logical and 2 prediction"):
            preflight.decode_bindings(json.dumps(broken))

    def test_rejects_unresolved_source_model_placeholder(self) -> None:
        broken = [dict(item) for item in VALID_BINDINGS]
        broken[0]["prediction_model_id"] = "EXACT_ECHO_SNAPSHOT_MODEL_NAME"
        with self.assertRaisesRegex(preflight.PreflightError, "unresolved"):
            preflight.decode_bindings(json.dumps(broken))


class DatabaseStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.bindings = preflight.decode_bindings(json.dumps(VALID_BINDINGS))
        self.active_bindings = preflight.active_rollout_bindings(
            self.bindings, ("wallet-6", "wallet-7")
        )

    def test_accepts_exact_active_and_quarantined_authorization_set(self) -> None:
        preflight.validate_database_state(database_state(), self.bindings)

    def test_rejects_missing_authorization(self) -> None:
        state = database_state()
        state["bindings"] = state["bindings"][:-1]
        with self.assertRaisesRegex(preflight.PreflightError, "missing configured"):
            preflight.validate_database_state(state, self.bindings)

    def test_rejects_unexpected_enabled_authorization(self) -> None:
        state = database_state()
        state["bindings"].append(
            {
                "model_id": "legacy",
                "strategy_id": "multfactor_v1",
                "execution_account_id": "legacy-wallet",
                "enabled": True,
            }
        )
        with self.assertRaisesRegex(preflight.PreflightError, "unexpected="):
            preflight.validate_database_state(state, self.bindings)

    def test_rejects_open_global_kill_switch(self) -> None:
        state = database_state()
        state["global_kill_switch"] = False
        with self.assertRaisesRegex(preflight.PreflightError, "kill switch"):
            preflight.validate_database_state(state, self.bindings)

    def test_rejects_missing_or_disabled_expected_risk_policy(self) -> None:
        missing = database_state()
        missing["risk_policies"] = missing["risk_policies"][:-1]
        with self.assertRaisesRegex(preflight.PreflightError, "missing risk policy"):
            preflight.validate_database_state(missing, self.bindings)

        disabled = database_state()
        disabled["risk_policies"][0]["enabled"] = False
        with self.assertRaisesRegex(preflight.PreflightError, "enabled=True"):
            preflight.validate_database_state(disabled, self.bindings)

    def test_rejects_missing_or_duplicate_account_control(self) -> None:
        missing = database_state()
        missing["account_controls"] = missing["account_controls"][:-1]
        with self.assertRaisesRegex(preflight.PreflightError, "missing ACCOUNT control"):
            preflight.validate_database_state(missing, self.bindings)

        duplicate = database_state()
        duplicate["account_controls"].append(dict(duplicate["account_controls"][0]))
        with self.assertRaisesRegex(preflight.PreflightError, "duplicate or invalid ACCOUNT"):
            preflight.validate_database_state(duplicate, self.bindings)

    def test_disabled_phase_rejects_paused_active_control(self) -> None:
        state = database_state()
        state["account_controls"][0]["paused"] = True
        state["account_controls"][0]["reason"] = "CUTOVER_NOT_ARMED"
        with self.assertRaisesRegex(preflight.PreflightError, "to be unpaused"):
            preflight.validate_database_state(
                state, self.bindings, submission_state="disabled"
            )

    def test_enabled_phase_requires_active_controls_unpaused(self) -> None:
        state = database_state()
        state["account_controls"][0]["paused"] = True
        state["account_controls"][0]["reason"] = "CUTOVER_NOT_ARMED"
        with self.assertRaisesRegex(preflight.PreflightError, "to be unpaused"):
            preflight.validate_database_state(
                state, self.bindings, submission_state="enabled"
            )

    def test_enabled_phase_requires_quarantined_control_paused(self) -> None:
        state = database_state()
        preflight.validate_database_state(
            state,
            self.bindings,
            submission_state="enabled",
            submission_disabled_accounts=("wallet-6", "wallet-7"),
        )

        for control in state["account_controls"]:
            if control["execution_account_id"] == "wallet-7":
                control["paused"] = False
        with self.assertRaisesRegex(preflight.PreflightError, "to be paused"):
            preflight.validate_database_state(
                state,
                self.bindings,
                submission_state="enabled",
                submission_disabled_accounts=("wallet-6", "wallet-7"),
            )

    def test_accepts_initial_wallet6_wallet7_quarantine_contract(self) -> None:
        state = database_state()
        preflight.validate_database_state(
            state,
            self.bindings,
            submission_state="disabled",
            submission_disabled_accounts=("wallet-6", "wallet-7"),
        )

    def test_rejects_enabled_binding_or_policy_for_quarantined_account(self) -> None:
        state = database_state()
        wallet7_binding = next(
            row for row in state["bindings"] if row["execution_account_id"] == "wallet-7"
        )
        wallet7_binding["enabled"] = True
        with self.assertRaisesRegex(preflight.PreflightError, "active managed-wallet set"):
            preflight.validate_database_state(
                state,
                self.bindings,
                submission_disabled_accounts=("wallet-6", "wallet-7"),
            )

        wallet7_binding["enabled"] = False
        wallet7_policy = next(
            row
            for row in state["risk_policies"]
            if row["execution_account_id"] == "wallet-7"
        )
        wallet7_policy["enabled"] = True
        with self.assertRaisesRegex(preflight.PreflightError, "enabled=False"):
            preflight.validate_database_state(
                state,
                self.bindings,
                submission_disabled_accounts=("wallet-6", "wallet-7"),
            )

    def test_active_wallet67_requires_matching_approved_risk_contract(self) -> None:
        state = database_state()
        activate_accounts(state, "wallet-6")
        with self.assertRaisesRegex(preflight.PreflightError, "risk approvals"):
            preflight.validate_database_state(
                state,
                self.bindings,
                submission_disabled_accounts=("wallet-7",),
            )
        contracts = approved_risk_contracts(state, "wallet-6")
        preflight.validate_database_state(
            state,
            self.bindings,
            submission_disabled_accounts=("wallet-7",),
            approved_risk_contracts=contracts,
        )
        contracts["wallet-6"]["policy_id"] = "disabled-template"
        with self.assertRaisesRegex(preflight.PreflightError, "approved activation artifact"):
            preflight.validate_database_state(
                state,
                self.bindings,
                submission_disabled_accounts=("wallet-7",),
                approved_risk_contracts=contracts,
            )

    def test_accepts_wallet67_only_authorization_partition(self) -> None:
        state = database_state()
        quarantine_accounts(state, "main", "wallet-1")
        activate_accounts(state, "wallet-6", "wallet-7")
        approved_limits = {
            "max_order_notional": 20,
            "max_market_exposure": 40,
            "max_strategy_exposure": 40,
            "max_wallet_exposure": 40,
            "max_daily_traded_notional": 20,
            "max_price_age_ms": 90_000,
            "max_signal_age_ms": 30_000,
            "max_state_age_ms": 600_000,
            "daily_timezone": "UTC",
        }
        for policy in state["risk_policies"]:
            if policy["execution_account_id"] in {"wallet-6", "wallet-7"}:
                policy.update(approved_limits)
                policy["version"] = 2
        contracts = approved_risk_contracts(state, "wallet-6", "wallet-7")
        preflight.validate_database_state(
            state,
            self.bindings,
            submission_state="disabled",
            submission_disabled_accounts=("main", "wallet-1"),
            approved_risk_contracts=contracts,
        )

        state["bindings"][0]["enabled"] = True
        with self.assertRaisesRegex(preflight.PreflightError, "unexpected="):
            preflight.validate_database_state(
                state,
                self.bindings,
                submission_state="disabled",
                submission_disabled_accounts=("main", "wallet-1"),
                approved_risk_contracts=contracts,
            )

    def test_rejects_unapproved_or_invalid_risk_contract(self) -> None:
        drifted = database_state()
        drifted["risk_policies"][0]["max_wallet_exposure"] = 99
        with self.assertRaisesRegex(preflight.PreflightError, "reviewed current-rollout"):
            preflight.validate_database_state(drifted, self.bindings)

        invalid = database_state()
        invalid["risk_policies"][0]["max_order_notional"] = 0
        with self.assertRaisesRegex(preflight.PreflightError, "invalid max_order_notional"):
            preflight.validate_database_state(invalid, self.bindings)

        timezone = database_state()
        timezone["risk_policies"][0]["daily_timezone"] = "Asia/Shanghai"
        with self.assertRaisesRegex(preflight.PreflightError, "daily_timezone UTC"):
            preflight.validate_database_state(timezone, self.bindings)

    def test_retired_rows_remain_auditable_without_blocking_managed_scope(self) -> None:
        state = database_state()
        state["accounts"].append(
            {"execution_account_id": "wallet-2", "wallet_address": "0x22"}
        )
        state["bindings"].append(
            {
                "model_id": "gemini_masked",
                "strategy_id": "multfactor_v1",
                "execution_account_id": "wallet-2",
                "enabled": False,
            }
        )
        state["risk_policies"].append(
            {
                "execution_account_id": "wallet-2",
                "policy_id": "retired-policy",
                "version": 9,
                "enabled": False,
            }
        )
        state["account_controls"].append(
            {
                "execution_account_id": "wallet-2",
                "control_scope": "ACCOUNT",
                "control_key": "",
                "paused": True,
                "reason": "RETIRED",
                "version": 9,
            }
        )
        preflight.validate_database_state(state, self.bindings)

    def test_database_query_scopes_mutating_history_checks_to_managed_accounts(self) -> None:
        sql = preflight.DATABASE_STATE_SQL
        self.assertIn("WITH managed_accounts(execution_account_id)", sql)
        self.assertIn(
            "intent_payload->>'execution_account_id' IN (SELECT execution_account_id FROM managed_accounts)",
            sql,
        )
        self.assertIn(
            "execution_account_id IN (SELECT execution_account_id FROM managed_accounts)",
            sql,
        )

    def test_account_identity_hash_changes_with_wallet_address(self) -> None:
        state = database_state()
        original = preflight.validate_database_state(state, self.bindings)
        state["accounts"][0]["wallet_address"] = "0x11"
        self.assertNotEqual(
            preflight.validate_database_state(state, self.bindings), original
        )

    def test_database_identity_hash_covers_policy_and_control_identity(self) -> None:
        state = database_state()
        original = preflight.validate_database_state(state, self.bindings)

        changed_policy = database_state()
        changed_policy["risk_policies"][0]["version"] = 2
        self.assertNotEqual(
            preflight.validate_database_state(changed_policy, self.bindings), original
        )

        changed_control = database_state()
        changed_control["account_controls"][0]["reason"] = "REVIEWED_READY"
        self.assertNotEqual(
            preflight.validate_database_state(changed_control, self.bindings), original
        )


class EnvironmentTests(unittest.TestCase):
    def write_wallet_file(self, directory: pathlib.Path, accounts: list[str]) -> pathlib.Path:
        path = directory / "wallets.json"
        path.write_text(
            json.dumps(
                {
                    "accounts": [
                        {"execution_account_id": account, "private_key": "not-read-by-preflight"}
                        for account in accounts
                    ]
                }
            ),
            encoding="utf-8",
        )
        path.chmod(0o600)
        return path

    def environment(self, wallet_path: pathlib.Path) -> dict[str, str]:
        return {
            **STATIC_TRADING_ENVIRONMENT,
            "DECISION_CYCLE_ENABLED": "true",
            "DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE": "true",
            "DECISION_CYCLE_ORDER_SUBMISSION_ENABLED": "false",
            "DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED": "false",
            "DECISION_CYCLE_BINDINGS_JSON": json.dumps(VALID_BINDINGS),
            "DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON": '["wallet-6","wallet-7"]',
            "POLYMARKET_ACCOUNTS_FILE": str(wallet_path),
            "TRADING_EXECUTION_DATABASE_URL": "postgres://user:secret@db/trading",
            "DECISION_CYCLE_PREDICTION_LOOKBACK": "3h",
        }

    def test_accepts_complete_environment_with_submission_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            wallet_path = self.write_wallet_file(
                pathlib.Path(temporary), sorted(preflight.EXPECTED_ACCOUNTS)
            )
            bindings = preflight.validate_environment(
                self.environment(wallet_path), submission_state="disabled"
            )
        self.assertEqual(len(bindings), 4)

    def test_source_modes_require_exact_models_and_rollout_modes(self) -> None:
        bindings = preflight.decode_bindings(json.dumps(VALID_BINDINGS))
        self.assertEqual(
            preflight.decode_prediction_model_source_modes(
                json.dumps(SOURCE_MODES), bindings
            ),
            SOURCE_MODES,
        )
        for payload, message in (
            ({"echo-producer-v7": "DIRECT"}, "gemini-3.6-flash"),
            (
                {
                    **SOURCE_MODES,
                    "unconfigured-model": "SANDBOX",
                },
                "unconfigured-model",
            ),
            (
                {
                    "echo-producer-v7": "SANDBOX",
                    "gemini-3.6-flash": "DIRECT",
                },
                "echo source model",
            ),
            (
                {
                    "echo-producer-v7": "direct",
                    "gemini-3.6-flash": "SANDBOX",
                },
                "exactly DIRECT or SANDBOX",
            ),
            (
                {
                    "echo-producer-v7": {"mode": "DIRECT"},
                    "gemini-3.6-flash": "SANDBOX",
                },
                "exactly DIRECT or SANDBOX",
            ),
        ):
            with self.subTest(payload=payload), self.assertRaisesRegex(
                preflight.PreflightError, message
            ):
                preflight.decode_prediction_model_source_modes(
                    json.dumps(payload), bindings
                )
        with self.assertRaisesRegex(preflight.PreflightError, "duplicate model"):
            preflight.decode_prediction_model_source_modes(
                '{"echo-producer-v7":"DIRECT","echo-producer-v7":"DIRECT",'
                '"gemini-3.6-flash":"SANDBOX"}',
                bindings,
            )

    def test_accepts_reviewed_quarantine_cohorts_or_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            wallet_path = self.write_wallet_file(
                pathlib.Path(temporary), sorted(preflight.EXPECTED_ACCOUNTS)
            )
            environment = self.environment(wallet_path)
            for value in (
                '[]',
                '["wallet-6"]',
                '["wallet-7"]',
                '["main","wallet-1"]',
            ):
                environment["DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON"] = value
                with self.subTest(value=value):
                    self.assertEqual(
                        len(
                            preflight.validate_environment(
                                environment, submission_state="disabled"
                            )
                        ),
                        4,
                    )

    def test_rejects_invalid_submission_disabled_accounts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            wallet_path = self.write_wallet_file(
                pathlib.Path(temporary), sorted(preflight.EXPECTED_ACCOUNTS)
            )
            for value, message in (
                ('["wallet-7","wallet-7"]', "duplicates"),
                ('["wallet-missing"]', "unbound accounts"),
                ('["main"]', "reviewed release cohort"),
                ('["main","wallet-6"]', "reviewed release cohort"),
            ):
                environment = self.environment(wallet_path)
                environment["DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON"] = value
                with self.subTest(value=value), self.assertRaisesRegex(
                    preflight.PreflightError, message
                ):
                    preflight.validate_environment(
                        environment, submission_state="disabled"
                    )

    def test_sell_only_preflight_requires_entry_submission_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            wallet_path = self.write_wallet_file(
                pathlib.Path(temporary), sorted(preflight.EXPECTED_ACCOUNTS)
            )
            environment = self.environment(wallet_path)
            with self.assertRaisesRegex(preflight.PreflightError, "ENTRY_SUBMISSION_DISABLED"):
                preflight.validate_environment(
                    environment,
                    submission_state="disabled",
                    entry_submission_state="blocked",
                )
            environment["DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED"] = "true"
            bindings = preflight.validate_environment(
                environment,
                submission_state="disabled",
                entry_submission_state="blocked",
            )
        self.assertEqual(len(bindings), 4)

    def test_rejects_disabled_complete_model_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            wallet_path = self.write_wallet_file(
                pathlib.Path(temporary), sorted(preflight.EXPECTED_ACCOUNTS)
            )
            environment = self.environment(wallet_path)
            environment["DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE"] = "false"
            with self.assertRaisesRegex(preflight.PreflightError, "COMPLETE_MODEL_COVERAGE"):
                preflight.validate_environment(environment, submission_state="disabled")

    def test_requires_explicit_static_risk_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            wallet_path = self.write_wallet_file(
                pathlib.Path(temporary), sorted(preflight.EXPECTED_ACCOUNTS)
            )
            for key in STATIC_TRADING_ENVIRONMENT:
                environment = self.environment(wallet_path)
                environment.pop(key)
                with self.subTest(key=key), self.assertRaisesRegex(
                    preflight.PreflightError, key
                ):
                    preflight.validate_environment(
                        environment, submission_state="disabled"
                    )

    def test_rejects_enabled_static_market_orders(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            wallet_path = self.write_wallet_file(
                pathlib.Path(temporary), sorted(preflight.EXPECTED_ACCOUNTS)
            )
            environment = self.environment(wallet_path)
            environment["EXECUTION_ALLOW_MARKET_ORDERS"] = "true"
            with self.assertRaisesRegex(
                preflight.PreflightError, "EXECUTION_ALLOW_MARKET_ORDERS"
            ):
                preflight.validate_environment(
                    environment, submission_state="disabled"
                )

    def test_static_decimal_validation_is_exact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            wallet_path = self.write_wallet_file(
                pathlib.Path(temporary), sorted(preflight.EXPECTED_ACCOUNTS)
            )
            environment = self.environment(wallet_path)
            environment["POLYMARKET_MAX_BUY_FEE_RATE_BPS"] = "10000.0000000000000001"
            with self.assertRaisesRegex(preflight.PreflightError, "must not exceed 10000"):
                preflight.validate_environment(environment, submission_state="disabled")

            environment = self.environment(wallet_path)
            environment["EXECUTION_MAX_ORDER_SIZE"] = "0.0000000000000001"
            preflight.validate_environment(environment, submission_state="disabled")

    def test_rejects_wallet_subset(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            wallet_path = self.write_wallet_file(pathlib.Path(temporary), ["wallet-6"])
            with self.assertRaisesRegex(preflight.PreflightError, "wallet accounts must be exactly"):
                preflight.validate_environment(
                    self.environment(wallet_path), submission_state="disabled"
                )


class ActivationRiskApprovalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now = dt.datetime.now(dt.timezone.utc)
        self.state = database_state()
        activate_accounts(self.state, "wallet-6")
        self.approval = activation_risk_approval(
            self.now, self.state, "wallet-6"
        )

    def test_accepts_independent_exact_activation_artifact(self) -> None:
        contracts = preflight.validate_activation_risk_approval(
            self.approval,
            active_accounts=frozenset({"wallet-6"}),
            expected_trading_commit=TRADING_COMMIT,
            now=self.now,
            max_age=dt.timedelta(hours=1),
        )
        self.assertEqual(set(contracts), {"wallet-6"})
        self.assertEqual(contracts["wallet-6"]["policy_id"], "policy-wallet-6")

    def test_rejects_disabled_evidence_as_risk_approval(self) -> None:
        reused = dict(self.approval)
        reused["schema_version"] = preflight.DISABLED_EVIDENCE_SCHEMA
        with self.assertRaisesRegex(
            preflight.PreflightError, "disabled-phase evidence cannot authorize"
        ):
            preflight.validate_activation_risk_approval(
                reused,
                active_accounts=frozenset({"wallet-6"}),
                expected_trading_commit=TRADING_COMMIT,
                now=self.now,
                max_age=dt.timedelta(hours=1),
            )

    def test_rejects_wrong_account_or_stale_approval(self) -> None:
        with self.assertRaisesRegex(preflight.PreflightError, "account set differs"):
            preflight.validate_activation_risk_approval(
                self.approval,
                active_accounts=frozenset({"wallet-6", "wallet-7"}),
                expected_trading_commit=TRADING_COMMIT,
                now=self.now,
                max_age=dt.timedelta(hours=1),
            )
        stale = dict(self.approval)
        stale["approved_at"] = iso(self.now - dt.timedelta(days=2))
        with self.assertRaisesRegex(preflight.PreflightError, "approved_at is stale"):
            preflight.validate_activation_risk_approval(
                stale,
                active_accounts=frozenset({"wallet-6"}),
                expected_trading_commit=TRADING_COMMIT,
                now=self.now,
                max_age=dt.timedelta(hours=1),
            )

    def test_reader_requires_external_digest_and_protected_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "risk-approval.json"
            payload = (json.dumps(self.approval, sort_keys=True) + "\n").encode()
            path.write_bytes(payload)
            path.chmod(0o600)
            digest = preflight.hashlib.sha256(payload).hexdigest()
            self.assertEqual(
                preflight.read_activation_risk_approval(path, digest), self.approval
            )
            with self.assertRaisesRegex(preflight.PreflightError, "differs"):
                preflight.read_activation_risk_approval(path, "0" * 64)


class PredictionEnvironmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.bindings = preflight.decode_bindings(json.dumps(VALID_BINDINGS))
        self.environment = {
            "REDIS_ENABLED": "true",
            "REDIS_ADDRESS": "127.0.0.1:6379",
            "REDIS_DATABASE": "2",
            "REDIS_PASSWORD": "",
            "DATABASE_URL": "postgres://prediction:secret@db/prediction",
            "DIRECT_PREDICTION_ENABLED": "true",
            "PREDICTION_RESULT_ENABLED": "true",
            "REDIS_DIRECT_PREDICTION_STREAM_KEY": "prediction_pm_directprediction",
            "DIRECT_PREDICTION_MODEL_IDS_JSON": '["echo-producer-v7"]',
            "TRADING_INPUT_ENABLED": "true",
        }

    def validate(self) -> None:
        preflight.validate_prediction_environment(
            self.environment, self.bindings, SOURCE_MODES
        )

    def test_accepts_exact_direct_source_mode_subset(self) -> None:
        self.validate()

    def test_rejects_different_direct_model_set(self) -> None:
        self.environment["DIRECT_PREDICTION_MODEL_IDS_JSON"] = '["gemini-3.6-flash"]'
        with self.assertRaisesRegex(preflight.PreflightError, "DIRECT source-mode subset"):
            self.validate()

    def test_rejects_sandbox_model_in_direct_set(self) -> None:
        self.environment["DIRECT_PREDICTION_MODEL_IDS_JSON"] = (
            '["echo-producer-v7","gemini-3.6-flash"]'
        )
        with self.assertRaisesRegex(preflight.PreflightError, "gemini-3.6-flash"):
            self.validate()

    def test_requires_explicit_redis_identity(self) -> None:
        self.environment.pop("REDIS_DATABASE")
        with self.assertRaisesRegex(preflight.PreflightError, "REDIS_DATABASE"):
            self.validate()

    def test_rejects_disabled_prediction_result_callback(self) -> None:
        self.environment["PREDICTION_RESULT_ENABLED"] = "false"
        with self.assertRaisesRegex(preflight.PreflightError, "PREDICTION_RESULT_ENABLED"):
            self.validate()


class RuntimeEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now = dt.datetime.now(dt.timezone.utc)
        self.trading_environment = {
            **STATIC_TRADING_ENVIRONMENT,
            "DECISION_CYCLE_ENABLED": "true",
            "DECISION_CYCLE_ORDER_SUBMISSION_ENABLED": "false",
            "DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE": "true",
            "DECISION_CYCLE_BINDINGS_JSON": json.dumps(VALID_BINDINGS, separators=(",", ":")),
            "POLYMARKET_ACCOUNTS_FILE": "/run/secrets/wallets.json",
            "DECISION_CYCLE_PREDICTION_INFRA_URL": "http://127.0.0.1:11000",
            "DECISION_CYCLE_PREDICTION_LOOKBACK": "3h",
            "TRADING_EXECUTION_DATABASE_URL": "postgres://trading:secret@db/trading",
            "EXECUTION_API_TOKEN": "execution-token",
            "POSITION_EXIT_JOB_TOKEN": "job-token",
            "LIVE_OPERATIONS_READ_ONLY_TOKEN": "read-only-token",
            "DECISION_CYCLE_PREDICTION_INFRA_TOKEN": "snapshot-token",
            "DECISION_CYCLE_STRATEGY_TOKEN": "strategy-token",
        }
        self.prediction_environment = {
            "REDIS_ENABLED": "true",
            "DIRECT_PREDICTION_ENABLED": "true",
            "REDIS_DIRECT_PREDICTION_STREAM_KEY": "prediction_pm_directprediction",
            "DIRECT_PREDICTION_MODEL_IDS_JSON": '["echo-producer-v7"]',
            "PREDICTION_RESULT_ENABLED": "true",
            "TRADING_INPUT_ENABLED": "true",
            "REDIS_ADDRESS": "redis.internal:6379",
            "REDIS_DATABASE": "2",
            "REDIS_PASSWORD": "redis-secret",
            "DATABASE_URL": "postgres://prediction:secret@db/prediction",
            "TRADING_INPUT_TOKEN": "snapshot-token",
            "PREDICTION_RESULT_TOKEN": "result-token",
        }
        self.state = {
            "observed_at": iso(self.now),
            "trading": {
                "pid": 101,
                "started_at": iso(self.now - dt.timedelta(minutes=20)),
                "environment": {
                    key: self.trading_environment[key]
                    for key in preflight.TRADING_RUNTIME_KEYS
                    if key in self.trading_environment
                },
                "secret_sha256": {
                    key: preflight._credential_sha256(
                        key, self.trading_environment[key]
                    )
                    for key in preflight.TRADING_RUNTIME_SECRET_KEYS
                },
                "health": {"status": "ok", "commit": TRADING_COMMIT},
            },
            "prediction": {
                "pid": 202,
                "started_at": iso(self.now - dt.timedelta(minutes=20)),
                "environment": {
                    key: self.prediction_environment[key]
                    for key in preflight.PREDICTION_RUNTIME_KEYS
                    if key in self.prediction_environment
                },
                "secret_sha256": {
                    key: preflight._credential_sha256(
                        key, self.prediction_environment[key]
                    )
                    for key in preflight.PREDICTION_RUNTIME_SECRET_KEYS
                },
                "health": {"status": "up", "version": PREDICTION_COMMIT},
            },
        }

    def validate(self) -> dt.datetime:
        return preflight.validate_runtime_state(
            self.state,
            self.trading_environment,
            self.prediction_environment,
            expected_trading_commit=TRADING_COMMIT,
            expected_prediction_version=PREDICTION_COMMIT,
            now=self.now,
            max_age=dt.timedelta(minutes=2),
        )

    def test_accepts_actual_process_identity_and_environment(self) -> None:
        self.assertEqual(self.validate(), self.now - dt.timedelta(minutes=20))

    def test_rejects_prediction_redis_address_mismatch(self) -> None:
        self.state["prediction"]["environment"]["REDIS_ADDRESS"] = "other-redis:6379"
        with self.assertRaisesRegex(preflight.PreflightError, "REDIS_ADDRESS"):
            self.validate()

    def test_rejects_prediction_redis_database_mismatch(self) -> None:
        self.state["prediction"]["environment"]["REDIS_DATABASE"] = "3"
        with self.assertRaisesRegex(preflight.PreflightError, "REDIS_DATABASE"):
            self.validate()

    def test_rejects_trading_lookback_mismatch(self) -> None:
        self.state["trading"]["environment"]["DECISION_CYCLE_PREDICTION_LOOKBACK"] = "1h"
        with self.assertRaisesRegex(preflight.PreflightError, "PREDICTION_LOOKBACK"):
            self.validate()

    def test_rejects_prediction_source_mode_runtime_drift(self) -> None:
        self.state["trading"]["environment"][
            "DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON"
        ] = '{"gemini-3.6-flash":"SANDBOX","echo-producer-v7":"DIRECT"}'
        with self.assertRaisesRegex(
            preflight.PreflightError,
            "DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON",
        ):
            self.validate()

    def test_rejects_static_risk_runtime_drift(self) -> None:
        for key, drifted_value in STATIC_TRADING_DRIFT.items():
            original = self.state["trading"]["environment"][key]
            self.state["trading"]["environment"][key] = drifted_value
            with self.subTest(key=key), self.assertRaisesRegex(
                preflight.PreflightError, key
            ):
                self.validate()
            self.state["trading"]["environment"][key] = original

    def test_rejects_trading_database_fingerprint_mismatch_without_url(self) -> None:
        self.state["trading"]["secret_sha256"]["TRADING_EXECUTION_DATABASE_URL"] = (
            preflight._credential_sha256(
                "TRADING_EXECUTION_DATABASE_URL",
                "postgres://trading:secret@other-db/other",
            )
        )
        with self.assertRaises(preflight.PreflightError) as raised:
            self.validate()
        message = str(raised.exception)
        self.assertIn("TRADING_EXECUTION_DATABASE_URL", message)
        self.assertNotIn("postgres://", message)
        self.assertNotIn("other-db", message)

    def test_rejects_strategy_token_fingerprint_mismatch_without_token(self) -> None:
        self.state["trading"]["secret_sha256"]["DECISION_CYCLE_STRATEGY_TOKEN"] = (
            preflight._credential_sha256("DECISION_CYCLE_STRATEGY_TOKEN", "wrong-token")
        )
        with self.assertRaises(preflight.PreflightError) as raised:
            self.validate()
        self.assertIn("DECISION_CYCLE_STRATEGY_TOKEN", str(raised.exception))
        self.assertNotIn("wrong-token", str(raised.exception))

    def test_rejects_process_change_during_enabled_preflight(self) -> None:
        final_state = json.loads(json.dumps(self.state))
        final_state["trading"]["pid"] = 303
        with self.assertRaisesRegex(preflight.PreflightError, "process changed"):
            preflight.validate_runtime_processes_unchanged(self.state, final_state)
        final_state = json.loads(json.dumps(self.state))
        final_state["prediction"]["started_at"] = iso(
            self.now - dt.timedelta(minutes=10)
        )
        with self.assertRaisesRegex(preflight.PreflightError, "process changed"):
            preflight.validate_runtime_processes_unchanged(self.state, final_state)


class DryRunEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now = dt.datetime.now(dt.timezone.utc)
        self.bindings = preflight.decode_bindings(json.dumps(VALID_BINDINGS))
        self.state = database_state()
        self.state["dry_runs"] = [
            {
                "decision_at": iso(self.now - dt.timedelta(minutes=10)),
                "created_at": iso(self.now - dt.timedelta(minutes=9)),
                "prediction_snapshot_id": "snapshot-1",
                "bindings": dry_run_rows(),
            }
        ]

    def validate(self) -> dict[str, object]:
        return preflight.validate_dry_run_state(
            self.state,
            self.bindings,
            not_before=self.now - dt.timedelta(minutes=20),
            now=self.now,
            max_age=dt.timedelta(minutes=30),
        )

    def validate_sell_only(self) -> dict[str, object]:
        return preflight.validate_dry_run_state(
            self.state,
            self.bindings,
            not_before=self.now - dt.timedelta(minutes=20),
            now=self.now,
            max_age=dt.timedelta(minutes=30),
            entry_submission_state="blocked",
        )

    def validate_wallet7_quarantine(self) -> dict[str, object]:
        return preflight.validate_dry_run_state(
            self.state,
            self.bindings,
            submission_disabled_accounts=("wallet-7",),
            not_before=self.now - dt.timedelta(minutes=20),
            now=self.now,
            max_age=dt.timedelta(minutes=30),
        )

    def test_accepts_legacy_healthy_output_without_entry_policy(self) -> None:
        self.assertEqual(self.validate()["prediction_snapshot_id"], "snapshot-1")

    def test_accepts_exact_three_active_rows_when_wallet7_is_quarantined(self) -> None:
        self.state["dry_runs"][0]["bindings"] = [
            row
            for row in dry_run_rows()
            if row["execution_account_id"] != "wallet-7"
        ]
        self.assertEqual(
            self.validate_wallet7_quarantine()["prediction_snapshot_id"],
            "snapshot-1",
        )

    def test_rejects_quarantined_wallet_row_in_dry_run(self) -> None:
        with self.assertRaisesRegex(preflight.PreflightError, "exactly 3 active"):
            self.validate_wallet7_quarantine()

    def test_rejects_missing_active_row_when_wallet7_is_quarantined(self) -> None:
        self.state["dry_runs"][0]["bindings"] = [
            row
            for row in dry_run_rows()
            if row["execution_account_id"] not in {"wallet-6", "wallet-7"}
        ]
        with self.assertRaisesRegex(preflight.PreflightError, "exactly 3 active"):
            self.validate_wallet7_quarantine()

    def test_accepts_explicit_enabled_entry_policy(self) -> None:
        self.state["dry_runs"][0]["bindings"] = dry_run_rows(True, "")
        self.validate()

    def test_rejects_explicit_disabled_policy_even_without_reason(self) -> None:
        self.state["dry_runs"][0]["bindings"] = dry_run_rows(False, "")
        with self.assertRaisesRegex(preflight.PreflightError, "entry block"):
            self.validate()

    def test_rejects_coverage_block_reason(self) -> None:
        self.state["dry_runs"][0]["bindings"] = dry_run_rows(
            False, "INCOMPLETE_MODEL_COVERAGE"
        )
        with self.assertRaisesRegex(preflight.PreflightError, "entry block"):
            self.validate()

    def test_accepts_exact_operator_policy_in_sell_only_mode(self) -> None:
        self.state["dry_runs"][0]["bindings"] = dry_run_rows(
            False, "ENTRY_SUBMISSION_DISABLED"
        )
        self.assertEqual(
            self.validate_sell_only()["prediction_snapshot_id"], "snapshot-1"
        )

    def test_rejects_missing_operator_policy_in_sell_only_mode(self) -> None:
        with self.assertRaisesRegex(preflight.PreflightError, "exact sell-only"):
            self.validate_sell_only()

    def test_rejects_wrong_operator_reason_in_sell_only_mode(self) -> None:
        self.state["dry_runs"][0]["bindings"] = dry_run_rows(
            False, "INCOMPLETE_MODEL_COVERAGE"
        )
        with self.assertRaisesRegex(preflight.PreflightError, "exact sell-only"):
            self.validate_sell_only()

    def test_rejects_invalid_entry_policy_type_in_allowed_mode(self) -> None:
        self.state["dry_runs"][0]["bindings"] = dry_run_rows("true", "")
        with self.assertRaisesRegex(preflight.PreflightError, "entry block"):
            self.validate()


class SnapshotManifestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.decision_at = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=10)
        self.bindings = preflight.decode_bindings(json.dumps(VALID_BINDINGS))
        self.active_bindings = preflight.active_rollout_bindings(
            self.bindings, ("wallet-6", "wallet-7")
        )
        self.wallet67_bindings = preflight.active_rollout_bindings(
            self.bindings, ("main", "wallet-1")
        )
        self.dry_run = {
            "decision_at": iso(self.decision_at),
            "prediction_snapshot_id": "snapshot-1",
        }
        self.snapshot = complete_snapshot(self.decision_at, "snapshot-1")

    def validate(
        self, bindings: tuple[preflight.Binding, ...] | None = None
    ) -> int:
        return preflight.validate_snapshot_manifest(
            self.snapshot,
            self.dry_run,
            self.bindings if bindings is None else bindings,
            SOURCE_MODES,
            dt.timedelta(hours=3),
        )

    def test_accepts_direct_manifest_and_fresh_sandbox_result(self) -> None:
        self.assertEqual(
            self.validate(), 1
        )

    def test_direct_identity_uses_go_trim_and_utc_instant_semantics(self) -> None:
        expectation = self.snapshot["data"]["expected_predictions"][0]
        expectation["condition_id"] = " condition-1 "
        expectation["outcomes"][0]["name"] = " Yes "
        expectation["outcomes"][0]["token_id"] = " yes-token "
        prediction_as_of = self.decision_at - dt.timedelta(minutes=12)
        expectation["prediction_as_of"] = prediction_as_of.astimezone(
            dt.timezone(dt.timedelta(hours=1))
        ).isoformat()
        result_available_at = self.decision_at - dt.timedelta(minutes=1)
        expectation["result_available_at"] = result_available_at.astimezone(
            dt.timezone(dt.timedelta(hours=-1))
        ).isoformat()
        self.assertEqual(self.validate(self.active_bindings), 1)

    def test_rejects_each_direct_result_identity_tamper(self) -> None:
        def mutate_outcome_name(result: dict[str, object]) -> None:
            result["outcomes"][0]["name"] = "Maybe"

        def mutate_outcome_token(result: dict[str, object]) -> None:
            result["outcomes"][0]["token_id"] = "other-token"

        cases = (
            (
                "source_job_id",
                lambda result: result.__setitem__(
                    "source_job_id", " pm-direct:1 "
                ),
            ),
            (
                "market_id",
                lambda result: result.__setitem__("market_id", "other-market"),
            ),
            (
                "condition_id",
                lambda result: result.__setitem__(
                    "condition_id", "other-condition"
                ),
            ),
            (
                "model",
                lambda result: result["model"].__setitem__(
                    "name", "other-direct-model"
                ),
            ),
            (
                "sandbox_id",
                lambda result: result.__setitem__("sandbox_id", "sandbox-echo"),
            ),
            (
                "prediction_as_of",
                lambda result: result.__setitem__(
                    "prediction_as_of",
                    iso(self.decision_at - dt.timedelta(minutes=13)),
                ),
            ),
            ("outcomes", mutate_outcome_name),
            ("outcomes", mutate_outcome_token),
            (
                "result_available_at",
                lambda result: result.__setitem__(
                    "available_at",
                    iso(self.decision_at - dt.timedelta(seconds=30)),
                ),
            ),
        )
        for expected_field, mutate in cases:
            with self.subTest(expected_field=expected_field):
                self.snapshot = complete_snapshot(self.decision_at, "snapshot-1")
                direct_result = self.snapshot["data"]["predictions"][0]
                mutate(direct_result)
                with self.assertRaisesRegex(
                    preflight.PreflightError, expected_field
                ):
                    self.validate(self.active_bindings)

    def test_rejects_malformed_direct_expectation_fields(self) -> None:
        def set_one_outcome(expectation: dict[str, object]) -> None:
            expectation["outcomes"] = expectation["outcomes"][:1]

        def set_duplicate_token(expectation: dict[str, object]) -> None:
            expectation["outcomes"][1]["token_id"] = " yes-token "

        def set_invalid_index(expectation: dict[str, object]) -> None:
            expectation["outcomes"][0]["index"] = "0"

        def set_task_before_prediction(expectation: dict[str, object]) -> None:
            expectation["task_available_at"] = iso(
                self.decision_at - dt.timedelta(minutes=13)
            )

        def set_result_before_task(expectation: dict[str, object]) -> None:
            expectation["result_available_at"] = iso(
                self.decision_at - dt.timedelta(minutes=11, seconds=30)
            )

        cases = (
            (
                "source_job_id",
                lambda expectation: expectation.__setitem__("source_job_id", ""),
            ),
            (
                "prediction_model_id",
                lambda expectation: expectation.__setitem__(
                    "prediction_model_id", ""
                ),
            ),
            (
                "market_id",
                lambda expectation: expectation.__setitem__("market_id", ""),
            ),
            (
                "condition_id",
                lambda expectation: expectation.__setitem__("condition_id", ""),
            ),
            (
                "selection identity",
                lambda expectation: expectation.__setitem__("selection_id", True),
            ),
            (
                "selection identity",
                lambda expectation: expectation.__setitem__(
                    "selection_run_id", 0
                ),
            ),
            ("exactly two outcomes", set_one_outcome),
            ("invalid index", set_invalid_index),
            (
                "invalid name",
                lambda expectation: expectation["outcomes"][0].__setitem__(
                    "name", ""
                ),
            ),
            (
                "invalid token_id",
                lambda expectation: expectation["outcomes"][0].__setitem__(
                    "token_id", ""
                ),
            ),
            ("duplicate token ids", set_duplicate_token),
            ("before prediction_as_of", set_task_before_prediction),
            ("before its task", set_result_before_task),
        )
        for expected_message, mutate in cases:
            with self.subTest(expected_message=expected_message):
                self.snapshot = complete_snapshot(self.decision_at, "snapshot-1")
                expectation = self.snapshot["data"]["expected_predictions"][0]
                mutate(expectation)
                with self.assertRaisesRegex(
                    preflight.PreflightError, expected_message
                ):
                    self.validate(self.active_bindings)

    def test_rejects_pending_direct_task(self) -> None:
        self.snapshot["data"]["expected_predictions"][0]["status"] = "PENDING"
        self.snapshot["data"]["expected_predictions"][0].pop(
            "result_available_at"
        )
        with self.assertRaisesRegex(preflight.PreflightError, "not COMPLETED"):
            self.validate()

    def test_rejects_missing_direct_manifest(self) -> None:
        self.snapshot["data"]["expected_predictions"] = []
        with self.assertRaisesRegex(preflight.PreflightError, "no configured Direct"):
            self.validate()

    def test_rejects_direct_expectation_for_sandbox_model(self) -> None:
        expectation = dict(self.snapshot["data"]["expected_predictions"][0])
        expectation["prediction_id"] = "pred-sandbox-2"
        expectation["source_job_id"] = "pm-direct:gemini"
        expectation["prediction_model_id"] = "gemini-3.6-flash"
        self.snapshot["data"]["expected_predictions"].append(expectation)
        with self.assertRaisesRegex(preflight.PreflightError, "must not have a Direct"):
            self.validate()

    def test_rejects_sandbox_result_for_direct_manifest(self) -> None:
        self.snapshot["data"]["predictions"][0]["sandbox_id"] = "sandbox-echo"
        with self.assertRaisesRegex(preflight.PreflightError, "sandbox_id"):
            self.validate()

    def test_rejects_missing_or_stale_sandbox_result(self) -> None:
        self.snapshot["data"]["predictions"] = self.snapshot["data"]["predictions"][:1]
        with self.assertRaisesRegex(preflight.PreflightError, "no fresh effective SANDBOX"):
            self.validate()
        self.snapshot = complete_snapshot(self.decision_at, "snapshot-1")
        sandbox = self.snapshot["data"]["predictions"][1]
        sandbox["prediction_as_of"] = iso(self.decision_at - dt.timedelta(hours=4))
        sandbox["completed_at"] = iso(self.decision_at - dt.timedelta(hours=3, minutes=30))
        sandbox["available_at"] = iso(self.decision_at - dt.timedelta(hours=3, minutes=20))
        with self.assertRaisesRegex(preflight.PreflightError, "no fresh effective SANDBOX"):
            self.validate()

    def test_rejects_future_sandbox_result(self) -> None:
        self.snapshot["data"]["predictions"][1]["available_at"] = iso(
            self.decision_at + dt.timedelta(seconds=1)
        )
        with self.assertRaisesRegex(preflight.PreflightError, "not PIT-visible"):
            self.validate()

    def test_sandbox_mode_ignores_newer_direct_result(self) -> None:
        newer = prediction_result(
            self.decision_at,
            prediction_id="pred-gemini-direct-newer",
            model_id="gemini-3.6-flash",
            sandbox_id="",
        )
        newer["prediction_as_of"] = iso(self.decision_at - dt.timedelta(minutes=5))
        newer["completed_at"] = iso(self.decision_at - dt.timedelta(seconds=90))
        newer["available_at"] = iso(self.decision_at - dt.timedelta(minutes=1))
        self.snapshot["data"]["predictions"].append(newer)
        self.assertEqual(self.validate(), 1)

    def test_direct_mode_ignores_newer_sandbox_result(self) -> None:
        newer = prediction_result(
            self.decision_at,
            prediction_id="pred-echo-sandbox-newer",
            model_id="echo-producer-v7",
            sandbox_id="sandbox-echo",
        )
        newer["prediction_as_of"] = iso(self.decision_at - dt.timedelta(minutes=5))
        newer["completed_at"] = iso(self.decision_at - dt.timedelta(seconds=90))
        newer["available_at"] = iso(self.decision_at - dt.timedelta(minutes=1))
        self.snapshot["data"]["predictions"].append(newer)
        self.assertEqual(self.validate(), 1)

    def test_rejects_equal_timestamp_ambiguous_sandbox_revisions(self) -> None:
        conflicting = json.loads(
            json.dumps(self.snapshot["data"]["predictions"][1])
        )
        conflicting["prediction_id"] = "pred-sandbox-conflict"
        conflicting["source_job_id"] = "job-conflict"
        conflicting["outcomes"][0]["probability"] = 0.6
        conflicting["outcomes"][1]["probability"] = 0.4
        self.snapshot["data"]["predictions"].append(conflicting)
        with self.assertRaisesRegex(preflight.PreflightError, "ambiguous revisions"):
            self.validate()

    def test_equivalent_delivery_duplicates_are_order_independent(self) -> None:
        duplicate = json.loads(json.dumps(self.snapshot["data"]["predictions"][1]))
        duplicate["prediction_id"] = "pred-sandbox-z"
        duplicate["source_job_id"] = "job-z"
        duplicate["sandbox_id"] = "sandbox-z"
        self.snapshot["data"]["predictions"].append(duplicate)
        self.assertEqual(self.validate(), 1)
        self.snapshot["data"]["predictions"].reverse()
        self.assertEqual(self.validate(), 1)

    def test_rejects_cross_model_market_identity_conflict(self) -> None:
        self.snapshot["data"]["predictions"][1]["condition_id"] = "other-condition"
        with self.assertRaisesRegex(preflight.PreflightError, "conflicting market identity"):
            self.validate()

    def test_quarantine_does_not_require_sandbox_liveness(self) -> None:
        self.snapshot["data"]["predictions"] = self.snapshot["data"]["predictions"][:1]
        self.assertEqual(self.validate(self.active_bindings), 1)

    def test_wallet67_only_cohort_does_not_require_direct_manifest_liveness(self) -> None:
        self.snapshot["data"]["expected_predictions"] = []
        self.snapshot["data"]["predictions"] = self.snapshot["data"]["predictions"][1:]
        self.assertEqual(self.validate(self.wallet67_bindings), 0)


class ConsumerEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now = dt.datetime.now(dt.timezone.utc)
        self.environment = {
            "REDIS_DIRECT_PREDICTION_STREAM_KEY": "prediction_pm_directprediction"
        }
        self.state = consumer_state(self.now)

    def validate(self) -> None:
        preflight.validate_consumer_state(
            self.state,
            self.environment,
            model_groups=MODEL_GROUPS,
            now=self.now,
            max_age=dt.timedelta(minutes=2),
            max_idle=dt.timedelta(minutes=15),
        )

    def test_requires_current_drained_group_for_every_model(self) -> None:
        self.validate()
        self.state["groups"][0]["lag"] = 1
        with self.assertRaisesRegex(preflight.PreflightError, "undelivered"):
            self.validate()

    def test_rejects_missing_model_group_evidence(self) -> None:
        self.state["groups"] = []
        with self.assertRaisesRegex(preflight.PreflightError, "every configured model"):
            self.validate()

    def test_rejects_pending_consumer_detail(self) -> None:
        self.state["groups"][0]["consumer_details"][0]["pending"] = 1
        with self.assertRaisesRegex(preflight.PreflightError, "pending tasks"):
            self.validate()

    def test_rejects_sandbox_model_group(self) -> None:
        bindings = preflight.decode_bindings(json.dumps(VALID_BINDINGS))
        with self.assertRaisesRegex(preflight.PreflightError, "gemini-3.6-flash"):
            preflight.decode_model_groups(
                json.dumps(
                    {
                        **MODEL_GROUPS,
                        "gemini-3.6-flash": "predict-gemini-v1",
                    }
                ),
                bindings,
                SOURCE_MODES,
            )

    def test_rejects_duplicate_direct_model_group_key(self) -> None:
        bindings = preflight.decode_bindings(json.dumps(VALID_BINDINGS))
        with self.assertRaisesRegex(preflight.PreflightError, "duplicates model"):
            preflight.decode_model_groups(
                '{"echo-producer-v7":"predict-echo-a",'
                '"echo-producer-v7":"predict-echo-b"}',
                bindings,
                SOURCE_MODES,
            )

    def test_rejects_mapping_missing_a_source_model(self) -> None:
        bindings = preflight.decode_bindings(json.dumps(VALID_BINDINGS))
        with self.assertRaisesRegex(preflight.PreflightError, "echo-producer-v7"):
            preflight.decode_model_groups(
                json.dumps({}), bindings, SOURCE_MODES
            )

    def test_current_rollout_checks_only_direct_echo_consumer(self) -> None:
        bindings = preflight.decode_bindings(json.dumps(VALID_BINDINGS))
        model_groups = preflight.decode_model_groups(
            json.dumps(MODEL_GROUPS),
            bindings,
            SOURCE_MODES,
        )
        self.assertEqual(model_groups, {"echo-producer-v7": "predict-echo-v1"})

        state = consumer_state(self.now)
        state["model_groups"] = model_groups
        state["groups"] = [
            group
            for group in state["groups"]
            if group["model_id"] == "echo-producer-v7"
        ]
        preflight.validate_consumer_state(
            state,
            self.environment,
            model_groups=model_groups,
            now=self.now,
            max_age=dt.timedelta(minutes=2),
            max_idle=dt.timedelta(minutes=15),
        )

        with self.assertRaisesRegex(preflight.PreflightError, "echo-producer-v7"):
            preflight.decode_model_groups(
                json.dumps({"gemini-3.6-flash": "predict-gemini-v1"}),
                bindings,
                SOURCE_MODES,
            )


class DeliveryStateTests(unittest.TestCase):
    def aggregate_state(self) -> dict[str, object]:
        state = database_state()
        state.pop("decision_deliveries")
        state.pop("manual_review_orders")
        state["decision_delivery_state"] = {
            "count": 100_000,
            "status_counts": {
                "PENDING": 0,
                "SUBMITTING": 0,
                "SUBMITTED": 99_000,
                "FAILED": 1_000,
                "UNKNOWN": 0,
            },
            "unsafe_order_status_count": 0,
            "max_updated_at": "2026-08-21T00:00:00Z",
            "risky_samples": [],
        }
        state["manual_review_state"] = {"count": 0, "orders": []}
        state["nonterminal_buy_state"] = {"count": 0, "orders": []}
        return state

    def test_accepts_bounded_terminal_summary_without_loading_history(self) -> None:
        watermark = preflight.validate_delivery_state(self.aggregate_state())
        self.assertEqual(watermark["count"], 100_000)
        self.assertEqual(watermark["non_terminal_counts"]["PENDING"], 0)

    def test_rejects_each_recoverable_or_unknown_delivery_count_without_samples(self) -> None:
        for status in ("PENDING", "SUBMITTING", "UNKNOWN"):
            with self.subTest(status=status):
                state = self.aggregate_state()
                counts = state["decision_delivery_state"]["status_counts"]
                counts["SUBMITTED"] -= 1
                counts[status] += 1
                with self.assertRaisesRegex(preflight.PreflightError, "not drained"):
                    preflight.validate_delivery_state(state)

    def test_rejects_unsafe_delivery_order_status_count(self) -> None:
        state = self.aggregate_state()
        state["decision_delivery_state"]["unsafe_order_status_count"] = 1
        with self.assertRaisesRegex(preflight.PreflightError, "not drained"):
            preflight.validate_delivery_state(state)

    def test_rejects_execution_manual_review_count(self) -> None:
        state = self.aggregate_state()
        state["manual_review_state"] = {"count": 1, "orders": []}
        with self.assertRaisesRegex(preflight.PreflightError, "explicit"):
            preflight.validate_delivery_state(state)

    def test_rejects_nonterminal_buy_execution_order(self) -> None:
        state = self.aggregate_state()
        state["nonterminal_buy_state"] = {
            "count": 1,
            "orders": [{"order_id": "buy-1", "status": "RESERVED"}],
        }
        with self.assertRaisesRegex(preflight.PreflightError, "zero nonterminal BUY"):
            preflight.validate_delivery_state(state, entry_submission_state="blocked")

    def test_allows_nonterminal_buy_execution_order_outside_sell_only_mode(self) -> None:
        state = self.aggregate_state()
        state["nonterminal_buy_state"] = {
            "count": 1,
            "orders": [{"order_id": "buy-1", "status": "LIVE"}],
        }
        watermark = preflight.validate_delivery_state(
            state, entry_submission_state="allowed"
        )
        self.assertEqual(
            watermark["non_terminal_counts"]["NONTERMINAL_BUY_EXECUTION_ORDER"], 1
        )

    def test_rejects_inconsistent_count_or_missing_nonempty_watermark(self) -> None:
        state = self.aggregate_state()
        state["decision_delivery_state"]["count"] += 1
        with self.assertRaisesRegex(preflight.PreflightError, "do not equal"):
            preflight.validate_delivery_state(state)
        state = self.aggregate_state()
        state["decision_delivery_state"]["max_updated_at"] = None
        with self.assertRaisesRegex(preflight.PreflightError, "max_updated_at"):
            preflight.validate_delivery_state(state)


class CredentialAndConfigurationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.bindings = preflight.decode_bindings(json.dumps(VALID_BINDINGS))
        self.trading = {
            **STATIC_TRADING_ENVIRONMENT,
            "TRADING_EXECUTION_DATABASE_URL": "postgres://trading:secret@db/trading",
            "EXECUTION_API_TOKEN": "execution-token",
            "POSITION_EXIT_JOB_TOKEN": "job-token",
            "LIVE_OPERATIONS_READ_ONLY_TOKEN": "read-only-token",
            "DECISION_CYCLE_PREDICTION_INFRA_TOKEN": "snapshot-token",
            "DECISION_CYCLE_STRATEGY_TOKEN": "strategy-token",
            "DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE": "true",
            "DECISION_CYCLE_ORDER_SUBMISSION_ENABLED": "false",
        }
        self.prediction = {
            "DATABASE_URL": "postgres://prediction:secret@db/prediction",
            "REDIS_PASSWORD": "redis-secret",
            "TRADING_INPUT_TOKEN": "snapshot-token",
            "PREDICTION_RESULT_TOKEN": "result-token",
            "PREDICTION_RESULT_ENABLED": "true",
        }

    def configuration_hash(self) -> str:
        return preflight.configuration_sha256(
            self.trading,
            self.prediction,
            self.bindings,
            MODEL_GROUPS,
            approved_trading_commit=TRADING_COMMIT,
            approved_prediction_version=PREDICTION_COMMIT,
            wallet_file_sha256="f" * 64,
        )

    def test_cross_service_snapshot_tokens_must_match(self) -> None:
        preflight.validate_cross_service_credentials(self.trading, self.prediction)
        self.prediction["TRADING_INPUT_TOKEN"] = "different"
        with self.assertRaisesRegex(preflight.PreflightError, "does not match"):
            preflight.validate_cross_service_credentials(self.trading, self.prediction)

    def test_configuration_hash_binds_database_token_wallet_and_release(self) -> None:
        original = self.configuration_hash()
        self.trading["TRADING_EXECUTION_DATABASE_URL"] = (
            "postgres://trading:secret@other-db/trading"
        )
        self.assertNotEqual(self.configuration_hash(), original)
        self.trading["TRADING_EXECUTION_DATABASE_URL"] = (
            "postgres://trading:secret@db/trading"
        )
        self.prediction["PREDICTION_RESULT_TOKEN"] = "rotated-result-token"
        self.assertNotEqual(self.configuration_hash(), original)
        self.prediction["PREDICTION_RESULT_TOKEN"] = "result-token"
        changed_wallet = preflight.configuration_sha256(
            self.trading,
            self.prediction,
            self.bindings,
            MODEL_GROUPS,
            approved_trading_commit=TRADING_COMMIT,
            approved_prediction_version=PREDICTION_COMMIT,
            wallet_file_sha256="0" * 64,
        )
        self.assertNotEqual(changed_wallet, original)
        changed_release = preflight.configuration_sha256(
            self.trading,
            self.prediction,
            self.bindings,
            MODEL_GROUPS,
            approved_trading_commit=TRADING_COMMIT,
            approved_prediction_version="c" * 40,
            wallet_file_sha256="f" * 64,
        )
        self.assertNotEqual(changed_release, original)

    def test_configuration_hash_binds_dedicated_job_token(self) -> None:
        original = self.configuration_hash()
        self.trading["POSITION_EXIT_JOB_TOKEN"] = "rotated-job-token"
        self.assertNotEqual(self.configuration_hash(), original)

    def test_configuration_hash_binds_static_risk_environment(self) -> None:
        original = self.configuration_hash()
        for key, drifted_value in STATIC_TRADING_DRIFT.items():
            original_value = self.trading[key]
            self.trading[key] = drifted_value
            with self.subTest(key=key):
                self.assertNotEqual(self.configuration_hash(), original)
            self.trading[key] = original_value

    def test_configuration_hash_binds_prediction_source_modes(self) -> None:
        original = self.configuration_hash()
        self.trading["DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON"] = (
            '{"gemini-3.6-flash":"SANDBOX","echo-producer-v7":"DIRECT"}'
        )
        self.assertNotEqual(self.configuration_hash(), original)

    def test_subsecond_redis_timeout_is_not_truncated_to_zero(self) -> None:
        self.assertEqual(preflight._duration_seconds("500ms", "timeout"), 0.5)


class PostgreSQLConnectionTests(unittest.TestCase):
    def test_password_stays_in_protected_passfile(self) -> None:
        child, pgpass_path = preflight._pg_environment(
            "postgres://deploy:sec%3Aret@db.example:5433/trading?sslmode=require"
        )
        self.assertIsNotNone(pgpass_path)
        assert pgpass_path is not None
        try:
            self.assertNotIn("PGPASSWORD", child)
            self.assertEqual(child["PGHOST"], "db.example")
            self.assertEqual(child["PGPORT"], "5433")
            self.assertEqual(child["PGSSLMODE"], "require")
            self.assertEqual(pgpass_path.stat().st_mode & 0o777, 0o600)
            self.assertIn(r"sec\:ret", pgpass_path.read_text(encoding="utf-8"))
        finally:
            pgpass_path.unlink(missing_ok=True)

    def test_rejects_unsupported_connection_parameter(self) -> None:
        with self.assertRaisesRegex(preflight.PreflightError, "unsupported"):
            preflight._pg_environment("postgres://deploy@db/trading?options=-csearch_path%3Dother")


class MainWiringTests(unittest.TestCase):
    def write_json(self, directory: pathlib.Path, name: str, value: object) -> pathlib.Path:
        path = directory / name
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def write_environment(
        self, directory: pathlib.Path, name: str, values: dict[str, str]
    ) -> pathlib.Path:
        path = directory / name
        path.write_text(
            "\n".join(f"{key}='{value}'" for key, value in values.items()) + "\n",
            encoding="utf-8",
        )
        return path

    def test_main_rejects_reusable_prediction_version_before_reading_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            error = io.StringIO()
            with contextlib.redirect_stderr(error):
                result = preflight.main(
                    [
                        "--env-file",
                        str(directory / "missing-trading.env"),
                        "--prediction-env-file",
                        str(directory / "missing-prediction.env"),
                        "--expected-trading-commit",
                        TRADING_COMMIT,
                        "--expected-prediction-version",
                        "dev",
                        "--direct-model-groups-json",
                        json.dumps(MODEL_GROUPS),
                        "--write-disabled-evidence",
                        str(directory / "evidence.json"),
                    ]
                )
        self.assertEqual(result, 1)
        self.assertIn("immutable", error.getvalue())

    def test_enabled_offline_mode_requires_independent_final_database_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            error = io.StringIO()
            with contextlib.redirect_stderr(error):
                result = preflight.main(
                    [
                        "--submission-state",
                        "enabled",
                        "--disabled-evidence-json",
                        str(directory / "disabled.json"),
                        "--database-state-json",
                        str(directory / "initial.json"),
                        "--expected-trading-commit",
                        TRADING_COMMIT,
                        "--expected-prediction-version",
                        PREDICTION_COMMIT,
                        "--direct-model-groups-json",
                        json.dumps(MODEL_GROUPS),
                    ]
                )
        self.assertEqual(result, 1)
        self.assertIn("independent --final-database-state-json", error.getvalue())

    def test_captured_due_is_not_rolled_forward_when_preflight_crosses_it(self) -> None:
        started_at = dt.datetime(2026, 8, 21, 12, 9, 30, tzinfo=dt.timezone.utc)
        finished_at = dt.datetime(2026, 8, 21, 12, 10, 1, tzinfo=dt.timezone.utc)
        environment = {
            "DECISION_CYCLE_INTERVAL": "10m",
            "DECISION_CYCLE_STARTUP_DELAY": "0s",
        }
        captured_due = preflight.next_decision_due(started_at, environment)
        self.assertEqual(
            captured_due,
            dt.datetime(2026, 8, 21, 12, 10, tzinfo=dt.timezone.utc),
        )
        self.assertEqual(
            preflight.next_decision_due(finished_at, environment),
            dt.datetime(2026, 8, 21, 12, 20, tzinfo=dt.timezone.utc),
        )
        with self.assertRaisesRegex(preflight.PreflightError, "captured decision due"):
            preflight.activation_deadline(
                captured_due,
                finished_at,
                safety_seconds=60,
            )

    def test_rejects_process_whose_first_due_precedes_preflight_entry(self) -> None:
        environment = {
            "DECISION_CYCLE_INTERVAL": "10m",
            "DECISION_CYCLE_STARTUP_DELAY": "15s",
        }
        process_started_at = dt.datetime(
            2026, 8, 21, 12, 0, 10, tzinfo=dt.timezone.utc
        )
        preflight_started_at = dt.datetime(
            2026, 8, 21, 12, 0, 16, tzinfo=dt.timezone.utc
        )
        captured_due = preflight.next_decision_due(
            preflight_started_at, environment
        )
        with self.assertRaisesRegex(preflight.PreflightError, "already have started"):
            preflight.bind_process_activation_due(
                captured_due,
                process_started_at,
                preflight_started_at,
                environment,
            )

    def test_main_requires_and_validates_all_runtime_evidence(self) -> None:
        now = dt.datetime.now(dt.timezone.utc)
        decision_at = now - dt.timedelta(minutes=10)
        trading_commit = TRADING_COMMIT
        prediction_commit = PREDICTION_COMMIT
        startup_delay = (int(now.timestamp()) % 600 + 300) % 600
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            wallet_path = directory / "wallets.json"
            wallet_path.write_text(
                json.dumps(
                    {
                        "accounts": [
                            {"execution_account_id": account}
                            for account in sorted(preflight.EXPECTED_ACCOUNTS)
                        ]
                    }
                ),
                encoding="utf-8",
            )
            wallet_path.chmod(0o600)
            compact_bindings = json.dumps(VALID_BINDINGS, separators=(",", ":"))
            trading_environment = {
                **STATIC_TRADING_ENVIRONMENT,
                "DECISION_CYCLE_ENABLED": "true",
                "DECISION_CYCLE_ORDER_SUBMISSION_ENABLED": "false",
                "DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED": "false",
                "DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE": "true",
                "DECISION_CYCLE_BINDINGS_JSON": compact_bindings,
                "DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON": '["wallet-6","wallet-7"]',
                "POLYMARKET_ACCOUNTS_FILE": str(wallet_path),
                "TRADING_EXECUTION_DATABASE_URL": "postgres://user:secret@db/trading",
                "EXECUTION_API_TOKEN": "execution-token",
                "POSITION_EXIT_JOB_TOKEN": "job-token",
                "LIVE_OPERATIONS_READ_ONLY_TOKEN": "read-only-token",
                "DECISION_CYCLE_PREDICTION_INFRA_URL": "http://127.0.0.1:11000",
                "DECISION_CYCLE_PREDICTION_INFRA_TOKEN": "snapshot-token",
                "DECISION_CYCLE_PREDICTION_LOOKBACK": "3h",
                "DECISION_CYCLE_STRATEGY_TOKEN": "strategy-token",
                "DECISION_CYCLE_INTERVAL": "10m",
                "DECISION_CYCLE_STARTUP_DELAY": f"{startup_delay}s",
            }
            prediction_environment = {
                "REDIS_ENABLED": "true",
                "REDIS_ADDRESS": "redis.internal:6379",
                "REDIS_DATABASE": "2",
                "REDIS_PASSWORD": "not-written-to-runtime-evidence",
                "DATABASE_URL": "postgres://prediction:secret@db/prediction",
                "REDIS_DIAL_TIMEOUT": "5s",
                "DIRECT_PREDICTION_ENABLED": "true",
                "REDIS_DIRECT_PREDICTION_STREAM_KEY": "prediction_pm_directprediction",
                "DIRECT_PREDICTION_MODEL_IDS_JSON": '["echo-producer-v7"]',
                "PREDICTION_RESULT_ENABLED": "true",
                "TRADING_INPUT_ENABLED": "true",
                "TRADING_INPUT_TOKEN": "snapshot-token",
                "PREDICTION_RESULT_TOKEN": "result-token",
            }
            trading_env_path = self.write_environment(
                directory, "trading.env", trading_environment
            )
            prediction_env_path = self.write_environment(
                directory, "prediction.env", prediction_environment
            )

            runtime_state = {
                "observed_at": iso(now - dt.timedelta(seconds=3)),
                "trading": {
                    "pid": 101,
                    "started_at": iso(now - dt.timedelta(minutes=20)),
                    "environment": {
                        key: trading_environment[key]
                        for key in preflight.TRADING_RUNTIME_KEYS
                        if key in trading_environment
                    },
                    "secret_sha256": {
                        key: preflight._credential_sha256(key, trading_environment[key])
                        for key in preflight.TRADING_RUNTIME_SECRET_KEYS
                    },
                    "health": {"status": "ok", "commit": trading_commit},
                },
                "prediction": {
                    "pid": 202,
                    "started_at": iso(now - dt.timedelta(minutes=20)),
                    "environment": {
                        key: prediction_environment[key]
                        for key in preflight.PREDICTION_RUNTIME_KEYS
                        if key in prediction_environment
                    },
                    "secret_sha256": {
                        key: preflight._credential_sha256(key, prediction_environment[key])
                        for key in preflight.PREDICTION_RUNTIME_SECRET_KEYS
                    },
                    "health": {"status": "up", "version": prediction_commit},
                },
            }
            state = database_state()
            state["observed_at"] = iso(now - dt.timedelta(seconds=2))
            state["dry_runs"] = [
                {
                    "decision_at": iso(decision_at),
                    "created_at": iso(decision_at + dt.timedelta(minutes=1)),
                    "prediction_snapshot_id": "snapshot-1",
                    "bindings": [
                        row
                        for row in dry_run_rows()
                        if row["execution_account_id"] in {"main", "wallet-1"}
                    ],
                }
            ]
            direct_consumer_state = consumer_state(now - dt.timedelta(seconds=1))
            direct_consumer_state["model_groups"] = {
                "echo-producer-v7": "predict-echo-v1"
            }
            direct_consumer_state["groups"] = [direct_consumer_state["groups"][0]]
            runtime_path = self.write_json(directory, "runtime.json", runtime_state)
            database_path = self.write_json(directory, "database.json", state)
            snapshot_path = self.write_json(
                directory,
                "snapshot.json",
                complete_snapshot(decision_at, "snapshot-1"),
            )
            consumer_path = self.write_json(directory, "consumer.json", direct_consumer_state)
            disabled_evidence_path = directory / "disabled-evidence.json"
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                result = preflight.main(
                    [
                        "--env-file",
                        str(trading_env_path),
                        "--prediction-env-file",
                        str(prediction_env_path),
                        "--database-state-json",
                        str(database_path),
                        "--runtime-state-json",
                        str(runtime_path),
                        "--prediction-snapshot-json",
                        str(snapshot_path),
                        "--consumer-state-json",
                        str(consumer_path),
                        "--expected-trading-commit",
                        trading_commit,
                        "--expected-prediction-version",
                        prediction_commit,
                        "--direct-model-groups-json",
                        json.dumps(MODEL_GROUPS),
                        "--write-disabled-evidence",
                        str(disabled_evidence_path),
                    ]
                )
            self.assertEqual(result, 0)
            self.assertIn("manifest_markets=1", output.getvalue())
            self.assertEqual(disabled_evidence_path.stat().st_mode & 0o777, 0o600)

            trading_environment["DECISION_CYCLE_ORDER_SUBMISSION_ENABLED"] = "true"
            self.write_environment(directory, "trading.env", trading_environment)
            runtime_state["trading"]["pid"] = 303
            runtime_state["trading"]["started_at"] = iso(now - dt.timedelta(seconds=30))
            runtime_state["trading"]["environment"][
                "DECISION_CYCLE_ORDER_SUBMISSION_ENABLED"
            ] = "true"
            self.write_json(directory, "runtime.json", runtime_state)
            final_runtime_state = json.loads(json.dumps(runtime_state))
            final_runtime_state["observed_at"] = iso(dt.datetime.now(dt.timezone.utc))
            final_runtime_path = self.write_json(
                directory, "final-runtime.json", final_runtime_state
            )
            final_state = json.loads(json.dumps(state))
            final_state["observed_at"] = iso(dt.datetime.now(dt.timezone.utc))
            final_database_path = self.write_json(
                directory, "final-database.json", final_state
            )
            enabled_args = [
                "--env-file",
                str(trading_env_path),
                "--prediction-env-file",
                str(prediction_env_path),
                "--submission-state",
                "enabled",
                "--disabled-evidence-json",
                str(disabled_evidence_path),
                "--database-state-json",
                str(database_path),
                "--final-database-state-json",
                str(final_database_path),
                "--runtime-state-json",
                str(runtime_path),
                "--final-runtime-state-json",
                str(final_runtime_path),
                "--prediction-snapshot-json",
                str(snapshot_path),
                "--consumer-state-json",
                str(consumer_path),
                "--expected-trading-commit",
                trading_commit,
                "--expected-prediction-version",
                prediction_commit,
                "--direct-model-groups-json",
                json.dumps(MODEL_GROUPS),
            ]
            enabled_output = io.StringIO()
            with contextlib.redirect_stdout(enabled_output):
                enabled_result = preflight.main(enabled_args)
            self.assertEqual(enabled_result, 0)
            self.assertIn("submission_state=enabled", enabled_output.getvalue())

            final_state["observed_at"] = iso(dt.datetime.now(dt.timezone.utc))
            final_state["dry_runs"] = []
            self.write_json(directory, "final-database.json", final_state)
            deleted_error = io.StringIO()
            with contextlib.redirect_stderr(deleted_error):
                deleted_result = preflight.main(enabled_args)
            self.assertEqual(deleted_result, 1)
            self.assertIn("no qualifying submission=false", deleted_error.getvalue())


class DisabledPhaseEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now = dt.datetime.now(dt.timezone.utc)
        self.dry_run = {
            "decision_at": iso(self.now - dt.timedelta(minutes=10)),
            "prediction_snapshot_id": "snapshot-1",
        }
        self.evidence = preflight.build_disabled_evidence(
            now=self.now,
            expected_trading_commit=TRADING_COMMIT,
            expected_prediction_version=PREDICTION_COMMIT,
            model_groups=MODEL_GROUPS,
            configuration_sha="c" * 64,
            dry_run=self.dry_run,
            manifest_markets=1,
            delivery_watermark=delivery_watermark(),
            database_account_identity_sha256="e" * 64,
            runtime_state={
                "trading": {
                    "pid": 101,
                    "started_at": iso(self.now - dt.timedelta(minutes=20)),
                }
            },
        )

    def validate(self) -> dict[str, object]:
        return preflight.validate_disabled_evidence(
            self.evidence,
            now=self.now,
            max_age=dt.timedelta(minutes=30),
            expected_trading_commit=TRADING_COMMIT,
            expected_prediction_version=PREDICTION_COMMIT,
            model_groups=MODEL_GROUPS,
            configuration_sha="c" * 64,
            delivery_watermark=delivery_watermark(),
            database_account_identity_sha256="e" * 64,
        )

    def test_accepts_checksum_and_same_non_submission_configuration(self) -> None:
        self.assertEqual(self.validate(), self.dry_run)

    def test_rejects_configuration_change_between_phases(self) -> None:
        with self.assertRaisesRegex(preflight.PreflightError, "configuration differs"):
            preflight.validate_disabled_evidence(
                self.evidence,
                now=self.now,
                max_age=dt.timedelta(minutes=30),
                expected_trading_commit=TRADING_COMMIT,
                expected_prediction_version=PREDICTION_COMMIT,
                model_groups=MODEL_GROUPS,
                configuration_sha="d" * 64,
                delivery_watermark=delivery_watermark(),
                database_account_identity_sha256="e" * 64,
            )

    def test_rejects_tampered_evidence(self) -> None:
        self.evidence["manifest_markets"] = 2
        with self.assertRaisesRegex(preflight.PreflightError, "checksum"):
            self.validate()

    def test_rejects_delivery_watermark_change_between_phases(self) -> None:
        changed = delivery_watermark()
        changed["count"] = 1
        with self.assertRaisesRegex(preflight.PreflightError, "delivery state changed"):
            preflight.validate_disabled_evidence(
                self.evidence,
                now=self.now,
                max_age=dt.timedelta(minutes=30),
                expected_trading_commit=TRADING_COMMIT,
                expected_prediction_version=PREDICTION_COMMIT,
                model_groups=MODEL_GROUPS,
                configuration_sha="c" * 64,
                delivery_watermark=changed,
                database_account_identity_sha256="e" * 64,
            )

    def test_rejects_database_wallet_address_change_between_phases(self) -> None:
        with self.assertRaisesRegex(preflight.PreflightError, "address identity changed"):
            preflight.validate_disabled_evidence(
                self.evidence,
                now=self.now,
                max_age=dt.timedelta(minutes=30),
                expected_trading_commit=TRADING_COMMIT,
                expected_prediction_version=PREDICTION_COMMIT,
                model_groups=MODEL_GROUPS,
                configuration_sha="c" * 64,
                delivery_watermark=delivery_watermark(),
                database_account_identity_sha256="f" * 64,
            )

    def test_rejects_submission_enabled_cycle_after_evidence(self) -> None:
        state = database_state()
        state["dry_runs"] = [
            {
                "decision_at": iso(self.now),
                "prediction_snapshot_id": "snapshot-2",
                "bindings": [
                    {
                        "order_submission_enabled": True,
                    }
                ],
            }
        ]
        with self.assertRaisesRegex(preflight.PreflightError, "submission=true"):
            preflight.reject_enabled_cycles_after_evidence(state, self.dry_run)

    def test_rejects_any_later_cycle_even_if_submission_flag_is_false(self) -> None:
        state = database_state()
        state["dry_runs"] = [
            {
                "decision_at": iso(self.now),
                "prediction_snapshot_id": "snapshot-2",
                "bindings": [{"order_submission_enabled": False}],
            }
        ]
        with self.assertRaisesRegex(preflight.PreflightError, "incomplete-or-unexpected"):
            preflight.reject_enabled_cycles_after_evidence(state, self.dry_run)


if __name__ == "__main__":
    unittest.main()
