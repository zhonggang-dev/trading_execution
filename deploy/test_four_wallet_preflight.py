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
MODEL_GROUPS = {
    "echo-producer-v7": "predict-echo-v1",
    "gemini-3.6-flash": "predict-gemini-v1",
}
TRADING_COMMIT = "a" * 40
PREDICTION_COMMIT = "b" * 40


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
    expectations = []
    predictions = []
    for index, model_id in enumerate(("echo-producer-v7", "gemini-3.6-flash"), start=1):
        prediction_id = f"pred-direct-{index}"
        expectations.append(
            {
                "prediction_id": prediction_id,
                "source_job_id": f"pm-direct:{index}",
                "prediction_model_id": model_id,
                "selection_id": 101,
                "selection_run_id": 41,
                "market_id": "market-1",
                "condition_id": "condition-1",
                "prediction_as_of": iso(decision_at - dt.timedelta(minutes=12)),
                "task_available_at": iso(decision_at - dt.timedelta(minutes=11)),
                "status": "COMPLETED",
                "result_available_at": iso(decision_at - dt.timedelta(minutes=1)),
            }
        )
        predictions.append(
            {
                "prediction_id": prediction_id,
                "market_id": "market-1",
                "model": {"name": model_id},
            }
        )
    return {
        "data": {
            "snapshot_id": snapshot_id,
            "decision_at": iso(decision_at),
            "expected_predictions": expectations,
            "predictions": predictions,
        }
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

    def test_accepts_wallet67_quarantine_subset_or_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            wallet_path = self.write_wallet_file(
                pathlib.Path(temporary), sorted(preflight.EXPECTED_ACCOUNTS)
            )
            environment = self.environment(wallet_path)
            for value in ('[]', '["wallet-6"]', '["wallet-7"]'):
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
                ('["main"]', "permits only wallet-6 and wallet-7"),
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
        self.active_bindings = preflight.active_rollout_bindings(
            self.bindings, ("wallet-6", "wallet-7")
        )
        self.environment = {
            "REDIS_ENABLED": "true",
            "REDIS_ADDRESS": "127.0.0.1:6379",
            "REDIS_DATABASE": "2",
            "REDIS_PASSWORD": "",
            "DATABASE_URL": "postgres://prediction:secret@db/prediction",
            "DIRECT_PREDICTION_ENABLED": "true",
            "PREDICTION_RESULT_ENABLED": "true",
            "REDIS_DIRECT_PREDICTION_STREAM_KEY": "prediction_pm_directprediction",
            "DIRECT_PREDICTION_MODEL_IDS_JSON": json.dumps(
                ["echo-producer-v7", "gemini-3.6-flash"]
            ),
            "TRADING_INPUT_ENABLED": "true",
        }

    def test_accepts_exact_trading_source_model_set(self) -> None:
        preflight.validate_prediction_environment(self.environment, self.bindings)

    def test_rejects_different_direct_model_set(self) -> None:
        self.environment["DIRECT_PREDICTION_MODEL_IDS_JSON"] = '["gemini-3.6-flash"]'
        with self.assertRaisesRegex(preflight.PreflightError, "include every active"):
            preflight.validate_prediction_environment(self.environment, self.bindings)

    def test_current_rollout_allows_missing_quarantined_gemini_model(self) -> None:
        self.environment["DIRECT_PREDICTION_MODEL_IDS_JSON"] = '["echo-producer-v7"]'
        preflight.validate_prediction_environment(
            self.environment,
            self.bindings,
            required_bindings=self.active_bindings,
        )

    def test_current_rollout_rejects_missing_active_echo_model(self) -> None:
        self.environment["DIRECT_PREDICTION_MODEL_IDS_JSON"] = '["gemini-3.6-flash"]'
        with self.assertRaisesRegex(preflight.PreflightError, "echo-producer-v7"):
            preflight.validate_prediction_environment(
                self.environment,
                self.bindings,
                required_bindings=self.active_bindings,
            )

    def test_requires_explicit_redis_identity(self) -> None:
        self.environment.pop("REDIS_DATABASE")
        with self.assertRaisesRegex(preflight.PreflightError, "REDIS_DATABASE"):
            preflight.validate_prediction_environment(self.environment, self.bindings)

    def test_rejects_disabled_prediction_result_callback(self) -> None:
        self.environment["PREDICTION_RESULT_ENABLED"] = "false"
        with self.assertRaisesRegex(preflight.PreflightError, "PREDICTION_RESULT_ENABLED"):
            preflight.validate_prediction_environment(self.environment, self.bindings)


class RuntimeEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now = dt.datetime.now(dt.timezone.utc)
        self.trading_environment = {
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
            "DIRECT_PREDICTION_MODEL_IDS_JSON": '["echo-producer-v7","gemini-3.6-flash"]',
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
        self.dry_run = {
            "decision_at": iso(self.decision_at),
            "prediction_snapshot_id": "snapshot-1",
        }
        self.snapshot = complete_snapshot(self.decision_at, "snapshot-1")

    def test_accepts_same_market_two_model_completed_manifest(self) -> None:
        self.assertEqual(
            preflight.validate_snapshot_manifest(self.snapshot, self.dry_run, self.bindings), 1
        )

    def test_rejects_pending_model_task(self) -> None:
        self.snapshot["data"]["expected_predictions"][1]["status"] = "PENDING"
        with self.assertRaisesRegex(preflight.PreflightError, "not COMPLETED"):
            preflight.validate_snapshot_manifest(self.snapshot, self.dry_run, self.bindings)

    def test_rejects_market_missing_one_model(self) -> None:
        self.snapshot["data"]["expected_predictions"] = self.snapshot["data"][
            "expected_predictions"
        ][:1]
        with self.assertRaisesRegex(preflight.PreflightError, "incomplete same-market"):
            preflight.validate_snapshot_manifest(self.snapshot, self.dry_run, self.bindings)

    def test_rejects_cross_model_selection_id_mismatch(self) -> None:
        self.snapshot["data"]["expected_predictions"][1]["selection_id"] = 102
        with self.assertRaisesRegex(preflight.PreflightError, "mixes generations"):
            preflight.validate_snapshot_manifest(self.snapshot, self.dry_run, self.bindings)

    def test_current_rollout_ignores_missing_quarantined_gemini_manifest(self) -> None:
        self.snapshot["data"]["expected_predictions"] = self.snapshot["data"][
            "expected_predictions"
        ][:1]
        self.snapshot["data"]["predictions"] = self.snapshot["data"]["predictions"][:1]
        self.assertEqual(
            preflight.validate_snapshot_manifest(
                self.snapshot, self.dry_run, self.active_bindings
            ),
            1,
        )

    def test_current_rollout_rejects_missing_active_echo_manifest(self) -> None:
        self.snapshot["data"]["expected_predictions"] = self.snapshot["data"][
            "expected_predictions"
        ][1:]
        self.snapshot["data"]["predictions"] = self.snapshot["data"]["predictions"][1:]
        with self.assertRaisesRegex(preflight.PreflightError, "no configured"):
            preflight.validate_snapshot_manifest(
                self.snapshot, self.dry_run, self.active_bindings
            )


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
        self.state["groups"][1]["lag"] = 1
        with self.assertRaisesRegex(preflight.PreflightError, "undelivered"):
            self.validate()

    def test_rejects_missing_model_group_evidence(self) -> None:
        self.state["groups"] = self.state["groups"][:1]
        with self.assertRaisesRegex(preflight.PreflightError, "every configured model"):
            self.validate()

    def test_rejects_pending_consumer_detail(self) -> None:
        self.state["groups"][0]["consumer_details"][0]["pending"] = 1
        with self.assertRaisesRegex(preflight.PreflightError, "pending tasks"):
            self.validate()

    def test_rejects_shared_group_for_model_specific_consumers(self) -> None:
        bindings = preflight.decode_bindings(json.dumps(VALID_BINDINGS))
        shared = {model_id: "shared-single-model-workers" for model_id in MODEL_GROUPS}
        with self.assertRaisesRegex(preflight.PreflightError, "must be distinct"):
            preflight.decode_model_groups(json.dumps(shared), bindings)

    def test_rejects_mapping_missing_a_source_model(self) -> None:
        bindings = preflight.decode_bindings(json.dumps(VALID_BINDINGS))
        with self.assertRaisesRegex(preflight.PreflightError, "include every active"):
            preflight.decode_model_groups(
                json.dumps({"echo-producer-v7": "predict-echo-v1"}), bindings
            )

    def test_current_rollout_checks_only_active_echo_consumer(self) -> None:
        bindings = preflight.decode_bindings(json.dumps(VALID_BINDINGS))
        active_bindings = preflight.active_rollout_bindings(
            bindings, ("wallet-6", "wallet-7")
        )
        model_groups = preflight.decode_model_groups(
            json.dumps(MODEL_GROUPS),
            bindings,
            required_bindings=active_bindings,
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
                required_bindings=active_bindings,
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
                "DIRECT_PREDICTION_MODEL_IDS_JSON": '["echo-producer-v7","gemini-3.6-flash"]',
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
