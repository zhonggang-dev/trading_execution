from __future__ import annotations

import contextlib
import copy
import hashlib
import io
import json
import os
import pathlib
import stat
import sys
import tempfile
import unittest
from unittest import mock


DEPLOY_DIR = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(DEPLOY_DIR))

import wallet67_onboarding as onboarding  # noqa: E402


def _hash(seed: int) -> str:
    return f"0x{seed:064x}"


def _address(seed: int) -> str:
    return f"0x{seed:040x}"


def _transfer(
    *,
    wallet: str,
    seed: int,
    block_number: int,
    timestamp: str,
    amount_base_units: int,
    transaction_index: int = 1,
    log_index: int = 0,
) -> dict[str, object]:
    return {
        "amount_base_units": str(amount_base_units),
        "block_hash": _hash(seed + 10_000),
        "block_number": block_number,
        "block_timestamp": timestamp,
        "confirmations": 128,
        "finalized": True,
        "from_address": _address(seed + 100),
        "log_index": log_index,
        "receipt_status": 1,
        "to_address": wallet,
        "token_address": onboarding.PUSD_ADDRESS,
        "transaction_hash": _hash(seed),
        "transaction_index": transaction_index,
    }


def _account(
    account_id: str,
    *,
    transfers: list[dict[str, object]],
    expected_version: int,
) -> dict[str, object]:
    wallet = onboarding.EXPECTED_WALLETS[account_id]
    before_block = int(transfers[0]["block_number"]) - 1
    after_block = int(transfers[-1]["block_number"]) + 1
    target = sum(int(str(item["amount_base_units"])) for item in transfers)
    return {
        "balance_evidence": {
            "after_base_units": str(target),
            "after_block_finalized": True,
            "after_block_hash": _hash(after_block + 20_000),
            "after_block_number": after_block,
            "before_base_units": "0",
            "before_block_finalized": True,
            "before_block_hash": _hash(before_block + 20_000),
            "before_block_number": before_block,
            "observed_at": "2026-08-24T03:00:00Z",
            "source": "POLYGON_RPC_ETH_CALL",
        },
        "execution_account_id": account_id,
        "expected_database": {
            "available_balance": "0",
            "reconciled_at": "2026-08-24T00:00:00Z",
            "reserved_balance": "0",
            "total_balance": "0",
            "version": expected_version,
        },
        "funding_transfers": transfers,
        "transfer_scan": {
            "from_block_number": before_block + 1,
            "inbound_log_count": len(transfers),
            "observed_at": "2026-08-24T03:05:00Z",
            "outbound_log_count": 0,
            "pagination_complete": True,
            "source": "POLYGON_RPC_ETH_GET_LOGS",
            "to_block_number": after_block,
        },
        "wallet_address": wallet,
    }


def valid_evidence() -> dict[str, object]:
    wallet6 = onboarding.EXPECTED_WALLETS["wallet-6"]
    wallet7 = onboarding.EXPECTED_WALLETS["wallet-7"]
    wallet6_transfers = [
        _transfer(
            wallet=wallet6,
            seed=1,
            block_number=101,
            timestamp="2026-08-24T01:00:00Z",
            amount_base_units=100_000_000,
        ),
        _transfer(
            wallet=wallet6,
            seed=2,
            block_number=102,
            timestamp="2026-08-24T01:01:00Z",
            amount_base_units=101_000_000,
            transaction_index=2,
            log_index=3,
        ),
    ]
    wallet7_transfers = [
        _transfer(
            wallet=wallet7,
            seed=3,
            block_number=201,
            timestamp="2026-08-24T02:00:00Z",
            amount_base_units=221_000_000,
        )
    ]
    return {
        "accounts": [
            _account("wallet-6", transfers=wallet6_transfers, expected_version=7),
            _account("wallet-7", transfers=wallet7_transfers, expected_version=9),
        ],
        "chain_id": 137,
        "collateral": {
            "asset": "pUSD",
            "decimals": 6,
            "token_address": onboarding.PUSD_ADDRESS,
        },
        "generated_at": "2026-08-24T04:00:00Z",
        "schema_version": onboarding.SCHEMA_VERSION,
        "wallet6_position_baseline": {
            "actor": "wallet67-onboarding",
            "include_archived": True,
            "observed_at": "2026-08-24T03:10:00Z",
            "pagination_complete": True,
            "positions": [
                {
                    "condition_id": "condition-a",
                    "neg_risk": False,
                    "outcome_index": 0,
                    "outcome_name": "No",
                    "shares": "1.25",
                    "token_id": "100",
                },
                {
                    "condition_id": "condition-b",
                    "neg_risk": True,
                    "outcome_index": 1,
                    "outcome_name": "Yes",
                    "shares": "2",
                    "token_id": "200",
                },
            ],
            "post_reconciled_trades": {
                "observed_at": "2026-08-24T03:20:00Z",
                "pagination_complete": True,
                "query_after": "2026-08-24T00:00:00Z",
                "source": "POLYMARKET_CLOB_DATA_TRADES",
                "unattributed_count": 0,
            },
            "reason": "Seal all current wallet-6 positions as unmanaged",
            "size_threshold": "0",
            "source": "POLYMARKET_DATA_API",
        },
    }


class EvidenceFileMixin:
    def write_evidence(
        self,
        directory: str,
        value: object,
        *,
        mode: int = 0o600,
        canonical: bool = True,
    ) -> pathlib.Path:
        path = pathlib.Path(directory) / "evidence.json"
        payload = (
            onboarding.canonical_json(value)
            if canonical
            else (json.dumps(value, indent=2) + "\n").encode("utf-8")
        )
        path.write_bytes(payload)
        path.chmod(mode)
        return path


class EvidenceValidationTests(EvidenceFileMixin, unittest.TestCase):
    def test_accepts_variable_actual_balances_and_multiple_logs(self) -> None:
        value = valid_evidence()
        with tempfile.TemporaryDirectory() as directory:
            evidence = onboarding.read_evidence(self.write_evidence(directory, value))

        accounts = evidence.account_by_id
        self.assertEqual(accounts["wallet-6"].target_base_units, 201_000_000)
        self.assertEqual(accounts["wallet-7"].target_base_units, 221_000_000)
        self.assertEqual(len(accounts["wallet-6"].transfers), 2)
        self.assertEqual(
            evidence.sha256,
            hashlib.sha256(onboarding.canonical_json(value)).hexdigest(),
        )

    def test_rejects_noncanonical_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_evidence(directory, valid_evidence(), canonical=False)
            with self.assertRaisesRegex(onboarding.OnboardingError, "not canonical"):
                onboarding.read_evidence(path)

    def test_rejects_non0600_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_evidence(directory, valid_evidence(), mode=0o640)
            with self.assertRaisesRegex(onboarding.OnboardingError, "exactly 0600"):
                onboarding.read_evidence(path)

    def test_rejects_sum_mismatch(self) -> None:
        value = valid_evidence()
        value["accounts"][0]["balance_evidence"]["after_base_units"] = "201000001"
        with self.assertRaisesRegex(onboarding.OnboardingError, "do not sum"):
            onboarding.parse_evidence(value, "0" * 64)

    def test_rejects_outbound_transfer_in_bootstrap_interval(self) -> None:
        value = valid_evidence()
        value["accounts"][1]["transfer_scan"]["outbound_log_count"] = 1
        with self.assertRaisesRegex(onboarding.OnboardingError, "outbound pUSD"):
            onboarding.parse_evidence(value, "0" * 64)

    def test_rejects_incomplete_transfer_scan(self) -> None:
        value = valid_evidence()
        value["accounts"][0]["transfer_scan"]["pagination_complete"] = False
        with self.assertRaisesRegex(onboarding.OnboardingError, "must be true"):
            onboarding.parse_evidence(value, "0" * 64)

    def test_rejects_transfer_before_preserved_reconciliation(self) -> None:
        value = valid_evidence()
        value["accounts"][0]["funding_transfers"][0]["block_timestamp"] = (
            "2026-08-23T23:59:59Z"
        )
        with self.assertRaisesRegex(onboarding.OnboardingError, "before the preserved"):
            onboarding.parse_evidence(value, "0" * 64)

    def test_rejects_transfer_on_preserved_reconciliation_boundary(self) -> None:
        value = valid_evidence()
        value["accounts"][0]["funding_transfers"][0]["block_timestamp"] = (
            "2026-08-24T00:00:00Z"
        )
        with self.assertRaisesRegex(onboarding.OnboardingError, "before the preserved"):
            onboarding.parse_evidence(value, "0" * 64)

    def test_rejects_contradictory_finalized_block_identity(self) -> None:
        value = valid_evidence()
        first = value["accounts"][0]["funding_transfers"][0]
        second = value["accounts"][0]["funding_transfers"][1]
        second["block_number"] = first["block_number"]
        second["block_hash"] = _hash(999_999)
        with self.assertRaisesRegex(onboarding.OnboardingError, "identity of a block"):
            onboarding.parse_evidence(value, "0" * 64)

    def test_rejects_cross_account_duplicate_log(self) -> None:
        value = valid_evidence()
        duplicate = copy.deepcopy(value["accounts"][0]["funding_transfers"][0])
        duplicate["to_address"] = onboarding.EXPECTED_WALLETS["wallet-7"]
        duplicate["amount_base_units"] = "221000000"
        duplicate["block_number"] = 201
        duplicate["block_timestamp"] = "2026-08-24T02:00:00Z"
        value["accounts"][1]["funding_transfers"] = [duplicate]
        with self.assertRaisesRegex(onboarding.OnboardingError, "more than one account"):
            onboarding.parse_evidence(value, "0" * 64)

    def test_rejects_unsorted_accounts_and_positions(self) -> None:
        value = valid_evidence()
        value["accounts"].reverse()
        with self.assertRaisesRegex(onboarding.OnboardingError, "accounts must be sorted"):
            onboarding.parse_evidence(value, "0" * 64)

        value = valid_evidence()
        value["wallet6_position_baseline"]["positions"].reverse()
        with self.assertRaisesRegex(onboarding.OnboardingError, "positions must be sorted"):
            onboarding.parse_evidence(value, "0" * 64)

    def test_rejects_nonbinary_baseline_outcome(self) -> None:
        value = valid_evidence()
        value["wallet6_position_baseline"]["positions"][0]["outcome_index"] = 2
        with self.assertRaisesRegex(onboarding.OnboardingError, "must be 0 or 1"):
            onboarding.parse_evidence(value, "0" * 64)

    def test_rejects_uint256_overflow(self) -> None:
        value = valid_evidence()
        overflow = str(1 << 256)
        value["accounts"][1]["funding_transfers"][0]["amount_base_units"] = overflow
        value["accounts"][1]["balance_evidence"]["after_base_units"] = overflow
        with self.assertRaisesRegex(onboarding.OnboardingError, "unsigned 256-bit"):
            onboarding.parse_evidence(value, "0" * 64)


class SQLGenerationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.evidence = onboarding.parse_evidence(valid_evidence(), "a" * 64)
        self.sql = onboarding.generate_sql(self.evidence)

    def test_sql_is_deterministic_and_uses_evidence_balances(self) -> None:
        self.assertEqual(self.sql, onboarding.generate_sql(self.evidence))
        self.assertIn("'201'::numeric", self.sql)
        self.assertIn("'221'::numeric", self.sql)
        self.assertNotIn("'200'::numeric", self.sql)
        self.assertIn("BEGIN ISOLATION LEVEL SERIALIZABLE", self.sql)
        self.assertNotIn("ON CONFLICT", self.sql)

    def test_base_unit_rendering_never_rounds_large_uint256_values(self) -> None:
        value = onboarding.MAX_UINT256
        whole, fractional = divmod(value, 1_000_000)
        expected = f"{whole}.{fractional:06d}".rstrip("0")
        self.assertEqual(onboarding._decimal_from_base_units(value), expected)

    def test_cash_update_preserves_reconciled_at(self) -> None:
        update = self.sql.split("UPDATE execution_accounts", 1)[1].split(
            "WHERE execution_account_id", 1
        )[0]
        self.assertNotIn("reconciled_at=", update.replace(" ", ""))
        self.assertIn(
            "reconciled_at IS NOT DISTINCT FROM target.expected_reconciled_at",
            self.sql,
        )

    def test_strict_idempotency_and_deterministic_events_are_present(self) -> None:
        self.assertIn("present_events=expected_events", self.sql)
        self.assertIn("partial onboarding event set", self.sql)
        self.assertIn("conflicting prior onboarding", self.sql)
        self.assertIn("account-event:external-funding:v1:137", self.sql)
        self.assertEqual(self.sql.count("account-event:external-funding:v1:137"), 3)

    def test_baseline_items_are_written_before_header(self) -> None:
        item_insert = self.sql.index(
            "INSERT INTO execution_external_position_baseline_items("
        )
        header_insert = self.sql.index(
            "INSERT INTO execution_external_position_baselines("
        )
        self.assertLess(item_insert, header_insert)
        self.assertNotIn("INSERT INTO execution_positions", self.sql)
        self.assertNotIn("INSERT INTO execution_fills", self.sql)
        self.assertNotIn("INSERT INTO position_lots", self.sql)

    def test_immutable_baseline_binds_complete_root_evidence(self) -> None:
        self.assertIn("onboarding_evidence_sha256", self.sql)
        self.assertIn(self.evidence.sha256, self.sql)
        self.assertIn(_hash(3), self.sql)
        self.assertIn("funding_transfers", self.sql)


class CLITests(EvidenceFileMixin, unittest.TestCase):
    def test_default_is_offline_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_evidence(directory, valid_evidence())
            stdout = io.StringIO()
            stderr = io.StringIO()
            with mock.patch.object(onboarding, "execute_sql") as execute:
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    result = onboarding.main(["--evidence", str(path)])
        self.assertEqual(result, 0)
        execute.assert_not_called()
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "VALIDATED_DRY_RUN")
        self.assertFalse(payload["database_access_attempted"])
        self.assertFalse(payload["funding_events_database_immutable"])
        self.assertEqual(stderr.getvalue(), "")

    def test_wrong_execution_token_never_calls_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_evidence(directory, valid_evidence())
            stderr = io.StringIO()
            with mock.patch.object(onboarding, "execute_sql") as execute:
                with contextlib.redirect_stderr(stderr):
                    result = onboarding.main(
                        ["--evidence", str(path), "--execute-token", "wrong"]
                    )
        self.assertEqual(result, 2)
        execute.assert_not_called()
        self.assertIn("no database access was attempted", stderr.getvalue())

    def test_execute_requires_approved_sha_and_does_not_print_secret(self) -> None:
        value = valid_evidence()
        digest = hashlib.sha256(onboarding.canonical_json(value)).hexdigest()
        secret_url = "postgresql://operator:supersecret@db.example/execution"
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_evidence(directory, value)
            stdout = io.StringIO()
            stderr = io.StringIO()
            with mock.patch.dict(os.environ, {"WALLET67_TEST_DATABASE_URL": secret_url}):
                with mock.patch.object(onboarding, "execute_sql", return_value="{}") as execute:
                    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                        result = onboarding.main(
                            [
                                "--evidence",
                                str(path),
                                "--expected-evidence-sha256",
                                digest,
                                "--execute-token",
                                onboarding.EXECUTE_TOKEN,
                                "--database-url-env",
                                "WALLET67_TEST_DATABASE_URL",
                            ]
                        )
        self.assertEqual(result, 0)
        execute.assert_called_once()
        self.assertEqual(execute.call_args.args[1:], (secret_url, "WALLET67_TEST_DATABASE_URL"))
        self.assertNotIn("supersecret", stdout.getvalue())
        self.assertNotIn("supersecret", stderr.getvalue())
        self.assertEqual(json.loads(stdout.getvalue())["status"], "EXECUTED")

    def test_sql_output_is_created_exclusively_as_0600(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            evidence_path = self.write_evidence(directory, valid_evidence())
            sql_path = pathlib.Path(directory) / "apply.sql"
            with contextlib.redirect_stdout(io.StringIO()):
                result = onboarding.main(
                    [
                        "--evidence",
                        str(evidence_path),
                        "--sql-output",
                        str(sql_path),
                    ]
                )
            self.assertEqual(result, 0)
            self.assertEqual(stat.S_IMODE(sql_path.stat().st_mode), 0o600)
            with contextlib.redirect_stderr(io.StringIO()):
                second = onboarding.main(
                    [
                        "--evidence",
                        str(evidence_path),
                        "--sql-output",
                        str(sql_path),
                    ]
                )
            self.assertEqual(second, 2)

    def test_database_command_keeps_password_out_of_argv(self) -> None:
        env_name = "WALLET67_TEST_DATABASE_URL"
        url = "postgresql://operator:supersecret@db.example:5433/execution?sslmode=require"
        with mock.patch.dict(os.environ, {env_name: url}):
            command, child_environment = onboarding._database_command(url, env_name)
        self.assertNotIn("supersecret", " ".join(command))
        self.assertNotIn(env_name, child_environment)
        self.assertEqual(child_environment["PGPASSWORD"], "supersecret")
        self.assertEqual(child_environment["PGSSLMODE"], "require")

    def test_database_failure_does_not_echo_captured_output(self) -> None:
        failed = mock.Mock(returncode=1, stdout="ERROR: row contained supersecret")
        with mock.patch.object(onboarding.subprocess, "run", return_value=failed):
            with self.assertRaises(onboarding.OnboardingError) as raised:
                onboarding.execute_sql(
                    "SELECT 1;",
                    "postgresql://operator:supersecret@db.example/execution",
                    "WALLET67_TEST_DATABASE_URL",
                )
        self.assertNotIn("supersecret", str(raised.exception))
        self.assertIn("not reported committed", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
