#!/usr/bin/env python3
"""Validate and apply the one-time wallet-6/wallet-7 ledger onboarding.

The default mode is offline validation.  It reads one canonical, mode-0600
evidence file, validates finalized Polygon pUSD funding logs, and prepares one
SERIALIZABLE PostgreSQL transaction.  It never reads a database or invokes
``psql`` unless the exact execution token and the externally approved evidence
SHA-256 are both supplied.

The transaction has two deliberately narrow responsibilities:

* bootstrap wallet-6 and wallet-7 from an exact zero local cash ledger to the
  balance proven by one or more finalized pUSD Transfer logs; and
* seal every current wallet-6 position as an immutable migration-0014
  unmanaged baseline (items first, header last).

It never changes ``reconciled_at``, creates a fill/order/lot/managed position,
resolves reconciliation issues, changes a risk gate, or enables a wallet.

Migration 0016 does not make ``EXTERNAL_FUNDING_DEPOSIT`` account-event rows
immutable.  The transaction therefore stores the complete canonical capture
and its SHA-256 in the immutable 0014 baseline, while strict re-runs detect any
later event mutation.  The original mode-0600 evidence file remains part of
the operator audit record and must be retained.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import stat
import subprocess
import sys
import urllib.parse
from decimal import Decimal, InvalidOperation
from typing import NoReturn


SCHEMA_VERSION = "trading.wallet67-onboarding.v1"
BASELINE_EVIDENCE_SCHEMA = "trading.external-position-baseline-evidence.v1"
EXECUTE_TOKEN = "APPLY_WALLET67_ONBOARDING_WITH_GATES_CLOSED_20260824"
DATABASE_ENV = "TRADING_EXECUTION_DATABASE_URL"
MAX_EVIDENCE_BYTES = 1024 * 1024
MAX_UINT256 = (1 << 256) - 1
CHAIN_ID = 137
PUSD_ADDRESS = "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb"
PUSD_DECIMALS = 6
PUSD_BASE_UNITS = 10**PUSD_DECIMALS
EXPECTED_WALLETS = {
    "wallet-6": "0x0aefd80df02cc35e81aede40b34e2e961bb4593f",
    "wallet-7": "0xc9ba353781f13ec9507bc0677156814d805fe6d9",
}

HEX_ADDRESS = re.compile(r"^0x[0-9a-f]{40}$")
HEX_HASH = re.compile(r"^0x[0-9a-f]{64}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
UNSIGNED_INTEGER = re.compile(r"^(0|[1-9][0-9]*)$")
CANONICAL_DECIMAL = re.compile(r"^(0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?$")
ENVIRONMENT_NAME = re.compile(r"^[A-Z][A-Z0-9_]*$")
RFC3339_UTC = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z$"
)


class OnboardingError(RuntimeError):
    """A fail-closed evidence, SQL-generation, or execution error."""


def _fail(message: str) -> NoReturn:
    raise OnboardingError(message)


def _reject_float(_: str) -> NoReturn:
    _fail("evidence JSON numbers must be integers or exact decimal strings")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            _fail(f"evidence JSON contains duplicate key {key!r}")
        result[key] = value
    return result


def canonical_json(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _require_object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        _fail(f"{label} must be a JSON object")
    return value


def _require_array(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        _fail(f"{label} must be a JSON array")
    return value


def _require_keys(
    value: dict[str, object], label: str, required: set[str]
) -> None:
    actual = set(value)
    if actual != required:
        missing = sorted(required - actual)
        unknown = sorted(actual - required)
        _fail(f"{label} keys differ (missing={missing}, unknown={unknown})")


def _string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        _fail(f"{label} must be a non-empty trimmed string")
    if "\x00" in value:
        _fail(f"{label} must not contain NUL")
    return value


def _lower_address(value: object, label: str) -> str:
    result = _string(value, label)
    if result != result.lower() or not HEX_ADDRESS.fullmatch(result):
        _fail(f"{label} must be a lowercase 20-byte address")
    return result


def _lower_hash(value: object, label: str) -> str:
    result = _string(value, label)
    if result != result.lower() or not HEX_HASH.fullmatch(result):
        _fail(f"{label} must be a lowercase 32-byte hash")
    return result


def _integer(value: object, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        _fail(f"{label} must be an integer >= {minimum}")
    return value


def _integer_string(value: object, label: str, *, positive: bool = False) -> int:
    raw = _string(value, label)
    if not UNSIGNED_INTEGER.fullmatch(raw):
        _fail(f"{label} must be a canonical unsigned integer string")
    parsed = int(raw)
    if parsed > MAX_UINT256:
        _fail(f"{label} exceeds an unsigned 256-bit token amount")
    if positive and parsed <= 0:
        _fail(f"{label} must be positive")
    return parsed


def _decimal_string(value: object, label: str, *, positive: bool = False) -> str:
    raw = _string(value, label)
    if not CANONICAL_DECIMAL.fullmatch(raw):
        _fail(f"{label} must be a canonical non-negative decimal string")
    try:
        parsed = Decimal(raw)
    except InvalidOperation as error:
        raise OnboardingError(f"{label} is not a decimal") from error
    if positive and parsed <= 0:
        _fail(f"{label} must be positive")
    return raw


def _timestamp(value: object, label: str) -> tuple[str, dt.datetime]:
    raw = _string(value, label)
    if not RFC3339_UTC.fullmatch(raw):
        _fail(f"{label} must be a canonical RFC3339 UTC timestamp ending in Z")
    try:
        parsed = dt.datetime.fromisoformat(raw[:-1] + "+00:00")
    except ValueError as error:
        raise OnboardingError(f"{label} must be an RFC3339 timestamp") from error
    if parsed.utcoffset() != dt.timedelta(0):
        _fail(f"{label} must be UTC")
    return raw, parsed.astimezone(dt.timezone.utc)


def _optional_timestamp(value: object, label: str) -> tuple[str | None, dt.datetime | None]:
    if value is None:
        return None, None
    return _timestamp(value, label)


def _boolean(value: object, label: str, expected: bool) -> None:
    if not isinstance(value, bool) or value is not expected:
        _fail(f"{label} must be {str(expected).lower()}")


@dataclasses.dataclass(frozen=True)
class Transfer:
    transaction_hash: str
    log_index: int
    transaction_index: int
    block_number: int
    block_hash: str
    block_timestamp: str
    block_time: dt.datetime
    token_address: str
    from_address: str
    to_address: str
    amount_base_units: int
    confirmations: int

    @property
    def identity(self) -> tuple[int, str, int]:
        return CHAIN_ID, self.transaction_hash, self.log_index

    @property
    def order_key(self) -> tuple[int, int, int, str]:
        return (
            self.block_number,
            self.transaction_index,
            self.log_index,
            self.transaction_hash,
        )

    @property
    def event_id(self) -> str:
        return (
            "account-event:external-funding:v1:137:"
            f"{self.block_number:020d}:{self.transaction_index:08d}:"
            f"{self.log_index:08d}:{self.transaction_hash}"
        )


@dataclasses.dataclass(frozen=True)
class AccountEvidence:
    execution_account_id: str
    wallet_address: str
    expected_version: int
    expected_reconciled_at: str | None
    expected_reconciled_time: dt.datetime | None
    before_block_number: int
    after_block_number: int
    target_base_units: int
    observed_at: str
    observed_time: dt.datetime
    transfer_scan_observed_time: dt.datetime
    transfers: tuple[Transfer, ...]

    @property
    def target_balance(self) -> Decimal:
        return Decimal(_decimal_from_base_units(self.target_base_units))


@dataclasses.dataclass(frozen=True)
class BaselinePosition:
    token_id: str
    condition_id: str
    outcome_index: int
    outcome_name: str
    neg_risk: bool
    shares: str


@dataclasses.dataclass(frozen=True)
class PositionBaseline:
    source: str
    observed_at: str
    observed_time: dt.datetime
    trade_observed_time: dt.datetime
    actor: str
    reason: str
    raw: dict[str, object]
    positions: tuple[BaselinePosition, ...]
    baseline_id: str


@dataclasses.dataclass(frozen=True)
class Evidence:
    raw: dict[str, object]
    sha256: str
    generated_at: str
    generated_time: dt.datetime
    accounts: tuple[AccountEvidence, ...]
    baseline: PositionBaseline

    @property
    def account_by_id(self) -> dict[str, AccountEvidence]:
        return {item.execution_account_id: item for item in self.accounts}


def _parse_transfer(
    value: object,
    *,
    label: str,
    wallet_address: str,
) -> Transfer:
    row = _require_object(value, label)
    _require_keys(
        row,
        label,
        {
            "transaction_hash",
            "log_index",
            "transaction_index",
            "block_number",
            "block_hash",
            "block_timestamp",
            "token_address",
            "from_address",
            "to_address",
            "amount_base_units",
            "receipt_status",
            "confirmations",
            "finalized",
        },
    )
    if _integer(row["receipt_status"], f"{label}.receipt_status") != 1:
        _fail(f"{label}.receipt_status must be 1")
    _boolean(row["finalized"], f"{label}.finalized", True)
    timestamp, block_time = _timestamp(row["block_timestamp"], f"{label}.block_timestamp")
    token_address = _lower_address(row["token_address"], f"{label}.token_address")
    if token_address != PUSD_ADDRESS:
        _fail(f"{label}.token_address is not the configured pUSD contract")
    to_address = _lower_address(row["to_address"], f"{label}.to_address")
    if to_address != wallet_address:
        _fail(f"{label}.to_address does not match its execution wallet")
    from_address = _lower_address(row["from_address"], f"{label}.from_address")
    if from_address == to_address:
        _fail(f"{label} is a self-transfer, not attributable external funding")
    return Transfer(
        transaction_hash=_lower_hash(row["transaction_hash"], f"{label}.transaction_hash"),
        log_index=_integer(row["log_index"], f"{label}.log_index"),
        transaction_index=_integer(row["transaction_index"], f"{label}.transaction_index"),
        block_number=_integer(row["block_number"], f"{label}.block_number", minimum=1),
        block_hash=_lower_hash(row["block_hash"], f"{label}.block_hash"),
        block_timestamp=timestamp,
        block_time=block_time,
        token_address=token_address,
        from_address=from_address,
        to_address=to_address,
        amount_base_units=_integer_string(
            row["amount_base_units"], f"{label}.amount_base_units", positive=True
        ),
        confirmations=_integer(row["confirmations"], f"{label}.confirmations", minimum=1),
    )


def _parse_account(value: object, index: int) -> AccountEvidence:
    label = f"accounts[{index}]"
    row = _require_object(value, label)
    _require_keys(
        row,
        label,
        {
            "execution_account_id",
            "wallet_address",
            "expected_database",
            "balance_evidence",
            "transfer_scan",
            "funding_transfers",
        },
    )
    account_id = _string(row["execution_account_id"], f"{label}.execution_account_id")
    if account_id not in EXPECTED_WALLETS:
        _fail(f"{label}.execution_account_id is not wallet-6 or wallet-7")
    wallet_address = _lower_address(row["wallet_address"], f"{label}.wallet_address")
    if wallet_address != EXPECTED_WALLETS[account_id]:
        _fail(f"{label}.wallet_address differs from the approved account identity")

    database = _require_object(row["expected_database"], f"{label}.expected_database")
    _require_keys(
        database,
        f"{label}.expected_database",
        {
            "version",
            "reconciled_at",
            "total_balance",
            "available_balance",
            "reserved_balance",
        },
    )
    for field in ("total_balance", "available_balance", "reserved_balance"):
        if _decimal_string(database[field], f"{label}.expected_database.{field}") != "0":
            _fail(f"{label}.expected_database.{field} must be exactly '0'")
    expected_reconciled_at, expected_reconciled_time = _optional_timestamp(
        database["reconciled_at"], f"{label}.expected_database.reconciled_at"
    )

    balance = _require_object(row["balance_evidence"], f"{label}.balance_evidence")
    _require_keys(
        balance,
        f"{label}.balance_evidence",
        {
            "source",
            "before_block_number",
            "before_block_hash",
            "before_base_units",
            "after_block_number",
            "after_block_hash",
            "after_base_units",
            "before_block_finalized",
            "after_block_finalized",
            "observed_at",
        },
    )
    if _string(balance["source"], f"{label}.balance_evidence.source") != "POLYGON_RPC_ETH_CALL":
        _fail(f"{label}.balance_evidence.source must be POLYGON_RPC_ETH_CALL")
    before_base_units = _integer_string(
        balance["before_base_units"], f"{label}.balance_evidence.before_base_units"
    )
    if before_base_units != 0:
        _fail(f"{label}.balance_evidence.before_base_units must be zero")
    target_base_units = _integer_string(
        balance["after_base_units"],
        f"{label}.balance_evidence.after_base_units",
        positive=True,
    )
    before_block_number = _integer(
        balance["before_block_number"],
        f"{label}.balance_evidence.before_block_number",
        minimum=1,
    )
    after_block_number = _integer(
        balance["after_block_number"],
        f"{label}.balance_evidence.after_block_number",
        minimum=1,
    )
    _lower_hash(balance["before_block_hash"], f"{label}.balance_evidence.before_block_hash")
    _lower_hash(balance["after_block_hash"], f"{label}.balance_evidence.after_block_hash")
    _boolean(
        balance["before_block_finalized"],
        f"{label}.balance_evidence.before_block_finalized",
        True,
    )
    _boolean(
        balance["after_block_finalized"],
        f"{label}.balance_evidence.after_block_finalized",
        True,
    )
    observed_at, observed_time = _timestamp(
        balance["observed_at"], f"{label}.balance_evidence.observed_at"
    )

    transfer_values = _require_array(row["funding_transfers"], f"{label}.funding_transfers")
    if not transfer_values:
        _fail(f"{label}.funding_transfers must not be empty")
    transfers = tuple(
        _parse_transfer(
            item,
            label=f"{label}.funding_transfers[{item_index}]",
            wallet_address=wallet_address,
        )
        for item_index, item in enumerate(transfer_values)
    )
    if tuple(sorted(transfers, key=lambda item: item.order_key)) != transfers:
        _fail(f"{label}.funding_transfers must be in finalized chain order")
    if len({item.identity for item in transfers}) != len(transfers):
        _fail(f"{label}.funding_transfers contains a duplicate chain log")
    if sum(item.amount_base_units for item in transfers) != target_base_units:
        _fail(f"{label} finalized funding logs do not sum to after_base_units")
    if before_block_number >= transfers[0].block_number:
        _fail(f"{label} zero-balance proof must predate the first funding log")
    if after_block_number < transfers[-1].block_number:
        _fail(f"{label} final-balance proof predates a funding log")
    if observed_time < transfers[-1].block_time:
        _fail(f"{label} balance observation predates a funding log")
    if expected_reconciled_time is not None and any(
        item.block_time <= expected_reconciled_time for item in transfers
    ):
        _fail(f"{label} contains funding before the preserved reconciled_at boundary")

    transfer_scan = _require_object(row["transfer_scan"], f"{label}.transfer_scan")
    _require_keys(
        transfer_scan,
        f"{label}.transfer_scan",
        {
            "source",
            "from_block_number",
            "to_block_number",
            "observed_at",
            "pagination_complete",
            "inbound_log_count",
            "outbound_log_count",
        },
    )
    if _string(transfer_scan["source"], f"{label}.transfer_scan.source") != (
        "POLYGON_RPC_ETH_GET_LOGS"
    ):
        _fail(f"{label}.transfer_scan.source must be POLYGON_RPC_ETH_GET_LOGS")
    if _integer(
        transfer_scan["from_block_number"],
        f"{label}.transfer_scan.from_block_number",
        minimum=1,
    ) != before_block_number + 1:
        _fail(f"{label}.transfer_scan must start immediately after the zero proof")
    if _integer(
        transfer_scan["to_block_number"],
        f"{label}.transfer_scan.to_block_number",
        minimum=1,
    ) != after_block_number:
        _fail(f"{label}.transfer_scan must end at the final balance proof")
    _, transfer_scan_observed_time = _timestamp(
        transfer_scan["observed_at"], f"{label}.transfer_scan.observed_at"
    )
    _boolean(
        transfer_scan["pagination_complete"],
        f"{label}.transfer_scan.pagination_complete",
        True,
    )
    if _integer(
        transfer_scan["inbound_log_count"],
        f"{label}.transfer_scan.inbound_log_count",
    ) != len(transfers):
        _fail(f"{label}.transfer_scan.inbound_log_count must match funding_transfers")
    if _integer(
        transfer_scan["outbound_log_count"],
        f"{label}.transfer_scan.outbound_log_count",
    ) != 0:
        _fail(f"{label} has outbound pUSD transfers inside the bootstrap interval")

    return AccountEvidence(
        execution_account_id=account_id,
        wallet_address=wallet_address,
        expected_version=_integer(
            database["version"], f"{label}.expected_database.version", minimum=1
        ),
        expected_reconciled_at=expected_reconciled_at,
        expected_reconciled_time=expected_reconciled_time,
        before_block_number=before_block_number,
        after_block_number=after_block_number,
        target_base_units=target_base_units,
        observed_at=observed_at,
        observed_time=observed_time,
        transfer_scan_observed_time=transfer_scan_observed_time,
        transfers=transfers,
    )


def _parse_baseline(
    value: object,
    *,
    wallet6: AccountEvidence,
) -> PositionBaseline:
    label = "wallet6_position_baseline"
    row = _require_object(value, label)
    _require_keys(
        row,
        label,
        {
            "source",
            "observed_at",
            "include_archived",
            "size_threshold",
            "pagination_complete",
            "actor",
            "reason",
            "post_reconciled_trades",
            "positions",
        },
    )
    source = _string(row["source"], f"{label}.source")
    if source != "POLYMARKET_DATA_API":
        _fail(f"{label}.source must be POLYMARKET_DATA_API")
    observed_at, observed_time = _timestamp(row["observed_at"], f"{label}.observed_at")
    _boolean(row["include_archived"], f"{label}.include_archived", True)
    if _string(row["size_threshold"], f"{label}.size_threshold") != "0":
        _fail(f"{label}.size_threshold must be '0'")
    _boolean(row["pagination_complete"], f"{label}.pagination_complete", True)
    actor = _string(row["actor"], f"{label}.actor")
    reason = _string(row["reason"], f"{label}.reason")

    trade_evidence = _require_object(
        row["post_reconciled_trades"], f"{label}.post_reconciled_trades"
    )
    _require_keys(
        trade_evidence,
        f"{label}.post_reconciled_trades",
        {
            "source",
            "query_after",
            "observed_at",
            "pagination_complete",
            "unattributed_count",
        },
    )
    if _string(
        trade_evidence["source"], f"{label}.post_reconciled_trades.source"
    ) != "POLYMARKET_CLOB_DATA_TRADES":
        _fail(f"{label}.post_reconciled_trades.source is invalid")
    query_after, query_after_time = _optional_timestamp(
        trade_evidence["query_after"], f"{label}.post_reconciled_trades.query_after"
    )
    if query_after != wallet6.expected_reconciled_at:
        _fail(f"{label}.post_reconciled_trades.query_after must equal preserved reconciled_at")
    _, trade_observed_time = _timestamp(
        trade_evidence["observed_at"], f"{label}.post_reconciled_trades.observed_at"
    )
    _boolean(
        trade_evidence["pagination_complete"],
        f"{label}.post_reconciled_trades.pagination_complete",
        True,
    )
    if _integer(
        trade_evidence["unattributed_count"],
        f"{label}.post_reconciled_trades.unattributed_count",
    ) != 0:
        _fail(f"{label}.post_reconciled_trades.unattributed_count must be zero")
    if query_after_time is not None and observed_time < query_after_time:
        _fail(f"{label}.observed_at predates reconciled_at")
    if trade_observed_time < observed_time:
        _fail(f"{label} trade evidence predates the position snapshot")

    raw_positions = _require_array(row["positions"], f"{label}.positions")
    if not raw_positions:
        _fail(f"{label}.positions must contain every current positive position")
    positions: list[BaselinePosition] = []
    for index, raw_position in enumerate(raw_positions):
        item_label = f"{label}.positions[{index}]"
        item = _require_object(raw_position, item_label)
        _require_keys(
            item,
            item_label,
            {
                "token_id",
                "condition_id",
                "outcome_index",
                "outcome_name",
                "neg_risk",
                "shares",
            },
        )
        outcome_index = _integer(item["outcome_index"], f"{item_label}.outcome_index")
        if outcome_index not in (0, 1):
            _fail(f"{item_label}.outcome_index must be 0 or 1")
        if not isinstance(item["neg_risk"], bool):
            _fail(f"{item_label}.neg_risk must be boolean")
        positions.append(
            BaselinePosition(
                token_id=_string(item["token_id"], f"{item_label}.token_id"),
                condition_id=_string(item["condition_id"], f"{item_label}.condition_id"),
                outcome_index=outcome_index,
                outcome_name=_string(item["outcome_name"], f"{item_label}.outcome_name"),
                neg_risk=item["neg_risk"],
                shares=_decimal_string(item["shares"], f"{item_label}.shares", positive=True),
            )
        )
    if positions != sorted(positions, key=lambda item: item.token_id):
        _fail(f"{label}.positions must be sorted by token_id")
    if len({item.token_id for item in positions}) != len(positions):
        _fail(f"{label}.positions contains a duplicate token_id")

    baseline_digest = canonical_sha256(row)
    return PositionBaseline(
        source=source,
        observed_at=observed_at,
        observed_time=observed_time,
        trade_observed_time=trade_observed_time,
        actor=actor,
        reason=reason,
        raw=row,
        positions=tuple(positions),
        baseline_id=f"external-position-baseline:wallet-6:{baseline_digest}",
    )


def parse_evidence(value: object, sha256: str) -> Evidence:
    root = _require_object(value, "evidence")
    _require_keys(
        root,
        "evidence",
        {
            "schema_version",
            "generated_at",
            "chain_id",
            "collateral",
            "accounts",
            "wallet6_position_baseline",
        },
    )
    if _string(root["schema_version"], "schema_version") != SCHEMA_VERSION:
        _fail(f"schema_version must be {SCHEMA_VERSION}")
    if _integer(root["chain_id"], "chain_id") != CHAIN_ID:
        _fail(f"chain_id must be {CHAIN_ID}")
    generated_at, generated_time = _timestamp(root["generated_at"], "generated_at")

    collateral = _require_object(root["collateral"], "collateral")
    _require_keys(collateral, "collateral", {"asset", "token_address", "decimals"})
    if _string(collateral["asset"], "collateral.asset") != "pUSD":
        _fail("collateral.asset must be pUSD")
    if _lower_address(collateral["token_address"], "collateral.token_address") != PUSD_ADDRESS:
        _fail("collateral.token_address is not the configured pUSD contract")
    if _integer(collateral["decimals"], "collateral.decimals") != PUSD_DECIMALS:
        _fail(f"collateral.decimals must be {PUSD_DECIMALS}")

    account_values = _require_array(root["accounts"], "accounts")
    accounts = tuple(_parse_account(item, index) for index, item in enumerate(account_values))
    if [item.execution_account_id for item in accounts] != sorted(
        item.execution_account_id for item in accounts
    ):
        _fail("accounts must be sorted by execution_account_id")
    account_by_id = {item.execution_account_id: item for item in accounts}
    if set(account_by_id) != set(EXPECTED_WALLETS) or len(accounts) != len(EXPECTED_WALLETS):
        _fail("accounts must contain wallet-6 and wallet-7 exactly once")
    all_log_ids = [transfer.identity for account in accounts for transfer in account.transfers]
    if len(set(all_log_ids)) != len(all_log_ids):
        _fail("the same finalized chain log appears in more than one account")
    blocks: dict[int, tuple[str, str]] = {}
    transactions: dict[str, tuple[int, str, str, int]] = {}
    for account in accounts:
        for transfer in account.transfers:
            block_identity = (transfer.block_hash, transfer.block_timestamp)
            if blocks.setdefault(transfer.block_number, block_identity) != block_identity:
                _fail("finalized logs disagree on the identity of a block")
            transaction_identity = (
                transfer.block_number,
                transfer.block_hash,
                transfer.block_timestamp,
                transfer.transaction_index,
            )
            if (
                transactions.setdefault(
                    transfer.transaction_hash, transaction_identity
                )
                != transaction_identity
            ):
                _fail("finalized logs disagree on the identity of a transaction")
    if generated_time < max(
        max(item.observed_time, item.transfer_scan_observed_time) for item in accounts
    ):
        _fail("generated_at predates an account balance observation")

    baseline = _parse_baseline(root["wallet6_position_baseline"], wallet6=account_by_id["wallet-6"])
    if generated_time < max(baseline.observed_time, baseline.trade_observed_time):
        _fail("generated_at predates the wallet-6 baseline observation")
    return Evidence(
        raw=root,
        sha256=sha256,
        generated_at=generated_at,
        generated_time=generated_time,
        accounts=tuple(sorted(accounts, key=lambda item: item.execution_account_id)),
        baseline=baseline,
    )


def read_evidence(path: pathlib.Path) -> Evidence:
    try:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
    except OSError as error:
        raise OnboardingError(f"cannot securely open evidence file: {error}") from error
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            _fail("evidence must be a non-symlink regular file")
        if stat.S_IMODE(info.st_mode) != 0o600:
            _fail("evidence file mode must be exactly 0600")
        if info.st_uid != os.geteuid():
            _fail("evidence file must be owned by the current user")
        if info.st_size <= 0 or info.st_size > MAX_EVIDENCE_BYTES:
            _fail(f"evidence file size must be 1..{MAX_EVIDENCE_BYTES} bytes")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            payload = handle.read(MAX_EVIDENCE_BYTES + 1)
        if len(payload) != info.st_size:
            _fail("evidence file changed while it was being read")
    except OSError as error:
        raise OnboardingError(f"cannot read evidence file: {error}") from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    try:
        value = json.loads(
            payload,
            object_pairs_hook=_unique_object,
            parse_float=_reject_float,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise OnboardingError("evidence file is not valid UTF-8 JSON") from error
    if payload != canonical_json(value):
        _fail("evidence file is not canonical sorted compact JSON with one trailing newline")
    digest = hashlib.sha256(payload).hexdigest()
    return parse_evidence(value, digest)


def _sql_text(value: str) -> str:
    if "\x00" in value:
        _fail("SQL text contains NUL")
    return "'" + value.replace("'", "''") + "'"


def _sql_timestamp(value: str | None) -> str:
    if value is None:
        return "NULL::timestamptz"
    return f"{_sql_text(value)}::timestamptz"


def _sql_json(value: object) -> str:
    return f"{_sql_text(canonical_json(value).decode('utf-8').rstrip())}::jsonb"


def _decimal_from_base_units(value: int) -> str:
    whole, fractional = divmod(value, PUSD_BASE_UNITS)
    if fractional == 0:
        return str(whole)
    return f"{whole}.{fractional:0{PUSD_DECIMALS}d}".rstrip("0")


def generate_sql(evidence: Evidence) -> str:
    account_rows = []
    event_rows = []
    for account in evidence.accounts:
        account_rows.append(
            "(" + ",".join(
                (
                    _sql_text(account.execution_account_id),
                    _sql_text(account.wallet_address),
                    str(account.expected_version),
                    _sql_timestamp(account.expected_reconciled_at),
                    _sql_text(_decimal_from_base_units(account.target_base_units)) + "::numeric",
                )
            ) + ")"
        )
        cumulative = 0
        for transfer in account.transfers:
            cumulative += transfer.amount_base_units
            event_rows.append(
                "(" + ",".join(
                    (
                        _sql_text(account.execution_account_id),
                        _sql_text(transfer.event_id),
                        _sql_text(_decimal_from_base_units(transfer.amount_base_units))
                        + "::numeric",
                        _sql_text(_decimal_from_base_units(cumulative)) + "::numeric",
                        _sql_text(transfer.block_timestamp) + "::timestamptz",
                    )
                ) + ")"
            )

    baseline_wrapper = {
        "schema_version": BASELINE_EVIDENCE_SCHEMA,
        "onboarding_evidence_sha256": evidence.sha256,
        "wallet_address": EXPECTED_WALLETS["wallet-6"],
        # Migration 0014 makes this JSONB value immutable.  Retaining the
        # complete canonical capture binds the otherwise-mutable account-event
        # rows to their finalized chain evidence without inventing a new ledger.
        "onboarding_evidence": evidence.raw,
    }
    baseline_meta = "(" + ",".join(
        (
            _sql_text(evidence.baseline.baseline_id),
            _sql_text("wallet-6"),
            _sql_text(evidence.baseline.source),
            _sql_text(evidence.baseline.observed_at) + "::timestamptz",
            _sql_json(baseline_wrapper),
            _sql_text(evidence.baseline.actor),
            _sql_text(evidence.baseline.reason),
        )
    ) + ")"
    baseline_rows = [
        "(" + ",".join(
            (
                _sql_text(evidence.baseline.baseline_id),
                _sql_text("wallet-6"),
                _sql_text(item.token_id),
                _sql_text(item.condition_id),
                str(item.outcome_index),
                _sql_text(item.outcome_name),
                "TRUE" if item.neg_risk else "FALSE",
                _sql_text(item.shares) + "::numeric",
            )
        ) + ")"
        for item in evidence.baseline.positions
    ]
    account_values = ",\n    ".join(account_rows)
    event_values = ",\n    ".join(event_rows)
    baseline_values = ",\n    ".join(baseline_rows)

    return f"""-- Generated by wallet67_onboarding.py from evidence SHA-256 {evidence.sha256}
-- This SQL never changes reconciled_at and never enables an execution account.
BEGIN ISOLATION LEVEL SERIALIZABLE;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';
SET LOCAL search_path = pg_catalog, public, pg_temp;
SET LOCAL standard_conforming_strings = on;
SET CONSTRAINTS ALL DEFERRED;

CREATE TEMP TABLE _wallet67_accounts(
    execution_account_id TEXT PRIMARY KEY,
    wallet_address TEXT NOT NULL,
    expected_version BIGINT NOT NULL,
    expected_reconciled_at TIMESTAMPTZ,
    target_balance NUMERIC NOT NULL
) ON COMMIT DROP;
INSERT INTO _wallet67_accounts VALUES
    {account_values};

CREATE TEMP TABLE _wallet67_events(
    execution_account_id TEXT NOT NULL,
    account_event_id TEXT PRIMARY KEY,
    balance_delta NUMERIC NOT NULL,
    balance_after NUMERIC NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL
) ON COMMIT DROP;
INSERT INTO _wallet67_events VALUES
    {event_values};

CREATE TEMP TABLE _wallet67_baseline_meta(
    baseline_id TEXT PRIMARY KEY,
    execution_account_id TEXT NOT NULL,
    source TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    evidence JSONB NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL
) ON COMMIT DROP;
INSERT INTO _wallet67_baseline_meta VALUES {baseline_meta};

CREATE TEMP TABLE _wallet67_baseline_items(
    baseline_id TEXT NOT NULL,
    execution_account_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    outcome_index INTEGER NOT NULL,
    outcome_name TEXT NOT NULL,
    neg_risk BOOLEAN NOT NULL,
    shares NUMERIC NOT NULL,
    PRIMARY KEY (baseline_id, token_id)
) ON COMMIT DROP;
INSERT INTO _wallet67_baseline_items VALUES
    {baseline_values};

DO $wallet67_onboarding$
DECLARE
    target RECORD;
    current_account RECORD;
    expected_events BIGINT;
    present_events BIGINT;
    exact_events BIGINT;
    changed BIGINT;
    baseline RECORD;
    existing_headers BIGINT;
BEGIN
    IF to_regclass('execution_accounts') IS NULL
       OR to_regclass('execution_account_events') IS NULL
       OR to_regclass('execution_orders') IS NULL
       OR to_regclass('execution_fills') IS NULL
       OR to_regclass('execution_positions') IS NULL
       OR to_regclass('position_lots') IS NULL
       OR to_regclass('position_events') IS NULL
       OR to_regclass('asset_reservations') IS NULL
       OR to_regclass('reconciliation_runs') IS NULL
       OR to_regclass('strategy_order_intent_deliveries') IS NULL
       OR to_regclass('execution_risk_global_control') IS NULL
       OR to_regclass('execution_risk_controls') IS NULL
       OR to_regclass('execution_risk_policies') IS NULL
       OR to_regclass('execution_strategy_bindings') IS NULL
       OR to_regclass('execution_external_position_baselines') IS NULL
       OR to_regclass('execution_external_position_baseline_items') IS NULL
       OR to_regclass('execution_external_cash_adjustments') IS NULL
       OR to_regclass('execution_external_position_adoptions') IS NULL
       OR to_regclass('execution_external_position_dispositions') IS NULL THEN
        RAISE EXCEPTION 'wallet67 onboarding requires migrations through 0016';
    END IF;

    PERFORM 1 FROM execution_risk_global_control
     WHERE singleton=TRUE AND kill_switch=TRUE FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'wallet67 onboarding requires the global kill switch';
    END IF;

    LOCK TABLE execution_account_events IN SHARE ROW EXCLUSIVE MODE;
    LOCK TABLE execution_risk_controls IN SHARE ROW EXCLUSIVE MODE;
    LOCK TABLE execution_risk_policies IN SHARE ROW EXCLUSIVE MODE;
    LOCK TABLE execution_strategy_bindings IN SHARE ROW EXCLUSIVE MODE;
    LOCK TABLE execution_external_position_baselines IN SHARE ROW EXCLUSIVE MODE;
    LOCK TABLE execution_external_position_baseline_items IN SHARE ROW EXCLUSIVE MODE;

    FOR target IN SELECT * FROM _wallet67_accounts ORDER BY execution_account_id LOOP
        PERFORM pg_advisory_xact_lock(
            hashtextextended('external-funding' || E'\\n' || target.execution_account_id, 160018)
        );
        SELECT wallet_address, collateral_asset, total_balance, available_balance,
               reserved_balance, version, reconciled_at
          INTO current_account
          FROM execution_accounts
         WHERE execution_account_id=target.execution_account_id
         FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'execution account % does not exist', target.execution_account_id;
        END IF;
        IF lower(current_account.wallet_address)<>target.wallet_address
           OR current_account.collateral_asset<>'pUSD'
           OR current_account.reconciled_at IS DISTINCT FROM target.expected_reconciled_at THEN
            RAISE EXCEPTION 'execution account % identity or reconciled_at changed', target.execution_account_id;
        END IF;
        PERFORM 1 FROM execution_risk_controls
         WHERE execution_account_id=target.execution_account_id
           AND control_scope='ACCOUNT' AND control_key='' AND paused=TRUE
         FOR SHARE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'execution account % is not durably paused', target.execution_account_id;
        END IF;
        IF EXISTS (SELECT 1 FROM execution_risk_policies
                    WHERE execution_account_id=target.execution_account_id AND enabled=TRUE)
           OR EXISTS (SELECT 1 FROM execution_strategy_bindings
                       WHERE execution_account_id=target.execution_account_id AND enabled=TRUE) THEN
            RAISE EXCEPTION 'execution account % is enabled', target.execution_account_id;
        END IF;
        IF EXISTS (SELECT 1 FROM execution_orders WHERE execution_account_id=target.execution_account_id)
           OR EXISTS (SELECT 1 FROM execution_fills WHERE execution_account_id=target.execution_account_id)
           OR EXISTS (SELECT 1 FROM execution_positions WHERE execution_account_id=target.execution_account_id)
           OR EXISTS (SELECT 1 FROM position_lots WHERE execution_account_id=target.execution_account_id)
           OR EXISTS (SELECT 1 FROM position_events WHERE execution_account_id=target.execution_account_id)
           OR EXISTS (SELECT 1 FROM asset_reservations WHERE execution_account_id=target.execution_account_id)
           OR EXISTS (SELECT 1 FROM strategy_order_intent_deliveries
                       WHERE intent_payload->>'execution_account_id'=target.execution_account_id)
           OR EXISTS (SELECT 1 FROM reconciliation_runs
                       WHERE execution_account_id=target.execution_account_id AND status='RUNNING')
           OR EXISTS (SELECT 1 FROM execution_external_cash_adjustments
                       WHERE execution_account_id=target.execution_account_id)
           OR EXISTS (SELECT 1 FROM execution_external_position_adoptions
                       WHERE execution_account_id=target.execution_account_id)
           OR EXISTS (SELECT 1 FROM execution_external_position_dispositions
                       WHERE execution_account_id=target.execution_account_id) THEN
            RAISE EXCEPTION 'execution account % is not an idle onboarding ledger', target.execution_account_id;
        END IF;

        SELECT count(*) INTO expected_events FROM _wallet67_events
         WHERE execution_account_id=target.execution_account_id;
        SELECT count(*) INTO present_events
          FROM _wallet67_events expected
          JOIN execution_account_events actual USING (account_event_id)
         WHERE expected.execution_account_id=target.execution_account_id;
        SELECT count(*) INTO exact_events
          FROM _wallet67_events expected
          JOIN execution_account_events actual USING (account_event_id)
         WHERE expected.execution_account_id=target.execution_account_id
           AND actual.execution_account_id=expected.execution_account_id
           AND actual.event_type='EXTERNAL_FUNDING_DEPOSIT'
           AND actual.order_id='' AND actual.fill_key=''
           AND actual.total_balance_delta=expected.balance_delta
           AND actual.available_balance_delta=expected.balance_delta
           AND actual.reserved_balance_delta=0
           AND actual.total_balance_after=expected.balance_after
           AND actual.available_balance_after=expected.balance_after
           AND actual.reserved_balance_after=0
           AND actual.occurred_at=expected.occurred_at;

        IF present_events=expected_events THEN
            IF exact_events<>expected_events
               OR current_account.total_balance<>target.target_balance
               OR current_account.available_balance<>target.target_balance
               OR current_account.reserved_balance<>0
               OR current_account.version<>target.expected_version+1
               OR (SELECT count(*) FROM execution_account_events
                    WHERE execution_account_id=target.execution_account_id)<>expected_events THEN
                RAISE EXCEPTION 'execution account % has a conflicting prior onboarding', target.execution_account_id;
            END IF;
            CONTINUE;
        END IF;
        IF present_events<>0 THEN
            RAISE EXCEPTION 'execution account % has a partial onboarding event set', target.execution_account_id;
        END IF;
        IF current_account.total_balance<>0 OR current_account.available_balance<>0
           OR current_account.reserved_balance<>0
           OR current_account.version<>target.expected_version
           OR EXISTS (SELECT 1 FROM execution_account_events
                       WHERE execution_account_id=target.execution_account_id) THEN
            RAISE EXCEPTION 'execution account % no longer matches the exact zero bootstrap state', target.execution_account_id;
        END IF;

        UPDATE execution_accounts
           SET total_balance=target.target_balance,
               available_balance=target.target_balance,
               version=version+1,
               updated_at=clock_timestamp()
         WHERE execution_account_id=target.execution_account_id
           AND lower(wallet_address)=target.wallet_address
           AND collateral_asset='pUSD'
           AND total_balance=0 AND available_balance=0 AND reserved_balance=0
           AND version=target.expected_version
           AND reconciled_at IS NOT DISTINCT FROM target.expected_reconciled_at;
        GET DIAGNOSTICS changed = ROW_COUNT;
        IF changed<>1 THEN
            RAISE EXCEPTION 'execution account % cash CAS failed', target.execution_account_id;
        END IF;

        INSERT INTO execution_account_events(
            account_event_id,execution_account_id,event_type,order_id,fill_key,
            total_balance_delta,available_balance_delta,reserved_balance_delta,
            total_balance_after,available_balance_after,reserved_balance_after,occurred_at
        )
        SELECT account_event_id,execution_account_id,'EXTERNAL_FUNDING_DEPOSIT','','',
               balance_delta,balance_delta,0,balance_after,balance_after,0,occurred_at
          FROM _wallet67_events
         WHERE execution_account_id=target.execution_account_id
         ORDER BY occurred_at,account_event_id;
    END LOOP;

    SELECT * INTO baseline FROM _wallet67_baseline_meta;
    PERFORM pg_advisory_xact_lock(
        hashtextextended(baseline.baseline_id || E'\\n' || baseline.execution_account_id, 0)
    );
    SELECT count(*) INTO existing_headers
      FROM execution_external_position_baselines
     WHERE execution_account_id=baseline.execution_account_id;
    IF existing_headers=1 THEN
        IF NOT EXISTS (
            SELECT 1 FROM execution_external_position_baselines actual
             WHERE actual.baseline_id=baseline.baseline_id
               AND actual.execution_account_id=baseline.execution_account_id
               AND actual.source=baseline.source
               AND actual.observed_at=baseline.observed_at
               AND actual.evidence=baseline.evidence
               AND actual.actor=baseline.actor
               AND actual.reason=baseline.reason
        ) OR EXISTS (
            (SELECT baseline_id,execution_account_id,token_id,condition_id,
                    outcome_index,outcome_name,neg_risk,shares
               FROM _wallet67_baseline_items
             EXCEPT
             SELECT baseline_id,execution_account_id,token_id,condition_id,
                    outcome_index,outcome_name,neg_risk,shares
               FROM execution_external_position_baseline_items
              WHERE execution_account_id=baseline.execution_account_id)
            UNION ALL
            (SELECT baseline_id,execution_account_id,token_id,condition_id,
                    outcome_index,outcome_name,neg_risk,shares
               FROM execution_external_position_baseline_items
              WHERE execution_account_id=baseline.execution_account_id
             EXCEPT
             SELECT baseline_id,execution_account_id,token_id,condition_id,
                    outcome_index,outcome_name,neg_risk,shares
               FROM _wallet67_baseline_items)
        ) THEN
            RAISE EXCEPTION 'wallet-6 has a conflicting immutable position baseline';
        END IF;
    ELSIF existing_headers<>0 THEN
        RAISE EXCEPTION 'wallet-6 has multiple position baseline headers';
    ELSE
        IF EXISTS (SELECT 1 FROM execution_external_position_baselines
                    WHERE baseline_id=baseline.baseline_id)
           OR EXISTS (SELECT 1 FROM execution_external_position_baseline_items
                       WHERE baseline_id=baseline.baseline_id
                          OR execution_account_id=baseline.execution_account_id) THEN
            RAISE EXCEPTION 'wallet-6 has conflicting or orphan baseline rows';
        END IF;
        -- Migration 0014 requires the complete item set before the irreversible header seal.
        INSERT INTO execution_external_position_baseline_items(
            baseline_id,execution_account_id,token_id,condition_id,
            outcome_index,outcome_name,neg_risk,shares
        )
        SELECT baseline_id,execution_account_id,token_id,condition_id,
               outcome_index,outcome_name,neg_risk,shares
          FROM _wallet67_baseline_items ORDER BY token_id;

        INSERT INTO execution_external_position_baselines(
            baseline_id,execution_account_id,source,observed_at,
            evidence,actor,reason
        ) VALUES (
            baseline.baseline_id,baseline.execution_account_id,baseline.source,
            baseline.observed_at,baseline.evidence,baseline.actor,baseline.reason
        );
    END IF;

    FOR target IN SELECT * FROM _wallet67_accounts ORDER BY execution_account_id LOOP
        IF NOT EXISTS (
            SELECT 1 FROM execution_accounts actual
             WHERE actual.execution_account_id=target.execution_account_id
               AND lower(actual.wallet_address)=target.wallet_address
               AND actual.collateral_asset='pUSD'
               AND actual.total_balance=target.target_balance
               AND actual.available_balance=target.target_balance
               AND actual.reserved_balance=0
               AND actual.version=target.expected_version+1
               AND actual.reconciled_at IS NOT DISTINCT FROM target.expected_reconciled_at
        ) THEN
            RAISE EXCEPTION 'execution account % postcondition failed', target.execution_account_id;
        END IF;
    END LOOP;
END;
$wallet67_onboarding$;

SELECT json_build_object(
    'status','APPLIED_OR_ALREADY_APPLIED',
    'accounts',(SELECT json_object_agg(execution_account_id,target_balance ORDER BY execution_account_id)
                  FROM _wallet67_accounts),
    'funding_event_count',(SELECT count(*) FROM _wallet67_events),
    'wallet6_unmanaged_position_count',(SELECT count(*) FROM _wallet67_baseline_items),
    'wallet6_baseline_id',(SELECT baseline_id FROM _wallet67_baseline_meta)
)::text;
COMMIT;
"""


def write_sql(path: pathlib.Path, sql: str) -> None:
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except OSError as error:
        raise OnboardingError(f"cannot create SQL output: {error}") from error
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(sql)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        try:
            path.unlink()
        except OSError:
            pass
        raise


def _database_command(
    database_url: str, database_url_env: str
) -> tuple[list[str], dict[str, str]]:
    parsed = urllib.parse.urlsplit(database_url)
    if parsed.scheme not in {"postgres", "postgresql"}:
        _fail("database URL scheme must be postgres or postgresql")
    host = parsed.hostname or ""
    username = urllib.parse.unquote(parsed.username or "")
    database = urllib.parse.unquote(parsed.path.lstrip("/"))
    if not host or not username or not database:
        _fail("database URL is incomplete")
    environment = os.environ.copy()
    environment.pop(database_url_env, None)
    environment["PGPASSWORD"] = urllib.parse.unquote(parsed.password or "")
    environment["PGCONNECT_TIMEOUT"] = "10"
    environment["PGAPPNAME"] = "wallet67-onboarding-v1"
    query = urllib.parse.parse_qs(parsed.query)
    if query.get("sslmode"):
        environment["PGSSLMODE"] = query["sslmode"][0]
    command = [
        "psql",
        "-X",
        "-A",
        "-t",
        "-v",
        "ON_ERROR_STOP=1",
        "-h",
        host,
        "-p",
        str(parsed.port or 5432),
        "-U",
        username,
        "-d",
        database,
    ]
    return command, environment


def execute_sql(sql: str, database_url: str, database_url_env: str) -> str:
    command, environment = _database_command(database_url, database_url_env)
    try:
        result = subprocess.run(
            command,
            input=sql,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=environment,
            timeout=150,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise OnboardingError(
            "psql execution failed before a known result: "
            f"{type(error).__name__}"
        ) from error
    output = (result.stdout or "").strip()
    if result.returncode != 0:
        # PostgreSQL diagnostics can echo row values or connection metadata.
        # Keep stdout/stderr captured and return only a secret-free status.
        raise OnboardingError(
            f"onboarding transaction failed with psql exit {result.returncode}; "
            "the transaction was not reported committed"
        )
    return output


def summary(evidence: Evidence, *, executed: bool) -> dict[str, object]:
    return {
        "status": "EXECUTED" if executed else "VALIDATED_DRY_RUN",
        "schema_version": SCHEMA_VERSION,
        "evidence_sha256": evidence.sha256,
        "accounts": [
            {
                "execution_account_id": account.execution_account_id,
                "wallet_address": account.wallet_address,
                "target_balance": format(account.target_balance, "f"),
                "funding_event_count": len(account.transfers),
                "reconciled_at_preserved": account.expected_reconciled_at,
            }
            for account in evidence.accounts
        ],
        "wallet6_baseline_id": evidence.baseline.baseline_id,
        "wallet6_unmanaged_position_count": len(evidence.baseline.positions),
        "retain_mode0600_evidence_file": True,
        "funding_events_database_immutable": False,
        "database_access_attempted": executed,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--evidence",
        type=pathlib.Path,
        required=True,
        help="canonical mode-0600 onboarding evidence JSON",
    )
    parser.add_argument(
        "--expected-evidence-sha256",
        default="",
        help="externally approved lowercase SHA-256 (mandatory for execution)",
    )
    parser.add_argument(
        "--sql-output",
        type=pathlib.Path,
        help="exclusively create a mode-0600 review copy of the generated SQL",
    )
    parser.add_argument(
        "--execute-token",
        default="",
        help=f"enable DB access only with the exact token {EXECUTE_TOKEN}",
    )
    parser.add_argument(
        "--database-url-env",
        default=DATABASE_ENV,
        help=f"DB URL environment variable name (default: {DATABASE_ENV})",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        evidence = read_evidence(args.evidence)
        expected_sha = args.expected_evidence_sha256.strip()
        if expected_sha:
            if not SHA256.fullmatch(expected_sha):
                _fail("expected evidence SHA-256 must be 64 lowercase hex characters")
            if expected_sha != evidence.sha256:
                _fail("evidence SHA-256 differs from the externally approved digest")
        sql = generate_sql(evidence)
        if args.sql_output is not None:
            write_sql(args.sql_output, sql)

        executed = bool(args.execute_token)
        if executed:
            if args.execute_token != EXECUTE_TOKEN:
                _fail("exact execute token is required; no database access was attempted")
            if not expected_sha:
                _fail("execution requires --expected-evidence-sha256")
            env_name = args.database_url_env.strip()
            if not ENVIRONMENT_NAME.fullmatch(env_name):
                _fail("database URL environment variable name is invalid")
            database_url = os.environ.get(env_name, "").strip()
            if not database_url:
                _fail(f"execution requires database URL environment variable {env_name}")
            execute_sql(sql, database_url, env_name)

        print(json.dumps(summary(evidence, executed=executed), sort_keys=True))
        return 0
    except OnboardingError as error:
        print(f"wallet67 onboarding: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
