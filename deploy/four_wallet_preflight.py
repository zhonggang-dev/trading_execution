#!/usr/bin/env python3
"""Fail-closed preflight for the four-wallet decision-cycle topology.

The command is intentionally read-only.  It validates the complete routing JSON,
the wallet secret inventory, and the database authorization rows while the
database global kill switch is still closed.  It never changes an environment
file, a database row, or a running service.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import math
import os
import pathlib
import re
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request


EXPECTED_STRATEGIES = frozenset({"multfactor_v1", "multfactor_v2"})
EXPECTED_ROUTES = {
    ("echo", "multfactor_v2"): "main",
    ("echo", "multfactor_v1"): "wallet-1",
    ("gemini_masked", "multfactor_v1"): "wallet-6",
    ("gemini_masked", "multfactor_v2"): "wallet-7",
}
EXPECTED_ACCOUNTS = frozenset(EXPECTED_ROUTES.values())
CURRENT_ROLLOUT_QUARANTINED_ACCOUNTS = frozenset({"wallet-6", "wallet-7"})
ACTIVATABLE_ACCOUNTS = CURRENT_ROLLOUT_QUARANTINED_ACCOUNTS
POLICY_LIMIT_FIELDS = (
    "max_order_notional",
    "max_market_exposure",
    "max_strategy_exposure",
    "max_wallet_exposure",
    "max_daily_traded_notional",
    "max_price_age_ms",
    "max_signal_age_ms",
    "max_state_age_ms",
)
# Current rollout evidence contract. main/wallet-1 values come from the
# production read-only baseline; wallet-6/wallet-7 are the disabled quarantine
# template. This constant is not approval to activate wallet-6/wallet-7: that
# requires a new evidence pass and explicit operator approval.
CURRENT_ROLLOUT_RISK_CONTRACT_BY_ACCOUNT = {
    "main": {
        "max_order_notional": 1.10,
        "max_market_exposure": 2.10,
        "max_strategy_exposure": 2.10,
        "max_wallet_exposure": 2.10,
        "max_daily_traded_notional": 1.10,
        "max_price_age_ms": 90_000,
        "max_signal_age_ms": 30_000,
        "max_state_age_ms": 600_000,
        "daily_timezone": "UTC",
    },
    "wallet-1": {
        "max_order_notional": 1.10,
        "max_market_exposure": 2.10,
        "max_strategy_exposure": 2.10,
        "max_wallet_exposure": 2.10,
        "max_daily_traded_notional": 1.10,
        "max_price_age_ms": 90_000,
        "max_signal_age_ms": 30_000,
        "max_state_age_ms": 600_000,
        "daily_timezone": "UTC",
    },
    "wallet-6": {
        "max_order_notional": 1.10,
        "max_market_exposure": 2.10,
        "max_strategy_exposure": 2.10,
        "max_wallet_exposure": 2.10,
        "max_daily_traded_notional": 1.10,
        "max_price_age_ms": 90_000,
        "max_signal_age_ms": 30_000,
        "max_state_age_ms": 600_000,
        "daily_timezone": "UTC",
    },
    "wallet-7": {
        "max_order_notional": 1.10,
        "max_market_exposure": 2.10,
        "max_strategy_exposure": 2.10,
        "max_wallet_exposure": 2.10,
        "max_daily_traded_notional": 1.10,
        "max_price_age_ms": 90_000,
        "max_signal_age_ms": 30_000,
        "max_state_age_ms": 600_000,
        "daily_timezone": "UTC",
    },
}
MAX_CONFIG_BYTES = 4 << 20
DISABLED_EVIDENCE_SCHEMA = "four_wallet.disabled_preflight.v1"
ACTIVATION_RISK_APPROVAL_SCHEMA = "four_wallet.activation_risk_approval.v1"
ACTIVATION_RISK_DECISION = "APPROVED_FOR_LIVE_ACTIVATION"
IMMUTABLE_RELEASE_IDENTITY = re.compile(r"(?:[0-9a-f]{40}|sha256:[0-9a-f]{64})")

TRADING_RUNTIME_KEYS = frozenset(
    {
        "EXECUTION_MODE",
        "EXECUTION_VENUE",
        "EXECUTION_ALLOW_MARKET_ORDERS",
        "EXECUTION_MAX_ORDER_SIZE",
        "EXECUTION_MAX_ORDER_NOTIONAL",
        "POLYMARKET_LIVE_TRADING_ENABLED",
        "POLYMARKET_MAX_BUY_FEE_RATE_BPS",
        "POLYGON_ORDER_FILLED_CONFIRMATIONS",
        "DECISION_CYCLE_ENABLED",
        "DECISION_CYCLE_ORDER_SUBMISSION_ENABLED",
        "DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED",
        "DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE",
        "DECISION_CYCLE_BINDINGS_JSON",
        "DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON",
        "POLYMARKET_ACCOUNTS_FILE",
        "DECISION_CYCLE_PREDICTION_INFRA_URL",
        "DECISION_CYCLE_PREDICTION_LOOKBACK",
        "DECISION_CYCLE_MID_PRICE_LOOKBACK",
        "DECISION_CYCLE_STRATEGY_URL",
        "DECISION_CYCLE_INTERVAL",
        "DECISION_CYCLE_STARTUP_DELAY",
        "DECISION_CYCLE_MAX_START_LATENESS",
        "DECISION_CYCLE_TIMEOUT",
    }
)
PREDICTION_RUNTIME_KEYS = frozenset(
    {
        "REDIS_ENABLED",
        "REDIS_ADDRESS",
        "REDIS_DATABASE",
        "DIRECT_PREDICTION_ENABLED",
        "REDIS_DIRECT_PREDICTION_STREAM_KEY",
        "DIRECT_PREDICTION_MODEL_IDS_JSON",
        "PREDICTION_RESULT_ENABLED",
        "TRADING_INPUT_ENABLED",
    }
)
TRADING_RUNTIME_SECRET_KEYS = frozenset(
    {
        "TRADING_EXECUTION_DATABASE_URL",
        "EXECUTION_API_TOKEN",
        "POSITION_EXIT_JOB_TOKEN",
        "LIVE_OPERATIONS_READ_ONLY_TOKEN",
        "DECISION_CYCLE_PREDICTION_INFRA_TOKEN",
        "DECISION_CYCLE_STRATEGY_TOKEN",
    }
)
PREDICTION_RUNTIME_SECRET_KEYS = frozenset(
    {
        "DATABASE_URL",
        "REDIS_PASSWORD",
        "TRADING_INPUT_TOKEN",
        "PREDICTION_RESULT_TOKEN",
    }
)

DATABASE_STATE_SQL = r"""
WITH managed_accounts(execution_account_id) AS (
  VALUES ('main'),('wallet-1'),('wallet-6'),('wallet-7')
)
SELECT json_build_object(
  'observed_at',clock_timestamp(),
  'global_kill_switch',(
    SELECT kill_switch FROM execution_risk_global_control WHERE singleton=TRUE
  ),
  'accounts',(
    SELECT COALESCE(json_agg(json_build_object(
      'execution_account_id',execution_account_id,
      'wallet_address',lower(wallet_address)
    ) ORDER BY execution_account_id),'[]'::json)
      FROM execution_accounts
  ),
  'bindings',(
    SELECT COALESCE(json_agg(json_build_object(
      'model_id',model_id,
      'strategy_id',strategy_id,
      'execution_account_id',execution_account_id,
      'enabled',enabled
    ) ORDER BY model_id,strategy_id,execution_account_id),'[]'::json)
      FROM execution_strategy_bindings
  ),
  'risk_policies',(
    SELECT COALESCE(json_agg(json_build_object(
      'execution_account_id',execution_account_id,
      'policy_id',policy_id,
      'version',version,
      'enabled',enabled,
      'max_order_notional',max_order_notional,
      'max_market_exposure',max_market_exposure,
      'max_strategy_exposure',max_strategy_exposure,
      'max_wallet_exposure',max_wallet_exposure,
      'max_daily_traded_notional',max_daily_traded_notional,
      'max_price_age_ms',max_price_age_ms,
      'max_signal_age_ms',max_signal_age_ms,
      'max_state_age_ms',max_state_age_ms,
      'daily_timezone',daily_timezone
    ) ORDER BY execution_account_id),'[]'::json)
      FROM execution_risk_policies
  ),
  'account_controls',(
    SELECT COALESCE(json_agg(json_build_object(
      'execution_account_id',execution_account_id,
      'control_scope',control_scope,
      'control_key',control_key,
      'paused',paused,
      'reason',reason,
      'version',version
    ) ORDER BY execution_account_id),'[]'::json)
      FROM execution_risk_controls
     WHERE control_scope='ACCOUNT' AND control_key=''
  ),
  'dry_runs',(
    SELECT COALESCE(json_agg(row_to_json(run) ORDER BY run.decision_at DESC),'[]'::json)
      FROM (
        SELECT
          decision_at,
          input_payload->>'prediction_snapshot_id' AS prediction_snapshot_id,
          min(created_at) AS created_at,
          json_agg(json_build_object(
            'model_id',model_id,
            'strategy_id',strategy_id,
            'execution_account_id',execution_account_id,
            'order_submission_enabled',order_submission_enabled,
            'has_output',output_payload IS NOT NULL,
            'prediction_count',COALESCE(jsonb_array_length(input_payload->'predictions'),0),
            'entry_policy_enabled',CASE
              WHEN output_payload #> '{entry_policy}' IS NULL THEN NULL
              ELSE (output_payload #>> '{entry_policy,enabled}')::boolean
            END,
            'entry_block_reason',COALESCE(
              output_payload #>> '{entry_policy,block_reason}',''
            )
          ) ORDER BY model_id,strategy_id,execution_account_id) AS bindings
          FROM strategy_decision_runs
         WHERE decision_at >= clock_timestamp() - interval '48 hours'
           AND execution_account_id IN (SELECT execution_account_id FROM managed_accounts)
         GROUP BY decision_at,input_payload->>'prediction_snapshot_id'
         ORDER BY decision_at DESC
         LIMIT 20
      ) AS run
  ),
  'decision_delivery_state',json_build_object(
    'count',(SELECT count(*) FROM strategy_order_intent_deliveries
             WHERE intent_payload->>'execution_account_id' IN (SELECT execution_account_id FROM managed_accounts)),
    'status_counts',json_build_object(
      'PENDING',(SELECT count(*) FROM strategy_order_intent_deliveries WHERE status='PENDING' AND intent_payload->>'execution_account_id' IN (SELECT execution_account_id FROM managed_accounts)),
      'SUBMITTING',(SELECT count(*) FROM strategy_order_intent_deliveries WHERE status='SUBMITTING' AND intent_payload->>'execution_account_id' IN (SELECT execution_account_id FROM managed_accounts)),
      'SUBMITTED',(SELECT count(*) FROM strategy_order_intent_deliveries WHERE status='SUBMITTED' AND intent_payload->>'execution_account_id' IN (SELECT execution_account_id FROM managed_accounts)),
      'FAILED',(SELECT count(*) FROM strategy_order_intent_deliveries WHERE status='FAILED' AND intent_payload->>'execution_account_id' IN (SELECT execution_account_id FROM managed_accounts)),
      'UNKNOWN',(SELECT count(*) FROM strategy_order_intent_deliveries WHERE status='UNKNOWN' AND intent_payload->>'execution_account_id' IN (SELECT execution_account_id FROM managed_accounts))
    ),
    'unsafe_order_status_count',(
      SELECT count(*) FROM strategy_order_intent_deliveries
       WHERE order_status IN ('UNKNOWN','MANUAL_REVIEW')
         AND intent_payload->>'execution_account_id' IN (SELECT execution_account_id FROM managed_accounts)
    ),
    'max_updated_at',(SELECT max(updated_at) FROM strategy_order_intent_deliveries
                      WHERE intent_payload->>'execution_account_id' IN (SELECT execution_account_id FROM managed_accounts)),
    'risky_samples',(
      SELECT COALESCE(json_agg(json_build_object(
        'client_order_id',client_order_id,
        'status',status,
        'order_status',order_status
      ) ORDER BY client_order_id),'[]'::json)
        FROM (
          SELECT client_order_id,status,order_status
            FROM strategy_order_intent_deliveries
           WHERE intent_payload->>'execution_account_id' IN (SELECT execution_account_id FROM managed_accounts)
             AND (status IN ('PENDING','SUBMITTING','UNKNOWN')
               OR order_status IN ('UNKNOWN','MANUAL_REVIEW'))
           ORDER BY client_order_id
           LIMIT 20
        ) AS risky_delivery
    )
  ),
  'manual_review_state',json_build_object(
    'count',(
      SELECT count(*) FROM execution_orders
       WHERE status IN ('UNKNOWN','MANUAL_REVIEW')
         AND execution_account_id IN (SELECT execution_account_id FROM managed_accounts)
    ),
    'orders',(
      SELECT COALESCE(json_agg(json_build_object(
        'order_id',order_id,
        'client_order_id',client_order_id,
        'execution_account_id',execution_account_id,
        'status',status,
        'updated_at',updated_at
      ) ORDER BY order_id),'[]'::json)
        FROM (
          SELECT order_id,client_order_id,execution_account_id,status,updated_at
            FROM execution_orders
           WHERE status IN ('UNKNOWN','MANUAL_REVIEW')
             AND execution_account_id IN (SELECT execution_account_id FROM managed_accounts)
           ORDER BY order_id
           LIMIT 20
      ) AS risky_order
    )
  ),
  'nonterminal_buy_state',json_build_object(
    'count',(
      SELECT count(*) FROM execution_orders
       WHERE intent->>'side'='BUY'
         AND execution_account_id IN (SELECT execution_account_id FROM managed_accounts)
         AND status NOT IN ('FILLED','CANCELLED','REJECTED','MANUAL_REVIEW')
    ),
    'orders',(
      SELECT COALESCE(json_agg(json_build_object(
        'order_id',order_id,
        'client_order_id',client_order_id,
        'execution_account_id',execution_account_id,
        'status',status,
        'updated_at',updated_at
      ) ORDER BY order_id),'[]'::json)
        FROM (
          SELECT order_id,client_order_id,execution_account_id,status,updated_at
            FROM execution_orders
           WHERE intent->>'side'='BUY'
             AND execution_account_id IN (SELECT execution_account_id FROM managed_accounts)
             AND status NOT IN ('FILLED','CANCELLED','REJECTED','MANUAL_REVIEW')
           ORDER BY order_id
           LIMIT 20
        ) AS nonterminal_buy
    )
  )
)::text;
"""


class PreflightError(RuntimeError):
    """A safe-to-report deployment invariant violation."""


@dataclasses.dataclass(frozen=True, order=True)
class Binding:
    prediction_model_id: str
    model_id: str
    strategy_id: str
    execution_account_id: str

    @property
    def authorization(self) -> tuple[str, str, str]:
        return (self.model_id, self.strategy_id, self.execution_account_id)


def _nonempty_string(value: object, field: str, index: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PreflightError(f"binding {index} has an empty or non-string {field}")
    return value.strip()


def decode_bindings(payload: str) -> tuple[Binding, ...]:
    try:
        raw = json.loads(payload)
    except json.JSONDecodeError as error:
        raise PreflightError(f"DECISION_CYCLE_BINDINGS_JSON is invalid JSON: {error.msg}") from error
    if not isinstance(raw, list):
        raise PreflightError("DECISION_CYCLE_BINDINGS_JSON must be one JSON array")
    if len(raw) != 4:
        raise PreflightError(f"four-wallet topology requires exactly 4 bindings, got {len(raw)}")

    allowed_fields = {
        "prediction_model_id",
        "model_id",
        "strategy_id",
        "execution_account_id",
    }
    bindings: list[Binding] = []
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            raise PreflightError(f"binding {index} must be a JSON object")
        unknown = set(item) - allowed_fields
        if unknown:
            raise PreflightError(f"binding {index} contains unknown fields: {sorted(unknown)}")
        model_id = _nonempty_string(item.get("model_id"), "model_id", index)
        prediction_model_id = _nonempty_string(
            item.get("prediction_model_id", model_id), "prediction_model_id", index
        )
        if "EXACT_" in prediction_model_id.upper() or "PLACEHOLDER" in prediction_model_id.upper():
            raise PreflightError(
                f"binding {index} still contains an unresolved prediction_model_id placeholder"
            )
        bindings.append(
            Binding(
                prediction_model_id=prediction_model_id,
                model_id=model_id,
                strategy_id=_nonempty_string(item.get("strategy_id"), "strategy_id", index),
                execution_account_id=_nonempty_string(
                    item.get("execution_account_id"), "execution_account_id", index
                ),
            )
        )
    return validate_topology(bindings)


def decode_submission_disabled_accounts(
    payload: str, bindings: tuple[Binding, ...]
) -> tuple[str, ...]:
    try:
        raw = json.loads(payload)
    except json.JSONDecodeError as error:
        raise PreflightError(
            "DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON is invalid JSON: "
            f"{error.msg}"
        ) from error
    if not isinstance(raw, list):
        raise PreflightError(
            "DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON must be one JSON array"
        )
    accounts: list[str] = []
    for index, value in enumerate(raw):
        if not isinstance(value, str) or not value.strip():
            raise PreflightError(
                "DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON "
                f"account {index} is empty or non-string"
            )
        accounts.append(value.strip())
    if len(set(accounts)) != len(accounts):
        raise PreflightError(
            "DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON contains duplicates"
        )
    bound_accounts = {binding.execution_account_id for binding in bindings}
    unknown = set(accounts) - bound_accounts
    if unknown:
        raise PreflightError(
            "DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON contains unbound accounts: "
            f"{sorted(unknown)}"
        )
    return tuple(sorted(accounts))


def validate_rollout_quarantine(accounts: tuple[str, ...]) -> None:
    unsupported = set(accounts) - ACTIVATABLE_ACCOUNTS
    if unsupported:
        raise PreflightError(
            "this release permits only wallet-6 and wallet-7 in the submission-disabled "
            f"set; unsupported={sorted(unsupported)}"
        )


def active_rollout_bindings(
    bindings: tuple[Binding, ...], quarantined_accounts: tuple[str, ...]
) -> tuple[Binding, ...]:
    quarantined = set(quarantined_accounts)
    result = tuple(
        binding
        for binding in bindings
        if binding.execution_account_id not in quarantined
    )
    if not result:
        raise PreflightError("current rollout quarantine excludes every binding")
    return result


def validate_topology(bindings: list[Binding] | tuple[Binding, ...]) -> tuple[Binding, ...]:
    normalized = tuple(sorted(bindings))
    accounts = {binding.execution_account_id for binding in normalized}
    if accounts != EXPECTED_ACCOUNTS:
        raise PreflightError(
            f"binding accounts must be exactly {sorted(EXPECTED_ACCOUNTS)}, got {sorted(accounts)}"
        )
    if len(accounts) != len(normalized):
        raise PreflightError("each execution account must appear in exactly one binding")

    strategies = {binding.strategy_id for binding in normalized}
    if strategies != EXPECTED_STRATEGIES:
        raise PreflightError(
            f"strategies must be exactly {sorted(EXPECTED_STRATEGIES)}, got {sorted(strategies)}"
        )
    logical_models = {binding.model_id for binding in normalized}
    prediction_models = {binding.prediction_model_id for binding in normalized}
    if len(logical_models) != 2 or len(prediction_models) != 2:
        raise PreflightError("four-wallet topology requires exactly 2 logical and 2 prediction models")

    source_for_logical: dict[str, str] = {}
    logical_for_source: dict[str, str] = {}
    pairs: set[tuple[str, str]] = set()
    authorizations: set[tuple[str, str, str]] = set()
    for binding in normalized:
        previous_source = source_for_logical.setdefault(binding.model_id, binding.prediction_model_id)
        if previous_source != binding.prediction_model_id:
            raise PreflightError(f"logical model {binding.model_id!r} maps to multiple prediction models")
        previous_logical = logical_for_source.setdefault(binding.prediction_model_id, binding.model_id)
        if previous_logical != binding.model_id:
            raise PreflightError(
                f"prediction model {binding.prediction_model_id!r} maps to multiple logical models"
            )
        pair = (binding.model_id, binding.strategy_id)
        if pair in pairs:
            raise PreflightError(f"duplicate logical model/strategy binding: {pair}")
        pairs.add(pair)
        if binding.authorization in authorizations:
            raise PreflightError(f"duplicate database authorization: {binding.authorization}")
        authorizations.add(binding.authorization)

    expected_pairs = set(EXPECTED_ROUTES)
    if pairs != expected_pairs:
        raise PreflightError("bindings must contain the exact echo/gemini_masked route matrix")
    for binding in normalized:
        expected_account = EXPECTED_ROUTES[(binding.model_id, binding.strategy_id)]
        if binding.execution_account_id != expected_account:
            raise PreflightError(
                f"route {binding.model_id!r}/{binding.strategy_id!r} must use "
                f"execution account {expected_account!r}"
            )
    return normalized


def read_environment_file(path: pathlib.Path) -> dict[str, str]:
    payload = _read_bounded_regular_file(path, "environment")
    values: dict[str, str] = {}
    for line_number, source_line in enumerate(payload.decode("utf-8").splitlines(), start=1):
        line = source_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        if "=" not in line:
            raise PreflightError(f"environment line {line_number} is not KEY=VALUE")
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or key in values:
            raise PreflightError(f"environment key is empty or duplicated at line {line_number}")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def _read_bounded_regular_file(path: pathlib.Path, label: str) -> bytes:
    try:
        info = path.lstat()
    except OSError as error:
        raise PreflightError(f"cannot stat {label} file {path}: {error}") from error
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise PreflightError(f"{label} file must be a non-symlink regular file: {path}")
    if info.st_size > MAX_CONFIG_BYTES:
        raise PreflightError(f"{label} file exceeds {MAX_CONFIG_BYTES} bytes")
    try:
        return path.read_bytes()
    except OSError as error:
        raise PreflightError(f"cannot read {label} file {path}: {error}") from error


def wallet_accounts(path: pathlib.Path) -> frozenset[str]:
    payload = _read_bounded_regular_file(path, "wallet")
    mode = path.stat().st_mode
    if stat.S_IMODE(mode) & 0o077:
        raise PreflightError("wallet file must not be accessible to group or other users")
    try:
        root = json.loads(payload)
    except json.JSONDecodeError as error:
        raise PreflightError(f"wallet file is invalid JSON: {error.msg}") from error
    if not isinstance(root, dict) or not root:
        raise PreflightError("wallet file must be a non-empty JSON object")

    if "accounts" in root:
        items = root["accounts"]
        if not isinstance(items, list):
            raise PreflightError("wallet accounts must be a JSON array")
        account_ids = []
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                raise PreflightError(f"wallet account {index} must be a JSON object")
            account_ids.append(_nonempty_string(item.get("execution_account_id"), "execution_account_id", index))
    else:
        if any(not isinstance(item, dict) for item in root.values()):
            raise PreflightError("legacy wallet entries must be JSON objects")
        account_ids = [str(key).strip() for key in root]

    if len(account_ids) != len(set(account_ids)):
        raise PreflightError("wallet file contains a duplicate execution account")
    accounts = frozenset(account_ids)
    if accounts != EXPECTED_ACCOUNTS:
        raise PreflightError(
            f"wallet accounts must be exactly {sorted(EXPECTED_ACCOUNTS)}, got {sorted(accounts)}"
        )
    return accounts


def _required_boolean(environment: dict[str, str], key: str, expected: bool) -> None:
    value = environment.get(key, "").strip().lower()
    if value not in {"true", "false"}:
        raise PreflightError(f"{key} must be explicitly true or false")
    if (value == "true") != expected:
        raise PreflightError(f"{key} must be {str(expected).lower()}")


def _explicit_boolean(environment: dict[str, str], key: str) -> None:
    if environment.get(key, "").strip().lower() not in {"true", "false"}:
        raise PreflightError(f"{key} must be explicitly true or false")


def _required_static_decimal(
    environment: dict[str, str], key: str, *, allow_zero: bool = False
) -> None:
    value = environment.get(key, "").strip()
    if not re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)", value):
        raise PreflightError(f"{key} must be an explicit base-10 decimal")
    try:
        parsed = float(value)
    except ValueError as error:
        raise PreflightError(f"{key} must be an explicit base-10 decimal") from error
    if not math.isfinite(parsed) or parsed < 0 or (parsed == 0 and not allow_zero):
        qualifier = "non-negative" if allow_zero else "positive"
        raise PreflightError(f"{key} must be an explicit {qualifier} base-10 decimal")


def validate_environment(
    environment: dict[str, str], *, submission_state: str, entry_submission_state: str = "allowed"
) -> tuple[Binding, ...]:
    _required_boolean(environment, "EXECUTION_ALLOW_MARKET_ORDERS", False)
    _required_static_decimal(environment, "EXECUTION_MAX_ORDER_SIZE")
    _required_static_decimal(environment, "EXECUTION_MAX_ORDER_NOTIONAL")
    _required_static_decimal(
        environment, "POLYMARKET_MAX_BUY_FEE_RATE_BPS", allow_zero=True
    )
    if float(environment["POLYMARKET_MAX_BUY_FEE_RATE_BPS"].strip()) > 10_000:
        raise PreflightError("POLYMARKET_MAX_BUY_FEE_RATE_BPS must not exceed 10000")
    confirmations = environment.get("POLYGON_ORDER_FILLED_CONFIRMATIONS", "").strip()
    if not re.fullmatch(r"[+-]?\d+", confirmations):
        raise PreflightError(
            "POLYGON_ORDER_FILLED_CONFIRMATIONS must be an explicit integer"
        )
    try:
        confirmation_count = int(confirmations)
    except ValueError as error:
        raise PreflightError(
            "POLYGON_ORDER_FILLED_CONFIRMATIONS must be an explicit integer"
        ) from error
    if confirmation_count < 1 or confirmation_count > 10_000:
        raise PreflightError(
            "POLYGON_ORDER_FILLED_CONFIRMATIONS must be between 1 and 10000"
        )
    mid_price_lookback = environment.get(
        "DECISION_CYCLE_MID_PRICE_LOOKBACK", ""
    ).strip()
    mid_price_lookback_seconds = _duration_seconds(
        mid_price_lookback,
        "DECISION_CYCLE_MID_PRICE_LOOKBACK",
        max_seconds=7 * 24 * 60 * 60,
    )
    if not 2 * 60 * 60 <= mid_price_lookback_seconds <= 7 * 24 * 60 * 60:
        raise PreflightError(
            "DECISION_CYCLE_MID_PRICE_LOOKBACK must be between 2h and 168h"
        )
    _required_boolean(environment, "DECISION_CYCLE_ENABLED", True)
    _required_boolean(environment, "DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE", True)
    if entry_submission_state not in {"allowed", "blocked"}:
        raise PreflightError("entry submission state must be allowed or blocked")
    _required_boolean(
        environment,
        "DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED",
        entry_submission_state == "blocked",
    )
    if submission_state != "either":
        _required_boolean(
            environment,
            "DECISION_CYCLE_ORDER_SUBMISSION_ENABLED",
            submission_state == "enabled",
        )
    bindings = decode_bindings(environment.get("DECISION_CYCLE_BINDINGS_JSON", ""))
    quarantined_accounts = decode_submission_disabled_accounts(
        environment.get("DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON", "[]"),
        bindings,
    )
    validate_rollout_quarantine(quarantined_accounts)
    wallet_path = environment.get("POLYMARKET_ACCOUNTS_FILE", "").strip()
    if not wallet_path:
        raise PreflightError("POLYMARKET_ACCOUNTS_FILE is required")
    wallet_accounts(pathlib.Path(wallet_path))
    if not environment.get("TRADING_EXECUTION_DATABASE_URL", "").strip():
        raise PreflightError("TRADING_EXECUTION_DATABASE_URL is required")
    lookback = environment.get("DECISION_CYCLE_PREDICTION_LOOKBACK", "").strip()
    if not lookback:
        raise PreflightError("DECISION_CYCLE_PREDICTION_LOOKBACK must be explicit")
    _whole_duration_seconds(lookback, "DECISION_CYCLE_PREDICTION_LOOKBACK")
    return bindings


def validate_prediction_environment(
    environment: dict[str, str],
    bindings: tuple[Binding, ...],
    *,
    required_bindings: tuple[Binding, ...] | None = None,
) -> None:
    _required_boolean(environment, "REDIS_ENABLED", True)
    _required_boolean(environment, "DIRECT_PREDICTION_ENABLED", True)
    _required_boolean(environment, "PREDICTION_RESULT_ENABLED", True)
    _required_boolean(environment, "TRADING_INPUT_ENABLED", True)
    stream_key = environment.get("REDIS_DIRECT_PREDICTION_STREAM_KEY", "").strip()
    if not stream_key:
        raise PreflightError("REDIS_DIRECT_PREDICTION_STREAM_KEY is required")
    if not environment.get("REDIS_ADDRESS", "").strip():
        raise PreflightError("REDIS_ADDRESS must be explicit for Direct consumer verification")
    if not environment.get("DATABASE_URL", "").strip():
        raise PreflightError("Prediction DATABASE_URL must be explicit")
    if "REDIS_PASSWORD" not in environment:
        raise PreflightError("REDIS_PASSWORD must be explicit, including an explicit empty value")
    try:
        redis_database = int(environment.get("REDIS_DATABASE", ""))
    except ValueError as error:
        raise PreflightError("REDIS_DATABASE must be an explicit integer") from error
    if redis_database < 0 or redis_database > 15:
        raise PreflightError("REDIS_DATABASE must be between 0 and 15")
    try:
        configured_models = json.loads(
            environment.get("DIRECT_PREDICTION_MODEL_IDS_JSON", "")
        )
    except json.JSONDecodeError as error:
        raise PreflightError(
            f"DIRECT_PREDICTION_MODEL_IDS_JSON is invalid JSON: {error.msg}"
        ) from error
    if not isinstance(configured_models, list) or any(
        not isinstance(value, str) or not value.strip() for value in configured_models
    ):
        raise PreflightError("DIRECT_PREDICTION_MODEL_IDS_JSON must be a non-empty string array")
    normalized_models = [value.strip() for value in configured_models]
    if len(normalized_models) != len(set(normalized_models)):
        raise PreflightError("DIRECT_PREDICTION_MODEL_IDS_JSON contains duplicate models")
    configured_model_set = set(normalized_models)
    known_models = {binding.prediction_model_id for binding in bindings}
    required_models = {
        binding.prediction_model_id
        for binding in (bindings if required_bindings is None else required_bindings)
    }
    unknown_models = configured_model_set - known_models
    missing_models = required_models - configured_model_set
    if unknown_models or missing_models:
        raise PreflightError(
            "Prediction Direct model set must include every active Trading prediction model "
            "and no unconfigured model; "
            f"missing={sorted(missing_models)}, unknown={sorted(unknown_models)}"
        )


def _credential_sha256(key: str, value: str) -> str:
    normalized: object = value.strip()
    if key in {"TRADING_EXECUTION_DATABASE_URL", "DATABASE_URL"}:
        try:
            parsed = urllib.parse.urlsplit(value.strip())
            hostname = parsed.hostname
            port = parsed.port or 5432
        except ValueError as error:
            raise PreflightError(f"{key} is malformed") from error
        if parsed.scheme not in {"postgres", "postgresql"} or not hostname:
            raise PreflightError(f"{key} must be a PostgreSQL URL")
        normalized = {
            "scheme": "postgresql",
            "host": hostname.lower(),
            "port": port,
            "user": urllib.parse.unquote(parsed.username or ""),
            "password": urllib.parse.unquote(parsed.password or ""),
            "database": urllib.parse.unquote(parsed.path.lstrip("/")),
            "query": sorted(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)),
        }
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def validate_cross_service_credentials(
    trading_environment: dict[str, str], prediction_environment: dict[str, str]
) -> None:
    trading_snapshot_token = trading_environment.get(
        "DECISION_CYCLE_PREDICTION_INFRA_TOKEN", ""
    ).strip()
    prediction_snapshot_token = prediction_environment.get("TRADING_INPUT_TOKEN", "").strip()
    prediction_result_token = prediction_environment.get("PREDICTION_RESULT_TOKEN", "").strip()
    strategy_token = trading_environment.get("DECISION_CYCLE_STRATEGY_TOKEN", "").strip()
    if (
        not trading_snapshot_token
        or not prediction_snapshot_token
        or not prediction_result_token
        or not strategy_token
    ):
        raise PreflightError("Prediction snapshot/result and Trading strategy credentials must be non-empty")
    if trading_snapshot_token != prediction_snapshot_token:
        raise PreflightError(
            "Trading snapshot credential does not match Prediction input credential"
        )
    if prediction_result_token == prediction_snapshot_token:
        raise PreflightError("Prediction result and Trading input credentials must be distinct")


def decode_model_groups(
    payload: str,
    bindings: tuple[Binding, ...],
    *,
    required_bindings: tuple[Binding, ...] | None = None,
) -> dict[str, str]:
    try:
        raw = json.loads(payload)
    except json.JSONDecodeError as error:
        raise PreflightError(f"Direct model-group mapping is invalid JSON: {error.msg}") from error
    if not isinstance(raw, dict):
        raise PreflightError("Direct model-group mapping must be one JSON object")
    known_models = {binding.prediction_model_id for binding in bindings}
    required_models = {
        binding.prediction_model_id
        for binding in (bindings if required_bindings is None else required_bindings)
    }
    unknown_models = set(raw) - known_models
    missing_models = required_models - set(raw)
    if unknown_models or missing_models:
        raise PreflightError(
            "Direct model-group mapping must include every active Trading prediction model "
            "and no unconfigured model; "
            f"missing={sorted(missing_models)}, unknown={sorted(unknown_models)}"
        )
    result: dict[str, str] = {}
    for model_id, group in raw.items():
        if not isinstance(group, str) or not group.strip():
            raise PreflightError(f"Direct consumer group is empty for model {model_id!r}")
        if model_id in required_models:
            result[model_id] = group.strip()
    if len(set(result.values())) != len(result):
        raise PreflightError(
            "per-model Direct consumer groups must be distinct; consumers for different "
            "models in one Redis group would compete for tasks"
        )
    return dict(sorted(result.items()))


def validate_database_state(
    state: object,
    bindings: tuple[Binding, ...],
    *,
    submission_state: str = "disabled",
    submission_disabled_accounts: tuple[str, ...] = tuple(
        sorted(CURRENT_ROLLOUT_QUARANTINED_ACCOUNTS)
    ),
    approved_risk_contracts: dict[str, dict[str, object]] | None = None,
) -> str:
    if not isinstance(state, dict):
        raise PreflightError("database preflight result must be a JSON object")
    if submission_state not in {"disabled", "enabled"}:
        raise PreflightError("database submission state must be disabled or enabled")
    if state.get("global_kill_switch") is not True:
        raise PreflightError("database global kill switch must remain enabled during preflight")
    disabled = set(submission_disabled_accounts)
    validate_rollout_quarantine(tuple(sorted(disabled)))
    unknown_disabled = disabled - EXPECTED_ACCOUNTS
    if unknown_disabled:
        raise PreflightError(
            f"database quarantine contains unexpected accounts: {sorted(unknown_disabled)}"
        )

    raw_accounts = state.get("accounts")
    if not isinstance(raw_accounts, list):
        raise PreflightError("database accounts result must be an array")
    database_accounts: dict[str, str] = {}
    for item in raw_accounts:
        if not isinstance(item, dict) or not isinstance(item.get("execution_account_id"), str):
            raise PreflightError("database returned an invalid execution account row")
        account_id = item["execution_account_id"].strip()
        wallet_address = item.get("wallet_address")
        if (
            not account_id
            or account_id in database_accounts
            or not isinstance(wallet_address, str)
            or not wallet_address.strip()
        ):
            raise PreflightError("database returned a duplicate or invalid execution account identity")
        database_accounts[account_id] = wallet_address.strip().lower()
    missing_accounts = EXPECTED_ACCOUNTS - set(database_accounts)
    if missing_accounts:
        raise PreflightError(f"database is missing execution accounts: {sorted(missing_accounts)}")

    raw_bindings = state.get("bindings")
    if not isinstance(raw_bindings, list):
        raise PreflightError("database bindings result must be an array")
    database_bindings: dict[tuple[str, str, str], dict[str, object]] = {}
    for item in raw_bindings:
        if not isinstance(item, dict):
            raise PreflightError("database returned an invalid strategy binding row")
        try:
            authorization = (
                str(item["model_id"]).strip(),
                str(item["strategy_id"]).strip(),
                str(item["execution_account_id"]).strip(),
            )
        except KeyError as error:
            raise PreflightError(f"database binding is missing {error.args[0]}") from error
        if (
            any(not value for value in authorization)
            or not isinstance(item.get("enabled"), bool)
            or authorization in database_bindings
        ):
            raise PreflightError(f"database returned duplicate or invalid binding {authorization}")
        database_bindings[authorization] = item

    expected = {binding.authorization for binding in bindings}
    missing_rows = expected - set(database_bindings)
    if missing_rows:
        raise PreflightError(
            f"database is missing configured strategy binding rows: {sorted(missing_rows)}"
        )
    expected_enabled = {
        authorization for authorization in expected if authorization[2] not in disabled
    }
    enabled = {
        authorization
        for authorization, item in database_bindings.items()
        if item["enabled"] is True
    }
    if enabled != expected_enabled:
        missing = sorted(expected_enabled - enabled)
        unexpected = sorted(enabled - expected_enabled)
        raise PreflightError(
            f"enabled database authorizations are not the active managed-wallet set; "
            f"missing={missing}, unexpected={unexpected}"
        )
    for authorization in expected:
        should_enable = authorization in expected_enabled
        if database_bindings[authorization]["enabled"] is not should_enable:
            raise PreflightError(
                f"configured database binding {authorization} must have enabled={should_enable}"
            )

    raw_policies = state.get("risk_policies")
    if not isinstance(raw_policies, list):
        raise PreflightError("database risk policies result must be an array")
    policies: dict[str, dict[str, object]] = {}
    for item in raw_policies:
        if not isinstance(item, dict):
            raise PreflightError("database returned an invalid risk policy row")
        account_id = item.get("execution_account_id")
        policy_id = item.get("policy_id")
        version = item.get("version")
        enabled_policy = item.get("enabled")
        if (
            not isinstance(account_id, str)
            or not account_id.strip()
            or account_id.strip() in policies
            or not isinstance(policy_id, str)
            or not policy_id.strip()
            or not isinstance(version, int)
            or isinstance(version, bool)
            or version < 1
            or not isinstance(enabled_policy, bool)
        ):
            raise PreflightError("database returned a duplicate or invalid risk policy")
        policies[account_id.strip()] = item
    for account_id in sorted(EXPECTED_ACCOUNTS):
        policy = policies.get(account_id)
        if policy is None:
            raise PreflightError(f"database is missing risk policy for {account_id!r}")
        expected_policy_enabled = account_id not in disabled
        if policy["enabled"] is not expected_policy_enabled:
            raise PreflightError(
                f"database risk policy for {account_id!r} must have "
                f"enabled={expected_policy_enabled}"
            )
        for field in POLICY_LIMIT_FIELDS:
            value = policy.get(field)
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(value)
                or value <= 0
            ):
                raise PreflightError(
                    f"database risk policy for {account_id!r} has invalid {field}"
                )
        if policy.get("daily_timezone") != "UTC":
            raise PreflightError(
                f"database risk policy for {account_id!r} must use daily_timezone UTC"
            )
    approved_risk_contracts = approved_risk_contracts or {}
    activated_wallets = ACTIVATABLE_ACCOUNTS - disabled
    if set(approved_risk_contracts) != activated_wallets:
        raise PreflightError(
            "active wallet-6/wallet-7 risk approvals do not match the activation set; "
            f"missing={sorted(activated_wallets - set(approved_risk_contracts))}, "
            f"unexpected={sorted(set(approved_risk_contracts) - activated_wallets)}"
        )
    for account_id in sorted(EXPECTED_ACCOUNTS):
        contract = {
            field: policies[account_id][field] for field in POLICY_LIMIT_FIELDS
        }
        contract["daily_timezone"] = policies[account_id]["daily_timezone"]
        if account_id in activated_wallets:
            approval = approved_risk_contracts[account_id]
            if (
                approval.get("policy_id") != policies[account_id]["policy_id"]
                or approval.get("version") != policies[account_id]["version"]
            ):
                raise PreflightError(
                    f"database risk policy identity for {account_id!r} differs from its "
                    "approved activation artifact"
                )
            rollout_contract = {
                field: approval[field] for field in POLICY_LIMIT_FIELDS
            }
            rollout_contract["daily_timezone"] = approval["daily_timezone"]
        else:
            rollout_contract = CURRENT_ROLLOUT_RISK_CONTRACT_BY_ACCOUNT[account_id]
        if contract != rollout_contract:
            raise PreflightError(
                f"database risk policy contract for {account_id!r} differs from its "
                f"reviewed {'activation artifact' if account_id in activated_wallets else 'current-rollout contract'}"
            )

    raw_controls = state.get("account_controls")
    if not isinstance(raw_controls, list):
        raise PreflightError("database ACCOUNT controls result must be an array")
    controls: dict[str, dict[str, object]] = {}
    for item in raw_controls:
        if not isinstance(item, dict):
            raise PreflightError("database returned an invalid ACCOUNT control row")
        account_id = item.get("execution_account_id")
        paused = item.get("paused")
        reason = item.get("reason")
        version = item.get("version")
        control_scope = item.get("control_scope")
        control_key = item.get("control_key")
        if (
            not isinstance(account_id, str)
            or not account_id.strip()
            or account_id.strip() in controls
            or not isinstance(paused, bool)
            or not isinstance(reason, str)
            or not reason.strip()
            or not isinstance(version, int)
            or isinstance(version, bool)
            or version < 1
            or control_scope != "ACCOUNT"
            or control_key != ""
        ):
            raise PreflightError("database returned a duplicate or invalid ACCOUNT control")
        controls[account_id.strip()] = item
    for account_id in sorted(EXPECTED_ACCOUNTS):
        control = controls.get(account_id)
        if control is None:
            raise PreflightError(f"database is missing ACCOUNT control for {account_id!r}")
        expected_paused = account_id in disabled
        if control["paused"] is not expected_paused:
            expected_state = "paused" if expected_paused else "unpaused"
            raise PreflightError(
                f"account quarantine requires {account_id!r} ACCOUNT control "
                f"to be {expected_state}"
            )
    return _canonical_sha256(
        {
            "accounts": {
                account_id: database_accounts[account_id]
                for account_id in sorted(EXPECTED_ACCOUNTS)
            },
            "bindings": [
                database_bindings[authorization]
                for authorization in sorted(expected)
            ],
            "risk_policies": [
                policies[account_id] for account_id in sorted(EXPECTED_ACCOUNTS)
            ],
            "account_controls": [
                controls[account_id] for account_id in sorted(EXPECTED_ACCOUNTS)
            ],
        }
    )


def validate_database_observation(
    state: object, *, now: dt.datetime, max_age: dt.timedelta
) -> None:
    if not isinstance(state, dict):
        raise PreflightError("database preflight result must be a JSON object")
    _fresh_observation(
        state.get("observed_at"),
        "database observed_at",
        now=now,
        max_age=max_age,
    )


def validate_delivery_state(
    state: object, *, entry_submission_state: str = "allowed"
) -> dict[str, object]:
    if not isinstance(state, dict):
        raise PreflightError("database delivery state must be a JSON object")
    if entry_submission_state not in {"allowed", "blocked"}:
        raise PreflightError("entry submission state must be allowed or blocked")
    status_names = ("PENDING", "SUBMITTING", "SUBMITTED", "FAILED", "UNKNOWN")
    delivery_summary = state.get("decision_delivery_state")
    risky_samples: list[object]
    if isinstance(delivery_summary, dict):
        delivery_count = delivery_summary.get("count")
        raw_counts = delivery_summary.get("status_counts")
        unsafe_order_status_count = delivery_summary.get("unsafe_order_status_count")
        risky_samples = delivery_summary.get("risky_samples")  # type: ignore[assignment]
        if (
            not isinstance(delivery_count, int)
            or delivery_count < 0
            or not isinstance(raw_counts, dict)
            or set(raw_counts) != set(status_names)
            or not isinstance(unsafe_order_status_count, int)
            or unsafe_order_status_count < 0
            or not isinstance(risky_samples, list)
            or len(risky_samples) > 20
        ):
            raise PreflightError("database returned an invalid decision delivery summary")
        status_counts = {}
        for name in status_names:
            count = raw_counts.get(name)
            if not isinstance(count, int) or count < 0:
                raise PreflightError("database returned an invalid decision delivery status count")
            status_counts[name] = count
        if sum(status_counts.values()) != delivery_count:
            raise PreflightError("decision delivery status counts do not equal the total")
        raw_max_updated_at = delivery_summary.get("max_updated_at")
        if delivery_count == 0:
            if raw_max_updated_at is not None:
                raise PreflightError("empty decision delivery state has a non-empty watermark")
            max_updated_at = None
        else:
            max_updated_at = _utc_timestamp(
                raw_max_updated_at, "decision delivery max_updated_at"
            )
    else:
        # Backward-compatible offline evidence. Direct PostgreSQL collection uses
        # the bounded aggregate above and never materializes the terminal history.
        deliveries = state.get("decision_deliveries")
        if not isinstance(deliveries, list):
            raise PreflightError("database preflight did not return decision delivery state")
        seen: set[str] = set()
        status_counts = {name: 0 for name in status_names}
        unsafe_order_status_count = 0
        risky_samples = []
        max_updated_at: dt.datetime | None = None
        for row in deliveries:
            if not isinstance(row, dict):
                raise PreflightError("database returned an invalid decision delivery")
            client_order_id = row.get("client_order_id")
            status = row.get("status")
            if (
                not isinstance(client_order_id, str)
                or not client_order_id
                or client_order_id in seen
            ):
                raise PreflightError(
                    "database returned duplicate or invalid decision delivery identity"
                )
            seen.add(client_order_id)
            if not isinstance(status, str) or status not in status_counts:
                raise PreflightError("database returned an invalid decision delivery status")
            status_counts[status] += 1
            if status in {"PENDING", "SUBMITTING", "UNKNOWN"}:
                risky_samples.append(row)
            if row.get("order_status") in {"UNKNOWN", "MANUAL_REVIEW"}:
                unsafe_order_status_count += 1
                risky_samples.append(row)
            updated_at = _utc_timestamp(
                row.get("updated_at"), "decision delivery updated_at"
            )
            if max_updated_at is None or updated_at > max_updated_at:
                max_updated_at = updated_at
        delivery_count = len(deliveries)

    manual_summary = state.get("manual_review_state")
    if isinstance(manual_summary, dict):
        manual_review_count = manual_summary.get("count")
        review_orders = manual_summary.get("orders")
        if (
            not isinstance(manual_review_count, int)
            or manual_review_count < 0
            or not isinstance(review_orders, list)
            or len(review_orders) > 20
            or len(review_orders) > manual_review_count
        ):
            raise PreflightError("database returned an invalid manual-review order summary")
    else:
        review_orders = state.get("manual_review_orders")
        if not isinstance(review_orders, list):
            raise PreflightError("database preflight did not return manual-review order state")
        manual_review_count = len(review_orders)

    buy_summary = state.get("nonterminal_buy_state")
    if isinstance(buy_summary, dict):
        nonterminal_buy_count = buy_summary.get("count")
        nonterminal_buy_orders = buy_summary.get("orders")
        if (
            not isinstance(nonterminal_buy_count, int)
            or nonterminal_buy_count < 0
            or not isinstance(nonterminal_buy_orders, list)
            or len(nonterminal_buy_orders) > 20
            or len(nonterminal_buy_orders) > nonterminal_buy_count
        ):
            raise PreflightError("database returned an invalid nonterminal BUY summary")
    else:
        nonterminal_buy_orders = state.get("nonterminal_buy_orders")
        if not isinstance(nonterminal_buy_orders, list):
            raise PreflightError("database preflight did not return nonterminal BUY state")
        nonterminal_buy_count = len(nonterminal_buy_orders)

    risky_total = (
        status_counts["PENDING"]
        + status_counts["SUBMITTING"]
        + status_counts["UNKNOWN"]
        + unsafe_order_status_count
    )
    if risky_total:
        identities = []
        for row in risky_samples[:20]:
            if isinstance(row, dict):
                identities.append(
                    f"{row.get('client_order_id')}:{row.get('status')}:"
                    f"{row.get('order_status')}"
                )
        raise PreflightError(
            "decision delivery recovery is not drained; manually resolve before restart: "
            + ", ".join(identities or [f"unsafe_count={risky_total}"])
        )
    if manual_review_count:
        identities = []
        for row in review_orders[:20]:
            if isinstance(row, dict):
                identities.append(f"{row.get('order_id')}:{row.get('status')}")
        raise PreflightError(
            "execution orders require explicit UNKNOWN/MANUAL_REVIEW resolution before "
            "cutover: " + ", ".join(identities or [f"unsafe_count={manual_review_count}"])
        )
    if entry_submission_state == "blocked" and nonterminal_buy_count:
        identities = []
        for row in nonterminal_buy_orders[:20]:
            if isinstance(row, dict):
                identities.append(f"{row.get('order_id')}:{row.get('status')}")
        raise PreflightError(
            "sell-only cutover requires zero nonterminal BUY execution orders: "
            + ", ".join(identities or [f"unsafe_count={nonterminal_buy_count}"])
        )

    watermark: dict[str, object] = {
        "count": delivery_count,
        "status_counts": status_counts,
        "non_terminal_counts": {
            "PENDING": status_counts["PENDING"],
            "SUBMITTING": status_counts["SUBMITTING"],
            "UNKNOWN": status_counts["UNKNOWN"],
            "DELIVERY_ORDER_UNKNOWN_OR_MANUAL_REVIEW": unsafe_order_status_count,
            "EXECUTION_ORDER_UNKNOWN_OR_MANUAL_REVIEW": manual_review_count,
            "NONTERMINAL_BUY_EXECUTION_ORDER": nonterminal_buy_count,
        },
        "max_updated_at": (
            max_updated_at.isoformat().replace("+00:00", "Z")
            if max_updated_at is not None
            else None
        ),
    }
    watermark["sha256"] = _canonical_sha256(watermark)
    return watermark


def _utc_timestamp(value: object, field: str) -> dt.datetime:
    if not isinstance(value, str) or not value.strip():
        raise PreflightError(f"{field} must be an RFC3339 timestamp")
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(normalized)
    except ValueError as error:
        raise PreflightError(f"{field} must be an RFC3339 timestamp") from error
    if parsed.tzinfo is None:
        raise PreflightError(f"{field} must include a timezone")
    return parsed.astimezone(dt.timezone.utc)


def _fresh_observation(
    value: object, field: str, *, now: dt.datetime, max_age: dt.timedelta
) -> dt.datetime:
    observed_at = _utc_timestamp(value, field)
    if observed_at > now + dt.timedelta(seconds=5):
        raise PreflightError(f"{field} is in the future")
    if now - observed_at > max_age:
        raise PreflightError(f"{field} is stale")
    return observed_at


def validate_runtime_state(
    state: object,
    trading_environment: dict[str, str],
    prediction_environment: dict[str, str],
    *,
    expected_trading_commit: str,
    expected_prediction_version: str,
    now: dt.datetime,
    max_age: dt.timedelta,
) -> dt.datetime:
    if not isinstance(state, dict):
        raise PreflightError("runtime evidence must be a JSON object")
    _fresh_observation(state.get("observed_at"), "runtime observed_at", now=now, max_age=max_age)

    services = {
        "trading": (
            trading_environment,
            TRADING_RUNTIME_KEYS,
            TRADING_RUNTIME_SECRET_KEYS,
        ),
        "prediction": (
            prediction_environment,
            PREDICTION_RUNTIME_KEYS,
            PREDICTION_RUNTIME_SECRET_KEYS,
        ),
    }
    starts: dict[str, dt.datetime] = {}
    for name, (expected_environment, keys, secret_keys) in services.items():
        service = state.get(name)
        if not isinstance(service, dict):
            raise PreflightError(f"runtime evidence is missing {name} process")
        if not isinstance(service.get("pid"), int) or service["pid"] < 1:
            raise PreflightError(f"runtime {name} pid is invalid")
        starts[name] = _utc_timestamp(service.get("started_at"), f"runtime {name} started_at")
        if starts[name] > now:
            raise PreflightError(f"runtime {name} started_at is in the future")
        actual_environment = service.get("environment")
        if not isinstance(actual_environment, dict):
            raise PreflightError(f"runtime {name} environment is missing")
        for key in sorted(keys):
            if actual_environment.get(key) != expected_environment.get(key):
                raise PreflightError(
                    f"runtime {name} environment does not match the audited file for {key}"
                )
        actual_secret_hashes = service.get("secret_sha256")
        if not isinstance(actual_secret_hashes, dict):
            raise PreflightError(f"runtime {name} credential fingerprints are missing")
        for key in sorted(secret_keys):
            expected_secret = expected_environment.get(key, "").strip()
            if actual_secret_hashes.get(key) != _credential_sha256(key, expected_secret):
                raise PreflightError(
                    f"runtime {name} credential fingerprint does not match for {key}"
                )

    trading_health = state["trading"].get("health")
    if not isinstance(trading_health, dict) or trading_health.get("status") != "ok":
        raise PreflightError("Trading runtime liveness is not ok")
    if trading_health.get("commit") != expected_trading_commit:
        raise PreflightError("Trading runtime commit does not match the approved release")

    prediction_health = state["prediction"].get("health")
    if not isinstance(prediction_health, dict) or prediction_health.get("status") != "up":
        raise PreflightError("Prediction runtime liveness is not up")
    if prediction_health.get("version") != expected_prediction_version:
        raise PreflightError("Prediction runtime version does not match the approved release")
    return starts["trading"]


def validate_runtime_processes_unchanged(initial: object, final: object) -> None:
    if not isinstance(initial, dict) or not isinstance(final, dict):
        raise PreflightError("runtime continuity evidence must be JSON objects")
    for name in ("trading", "prediction"):
        initial_service = initial.get(name)
        final_service = final.get(name)
        if not isinstance(initial_service, dict) or not isinstance(final_service, dict):
            raise PreflightError(f"runtime continuity evidence is missing {name}")
        if (
            final_service.get("pid") != initial_service.get("pid")
            or final_service.get("started_at") != initial_service.get("started_at")
        ):
            raise PreflightError(
                f"runtime {name} process changed during enabled preflight"
            )


def validate_dry_run_state(
    state: object,
    bindings: tuple[Binding, ...],
    *,
    submission_disabled_accounts: tuple[str, ...] = (),
    not_before: dt.datetime,
    now: dt.datetime,
    max_age: dt.timedelta,
    required: dict[str, object] | None = None,
    entry_submission_state: str = "allowed",
) -> dict[str, object]:
    if entry_submission_state not in {"allowed", "blocked"}:
        raise PreflightError("entry submission state must be allowed or blocked")
    if not isinstance(state, dict) or not isinstance(state.get("dry_runs"), list):
        raise PreflightError("database preflight did not return dry-run evidence")
    candidates: list[tuple[dt.datetime, dict[str, object]]] = []
    for item in state["dry_runs"]:
        if not isinstance(item, dict):
            raise PreflightError("database returned an invalid dry-run group")
        decision_at = _utc_timestamp(item.get("decision_at"), "dry-run decision_at")
        created_at = _utc_timestamp(item.get("created_at"), "dry-run created_at")
        if created_at >= not_before - dt.timedelta(seconds=2):
            candidates.append((decision_at, item))
    if not candidates:
        raise PreflightError("no qualifying submission=false decision-cycle run was recorded")
    if required is None:
        latest_decision_at = max(item[0] for item in candidates)
        latest = [item for item in candidates if item[0] == latest_decision_at]
        if len(latest) != 1:
            raise PreflightError("latest decision boundary has ambiguous Prediction snapshots")
        decision_at, selected = latest[0]
    else:
        required_decision_at = _utc_timestamp(
            required.get("decision_at"), "disabled evidence dry-run decision_at"
        )
        required_snapshot_id = required.get("prediction_snapshot_id")
        matching = [
            item
            for item in candidates
            if item[0] == required_decision_at
            and item[1].get("prediction_snapshot_id") == required_snapshot_id
        ]
        if len(matching) != 1:
            raise PreflightError("disabled evidence dry run is missing or ambiguous in PostgreSQL")
        decision_at, selected = matching[0]
    if now - decision_at > max_age:
        raise PreflightError("latest current-process decision-cycle dry run is stale")
    if decision_at > now + dt.timedelta(seconds=5):
        raise PreflightError("latest decision-cycle dry run is in the future")
    snapshot_id = selected.get("prediction_snapshot_id")
    if not isinstance(snapshot_id, str) or not snapshot_id.strip():
        raise PreflightError("dry-run prediction_snapshot_id is missing")
    disabled = set(submission_disabled_accounts)
    configured_accounts = {binding.execution_account_id for binding in bindings}
    if not disabled <= configured_accounts:
        raise PreflightError("dry-run quarantine contains an unbound execution account")
    expected = {
        binding.authorization
        for binding in bindings
        if binding.execution_account_id not in disabled
    }
    if not expected:
        raise PreflightError("submission-disabled accounts cannot exclude every dry-run binding")
    rows = selected.get("bindings")
    if not isinstance(rows, list) or len(rows) != len(expected):
        raise PreflightError(
            f"latest dry run must contain exactly {len(expected)} active binding rows"
        )
    actual: set[tuple[str, str, str]] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise PreflightError("latest dry run contains an invalid binding row")
        authorization = (
            str(row.get("model_id", "")).strip(),
            str(row.get("strategy_id", "")).strip(),
            str(row.get("execution_account_id", "")).strip(),
        )
        if authorization in actual:
            raise PreflightError(f"latest dry run duplicated binding {authorization}")
        actual.add(authorization)
        if row.get("order_submission_enabled") is not False:
            raise PreflightError("latest binding run was not recorded with submission=false")
        if row.get("has_output") is not True:
            raise PreflightError(f"latest binding run {authorization} did not complete")
        if not isinstance(row.get("prediction_count"), int) or row["prediction_count"] < 1:
            raise PreflightError(f"latest binding run {authorization} received no predictions")
        policy_enabled = row.get("entry_policy_enabled")
        block_reason = row.get("entry_block_reason")
        if entry_submission_state == "blocked":
            if policy_enabled is not False or block_reason != "ENTRY_SUBMISSION_DISABLED":
                raise PreflightError(
                    f"latest binding run {authorization} does not carry the exact "
                    "sell-only entry policy"
                )
        elif (policy_enabled is not None and policy_enabled is not True) or block_reason not in {"", None}:
            raise PreflightError(f"latest binding run {authorization} has an entry block")
    if actual != expected:
        raise PreflightError(
            f"latest dry-run bindings differ from the atomic configuration; "
            f"missing={sorted(expected - actual)}, unexpected={sorted(actual - expected)}"
        )
    return {
        "decision_at": decision_at.isoformat().replace("+00:00", "Z"),
        "prediction_snapshot_id": snapshot_id.strip(),
    }


def _expectation_recency(item: dict[str, object]) -> tuple[dt.datetime, dt.datetime, int, int, str]:
    try:
        selection_run_id = int(item["selection_run_id"])
        selection_id = int(item["selection_id"])
    except (KeyError, TypeError, ValueError) as error:
        raise PreflightError("snapshot manifest contains invalid selection identity") from error
    prediction_id = item.get("prediction_id")
    if not isinstance(prediction_id, str) or not prediction_id.strip():
        raise PreflightError("snapshot manifest contains an invalid prediction_id")
    return (
        _utc_timestamp(item.get("prediction_as_of"), "manifest prediction_as_of"),
        _utc_timestamp(item.get("task_available_at"), "manifest task_available_at"),
        selection_run_id,
        selection_id,
        prediction_id,
    )


def validate_snapshot_manifest(
    payload: object, dry_run: dict[str, object], bindings: tuple[Binding, ...]
) -> int:
    if not isinstance(payload, dict):
        raise PreflightError("Prediction snapshot evidence must be a JSON object")
    snapshot = payload.get("data", payload)
    if not isinstance(snapshot, dict):
        raise PreflightError("Prediction snapshot data is missing")
    if snapshot.get("snapshot_id") != dry_run["prediction_snapshot_id"]:
        raise PreflightError("Prediction snapshot ID does not match the audited dry run")
    snapshot_decision_at = _utc_timestamp(snapshot.get("decision_at"), "snapshot decision_at")
    dry_run_decision_at = _utc_timestamp(dry_run["decision_at"], "dry-run decision_at")
    if snapshot_decision_at != dry_run_decision_at:
        raise PreflightError("Prediction snapshot decision_at does not match the audited dry run")
    expectations = snapshot.get("expected_predictions")
    predictions = snapshot.get("predictions")
    if not isinstance(expectations, list) or not isinstance(predictions, list):
        raise PreflightError("Prediction snapshot must contain manifest and result arrays")
    expected_models = {binding.prediction_model_id for binding in bindings}
    latest: dict[tuple[str, str], dict[str, object]] = {}
    latest_recency: dict[tuple[str, str], tuple[dt.datetime, dt.datetime, int, int, str]] = {}
    for item in expectations:
        if not isinstance(item, dict):
            raise PreflightError("Prediction snapshot manifest contains an invalid task")
        model_id = item.get("prediction_model_id")
        if model_id not in expected_models:
            continue
        market_id = item.get("market_id")
        if not isinstance(market_id, str) or not market_id.strip():
            raise PreflightError("Prediction snapshot manifest contains an invalid market_id")
        key = (market_id.strip(), str(model_id))
        recency = _expectation_recency(item)
        if recency[0] > snapshot_decision_at or recency[1] > snapshot_decision_at:
            raise PreflightError(f"Direct task manifest is not PIT-visible for {key[0]}/{key[1]}")
        if key not in latest or recency > latest_recency[key]:
            latest[key] = item
            latest_recency[key] = recency
    if not latest:
        raise PreflightError("Prediction snapshot has no configured Direct task manifest")

    results: dict[str, tuple[str, str]] = {}
    for result in predictions:
        if not isinstance(result, dict):
            raise PreflightError("Prediction snapshot contains an invalid result")
        prediction_id = result.get("prediction_id")
        model = result.get("model")
        if isinstance(prediction_id, str) and isinstance(model, dict):
            model_name = model.get("name")
            market_id = result.get("market_id")
            if isinstance(model_name, str) and isinstance(market_id, str):
                if prediction_id in results:
                    raise PreflightError(
                        f"Prediction snapshot contains duplicate result {prediction_id}"
                    )
                results[prediction_id] = (market_id.strip(), model_name.strip())

    market_models: dict[str, set[str]] = {}
    market_runs: dict[str, set[int]] = {}
    market_selections: dict[str, set[int]] = {}
    market_as_of: dict[str, set[dt.datetime]] = {}
    market_conditions: dict[str, set[str]] = {}
    for (market_id, model_id), expectation in latest.items():
        market_models.setdefault(market_id, set()).add(model_id)
        market_runs.setdefault(market_id, set()).add(int(expectation["selection_run_id"]))
        market_selections.setdefault(market_id, set()).add(int(expectation["selection_id"]))
        market_as_of.setdefault(market_id, set()).add(
            _utc_timestamp(expectation.get("prediction_as_of"), "manifest prediction_as_of")
        )
        condition_id = expectation.get("condition_id")
        if not isinstance(condition_id, str) or not condition_id.strip():
            raise PreflightError("Direct task manifest contains an invalid condition_id")
        market_conditions.setdefault(market_id, set()).add(condition_id.strip())
        if expectation.get("status") != "COMPLETED":
            raise PreflightError(f"Direct task manifest is not COMPLETED for {market_id}/{model_id}")
        result_available_at = _utc_timestamp(
            expectation.get("result_available_at"), "manifest result_available_at"
        )
        if result_available_at > snapshot_decision_at:
            raise PreflightError(
                f"COMPLETED Direct task is not PIT-visible for {market_id}/{model_id}"
            )
        prediction_id = str(expectation.get("prediction_id", ""))
        if results.get(prediction_id) != (market_id, model_id):
            raise PreflightError(
                f"COMPLETED Direct task {market_id}/{model_id} has no matching PIT result"
            )
    for market_id, model_ids in market_models.items():
        if model_ids != expected_models:
            raise PreflightError(
                f"Direct task manifest has incomplete same-market models for {market_id}; "
                f"expected={sorted(expected_models)}, actual={sorted(model_ids)}"
            )
        if (
            len(market_runs[market_id]) != 1
            or len(market_selections[market_id]) != 1
            or len(market_as_of[market_id]) != 1
        ):
            raise PreflightError(
                f"Direct task manifest mixes generations across models for {market_id}"
            )
        if len(market_conditions[market_id]) != 1:
            raise PreflightError(
                f"Direct task manifest has conflicting market identity for {market_id}"
            )
    return len(market_models)


def validate_consumer_state(
    state: object,
    prediction_environment: dict[str, str],
    *,
    model_groups: dict[str, str],
    now: dt.datetime,
    max_age: dt.timedelta,
    max_idle: dt.timedelta,
) -> None:
    if not isinstance(state, dict):
        raise PreflightError("Direct consumer evidence must be a JSON object")
    _fresh_observation(state.get("observed_at"), "consumer observed_at", now=now, max_age=max_age)
    if state.get("stream_key") != prediction_environment.get("REDIS_DIRECT_PREDICTION_STREAM_KEY"):
        raise PreflightError("Direct consumer is attached to a different Redis stream")
    if state.get("model_groups") != model_groups:
        raise PreflightError("Direct consumer evidence has a different model-group topology")
    groups = state.get("groups")
    if not isinstance(groups, list) or len(groups) != len(model_groups):
        raise PreflightError("Direct consumer evidence does not cover every configured model")
    by_model: dict[str, dict[str, object]] = {}
    for group in groups:
        if not isinstance(group, dict) or not isinstance(group.get("model_id"), str):
            raise PreflightError("Direct consumer evidence contains an invalid model group")
        model_id = group["model_id"]
        if model_id in by_model:
            raise PreflightError(f"Direct consumer evidence duplicates model {model_id!r}")
        by_model[model_id] = group
    if set(by_model) != set(model_groups):
        raise PreflightError("Direct consumer evidence model set is incomplete")
    for model_id, expected_group in model_groups.items():
        group = by_model[model_id]
        if group.get("group") != expected_group:
            raise PreflightError(f"Direct consumer group differs for model {model_id!r}")
        if not isinstance(group.get("consumers"), int) or group["consumers"] < 1:
            raise PreflightError(f"Direct Redis group has no consumer for model {model_id!r}")
        if group.get("pending") != 0 or group.get("lag") != 0:
            raise PreflightError(
                f"Direct Redis group has pending or undelivered tasks for model {model_id!r}"
            )
        details = group.get("consumer_details")
        if not isinstance(details, list) or len(details) != group["consumers"]:
            raise PreflightError(f"Direct Redis consumer details differ for model {model_id!r}")
        names: set[str] = set()
        active = []
        for detail in details:
            if not isinstance(detail, dict) or not isinstance(detail.get("name"), str) or not detail["name"]:
                raise PreflightError(f"Direct Redis consumer identity is invalid for model {model_id!r}")
            if detail["name"] in names:
                raise PreflightError(f"Direct Redis consumer is duplicated for model {model_id!r}")
            names.add(detail["name"])
            if detail.get("pending") != 0:
                raise PreflightError(
                    f"Direct Redis consumer has pending tasks for model {model_id!r}"
                )
            if (
                isinstance(detail.get("idle_ms"), int)
                and detail["idle_ms"] <= int(max_idle.total_seconds() * 1000)
            ):
                active.append(detail)
        if not active:
            raise PreflightError(
                f"Direct Redis group has no recently active consumer for model {model_id!r}"
            )


def _canonical_sha256(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def configuration_sha256(
    trading_environment: dict[str, str],
    prediction_environment: dict[str, str],
    bindings: tuple[Binding, ...],
    model_groups: dict[str, str],
    *,
    approved_trading_commit: str,
    approved_prediction_version: str,
    wallet_file_sha256: str,
    activation_risk_approval_sha256: str | None = None,
) -> str:
    if not re.fullmatch(r"[0-9a-f]{64}", wallet_file_sha256):
        raise PreflightError("wallet file SHA-256 identity is invalid")
    trading = {
        key: trading_environment.get(key)
        for key in sorted(TRADING_RUNTIME_KEYS - {"DECISION_CYCLE_ORDER_SUBMISSION_ENABLED", "DECISION_CYCLE_BINDINGS_JSON"})
    }
    prediction = {
        key: prediction_environment.get(key)
        for key in sorted(PREDICTION_RUNTIME_KEYS - {"DIRECT_PREDICTION_MODEL_IDS_JSON"})
    }
    identity = {
        "trading": trading,
        "prediction": prediction,
        "bindings": [dataclasses.asdict(binding) for binding in bindings],
        "direct_prediction_models": sorted(model_groups),
        "direct_model_groups": model_groups,
        "wallet_file_sha256": wallet_file_sha256,
        "activation_risk_approval_sha256": activation_risk_approval_sha256,
        "approved_release_identity": {
            "trading_commit": approved_trading_commit,
            "prediction_version": approved_prediction_version,
        },
        "credential_sha256": {
            key: _credential_sha256(
                key,
                (
                    trading_environment.get(key, "")
                    if key in TRADING_RUNTIME_SECRET_KEYS
                    else prediction_environment.get(key, "")
                ),
            )
            for key in sorted(
                TRADING_RUNTIME_SECRET_KEYS | PREDICTION_RUNTIME_SECRET_KEYS
            )
        },
    }
    return _canonical_sha256(identity)


def build_disabled_evidence(
    *,
    now: dt.datetime,
    expected_trading_commit: str,
    expected_prediction_version: str,
    model_groups: dict[str, str],
    configuration_sha: str,
    dry_run: dict[str, object],
    manifest_markets: int,
    delivery_watermark: dict[str, object],
    database_account_identity_sha256: str,
    runtime_state: object,
) -> dict[str, object]:
    if not isinstance(runtime_state, dict) or not isinstance(runtime_state.get("trading"), dict):
        raise PreflightError("cannot build disabled evidence without Trading runtime identity")
    trading_runtime = runtime_state["trading"]
    evidence: dict[str, object] = {
        "schema_version": DISABLED_EVIDENCE_SCHEMA,
        "issued_at": now.isoformat().replace("+00:00", "Z"),
        "submission_state": "disabled",
        "approved_trading_commit": expected_trading_commit,
        "approved_prediction_version": expected_prediction_version,
        "configuration_sha256": configuration_sha,
        "direct_model_groups": model_groups,
        "trading_runtime": {
            "pid": trading_runtime.get("pid"),
            "started_at": trading_runtime.get("started_at"),
        },
        "dry_run": dry_run,
        "manifest_markets": manifest_markets,
        "delivery_watermark": delivery_watermark,
        "database_account_identity_sha256": database_account_identity_sha256,
    }
    evidence["evidence_sha256"] = _canonical_sha256(evidence)
    return evidence


def validate_disabled_evidence(
    evidence: object,
    *,
    now: dt.datetime,
    max_age: dt.timedelta,
    expected_trading_commit: str,
    expected_prediction_version: str,
    model_groups: dict[str, str],
    configuration_sha: str,
    delivery_watermark: dict[str, object],
    database_account_identity_sha256: str,
) -> dict[str, object]:
    if not isinstance(evidence, dict):
        raise PreflightError("disabled-phase evidence must be a JSON object")
    supplied_digest = evidence.get("evidence_sha256")
    unsigned = dict(evidence)
    unsigned.pop("evidence_sha256", None)
    if not isinstance(supplied_digest, str) or supplied_digest != _canonical_sha256(unsigned):
        raise PreflightError("disabled-phase evidence checksum is invalid")
    if evidence.get("schema_version") != DISABLED_EVIDENCE_SCHEMA:
        raise PreflightError("disabled-phase evidence schema is unsupported")
    _fresh_observation(
        evidence.get("issued_at"), "disabled evidence issued_at", now=now, max_age=max_age
    )
    if evidence.get("submission_state") != "disabled":
        raise PreflightError("disabled-phase evidence has the wrong submission state")
    if evidence.get("approved_trading_commit") != expected_trading_commit:
        raise PreflightError("disabled-phase evidence belongs to a different Trading commit")
    if evidence.get("approved_prediction_version") != expected_prediction_version:
        raise PreflightError("disabled-phase evidence belongs to a different Prediction version")
    if evidence.get("configuration_sha256") != configuration_sha:
        raise PreflightError(
            "disabled-phase evidence configuration differs from the enabled process"
        )
    if evidence.get("direct_model_groups") != model_groups:
        raise PreflightError("disabled-phase evidence belongs to a different Direct model-group topology")
    if not isinstance(evidence.get("manifest_markets"), int) or evidence["manifest_markets"] < 1:
        raise PreflightError("disabled-phase evidence has no completed manifest markets")
    if evidence.get("delivery_watermark") != delivery_watermark:
        raise PreflightError(
            "decision delivery state changed after the disabled-phase evidence"
        )
    if evidence.get("database_account_identity_sha256") != database_account_identity_sha256:
        raise PreflightError(
            "four-wallet database address identity changed after disabled evidence"
        )
    dry_run = evidence.get("dry_run")
    if not isinstance(dry_run, dict):
        raise PreflightError("disabled-phase evidence has no dry-run identity")
    return dry_run


def reject_enabled_cycles_after_evidence(
    state: object, disabled_dry_run: dict[str, object]
) -> None:
    if not isinstance(state, dict) or not isinstance(state.get("dry_runs"), list):
        raise PreflightError("database preflight did not return cycle evidence")
    disabled_at = _utc_timestamp(
        disabled_dry_run.get("decision_at"), "disabled evidence dry-run decision_at"
    )
    for run in state["dry_runs"]:
        if not isinstance(run, dict):
            raise PreflightError("database returned an invalid decision-cycle group")
        decision_at = _utc_timestamp(run.get("decision_at"), "decision-cycle decision_at")
        if decision_at <= disabled_at:
            continue
        rows = run.get("bindings")
        if not isinstance(rows, list):
            raise PreflightError("database returned invalid decision-cycle bindings")
        submission_mode = "submission=true" if any(
            isinstance(row, dict) and row.get("order_submission_enabled") is True
            for row in rows
        ) else "incomplete-or-unexpected"
        raise PreflightError(
            f"a {submission_mode} decision cycle exists after disabled evidence; keep the "
            "kill switch closed, discard this activation attempt, and repeat the dry-run phase"
        )


def write_disabled_evidence(path: pathlib.Path, evidence: dict[str, object]) -> None:
    payload = (json.dumps(evidence, indent=2, sort_keys=True) + "\n").encode("utf-8")
    if len(payload) > MAX_CONFIG_BYTES:
        raise PreflightError("disabled-phase evidence exceeds the size limit")
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except OSError as error:
        raise PreflightError(f"cannot create disabled-phase evidence {path}: {error}") from error
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as error:
        raise PreflightError(f"cannot write disabled-phase evidence {path}: {error}") from error


def read_disabled_evidence(path: pathlib.Path) -> object:
    payload = _read_bounded_regular_file(path, "disabled-phase evidence")
    info = path.stat()
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise PreflightError("disabled-phase evidence must not be accessible to group or other users")
    if info.st_uid != os.geteuid():
        raise PreflightError("disabled-phase evidence must be owned by the preflight user")
    try:
        return json.loads(payload)
    except json.JSONDecodeError as error:
        raise PreflightError("disabled-phase evidence is invalid JSON") from error


def read_activation_risk_approval(
    path: pathlib.Path, expected_sha256: str
) -> object:
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        raise PreflightError(
            "approved activation-risk artifact SHA-256 must be 64 lowercase hex characters"
        )
    payload = _read_bounded_regular_file(path, "activation-risk approval")
    info = path.stat()
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise PreflightError(
            "activation-risk approval must not be accessible to group or other users"
        )
    if info.st_uid != os.geteuid():
        raise PreflightError("activation-risk approval must be owned by the preflight user")
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise PreflightError("activation-risk approval SHA-256 differs from the approved digest")
    try:
        return json.loads(payload)
    except json.JSONDecodeError as error:
        raise PreflightError("activation-risk approval is invalid JSON") from error


def validate_activation_risk_approval(
    approval: object,
    *,
    active_accounts: frozenset[str],
    expected_trading_commit: str,
    now: dt.datetime,
    max_age: dt.timedelta,
) -> dict[str, dict[str, object]]:
    if not isinstance(approval, dict):
        raise PreflightError("activation-risk approval must be a JSON object")
    expected_fields = {
        "schema_version",
        "decision",
        "approval_id",
        "approved_by",
        "approved_at",
        "expires_at",
        "approved_trading_commit",
        "accounts",
    }
    if set(approval) != expected_fields:
        raise PreflightError(
            "activation-risk approval must contain the exact reviewed schema fields"
        )
    if approval.get("schema_version") != ACTIVATION_RISK_APPROVAL_SCHEMA:
        raise PreflightError(
            "activation-risk approval schema is unsupported; disabled-phase evidence "
            "cannot authorize wallet activation"
        )
    if approval.get("decision") != ACTIVATION_RISK_DECISION:
        raise PreflightError("activation-risk decision is not approved for live activation")
    for field in ("approval_id", "approved_by"):
        value = approval.get(field)
        if not isinstance(value, str) or not value.strip():
            raise PreflightError(f"activation-risk approval has an empty {field}")
    if approval.get("approved_trading_commit") != expected_trading_commit:
        raise PreflightError("activation-risk approval belongs to a different Trading commit")
    approved_at = _fresh_observation(
        approval.get("approved_at"),
        "activation-risk approved_at",
        now=now,
        max_age=max_age,
    )
    expires_at = _utc_timestamp(
        approval.get("expires_at"), "activation-risk expires_at"
    )
    if expires_at <= approved_at or expires_at <= now:
        raise PreflightError("activation-risk approval is expired or has an invalid expiry")

    rows = approval.get("accounts")
    if not isinstance(rows, list):
        raise PreflightError("activation-risk approval accounts must be an array")
    account_fields = {
        "execution_account_id",
        "policy_id",
        "version",
        *POLICY_LIMIT_FIELDS,
        "daily_timezone",
    }
    contracts: dict[str, dict[str, object]] = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != account_fields:
            raise PreflightError(
                "activation-risk approval account must contain the exact policy fields"
            )
        account_id = row.get("execution_account_id")
        policy_id = row.get("policy_id")
        version = row.get("version")
        if (
            not isinstance(account_id, str)
            or not account_id.strip()
            or account_id.strip() in contracts
            or not isinstance(policy_id, str)
            or not policy_id.strip()
            or not isinstance(version, int)
            or isinstance(version, bool)
            or version < 1
        ):
            raise PreflightError(
                "activation-risk approval contains a duplicate or invalid account identity"
            )
        contract = dict(row)
        contract.pop("execution_account_id")
        for field in POLICY_LIMIT_FIELDS:
            value = contract.get(field)
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(value)
                or value <= 0
            ):
                raise PreflightError(
                    f"activation-risk approval for {account_id!r} has invalid {field}"
                )
        if contract.get("daily_timezone") != "UTC":
            raise PreflightError(
                f"activation-risk approval for {account_id!r} must use daily_timezone UTC"
            )
        contracts[account_id.strip()] = contract
    if set(contracts) != active_accounts:
        raise PreflightError(
            "activation-risk approval account set differs from active wallet-6/wallet-7; "
            f"missing={sorted(active_accounts - set(contracts))}, "
            f"unexpected={sorted(set(contracts) - active_accounts)}"
        )
    return contracts


def _pgpass_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace(":", "\\:")


def _pg_environment(database_url: str) -> tuple[dict[str, str], pathlib.Path | None]:
    try:
        parsed = urllib.parse.urlparse(database_url)
        hostname = parsed.hostname
        port = parsed.port or 5432
    except ValueError as error:
        raise PreflightError("TRADING_EXECUTION_DATABASE_URL is malformed") from error
    if parsed.scheme not in {"postgres", "postgresql"} or not hostname:
        raise PreflightError("TRADING_EXECUTION_DATABASE_URL must be a PostgreSQL URL with a host")
    if not parsed.path or parsed.path == "/":
        raise PreflightError("TRADING_EXECUTION_DATABASE_URL must include a database name")
    username = urllib.parse.unquote(parsed.username or "")
    if not username:
        raise PreflightError("TRADING_EXECUTION_DATABASE_URL must include a user")

    child = {key: value for key, value in os.environ.items() if not key.startswith("PG")}
    child.update(
        {
            "PGHOST": hostname,
            "PGPORT": str(port),
            "PGDATABASE": urllib.parse.unquote(parsed.path.lstrip("/")),
            "PGUSER": username,
        }
    )
    supported = {
        "sslmode": "PGSSLMODE",
        "sslrootcert": "PGSSLROOTCERT",
        "sslcert": "PGSSLCERT",
        "sslkey": "PGSSLKEY",
        "connect_timeout": "PGCONNECT_TIMEOUT",
        "application_name": "PGAPPNAME",
    }
    for key, values in urllib.parse.parse_qs(parsed.query, keep_blank_values=True).items():
        if key not in supported or len(values) != 1:
            raise PreflightError(f"unsupported PostgreSQL URL parameter: {key}")
        child[supported[key]] = values[0]
    child.setdefault("PGCONNECT_TIMEOUT", "10")

    pgpass_path: pathlib.Path | None = None
    password = urllib.parse.unquote(parsed.password or "")
    if password:
        handle = tempfile.NamedTemporaryFile(prefix="four-wallet-pgpass-", delete=False)
        pgpass_path = pathlib.Path(handle.name)
        pgpass_line = (
            f"{_pgpass_escape(hostname)}:{port}:*:{_pgpass_escape(username)}:"
            f"{_pgpass_escape(password)}\n"
        )
        handle.write(pgpass_line.encode())
        handle.close()
        pgpass_path.chmod(0o600)
        child["PGPASSFILE"] = str(pgpass_path)
    return child, pgpass_path


def query_database_state(database_url: str) -> object:
    if not shutil.which("psql"):
        raise PreflightError("psql is required for the database authorization preflight")
    child, pgpass_path = _pg_environment(database_url)
    try:
        result = subprocess.run(
            ["psql", "-X", "-Atq", "-v", "ON_ERROR_STOP=1", "-c", DATABASE_STATE_SQL],
            env=child,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise PreflightError(f"database preflight command failed: {type(error).__name__}") from error
    finally:
        if pgpass_path is not None:
            pgpass_path.unlink(missing_ok=True)
    if result.returncode != 0:
        detail = result.stderr.strip().replace("\n", " ")[-1000:]
        raise PreflightError(f"database preflight query failed: {detail}")
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if len(lines) != 1:
        raise PreflightError("database preflight returned an unexpected result shape")
    try:
        return json.loads(lines[0])
    except json.JSONDecodeError as error:
        raise PreflightError("database preflight returned invalid JSON") from error


def _get_json(url: str, headers: dict[str, str] | None = None) -> object:
    request = urllib.request.Request(url, headers=headers or {}, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            payload = response.read(MAX_CONFIG_BYTES + 1)
            if response.status != 200:
                raise PreflightError(f"read-only HTTP evidence returned status {response.status}")
    except PreflightError:
        raise
    except Exception as error:
        raise PreflightError(f"read-only HTTP evidence failed: {type(error).__name__}") from error
    if len(payload) > MAX_CONFIG_BYTES:
        raise PreflightError("read-only HTTP evidence exceeds the size limit")
    try:
        return json.loads(payload)
    except json.JSONDecodeError as error:
        raise PreflightError("read-only HTTP evidence returned invalid JSON") from error


def _service_pid(unit: str) -> int:
    if not shutil.which("systemctl"):
        raise PreflightError("systemctl is required to prove the actual runtime process")
    try:
        result = subprocess.run(
            ["systemctl", "show", unit, "--property=MainPID", "--value"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise PreflightError(
            f"cannot inspect runtime unit {unit}: {type(error).__name__}"
        ) from error
    if result.returncode != 0:
        raise PreflightError(f"cannot inspect runtime unit {unit}")
    try:
        pid = int(result.stdout.strip())
    except ValueError as error:
        raise PreflightError(f"runtime unit {unit} returned an invalid pid") from error
    if pid < 1:
        raise PreflightError(f"runtime unit {unit} has no running process")
    return pid


def _process_started_at(pid: int, now: dt.datetime) -> dt.datetime:
    try:
        stat_payload = pathlib.Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        system_stat = pathlib.Path("/proc/stat").read_text(encoding="utf-8")
        closing_parenthesis = stat_payload.rfind(")")
        fields = stat_payload[closing_parenthesis + 2 :].split()
        start_ticks = int(fields[19])
        clock_ticks = int(os.sysconf("SC_CLK_TCK"))
        boot_seconds = int(
            next(
                line.split()[1]
                for line in system_stat.splitlines()
                if line.startswith("btime ")
            )
        )
    except (OSError, ValueError, IndexError) as error:
        raise PreflightError(f"cannot derive start time for runtime pid {pid}") from error
    except StopIteration as error:
        raise PreflightError("cannot derive Linux boot time for runtime identity") from error
    started_at = dt.datetime.fromtimestamp(
        boot_seconds + (start_ticks / clock_ticks), tz=dt.timezone.utc
    )
    if started_at > now + dt.timedelta(seconds=1):
        raise PreflightError(f"runtime pid {pid} has an invalid start time")
    return started_at


def _process_environment(pid: int, keys: set[str]) -> dict[str, str]:
    try:
        payload = pathlib.Path(f"/proc/{pid}/environ").read_bytes()
    except OSError as error:
        raise PreflightError(f"cannot read environment for runtime pid {pid}") from error
    result: dict[str, str] = {}
    for item in payload.split(b"\0"):
        if b"=" not in item:
            continue
        raw_key, raw_value = item.split(b"=", 1)
        try:
            key = raw_key.decode("utf-8")
            value = raw_value.decode("utf-8")
        except UnicodeDecodeError as error:
            raise PreflightError(f"runtime pid {pid} has non-UTF8 environment data") from error
        if key in keys:
            result[key] = value
    return result


def collect_runtime_state(
    trading_health_url: str, prediction_health_url: str
) -> dict[str, object]:
    observed_at = dt.datetime.now(dt.timezone.utc)
    definitions = {
        "trading": (
            "trading-execution.service",
            trading_health_url,
            TRADING_RUNTIME_KEYS,
            TRADING_RUNTIME_SECRET_KEYS,
        ),
        "prediction": (
            "prediction-infra.service",
            prediction_health_url,
            PREDICTION_RUNTIME_KEYS,
            PREDICTION_RUNTIME_SECRET_KEYS,
        ),
    }
    result: dict[str, object] = {
        "observed_at": observed_at.isoformat().replace("+00:00", "Z")
    }
    for name, (unit, health_url, keys, secret_keys) in definitions.items():
        pid = _service_pid(unit)
        process_environment = _process_environment(pid, set(keys) | set(secret_keys))
        result[name] = {
            "pid": pid,
            "started_at": _process_started_at(pid, observed_at)
            .isoformat()
            .replace("+00:00", "Z"),
            "environment": {
                key: process_environment[key]
                for key in keys
                if key in process_environment
            },
            "secret_sha256": {
                key: _credential_sha256(key, process_environment[key])
                for key in secret_keys
                if key in process_environment
            },
            "health": _get_json(health_url),
        }
    return result


def _duration_seconds(
    value: str,
    field: str,
    *,
    allow_zero: bool = False,
    max_seconds: float = 24 * 60 * 60,
) -> float:
    value = value.strip()
    if not value:
        raise PreflightError(f"{field} is required")
    units = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}
    cursor = 0
    total = 0.0
    for match in re.finditer(r"(\d+(?:\.\d+)?)(ms|s|m|h)", value):
        if match.start() != cursor:
            raise PreflightError(f"{field} has an unsupported duration")
        total += float(match.group(1)) * units[match.group(2)]
        cursor = match.end()
    if (
        cursor != len(value)
        or total < 0
        or (total == 0 and not allow_zero)
        or total > max_seconds
    ):
        raise PreflightError(f"{field} has an unsupported duration")
    return total


def _whole_duration_seconds(
    value: str, field: str, *, allow_zero: bool = False
) -> int:
    total = _duration_seconds(value, field, allow_zero=allow_zero)
    if not total.is_integer():
        raise PreflightError(f"{field} must resolve to whole seconds")
    return int(total)


def next_decision_due(
    now: dt.datetime,
    trading_environment: dict[str, str],
    *,
    include_due_at_now: bool = False,
) -> dt.datetime:
    interval = _whole_duration_seconds(
        trading_environment.get("DECISION_CYCLE_INTERVAL", "10m"),
        "DECISION_CYCLE_INTERVAL",
    )
    startup_delay = _duration_seconds(
        trading_environment.get("DECISION_CYCLE_STARTUP_DELAY", "15s"),
        "DECISION_CYCLE_STARTUP_DELAY",
        allow_zero=True,
    )
    if interval != 10 * 60 or startup_delay >= interval:
        raise PreflightError(
            "four-wallet activation requires the 10m scheduler and a smaller startup delay"
        )
    now = now.astimezone(dt.timezone.utc)
    boundary_epoch = int(now.timestamp()) // interval * interval
    due = dt.datetime.fromtimestamp(
        boundary_epoch + startup_delay, tz=dt.timezone.utc
    )
    if due < now or (due == now and not include_due_at_now):
        due += dt.timedelta(seconds=interval)
    return due


def bind_process_activation_due(
    captured_next_due: dt.datetime,
    process_started_at: dt.datetime,
    preflight_started_at: dt.datetime,
    trading_environment: dict[str, str],
) -> dt.datetime:
    first_process_due = next_decision_due(
        process_started_at,
        trading_environment,
        include_due_at_now=True,
    )
    if first_process_due <= preflight_started_at:
        raise PreflightError(
            "enabled Trading process may already have started its first decision cycle; "
            "keep the kill switch closed and restart in a new maintenance window"
        )
    return min(captured_next_due, first_process_due)


def activation_deadline(
    captured_next_due: dt.datetime, now: dt.datetime, safety_seconds: int
) -> dt.datetime:
    deadline = captured_next_due - dt.timedelta(seconds=safety_seconds)
    if now >= deadline:
        raise PreflightError(
            "enabled preflight crossed or is too close to its captured decision due "
            "time; keep the kill switch closed and retry in the next maintenance window"
        )
    return deadline


def fetch_prediction_snapshot(
    trading_environment: dict[str, str], decision_at: str
) -> object:
    base_url = trading_environment.get("DECISION_CYCLE_PREDICTION_INFRA_URL", "").strip()
    token = trading_environment.get("DECISION_CYCLE_PREDICTION_INFRA_TOKEN", "").strip()
    if not base_url or not token:
        raise PreflightError("Trading Prediction snapshot URL and token are required")
    lookback = _whole_duration_seconds(
        trading_environment.get("DECISION_CYCLE_PREDICTION_LOOKBACK", "3h"),
        "DECISION_CYCLE_PREDICTION_LOOKBACK",
    )
    query = urllib.parse.urlencode(
        {"decision_at": decision_at, "lookback_seconds": str(lookback)}
    )
    url = base_url.rstrip("/") + "/api/v1/live-predictions/snapshot?" + query
    return _get_json(url, {"Authorization": f"Bearer {token}"})


class _RedisProtocolError(RuntimeError):
    pass


def _redis_command(*parts: str) -> bytes:
    payload = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        raw = part.encode("utf-8")
        payload.extend((f"${len(raw)}\r\n".encode(), raw, b"\r\n"))
    return b"".join(payload)


def _read_redis_response(stream) -> object:
    prefix = stream.read(1)
    if not prefix:
        raise _RedisProtocolError("Redis closed the connection")
    line = stream.readline()
    if not line.endswith(b"\r\n"):
        raise _RedisProtocolError("Redis returned a malformed response")
    content = line[:-2]
    if prefix == b"+":
        return content.decode("utf-8")
    if prefix == b"-":
        raise _RedisProtocolError(content.decode("utf-8", errors="replace"))
    if prefix == b":":
        return int(content)
    if prefix == b"$":
        length = int(content)
        if length == -1:
            return None
        value = stream.read(length)
        if len(value) != length or stream.read(2) != b"\r\n":
            raise _RedisProtocolError("Redis returned a truncated bulk response")
        return value.decode("utf-8")
    if prefix == b"*":
        length = int(content)
        if length == -1:
            return None
        return [_read_redis_response(stream) for _ in range(length)]
    raise _RedisProtocolError("Redis returned an unsupported response type")


def _redis_pairs(value: object) -> dict[str, object]:
    if not isinstance(value, list) or len(value) % 2 != 0:
        raise PreflightError("Redis XINFO returned an invalid field list")
    result: dict[str, object] = {}
    for index in range(0, len(value), 2):
        key = value[index]
        if not isinstance(key, str):
            raise PreflightError("Redis XINFO returned an invalid field name")
        result[key] = value[index + 1]
    return result


def collect_consumer_state(
    prediction_environment: dict[str, str], model_groups: dict[str, str]
) -> dict[str, object]:
    address = prediction_environment.get("REDIS_ADDRESS", "").strip()
    parsed = urllib.parse.urlsplit("//" + address)
    if not parsed.hostname:
        raise PreflightError("REDIS_ADDRESS must contain a host")
    port = parsed.port or 6379
    timeout = _duration_seconds(
        prediction_environment.get("REDIS_DIAL_TIMEOUT", "5s"), "REDIS_DIAL_TIMEOUT"
    )
    stream_key = prediction_environment["REDIS_DIRECT_PREDICTION_STREAM_KEY"]
    password = prediction_environment.get("REDIS_PASSWORD", "")
    database = prediction_environment.get("REDIS_DATABASE", "0").strip()
    try:
        database_number = int(database)
    except ValueError as error:
        raise PreflightError("REDIS_DATABASE must be an integer") from error
    try:
        connection = socket.create_connection((parsed.hostname, port), timeout=timeout)
        connection.settimeout(timeout)
        stream = connection.makefile("rwb", buffering=0)
        try:
            def execute(*parts: str) -> object:
                stream.write(_redis_command(*parts))
                return _read_redis_response(stream)

            if password:
                execute("AUTH", password)
            if database_number:
                execute("SELECT", str(database_number))
            raw_groups = execute("XINFO", "GROUPS", stream_key)
            if not isinstance(raw_groups, list):
                raise PreflightError("Redis XINFO GROUPS returned an invalid response")
            groups = [_redis_pairs(group) for group in raw_groups]
            group_by_name: dict[str, dict[str, object]] = {}
            for group in groups:
                name = group.get("name")
                if not isinstance(name, str) or name in group_by_name:
                    raise PreflightError("Redis XINFO GROUPS returned duplicate or invalid names")
                group_by_name[name] = group
            evidence_groups = []
            for model_id, expected_group in model_groups.items():
                group = group_by_name.get(expected_group)
                if group is None:
                    raise PreflightError(
                        f"approved Direct Redis group is missing for model {model_id!r}"
                    )
                raw_consumers = execute(
                    "XINFO", "CONSUMERS", stream_key, expected_group
                )
                if not isinstance(raw_consumers, list):
                    raise PreflightError("Redis XINFO CONSUMERS returned an invalid response")
                consumers = [_redis_pairs(consumer) for consumer in raw_consumers]
                evidence_groups.append(
                    {
                        "model_id": model_id,
                        "group": expected_group,
                        "consumers": group.get("consumers"),
                        "pending": group.get("pending"),
                        "lag": group.get("lag"),
                        "consumer_details": [
                            {
                                "name": consumer.get("name"),
                                "pending": consumer.get("pending"),
                                "idle_ms": consumer.get("idle"),
                            }
                            for consumer in consumers
                        ],
                    }
                )
        finally:
            stream.close()
            connection.close()
    except PreflightError:
        raise
    except (OSError, ValueError, _RedisProtocolError) as error:
        raise PreflightError(f"read Direct Redis consumer state failed: {type(error).__name__}") from error
    return {
        "observed_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "stream_key": stream_key,
        "model_groups": model_groups,
        "groups": evidence_groups,
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        type=pathlib.Path,
        default=pathlib.Path("/etc/trading-execution/env"),
    )
    parser.add_argument(
        "--prediction-env-file",
        type=pathlib.Path,
        default=pathlib.Path("/etc/prediction-infra/env"),
    )
    parser.add_argument(
        "--submission-state",
        choices=("disabled", "enabled"),
        default="disabled",
        help="expected order-submission flag; use enabled after staging the second gate",
    )
    parser.add_argument(
        "--entry-submission-state",
        choices=("allowed", "blocked"),
        default="allowed",
        help="expected process-wide BUY gate; use blocked for an audited sell-only window",
    )
    parser.add_argument(
        "--write-disabled-evidence",
        type=pathlib.Path,
        help="new mode-0600 evidence artifact; required for the disabled phase",
    )
    parser.add_argument(
        "--disabled-evidence-json",
        type=pathlib.Path,
        help="disabled-phase artifact; required after restarting with submission enabled",
    )
    parser.add_argument(
        "--activation-risk-approval-json",
        type=pathlib.Path,
        help=(
            "independently reviewed mode-0600 risk approval; required when wallet-6 "
            "or wallet-7 is active"
        ),
    )
    parser.add_argument(
        "--activation-risk-approval-sha256",
        help="externally approved SHA-256 of the activation-risk artifact",
    )
    parser.add_argument(
        "--database-state-json",
        type=pathlib.Path,
        help="offline/test input; production should query PostgreSQL directly",
    )
    parser.add_argument(
        "--final-database-state-json",
        type=pathlib.Path,
        help=(
            "independent final DB evidence required in enabled offline mode; "
            "live PostgreSQL mode performs its own final query"
        ),
    )
    parser.add_argument(
        "--runtime-state-json",
        type=pathlib.Path,
        help="explicit fresh runtime evidence when systemd and /proc cannot be read",
    )
    parser.add_argument(
        "--final-runtime-state-json",
        type=pathlib.Path,
        help=(
            "independent final runtime evidence required in enabled offline mode; "
            "live mode re-reads systemd, /proc, and health"
        ),
    )
    parser.add_argument(
        "--prediction-snapshot-json",
        type=pathlib.Path,
        help="explicit PIT snapshot evidence when the local HTTP endpoint cannot be read",
    )
    parser.add_argument(
        "--consumer-state-json",
        type=pathlib.Path,
        help="explicit fresh Redis XINFO evidence when Redis cannot be read directly",
    )
    parser.add_argument("--expected-trading-commit", required=True)
    parser.add_argument("--expected-prediction-version", required=True)
    parser.add_argument(
        "--direct-model-groups-json",
        required=True,
        help="JSON object mapping every source prediction_model_id to one distinct Redis group",
    )
    parser.add_argument(
        "--trading-health-url", default="http://127.0.0.1:14000/health/live"
    )
    parser.add_argument(
        "--prediction-health-url", default="http://127.0.0.1:11000/health/live"
    )
    parser.add_argument("--max-evidence-age-seconds", type=int, default=120)
    parser.add_argument("--max-dry-run-age-seconds", type=int, default=1800)
    parser.add_argument("--max-risk-approval-age-seconds", type=int, default=86400)
    parser.add_argument("--max-consumer-idle-seconds", type=int, default=900)
    parser.add_argument(
        "--minimum-activation-window-seconds",
        type=int,
        default=60,
        help="enabled PASS requires this many seconds before the next decision due time",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        preflight_started_at = dt.datetime.now(dt.timezone.utc)
        if (
            args.max_evidence_age_seconds < 1
            or args.max_dry_run_age_seconds < 1
            or args.max_risk_approval_age_seconds < 1
            or args.max_consumer_idle_seconds < 1
            or args.minimum_activation_window_seconds < 1
            or args.minimum_activation_window_seconds >= 600
        ):
            raise PreflightError(
                "evidence/consumer ages must be positive and activation window must be 1..599 seconds"
            )
        if args.submission_state == "disabled":
            if (
                args.write_disabled_evidence is None
                or args.disabled_evidence_json is not None
                or args.final_database_state_json is not None
                or args.final_runtime_state_json is not None
            ):
                raise PreflightError(
                    "disabled phase requires --write-disabled-evidence and forbids "
                    "--disabled-evidence-json/final evidence options"
                )
        elif args.disabled_evidence_json is None or args.write_disabled_evidence is not None:
            raise PreflightError(
                "enabled phase requires --disabled-evidence-json and forbids "
                "--write-disabled-evidence"
            )
        if args.submission_state == "enabled":
            if args.database_state_json is None and args.final_database_state_json is not None:
                raise PreflightError(
                    "live database mode forbids a static final database artifact"
                )
            if args.database_state_json is not None and args.final_database_state_json is None:
                raise PreflightError(
                    "enabled offline mode requires an independent --final-database-state-json"
                )
            if args.runtime_state_json is None and args.final_runtime_state_json is not None:
                raise PreflightError(
                    "live runtime mode forbids a static final runtime artifact"
                )
            if args.runtime_state_json is not None and args.final_runtime_state_json is None:
                raise PreflightError(
                    "enabled offline mode requires an independent --final-runtime-state-json"
                )
        expected_trading_commit = args.expected_trading_commit.strip()
        expected_prediction_version = args.expected_prediction_version.strip()
        if not expected_trading_commit or not expected_prediction_version:
            raise PreflightError("approved runtime identities are required")
        unresolved_identity = (
            expected_trading_commit
            + expected_prediction_version
            + args.direct_model_groups_json
        ).upper()
        if any(marker in unresolved_identity for marker in ("PLACEHOLDER", "APPROVED_", "EXACT_")):
            raise PreflightError("approved runtime identity still contains a placeholder")
        if not re.fullmatch(r"[0-9a-f]{40}", expected_trading_commit):
            raise PreflightError("approved Trading commit must be the full lowercase Git SHA")
        if not IMMUTABLE_RELEASE_IDENTITY.fullmatch(expected_prediction_version):
            raise PreflightError(
                "approved Prediction version must be an immutable lowercase full Git SHA "
                "or sha256 image digest"
            )

        evidence_age = dt.timedelta(seconds=args.max_evidence_age_seconds)
        dry_run_age = dt.timedelta(seconds=args.max_dry_run_age_seconds)
        consumer_idle = dt.timedelta(seconds=args.max_consumer_idle_seconds)
        environment = read_environment_file(args.env_file)
        # Capture this once, before wallet/runtime/DB/HTTP/Redis checks. Never
        # roll it forward if a due time passes while enabled preflight runs.
        captured_next_due = (
            next_decision_due(preflight_started_at, environment)
            if args.submission_state == "enabled"
            else None
        )
        prediction_environment = read_environment_file(args.prediction_env_file)
        bindings = validate_environment(
            environment,
            submission_state=args.submission_state,
            entry_submission_state=args.entry_submission_state,
        )
        submission_disabled_accounts = decode_submission_disabled_accounts(
            environment.get("DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON", "[]"),
            bindings,
        )
        validate_rollout_quarantine(submission_disabled_accounts)
        active_wallet67 = frozenset(
            ACTIVATABLE_ACCOUNTS - set(submission_disabled_accounts)
        )
        activation_risk_approval_sha = (
            args.activation_risk_approval_sha256 or ""
        ).strip()
        if active_wallet67:
            if (
                args.activation_risk_approval_json is None
                or not activation_risk_approval_sha
            ):
                raise PreflightError(
                    "active wallet-6/wallet-7 requires an independent activation-risk "
                    "approval artifact and its externally approved SHA-256"
                )
            activation_risk_approval = read_activation_risk_approval(
                args.activation_risk_approval_json,
                activation_risk_approval_sha,
            )
            approved_risk_contracts = validate_activation_risk_approval(
                activation_risk_approval,
                active_accounts=active_wallet67,
                expected_trading_commit=expected_trading_commit,
                now=dt.datetime.now(dt.timezone.utc),
                max_age=dt.timedelta(seconds=args.max_risk_approval_age_seconds),
            )
        else:
            if (
                args.activation_risk_approval_json is not None
                or activation_risk_approval_sha
            ):
                raise PreflightError(
                    "activation-risk approval is forbidden while wallet-6 and wallet-7 "
                    "remain quarantined"
                )
            approved_risk_contracts = {}
        active_bindings = active_rollout_bindings(
            bindings, submission_disabled_accounts
        )
        wallet_path = pathlib.Path(environment["POLYMARKET_ACCOUNTS_FILE"])
        wallet_file_sha = hashlib.sha256(
            _read_bounded_regular_file(wallet_path, "wallet")
        ).hexdigest()
        validate_prediction_environment(
            prediction_environment,
            bindings,
            required_bindings=active_bindings,
        )
        validate_cross_service_credentials(environment, prediction_environment)
        model_groups = decode_model_groups(
            args.direct_model_groups_json,
            bindings,
            required_bindings=active_bindings,
        )
        configuration_sha = configuration_sha256(
            environment,
            prediction_environment,
            bindings,
            model_groups,
            approved_trading_commit=expected_trading_commit,
            approved_prediction_version=expected_prediction_version,
            wallet_file_sha256=wallet_file_sha,
            activation_risk_approval_sha256=(
                activation_risk_approval_sha or None
            ),
        )

        if args.runtime_state_json is None:
            runtime_state = collect_runtime_state(
                args.trading_health_url, args.prediction_health_url
            )
        else:
            runtime_state = json.loads(
                _read_bounded_regular_file(args.runtime_state_json, "runtime state")
            )
        process_started_at = validate_runtime_state(
            runtime_state,
            environment,
            prediction_environment,
            expected_trading_commit=expected_trading_commit,
            expected_prediction_version=expected_prediction_version,
            now=dt.datetime.now(dt.timezone.utc),
            max_age=evidence_age,
        )
        if args.submission_state == "enabled":
            if captured_next_due is None:
                raise PreflightError("enabled preflight lost its captured decision due time")
            captured_next_due = bind_process_activation_due(
                captured_next_due,
                process_started_at,
                preflight_started_at,
                environment,
            )

        if args.database_state_json is None:
            state = query_database_state(environment["TRADING_EXECUTION_DATABASE_URL"])
        else:
            state = json.loads(_read_bounded_regular_file(args.database_state_json, "database state"))
        database_now = dt.datetime.now(dt.timezone.utc)
        validate_database_observation(state, now=database_now, max_age=evidence_age)
        database_account_identity_sha = validate_database_state(
            state,
            bindings,
            submission_state=args.submission_state,
            submission_disabled_accounts=submission_disabled_accounts,
            approved_risk_contracts=approved_risk_contracts,
        )
        delivery_watermark = validate_delivery_state(
            state, entry_submission_state=args.entry_submission_state
        )
        disabled_dry_run: dict[str, object] | None = None
        if args.submission_state == "enabled":
            disabled_evidence = read_disabled_evidence(args.disabled_evidence_json)
            disabled_dry_run = validate_disabled_evidence(
                disabled_evidence,
                now=database_now,
                max_age=dry_run_age,
                expected_trading_commit=expected_trading_commit,
                expected_prediction_version=expected_prediction_version,
                model_groups=model_groups,
                configuration_sha=configuration_sha,
                delivery_watermark=delivery_watermark,
                database_account_identity_sha256=database_account_identity_sha,
            )
            reject_enabled_cycles_after_evidence(state, disabled_dry_run)
        dry_run_now = dt.datetime.now(dt.timezone.utc)
        dry_run = validate_dry_run_state(
            state,
            bindings,
            submission_disabled_accounts=submission_disabled_accounts,
            not_before=(
                process_started_at
                if args.submission_state == "disabled"
                else dry_run_now - dry_run_age
            ),
            now=dry_run_now,
            max_age=dry_run_age,
            required=disabled_dry_run,
            entry_submission_state=args.entry_submission_state,
        )

        if args.prediction_snapshot_json is None:
            snapshot = fetch_prediction_snapshot(environment, str(dry_run["decision_at"]))
        else:
            snapshot = json.loads(
                _read_bounded_regular_file(args.prediction_snapshot_json, "Prediction snapshot")
            )
        manifest_markets = validate_snapshot_manifest(
            snapshot, dry_run, active_bindings
        )

        if args.consumer_state_json is None:
            consumer_state = collect_consumer_state(
                prediction_environment, model_groups
            )
        else:
            consumer_state = json.loads(
                _read_bounded_regular_file(args.consumer_state_json, "consumer state")
            )
        validate_consumer_state(
            consumer_state,
            prediction_environment,
            model_groups=model_groups,
            now=dt.datetime.now(dt.timezone.utc),
            max_age=evidence_age,
            max_idle=consumer_idle,
        )
        activation_deadline_text = "not-applicable"
        if args.submission_state == "enabled":
            if args.runtime_state_json is None:
                final_runtime_state = collect_runtime_state(
                    args.trading_health_url, args.prediction_health_url
                )
            else:
                try:
                    if os.path.samefile(
                        args.runtime_state_json, args.final_runtime_state_json
                    ):
                        raise PreflightError(
                            "enabled offline mode requires two different runtime evidence files"
                        )
                except OSError as error:
                    raise PreflightError(
                        f"cannot compare runtime evidence files: {error}"
                    ) from error
                final_runtime_state = json.loads(
                    _read_bounded_regular_file(
                        args.final_runtime_state_json, "final runtime state"
                    )
                )
            final_runtime_validation_now = dt.datetime.now(dt.timezone.utc)
            validate_runtime_state(
                final_runtime_state,
                environment,
                prediction_environment,
                expected_trading_commit=expected_trading_commit,
                expected_prediction_version=expected_prediction_version,
                now=final_runtime_validation_now,
                max_age=evidence_age,
            )
            validate_runtime_processes_unchanged(runtime_state, final_runtime_state)
            initial_runtime_observed_at = _utc_timestamp(
                runtime_state.get("observed_at"), "runtime observed_at"
            )
            final_runtime_observed_at = _utc_timestamp(
                final_runtime_state.get("observed_at"), "final runtime observed_at"
            )
            consumer_observed_at = _utc_timestamp(
                consumer_state.get("observed_at"), "consumer observed_at"
            )
            if final_runtime_observed_at <= max(
                initial_runtime_observed_at, consumer_observed_at
            ):
                raise PreflightError(
                    "final runtime evidence must be collected after the initial runtime "
                    "and Direct consumer evidence"
                )
            # Re-read the database last. RecoverStartup and a scheduled cycle can
            # mutate deliveries while the PIT/Redis evidence is being inspected.
            if args.database_state_json is None:
                final_state = query_database_state(
                    environment["TRADING_EXECUTION_DATABASE_URL"]
                )
            else:
                try:
                    if os.path.samefile(
                        args.database_state_json, args.final_database_state_json
                    ):
                        raise PreflightError(
                            "enabled offline mode requires two different database evidence files"
                        )
                except OSError as error:
                    raise PreflightError(
                        f"cannot compare database evidence files: {error}"
                    ) from error
                final_state = json.loads(
                    _read_bounded_regular_file(
                        args.final_database_state_json, "final database state"
                    )
                )
            final_validation_now = dt.datetime.now(dt.timezone.utc)
            validate_database_observation(
                final_state, now=final_validation_now, max_age=evidence_age
            )
            initial_database_observed_at = _utc_timestamp(
                state.get("observed_at"), "database observed_at"
            )
            final_database_observed_at = _utc_timestamp(
                final_state.get("observed_at"), "final database observed_at"
            )
            if final_database_observed_at <= max(
                initial_database_observed_at,
                consumer_observed_at,
                final_runtime_observed_at,
            ):
                raise PreflightError(
                    "final database evidence must be collected after the initial database "
                    "and final runtime/Direct consumer evidence"
                )
            final_account_identity_sha = validate_database_state(
                final_state,
                bindings,
                submission_state=args.submission_state,
                submission_disabled_accounts=submission_disabled_accounts,
                approved_risk_contracts=approved_risk_contracts,
            )
            if final_account_identity_sha != database_account_identity_sha:
                raise PreflightError(
                    "four-wallet database address identity changed during enabled preflight"
                )
            final_watermark = validate_delivery_state(
                final_state, entry_submission_state=args.entry_submission_state
            )
            if final_watermark != delivery_watermark:
                raise PreflightError(
                    "decision delivery state changed during the enabled preflight"
                )
            if disabled_dry_run is None:
                raise PreflightError("enabled preflight lost disabled dry-run evidence")
            final_dry_run_now = dt.datetime.now(dt.timezone.utc)
            final_dry_run = validate_dry_run_state(
                final_state,
                bindings,
                submission_disabled_accounts=submission_disabled_accounts,
                not_before=final_dry_run_now - dry_run_age,
                now=final_dry_run_now,
                max_age=dry_run_age,
                required=disabled_dry_run,
                entry_submission_state=args.entry_submission_state,
            )
            if final_dry_run != dry_run:
                raise PreflightError(
                    "disabled dry-run identity changed during the enabled preflight"
                )
            reject_enabled_cycles_after_evidence(final_state, disabled_dry_run)
            deadline_now = dt.datetime.now(dt.timezone.utc)
            if captured_next_due is None:
                raise PreflightError("enabled preflight lost its captured decision due time")
            deadline = activation_deadline(
                captured_next_due,
                deadline_now,
                args.minimum_activation_window_seconds,
            )
            activation_deadline_text = deadline.isoformat().replace("+00:00", "Z")
        evidence_path = ""
        if args.submission_state == "disabled":
            disabled_evidence = build_disabled_evidence(
                now=dt.datetime.now(dt.timezone.utc),
                expected_trading_commit=expected_trading_commit,
                expected_prediction_version=expected_prediction_version,
                model_groups=model_groups,
                configuration_sha=configuration_sha,
                dry_run=dry_run,
                manifest_markets=manifest_markets,
                delivery_watermark=delivery_watermark,
                database_account_identity_sha256=database_account_identity_sha,
                runtime_state=runtime_state,
            )
            write_disabled_evidence(args.write_disabled_evidence, disabled_evidence)
            evidence_path = str(args.write_disabled_evidence)
        elif not isinstance(disabled_evidence, dict) or disabled_evidence.get(
            "manifest_markets"
        ) != manifest_markets:
            raise PreflightError(
                "enabled phase manifest evidence differs from the disabled phase"
            )
    except (PreflightError, json.JSONDecodeError) as error:
        print(f"FOUR_WALLET_PREFLIGHT=FAIL reason={error}", file=sys.stderr)
        return 1
    print(
        "FOUR_WALLET_PREFLIGHT=PASS "
        f"bindings={len(bindings)} accounts={len(EXPECTED_ACCOUNTS)} "
        f"manifest_markets={manifest_markets} dry_run={dry_run['decision_at']} "
        f"submission_state={args.submission_state} consumer_groups={len(model_groups)} "
        f"disabled_evidence={evidence_path or args.disabled_evidence_json} "
        f"activation_deadline={activation_deadline_text} kill_switch=true"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
