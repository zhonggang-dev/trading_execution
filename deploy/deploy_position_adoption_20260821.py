#!/usr/bin/env python3
"""Fail-closed production cutover for adopting wallet positions into Python.

This one-shot script is deliberately conservative.  It proves the current
four-wallet topology, seals two independent venue snapshots, attributes the
complete post-baseline main-wallet SELL set to stable finalized Polygon receipts, and
only then adopts the remaining non-redeemable external positions as managed
lots.  It never enables order submission and never opens the database kill
switch.

The migration is additive and is installed before a candidate paper-mode
schema probe.  The adoption transaction is the irreversible boundary.  A
failure before that boundary restores the old runtime; a failure afterwards
keeps the candidate selected, keeps all gates closed, and requires fix-forward.

Running without the exact --execute-token performs local argument/self checks
only and cannot touch production.
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
import datetime as dt
import hashlib
import hmac
import json
import os
import pathlib
import pwd
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.error
import urllib.request
from collections.abc import Iterable
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR


SCRIPT_DIR = pathlib.Path(__file__).resolve().parent


class RuntimeTools:
    """Small self-contained deployment primitives used by this one-shot script."""

    @staticmethod
    def run(
        args: list[str],
        *,
        env: dict[str, str] | None = None,
        capture: bool = False,
        check: bool = True,
        preexec_fn=None,
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            args,
            env=env,
            text=True,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.STDOUT if capture else None,
            check=False,
            preexec_fn=preexec_fn,
        )
        if check and result.returncode != 0:
            output = (result.stdout or "").strip()
            if output:
                print(output, flush=True)
            raise RuntimeError(f"command failed ({result.returncode}): {args[0]}")
        return result

    @staticmethod
    def sha256(path: pathlib.Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def require_regular(path: pathlib.Path, expected_sha256: str | None = None) -> None:
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
            raise RuntimeError(f"required path is not a regular file: {path}")
        if expected_sha256 is not None and RuntimeTools.sha256(path) != expected_sha256:
            raise RuntimeError(f"SHA256 mismatch: {path}")

    @staticmethod
    def main_pid(unit: str) -> int:
        pid = RuntimeTools.run(
            ["systemctl", "show", unit, "-p", "MainPID", "--value"], capture=True
        ).stdout.strip()
        if not pid or pid == "0" or not pid.isdigit():
            raise RuntimeError(f"{unit} has no running process")
        return int(pid)

    @staticmethod
    def process_environment_for_pid(pid: int) -> dict[str, str]:
        raw = pathlib.Path(f"/proc/{pid}/environ").read_bytes()
        result: dict[str, str] = {}
        for item in raw.split(b"\0"):
            if b"=" not in item:
                continue
            key, value = item.split(b"=", 1)
            result[key.decode()] = value.decode()
        return result

    @staticmethod
    def process_environment(unit: str) -> dict[str, str]:
        return RuntimeTools.process_environment_for_pid(RuntimeTools.main_pid(unit))

    @staticmethod
    def environment_file(path: pathlib.Path) -> dict[str, str]:
        result: dict[str, str] = {}
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parts = shlex.split(line, comments=False, posix=True)
            if len(parts) != 1 or "=" not in parts[0]:
                raise RuntimeError(f"unsupported EnvironmentFile line for {path.name}")
            key, value = parts[0].split("=", 1)
            if key in result:
                raise RuntimeError(f"duplicate EnvironmentFile key: {key}")
            result[key] = value
        return result

    @staticmethod
    def atomic_write(
        path: pathlib.Path,
        content: str,
        mode: int = 0o600,
        uid: int = 0,
        gid: int = 0,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.chown(temporary, uid, gid)
            os.chmod(temporary, mode)
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    @staticmethod
    def update_environment_file(
        path: pathlib.Path, updates: dict[str, str], removes: set[str]
    ) -> None:
        lines = path.read_text(encoding="utf-8").splitlines()
        seen: set[str] = set()
        output: list[str] = []
        for line in lines:
            match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)=", line)
            if not match:
                output.append(line)
                continue
            key = match.group(1)
            if key in removes:
                continue
            if key in updates:
                output.append(f"{key}={updates[key]}")
                seen.add(key)
            else:
                output.append(line)
        if output and output[-1] != "":
            output.append("")
        for key, value in updates.items():
            if key not in seen:
                output.append(f"{key}={value}")
        RuntimeTools.atomic_write(path, "\n".join(output) + "\n")

    @staticmethod
    def atomic_symlink(target: pathlib.Path, link: pathlib.Path) -> None:
        temporary = link.with_name(f".{link.name}.next")
        if temporary.exists() or temporary.is_symlink():
            temporary.unlink()
        os.symlink(target, temporary)
        os.replace(temporary, link)

    @staticmethod
    def parse_database_url(
        value: str,
    ) -> tuple[list[str], dict[str, str], pathlib.Path]:
        parsed = urllib.parse.urlsplit(value)
        if parsed.scheme not in {"postgres", "postgresql"}:
            raise RuntimeError("unsupported PostgreSQL URL scheme")
        host = parsed.hostname or ""
        port = parsed.port or 5432
        username = urllib.parse.unquote(parsed.username or "")
        password = urllib.parse.unquote(parsed.password or "")
        database = urllib.parse.unquote(parsed.path.lstrip("/"))
        if not host or not username or not database:
            raise RuntimeError("PostgreSQL URL is incomplete")

        def escaped(item: str) -> str:
            return item.replace("\\", "\\\\").replace(":", "\\:")

        fd, pgpass_name = tempfile.mkstemp(prefix="trading-pgpass.", dir="/run")
        pgpass = pathlib.Path(pgpass_name)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(
                f"{escaped(host)}:{port}:{escaped(database)}:{escaped(username)}:"
                f"{escaped(password)}\n"
            )
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(pgpass, 0o600)
        query = urllib.parse.parse_qs(parsed.query)
        child_env = os.environ.copy()
        child_env["PGPASSFILE"] = str(pgpass)
        child_env["PGCONNECT_TIMEOUT"] = "10"
        child_env["PGOPTIONS"] = "-c lock_timeout=5s -c statement_timeout=120s"
        if query.get("sslmode"):
            child_env["PGSSLMODE"] = query["sslmode"][0]
        args = [
            "psql", "-X", "-v", "ON_ERROR_STOP=1", "-h", host, "-p", str(port),
            "-U", username, "-d", database,
        ]
        return args, child_env, pgpass

    @staticmethod
    def owner_url_for_database(
        owner_url: str, application_url: str, expected_owner: str
    ) -> str:
        owner = urllib.parse.urlsplit(owner_url)
        application = urllib.parse.urlsplit(application_url)
        if owner.scheme not in {"postgres", "postgresql"} or application.scheme not in {
            "postgres", "postgresql"
        }:
            raise RuntimeError("unsupported owner or application PostgreSQL URL")
        if owner.hostname != application.hostname or (owner.port or 5432) != (
            application.port or 5432
        ):
            raise RuntimeError("owner and Trading databases are not on the same instance")
        if urllib.parse.unquote(owner.username or "") != expected_owner:
            raise RuntimeError("owner credential does not belong to Trading table owner")
        return urllib.parse.urlunsplit(
            (owner.scheme, owner.netloc, application.path, application.query, "")
        )

    @staticmethod
    def http_status(url: str, timeout: int = 10) -> tuple[int, bytes]:
        request = urllib.request.Request(
            url, headers={"User-Agent": "trading-position-adoption/20260821"}
        )
        try:
            with NO_REDIRECT_OPENER.open(request, timeout=timeout) as response:
                return response.status, response.read(1024 * 1024)
        except urllib.error.HTTPError as error:
            return error.code, error.read(1024 * 1024)

    @staticmethod
    def wait_http(
        url: str, expected: set[int], timeout_seconds: int
    ) -> tuple[int, bytes]:
        deadline = time.monotonic() + timeout_seconds
        last: tuple[int, bytes] = (0, b"")
        while time.monotonic() < deadline:
            try:
                last = RuntimeTools.http_status(url)
                if last[0] in expected:
                    return last
            except Exception:
                pass
            time.sleep(2)
        raise RuntimeError(f"HTTP readiness timeout for {url}; last status={last[0]}")


deploy = RuntimeTools


ENV = pathlib.Path("/etc/trading-execution/env")
PREDICTION_ENV = pathlib.Path("/etc/prediction-infra/env")
CURRENT = pathlib.Path("/opt/trading-execution/current")
RELEASES = pathlib.Path("/opt/trading-execution/releases")
WALLET_FILE = pathlib.Path(
    "/run/secrets/trading_execution/wallets.decision-0-3.json"
)
SERVICE = "trading-execution.service"
HEALTH_URL = "http://127.0.0.1:14000/health/live"
READY_URL = "http://127.0.0.1:14000/health/ready"
PROBE_URL = "http://127.0.0.1:14101/health/ready"
ACTOR = "position-adoption-cutover-20260821"
EXPECTED_PREVIOUS_COMMIT = "49f50538f3189e82738f291ed3e923b0caaa8512"
EXECUTE_TOKEN = "ADOPT_EXTERNAL_POSITIONS_WITH_GATES_CLOSED_20260821"
PUSD_ADDRESS = "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb"
POLYGON_CHAIN_ID = 137
STANDARD_EXCHANGE = "0xe111180000d2663c0091e4f400237545b87b996b"
NEG_RISK_EXCHANGE = "0xe2222d279d744050d28e00520010520000310f59"
ORDER_FILLED_TOPIC = (
    "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
)
ERC20_TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)

NONTERMINAL_ORDER_STATUSES = (
    "RECEIVED",
    "VALIDATING",
    "RESERVED",
    "SUBMITTING",
    "ACKNOWLEDGED",
    "LIVE",
    "PARTIALLY_FILLED",
    "UNKNOWN",
    "CANCEL_PENDING",
    "RECONCILING",
)

ACCOUNT_ADDRESSES = {
    "main": "0x6c07b5e271ffa2da038ce05e498cff856bf32357",
    "wallet-1": "0x7b14065318fc2f00e2c971bd4cb3770eda7f6d17",
    "wallet-2": "0x6d280cc5b8e890ccdfdab99f51b0523d70a5985d",
    "wallet-3": "0x5344dcfd3beb2013701be5ae852a524c50297dc1",
}

BINDINGS = [
    {
        "prediction_model_id": "deepseek-v4-flash",
        "model_id": "echo",
        "strategy_id": "multfactor_v2",
        "execution_account_id": "main",
    },
    {
        "prediction_model_id": "deepseek-v4-flash",
        "model_id": "echo",
        "strategy_id": "multfactor_v1",
        "execution_account_id": "wallet-1",
    },
    {
        "prediction_model_id": "gemini-3.6-flash",
        "model_id": "gemini_masked",
        "strategy_id": "multfactor_v1",
        "execution_account_id": "wallet-2",
    },
    {
        "prediction_model_id": "gemini-3.6-flash",
        "model_id": "gemini_masked",
        "strategy_id": "multfactor_v2",
        "execution_account_id": "wallet-3",
    },
]

MAIN_ACTIVE_TOKEN = (
    "42006549920912635238720143234628794318400295281074422564226738635663399130499"
)
MAIN_REDEEMABLE_TOKEN = (
    "63284803361703132321048739956780460363879925620696486229140595548329650415109"
)
WALLET_1_TOKEN = (
    "30676432546818066080538703205639237969080188796746196694446265523208566227883"
)
WALLET_2_TOKEN = (
    "16040015440196279900485035793550429453516625694844857319147506590755961451627"
)
WALLET_3_TOKEN_A = (
    "77737603049704371401985143030203551130624520784925815473217168646825527075459"
)
WALLET_3_TOKEN_B = (
    "94909497204791422218953326989433848804063787259853889845128791095089809741999"
)

KNOWN_POSITIONS = {
    "main": {
        MAIN_ACTIVE_TOKEN: {
            "condition_id": "0x25fb28382075f418a944a781a9f8840e2f541152eea0d9798d1cabfa1466adbb",
            "outcome_index": 1,
            "outcome_name": "No",
            "neg_risk": True,
            "shares": "0.14",
            "redeemable": False,
            "action": "ADOPT",
        },
        MAIN_REDEEMABLE_TOKEN: {
            "condition_id": "0xa055e1cf533c2fcb1f1c89c20c41c8aa25526dcf27c3ff6be69c230fd8898282",
            "outcome_index": 0,
            "outcome_name": "Yes",
            "neg_risk": False,
            "shares": "2.94",
            "redeemable": True,
            "action": "RETAIN_BASELINE",
        },
    },
    "wallet-1": {
        WALLET_1_TOKEN: {
            "condition_id": "0x1e5204036bd51e6ca0e0da6221319c5839bd9940782adc6d0f6fa703aa8a3bf4",
            "outcome_index": 1,
            "outcome_name": "No",
            "neg_risk": False,
            "shares": "1",
            "redeemable": False,
            "action": "ADOPT",
        },
    },
    "wallet-2": {
        WALLET_2_TOKEN: {
            "condition_id": "0x7ad403c3508f8e3912940fd1a913f227591145ca0614074208e0b962d5fcc422",
            "outcome_index": 0,
            "outcome_name": "Yes",
            "neg_risk": True,
            "shares": "5",
            "redeemable": False,
            "action": "KEEP_MANAGED",
            "market_id": "561229",
        },
    },
    "wallet-3": {
        WALLET_3_TOKEN_A: {
            "condition_id": "0xcee04ba47d02f6c64642369b34193c355f64de1b9ae425c71ea4efef1989d271",
            "outcome_index": 1,
            "outcome_name": "No",
            "neg_risk": False,
            "shares": "1.98",
            "redeemable": False,
            "action": "ADOPT",
        },
        WALLET_3_TOKEN_B: {
            "condition_id": "0xc9615bb82d6535d630ff98e8094060bc767d8b9446516e98f84d2001a35e50e7",
            "outcome_index": 1,
            "outcome_name": "No",
            "neg_risk": True,
            "shares": "1.59",
            "redeemable": False,
            "action": "ADOPT",
        },
    },
}

ADOPTION_BINDING = {
    "main": ("echo", "multfactor_v2"),
    "wallet-1": ("echo", "multfactor_v1"),
    "wallet-3": ("gemini_masked", "multfactor_v2"),
}
EXPECTED_PYTHON_POSITION_COUNTS = {
    "main": 1,
    "wallet-1": 1,
    "wallet-2": 1,
    "wallet-3": 2,
}

REQUIRED_0016_TRIGGER_NAMES = (
    "execution_external_position_batches_items_required_trigger",
    "execution_external_position_batches_append_only_trigger",
    "execution_external_position_dispositions_insert_guard_trigger",
    "execution_external_position_dispositions_validate_trigger",
    "execution_external_position_dispositions_append_only_trigger",
    "execution_external_cash_adjustments_insert_guard_trigger",
    "execution_external_cash_adjustments_validate_trigger",
    "execution_external_cash_adjustments_append_only_trigger",
    "execution_external_position_adoptions_insert_guard_trigger",
    "execution_external_position_adoptions_validate_trigger",
    "execution_external_position_adoptions_append_only_trigger",
    "position_lots_origin_immutable_trigger",
    "position_events_external_adoption_immutable_trigger",
    "execution_account_events_external_adjustment_immutable_trigger",
)


class CutoverError(RuntimeError):
    """A production invariant was not satisfied; all mutable gates stay closed."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


NO_REDIRECT_OPENER = urllib.request.build_opener(NoRedirect())


@dataclasses.dataclass
class DatabaseSession:
    args: list[str]
    env: dict[str, str]
    pgpass: pathlib.Path


@dataclasses.dataclass
class MigrationSession:
    args: list[str]
    env: dict[str, str]
    role_prefix: list[str]
    application_role: str
    migration_role: str
    extra_pgpass: pathlib.Path | None


@dataclasses.dataclass
class RuntimeBackup:
    directory: pathlib.Path
    previous_release: pathlib.Path
    env_text: str


def log(message: str) -> None:
    print(message, flush=True)


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def rfc3339(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def parse_rfc3339(value: str) -> dt.datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(normalized)
    except ValueError as error:
        raise CutoverError("timestamp is not RFC3339") from error
    if parsed.tzinfo is None:
        raise CutoverError("timestamp is missing timezone")
    return parsed.astimezone(dt.timezone.utc)


def decimal(value: object, field: str = "decimal") -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise CutoverError(f"{field} is not a decimal") from error
    if not result.is_finite():
        raise CutoverError(f"{field} is not finite")
    return result


def decimal_text(value: object, field: str = "decimal") -> str:
    result = decimal(value, field)
    rendered = format(result, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered or "0"


def base_units(value: int, decimals: int = 6) -> Decimal:
    return Decimal(value) / (Decimal(10) ** decimals)


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def require_equal_sha256(left: bytes, right: bytes, label: str) -> str:
    left_sha256 = sha256_bytes(left)
    right_sha256 = sha256_bytes(right)
    if left_sha256 != right_sha256:
        raise CutoverError(f"{label} SHA-256 mismatch")
    return left_sha256


def stable_id(prefix: str, *parts: object) -> str:
    digest = sha256_bytes(canonical_json(list(parts)))
    return f"{prefix}:{digest}"


def sql_text(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_numeric(value: object) -> str:
    return sql_text(decimal_text(value)) + "::numeric"


def sql_derived_average(cost_expression: str, shares_expression: str) -> str:
    """Render a PostgreSQL-side cost/share quotient without Python rounding."""
    if not cost_expression or not shares_expression:
        raise CutoverError("average-cost SQL expression is empty")
    return f"({cost_expression})/NULLIF(({shares_expression}),0)"


def sql_bool(value: bool) -> str:
    return "TRUE" if value else "FALSE"


def db_scalar(session: DatabaseSession, sql: str) -> str:
    return deploy.run(
        session.args + ["-X", "-qAtc", sql], env=session.env, capture=True
    ).stdout.strip()


def db_json(session: DatabaseSession, sql: str) -> object:
    raw = db_scalar(session, sql)
    try:
        return json.loads(raw)
    except json.JSONDecodeError as error:
        raise CutoverError("database query returned invalid JSON") from error


def db_execute(session: DatabaseSession, sql: str) -> str:
    return db_scalar(session, sql)


def database_session(environment: dict[str, str]) -> DatabaseSession:
    url = environment.get("TRADING_EXECUTION_DATABASE_URL", "")
    if not url:
        raise CutoverError("TRADING_EXECUTION_DATABASE_URL is missing")
    args, child_env, pgpass = deploy.parse_database_url(url)
    return DatabaseSession(args=args, env=child_env, pgpass=pgpass)


def migration_session(
    database: DatabaseSession,
    trading_environment: dict[str, str],
    prediction_environment: dict[str, str],
) -> MigrationSession:
    owner_line = db_scalar(
        database,
        "SELECT current_user||'|'||c.relowner::regrole::text||'|'||"
        "pg_has_role(current_user,c.relowner,'USAGE')::text "
        "FROM pg_class c WHERE c.oid='public.execution_orders'::regclass",
    )
    parts = owner_line.split("|")
    if len(parts) != 3:
        raise CutoverError("cannot identify Trading table owner")
    application_role, owner_role, can_set_role = parts
    identifier = r"[A-Za-z_][A-Za-z0-9_$-]*"
    if not re.fullmatch(identifier, application_role) or not re.fullmatch(
        identifier, owner_role
    ):
        raise CutoverError("database role identifier is unsafe")
    if application_role == owner_role:
        return MigrationSession(
            database.args, database.env, [], application_role, owner_role, None
        )
    if can_set_role == "true":
        return MigrationSession(
            database.args,
            database.env,
            ["-v", f"migration_role={owner_role}", "-c", 'SET ROLE :"migration_role"'],
            application_role,
            owner_role,
            None,
        )
    owner_database_url = prediction_environment.get("DATABASE_URL", "")
    if not owner_database_url:
        raise CutoverError("database owner credential is unavailable")
    owner_url = deploy.owner_url_for_database(
        owner_database_url,
        trading_environment["TRADING_EXECUTION_DATABASE_URL"],
        owner_role,
    )
    args, child_env, pgpass = deploy.parse_database_url(owner_url)
    identity = deploy.run(
        args + ["-X", "-qAtc", "SELECT current_user"],
        env=child_env,
        capture=True,
    ).stdout.strip()
    if identity != owner_role:
        pgpass.unlink(missing_ok=True)
        raise CutoverError("owner connection authenticated as an unexpected role")
    return MigrationSession(args, child_env, [], application_role, owner_role, pgpass)


def migration_execute(session: MigrationSession, arguments: list[str]) -> str:
    return deploy.run(
        session.args + ["-X"] + session.role_prefix + arguments,
        env=session.env,
        capture=True,
    ).stdout.strip()


def get_json(url: str, headers: dict[str, str] | None = None) -> object:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "trading-position-adoption/20260821", **(headers or {})},
    )
    with NO_REDIRECT_OPENER.open(request, timeout=30) as response:
        body = response.read((16 << 20) + 1)
        if response.status != 200:
            raise CutoverError(f"GET returned HTTP {response.status}")
    if len(body) > 16 << 20:
        raise CutoverError("GET response exceeded 16 MiB")
    try:
        return json.loads(body)
    except json.JSONDecodeError as error:
        raise CutoverError("GET response was not JSON") from error


def rpc_json(url: str, method: str, params: list[object]) -> object:
    payload = canonical_json(
        {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    )
    request = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "trading-position-adoption/20260821",
        },
        method="POST",
    )
    with NO_REDIRECT_OPENER.open(request, timeout=30) as response:
        body = response.read((16 << 20) + 1)
        if response.status != 200:
            raise CutoverError(f"Polygon RPC returned HTTP {response.status}")
    if len(body) > 16 << 20:
        raise CutoverError("Polygon RPC response exceeded 16 MiB")
    try:
        envelope = json.loads(body)
    except json.JSONDecodeError as error:
        raise CutoverError("Polygon RPC response was not JSON") from error
    if not isinstance(envelope, dict) or envelope.get("error") is not None:
        raise CutoverError("Polygon RPC returned an error")
    if "result" not in envelope:
        raise CutoverError("Polygon RPC omitted result")
    return envelope["result"]


# Minimal secp256k1 + Keccak implementation keeps the wallet secret in memory
# and avoids logging it or installing a production dependency merely to derive
# the public signer address needed by the read-only CLOB L2 API.
SECP_P = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F
SECP_N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
SECP_G = (
    0x79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798,
    0x483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8,
)
KECCAK_ROUND_CONSTANTS = (
    0x0000000000000001,
    0x0000000000008082,
    0x800000000000808A,
    0x8000000080008000,
    0x000000000000808B,
    0x0000000080000001,
    0x8000000080008081,
    0x8000000000008009,
    0x000000000000008A,
    0x0000000000000088,
    0x0000000080008009,
    0x000000008000000A,
    0x000000008000808B,
    0x800000000000008B,
    0x8000000000008089,
    0x8000000000008003,
    0x8000000000008002,
    0x8000000000000080,
    0x000000000000800A,
    0x800000008000000A,
    0x8000000080008081,
    0x8000000000008080,
    0x0000000080000001,
    0x8000000080008008,
)
KECCAK_ROTATION = (
    (0, 36, 3, 41, 18),
    (1, 44, 10, 45, 2),
    (62, 6, 43, 15, 61),
    (28, 55, 25, 21, 56),
    (27, 20, 39, 8, 14),
)
MASK_64 = (1 << 64) - 1


def _rol64(value: int, shift: int) -> int:
    if shift == 0:
        return value & MASK_64
    return ((value << shift) | (value >> (64 - shift))) & MASK_64


def _keccak_f(state: list[int]) -> None:
    for round_constant in KECCAK_ROUND_CONSTANTS:
        c = [state[x] ^ state[x + 5] ^ state[x + 10] ^ state[x + 15] ^ state[x + 20] for x in range(5)]
        d = [c[(x - 1) % 5] ^ _rol64(c[(x + 1) % 5], 1) for x in range(5)]
        for y in range(5):
            for x in range(5):
                state[x + 5 * y] ^= d[x]
        b = [0] * 25
        for y in range(5):
            for x in range(5):
                b[y + 5 * ((2 * x + 3 * y) % 5)] = _rol64(
                    state[x + 5 * y], KECCAK_ROTATION[x][y]
                )
        for y in range(5):
            row = b[5 * y : 5 * y + 5]
            for x in range(5):
                state[x + 5 * y] = row[x] ^ ((~row[(x + 1) % 5]) & row[(x + 2) % 5])
        state[0] ^= round_constant


def keccak_256(payload: bytes) -> bytes:
    rate = 136
    padded = bytearray(payload)
    padded.append(0x01)
    while len(padded) % rate != rate - 1:
        padded.append(0)
    padded.append(0x80)
    state = [0] * 25
    for offset in range(0, len(padded), rate):
        block = padded[offset : offset + rate]
        for index in range(rate // 8):
            state[index] ^= int.from_bytes(block[index * 8 : index * 8 + 8], "little")
        _keccak_f(state)
    output = bytearray()
    while len(output) < 32:
        for index in range(rate // 8):
            output.extend(state[index].to_bytes(8, "little"))
        if len(output) < 32:
            _keccak_f(state)
    return bytes(output[:32])


def _secp_add(
    left: tuple[int, int] | None, right: tuple[int, int] | None
) -> tuple[int, int] | None:
    if left is None:
        return right
    if right is None:
        return left
    x1, y1 = left
    x2, y2 = right
    if x1 == x2 and (y1 + y2) % SECP_P == 0:
        return None
    if left == right:
        slope = (3 * x1 * x1) * pow(2 * y1, SECP_P - 2, SECP_P) % SECP_P
    else:
        slope = (y2 - y1) * pow((x2 - x1) % SECP_P, SECP_P - 2, SECP_P) % SECP_P
    x3 = (slope * slope - x1 - x2) % SECP_P
    y3 = (slope * (x1 - x3) - y1) % SECP_P
    return x3, y3


def _secp_multiply(scalar: int) -> tuple[int, int]:
    if scalar <= 0 or scalar >= SECP_N:
        raise CutoverError("wallet private key is outside secp256k1 range")
    result: tuple[int, int] | None = None
    addend: tuple[int, int] | None = SECP_G
    while scalar:
        if scalar & 1:
            result = _secp_add(result, addend)
        addend = _secp_add(addend, addend)
        scalar >>= 1
    if result is None:
        raise CutoverError("wallet private key produced point at infinity")
    return result


def ethereum_address(private_key: str) -> str:
    normalized = private_key.strip().lower()
    if normalized.startswith("0x"):
        normalized = normalized[2:]
    if not re.fullmatch(r"[0-9a-f]{64}", normalized):
        raise CutoverError("wallet private key is malformed")
    x, y = _secp_multiply(int(normalized, 16))
    public_key = x.to_bytes(32, "big") + y.to_bytes(32, "big")
    return "0x" + keccak_256(public_key)[-20:].hex()


def wallet_records() -> dict[str, dict[str, object]]:
    service = pwd.getpwnam("trading-execution")
    info = WALLET_FILE.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or stat.S_ISLNK(info.st_mode)
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_uid != service.pw_uid
        or info.st_gid != service.pw_gid
    ):
        raise CutoverError("four-wallet file ownership or mode is unsafe")
    root = json.loads(WALLET_FILE.read_text(encoding="utf-8"))
    if isinstance(root, dict) and isinstance(root.get("accounts"), list):
        records = {
            str(item.get("execution_account_id", "")): item
            for item in root["accounts"]
            if isinstance(item, dict)
        }
    elif isinstance(root, dict):
        records = {str(key): value for key, value in root.items() if isinstance(value, dict)}
    else:
        raise CutoverError("four-wallet file has unsupported JSON shape")
    if set(records) != set(ACCOUNT_ADDRESSES):
        raise CutoverError("four-wallet file account set is not exact")
    for account_id, expected_funder in ACCOUNT_ADDRESSES.items():
        record = records[account_id]
        funder = str(record.get("funder_address") or record.get("address") or "").lower()
        if funder != expected_funder:
            raise CutoverError(f"wallet identity mismatch for {account_id}")
        signer = ethereum_address(str(record.get("private_key", "")))
        signature_type = record.get("signature_type")
        if signer != funder and signature_type is None:
            raise CutoverError(f"proxy wallet {account_id} omitted signature_type")
        for field in ("api_key", "api_secret", "api_passphrase"):
            if not str(record.get(field, "")).strip():
                raise CutoverError(f"wallet {account_id} omitted {field}")
        record["_signer_address"] = signer
    return records


def fetch_data_api_positions(base_url: str, account_id: str) -> dict[str, object]:
    started_at = utc_now()
    pages: list[object] = []
    values: list[dict[str, object]] = []
    for page_number in range(20):
        endpoint = urllib.parse.urljoin(base_url.rstrip("/") + "/", "positions")
        endpoint += "?" + urllib.parse.urlencode(
            {
                "user": ACCOUNT_ADDRESSES[account_id],
                "sizeThreshold": "0",
                "includeArchived": "true",
                "limit": "500",
                "offset": str(page_number * 500),
            }
        )
        page = get_json(endpoint)
        if not isinstance(page, list) or any(not isinstance(item, dict) for item in page):
            raise CutoverError("Data API positions response is not an object array")
        pages.append(page)
        values.extend(page)
        if len(page) < 500:
            break
    else:
        raise CutoverError("Data API position pagination exceeded 20 pages")

    canonical: list[dict[str, object]] = []
    seen: set[str] = set()
    for value in values:
        shares = decimal_text(value.get("size"), "Data API shares")
        if decimal(shares) <= 0:
            continue
        token_id = str(value.get("asset", "")).strip()
        condition_id = str(value.get("conditionId", "")).strip().lower()
        outcome_name = str(value.get("outcome", "")).strip()
        outcome_index = value.get("outcomeIndex")
        neg_risk = value.get("negativeRisk")
        redeemable = value.get("redeemable")
        avg_price = decimal_text(value.get("avgPrice"), "Data API avgPrice")
        current_price = decimal_text(value.get("curPrice"), "Data API curPrice")
        if (
            not re.fullmatch(r"[0-9]+", token_id)
            or not re.fullmatch(r"0x[0-9a-f]{64}", condition_id)
            or not outcome_name
            or not isinstance(outcome_index, int)
            or outcome_index not in (0, 1)
            or not isinstance(neg_risk, bool)
            or not isinstance(redeemable, bool)
            or not (Decimal(0) < decimal(avg_price) < Decimal(1))
            or not (Decimal(0) <= decimal(current_price) <= Decimal(1))
        ):
            raise CutoverError("Data API returned incomplete position identity or valuation")
        if token_id in seen:
            raise CutoverError("Data API returned a duplicate token")
        seen.add(token_id)
        canonical.append(
            {
                "token_id": token_id,
                "condition_id": condition_id,
                "outcome_index": outcome_index,
                "outcome_name": outcome_name,
                "neg_risk": neg_risk,
                "shares": shares,
                "avg_price": avg_price,
                "current_price": current_price,
                "redeemable": redeemable,
            }
        )
    canonical.sort(key=lambda item: str(item["token_id"]))
    validate_known_position_snapshot(account_id, canonical)
    completed_at = utc_now()
    return {
        "started_at": rfc3339(started_at),
        "completed_at": rfc3339(completed_at),
        "raw_sha256": sha256_bytes(canonical_json(pages)),
        "canonical_sha256": sha256_bytes(canonical_json(canonical)),
        "canonical_items": canonical,
    }


def validate_known_position_snapshot(
    account_id: str, items: list[dict[str, object]]
) -> None:
    expected = KNOWN_POSITIONS[account_id]
    indexed = {str(item["token_id"]): item for item in items}
    if set(indexed) != set(expected):
        raise CutoverError(f"external position token set changed for {account_id}")
    for token_id, approved in expected.items():
        actual = indexed[token_id]
        for field in (
            "condition_id",
            "outcome_index",
            "outcome_name",
            "neg_risk",
            "shares",
            "redeemable",
        ):
            if actual.get(field) != approved.get(field):
                raise CutoverError(
                    f"external position {field} changed for {account_id}/{token_id}"
                )


def fetch_gamma_market(base_url: str, condition_id: str) -> dict[str, object]:
    candidates: list[dict[str, object]] = []
    for closed in ("false", "true"):
        endpoint = urllib.parse.urljoin(base_url.rstrip("/") + "/", "markets")
        endpoint += "?" + urllib.parse.urlencode(
            {"condition_ids": condition_id, "closed": closed, "limit": "2"}
        )
        page = get_json(endpoint)
        if not isinstance(page, list) or any(not isinstance(item, dict) for item in page):
            raise CutoverError("Gamma response is not an object array")
        if len(page) > 1:
            raise CutoverError("Gamma returned multiple markets in one state partition")
        for value in page:
            if str(value.get("conditionId", "")).strip().lower() != condition_id:
                raise CutoverError("Gamma condition_id mismatch")
            if bool(value.get("closed")) != (closed == "true"):
                raise CutoverError("Gamma closed partition contradicted market state")
            candidates.append(value)
    unique = {str(value.get("id", "")): value for value in candidates}
    if len(unique) != 1:
        raise CutoverError("Gamma did not return exactly one authoritative market")
    raw = next(iter(unique.values()))
    outcomes = raw.get("outcomes")
    tokens = raw.get("clobTokenIds")
    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)
    if isinstance(tokens, str):
        tokens = json.loads(tokens)
    if (
        not isinstance(outcomes, list)
        or not isinstance(tokens, list)
        or len(outcomes) != 2
        or len(tokens) != 2
        or any(not isinstance(item, str) or not item for item in outcomes)
        or any(not isinstance(item, str) or not re.fullmatch(r"[0-9]+", item) for item in tokens)
        or len(set(tokens)) != 2
        or not isinstance(raw.get("negRisk"), bool)
    ):
        raise CutoverError("Gamma market has invalid outcome mapping")
    market_id = str(raw.get("id", "")).strip()
    if not market_id:
        raise CutoverError("Gamma market omitted id")
    canonical = {
        "market_id": market_id,
        "condition_id": condition_id,
        "active": raw.get("active"),
        "closed": raw.get("closed"),
        "resolved": raw.get("resolved"),
        "accepting_orders": raw.get("acceptingOrders"),
        "enable_order_book": raw.get("enableOrderBook"),
        "neg_risk": raw.get("negRisk"),
        "outcomes": [
            {"index": index, "name": outcomes[index], "token_id": tokens[index]}
            for index in (0, 1)
        ],
    }
    return {
        **canonical,
        "canonical_sha256": sha256_bytes(canonical_json(canonical)),
    }


def capture_gamma_identities(
    base_url: str, position_snapshot: dict[str, dict[str, object]]
) -> dict[str, dict[str, object]]:
    conditions = sorted(
        {
            str(item["condition_id"])
            for account in position_snapshot.values()
            for item in account["canonical_items"]
        }
    )
    markets = {condition: fetch_gamma_market(base_url, condition) for condition in conditions}
    for account_id, account in position_snapshot.items():
        for item in account["canonical_items"]:
            condition = str(item["condition_id"])
            market = markets[condition]
            outcome = market["outcomes"][int(item["outcome_index"])]
            if (
                outcome["token_id"] != item["token_id"]
                or outcome["name"] != item["outcome_name"]
                or market["neg_risk"] is not item["neg_risk"]
            ):
                raise CutoverError(
                    f"Gamma/Data API identity mismatch for {account_id}/{item['token_id']}"
                )
            action = KNOWN_POSITIONS[account_id][str(item["token_id"])]["action"]
            if action in {"ADOPT", "KEEP_MANAGED"} and (
                market["active"] is not True
                or market["closed"] is not False
                or market["accepting_orders"] is not True
                or market["enable_order_book"] is not True
            ):
                raise CutoverError(
                    f"managed position market is not currently sellable for {account_id}/{item['token_id']}"
                )
    return markets


def capture_all_positions(base_url: str) -> dict[str, dict[str, object]]:
    return {
        account_id: fetch_data_api_positions(base_url, account_id)
        for account_id in ACCOUNT_ADDRESSES
    }


POSITION_STABILITY_FIELDS = (
    "token_id",
    "condition_id",
    "outcome_index",
    "outcome_name",
    "neg_risk",
    "shares",
    "avg_price",
    "redeemable",
)


def position_stability_projection(snapshot: dict[str, object]) -> list[dict[str, object]]:
    """Return ownership/economics fields, excluding the volatile mark price."""
    items = snapshot.get("canonical_items")
    if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
        raise CutoverError("Data API snapshot canonical_items is invalid")
    projected: list[dict[str, object]] = []
    for item in items:
        if any(field not in item for field in POSITION_STABILITY_FIELDS):
            raise CutoverError("Data API snapshot omitted a stability field")
        projected.append({field: item[field] for field in POSITION_STABILITY_FIELDS})
    projected.sort(key=lambda item: str(item["token_id"]))
    return projected


def database_clock(database: DatabaseSession) -> dt.datetime:
    value = db_scalar(
        database,
        "SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC',"
        "'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')",
    )
    return parse_rfc3339(value)


def dual_venue_snapshot(
    database: DatabaseSession, data_api_url: str, gamma_url: str
) -> tuple[
    dt.datetime,
    dict[str, dict[str, object]],
    dict[str, dict[str, object]],
    dict[str, dict[str, object]],
]:
    first = capture_all_positions(data_api_url)
    first_gamma = capture_gamma_identities(gamma_url, first)
    boundary = database_clock(database).replace(microsecond=0) + dt.timedelta(seconds=4)
    if any(
        parse_rfc3339(str(item["completed_at"])) >= boundary
        for item in first.values()
    ):
        raise CutoverError("snapshot A did not complete before the evidence boundary")
    while utc_now() <= boundary + dt.timedelta(seconds=1):
        time.sleep(0.25)
    if database_clock(database) <= boundary:
        raise CutoverError("database clock did not cross the evidence boundary")
    second = capture_all_positions(data_api_url)
    second_gamma = capture_gamma_identities(gamma_url, second)
    if any(
        parse_rfc3339(str(item["started_at"])) <= boundary
        for item in second.values()
    ):
        raise CutoverError("snapshot B did not begin after the evidence boundary")
    for account_id in ACCOUNT_ADDRESSES:
        if position_stability_projection(
            first[account_id]
        ) != position_stability_projection(second[account_id]):
            raise CutoverError(f"dual Data API snapshots differ for {account_id}")
    if first_gamma != second_gamma:
        raise CutoverError("dual Gamma identity snapshots differ")
    return boundary, first, second, second_gamma


def clob_server_time(base_url: str) -> int:
    endpoint = urllib.parse.urljoin(base_url.rstrip("/") + "/", "time")
    value = get_json(endpoint)
    if isinstance(value, str) and value.isdigit():
        parsed = int(value)
    elif isinstance(value, int):
        parsed = value
    else:
        raise CutoverError("CLOB /time returned an invalid timestamp")
    if parsed <= 0 or abs(parsed - int(time.time())) > 30:
        raise CutoverError("CLOB server clock differs from the production host")
    return parsed


def clob_auth_headers(
    record: dict[str, object], timestamp: int, method: str, path: str
) -> dict[str, str]:
    encoded_secret = str(record.get("api_secret", "")).strip()
    try:
        secret = base64.urlsafe_b64decode(
            encoded_secret + "=" * ((4 - len(encoded_secret) % 4) % 4)
        )
    except Exception as error:
        raise CutoverError("CLOB API secret is not base64url") from error
    message = f"{timestamp}{method.upper()}{path}".encode("utf-8")
    signature = base64.urlsafe_b64encode(
        hmac.new(secret, message, hashlib.sha256).digest()
    ).decode("ascii")
    return {
        "POLY_ADDRESS": str(record["_signer_address"]),
        "POLY_API_KEY": str(record["api_key"]),
        "POLY_PASSPHRASE": str(record["api_passphrase"]),
        "POLY_TIMESTAMP": str(timestamp),
        "POLY_SIGNATURE": signature,
    }


def clob_history_for_token(
    account_index: dict[str, object],
    account_id: str,
    condition_id: str,
    token_id: str,
) -> dict[str, object]:
    if account_index.get("execution_account_id") != account_id:
        raise CutoverError("CLOB account index identity changed")
    components = account_index.get("trade_components")
    if not isinstance(components, list) or any(
        not isinstance(item, dict) for item in components
    ):
        raise CutoverError("CLOB account component history is invalid")
    normalized = [
        item
        for item in components
        if item.get("condition_id") == condition_id and item.get("token_id") == token_id
    ]
    if not normalized:
        raise CutoverError(f"CLOB account history is empty for {account_id}/{token_id}")
    indexed: dict[tuple[str, str], dict[str, object]] = {}
    for item in normalized:
        key = (str(item["trade_id"]), str(item["order_hash"]))
        previous = indexed.get(key)
        if previous is not None and previous != item:
            raise CutoverError("duplicate CLOB trade component changed economic identity")
        indexed[key] = item
    ordered = sorted(
        indexed.values(),
        key=lambda item: (
            parse_rfc3339(str(item["match_time"])),
            str(item["trade_id"]),
            str(item["order_hash"]),
        ),
    )
    return {
        "account_id": account_id,
        "condition_id": condition_id,
        "token_id": token_id,
        "maker_address_filter": ACCOUNT_ADDRESSES[account_id],
        "account_history_sha256": account_index["canonical_sha256"],
        "normalized_sha256": sha256_bytes(canonical_json(ordered)),
        "trades": ordered,
    }


def parse_trade_time(value: object) -> dt.datetime:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if number > 1_000_000_000_000:
            number /= 1000
        return dt.datetime.fromtimestamp(number, tz=dt.timezone.utc)
    raw = str(value or "").strip()
    if raw.isdigit():
        number = int(raw)
        if number > 1_000_000_000_000:
            number //= 1000
        return dt.datetime.fromtimestamp(number, tz=dt.timezone.utc)
    return parse_rfc3339(raw)


def _hex_bytes(value: object, byte_length: int, field: str) -> bytes:
    raw = str(value or "").strip().lower()
    if not re.fullmatch(r"0x[0-9a-f]+", raw) or len(raw) != 2 + byte_length * 2:
        raise CutoverError(f"{field} is not canonical {byte_length}-byte hex")
    return bytes.fromhex(raw[2:])


def _hex_quantity(value: object, field: str) -> int:
    raw = str(value or "").strip().lower()
    if not re.fullmatch(r"0x(?:0|[1-9a-f][0-9a-f]*)", raw):
        raise CutoverError(f"{field} is not a canonical hex quantity")
    return int(raw, 16)


def _address_topic(value: object, field: str) -> str:
    raw = _hex_bytes(value, 32, field)
    if any(raw[:12]):
        raise CutoverError(f"{field} has nonzero address padding")
    return "0x" + raw[12:].hex()


def stable_receipt(
    rpc_url: str, transaction_hash: str, required_confirmations: int
) -> tuple[dict[str, object], int]:
    if rpc_json(rpc_url, "eth_chainId", []) != "0x89":
        raise CutoverError("Polygon RPC chain id is not 137")
    first = rpc_json(rpc_url, "eth_getTransactionReceipt", [transaction_hash])
    first_head = rpc_json(rpc_url, "eth_blockNumber", [])
    second = rpc_json(rpc_url, "eth_getTransactionReceipt", [transaction_hash])
    second_head = rpc_json(rpc_url, "eth_blockNumber", [])
    if not isinstance(first, dict) or first != second:
        raise CutoverError("Polygon receipt changed across reads")
    if first.get("transactionHash", "").lower() != transaction_hash:
        raise CutoverError("Polygon receipt transaction hash mismatch")
    if first.get("status") != "0x1":
        raise CutoverError("Polygon transaction is not finalized success")
    block_number = _hex_quantity(first.get("blockNumber"), "receipt blockNumber")
    block_hash = _hex_bytes(first.get("blockHash"), 32, "receipt blockHash")
    if not any(block_hash):
        raise CutoverError("Polygon receipt block hash is zero")
    head_a = _hex_quantity(first_head, "first Polygon head")
    head_b = _hex_quantity(second_head, "second Polygon head")
    if head_b < head_a or head_b < block_number:
        raise CutoverError("Polygon chain head regressed or trails the receipt")
    confirmations = head_b - block_number + 1
    if confirmations < required_confirmations:
        raise CutoverError(
            f"Polygon receipt has {confirmations} confirmations; require {required_confirmations}"
        )
    logs = first.get("logs")
    if not isinstance(logs, list) or any(not isinstance(item, dict) for item in logs):
        raise CutoverError("Polygon receipt logs are invalid")
    for item in logs:
        if item.get("removed") is not False:
            raise CutoverError("Polygon receipt contains removed/ambiguous log")
        if str(item.get("transactionHash", "")).lower() != transaction_hash:
            raise CutoverError("Polygon log transaction hash mismatch")
        if _hex_quantity(item.get("blockNumber"), "log blockNumber") != block_number:
            raise CutoverError("Polygon log block number mismatch")
        if _hex_bytes(item.get("blockHash"), 32, "log blockHash") != block_hash:
            raise CutoverError("Polygon log block hash mismatch")
    return first, confirmations


def decode_order_filled_events(
    receipt: dict[str, object], confirmations: int
) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for item in receipt["logs"]:
        address = str(item.get("address", "")).strip().lower()
        topics = item.get("topics")
        if (
            address not in {STANDARD_EXCHANGE, NEG_RISK_EXCHANGE}
            or not isinstance(topics, list)
            or not topics
            or str(topics[0]).strip().lower() != ORDER_FILLED_TOPIC
        ):
            continue
        if len(topics) != 4:
            raise CutoverError("OrderFilled event has an unexpected topic count")
        data = _hex_bytes(item.get("data"), 7 * 32, "OrderFilled data")
        words = [data[index * 32 : (index + 1) * 32] for index in range(7)]
        if any(words[0][:31]) or words[0][31] not in (0, 1):
            raise CutoverError("OrderFilled side is not canonical")
        result.append(
            {
                "exchange_address": address,
                "transaction_hash": str(item["transactionHash"]).lower(),
                "block_number": _hex_quantity(item["blockNumber"], "OrderFilled blockNumber"),
                "block_hash": str(item["blockHash"]).lower(),
                "log_index": _hex_quantity(item["logIndex"], "OrderFilled logIndex"),
                "confirmations": confirmations,
                "order_hash": "0x" + _hex_bytes(topics[1], 32, "OrderFilled order hash").hex(),
                "maker_address": _address_topic(topics[2], "OrderFilled maker"),
                "taker_address": _address_topic(topics[3], "OrderFilled taker"),
                "side": "BUY" if words[0][31] == 0 else "SELL",
                "token_id": str(int.from_bytes(words[1], "big")),
                "maker_amount_base_units": str(int.from_bytes(words[2], "big")),
                "taker_amount_base_units": str(int.from_bytes(words[3], "big")),
                "fee_base_units": str(int.from_bytes(words[4], "big")),
                "builder": "0x" + words[5].hex(),
                "metadata": "0x" + words[6].hex(),
            }
        )
    return result


def pusd_transfer_delta(receipt: dict[str, object], wallet_address: str) -> Decimal:
    delta = 0
    for item in receipt["logs"]:
        if str(item.get("address", "")).strip().lower() != PUSD_ADDRESS:
            continue
        topics = item.get("topics")
        if (
            not isinstance(topics, list)
            or len(topics) != 3
            or str(topics[0]).strip().lower() != ERC20_TRANSFER_TOPIC
        ):
            continue
        source = _address_topic(topics[1], "pUSD Transfer from")
        target = _address_topic(topics[2], "pUSD Transfer to")
        amount = int.from_bytes(_hex_bytes(item.get("data"), 32, "pUSD Transfer amount"), "big")
        if target == wallet_address:
            delta += amount
        if source == wallet_address:
            delta -= amount
    return base_units(delta)


def verify_trade_event(
    trade: dict[str, object],
    receipt: dict[str, object],
    confirmations: int,
    account_id: str,
    neg_risk: bool,
) -> dict[str, object]:
    events = decode_order_filled_events(receipt, confirmations)
    expected_exchange = NEG_RISK_EXCHANGE if neg_risk else STANDARD_EXCHANGE
    matches = [
        event
        for event in events
        if event["exchange_address"] == expected_exchange
        and event["order_hash"] == trade["order_hash"]
        and event["maker_address"] == ACCOUNT_ADDRESSES[account_id]
        and event["side"] == trade["side"]
        and event["token_id"] == trade["token_id"]
    ]
    if len(matches) != 1:
        raise CutoverError("receipt does not contain one exact account OrderFilled event")
    event = matches[0]
    if trade["side"] == "BUY":
        gross = base_units(int(str(event["maker_amount_base_units"])))
        shares = base_units(int(str(event["taker_amount_base_units"])))
    else:
        shares = base_units(int(str(event["maker_amount_base_units"])))
        gross = base_units(int(str(event["taker_amount_base_units"])))
    fee = base_units(int(str(event["fee_base_units"])))
    if shares != decimal(trade["shares"]):
        raise CutoverError("CLOB shares differ from finalized OrderFilled shares")
    expected_micro = decimal(trade["shares"]) * decimal(trade["price"]) * Decimal(1_000_000)
    allowed = {
        int(expected_micro.to_integral_value(rounding=ROUND_FLOOR)),
        int(expected_micro.to_integral_value(rounding=ROUND_CEILING)),
    }
    if int(gross * Decimal(1_000_000)) not in allowed:
        raise CutoverError("CLOB price/shares differ from finalized OrderFilled gross")
    net_cash = -(gross + fee) if trade["side"] == "BUY" else gross - fee
    if trade["side"] == "SELL" and net_cash < 0:
        raise CutoverError("finalized SELL fee exceeds gross proceeds")
    return {
        **trade,
        "gross_notional": decimal_text(gross),
        "total_fee": decimal_text(fee),
        "net_cash_delta": decimal_text(net_cash),
        "settlement_evidence": event,
    }


def reconstruct_fifo(
    history: dict[str, object], expected_shares: str
) -> dict[str, object]:
    queue: list[dict[str, object]] = []
    for trade in history["trades"]:
        shares = decimal(trade["shares"])
        if trade["side"] == "BUY":
            queue.append({**trade, "remaining_shares": shares})
            continue
        remaining_sell = shares
        while remaining_sell > 0:
            if not queue:
                raise CutoverError("FIFO history has a SELL before sufficient account BUYs")
            head = queue[0]
            consumed = min(decimal(head["remaining_shares"]), remaining_sell)
            head["remaining_shares"] = decimal(head["remaining_shares"]) - consumed
            remaining_sell -= consumed
            if decimal(head["remaining_shares"]) == 0:
                queue.pop(0)
    residual = [item for item in queue if decimal(item["remaining_shares"]) > 0]
    total = sum((decimal(item["remaining_shares"]) for item in residual), Decimal(0))
    if total != decimal(expected_shares):
        raise CutoverError(
            f"FIFO CLOB residual {decimal_text(total)} does not equal Data API {expected_shares}"
        )
    if not residual:
        raise CutoverError("FIFO reconstruction produced no residual acquisition")
    return {
        "rule": "FIFO",
        # A single adopted lot represents every residual FIFO acquisition.  Its
        # hold clock must therefore use the newest residual acquisition; using
        # the oldest would let newer shares exit before their own 48h minimum.
        "entered_at": max(str(item["match_time"]) for item in residual),
        "shares": decimal_text(total),
        "residual_acquisitions": [
            {**item, "remaining_shares": decimal_text(item["remaining_shares"])}
            for item in residual
        ],
    }


def collect_trade_and_fifo_evidence(
    environment: dict[str, str],
    records: dict[str, dict[str, object]],
    snapshot: dict[str, dict[str, object]],
    baseline_observed_at: dt.datetime,
) -> tuple[
    dict[str, object],
    dict[tuple[str, str], dict[str, object]],
    dict[str, dict[str, object]],
]:
    clob_url = environment.get("POLYMARKET_CLOB_URL", "").strip()
    rpc_url = environment.get("POLYGON_RPC_URL", "").strip()
    if not clob_url or not rpc_url:
        raise CutoverError("CLOB and Polygon RPC URLs are required")
    required_confirmations = int(environment.get("POLYGON_ORDER_FILLED_CONFIRMATIONS", "0"))
    if required_confirmations < 1 or required_confirmations > 10_000:
        raise CutoverError("POLYGON_ORDER_FILLED_CONFIRMATIONS is invalid")
    histories: dict[str, object] = {}
    fifo: dict[tuple[str, str], dict[str, object]] = {}
    receipt_cache: dict[str, tuple[dict[str, object], int]] = {}
    # Fetch one complete, funder-isolated history per account.  Token histories
    # are projections of owned components, never top-level trade.asset_id: a
    # MAKER trade can have a different top-level taker asset.
    account_trade_indexes = {
        account_id: fetch_clob_account_trade_ids(
            clob_url, records[account_id], account_id
        )
        for account_id in ACCOUNT_ADDRESSES
    }
    for account_id in ("main", "wallet-1", "wallet-3"):
        for token_id, approved in KNOWN_POSITIONS[account_id].items():
            if approved["action"] != "ADOPT":
                continue
            history = clob_history_for_token(
                account_trade_indexes[account_id],
                account_id,
                str(approved["condition_id"]),
                token_id,
            )
            key = account_id + ":" + token_id
            histories[key] = history
            rebuilt = reconstruct_fifo(history, str(approved["shares"]))
            verified_acquisitions: list[dict[str, object]] = []
            total_cost = Decimal(0)
            total_gross = Decimal(0)
            for acquisition in rebuilt["residual_acquisitions"]:
                tx_hash = str(acquisition["transaction_hash"])
                if tx_hash not in receipt_cache:
                    receipt_cache[tx_hash] = stable_receipt(
                        rpc_url, tx_hash, required_confirmations
                    )
                receipt, confirmations = receipt_cache[tx_hash]
                verified = verify_trade_event(
                    acquisition,
                    receipt,
                    confirmations,
                    account_id,
                    bool(approved["neg_risk"]),
                )
                ratio = decimal(acquisition["remaining_shares"]) / decimal(
                    acquisition["shares"]
                )
                allocated_gross = decimal(verified["gross_notional"]) * ratio
                allocated_fee = decimal(verified["total_fee"]) * ratio
                total_gross += allocated_gross
                total_cost += allocated_gross + allocated_fee
                verified_acquisitions.append(
                    {
                        **verified,
                        "remaining_shares": acquisition["remaining_shares"],
                        "allocated_gross": decimal_text(allocated_gross),
                        "allocated_fee": decimal_text(allocated_fee),
                        "allocated_cost": decimal_text(allocated_gross + allocated_fee),
                    }
                )
            shares = decimal(rebuilt["shares"])
            rebuilt["residual_acquisitions"] = verified_acquisitions
            rebuilt["remaining_cost"] = decimal_text(total_cost)
            rebuilt["average_entry_price"] = decimal_text(total_gross / shares)
            fifo[(account_id, token_id)] = rebuilt

    main_history = histories["main:" + MAIN_ACTIVE_TOKEN]
    post_baseline = [
        item
        for item in main_history["trades"]
        if parse_rfc3339(str(item["match_time"])) > baseline_observed_at
    ]
    if not post_baseline or any(item["side"] != "SELL" for item in post_baseline):
        raise CutoverError("main active token has a post-baseline trade that is not an external SELL")
    current_main_items = {
        str(item["token_id"]): item for item in snapshot["main"]["canonical_items"]
    }
    current_main_shares = decimal(current_main_items[MAIN_ACTIVE_TOKEN]["shares"])
    expected_disposed = Decimal("278.14") - current_main_shares
    disposed = sum(
        (decimal(item["shares"]) for item in post_baseline), Decimal(0)
    )
    if expected_disposed <= 0 or disposed != expected_disposed:
        raise CutoverError(
            "complete post-baseline main SELL history does not bridge immutable baseline to current shares"
        )
    sells = post_baseline
    verified_sells: list[dict[str, object]] = []
    by_transaction: dict[str, list[dict[str, object]]] = {}
    for trade in sells:
        tx_hash = str(trade["transaction_hash"])
        if tx_hash not in receipt_cache:
            receipt_cache[tx_hash] = stable_receipt(
                rpc_url, tx_hash, required_confirmations
            )
        receipt, confirmations = receipt_cache[tx_hash]
        verified = verify_trade_event(trade, receipt, confirmations, "main", True)
        verified_sells.append(verified)
        by_transaction.setdefault(tx_hash, []).append(verified)
    total_cash = Decimal(0)
    transaction_deltas: list[dict[str, object]] = []
    for tx_hash, components in sorted(by_transaction.items()):
        receipt, _confirmations = receipt_cache[tx_hash]
        exact_delta = pusd_transfer_delta(receipt, ACCOUNT_ADDRESSES["main"])
        expected_delta = sum(
            (decimal(item["net_cash_delta"]) for item in components), Decimal(0)
        )
        if exact_delta != expected_delta:
            raise CutoverError("pUSD Transfer delta differs from finalized SELL economics")
        total_cash += exact_delta
        transaction_deltas.append(
            {
                "transaction_hash": tx_hash,
                "trade_ids": sorted(str(item["trade_id"]) for item in components),
                "pusd_transfer_delta": decimal_text(exact_delta),
            }
        )
    if sum(
        (decimal(item["pusd_transfer_delta"]) for item in transaction_deltas),
        Decimal(0),
    ) != total_cash:
        raise CutoverError("transaction pUSD deltas do not sum to the main cash adjustment")
    evidence = {
        "schema": "trading.position-adoption-trade-evidence.v1",
        "captured_at": rfc3339(utc_now()),
        "histories": histories,
        "fifo": {
            account + ":" + token: value
            for (account, token), value in sorted(fifo.items())
        },
        "main_post_baseline_sells": verified_sells,
        "main_baseline_shares": "278.14",
        "main_current_shares": decimal_text(current_main_shares),
        "main_disposed_shares": decimal_text(disposed),
        "main_pusd_cash_delta": decimal_text(total_cash),
        "main_transaction_pusd_deltas": transaction_deltas,
    }
    evidence["evidence_sha256"] = sha256_bytes(canonical_json(evidence))
    return evidence, fifo, account_trade_indexes


def normalize_owned_account_trade_components(
    trade: dict[str, object], account_id: str
) -> list[dict[str, object]]:
    funder = ACCOUNT_ADDRESSES[account_id]
    trade_id = str(trade.get("id", "")).strip()
    condition_id = str(trade.get("market", "")).strip().lower()
    status = str(trade.get("status", "")).strip().upper()
    transaction_hash = str(trade.get("transaction_hash", "")).strip().lower()
    trader_side = str(trade.get("trader_side", "")).strip().upper()
    top_level_maker = str(trade.get("maker_address", "")).strip().lower()
    if (
        not trade_id
        or not re.fullmatch(r"0x[0-9a-f]{64}", condition_id)
        or status != "CONFIRMED"
        or not re.fullmatch(r"0x[0-9a-f]{64}", transaction_hash)
    ):
        raise CutoverError("CLOB account history contains incomplete trade identity")
    match_time = parse_trade_time(trade.get("match_time"))
    last_update = parse_trade_time(trade.get("last_update"))
    if last_update < match_time:
        raise CutoverError("CLOB account trade last_update predates match_time")
    maker_orders = trade.get("maker_orders")
    if not isinstance(maker_orders, list) or any(
        not isinstance(item, dict) for item in maker_orders
    ):
        raise CutoverError("CLOB account trade omitted maker_orders")

    common = {
        "trade_id": trade_id,
        "condition_id": condition_id,
        "transaction_hash": transaction_hash,
        "match_time": rfc3339(match_time),
        "last_update": rfc3339(last_update),
    }

    def component(
        order_id: object,
        token_id: object,
        side_value: object,
        shares_value: object,
        price_value: object,
        role: str,
    ) -> dict[str, object]:
        normalized_order = str(order_id or "").strip().lower()
        normalized_token = str(token_id or "").strip()
        side = str(side_value or "").strip().upper()
        shares = decimal_text(shares_value, "owned CLOB component shares")
        price = decimal_text(price_value, "owned CLOB component price")
        if (
            not re.fullmatch(r"0x[0-9a-f]{64}", normalized_order)
            or not re.fullmatch(r"[0-9]+", normalized_token)
            or side not in {"BUY", "SELL"}
            or decimal(shares) <= 0
            or not (Decimal(0) < decimal(price) < Decimal(1))
        ):
            raise CutoverError("owned CLOB trade component has invalid economic identity")
        return {
            **common,
            "venue_order_id": normalized_order,
            "order_hash": normalized_order,
            "token_id": normalized_token,
            "side": side,
            "shares": shares,
            "price": price,
            "ownership_role": role,
            "liquidity_role": "MAKER" if role == "MAKER" else "TAKER",
        }

    owned_maker_rows = [
        item
        for item in maker_orders
        if str(item.get("maker_address", "")).strip().lower() == funder
    ]

    def owned_maker_components() -> list[dict[str, object]]:
        return [
            component(
                item.get("order_id"),
                item.get("asset_id"),
                item.get("side"),
                item.get("matched_amount"),
                item.get("price"),
                "MAKER",
            )
            for item in owned_maker_rows
        ]

    result: list[dict[str, object]] = []
    if trader_side == "TAKER":
        if top_level_maker != funder:
            raise CutoverError("CLOB TAKER trade is not owned by requested funder")
        result.append(
            component(
                trade.get("taker_order_id"),
                trade.get("asset_id"),
                trade.get("side"),
                trade.get("size"),
                trade.get("price"),
                "TAKER",
            )
        )
    elif trader_side == "MAKER":
        owned_makers = owned_maker_components()
        if not owned_makers:
            raise CutoverError("CLOB MAKER trade has no requested-funder maker component")
        result.extend(owned_makers)
    elif trader_side == "":
        if top_level_maker == funder and str(trade.get("taker_order_id", "")).strip():
            result.append(
                component(
                    trade.get("taker_order_id"),
                    trade.get("asset_id"),
                    trade.get("side"),
                    trade.get("size"),
                    trade.get("price"),
                    "TAKER_LEGACY",
                )
            )
        result.extend(owned_maker_components())
        if not result:
            raise CutoverError("legacy CLOB trade has no address-proven owned component")
    else:
        raise CutoverError("CLOB account trade has unsupported trader_side")

    indexed: dict[tuple[str, str, str, str], dict[str, object]] = {}
    for item in result:
        key = (
            str(item["trade_id"]).lower(),
            str(item["venue_order_id"]).lower(),
            str(item["condition_id"]),
            str(item["token_id"]),
        )
        previous = indexed.get(key)
        if previous is not None and previous != item:
            raise CutoverError("owned CLOB trade component changed identity")
        indexed[key] = item
    return sorted(
        indexed.values(),
        key=lambda item: (
            str(item["trade_id"]).lower(),
            str(item["venue_order_id"]).lower(),
            str(item["token_id"]),
        ),
    )


def exact_issue_trade_component(
    issue: dict[str, object], account_index: dict[str, object]
) -> dict[str, object] | None:
    trade_id = str(issue.get("venue_trade_id", "")).strip().lower()
    order_id = str(issue.get("venue_order_id", "")).strip().lower()
    condition_id = str(issue.get("condition_id", "")).strip().lower()
    token_id = str(issue.get("token_id", "")).strip()
    if not trade_id or not order_id or not condition_id or not token_id:
        raise CutoverError("external-trade issue omitted exact component identity")
    trade_index = account_index.get("trade_index")
    if not isinstance(trade_index, dict):
        raise CutoverError("account-isolated trade component index is invalid")
    values = trade_index.get(trade_id, [])
    if not isinstance(values, list) or any(not isinstance(item, dict) for item in values):
        raise CutoverError("account-isolated trade component list is invalid")
    matches = [
        item
        for item in values
        if str(item["venue_order_id"]).lower() == order_id
        and item["condition_id"] == condition_id
        and item["token_id"] == token_id
    ]
    if len(matches) > 1:
        raise CutoverError("account-isolated history has duplicate exact issue component")
    return matches[0] if matches else None


def fetch_clob_account_trade_ids(
    base_url: str, record: dict[str, object], account_id: str
) -> dict[str, object]:
    path = "/data/trades"
    cursor = "MA=="
    pages: list[object] = []
    component_index: dict[tuple[str, str, str, str], dict[str, object]] = {}
    for _page_number in range(100):
        endpoint = urllib.parse.urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
        endpoint += "?" + urllib.parse.urlencode(
            {
                "maker_address": ACCOUNT_ADDRESSES[account_id],
                "next_cursor": cursor,
            }
        )
        timestamp = clob_server_time(base_url)
        payload = get_json(
            endpoint, clob_auth_headers(record, timestamp, "GET", path)
        )
        pages.append(payload)
        if isinstance(payload, list):
            page = payload
            next_cursor = ""
        elif isinstance(payload, dict):
            page = payload.get("data")
            if page is None:
                page = payload.get("items")
            next_cursor = str(
                payload.get("next_cursor") or payload.get("nextCursor") or ""
            )
        else:
            raise CutoverError("CLOB account history returned an unsupported shape")
        if not isinstance(page, list) or any(not isinstance(item, dict) for item in page):
            raise CutoverError("CLOB account history is not an object array")
        for item in page:
            for normalized in normalize_owned_account_trade_components(item, account_id):
                key = (
                    str(normalized["trade_id"]).lower(),
                    str(normalized["venue_order_id"]).lower(),
                    str(normalized["condition_id"]),
                    str(normalized["token_id"]),
                )
                previous = component_index.get(key)
                if previous is not None and previous != normalized:
                    raise CutoverError("CLOB account trade component changed identity")
                component_index[key] = normalized
        if not next_cursor or next_cursor == "LTE=" or next_cursor == cursor:
            break
        cursor = next_cursor
    else:
        raise CutoverError("CLOB account history pagination exceeded 100 pages")
    components = sorted(
        component_index.values(),
        key=lambda item: (
            parse_rfc3339(str(item["match_time"])),
            str(item["trade_id"]).lower(),
            str(item["venue_order_id"]).lower(),
            str(item["token_id"]),
        ),
    )
    trade_index: dict[str, list[dict[str, object]]] = {}
    for item in components:
        trade_index.setdefault(str(item["trade_id"]).lower(), []).append(item)
    return {
        "execution_account_id": account_id,
        "maker_address_filter": ACCOUNT_ADDRESSES[account_id],
        "raw_pages_sha256": sha256_bytes(canonical_json(pages)),
        "canonical_sha256": sha256_bytes(canonical_json(components)),
        "trade_ids": sorted(trade_index),
        "trade_components": components,
        "trade_index": trade_index,
    }


def require_account_trade_indexes_unchanged(
    expected: dict[str, dict[str, object]],
    actual: dict[str, dict[str, object]],
) -> None:
    if set(expected) != set(ACCOUNT_ADDRESSES) or set(actual) != set(ACCOUNT_ADDRESSES):
        raise CutoverError("CLOB stability check is not an exact four-account set")
    for account_id in ACCOUNT_ADDRESSES:
        left = expected[account_id]
        right = actual[account_id]
        if (
            left.get("execution_account_id") != account_id
            or right.get("execution_account_id") != account_id
            or left.get("maker_address_filter") != ACCOUNT_ADDRESSES[account_id]
            or right.get("maker_address_filter") != ACCOUNT_ADDRESSES[account_id]
        ):
            raise CutoverError(f"CLOB stability account identity changed for {account_id}")
        for value in (left, right):
            components = value.get("trade_components")
            if not isinstance(components, list) or value.get(
                "canonical_sha256"
            ) != sha256_bytes(canonical_json(components)):
                raise CutoverError(
                    f"CLOB canonical component hash is invalid for {account_id}"
                )
        if (
            left.get("canonical_sha256") != right.get("canonical_sha256")
            or left.get("trade_components") != right.get("trade_components")
        ):
            raise CutoverError(
                f"account-owned CLOB component history changed for {account_id}"
            )


def erc20_balance(rpc_url: str, wallet_address: str) -> Decimal:
    data = "0x70a08231" + ("0" * 24) + wallet_address[2:]
    raw = rpc_json(
        rpc_url,
        "eth_call",
        [{"to": PUSD_ADDRESS, "data": data}, "latest"],
    )
    if not isinstance(raw, str) or not re.fullmatch(r"0x[0-9a-fA-F]+", raw):
        raise CutoverError("Polygon pUSD balanceOf returned invalid data")
    return base_units(int(raw, 16))


def read_database_preflight(database: DatabaseSession) -> dict[str, object]:
    nonterminal = ",".join(sql_text(value) for value in NONTERMINAL_ORDER_STATUSES)
    result = db_json(
        database,
        f"""
SELECT json_build_object(
  'kill_switch',(SELECT kill_switch FROM execution_risk_global_control WHERE singleton=TRUE),
  'accounts',(SELECT COALESCE(json_agg(row_to_json(x) ORDER BY execution_account_id),'[]'::json)
    FROM (SELECT execution_account_id,lower(wallet_address) wallet_address,collateral_asset,
                 total_balance::text,available_balance::text,reserved_balance::text,
                 version,reconciled_at,created_at
            FROM execution_accounts
           WHERE execution_account_id IN ('main','wallet-1','wallet-2','wallet-3')) x),
  'bindings',(SELECT COALESCE(json_agg(row_to_json(x) ORDER BY execution_account_id),'[]'::json)
    FROM (SELECT model_id,strategy_id,execution_account_id,enabled
            FROM execution_strategy_bindings WHERE enabled) x),
  'policies',(SELECT COALESCE(json_agg(row_to_json(x) ORDER BY execution_account_id),'[]'::json)
    FROM (SELECT execution_account_id,policy_id,version,enabled,
                 max_order_notional::text,max_market_exposure::text,
                 max_strategy_exposure::text,max_wallet_exposure::text,
                 max_daily_traded_notional::text
            FROM execution_risk_policies
           WHERE execution_account_id IN ('main','wallet-1','wallet-2','wallet-3')) x),
  'controls',(SELECT COALESCE(json_agg(row_to_json(x) ORDER BY execution_account_id),'[]'::json)
    FROM (SELECT execution_account_id,paused,reason,version
            FROM execution_risk_controls
           WHERE execution_account_id IN ('main','wallet-1','wallet-2','wallet-3')
             AND control_scope='ACCOUNT' AND control_key='') x),
  'nonterminal_orders',(SELECT count(*) FROM execution_orders WHERE status IN ({nonterminal})),
  'active_reservations',(SELECT count(*) FROM asset_reservations
    WHERE status IN ('ACTIVE','RECONCILIATION_REQUIRED')),
  'pending_deliveries',(SELECT count(*) FROM strategy_order_intent_deliveries
    WHERE status IN ('PENDING','SUBMITTING')),
  'pending_buy_deliveries',(SELECT count(*) FROM strategy_order_intent_deliveries
    WHERE status IN ('PENDING','SUBMITTING')
      AND upper(COALESCE(intent_payload->>'side',intent_payload#>>'{{order,side}}',''))='BUY'),
  'positions',(SELECT COALESCE(json_agg(row_to_json(x) ORDER BY execution_account_id,token_id),'[]'::json)
    FROM (SELECT execution_account_id,market_id,condition_id,token_id,outcome_index,outcome_name,
                 total_shares::text,available_shares::text,reserved_shares::text,
                 cost_basis::text,average_cost_price::text,lifecycle_status
            FROM execution_positions
           WHERE total_shares<>0 OR reserved_shares<>0 OR cost_basis<>0) x),
  'lots',(SELECT COALESCE(json_agg(row_to_json(x) ORDER BY execution_account_id,opened_at,lot_id),'[]'::json)
    FROM (SELECT lot.lot_id,lot.execution_account_id,lot.market_id,lot.condition_id,lot.token_id,
                 lot.outcome_index,lot.outcome_name,lot.neg_risk,lot.model_id origin_model_id,
                 COALESCE(route.logical_model_id,lot.model_id) model_id,lot.strategy_id,
                 lot.original_shares::text,lot.remaining_shares::text,
                 lot.original_cost::text,lot.remaining_cost::text,
                 lot.average_entry_price::text,lot.status,lot.opened_at
            FROM position_lots lot LEFT JOIN position_lot_model_routes route ON route.lot_id=lot.lot_id
           WHERE lot.status IN ('OPEN','SETTLED_PENDING_REDEEM')) x),
  'baselines',(SELECT COALESCE(json_agg(row_to_json(x) ORDER BY execution_account_id,token_id),'[]'::json)
    FROM (SELECT header.baseline_id,header.execution_account_id,header.observed_at,
                 item.token_id,item.condition_id,item.outcome_index,item.outcome_name,item.neg_risk,
                 item.shares::text
            FROM execution_external_position_baselines header
            JOIN execution_external_position_baseline_items item
              ON item.baseline_id=header.baseline_id AND item.execution_account_id=header.execution_account_id
           WHERE header.execution_account_id IN ('main','wallet-1','wallet-3')) x),
  'open_issues',(SELECT COALESCE(json_agg(row_to_json(x) ORDER BY execution_account_id,issue_type,issue_id),'[]'::json)
    FROM (SELECT issue_id,run_id,fingerprint,execution_account_id,issue_type,resolution,status,
                 order_id,venue_order_id,venue_trade_id,market_id,condition_id,token_id,
                 local_value::text,remote_value::text,source,details,observed_at
            FROM reconciliation_issues
           WHERE execution_account_id IN ('main','wallet-1','wallet-2','wallet-3') AND status='OPEN') x)
);
""",
    )
    if not isinstance(result, dict):
        raise CutoverError("database preflight did not return an object")
    return result


def validate_database_preflight(state: dict[str, object]) -> dt.datetime:
    if state.get("kill_switch") is not True:
        raise CutoverError("database global kill switch is not closed")
    accounts = state.get("accounts")
    if not isinstance(accounts, list) or len(accounts) != 4:
        raise CutoverError("database does not contain the exact four accounts")
    account_index = {str(item.get("execution_account_id")): item for item in accounts}
    if set(account_index) != set(ACCOUNT_ADDRESSES):
        raise CutoverError("database account set changed")
    for account_id, address in ACCOUNT_ADDRESSES.items():
        row = account_index[account_id]
        if (
            row.get("wallet_address") != address
            or row.get("collateral_asset") != "pUSD"
            or decimal(row.get("reserved_balance")) != 0
            or decimal(row.get("total_balance")) != decimal(row.get("available_balance"))
        ):
            raise CutoverError(f"database account ledger is not settled for {account_id}")

    bindings = state.get("bindings")
    expected_bindings = {
        (item["model_id"], item["strategy_id"], item["execution_account_id"])
        for item in BINDINGS
    }
    actual_bindings = {
        (str(item.get("model_id")), str(item.get("strategy_id")), str(item.get("execution_account_id")))
        for item in bindings
        if isinstance(item, dict) and item.get("enabled") is True
    } if isinstance(bindings, list) else set()
    if actual_bindings != expected_bindings or len(bindings) != 4:
        raise CutoverError("database enabled binding set changed")
    policies = state.get("policies")
    controls = state.get("controls")
    if (
        not isinstance(policies, list)
        or len(policies) != 4
        or any(item.get("enabled") is not True for item in policies)
        or not isinstance(controls, list)
        or len(controls) != 4
        or any(item.get("paused") is not False for item in controls)
    ):
        raise CutoverError("four-wallet risk authorization is not in the approved staged state")
    for key in ("nonterminal_orders", "active_reservations", "pending_deliveries", "pending_buy_deliveries"):
        if state.get(key) != 0:
            raise CutoverError(f"unsafe pre-existing state: {key}={state.get(key)}")

    positions = state.get("positions")
    lots = state.get("lots")
    if not isinstance(positions, list) or len(positions) != 1:
        raise CutoverError("pre-adoption managed position set is not wallet-2 only")
    position = positions[0]
    approved_wallet_2 = KNOWN_POSITIONS["wallet-2"][WALLET_2_TOKEN]
    if (
        position.get("execution_account_id") != "wallet-2"
        or position.get("market_id") != approved_wallet_2["market_id"]
        or position.get("condition_id") != approved_wallet_2["condition_id"]
        or position.get("token_id") != WALLET_2_TOKEN
        or position.get("outcome_index") != approved_wallet_2["outcome_index"]
        or position.get("outcome_name") != approved_wallet_2["outcome_name"]
        or position.get("total_shares") != "5"
        or position.get("available_shares") != "5"
        or position.get("reserved_shares") != "0"
        or position.get("lifecycle_status") != "OPEN"
    ):
        raise CutoverError("wallet-2 managed position changed")
    if not isinstance(lots, list) or len(lots) != 1:
        raise CutoverError("pre-adoption open lot set is not wallet-2 only")
    lot = lots[0]
    if (
        lot.get("execution_account_id") != "wallet-2"
        or lot.get("token_id") != WALLET_2_TOKEN
        or lot.get("model_id") != "gemini_masked"
        or lot.get("strategy_id") != "multfactor_v1"
        or lot.get("remaining_shares") != "5"
        or lot.get("status") != "OPEN"
    ):
        raise CutoverError("wallet-2 managed lot route changed")

    expected_baselines = {
        (account_id, token_id): {
            **approved,
            "shares": "278.14" if token_id == MAIN_ACTIVE_TOKEN else approved["shares"],
        }
        for account_id, positions_by_token in KNOWN_POSITIONS.items()
        if account_id != "wallet-2"
        for token_id, approved in positions_by_token.items()
    }
    baselines = state.get("baselines")
    if not isinstance(baselines, list) or len(baselines) != 5:
        raise CutoverError("immutable external baseline item count changed")
    baseline_index = {
        (str(item.get("execution_account_id")), str(item.get("token_id"))): item
        for item in baselines
    }
    if set(baseline_index) != set(expected_baselines):
        raise CutoverError("immutable external baseline identity set changed")
    for key, approved in expected_baselines.items():
        row = baseline_index[key]
        for field in (
            "condition_id",
            "outcome_index",
            "outcome_name",
            "neg_risk",
            "shares",
        ):
            if row.get(field) != approved[field]:
                raise CutoverError(f"immutable external baseline {field} changed for {key}")
    main_baseline_at = parse_rfc3339(
        str(baseline_index[("main", MAIN_ACTIVE_TOKEN)]["observed_at"])
    )
    if account_index["main"].get("reconciled_at") is None or parse_rfc3339(
        str(account_index["main"]["reconciled_at"])
    ) != main_baseline_at:
        raise CutoverError("main account ownership boundary differs from immutable baseline")
    return main_baseline_at


def validate_onchain_balances(
    database_state: dict[str, object],
    environment: dict[str, str],
    main_cash_delta: str,
) -> dict[str, str]:
    rpc_url = environment.get("POLYGON_RPC_URL", "").strip()
    if not rpc_url:
        raise CutoverError("POLYGON_RPC_URL is missing")
    accounts = {
        str(item["execution_account_id"]): item for item in database_state["accounts"]
    }
    result: dict[str, str] = {}
    for account_id, address in ACCOUNT_ADDRESSES.items():
        actual = erc20_balance(rpc_url, address)
        local = decimal(accounts[account_id]["total_balance"])
        expected = local + (decimal(main_cash_delta) if account_id == "main" else Decimal(0))
        if actual != expected:
            raise CutoverError(
                f"on-chain pUSD does not equal attributable local ledger for {account_id}"
            )
        result[account_id] = decimal_text(actual)
    return result


def current_health(expected_commit: str) -> dict[str, object]:
    status, payload = deploy.http_status(HEALTH_URL)
    try:
        result = json.loads(payload)
    except json.JSONDecodeError as error:
        raise CutoverError("current liveness returned invalid JSON") from error
    if (
        status != 200
        or not isinstance(result, dict)
        or result.get("status") != "ok"
        or result.get("commit") != expected_commit
    ):
        raise CutoverError("current runtime health identity changed")
    return result


def verify_runtime_environment(environment: dict[str, str]) -> None:
    if environment.get("EXECUTION_MODE") != "live":
        raise CutoverError("Trading is not in live execution mode")
    if environment.get("POLYMARKET_LIVE_TRADING_ENABLED") != "true":
        raise CutoverError("Polymarket live adapter is not enabled")
    if environment.get("DECISION_CYCLE_ENABLED") != "true":
        raise CutoverError("decision cycle is not enabled")
    if environment.get("DECISION_CYCLE_ORDER_SUBMISSION_ENABLED") != "false":
        raise CutoverError("decision-cycle submission gate must already be false")
    if environment.get("POLYMARKET_ACCOUNTS_FILE") != str(WALLET_FILE):
        raise CutoverError("runtime wallet inventory path changed")
    try:
        bindings = json.loads(environment.get("DECISION_CYCLE_BINDINGS_JSON", "[]"))
    except json.JSONDecodeError as error:
        raise CutoverError("DECISION_CYCLE_BINDINGS_JSON is invalid") from error
    if bindings != BINDINGS:
        raise CutoverError("runtime four-wallet binding JSON changed")


def verify_candidate(
    args: argparse.Namespace,
) -> tuple[pathlib.Path, dict[str, str]]:
    source_tree = args.candidate_source_tree.resolve(strict=True)
    commit = deploy.run(
        [
            "git",
            "-c",
            f"safe.directory={source_tree}",
            "-C",
            str(source_tree),
            "rev-parse",
            "HEAD",
        ],
        capture=True,
    ).stdout.strip()
    if commit != args.expected_commit:
        raise CutoverError("candidate source commit mismatch")
    dirty = deploy.run(
        [
            "git",
            "-c",
            f"safe.directory={source_tree}",
            "-C",
            str(source_tree),
            "status",
            "--porcelain",
            "--untracked-files=no",
        ],
        capture=True,
    ).stdout.strip()
    if dirty:
        raise CutoverError("candidate source tree has modified tracked files")
    tracked_paths = [
        "migrations/0016_external_position_dispositions.sql",
        "deploy/deploy_position_adoption_20260821.py",
    ]
    deploy.run(
        [
            "git",
            "-c",
            f"safe.directory={source_tree}",
            "-C",
            str(source_tree),
            "ls-files",
            "--error-unmatch",
            *tracked_paths,
        ],
        capture=True,
    )
    relative_script = tracked_paths[1]
    source_script = source_tree / relative_script
    deploy.require_regular(source_script)
    committed_script = deploy.run(
        [
            "git",
            "-c",
            f"safe.directory={source_tree}",
            "-C",
            str(source_tree),
            "show",
            f"{commit}:{relative_script}",
        ],
        capture=True,
    ).stdout.encode("utf-8")
    source_bytes = source_script.read_bytes()
    running_script = pathlib.Path(__file__).resolve(strict=True)
    deploy.require_regular(running_script)
    script_sha256 = require_equal_sha256(
        committed_script, source_bytes, "candidate committed/source cutover script"
    )
    require_equal_sha256(
        source_bytes, running_script.read_bytes(), "candidate/running cutover script"
    )
    binary = source_tree / args.binary_relative_path
    deploy.require_regular(binary, args.expected_binary_sha256)
    return binary, {"path": relative_script, "sha256": script_sha256}


def install_release(binary: pathlib.Path, commit: str, expected_sha256: str) -> pathlib.Path:
    release = RELEASES / commit[:8]
    destination = release / "trading-execution"
    if release.exists():
        deploy.require_regular(destination, expected_sha256)
        return release
    release.mkdir(mode=0o755)
    shutil.copy2(binary, destination)
    os.chown(destination, 0, 0)
    os.chmod(destination, 0o755)
    os.chown(release, 0, 0)
    os.chmod(release, 0o755)
    return release


def backup_runtime(environment_text: str) -> RuntimeBackup:
    stamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
    directory = pathlib.Path("/var/backups/trading-execution") / (
        stamp + "-position-adoption"
    )
    directory.mkdir(parents=True, mode=0o700)
    os.chmod(directory, 0o700)
    previous_release = CURRENT.resolve(strict=True)
    deploy.atomic_write(directory / "env.before", environment_text, mode=0o600)
    deploy.atomic_write(
        directory / "release.before", str(previous_release) + "\n", mode=0o600
    )
    return RuntimeBackup(directory, previous_release, environment_text)


def write_private_json(path: pathlib.Path, value: object) -> None:
    deploy.atomic_write(
        path, json.dumps(value, indent=2, sort_keys=True) + "\n", mode=0o600
    )
    deploy.atomic_write(
        path.with_suffix(path.suffix + ".sha256"),
        sha256_bytes(path.read_bytes()) + "  " + path.name + "\n",
        mode=0o600,
    )


def configure_candidate_environment() -> None:
    deploy.update_environment_file(
        ENV,
        {
            "DECISION_CYCLE_ORDER_SUBMISSION_ENABLED": "false",
            "DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED": "true",
        },
        set(),
    )


def require_migration_trigger_names(
    migration_sql: str, required: Iterable[str] = REQUIRED_0016_TRIGGER_NAMES
) -> None:
    created = {
        match.group(1)
        for match in re.finditer(
            r"(?im)^CREATE\s+(?:CONSTRAINT\s+)?TRIGGER\s+([A-Za-z0-9_]+)\b",
            migration_sql,
        )
    }
    missing = sorted(set(required) - created)
    if missing:
        raise CutoverError(
            "0016 migration is missing required triggers: " + ",".join(missing)
        )


def migration_schema_assertions(database: DatabaseSession) -> None:
    """Prove the complete 0016 contract, not merely that one table exists."""

    result = db_scalar(
        database,
        r"""
SELECT (
  NOT EXISTS (
    SELECT 1 FROM (VALUES
      ('execution_external_position_adjustment_batches'),
      ('execution_external_position_dispositions'),
      ('execution_external_cash_adjustments'),
      ('execution_external_position_adoptions')
    ) expected(name)
    WHERE to_regclass('public.'||expected.name) IS NULL
  )
  AND NOT EXISTS (
    SELECT 1 FROM (VALUES
      ('position_lots','external_adoption_id'),
      ('position_events','external_adoption_id')
    ) expected(table_name,column_name)
    LEFT JOIN information_schema.columns actual
      ON actual.table_schema='public'
     AND actual.table_name=expected.table_name
     AND actual.column_name=expected.column_name
    WHERE actual.column_name IS NULL
  )
  AND NOT EXISTS (
    SELECT 1 FROM (VALUES
      ('execution_external_position_adjustment_batches_pkey'),
      ('execution_external_position_dispositions_batch_fk'),
      ('execution_external_position_dispositions_baseline_fk'),
      ('execution_external_position_dispositions_shape'),
      ('execution_external_position_dispositions_venue_shape'),
      ('execution_external_cash_adjustments_batch_fk'),
      ('execution_external_cash_adjustments_amount_shape'),
      ('position_lots_origin_xor'),
      ('position_lots_external_adoption_fk'),
      ('position_events_external_adoption_shape'),
      ('position_events_external_adoption_fk'),
      ('execution_external_position_adoptions_batch_fk'),
      ('execution_external_position_adoptions_disposition_fk'),
      ('execution_external_position_adoptions_baseline_fk'),
      ('execution_external_position_adoptions_lot_fk'),
      ('execution_external_position_adoptions_event_fk'),
      ('execution_external_position_adoptions_amount_shape')
    ) expected(name)
    LEFT JOIN pg_constraint actual
      ON actual.conname=left(expected.name,63)
     AND actual.connamespace='public'::regnamespace
    WHERE actual.oid IS NULL
  )
  AND NOT EXISTS (
    SELECT 1 FROM (VALUES
      ('execution_external_position_batches_items_required_trigger'),
      ('execution_external_position_batches_append_only_trigger'),
      ('execution_external_position_dispositions_insert_guard_trigger'),
      ('execution_external_position_dispositions_validate_trigger'),
      ('execution_external_position_dispositions_append_only_trigger'),
      ('execution_external_cash_adjustments_insert_guard_trigger'),
      ('execution_external_cash_adjustments_validate_trigger'),
      ('execution_external_cash_adjustments_append_only_trigger'),
      ('execution_external_position_adoptions_insert_guard_trigger'),
      ('execution_external_position_adoptions_validate_trigger'),
      ('execution_external_position_adoptions_append_only_trigger'),
      ('position_lots_origin_immutable_trigger'),
      ('position_events_external_adoption_immutable_trigger'),
      ('execution_account_events_external_adjustment_immutable_trigger')
    ) expected(name)
    LEFT JOIN pg_trigger actual
      ON actual.tgname=left(expected.name,63)
     AND NOT actual.tgisinternal AND actual.tgenabled<>'D'
    WHERE actual.oid IS NULL
  )
  AND to_regprocedure(
        'public.execution_require_external_position_adjustment_safety(text,text)'
      ) IS NOT NULL
  AND to_regprocedure(
        'public.execution_validate_external_position_disposition()'
      ) IS NOT NULL
  AND to_regprocedure(
        'public.execution_validate_external_cash_adjustment()'
      ) IS NOT NULL
  AND to_regprocedure(
        'public.execution_validate_external_position_adoption()'
      ) IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE connamespace='public'::regnamespace AND NOT convalidated
  )
)::text;
""",
    )
    if result != "true":
        raise CutoverError("0016 schema assertions failed")


def apply_migration_0016(
    database: DatabaseSession,
    migration: MigrationSession,
    source_tree: pathlib.Path,
) -> dict[str, object]:
    path = source_tree / "migrations/0016_external_position_dispositions.sql"
    deploy.require_regular(path)
    require_migration_trigger_names(path.read_text(encoding="utf-8"))
    migration_sha256 = deploy.sha256(path)
    state = db_scalar(
        database,
        r"""
SELECT concat_ws('|',
  (to_regclass('public.execution_external_position_adjustment_batches') IS NOT NULL)::text,
  (to_regclass('public.execution_external_position_dispositions') IS NOT NULL)::text,
  (to_regclass('public.execution_external_cash_adjustments') IS NOT NULL)::text,
  (to_regclass('public.execution_external_position_adoptions') IS NOT NULL)::text,
  EXISTS (SELECT 1 FROM information_schema.columns
           WHERE table_schema='public' AND table_name='position_lots'
             AND column_name='external_adoption_id')::text,
  EXISTS (SELECT 1 FROM information_schema.columns
           WHERE table_schema='public' AND table_name='position_events'
             AND column_name='external_adoption_id')::text
);
""",
    )
    absent = "false|false|false|false|false|false"
    present = "true|true|true|true|true|true"
    if state == absent:
        migration_execute(migration, ["-v", "ON_ERROR_STOP=1", "-f", str(path)])
        applied = True
        log("MIGRATION=0016:APPLIED")
    elif state == present:
        applied = False
        log("MIGRATION=0016:ALREADY_PRESENT")
    else:
        raise CutoverError("0016 schema is partially present")

    role = migration.application_role.replace('"', '""')
    migration_execute(
        migration,
        [
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "GRANT SELECT ON execution_external_position_adjustment_batches,"
            "execution_external_position_dispositions,"
            "execution_external_cash_adjustments,"
            f'execution_external_position_adoptions TO "{role}"',
        ],
    )
    migration_schema_assertions(database)
    return {
        "path": str(path),
        "sha256": migration_sha256,
        "applied": applied,
        "application_role": migration.application_role,
        "migration_role": migration.migration_role,
    }


def stop_service() -> None:
    deploy.run(["systemctl", "stop", SERVICE])
    state = deploy.run(
        ["systemctl", "is-active", SERVICE], capture=True, check=False
    ).stdout.strip()
    if state not in {"inactive", "failed"}:
        raise CutoverError("Trading service did not stop")


def start_service() -> None:
    deploy.run(["systemctl", "reset-failed", SERVICE], check=False)
    deploy.run(["systemctl", "start", SERVICE])


def close_mutable_gates(database: DatabaseSession, reason: str) -> None:
    db_execute(
        database,
        f"""
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='30s';
UPDATE execution_risk_global_control
   SET kill_switch=TRUE,reason={sql_text(reason)},version=version+1
 WHERE singleton=TRUE
   AND (kill_switch IS DISTINCT FROM TRUE OR reason<>{sql_text(reason)});
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM execution_orders WHERE status IN ({','.join(sql_text(value) for value in NONTERMINAL_ORDER_STATUSES)})) THEN
    RAISE EXCEPTION 'nonterminal orders exist';
  END IF;
  IF EXISTS (SELECT 1 FROM asset_reservations WHERE status IN ('ACTIVE','RECONCILIATION_REQUIRED')) THEN
    RAISE EXCEPTION 'active reservations exist';
  END IF;
  IF EXISTS (SELECT 1 FROM strategy_order_intent_deliveries WHERE status IN ('PENDING','SUBMITTING')) THEN
    RAISE EXCEPTION 'pending strategy delivery exists';
  END IF;
END $$;
COMMIT;
""",
    )


def run_candidate_schema_probe(
    binary: pathlib.Path,
    environment: dict[str, str],
    backup: RuntimeBackup,
    expected_commit: str,
) -> None:
    service = pwd.getpwnam("trading-execution")
    probe_environment = {
        **environment,
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "EXECUTION_MODE": "paper",
        "POLYMARKET_LIVE_TRADING_ENABLED": "false",
        "DECISION_CYCLE_ENABLED": "false",
        "DECISION_CYCLE_ORDER_SUBMISSION_ENABLED": "false",
        "DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED": "true",
        "HTTP_ADDRESS": "127.0.0.1:14101",
    }

    def demote() -> None:
        os.setgroups([])
        os.setgid(service.pw_gid)
        os.setuid(service.pw_uid)

    log_path = backup.directory / "candidate-schema-probe.log"
    with log_path.open("wb") as output:
        process = subprocess.Popen(
            [str(binary)],
            env=probe_environment,
            stdout=output,
            stderr=subprocess.STDOUT,
            preexec_fn=demote,
            start_new_session=True,
        )
        try:
            status, payload = deploy.wait_http(PROBE_URL, {200}, 90)
            value = json.loads(payload)
            if (
                status != 200
                or value.get("status") != "ready"
                or value.get("commit") != expected_commit
            ):
                raise CutoverError("candidate paper-mode schema probe identity mismatch")
        finally:
            process.terminate()
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
    if process.returncode not in {0, -15}:
        raise CutoverError("candidate paper-mode schema probe exited unexpectedly")


def run_candidate_pre_adoption_reconciliation(
    binary: pathlib.Path,
    environment: dict[str, str],
    backup: RuntimeBackup,
    database: DatabaseSession,
) -> dict[str, object]:
    """Run the new account-isolated reader once before disposition writes.

    The main baseline/balance mismatch must make live composition exit.  The
    sweep still writes four durable reconciliation results, proving that the
    candidate—not the old unfiltered CLOB reader—produced the issue set used by
    the irreversible transaction.
    """

    service = pwd.getpwnam("trading-execution")
    started_at = database_clock(database)
    child_environment = {
        **environment,
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "DECISION_CYCLE_ORDER_SUBMISSION_ENABLED": "false",
        "DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED": "true",
    }

    def demote() -> None:
        os.setgroups([])
        os.setgid(service.pw_gid)
        os.setuid(service.pw_uid)

    query = f"""
SELECT COALESCE(json_agg(row_to_json(x) ORDER BY execution_account_id),'[]'::json)
FROM (
  SELECT DISTINCT ON (execution_account_id)
         execution_account_id,run_id,trigger,status,summary,error,started_at,completed_at
    FROM reconciliation_runs
   WHERE execution_account_id IN ('main','wallet-1','wallet-2','wallet-3')
     AND started_at>={sql_text(rfc3339(started_at))}::timestamptz
   ORDER BY execution_account_id,started_at DESC,run_id DESC
) x;
"""
    log_path = backup.directory / "candidate-pre-adoption-reconciliation.log"
    result: object = []
    with log_path.open("wb") as output:
        process = subprocess.Popen(
            [str(binary)],
            env=child_environment,
            stdout=output,
            stderr=subprocess.STDOUT,
            preexec_fn=demote,
            start_new_session=True,
        )
        try:
            deadline = time.monotonic() + 660
            while time.monotonic() < deadline:
                result = db_json(database, query)
                if isinstance(result, list) and len(result) == 4:
                    break
                if process.poll() is not None:
                    raise CutoverError(
                        "candidate exited before recording four reconciliation runs"
                    )
                time.sleep(5)
            else:
                raise CutoverError(
                    "candidate did not record four pre-adoption reconciliation runs"
                )
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
    if not isinstance(result, list) or len(result) != 4:
        raise CutoverError("candidate pre-adoption sweep did not record four account runs")
    indexed = {str(item.get("execution_account_id")): item for item in result}
    if set(indexed) != set(ACCOUNT_ADDRESSES):
        raise CutoverError("candidate pre-adoption sweep account set changed")
    if indexed["main"].get("status") != "ATTENTION_REQUIRED":
        raise CutoverError("main pre-adoption sweep did not fail closed on attributable drift")
    for account_id in ("wallet-1", "wallet-2", "wallet-3"):
        if indexed[account_id].get("status") != "COMPLETED":
            raise CutoverError(
                f"candidate account-isolated reconciliation is not clean for {account_id}"
            )
    return {"started_at": rfc3339(started_at), "runs": result}


def classify_open_issues(
    issues: object,
    trade_evidence: dict[str, object],
    account_trade_indexes: dict[str, dict[str, object]],
    database_state: dict[str, object],
) -> list[dict[str, object]]:
    if not isinstance(issues, list) or any(not isinstance(item, dict) for item in issues):
        raise CutoverError("open reconciliation issue set is invalid")
    sell_by_id = {
        str(item["trade_id"]).lower(): item
        for item in trade_evidence["main_post_baseline_sells"]
    }
    account_balances = {
        str(item["execution_account_id"]): decimal(item["total_balance"])
        for item in database_state["accounts"]
    }
    main_expected_remote = account_balances["main"] + decimal(
        trade_evidence["main_pusd_cash_delta"]
    )
    classified: list[dict[str, object]] = []
    main_trade_ids: set[str] = set()
    baseline_boundaries: dict[str, dt.datetime] = {}
    for row in database_state["baselines"]:
        account_id = str(row["execution_account_id"])
        observed_at = parse_rfc3339(str(row["observed_at"]))
        previous = baseline_boundaries.get(account_id)
        if previous is None or observed_at < previous:
            baseline_boundaries[account_id] = observed_at
    account_rows = {
        str(item["execution_account_id"]): item for item in database_state["accounts"]
    }
    baseline_boundaries["wallet-2"] = parse_rfc3339(
        str(account_rows["wallet-2"]["created_at"])
    )

    ordered_sells = sorted(
        sell_by_id.values(),
        key=lambda item: (
            parse_rfc3339(str(item["match_time"])),
            str(item["trade_id"]),
        ),
    )
    position_prefixes: dict[Decimal, int] = {Decimal("278.14"): 0}
    balance_prefixes: dict[Decimal, int] = {account_balances["main"]: 0}
    cumulative_shares = Decimal(0)
    cumulative_cash = Decimal(0)
    for index, trade in enumerate(ordered_sells, start=1):
        cumulative_shares += decimal(trade["shares"])
        cumulative_cash += decimal(trade["net_cash_delta"])
        position_prefixes[Decimal("278.14") - cumulative_shares] = index
        balance_prefixes[account_balances["main"] + cumulative_cash] = index
    final_position_seen = False
    final_balance_seen = False
    for issue in issues:
        if issue.get("status") != "OPEN" or issue.get("resolution") != "MANUAL_REVIEW":
            raise CutoverError("adoption issue set contains a non-manual OPEN issue")
        account_id = str(issue.get("execution_account_id", ""))
        issue_type = str(issue.get("issue_type", ""))
        classification = ""
        if account_id == "main" and issue_type == "EXTERNAL_TRADE":
            trade_id = str(issue.get("venue_trade_id", "")).strip().lower()
            trade = sell_by_id.get(trade_id)
            if trade is not None:
                if (
                    issue.get("condition_id") != trade["condition_id"]
                    or issue.get("token_id") != trade["token_id"]
                    or str(issue.get("venue_order_id", "")).strip().lower()
                    != str(trade["order_hash"]).lower()
                ):
                    raise CutoverError("main EXTERNAL_TRADE issue changed finalized SELL identity")
                main_trade_ids.add(trade_id)
                classification = "ACCOUNTED_EXTERNAL_SELL"
            else:
                classification = classify_non_sell_external_trade(
                    issue, account_trade_indexes, baseline_boundaries
                )
        elif account_id == "main" and issue_type == "EXTERNAL_POSITION_BASELINE_DRIFT":
            remote = decimal(issue.get("remote_value"))
            if (
                issue.get("token_id") != MAIN_ACTIVE_TOKEN
                or issue.get("condition_id") != KNOWN_POSITIONS["main"][MAIN_ACTIVE_TOKEN]["condition_id"]
                or decimal(issue.get("local_value")) != Decimal("278.14")
                or remote not in position_prefixes
                or position_prefixes[remote] == 0
            ):
                raise CutoverError("main baseline-drift issue changed")
            final_position_seen = final_position_seen or remote == decimal(
                trade_evidence["main_current_shares"]
            )
            classification = "BASELINE_DISPOSED_AND_RESIDUAL_ADOPTED"
        elif account_id == "main" and issue_type == "BALANCE_DRIFT":
            remote = decimal(issue.get("remote_value"))
            if (
                decimal(issue.get("local_value")) != account_balances["main"]
                or remote not in balance_prefixes
                or balance_prefixes[remote] == 0
            ):
                raise CutoverError("main balance-drift issue is not the exact pUSD SELL delta")
            final_balance_seen = final_balance_seen or remote == main_expected_remote
            classification = "ATTRIBUTABLE_PUSD_CASH_ADJUSTMENT"
        elif issue_type == "EXTERNAL_TRADE" and account_id in ACCOUNT_ADDRESSES:
            classification = classify_non_sell_external_trade(
                issue, account_trade_indexes, baseline_boundaries
            )
        else:
            raise CutoverError(
                f"unrelated OPEN issue blocks adoption: {account_id}/{issue_type}"
            )
        classified.append({**issue, "classification": classification})

    if main_trade_ids != set(sell_by_id):
        raise CutoverError("OPEN main external-trade issues do not cover every external SELL component")
    if not final_position_seen or not final_balance_seen:
        raise CutoverError("latest main position/balance drift has not been durably observed")
    return classified


def classify_non_sell_external_trade(
    issue: dict[str, object],
    account_trade_indexes: dict[str, dict[str, object]],
    ownership_boundaries: dict[str, dt.datetime],
) -> str:
    account_id = str(issue.get("execution_account_id", ""))
    if account_id not in account_trade_indexes or account_id not in ownership_boundaries:
        raise CutoverError("external-trade issue has unknown account boundary")
    trade = exact_issue_trade_component(issue, account_trade_indexes[account_id])
    if trade is None:
        return "CLOB_ACCOUNT_FILTER_FALSE_ATTRIBUTION"
    if parse_rfc3339(str(trade["match_time"])) > ownership_boundaries[account_id]:
        raise CutoverError(
            "real post-ownership external trade remains unaccounted for: "
            f"{account_id}/{trade['trade_id']}/{trade['venue_order_id']}"
        )
    return "BASELINE_ACCOUNTED"


def fetch_order_book(base_url: str, token_id: str) -> dict[str, object]:
    endpoint = urllib.parse.urljoin(base_url.rstrip("/") + "/", "book")
    endpoint += "?" + urllib.parse.urlencode({"token_id": token_id})
    value = get_json(endpoint)
    if not isinstance(value, dict):
        raise CutoverError("CLOB order book is not an object")
    bids = value.get("bids")
    if not isinstance(bids, list) or any(not isinstance(item, dict) for item in bids):
        raise CutoverError("CLOB order book bids are invalid")
    best_bid: Decimal | None = None
    for row in bids:
        price = decimal(row.get("price"), "CLOB bid price")
        size = decimal(row.get("size"), "CLOB bid size")
        if not (Decimal(0) < price < Decimal(1)) or size <= 0:
            raise CutoverError("CLOB order book contains an invalid bid")
        if best_bid is None or price > best_bid:
            best_bid = price
    if best_bid is None:
        raise CutoverError("managed position has no current CLOB bid")
    minimum_size = decimal(value.get("min_order_size"), "CLOB minimum order size")
    if minimum_size <= 0:
        raise CutoverError("CLOB order book has invalid minimum order size")
    return {
        "best_bid": decimal_text(best_bid),
        "minimum_order_size": decimal_text(minimum_size),
        "observed_at": rfc3339(utc_now()),
        "raw_sha256": sha256_bytes(canonical_json(value)),
    }


def build_sell_plan(
    environment: dict[str, str],
    fifo: dict[tuple[str, str], dict[str, object]],
    database_state: dict[str, object],
) -> dict[str, object]:
    clob_url = environment.get("POLYMARKET_CLOB_URL", "").strip()
    if not clob_url:
        raise CutoverError("POLYMARKET_CLOB_URL is missing")
    policies = {
        str(item["execution_account_id"]): item for item in database_state["policies"]
    }
    now = utc_now()
    targets: list[dict[str, object]] = []
    maximum_by_account: dict[str, Decimal] = {}
    current_by_account: dict[str, Decimal] = {}
    staged: list[tuple[str, str, Decimal, dt.datetime, str]] = []
    for (account_id, token_id), reconstruction in sorted(fifo.items()):
        staged.append(
            (
                account_id,
                token_id,
                decimal(reconstruction["shares"]),
                parse_rfc3339(str(reconstruction["entered_at"])),
                "EXTERNAL_ADOPTION",
            )
        )
    wallet_2_lots = [
        lot
        for lot in database_state["lots"]
        if lot["execution_account_id"] == "wallet-2" and lot["token_id"] == WALLET_2_TOKEN
    ]
    if len(wallet_2_lots) != 1:
        raise CutoverError("sell plan cannot identify the exact wallet-2 managed lot")
    wallet_2_lot = wallet_2_lots[0]
    staged.append(
        (
            "wallet-2",
            WALLET_2_TOKEN,
            decimal(wallet_2_lot["remaining_shares"]),
            parse_rfc3339(str(wallet_2_lot["opened_at"])),
            "EXISTING_MANAGED_LOT",
        )
    )

    for account_id, token_id, shares, entered_at, source in sorted(staged):
        book = fetch_order_book(clob_url, token_id)
        current_notional = shares * decimal(book["best_bid"])
        absolute_max = shares
        eligible_at = entered_at + dt.timedelta(hours=48)
        below_minimum = shares < decimal(book["minimum_order_size"])
        if not below_minimum:
            maximum_by_account[account_id] = maximum_by_account.get(account_id, Decimal(0)) + absolute_max
            current_by_account[account_id] = current_by_account.get(account_id, Decimal(0)) + current_notional
        if below_minimum:
            planned_action = "HOLD_DUST_BELOW_MINIMUM_ORDER_SIZE"
        elif now < eligible_at:
            planned_action = "HOLD_48H"
        else:
            planned_action = "ELIGIBLE_FOR_SEPARATE_SELL_CUTOVER"
        targets.append(
            {
                "execution_account_id": account_id,
                "token_id": token_id,
                "source": source,
                "shares": decimal_text(shares),
                "entered_at": rfc3339(entered_at),
                "eligible_at": rfc3339(eligible_at),
                "held_48h": now >= eligible_at,
                "wait_seconds": max(0, int((eligible_at - now).total_seconds())),
                "minimum_order_size": book["minimum_order_size"],
                "below_minimum_order_size": below_minimum,
                "planned_action": planned_action,
                "top_up_forbidden": True,
                "current_best_bid": book["best_bid"],
                "current_best_bid_notional": decimal_text(current_notional),
                "absolute_max_sell_notional": decimal_text(absolute_max),
                "book_evidence": book,
            }
        )
    risk: list[dict[str, object]] = []
    for account_id in sorted(maximum_by_account):
        policy = policies[account_id]
        max_order = decimal(policy["max_order_notional"])
        max_daily = decimal(policy["max_daily_traded_notional"])
        required_order = max(
            decimal(target["absolute_max_sell_notional"])
            for target in targets
            if target["execution_account_id"] == account_id
        )
        required_daily = maximum_by_account[account_id]
        risk.append(
            {
                "execution_account_id": account_id,
                "policy_id": policy["policy_id"],
                "policy_version": policy["version"],
                "current_max_order_notional": decimal_text(max_order),
                "current_max_daily_traded_notional": decimal_text(max_daily),
                "required_max_order_notional_upper_bound": decimal_text(required_order),
                "required_max_daily_traded_notional_upper_bound": decimal_text(required_daily),
                "current_best_bid_total": decimal_text(current_by_account[account_id]),
                "requires_temporary_sell_limit_raise": max_order < required_order or max_daily < required_daily,
                "buy_exposure_caps_unchanged": True,
            }
        )
    body = {
        "schema": "trading.position-adoption-sell-plan.v1",
        "generated_at": rfc3339(now),
        "order_submission_enabled": False,
        "entry_submission_disabled": True,
        "global_kill_switch": True,
        "mutates_risk_policy": False,
        "top_up_dust_positions": False,
        "targets": targets,
        "risk": risk,
    }
    return {**body, "plan_sha256": sha256_bytes(canonical_json(body))}


def build_evidence_manifest(
    *,
    candidate_commit: str,
    candidate_binary_sha256: str,
    cutover_script: dict[str, str],
    migration: dict[str, object],
    snapshot_boundary: dt.datetime,
    snapshot_a: dict[str, dict[str, object]],
    snapshot_b: dict[str, dict[str, object]],
    gamma: dict[str, dict[str, object]],
    trade_evidence: dict[str, object],
    account_trade_indexes: dict[str, dict[str, object]],
    database_state: dict[str, object],
    classified_issues: list[dict[str, object]],
    onchain_balances: dict[str, str],
    sell_plan: dict[str, object],
    pre_adoption_reconciliation: dict[str, object],
    adopted_at: dt.datetime,
) -> dict[str, object]:
    body = {
        "schema": "trading.external-position-adjustment.v1",
        "created_at": rfc3339(utc_now()),
        "observed_at": rfc3339(adopted_at),
        "candidate": {
            "commit": candidate_commit,
            "binary_sha256": candidate_binary_sha256,
            "cutover_script": cutover_script,
            "migration_0016": migration,
        },
        "runtime_gates": {
            "decision_cycle_order_submission_enabled": False,
            "decision_cycle_entry_submission_disabled": True,
            "global_kill_switch": True,
        },
        "snapshot_boundary": rfc3339(snapshot_boundary),
        "data_api_snapshot_a": snapshot_a,
        "data_api_snapshot_b": snapshot_b,
        "gamma_markets": gamma,
        "trade_and_fifo_evidence": trade_evidence,
        "account_isolated_clob_indexes": account_trade_indexes,
        "database_preflight": database_state,
        "classified_open_issues": classified_issues,
        "onchain_pusd_balances": onchain_balances,
        "pre_adoption_candidate_reconciliation": pre_adoption_reconciliation,
        "sell_plan": sell_plan,
        "operator_scope": {
            "adopt_external_positions": True,
            "sell_orders_submitted_by_this_script": False,
            "risk_policy_mutated_by_this_script": False,
            "top_up_dust_positions": False,
        },
    }
    digest = sha256_bytes(canonical_json(body))
    return {**body, "manifest_sha256": digest}


def _baseline_indexes(
    database_state: dict[str, object],
) -> tuple[
    dict[tuple[str, str], dict[str, object]], dict[str, dict[str, object]]
]:
    by_token: dict[tuple[str, str], dict[str, object]] = {}
    by_account: dict[str, dict[str, object]] = {}
    for row in database_state["baselines"]:
        account_id = str(row["execution_account_id"])
        token_id = str(row["token_id"])
        key = (account_id, token_id)
        if key in by_token:
            raise CutoverError("database baseline contains a duplicate account/token")
        by_token[key] = row
        previous = by_account.get(account_id)
        if previous is not None and previous["baseline_id"] != row["baseline_id"]:
            raise CutoverError("database account has more than one immutable baseline header")
        by_account[account_id] = row
    return by_token, by_account


def build_adjustment_rows(
    *,
    manifest_sha256: str,
    adopted_at: dt.datetime,
    database_state: dict[str, object],
    snapshot: dict[str, dict[str, object]],
    gamma: dict[str, dict[str, object]],
    trade_evidence: dict[str, object],
    fifo: dict[tuple[str, str], dict[str, object]],
    account_trade_indexes: dict[str, dict[str, object]],
    classified_issues: list[dict[str, object]],
) -> dict[str, object]:
    if not re.fullmatch(r"[0-9a-f]{64}", manifest_sha256):
        raise CutoverError("adjustment manifest SHA-256 is invalid")
    batch_id = stable_id("external-position-adjustment", manifest_sha256)
    baseline_by_token, baseline_by_account = _baseline_indexes(database_state)
    snapshot_items = {
        (account_id, str(item["token_id"])): item
        for account_id, value in snapshot.items()
        for item in value["canonical_items"]
    }

    dispositions: list[dict[str, object]] = []
    cash_adjustments: list[dict[str, object]] = []
    account_events: list[dict[str, object]] = []
    adoptions: list[dict[str, object]] = []
    positions: list[dict[str, object]] = []
    lots: list[dict[str, object]] = []
    position_events: list[dict[str, object]] = []

    main_baseline = baseline_by_token.get(("main", MAIN_ACTIVE_TOKEN))
    if main_baseline is None or decimal(main_baseline["shares"]) != Decimal("278.14"):
        raise CutoverError("main immutable baseline is unavailable for dispositions")
    sell_issues: dict[str, list[dict[str, object]]] = {}
    for issue in classified_issues:
        if issue["classification"] == "ACCOUNTED_EXTERNAL_SELL":
            sell_issues.setdefault(str(issue["venue_trade_id"]), []).append(issue)

    running_shares = decimal(main_baseline["shares"])
    transition_tail: dict[tuple[str, str], tuple[int, Decimal, dt.datetime]] = {}
    sell_disposition_by_trade: dict[str, dict[str, object]] = {}
    ordered_sells = sorted(
        trade_evidence["main_post_baseline_sells"],
        key=lambda item: (
            parse_rfc3339(str(item["match_time"])),
            str(item["trade_id"]),
            str(item["order_hash"]),
        ),
    )
    if len({str(item["trade_id"]) for item in ordered_sells}) != len(ordered_sells):
        raise CutoverError("main SELL evidence has duplicate venue trade IDs")
    for sequence, trade in enumerate(ordered_sells, start=1):
        trade_id = str(trade["trade_id"])
        issues = sell_issues.get(trade_id, [])
        if not issues:
            raise CutoverError("finalized main SELL lacks an exact OPEN issue")
        before = running_shares
        delta = decimal(trade["shares"])
        after = before - delta
        if after < 0:
            raise CutoverError("main SELL transition over-consumes immutable baseline")
        disposition_id = stable_id(
            "external-sell", manifest_sha256, trade_id, trade["order_hash"]
        )
        row = {
            "disposition_id": disposition_id,
            "adjustment_batch_id": batch_id,
            "baseline_id": main_baseline["baseline_id"],
            "execution_account_id": "main",
            "condition_id": trade["condition_id"],
            "token_id": trade["token_id"],
            "disposition_kind": "EXTERNAL_SELL",
            "transition_sequence": sequence,
            "shares_before": decimal_text(before),
            "shares_delta": decimal_text(delta),
            "shares_after": decimal_text(after),
            "venue_trade_id": trade_id,
            "venue_order_id": trade["order_hash"],
            "transaction_hash": trade["transaction_hash"],
            "occurred_at": trade["match_time"],
            "evidence": {
                "manifest_sha256": manifest_sha256,
                "open_issue_ids": sorted(str(issue["issue_id"]) for issue in issues),
                "finalized_trade": trade,
            },
            "actor": ACTOR,
            "reason": "account for finalized external SELL after immutable ownership baseline",
        }
        dispositions.append(row)
        sell_disposition_by_trade[trade_id] = row
        running_shares = after
    expected_current = decimal(trade_evidence["main_current_shares"])
    if running_shares != expected_current:
        raise CutoverError("main disposition chain does not end at the sealed remote remainder")
    transition_tail[("main", MAIN_ACTIVE_TOKEN)] = (
        len(ordered_sells),
        running_shares,
        parse_rfc3339(str(ordered_sells[-1]["match_time"])),
    )

    # One pUSD cash adjustment is recorded per Polygon transaction.  The
    # account-event chain uses the same deterministic order as migration 0016.
    transaction_evidence = {
        str(item["transaction_hash"]): item
        for item in trade_evidence["main_transaction_pusd_deltas"]
    }
    grouped_sells: dict[str, list[dict[str, object]]] = {}
    for trade in ordered_sells:
        grouped_sells.setdefault(str(trade["transaction_hash"]), []).append(trade)
    cash_staged: list[dict[str, object]] = []
    for transaction_hash, trades in grouped_sells.items():
        transaction = transaction_evidence.get(transaction_hash)
        if transaction is None:
            raise CutoverError("main SELL transaction lacks exact pUSD Transfer evidence")
        trade_ids = sorted(str(item["trade_id"]) for item in trades)
        if trade_ids != sorted(str(value) for value in transaction["trade_ids"]):
            raise CutoverError("pUSD transaction evidence trade component set changed")
        delta = decimal(transaction["pusd_transfer_delta"])
        if delta <= 0:
            raise CutoverError("external SELL pUSD cash adjustment is not positive")
        occurred_at = max(parse_rfc3339(str(item["match_time"])) for item in trades)
        cash_id = stable_id("external-cash", manifest_sha256, transaction_hash)
        event_id = stable_id("account-event", manifest_sha256, transaction_hash)
        cash_staged.append(
            {
                "cash_adjustment_id": cash_id,
                "adjustment_batch_id": batch_id,
                "execution_account_id": "main",
                "transaction_hash": transaction_hash,
                "asset": "pUSD",
                "total_delta": decimal_text(delta),
                "available_delta": decimal_text(delta),
                "reserved_delta": "0",
                "account_event_id": event_id,
                "occurred_at": rfc3339(occurred_at),
                "evidence": {
                    "manifest_sha256": manifest_sha256,
                    "transaction_pusd_transfer": transaction,
                    "sell_disposition_ids": sorted(
                        sell_disposition_by_trade[trade_id]["disposition_id"]
                        for trade_id in trade_ids
                    ),
                },
                "actor": ACTOR,
                "reason": "apply exact finalized pUSD proceeds for external SELL transaction",
            }
        )
    cash_staged.sort(key=lambda item: (str(item["occurred_at"]), str(item["cash_adjustment_id"])))
    main_account = next(
        item for item in database_state["accounts"] if item["execution_account_id"] == "main"
    )
    balance = decimal(main_account["total_balance"])
    for cash in cash_staged:
        before = balance
        balance += decimal(cash["total_delta"])
        cash["balance_before"] = decimal_text(before)
        cash["balance_after"] = decimal_text(balance)
        cash_adjustments.append(cash)
        account_events.append(
            {
                "account_event_id": cash["account_event_id"],
                "execution_account_id": "main",
                "event_type": "EXTERNAL_POSITION_DISPOSITION",
                "order_id": "",
                "fill_key": "",
                "total_balance_delta": cash["total_delta"],
                "available_balance_delta": cash["available_delta"],
                "reserved_balance_delta": "0",
                "total_balance_after": cash["balance_after"],
                "available_balance_after": cash["balance_after"],
                "reserved_balance_after": "0",
                "occurred_at": cash["occurred_at"],
            }
        )
    if balance - decimal(main_account["total_balance"]) != decimal(
        trade_evidence["main_pusd_cash_delta"]
    ):
        raise CutoverError("cash adjustment chain does not equal sealed pUSD delta")

    # Audit and resolve only exact historical issues. BASELINE_ACCOUNTED may
    # refer to a fully closed token which is no longer a baseline item; its
    # ownership boundary is the account's immutable baseline header.
    audit_groups: dict[tuple[str, str, str, str, str], list[dict[str, object]]] = {}
    for issue in classified_issues:
        classification = str(issue["classification"])
        if classification not in {
            "BASELINE_ACCOUNTED",
            "CLOB_ACCOUNT_FILTER_FALSE_ATTRIBUTION",
        }:
            continue
        key = (
            classification,
            str(issue["execution_account_id"]),
            str(issue["venue_trade_id"]),
            str(issue["condition_id"]),
            str(issue["token_id"]),
        )
        audit_groups.setdefault(key, []).append(issue)
    for key, issues in sorted(audit_groups.items()):
        classification, account_id, trade_id, condition_id, token_id = key
        order_ids = {str(issue["venue_order_id"]) for issue in issues}
        if "" in order_ids or len(order_ids) != 1:
            raise CutoverError("historical issue does not identify one exact venue order")
        venue_order_id = next(iter(order_ids))
        account_index = account_trade_indexes[account_id]
        exact_components = [
            exact_issue_trade_component(issue, account_index) for issue in issues
        ]
        if classification == "BASELINE_ACCOUNTED":
            if any(item is None for item in exact_components):
                raise CutoverError(
                    "baseline-accounted issue lost exact account-owned trade component"
                )
            unique_components = {
                canonical_json(item) for item in exact_components if item is not None
            }
            if len(unique_components) != 1:
                raise CutoverError("baseline-accounted issue group changed exact component")
            trade = next(item for item in exact_components if item is not None)
            baseline = baseline_by_account.get(account_id)
            if baseline is None:
                raise CutoverError("baseline-accounted issue lost account-isolated evidence")
            kind = "BASELINE_ACCOUNTED"
            baseline_id: object = baseline["baseline_id"]
            transaction_hash = trade["transaction_hash"]
            occurred_at = trade["match_time"]
            evidence = {
                "manifest_sha256": manifest_sha256,
                "open_issue_ids": sorted(str(issue["issue_id"]) for issue in issues),
                "account_isolated_trade": trade,
                "account_history_sha256": account_index["raw_pages_sha256"],
                "account_components_sha256": account_index["canonical_sha256"],
                "ownership_boundary": baseline["observed_at"],
            }
        else:
            if any(item is not None for item in exact_components):
                raise CutoverError(
                    "false-attribution exact component appeared in account-isolated history"
                )
            kind = "FALSE_ATTRIBUTION"
            baseline_id = None
            transaction_hash = ""
            occurred_at = min(str(issue["observed_at"]) for issue in issues)
            evidence = {
                "manifest_sha256": manifest_sha256,
                "open_issue_ids": sorted(str(issue["issue_id"]) for issue in issues),
                "absent_from_account_isolated_history": True,
                "maker_address_filter": account_index["maker_address_filter"],
                "account_history_sha256": account_index["raw_pages_sha256"],
                "account_components_sha256": account_index["canonical_sha256"],
            }
        dispositions.append(
            {
                "disposition_id": stable_id(
                    "external-trade-audit", manifest_sha256, *key
                ),
                "adjustment_batch_id": batch_id,
                "baseline_id": baseline_id,
                "execution_account_id": account_id,
                "condition_id": condition_id,
                "token_id": token_id,
                "disposition_kind": kind,
                "transition_sequence": None,
                "shares_before": "0",
                "shares_delta": "0",
                "shares_after": "0",
                "venue_trade_id": trade_id,
                "venue_order_id": venue_order_id,
                "transaction_hash": transaction_hash,
                "occurred_at": occurred_at,
                "evidence": evidence,
                "actor": ACTOR,
                "reason": (
                    "trade predates immutable account ownership capture"
                    if kind == "BASELINE_ACCOUNTED"
                    else "resolve prior cross-account CLOB attribution after exact funder scan"
                ),
            }
        )

    for (account_id, token_id), reconstruction in sorted(fifo.items()):
        baseline = baseline_by_token.get((account_id, token_id))
        item = snapshot_items.get((account_id, token_id))
        if baseline is None or item is None:
            raise CutoverError("adoption target lost baseline or Data API identity")
        condition_id = str(item["condition_id"])
        market = gamma.get(condition_id)
        if market is None:
            raise CutoverError("adoption target lost Gamma market identity")
        shares = decimal(reconstruction["shares"])
        cost = decimal(reconstruction["remaining_cost"])
        entry_price = decimal(reconstruction["average_entry_price"])
        current_price = decimal(item["current_price"])
        if shares != decimal(item["shares"]) or cost <= 0 or not (
            Decimal(0) < entry_price <= Decimal(1)
        ):
            raise CutoverError("adoption FIFO economics changed")
        if current_price <= 0:
            raise CutoverError("adoption target has no positive current mark")
        opened_at = parse_rfc3339(str(reconstruction["entered_at"]))
        baseline_time = parse_rfc3339(str(baseline["observed_at"]))
        if opened_at > baseline_time or baseline_time > adopted_at:
            raise CutoverError(
                "adoption acquisition/ownership/cutover timestamps are not ordered"
            )
        model_id, strategy_id = ADOPTION_BINDING[account_id]
        tail_sequence, tail_shares, tail_time = transition_tail.get(
            (account_id, token_id),
            (0, decimal(baseline["shares"]), parse_rfc3339(str(baseline["observed_at"]))),
        )
        if tail_shares != shares or adopted_at < tail_time:
            raise CutoverError("adoption does not consume the exact effective baseline remainder")
        disposition_id = stable_id("adoption-disposition", manifest_sha256, account_id, token_id)
        adoption_id = stable_id("external-adoption", manifest_sha256, account_id, token_id)
        lot_id = stable_id("adopted-lot", manifest_sha256, account_id, token_id)
        event_id = stable_id("adopted-position-event", manifest_sha256, account_id, token_id)
        evidence = {
            "manifest_sha256": manifest_sha256,
            "data_api_snapshot_sha256": snapshot[account_id]["canonical_sha256"],
            "gamma_market_sha256": market["canonical_sha256"],
            "fifo_reconstruction": reconstruction,
        }
        dispositions.append(
            {
                "disposition_id": disposition_id,
                "adjustment_batch_id": batch_id,
                "baseline_id": baseline["baseline_id"],
                "execution_account_id": account_id,
                "condition_id": condition_id,
                "token_id": token_id,
                "disposition_kind": "ADOPTION",
                "transition_sequence": tail_sequence + 1,
                "shares_before": decimal_text(shares),
                "shares_delta": decimal_text(shares),
                "shares_after": "0",
                "venue_trade_id": "",
                "venue_order_id": "",
                "transaction_hash": "",
                "occurred_at": rfc3339(adopted_at),
                "evidence": evidence,
                "actor": ACTOR,
                "reason": "adopt exact external baseline remainder into managed Python lot",
            }
        )
        market_value = shares * current_price
        unrealized = market_value - cost
        adoptions.append(
            {
                "external_adoption_id": adoption_id,
                "adjustment_batch_id": batch_id,
                "disposition_id": disposition_id,
                "baseline_id": baseline["baseline_id"],
                "execution_account_id": account_id,
                "lot_id": lot_id,
                "position_event_id": event_id,
                "market_id": market["market_id"],
                "condition_id": condition_id,
                "token_id": token_id,
                "outcome_index": item["outcome_index"],
                "outcome_name": item["outcome_name"],
                "neg_risk": item["neg_risk"],
                "model_id": model_id,
                "strategy_id": strategy_id,
                "shares": decimal_text(shares),
                "remaining_cost": decimal_text(cost),
                "average_entry_price": decimal_text(entry_price),
                "opened_at": rfc3339(opened_at),
                "adopted_at": rfc3339(adopted_at),
                "evidence": evidence,
                "actor": ACTOR,
                "reason": "take managed ownership without fabricating a venue fill",
            }
        )
        positions.append(
            {
                "execution_account_id": account_id,
                "market_id": market["market_id"],
                "condition_id": condition_id,
                "token_id": token_id,
                "outcome_index": item["outcome_index"],
                "outcome_name": item["outcome_name"],
                "total_shares": decimal_text(shares),
                "available_shares": decimal_text(shares),
                "reserved_shares": "0",
                "cost_basis": decimal_text(cost),
                "realized_pnl": "0",
                "mark_price": decimal_text(current_price),
                "market_value": decimal_text(market_value),
                "unrealized_pnl": decimal_text(unrealized),
                "is_dust": False,
                "lifecycle_status": "OPEN",
                "reconciled_at": rfc3339(adopted_at),
            }
        )
        lots.append(
            {
                "lot_id": lot_id,
                "execution_account_id": account_id,
                "market_id": market["market_id"],
                "condition_id": condition_id,
                "token_id": token_id,
                "outcome_index": item["outcome_index"],
                "outcome_name": item["outcome_name"],
                "neg_risk": item["neg_risk"],
                "model_id": model_id,
                "strategy_id": strategy_id,
                "opening_order_id": None,
                "opening_fill_key": None,
                "external_adoption_id": adoption_id,
                "original_shares": decimal_text(shares),
                "remaining_shares": decimal_text(shares),
                "original_cost": decimal_text(cost),
                "remaining_cost": decimal_text(cost),
                "average_entry_price": decimal_text(entry_price),
                "status": "OPEN",
                "opened_at": rfc3339(opened_at),
            }
        )
        position_events.append(
            {
                "position_event_id": event_id,
                "event_type": "ADOPTED",
                "execution_account_id": account_id,
                "market_id": market["market_id"],
                "token_id": token_id,
                "order_id": "",
                "fill_key": "",
                "external_adoption_id": adoption_id,
                "model_id": model_id,
                "strategy_id": strategy_id,
                "shares_delta": decimal_text(shares),
                "cash_delta": "0",
                "cost_basis_delta": decimal_text(cost),
                "realized_pnl_delta": "0",
                "shares_after": decimal_text(shares),
                "cost_basis_after": decimal_text(cost),
                "realized_pnl_after": "0",
                "mark_price": decimal_text(current_price),
                "unrealized_pnl_after": decimal_text(unrealized),
                "occurred_at": rfc3339(adopted_at),
            }
        )

    if len(adoptions) != 4 or len(positions) != 4 or len(lots) != 4 or len(position_events) != 4:
        raise CutoverError("managed adoption row count is not exactly four")
    if {item["execution_account_id"] for item in adoptions} != {"main", "wallet-1", "wallet-3"}:
        raise CutoverError("managed adoption account set changed")
    return {
        "adjustment_batch_id": batch_id,
        "observed_at": rfc3339(adopted_at),
        "main_balance_after": decimal_text(balance),
        "dispositions": dispositions,
        "cash_adjustments": cash_adjustments,
        "account_events": account_events,
        "adoptions": adoptions,
        "positions": positions,
        "lots": lots,
        "position_events": position_events,
    }


def _json_sql(value: object) -> str:
    return sql_text(json.dumps(value, sort_keys=True, separators=(",", ":"))) + "::jsonb"


def require_preserved_main_boundary_sql(sql: str, reconciled_at: str) -> None:
    match = re.search(r"UPDATE\s+execution_accounts\s+(.*?)\;", sql, re.DOTALL)
    if match is None:
        raise CutoverError("main account adjustment UPDATE is missing")
    update = match.group(1)
    assignments = update.split(" WHERE ", 1)[0]
    if re.search(r"\breconciled_at\s*=", assignments):
        raise CutoverError("cash adjustment must not advance account ownership boundary")
    expected = (
        "reconciled_at IS NOT DISTINCT FROM "
        + sql_text(reconciled_at)
        + "::timestamptz"
    )
    if sql.count(expected) < 2:
        raise CutoverError("cash adjustment does not lock and reassert ownership boundary")


def require_database_derived_average_sql(sql: str) -> None:
    position_expression = sql_derived_average("cost_basis", "total_shares")
    event_expression = sql_derived_average("cost_basis_after", "shares_after")
    if sql.count(position_expression) != 1 or sql.count(event_expression) != 1:
        raise CutoverError("adoption SQL does not derive both averages in PostgreSQL")
    if (
        "cost_basis numeric,average_cost_price numeric" in sql
        or "cost_basis_after numeric,average_cost_after numeric" in sql
    ):
        raise CutoverError("adoption SQL still accepts a Python-computed average")


def adjustment_transaction_sql(
    *,
    manifest: dict[str, object],
    rows: dict[str, object],
    classified_issues: list[dict[str, object]],
    database_state: dict[str, object],
) -> str:
    batch_id = str(rows["adjustment_batch_id"])
    manifest_sha256 = str(manifest["manifest_sha256"])
    main_account = next(
        item for item in database_state["accounts"] if item["execution_account_id"] == "main"
    )
    if main_account.get("reconciled_at") is None:
        raise CutoverError("main ownership boundary is missing before adjustment")
    main_reconciled_at = rfc3339(
        parse_rfc3339(str(main_account["reconciled_at"]))
    )
    expected_issue_count = len(classified_issues)
    expected_dispositions = len(rows["dispositions"])
    expected_cash = len(rows["cash_adjustments"])
    expected_adoptions = len(rows["adoptions"])
    nonterminal = ",".join(sql_text(value) for value in NONTERMINAL_ORDER_STATUSES)
    sql = f"""
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL SERIALIZABLE;
SET LOCAL lock_timeout='10s';
SET LOCAL statement_timeout='180s';
SET CONSTRAINTS ALL DEFERRED;

CREATE TEMP TABLE adoption_expected_issues ON COMMIT DROP AS
SELECT * FROM jsonb_to_recordset({_json_sql(classified_issues)}) AS x(
  issue_id text,run_id text,fingerprint text,execution_account_id text,
  issue_type text,resolution text,status text,order_id text,venue_order_id text,
  venue_trade_id text,market_id text,condition_id text,token_id text,
  local_value text,remote_value text,source text,details text,observed_at text,
  classification text
);
CREATE UNIQUE INDEX adoption_expected_issues_id_uidx
  ON adoption_expected_issues(issue_id);

DO $$ BEGIN
  IF (SELECT count(*) FROM adoption_expected_issues)<>{expected_issue_count} THEN
    RAISE EXCEPTION 'captured expected issue count changed';
  END IF;
  IF EXISTS (
    SELECT 1 FROM reconciliation_issues issue
     WHERE issue.execution_account_id IN ('main','wallet-1','wallet-2','wallet-3')
       AND issue.status='OPEN'
       AND NOT EXISTS (
         SELECT 1 FROM adoption_expected_issues expected
          WHERE expected.issue_id=issue.issue_id
            AND expected.run_id=issue.run_id
            AND expected.fingerprint=issue.fingerprint
            AND expected.execution_account_id=issue.execution_account_id
            AND expected.issue_type=issue.issue_type
            AND expected.resolution=issue.resolution
            AND expected.status=issue.status
            AND expected.order_id=issue.order_id
            AND expected.venue_order_id=issue.venue_order_id
            AND expected.venue_trade_id=issue.venue_trade_id
            AND expected.market_id=issue.market_id
            AND expected.condition_id=issue.condition_id
            AND expected.token_id=issue.token_id
            AND expected.local_value IS NOT DISTINCT FROM issue.local_value::text
            AND expected.remote_value IS NOT DISTINCT FROM issue.remote_value::text
            AND expected.source=issue.source AND expected.details=issue.details
            AND expected.observed_at::timestamptz=issue.observed_at
       )
  ) OR EXISTS (
    SELECT 1 FROM adoption_expected_issues expected
     WHERE NOT EXISTS (
       SELECT 1 FROM reconciliation_issues issue
        WHERE issue.issue_id=expected.issue_id AND issue.status='OPEN'
          AND issue.run_id=expected.run_id AND issue.fingerprint=expected.fingerprint
     )
  ) THEN
    RAISE EXCEPTION 'OPEN issue set changed after evidence sealing';
  END IF;
  IF EXISTS (SELECT 1 FROM execution_external_position_adjustment_batches
              WHERE adjustment_batch_id={sql_text(batch_id)}) THEN
    RAISE EXCEPTION 'adjustment batch already exists';
  END IF;
  IF (SELECT kill_switch FROM execution_risk_global_control WHERE singleton=TRUE)
       IS DISTINCT FROM TRUE THEN
    RAISE EXCEPTION 'global kill switch is open';
  END IF;
  IF EXISTS (SELECT 1 FROM execution_orders WHERE status IN ({nonterminal}))
     OR EXISTS (SELECT 1 FROM asset_reservations
                 WHERE status IN ('ACTIVE','RECONCILIATION_REQUIRED'))
     OR EXISTS (SELECT 1 FROM strategy_order_intent_deliveries
                 WHERE status IN ('PENDING','SUBMITTING'))
     OR EXISTS (SELECT 1 FROM reconciliation_runs
                 WHERE execution_account_id IN ('main','wallet-1','wallet-2','wallet-3')
                   AND status='RUNNING') THEN
    RAISE EXCEPTION 'offline adjustment precondition changed';
  END IF;
END $$;

INSERT INTO execution_external_position_dispositions(
  disposition_id,adjustment_batch_id,baseline_id,execution_account_id,
  condition_id,token_id,disposition_kind,transition_sequence,
  shares_before,shares_delta,shares_after,venue_trade_id,venue_order_id,
  transaction_hash,occurred_at,evidence,actor,reason
)
SELECT disposition_id,adjustment_batch_id,baseline_id,execution_account_id,
       condition_id,token_id,disposition_kind,transition_sequence,
       shares_before,shares_delta,shares_after,venue_trade_id,venue_order_id,
       transaction_hash,occurred_at,evidence,actor,reason
FROM jsonb_to_recordset({_json_sql(rows['dispositions'])}) AS x(
  disposition_id text,adjustment_batch_id text,baseline_id text,
  execution_account_id text,condition_id text,token_id text,
  disposition_kind text,transition_sequence integer,shares_before numeric,
  shares_delta numeric,shares_after numeric,venue_trade_id text,
  venue_order_id text,transaction_hash text,occurred_at timestamptz,
  evidence jsonb,actor text,reason text
);

INSERT INTO execution_external_cash_adjustments(
  cash_adjustment_id,adjustment_batch_id,execution_account_id,transaction_hash,
  asset,total_delta,available_delta,reserved_delta,balance_before,balance_after,
  account_event_id,occurred_at,evidence,actor,reason
)
SELECT cash_adjustment_id,adjustment_batch_id,execution_account_id,transaction_hash,
       asset,total_delta,available_delta,reserved_delta,balance_before,balance_after,
       account_event_id,occurred_at,evidence,actor,reason
FROM jsonb_to_recordset({_json_sql(rows['cash_adjustments'])}) AS x(
  cash_adjustment_id text,adjustment_batch_id text,execution_account_id text,
  transaction_hash text,asset text,total_delta numeric,available_delta numeric,
  reserved_delta numeric,balance_before numeric,balance_after numeric,
  account_event_id text,occurred_at timestamptz,evidence jsonb,actor text,reason text
);

UPDATE execution_accounts
   SET total_balance={sql_numeric(rows['main_balance_after'])},
       available_balance={sql_numeric(rows['main_balance_after'])},
       version=version+1,updated_at=clock_timestamp()
 WHERE execution_account_id='main'
   AND total_balance={sql_numeric(main_account['total_balance'])}
   AND available_balance={sql_numeric(main_account['available_balance'])}
   AND reserved_balance=0 AND version={int(main_account['version'])}
   AND reconciled_at IS NOT DISTINCT FROM {sql_text(main_reconciled_at)}::timestamptz;
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM execution_accounts
     WHERE execution_account_id='main'
       AND total_balance={sql_numeric(rows['main_balance_after'])}
       AND available_balance={sql_numeric(rows['main_balance_after'])}
       AND reserved_balance=0 AND version={int(main_account['version']) + 1}
       AND reconciled_at IS NOT DISTINCT FROM {sql_text(main_reconciled_at)}::timestamptz
  ) THEN RAISE EXCEPTION 'main account balance/version changed'; END IF;
END $$;

INSERT INTO execution_account_events(
  account_event_id,execution_account_id,event_type,order_id,fill_key,
  total_balance_delta,available_balance_delta,reserved_balance_delta,
  total_balance_after,available_balance_after,reserved_balance_after,occurred_at
)
SELECT account_event_id,execution_account_id,event_type,order_id,fill_key,
       total_balance_delta,available_balance_delta,reserved_balance_delta,
       total_balance_after,available_balance_after,reserved_balance_after,occurred_at
FROM jsonb_to_recordset({_json_sql(rows['account_events'])}) AS x(
  account_event_id text,execution_account_id text,event_type text,order_id text,
  fill_key text,total_balance_delta numeric,available_balance_delta numeric,
  reserved_balance_delta numeric,total_balance_after numeric,
  available_balance_after numeric,reserved_balance_after numeric,occurred_at timestamptz
);

INSERT INTO execution_external_position_adoptions(
  external_adoption_id,adjustment_batch_id,disposition_id,baseline_id,
  execution_account_id,lot_id,position_event_id,market_id,condition_id,token_id,
  outcome_index,outcome_name,neg_risk,model_id,strategy_id,shares,remaining_cost,
  average_entry_price,opened_at,adopted_at,evidence,actor,reason
)
SELECT external_adoption_id,adjustment_batch_id,disposition_id,baseline_id,
       execution_account_id,lot_id,position_event_id,market_id,condition_id,token_id,
       outcome_index,outcome_name,neg_risk,model_id,strategy_id,shares,remaining_cost,
       average_entry_price,opened_at,adopted_at,evidence,actor,reason
FROM jsonb_to_recordset({_json_sql(rows['adoptions'])}) AS x(
  external_adoption_id text,adjustment_batch_id text,disposition_id text,baseline_id text,
  execution_account_id text,lot_id text,position_event_id text,market_id text,
  condition_id text,token_id text,outcome_index smallint,outcome_name text,
  neg_risk boolean,model_id text,strategy_id text,shares numeric,remaining_cost numeric,
  average_entry_price numeric,opened_at timestamptz,adopted_at timestamptz,
  evidence jsonb,actor text,reason text
);

INSERT INTO execution_positions(
  execution_account_id,market_id,condition_id,token_id,outcome_index,outcome_name,
  total_shares,available_shares,reserved_shares,cost_basis,average_cost_price,
  realized_pnl,mark_price,market_value,unrealized_pnl,is_dust,lifecycle_status,
  reconciled_at,last_marked_at,created_at,updated_at
)
SELECT execution_account_id,market_id,condition_id,token_id,outcome_index,outcome_name,
       total_shares,available_shares,reserved_shares,cost_basis,
       {sql_derived_average('cost_basis', 'total_shares')},
       realized_pnl,mark_price,market_value,unrealized_pnl,is_dust,lifecycle_status,
       reconciled_at,reconciled_at,reconciled_at,reconciled_at
FROM jsonb_to_recordset({_json_sql(rows['positions'])}) AS x(
  execution_account_id text,market_id text,condition_id text,token_id text,
  outcome_index smallint,outcome_name text,total_shares numeric,available_shares numeric,
  reserved_shares numeric,cost_basis numeric,
  realized_pnl numeric,mark_price numeric,market_value numeric,unrealized_pnl numeric,
  is_dust boolean,lifecycle_status text,reconciled_at timestamptz
);

INSERT INTO position_lots(
  lot_id,execution_account_id,market_id,condition_id,token_id,outcome_index,
  outcome_name,neg_risk,model_id,strategy_id,opening_order_id,opening_fill_key,
  external_adoption_id,original_shares,remaining_shares,original_cost,
  remaining_cost,average_entry_price,status,opened_at
)
SELECT lot_id,execution_account_id,market_id,condition_id,token_id,outcome_index,
       outcome_name,neg_risk,model_id,strategy_id,opening_order_id,opening_fill_key,
       external_adoption_id,original_shares,remaining_shares,original_cost,
       remaining_cost,average_entry_price,status,opened_at
FROM jsonb_to_recordset({_json_sql(rows['lots'])}) AS x(
  lot_id text,execution_account_id text,market_id text,condition_id text,token_id text,
  outcome_index smallint,outcome_name text,neg_risk boolean,model_id text,strategy_id text,
  opening_order_id text,opening_fill_key text,external_adoption_id text,
  original_shares numeric,remaining_shares numeric,original_cost numeric,
  remaining_cost numeric,average_entry_price numeric,status text,opened_at timestamptz
);

INSERT INTO position_events(
  position_event_id,event_type,execution_account_id,market_id,token_id,order_id,
  fill_key,external_adoption_id,model_id,strategy_id,shares_delta,cash_delta,
  cost_basis_delta,realized_pnl_delta,shares_after,cost_basis_after,
  average_cost_after,realized_pnl_after,mark_price,unrealized_pnl_after,occurred_at
)
SELECT position_event_id,event_type,execution_account_id,market_id,token_id,order_id,
       fill_key,external_adoption_id,model_id,strategy_id,shares_delta,cash_delta,
       cost_basis_delta,realized_pnl_delta,shares_after,cost_basis_after,
       {sql_derived_average('cost_basis_after', 'shares_after')},
       realized_pnl_after,mark_price,unrealized_pnl_after,occurred_at
FROM jsonb_to_recordset({_json_sql(rows['position_events'])}) AS x(
  position_event_id text,event_type text,execution_account_id text,market_id text,
  token_id text,order_id text,fill_key text,external_adoption_id text,model_id text,
  strategy_id text,shares_delta numeric,cash_delta numeric,cost_basis_delta numeric,
  realized_pnl_delta numeric,shares_after numeric,cost_basis_after numeric,
  realized_pnl_after numeric,mark_price numeric,
  unrealized_pnl_after numeric,occurred_at timestamptz
);

UPDATE reconciliation_issues issue
   SET status='RESOLVED',resolved_at=clock_timestamp(),
       details=issue.details||'; exact external-position adjustment batch='
         ||{sql_text(batch_id)}||'; manifest_sha256='||{sql_text(manifest_sha256)}
  FROM adoption_expected_issues expected
 WHERE issue.issue_id=expected.issue_id AND issue.run_id=expected.run_id
   AND issue.fingerprint=expected.fingerprint
   AND issue.execution_account_id=expected.execution_account_id
   AND issue.issue_type=expected.issue_type AND issue.resolution=expected.resolution
   AND issue.status='OPEN' AND issue.order_id=expected.order_id
   AND issue.venue_order_id=expected.venue_order_id
   AND issue.venue_trade_id=expected.venue_trade_id
   AND issue.market_id=expected.market_id AND issue.condition_id=expected.condition_id
   AND issue.token_id=expected.token_id
   AND issue.local_value::text IS NOT DISTINCT FROM expected.local_value
   AND issue.remote_value::text IS NOT DISTINCT FROM expected.remote_value
   AND issue.source=expected.source AND issue.details=expected.details
   AND issue.observed_at=expected.observed_at::timestamptz;
DO $$ BEGIN
  IF (SELECT count(*) FROM adoption_expected_issues expected
       JOIN reconciliation_issues issue USING(issue_id)
      WHERE issue.status='RESOLVED' AND issue.resolved_at IS NOT NULL
        AND issue.details LIKE '%manifest_sha256={manifest_sha256}')<>{expected_issue_count} THEN
    RAISE EXCEPTION 'exact issue resolution count changed';
  END IF;
END $$;

INSERT INTO execution_external_position_adjustment_batches(
  adjustment_batch_id,schema_version,observed_at,evidence,evidence_sha256,actor,reason
) VALUES (
  {sql_text(batch_id)},'trading.external-position-adjustment.v1',
  {sql_text(rows['observed_at'])}::timestamptz,{_json_sql(manifest)},
  {sql_text(manifest_sha256)},{sql_text(ACTOR)},
  'finalized external SELL disposition and exact managed position adoption'
);

SET CONSTRAINTS ALL IMMEDIATE;
DO $$ BEGIN
  IF (SELECT count(*) FROM execution_external_position_dispositions
       WHERE adjustment_batch_id={sql_text(batch_id)})<>{expected_dispositions}
     OR (SELECT count(*) FROM execution_external_cash_adjustments
          WHERE adjustment_batch_id={sql_text(batch_id)})<>{expected_cash}
     OR (SELECT count(*) FROM execution_external_position_adoptions
          WHERE adjustment_batch_id={sql_text(batch_id)})<>{expected_adoptions} THEN
    RAISE EXCEPTION 'sealed adjustment child row count mismatch';
  END IF;
  IF EXISTS (SELECT 1 FROM reconciliation_issues
              WHERE execution_account_id IN ('main','wallet-1','wallet-2','wallet-3')
                AND status='OPEN') THEN
    RAISE EXCEPTION 'OPEN reconciliation issue remains after exact resolution';
  END IF;
  IF (SELECT count(*) FROM execution_positions
       WHERE total_shares<>0 OR reserved_shares<>0 OR cost_basis<>0)<>5
     OR (SELECT count(*) FROM position_lots WHERE status='OPEN')<>5
     OR (SELECT count(*) FROM execution_external_position_dispositions
          WHERE disposition_kind='ADOPTION' AND shares_after=0
            AND adjustment_batch_id={sql_text(batch_id)})<>4 THEN
    RAISE EXCEPTION 'managed position/lot/adoption set is not exactly five/five/four';
  END IF;
  IF EXISTS (SELECT 1 FROM execution_orders WHERE status IN ({nonterminal}))
     OR EXISTS (SELECT 1 FROM asset_reservations
                 WHERE status IN ('ACTIVE','RECONCILIATION_REQUIRED'))
     OR EXISTS (SELECT 1 FROM strategy_order_intent_deliveries
                 WHERE status IN ('PENDING','SUBMITTING'))
     OR (SELECT kill_switch FROM execution_risk_global_control WHERE singleton=TRUE)
          IS DISTINCT FROM TRUE THEN
    RAISE EXCEPTION 'closed-gate post-adjustment invariant failed';
  END IF;
END $$;
COMMIT;
"""
    require_preserved_main_boundary_sql(sql, main_reconciled_at)
    require_database_derived_average_sql(sql)
    return sql


def execute_adjustment_transaction(
    migration: MigrationSession,
    backup: RuntimeBackup,
    manifest: dict[str, object],
    rows: dict[str, object],
    classified_issues: list[dict[str, object]],
    database_state: dict[str, object],
) -> dict[str, object]:
    sql = adjustment_transaction_sql(
        manifest=manifest,
        rows=rows,
        classified_issues=classified_issues,
        database_state=database_state,
    )
    path = backup.directory / "position-adoption-transaction.sql"
    deploy.atomic_write(path, sql, mode=0o600)
    migration_execute(migration, ["-v", "ON_ERROR_STOP=1", "-f", str(path)])
    return {
        "adjustment_batch_id": rows["adjustment_batch_id"],
        "manifest_sha256": manifest["manifest_sha256"],
        "disposition_count": len(rows["dispositions"]),
        "cash_adjustment_count": len(rows["cash_adjustments"]),
        "adoption_count": len(rows["adoptions"]),
        "transaction_sql_sha256": deploy.sha256(path),
    }


def wait_for_candidate_health(expected_commit: str, timeout: int = 300) -> dict[str, object]:
    status, payload = deploy.wait_http(HEALTH_URL, {200}, timeout)
    try:
        health = json.loads(payload)
    except json.JSONDecodeError as error:
        raise CutoverError("candidate liveness returned invalid JSON") from error
    if (
        status != 200
        or health.get("status") != "ok"
        or health.get("commit") != expected_commit
    ):
        raise CutoverError("candidate liveness identity mismatch")
    ready_status, ready_payload = deploy.wait_http(READY_URL, {200}, timeout)
    try:
        ready = json.loads(ready_payload)
    except json.JSONDecodeError as error:
        raise CutoverError("candidate readiness returned invalid JSON") from error
    if (
        ready_status != 200
        or ready.get("status") != "ready"
        or ready.get("commit") != expected_commit
    ):
        raise CutoverError("candidate did not become ready after adoption")
    return {"live": health, "ready": ready}


def require_executable_identity(
    expected_binary: pathlib.Path,
    observed_executable: pathlib.Path,
    expected_sha256: str,
) -> None:
    deploy.require_regular(expected_binary, expected_sha256)
    expected_real = expected_binary.resolve(strict=True)
    observed_real = observed_executable.resolve(strict=True)
    if observed_real != expected_real:
        raise CutoverError("running executable realpath is not the candidate release binary")
    expected_stat = expected_real.stat()
    observed_stat = observed_executable.stat()
    if (observed_stat.st_dev, observed_stat.st_ino) != (
        expected_stat.st_dev,
        expected_stat.st_ino,
    ):
        raise CutoverError("running executable inode is not the candidate release binary")
    if deploy.sha256(observed_real) != expected_sha256:
        raise CutoverError("running executable SHA256 is not the pinned candidate binary")


def verify_running_candidate(release: pathlib.Path, expected_sha256: str) -> None:
    if CURRENT.resolve(strict=True) != release:
        raise CutoverError("current release symlink does not select the candidate")
    pid = deploy.main_pid(SERVICE)
    require_executable_identity(
        release / "trading-execution",
        pathlib.Path(f"/proc/{pid}/exe"),
        expected_sha256,
    )
    running = deploy.process_environment_for_pid(pid)
    if running.get("POLYMARKET_ACCOUNTS_FILE") != str(WALLET_FILE):
        raise CutoverError("candidate process wallet inventory path changed")
    if running.get("DECISION_CYCLE_ORDER_SUBMISSION_ENABLED") != "false":
        raise CutoverError("candidate process submission gate is not closed")
    if running.get("DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED") != "true":
        raise CutoverError("candidate process entry submission gate is not closed")
    try:
        bindings = json.loads(running.get("DECISION_CYCLE_BINDINGS_JSON", "[]"))
    except json.JSONDecodeError as error:
        raise CutoverError("candidate process binding JSON is invalid") from error
    if bindings != BINDINGS:
        raise CutoverError("candidate process binding JSON changed")


def verify_post_adoption_reconciliation(
    database: DatabaseSession, not_before: dt.datetime
) -> dict[str, object]:
    result = db_json(
        database,
        f"""
SELECT json_build_object(
  'runs',(SELECT COALESCE(json_agg(row_to_json(x) ORDER BY execution_account_id),'[]'::json)
    FROM (SELECT DISTINCT ON (execution_account_id)
                 execution_account_id,run_id,trigger,status,summary,error,started_at,completed_at
            FROM reconciliation_runs
           WHERE execution_account_id IN ('main','wallet-1','wallet-2','wallet-3')
           ORDER BY execution_account_id,started_at DESC,run_id DESC) x),
  'open_issues',(SELECT count(*) FROM reconciliation_issues
    WHERE execution_account_id IN ('main','wallet-1','wallet-2','wallet-3') AND status='OPEN'),
  'kill_switch',(SELECT kill_switch FROM execution_risk_global_control WHERE singleton=TRUE),
  'nonterminal_orders',(SELECT count(*) FROM execution_orders
    WHERE status IN ({','.join(sql_text(value) for value in NONTERMINAL_ORDER_STATUSES)})),
  'active_reservations',(SELECT count(*) FROM asset_reservations
    WHERE status IN ('ACTIVE','RECONCILIATION_REQUIRED')),
  'pending_deliveries',(SELECT count(*) FROM strategy_order_intent_deliveries
    WHERE status IN ('PENDING','SUBMITTING'))
);
""",
    )
    if not isinstance(result, dict) or not isinstance(result.get("runs"), list):
        raise CutoverError("post-adoption reconciliation query is invalid")
    runs = result["runs"]
    indexed = {str(item.get("execution_account_id")): item for item in runs}
    if len(runs) != 4 or set(indexed) != set(ACCOUNT_ADDRESSES):
        raise CutoverError("post-adoption reconciliation set is not four accounts")
    for account_id, run in indexed.items():
        if (
            run.get("status") != "COMPLETED"
            or run.get("completed_at") is None
            or parse_rfc3339(str(run["started_at"])) < not_before - dt.timedelta(seconds=2)
        ):
            raise CutoverError(f"post-adoption reconciliation is not fresh/clean for {account_id}")
    for key, expected in {
        "open_issues": 0,
        "kill_switch": True,
        "nonterminal_orders": 0,
        "active_reservations": 0,
        "pending_deliveries": 0,
    }.items():
        if result.get(key) != expected:
            raise CutoverError(f"post-adoption invariant failed: {key}")
    return result


def database_open_lots(database: DatabaseSession) -> dict[str, list[dict[str, object]]]:
    value = db_json(
        database,
        r"""
SELECT COALESCE(json_agg(row_to_json(x) ORDER BY execution_account_id,opened_at,lot_id),'[]'::json)
FROM (
  SELECT lot.lot_id,lot.execution_account_id,lot.market_id,lot.condition_id,
         lot.outcome_index,lot.outcome_name,lot.token_id,lot.neg_risk,
         COALESCE(route.logical_model_id,lot.model_id) model_id,lot.strategy_id,
         lot.opened_at AS entered_at,lot.remaining_shares::text AS shares,
         lot.average_entry_price::text AS entry_price
    FROM position_lots lot
    LEFT JOIN position_lot_model_routes route ON route.lot_id=lot.lot_id
   WHERE lot.execution_account_id IN ('main','wallet-1','wallet-2','wallet-3')
     AND lot.status='OPEN'
) x;
""",
    )
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise CutoverError("open lot verification query is invalid")
    result = {account_id: [] for account_id in ACCOUNT_ADDRESSES}
    for item in value:
        account_id = str(item.get("execution_account_id", ""))
        if account_id not in result:
            raise CutoverError("open lot belongs to an unexpected execution account")
        result[account_id].append(item)
    if {key: len(value) for key, value in result.items()} != EXPECTED_PYTHON_POSITION_COUNTS:
        raise CutoverError("post-adoption open lot counts are not 1/1/1/2")
    return result


def normalize_python_positions(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise CutoverError("Python strategy positions are not an object array")
    fields = (
        "lot_id",
        "market_id",
        "condition_id",
        "outcome_index",
        "outcome_name",
        "token_id",
        "neg_risk",
        "entered_at",
        "shares",
        "entry_price",
    )
    return sorted(
        [{field: item.get(field) for field in fields} for item in value],
        key=lambda item: str(item["lot_id"]),
    )


def expected_python_positions(
    lots: dict[str, list[dict[str, object]]]
) -> dict[str, list[dict[str, object]]]:
    result: dict[str, list[dict[str, object]]] = {}
    for account_id, values in lots.items():
        items = []
        for lot in values:
            items.append(
                {
                    "lot_id": lot["lot_id"],
                    "market_id": lot["market_id"],
                    "condition_id": lot["condition_id"],
                    "outcome_index": lot["outcome_index"],
                    "outcome_name": lot["outcome_name"],
                    "token_id": lot["token_id"],
                    "neg_risk": lot["neg_risk"],
                    "entered_at": rfc3339(parse_rfc3339(str(lot["entered_at"]))),
                    "shares": decimal_text(lot["shares"]),
                    "entry_price": decimal_text(lot["entry_price"]),
                }
            )
        result[account_id] = sorted(items, key=lambda item: str(item["lot_id"]))
    return result


def wait_for_python_inputs(
    database: DatabaseSession, not_before: dt.datetime, timeout: int
) -> dict[str, object]:
    expected = expected_python_positions(database_open_lots(database))
    deadline = time.monotonic() + timeout
    last: object = None
    while time.monotonic() < deadline:
        last = db_json(
            database,
            f"""
SELECT COALESCE(json_agg(row_to_json(x) ORDER BY execution_account_id),'[]'::json)
FROM (
  SELECT DISTINCT ON (execution_account_id)
         execution_account_id,model_id,strategy_id,decision_at,created_at,
         order_submission_enabled,input_payload,output_payload
    FROM strategy_decision_runs
   WHERE execution_account_id IN ('main','wallet-1','wallet-2','wallet-3')
     AND created_at>={sql_text(rfc3339(not_before))}::timestamptz
   ORDER BY execution_account_id,decision_at DESC,created_at DESC
) x;
""",
        )
        if isinstance(last, list) and len(last) == 4:
            indexed = {str(item.get("execution_account_id", "")): item for item in last}
            if set(indexed) == set(ACCOUNT_ADDRESSES):
                valid = True
                for binding in BINDINGS:
                    account_id = binding["execution_account_id"]
                    row = indexed[account_id]
                    payload = row.get("input_payload")
                    output = row.get("output_payload")
                    if (
                        row.get("model_id") != binding["model_id"]
                        or row.get("strategy_id") != binding["strategy_id"]
                        or row.get("order_submission_enabled") is not False
                        or not isinstance(payload, dict)
                        or not isinstance(output, dict)
                        or normalize_python_positions(payload.get("positions")) != expected[account_id]
                        or output.get("entry_policy", {}).get("enabled") is not False
                        or output.get("entry_policy", {}).get("block_reason") != "ENTRY_SUBMISSION_DISABLED"
                    ):
                        valid = False
                        break
                if valid:
                    return {
                        "runs": last,
                        "position_counts": {
                            key: len(value) for key, value in expected.items()
                        },
                        "positions": expected,
                    }
        time.sleep(5)
    raise CutoverError(f"fresh four-binding Python inputs did not converge: {last}")


def restore_before_irreversible(
    backup: RuntimeBackup,
    database: DatabaseSession,
    expected_previous_commit: str,
) -> None:
    log("ROLLBACK=START boundary=pre-adoption")
    try:
        close_mutable_gates(database, "POSITION_ADOPTION_PRECOMMIT_ROLLBACK")
    except Exception:
        log("ROLLBACK_WARNING=database_gate_close_failed")
    try:
        deploy.run(["systemctl", "stop", SERVICE], check=False)
        deploy.atomic_write(ENV, backup.env_text, mode=0o600)
        deploy.update_environment_file(
            ENV,
            {"DECISION_CYCLE_ORDER_SUBMISSION_ENABLED": "false"},
            set(),
        )
        deploy.atomic_symlink(backup.previous_release, CURRENT)
        start_service()
        current_health(expected_previous_commit)
        log("ROLLBACK=COMPLETE submission=false kill_switch=true")
    except Exception:
        log("ROLLBACK_WARNING=previous_runtime_restore_failed")


def fix_forward_failure(
    database: DatabaseSession,
    release: pathlib.Path | None = None,
) -> None:
    try:
        close_mutable_gates(database, "POSITION_ADOPTION_FIX_FORWARD_REQUIRED")
    except Exception:
        log("FIX_FORWARD_WARNING=database_gate_close_failed")
    if release is not None:
        try:
            deploy.atomic_symlink(release, CURRENT)
            configure_candidate_environment()
        except Exception:
            log("FIX_FORWARD_WARNING=candidate_selection_failed")
    active = deploy.run(
        ["systemctl", "is-active", SERVICE], capture=True, check=False
    ).stdout.strip() == "active"
    if not active and release is not None:
        try:
            start_service()
            active = True
        except Exception:
            log("FIX_FORWARD_WARNING=candidate_start_failed")
    running = deploy.process_environment(SERVICE) if active else {}
    if running and running.get("DECISION_CYCLE_ORDER_SUBMISSION_ENABLED") != "false":
        deploy.run(["systemctl", "stop", SERVICE], check=False)
    log("FIX_FORWARD_REQUIRED=true old_binary_rollback_forbidden=true")


def self_test() -> None:
    if keccak_256(b"").hex() != (
        "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
    ):
        raise CutoverError("Keccak-256 self-test failed")
    if ethereum_address("0x" + "0" * 63 + "1") != (
        "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"
    ):
        raise CutoverError("secp256k1 address self-test failed")
    sample = {
        "trades": [
            {"side": "BUY", "shares": "7", "match_time": "2026-08-18T00:00:00Z"},
            {"side": "BUY", "shares": "5", "match_time": "2026-08-19T00:00:00Z"},
            {"side": "SELL", "shares": "8", "match_time": "2026-08-20T00:00:00Z"},
        ]
    }
    rebuilt = reconstruct_fifo(sample, "4")
    if rebuilt["entered_at"] != "2026-08-19T00:00:00Z" or len(
        rebuilt["residual_acquisitions"]
    ) != 1:
        raise CutoverError("FIFO self-test failed")
    multi_residual = reconstruct_fifo(
        {
            "trades": [
                {"side": "BUY", "shares": "7", "match_time": "2026-08-18T00:00:00Z"},
                {"side": "BUY", "shares": "5", "match_time": "2026-08-19T00:00:00Z"},
                {"side": "SELL", "shares": "2", "match_time": "2026-08-20T00:00:00Z"},
            ]
        },
        "10",
    )
    if (
        multi_residual["entered_at"] != "2026-08-19T00:00:00Z"
        or len(multi_residual["residual_acquisitions"]) != 2
    ):
        raise CutoverError("multi-acquisition FIFO hold-clock self-test failed")
    stable_item = {
        "token_id": "1",
        "condition_id": "0x" + "1" * 64,
        "outcome_index": 0,
        "outcome_name": "Yes",
        "neg_risk": False,
        "shares": "3",
        "avg_price": "0.3",
        "current_price": "0.4",
        "redeemable": False,
    }
    stable_snapshot = {"canonical_items": [stable_item]}
    mark_changed = {"canonical_items": [{**stable_item, "current_price": "0.9"}]}
    shares_changed = {"canonical_items": [{**stable_item, "shares": "2.99"}]}
    if position_stability_projection(stable_snapshot) != position_stability_projection(
        mark_changed
    ) or position_stability_projection(stable_snapshot) == position_stability_projection(
        shares_changed
    ):
        raise CutoverError("Data API stability projection self-test failed")

    condition = "0x" + "1" * 64
    transaction_hash = "0x" + "2" * 64
    owned_order_a = "0x" + "3" * 64
    owned_order_b = "0x" + "4" * 64
    foreign_order = "0x" + "5" * 64
    maker_trade = {
        "id": "Trade-A",
        "market": condition,
        "asset_id": "999",
        "status": "CONFIRMED",
        "transaction_hash": transaction_hash,
        "trader_side": "MAKER",
        "maker_address": ACCOUNT_ADDRESSES["wallet-1"],
        "match_time": "2026-08-18T00:00:00Z",
        "last_update": "2026-08-18T00:00:01Z",
        "maker_orders": [
            {
                "maker_address": ACCOUNT_ADDRESSES["main"],
                "order_id": owned_order_a,
                "asset_id": "111",
                "side": "BUY",
                "matched_amount": "7",
                "price": "0.2",
            },
            {
                "maker_address": ACCOUNT_ADDRESSES["main"],
                "order_id": owned_order_b,
                "asset_id": "222",
                "side": "BUY",
                "matched_amount": "5",
                "price": "0.3",
            },
            {
                "maker_address": ACCOUNT_ADDRESSES["wallet-1"],
                "order_id": foreign_order,
                "asset_id": "333",
                "side": "SELL",
                "matched_amount": "12",
                "price": "0.7",
            },
        ],
    }
    owned_components = normalize_owned_account_trade_components(maker_trade, "main")
    if (
        len(owned_components) != 2
        or {item["token_id"] for item in owned_components} != {"111", "222"}
        or any(item["token_id"] in {"999", "333"} for item in owned_components)
    ):
        raise CutoverError("multi-component MAKER ownership self-test failed")
    component_index = {
        "execution_account_id": "main",
        "maker_address_filter": ACCOUNT_ADDRESSES["main"],
        "canonical_sha256": sha256_bytes(canonical_json(owned_components)),
        "trade_components": owned_components,
        "trade_index": {"trade-a": owned_components},
    }
    exact_issue = {
        "venue_trade_id": "trade-a",
        "venue_order_id": owned_order_b,
        "condition_id": condition,
        "token_id": "222",
    }
    if exact_issue_trade_component(exact_issue, component_index) != owned_components[1]:
        raise CutoverError("exact owned CLOB component self-test failed")
    if exact_issue_trade_component({**exact_issue, "token_id": "999"}, component_index):
        raise CutoverError("top-level MAKER asset was incorrectly attributed")
    maker_token_history = clob_history_for_token(
        component_index, "main", condition, "222"
    )
    if (
        maker_token_history["trades"][0]["order_hash"] != owned_order_b
        or reconstruct_fifo(maker_token_history, "5")["shares"] != "5"
    ):
        raise CutoverError("MAKER component FIFO self-test failed")
    foreign_trade = {**maker_trade, "maker_orders": [maker_trade["maker_orders"][2]]}
    try:
        normalize_owned_account_trade_components(foreign_trade, "main")
    except CutoverError:
        pass
    else:
        raise CutoverError("foreign CLOB component self-test failed open")
    taker_order = "0x" + "6" * 64
    taker_trade = {
        **maker_trade,
        "id": "Trade-Taker",
        "trader_side": "TAKER",
        "maker_address": ACCOUNT_ADDRESSES["main"],
        "taker_order_id": taker_order,
        "asset_id": "444",
        "side": "SELL",
        "size": "2",
        "price": "0.4",
        # Go semantics retain only the top-level taker component even if a
        # nested self-maker happens to be present.
        "maker_orders": [maker_trade["maker_orders"][0]],
    }
    taker_components = normalize_owned_account_trade_components(taker_trade, "main")
    if (
        len(taker_components) != 1
        or taker_components[0]["venue_order_id"] != taker_order
        or taker_components[0]["token_id"] != "444"
    ):
        raise CutoverError("TAKER top-level ownership self-test failed")

    if decimal_text(Decimal("1.23000")) != "1.23":
        raise CutoverError("decimal canonicalization self-test failed")
    repeating_expression = sql_derived_average(sql_numeric("1"), sql_numeric("3"))
    if repeating_expression != "('1'::numeric)/NULLIF(('3'::numeric),0)":
        raise CutoverError("PostgreSQL-derived repeating average self-test failed")
    derived_sql = "\n".join(
        (
            sql_derived_average("cost_basis", "total_shares"),
            sql_derived_average("cost_basis_after", "shares_after"),
        )
    )
    require_database_derived_average_sql(derived_sql)
    try:
        require_database_derived_average_sql(
            derived_sql + "\ncost_basis numeric,average_cost_price numeric"
        )
    except CutoverError:
        pass
    else:
        raise CutoverError("Python-computed average SQL self-test failed open")
    first = stable_id("test", "a", 1)
    if first != stable_id("test", "a", 1) or first == stable_id("test", "a", 2):
        raise CutoverError("stable ID self-test failed")
    if require_equal_sha256(b"same", b"same", "self-test") != sha256_bytes(b"same"):
        raise CutoverError("script SHA equality self-test failed")
    try:
        require_equal_sha256(b"committed", b"modified", "self-test mismatch")
    except CutoverError:
        pass
    else:
        raise CutoverError("script SHA mismatch self-test failed open")
    empty_components_sha = sha256_bytes(canonical_json([]))
    stable_indexes = {
        account_id: {
            "execution_account_id": account_id,
            "maker_address_filter": ACCOUNT_ADDRESSES[account_id],
            "canonical_sha256": empty_components_sha,
            "trade_components": [],
        }
        for account_id in ACCOUNT_ADDRESSES
    }
    copied_indexes = json.loads(json.dumps(stable_indexes))
    require_account_trade_indexes_unchanged(stable_indexes, copied_indexes)
    copied_indexes["wallet-3"]["canonical_sha256"] = "0" * 64
    try:
        require_account_trade_indexes_unchanged(stable_indexes, copied_indexes)
    except CutoverError:
        pass
    else:
        raise CutoverError("CLOB evidence stability self-test failed open")
    with tempfile.TemporaryDirectory(prefix="cutover-exe-selftest.") as temporary:
        directory = pathlib.Path(temporary)
        candidate = directory / "candidate"
        candidate.write_bytes(b"candidate-binary")
        candidate.chmod(0o755)
        observed = directory / "observed"
        observed.symlink_to(candidate)
        candidate_sha = deploy.sha256(candidate)
        require_executable_identity(candidate, observed, candidate_sha)
        other = directory / "other"
        other.write_bytes(b"other-binary")
        other.chmod(0o755)
        observed.unlink()
        observed.symlink_to(other)
        try:
            require_executable_identity(candidate, observed, candidate_sha)
        except CutoverError:
            pass
        else:
            raise CutoverError("running executable identity self-test failed open")
    batch_triggers = REQUIRED_0016_TRIGGER_NAMES[:2]
    mini_migration = "\n".join(
        f"CREATE TRIGGER {name} BEFORE INSERT ON example FOR EACH ROW EXECUTE FUNCTION f();"
        for name in batch_triggers
    )
    require_migration_trigger_names(mini_migration, batch_triggers)
    try:
        require_migration_trigger_names(mini_migration, (*batch_triggers, "missing_trigger"))
    except CutoverError:
        pass
    else:
        raise CutoverError("migration trigger-name self-test failed open")
    boundary = "2026-08-21T00:00:00Z"
    boundary_clause = (
        "reconciled_at IS NOT DISTINCT FROM "
        + sql_text(boundary)
        + "::timestamptz"
    )
    require_preserved_main_boundary_sql(
        "UPDATE execution_accounts SET total_balance=2 WHERE "
        + boundary_clause
        + "; SELECT 1 WHERE "
        + boundary_clause
        + ";",
        boundary,
    )
    try:
        require_preserved_main_boundary_sql(
            "UPDATE execution_accounts SET reconciled_at=clock_timestamp() WHERE "
            + boundary_clause
            + "; SELECT 1 WHERE "
            + boundary_clause
            + ";",
            boundary,
        )
    except CutoverError:
        pass
    else:
        raise CutoverError("ownership-boundary SQL self-test failed open")
    source = pathlib.Path(__file__).read_text(encoding="utf-8")
    if EXECUTE_TOKEN not in source:
        raise CutoverError("execute-token guard is missing")
    if not re.search(
        r"irreversible\s*=\s*True\s+transaction\s*=\s*"
        r"execute_adjustment_transaction\s*\(",
        source,
    ):
        raise CutoverError(
            "transaction outcome-unknown boundary must precede transaction execution"
        )
    main_source = source[source.index("def main() -> int:") :]
    evidence_markers = (
        "deploy.atomic_symlink(release, CURRENT)",
        "final_onchain_balances = validate_onchain_balances(",
        "final_account_trade_indexes = {",
        "require_account_trade_indexes_unchanged(\n            account_trade_indexes, final_account_trade_indexes",
        "irreversible = True",
        "transaction = execute_adjustment_transaction(",
    )
    cursor = -1
    for marker in evidence_markers:
        cursor = main_source.find(marker, cursor + 1)
        if cursor < 0:
            raise CutoverError(
                "pre-transaction evidence/PID boundary static self-test failed"
            )
    if 'pathlib.Path(f"/proc/{pid}/exe")' not in source:
        raise CutoverError("running candidate PID executable pin is missing")
    log("STATIC_SELF_TEST=PASS")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fail-closed production position-adoption staging cutover"
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--execute-token", default="")
    parser.add_argument("--candidate-source-tree", type=pathlib.Path)
    parser.add_argument("--expected-commit", default="")
    parser.add_argument("--expected-binary-sha256", default="")
    parser.add_argument("--expected-previous-commit", default=EXPECTED_PREVIOUS_COMMIT)
    parser.add_argument("--binary-relative-path", default="bin/trading-execution")
    parser.add_argument("--binding-log-timeout-seconds", type=int, default=1200)
    return parser.parse_args()


def validate_production_args(args: argparse.Namespace) -> None:
    if args.execute_token != EXECUTE_TOKEN:
        raise CutoverError(
            "exact --execute-token is required; no production reads or writes were attempted"
        )
    if os.geteuid() != 0:
        raise CutoverError("production cutover must run as root")
    if args.candidate_source_tree is None:
        raise CutoverError("--candidate-source-tree is required")
    if not re.fullmatch(r"[0-9a-f]{40}", args.expected_commit):
        raise CutoverError("--expected-commit must be a full lowercase SHA-1")
    if not re.fullmatch(r"[0-9a-f]{40}", args.expected_previous_commit):
        raise CutoverError("--expected-previous-commit must be a full lowercase SHA-1")
    if not re.fullmatch(r"[0-9a-f]{64}", args.expected_binary_sha256):
        raise CutoverError("--expected-binary-sha256 must be lowercase SHA-256")
    if pathlib.PurePosixPath(args.binary_relative_path).is_absolute() or ".." in pathlib.PurePosixPath(
        args.binary_relative_path
    ).parts:
        raise CutoverError("--binary-relative-path must stay inside candidate source tree")
    if not 30 <= args.binding_log_timeout_seconds <= 3600:
        raise CutoverError("binding log timeout must be between 30 and 3600 seconds")


def assert_snapshot_unchanged(
    data_api_url: str,
    gamma_url: str,
    expected_positions: dict[str, dict[str, object]],
    expected_gamma: dict[str, dict[str, object]],
) -> dict[str, dict[str, object]]:
    actual = capture_all_positions(data_api_url)
    actual_gamma = capture_gamma_identities(gamma_url, actual)
    for account_id in ACCOUNT_ADDRESSES:
        if position_stability_projection(
            actual[account_id]
        ) != position_stability_projection(expected_positions[account_id]):
            raise CutoverError(f"external position changed after evidence for {account_id}")
    if actual_gamma != expected_gamma:
        raise CutoverError("Gamma identity changed after evidence")
    return actual


def verify_final_closed_gates(database: DatabaseSession) -> None:
    if db_scalar(
        database,
        "SELECT kill_switch::text FROM execution_risk_global_control WHERE singleton=TRUE",
    ) != "true":
        raise CutoverError("global kill switch is not closed at final handoff")
    environment = deploy.environment_file(ENV)
    if environment.get("DECISION_CYCLE_ORDER_SUBMISSION_ENABLED") != "false":
        raise CutoverError("submission environment gate reopened")
    if environment.get("DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED") != "true":
        raise CutoverError("entry submission environment gate reopened")


def main() -> int:
    args = parse_args()
    if args.self_test:
        self_test()
        return 0
    validate_production_args(args)

    source_tree = args.candidate_source_tree.resolve(strict=True)
    binary, cutover_script = verify_candidate(args)
    trading_environment = deploy.environment_file(ENV)
    prediction_environment = deploy.environment_file(PREDICTION_ENV)
    verify_runtime_environment(trading_environment)
    current_health(args.expected_previous_commit)
    records = wallet_records()
    database = database_session(trading_environment)
    migration: MigrationSession | None = None
    backup: RuntimeBackup | None = None
    release: pathlib.Path | None = None
    irreversible = False
    try:
        initial_state = read_database_preflight(database)
        baseline_observed_at = validate_database_preflight(initial_state)
        backup = backup_runtime(ENV.read_text(encoding="utf-8"))
        release = install_release(
            binary, args.expected_commit, args.expected_binary_sha256
        )

        log("PHASE=close-gates-and-stop")
        close_mutable_gates(database, "POSITION_ADOPTION_STAGING")
        stop_service()

        data_api_url = trading_environment.get("POLYMARKET_DATA_API_URL", "").strip()
        gamma_url = trading_environment.get("POLYMARKET_GAMMA_URL", "").strip()
        clob_url = trading_environment.get("POLYMARKET_CLOB_URL", "").strip()
        if not data_api_url or not gamma_url or not clob_url:
            raise CutoverError("Data API, Gamma, and CLOB URLs are required")

        log("PHASE=dual-data-api-gamma-snapshot")
        snapshot_boundary, snapshot_a, snapshot_b, gamma = dual_venue_snapshot(
            database, data_api_url, gamma_url
        )
        trade_evidence, fifo, initial_account_trade_indexes = collect_trade_and_fifo_evidence(
            trading_environment,
            records,
            snapshot_b,
            baseline_observed_at,
        )
        assert_snapshot_unchanged(data_api_url, gamma_url, snapshot_b, gamma)
        onchain_balances = validate_onchain_balances(
            initial_state,
            trading_environment,
            str(trade_evidence["main_pusd_cash_delta"]),
        )

        log("PHASE=apply-additive-0016")
        migration = migration_session(
            database, trading_environment, prediction_environment
        )
        migration_result = apply_migration_0016(database, migration, source_tree)
        configure_candidate_environment()
        run_candidate_schema_probe(
            binary, {**trading_environment, "DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED": "true"},
            backup, args.expected_commit
        )

        log("PHASE=candidate-account-isolated-reconciliation")
        pre_reconciliation = run_candidate_pre_adoption_reconciliation(
            binary,
            {**trading_environment, "DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED": "true"},
            backup,
            database,
        )
        state = read_database_preflight(database)
        if validate_database_preflight(state) != baseline_observed_at:
            raise CutoverError("immutable baseline timestamp changed during evidence collection")
        account_trade_indexes = {
            account_id: fetch_clob_account_trade_ids(
                clob_url, records[account_id], account_id
            )
            for account_id in ACCOUNT_ADDRESSES
        }
        require_account_trade_indexes_unchanged(
            initial_account_trade_indexes, account_trade_indexes
        )
        classified_issues = classify_open_issues(
            state["open_issues"], trade_evidence, account_trade_indexes, state
        )
        sell_plan = build_sell_plan(trading_environment, fifo, state)
        adopted_at = database_clock(database)
        manifest = build_evidence_manifest(
            candidate_commit=args.expected_commit,
            candidate_binary_sha256=args.expected_binary_sha256,
            cutover_script=cutover_script,
            migration=migration_result,
            snapshot_boundary=snapshot_boundary,
            snapshot_a=snapshot_a,
            snapshot_b=snapshot_b,
            gamma=gamma,
            trade_evidence=trade_evidence,
            account_trade_indexes=account_trade_indexes,
            database_state=state,
            classified_issues=classified_issues,
            onchain_balances=onchain_balances,
            sell_plan=sell_plan,
            pre_adoption_reconciliation=pre_reconciliation,
            adopted_at=adopted_at,
        )
        evidence_path = backup.directory / "position-adoption-evidence.json"
        write_private_json(evidence_path, manifest)
        rows = build_adjustment_rows(
            manifest_sha256=str(manifest["manifest_sha256"]),
            adopted_at=adopted_at,
            database_state=state,
            snapshot=snapshot_b,
            gamma=gamma,
            trade_evidence=trade_evidence,
            fifo=fifo,
            account_trade_indexes=account_trade_indexes,
            classified_issues=classified_issues,
        )
        write_private_json(backup.directory / "position-adoption-rows.json", rows)
        close_mutable_gates(database, "POSITION_ADOPTION_TRANSACTION")
        deploy.atomic_symlink(release, CURRENT)

        # Re-read every external evidence surface only after the last local
        # pre-transaction changes.  Any wallet activity during probe/reconcile
        # therefore fails before the outcome-unknown database boundary.
        assert_snapshot_unchanged(data_api_url, gamma_url, snapshot_b, gamma)
        final_onchain_balances = validate_onchain_balances(
            state,
            trading_environment,
            str(trade_evidence["main_pusd_cash_delta"]),
        )
        if final_onchain_balances != onchain_balances:
            raise CutoverError("four-account pUSD balances changed before adoption transaction")
        final_account_trade_indexes = {
            account_id: fetch_clob_account_trade_ids(
                clob_url, records[account_id], account_id
            )
            for account_id in ACCOUNT_ADDRESSES
        }
        require_account_trade_indexes_unchanged(
            account_trade_indexes, final_account_trade_indexes
        )
        write_private_json(
            backup.directory / "position-adoption-pretransaction-stability.json",
            {
                "verified_at": rfc3339(utc_now()),
                "manifest_sha256": manifest["manifest_sha256"],
                "onchain_pusd_balances": final_onchain_balances,
                "account_component_sha256": {
                    account_id: value["canonical_sha256"]
                    for account_id, value in sorted(final_account_trade_indexes.items())
                },
            },
        )

        log("PHASE=serializable-adjustment-and-adoption")
        # From the moment psql is asked to execute the transaction, its outcome
        # may become unknowable to this process (for example, COMMIT succeeds
        # and the connection drops before psql reports success).  Candidate +
        # closed gates is therefore the only safe recovery direction even when
        # execute_adjustment_transaction raises.
        irreversible = True
        transaction = execute_adjustment_transaction(
            migration, backup, manifest, rows, classified_issues, state
        )

        # This post-commit read cannot make venue+database atomic, but it makes
        # any race visible and forces fix-forward before the service can trade.
        assert_snapshot_unchanged(data_api_url, gamma_url, snapshot_b, gamma)
        post_state = read_database_preflight(database)
        validate_onchain_balances(post_state, trading_environment, "0")

        log("PHASE=start-candidate-with-all-gates-closed")
        configure_candidate_environment()
        start_at = database_clock(database)
        start_service()
        health = wait_for_candidate_health(args.expected_commit)
        verify_running_candidate(release, args.expected_binary_sha256)
        reconciliation = verify_post_adoption_reconciliation(database, start_at)
        python_inputs = wait_for_python_inputs(
            database, start_at, args.binding_log_timeout_seconds
        )
        verify_final_closed_gates(database)

        result = {
            "completed_at": rfc3339(utc_now()),
            "candidate_commit": args.expected_commit,
            "candidate_binary_sha256": args.expected_binary_sha256,
            "release": str(release),
            "previous_release": str(backup.previous_release),
            "evidence_path": str(evidence_path),
            "manifest_sha256": manifest["manifest_sha256"],
            "transaction": transaction,
            "health": health,
            "reconciliation": reconciliation,
            "python_inputs": python_inputs,
            "sell_plan": sell_plan,
            "order_submission_enabled": False,
            "entry_submission_disabled": True,
            "global_kill_switch": True,
            "orders_submitted": 0,
        }
        result_path = backup.directory / "position-adoption-result.json"
        write_private_json(result_path, result)
        log(
            "POSITION_ADOPTION=COMPLETE managed_positions=5 adoptions=4 "
            "submission=false entry_disabled=true kill_switch=true orders_submitted=0 "
            f"result={result_path}"
        )
        return 0
    except Exception:
        if irreversible:
            fix_forward_failure(database, release)
        elif backup is not None:
            restore_before_irreversible(
                backup, database, args.expected_previous_commit
            )
        raise
    finally:
        database.pgpass.unlink(missing_ok=True)
        if migration is not None and migration.extra_pgpass is not None:
            migration.extra_pgpass.unlink(missing_ok=True)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(
            f"POSITION_ADOPTION=FAILED type={type(error).__name__} reason={error}",
            file=sys.stderr,
            flush=True,
        )
        raise
