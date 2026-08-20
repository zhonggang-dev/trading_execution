from __future__ import annotations

import os
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Settings:
    bearer_token: str
    database_path: Path
    v1_target_notional: Decimal
    v2_target_notional: Decimal
    max_request_bytes: int = 256 * 1024 * 1024
    sqlite_busy_timeout_ms: int = 30_000

    def __post_init__(self) -> None:
        if len(self.bearer_token.strip()) < 32:
            raise ValueError("PM decision bearer token must contain at least 32 characters")
        if not self.database_path.is_absolute():
            raise ValueError("PM decision database path must be absolute")
        if (
            not self.v1_target_notional.is_finite()
            or not self.v2_target_notional.is_finite()
            or self.v1_target_notional <= 0
            or self.v2_target_notional <= 0
        ):
            raise ValueError("PM decision target notionals must be positive and finite")
        if self.max_request_bytes <= 0 or self.sqlite_busy_timeout_ms <= 0:
            raise ValueError("PM decision size and timeout limits must be positive")

    @classmethod
    def from_env(cls) -> "Settings":
        token = os.environ.get("PM_DECISION_BEARER_TOKEN", "").strip()
        token_file = os.environ.get("PM_DECISION_BEARER_TOKEN_FILE", "").strip()
        if token and token_file:
            raise ValueError(
                "configure exactly one of PM_DECISION_BEARER_TOKEN or "
                "PM_DECISION_BEARER_TOKEN_FILE"
            )
        if token_file:
            token = Path(token_file).read_text(encoding="utf-8").strip()
        if len(token) < 32:
            raise ValueError("PM decision bearer token must contain at least 32 characters")

        database_path = Path(
            os.environ.get(
                "PM_DECISION_DATABASE_PATH",
                "/var/lib/pm-decision/idempotency.sqlite3",
            )
        )
        max_request_bytes = _positive_int(
            "PM_DECISION_MAX_REQUEST_BYTES", 256 * 1024 * 1024
        )
        sqlite_busy_timeout_ms = _positive_int(
            "PM_DECISION_SQLITE_BUSY_TIMEOUT_MS", 30_000
        )
        return cls(
            bearer_token=token,
            database_path=database_path,
            v1_target_notional=_required_positive_decimal(
                "PM_DECISION_V1_TARGET_NOTIONAL"
            ),
            v2_target_notional=_required_positive_decimal(
                "PM_DECISION_V2_TARGET_NOTIONAL"
            ),
            max_request_bytes=max_request_bytes,
            sqlite_busy_timeout_ms=sqlite_busy_timeout_ms,
        )


def _positive_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be positive")
    return parsed


def _required_positive_decimal(name: str) -> Decimal:
    raw = os.environ.get(name, "").strip()
    if not re.fullmatch(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?", raw):
        raise ValueError(f"{name} is required and must be a non-exponent base-10 decimal")
    try:
        parsed = Decimal(raw)
    except InvalidOperation as exc:
        raise ValueError(f"{name} must be a base-10 decimal") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise ValueError(f"{name} must be positive and finite")
    return parsed
