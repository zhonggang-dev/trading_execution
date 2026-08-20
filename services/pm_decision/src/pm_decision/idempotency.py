from __future__ import annotations

import os
import sqlite3
from collections.abc import Callable
from pathlib import Path

from .errors import IdempotencyConflict, StoreUnavailable


class SQLiteIdempotencyStore:
    def __init__(self, path: Path, busy_timeout_ms: int = 30_000) -> None:
        self.path = path
        self.busy_timeout_ms = busy_timeout_ms
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            with self._connect() as connection:
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS decision_responses (
                        cycle_id TEXT PRIMARY KEY,
                        input_id TEXT NOT NULL,
                        request_hash TEXT NOT NULL,
                        response_json BLOB NOT NULL,
                        created_at TEXT NOT NULL
                    );
                    CREATE UNIQUE INDEX IF NOT EXISTS decision_responses_input_id
                        ON decision_responses(input_id);
                    """
                )
            os.chmod(self.path, 0o600)
        except (OSError, sqlite3.Error) as exc:
            raise StoreUnavailable("initialize SQLite idempotency store") from exc

    def resolve(
        self,
        *,
        cycle_id: str,
        input_id: str,
        request_hash: str,
        created_at: str,
        compute: Callable[[], bytes],
    ) -> tuple[bytes, bool]:
        """Return a durable response, computing it once under BEGIN IMMEDIATE."""

        connection: sqlite3.Connection | None = None
        try:
            connection = self._connect()
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT input_id, request_hash, response_json "
                "FROM decision_responses WHERE cycle_id = ?",
                (cycle_id,),
            ).fetchone()
            if row is not None:
                if row[0] != input_id or row[1] != request_hash:
                    raise IdempotencyConflict
                connection.commit()
                return bytes(row[2]), True

            # Also reject reuse of an input identity under a different cycle.
            other = connection.execute(
                "SELECT cycle_id, request_hash FROM decision_responses WHERE input_id = ?",
                (input_id,),
            ).fetchone()
            if other is not None and (other[0] != cycle_id or other[1] != request_hash):
                raise IdempotencyConflict

            response = compute()
            connection.execute(
                "INSERT INTO decision_responses "
                "(cycle_id, input_id, request_hash, response_json, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (cycle_id, input_id, request_hash, response, created_at),
            )
            connection.commit()
            return response, False
        except IdempotencyConflict:
            if connection is not None:
                connection.rollback()
            raise
        except (OSError, sqlite3.Error) as exc:
            if connection is not None:
                connection.rollback()
            raise StoreUnavailable("use SQLite idempotency store") from exc
        except Exception:
            if connection is not None:
                connection.rollback()
            raise
        finally:
            if connection is not None:
                connection.close()

    def ready(self) -> bool:
        try:
            with self._connect() as connection:
                return connection.execute("SELECT 1").fetchone() == (1,)
        except sqlite3.Error:
            return False

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1000,
            isolation_level=None,
        )
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA foreign_keys = ON")
        return connection
