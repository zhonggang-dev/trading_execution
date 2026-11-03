#!/usr/bin/env python3
"""Move the strategy service's Kalshi records from `main` to `kalshi:main`.

Before the account-portfolio cutover every Kalshi decision was requested under
the Polymarket wallet `main`; afterwards the Kalshi account requests under its
own id `kalshi:main`. The strategy service (brier_mid_edge_once) keys its
"enter once" history, claims and order attempts by account, so without this
move it would treat `kalshi:main` as a new account and could buy the same
markets again, and it would reject the account's order reports.

Run it while trading-execution is stopped (no strategy requests in flight).
Default is a dry run; --apply backs up the database first and commits in one
transaction. Only rows with account_id=<source> and a `kalshi:` condition_id
change; it refuses to run if the target account already holds Kalshi rows in
a table it moves. Rollback is the same script with --source kalshi:main
--target main (append-only rows already present on the target are kept).

Two tables are append-only (triggers reject UPDATE/DELETE):
- prediction_selections is read by account ("was this prediction evaluated"),
  so its Kalshi rows are copied to the target; the source history stays.
- decision_snapshots is read by decision_id only, so it is left unchanged.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


# Append-only tables read by account: copy instead of move.
COPY_TABLES = {"prediction_selections"}
# Append-only tables read by decision_id only: leave unchanged.
SKIP_TABLES = {"decision_snapshots"}


def account_tables(conn: sqlite3.Connection) -> list[str]:
    tables = []
    for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
        columns = {row[1] for row in conn.execute(f'PRAGMA table_info("{name}")')}
        if {"account_id", "condition_id"} <= columns and name not in SKIP_TABLES:
            tables.append(name)
    return tables


KALSHI = "condition_id LIKE 'kalshi:%'"


def missing_on_target(conn: sqlite3.Connection, table: str) -> str:
    """Rows of the source whose primary key (account aside) is not on the target yet."""
    keys = [row[1] for row in conn.execute(f'PRAGMA table_info("{table}")') if row[5] and row[1] != "account_id"]
    match = " AND ".join(f'other."{k}" IS row."{k}"' for k in keys) or "1"
    return f'NOT EXISTS (SELECT 1 FROM "{table}" other WHERE other.account_id=? AND {match})'


def plan(conn: sqlite3.Connection, source: str, target: str) -> dict[str, int]:
    counts = {}
    for table in account_tables(conn):
        if table in COPY_TABLES:
            counts[table] = conn.execute(
                f'SELECT count(*) FROM "{table}" row WHERE account_id=? AND {KALSHI} AND {missing_on_target(conn, table)}',
                (source, target),
            ).fetchone()[0]
            continue
        existing = conn.execute(f'SELECT count(*) FROM "{table}" WHERE account_id=? AND {KALSHI}', (target,)).fetchone()[0]
        if existing:
            raise SystemExit(f"{table} already has {existing} Kalshi rows for {target}; refusing to merge")
        counts[table] = conn.execute(
            f'SELECT count(*) FROM "{table}" WHERE account_id=? AND {KALSHI}', (source,)
        ).fetchone()[0]
    return counts


def migrate(conn: sqlite3.Connection, source: str, target: str) -> dict[str, int]:
    conn.execute("BEGIN IMMEDIATE")
    try:
        counts = plan(conn, source, target)
        for table, expected in counts.items():
            if table in COPY_TABLES:
                columns = [row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')]
                listed = ", ".join(f'"{c}"' for c in columns)
                selected = ", ".join("?" if c == "account_id" else f'"{c}"' for c in columns)
                changed = conn.execute(
                    f'INSERT INTO "{table}" ({listed}) SELECT {selected} FROM "{table}" row'
                    f" WHERE account_id=? AND {KALSHI} AND {missing_on_target(conn, table)}",
                    (target, source, target),
                ).rowcount
            else:
                changed = conn.execute(
                    f"UPDATE \"{table}\" SET account_id=? WHERE account_id=? AND condition_id LIKE 'kalshi:%'",
                    (target, source),
                ).rowcount
            if changed != expected:
                raise RuntimeError(f"{table}: updated {changed}, planned {expected}")
        conn.execute("COMMIT")
        return counts
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--source", default="main")
    parser.add_argument("--target", default="kalshi:main")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup", type=Path, help="backup file, required with --apply")
    args = parser.parse_args(argv)
    if args.source == args.target or args.source.startswith("kalshi:") == args.target.startswith("kalshi:"):
        parser.error("one of source/target must be the wallet and the other its kalshi: account")
    if args.apply and args.backup is None:
        parser.error("--apply requires --backup")
    conn = sqlite3.connect(str(args.db), isolation_level=None, timeout=30)
    try:
        if not args.apply:
            for table, count in plan(conn, args.source, args.target).items():
                print(f"{table}\t{count}")
            print("dry run: nothing changed")
            return 0
        backup = sqlite3.connect(str(args.backup))
        try:
            conn.backup(backup)
        finally:
            backup.close()
        for table, count in migrate(conn, args.source, args.target).items():
            print(f"{table}\t{count}")
        print(f"applied; backup at {args.backup}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
