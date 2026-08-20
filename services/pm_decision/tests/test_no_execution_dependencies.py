from __future__ import annotations

import ast
from pathlib import Path


def test_service_has_no_wallet_venue_or_outbound_client_imports():
    source_root = Path(__file__).parents[1] / "src" / "pm_decision"
    forbidden = {
        "requests",
        "httpx",
        "aiohttp",
        "web3",
        "py_clob_client",
        "redis",
        "psycopg",
        "asyncpg",
    }
    found: set[str] = set()
    for path in source_root.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                found.update(alias.name.split(".", 1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                found.add(node.module.split(".", 1)[0])
    assert forbidden.isdisjoint(found), f"forbidden runtime imports: {forbidden & found}"
