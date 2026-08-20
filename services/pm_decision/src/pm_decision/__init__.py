"""Polymarket decision adapter.

This package deliberately contains no wallet, venue, RPC, Redis, PostgreSQL,
or outbound HTTP client. It turns an authenticated frozen v4 strategy input
into a deterministic strategy output; Trading Execution remains the only
component allowed to choose accounts, apply risk policy, or submit orders.
"""

from .app import create_app

__all__ = ["create_app"]
