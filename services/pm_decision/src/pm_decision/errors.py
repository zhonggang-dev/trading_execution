from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class APIError(Exception):
    status_code: int
    code: str
    message: str
    retryable: bool = False


class IdempotencyConflict(Exception):
    """The cycle was already claimed with different immutable input."""


class StoreUnavailable(Exception):
    """The durable idempotency store could not safely serve a request."""
