from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from pm_decision.app import create_app
from pm_decision.config import Settings

UTC = timezone.utc
DECISION_AT = datetime(2026, 8, 19, 9, 20, tzinfo=UTC)
NOW = DECISION_AT + timedelta(seconds=5)
TOKEN = "test-token-0123456789-abcdefghijklmnop"


def timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def history(token: str, market: str, condition: str, outcome_index: int) -> dict:
    start = DECISION_AT - timedelta(hours=48)
    points = []
    for index in range(1, 49):
        # A continuous decreasing series produces finite, non-positive MOM and
        # MACD_SIGNAL at the final closed hour.
        price = 0.80 - index * 0.005
        points.append(
            {
                "interval_end_at": timestamp(start + timedelta(hours=index)),
                "p": f"{price:.3f}",
            }
        )
    return {
        "market_id": market,
        "condition_id": condition,
        "outcome_index": outcome_index,
        "token_id": token,
        "status": "OK",
        "window_start": timestamp(start),
        "window_end": timestamp(DECISION_AT),
        "fidelity_seconds": 60,
        "sampling": "UPSTREAM_RAW",
        "missing_value_policy": "NO_FILL",
        "timestamp_semantics": "INTERVAL_END_UTC",
        "fetched_at": timestamp(NOW - timedelta(seconds=1)),
        "coverage_start": points[0]["interval_end_at"],
        "coverage_end": points[-1]["interval_end_at"],
        "mid_prices": points,
    }


def book(
    token: str,
    market: str,
    condition: str,
    outcome_index: int,
    *,
    bid: str,
    ask: str,
) -> dict:
    return {
        "market_id": market,
        "condition_id": condition,
        "outcome_index": outcome_index,
        "token_id": token,
        "status": "OK",
        "source_at": timestamp(DECISION_AT),
        "observed_at": timestamp(NOW - timedelta(seconds=2)),
        "tick_size": "0.01",
        "min_order_size": "1",
        "depth_limit": 15,
        "best_bid": bid,
        "best_ask": ask,
        "bids": [{"price": bid, "size": "1000"}],
        "asks": [{"price": ask, "size": "10"}],
    }


def make_payload(strategy_id: str = "multfactor_v1") -> dict:
    account = f"account-{strategy_id}"
    market = "market-1"
    condition = "condition-1"
    yes_token = "token-yes"
    no_token = "token-no"
    return {
        "schema_version": "trading.strategy_input.v4",
        "cycle_id": f"{account}:20260819T092000Z",
        "input_id": f"strategy-input-{strategy_id}-001",
        "context": {
            "model_id": "model-a",
            "strategy_id": strategy_id,
            "execution_account_id": account,
        },
        "decision_at": timestamp(DECISION_AT),
        "generated_at": timestamp(DECISION_AT + timedelta(seconds=2)),
        "prediction_snapshot_id": "predsnap-001",
        "prediction_scope": "ALL_EFFECTIVE_AT_DECISION_AT",
        "predictions": [
            {
                "prediction_id": "prediction-1",
                "source_job_id": "job-1",
                "sandbox_id": "sandbox-1",
                "market_id": market,
                "condition_id": condition,
                "event_id": "event-1",
                "question": "Will the event occur?",
                "event_slug": "event-1",
                "market_slug": "market-1",
                "domains": ["World/Geopolitics"],
                "end_at": timestamp(DECISION_AT + timedelta(days=7)),
                "neg_risk": False,
                "outcomes": [
                    {
                        "index": 0,
                        "name": "Yes",
                        "token_id": yes_token,
                        "probability": 0.75,
                    },
                    {
                        "index": 1,
                        "name": "No",
                        "token_id": no_token,
                        "probability": 0.25,
                    },
                ],
                "prediction_as_of": timestamp(DECISION_AT - timedelta(minutes=10)),
                "completed_at": timestamp(DECISION_AT - timedelta(minutes=5)),
                "available_at": timestamp(DECISION_AT - timedelta(minutes=4)),
                "model": {
                    "name": "model-a",
                    "predictor_version": "predictor-v1",
                    "prompt_version": "prompt-v1",
                },
            }
        ],
        "positions": [],
        "orderbooks": [
            book(yes_token, market, condition, 0, bid="0.39", ask="0.40"),
            book(no_token, market, condition, 1, bid="0.69", ask="0.70"),
        ],
        "mid_price_histories": [
            history(yes_token, market, condition, 0),
            history(no_token, market, condition, 1),
        ],
        "execution_constraints": {
            "size_unit": "SHARES",
            "size_decimal_places": 2,
            "buy_notional_decimal_places": 2,
            "minimum_buy_notional": "1",
            "allowed_time_in_force": ["FOK"],
            "price_protection_policy": "EXACT_TOP_OF_BOOK",
        },
    }


def headers(payload: dict, token: str = TOKEN) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Idempotency-Key": payload["cycle_id"],
        "X-Strategy-Input-ID": payload["input_id"],
        "X-Model-ID": payload["context"]["model_id"],
        "X-Strategy-ID": payload["context"]["strategy_id"],
        "X-Execution-Account-ID": payload["context"]["execution_account_id"],
    }


@pytest.fixture
def payload() -> dict:
    return make_payload()


@pytest.fixture
def app_factory(tmp_path: Path):
    counter = 0

    def factory(
        *,
        database_path: Path | None = None,
        max_request_bytes: int = 256 * 1024 * 1024,
    ):
        nonlocal counter
        counter += 1
        path = database_path or tmp_path / f"idempotency-{counter}.sqlite3"
        settings = Settings(
            bearer_token=TOKEN,
            database_path=path,
            v1_target_notional=Decimal("5"),
            v2_target_notional=Decimal("10"),
            max_request_bytes=max_request_bytes,
        )
        return create_app(settings, clock=lambda: NOW)

    return factory


@pytest.fixture
def client(app_factory) -> TestClient:
    with TestClient(app_factory()) as test_client:
        yield test_client


@pytest.fixture
def deep_copy():
    return copy.deepcopy
