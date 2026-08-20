from __future__ import annotations

import json
import sqlite3
import stat
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from conftest import TOKEN, headers, make_payload


def post(client: TestClient, payload: dict):
    return client.post("/api/v4/decisions", headers=headers(payload), json=payload)


def test_v1_returns_every_outcome_and_exact_fok_order(client):
    payload = make_payload("multfactor_v1")
    response = post(client, payload)
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert set(response.json()) == {"data"}
    assert set(data) == {
        "schema_version",
        "cycle_id",
        "input_id",
        "context",
        "decided_at",
        "evaluations",
        "exits",
    }
    assert data["schema_version"] == "trading.strategy_output.v4"
    assert data["cycle_id"] == payload["cycle_id"]
    assert data["input_id"] == payload["input_id"]
    assert data["context"] == payload["context"]
    assert len(data["evaluations"]) == 2
    assert len({row["decision_id"] for row in data["evaluations"]}) == 2

    yes, no = data["evaluations"]
    assert yes["prediction_id"] == "prediction-1"
    assert yes["token_id"] == "token-yes"
    assert yes["action"] == "SUBMIT"
    assert yes["reason_code"] == "ENTRY_SIGNAL"
    assert yes["order"] == {
        "side": "BUY",
        "type": "LIMIT",
        "worst_price": "0.40",
        "size": "10",
        "time_in_force": "FOK",
    }
    # v1 does not depend on, and must not fabricate, MOM/MACD evidence.
    assert set(yes["evidence"]["metrics"]) == {
        "best_ask",
        "near_logdiff_usd",
        "rel_spread",
    }
    assert no["action"] == "SKIP"
    assert "order" not in no


def test_v2_uses_fixed_edge_point_10_and_hourly_factors(client):
    payload = make_payload("multfactor_v2")
    response = post(client, payload)
    assert response.status_code == 200, response.text
    yes = response.json()["data"]["evaluations"][0]
    assert yes["action"] == "SUBMIT"
    assert yes["order"]["worst_price"] == "0.40"
    assert yes["order"]["size"] == "10"
    assert set(yes["evidence"]["metrics"]) == {
        "best_ask",
        "near_logdiff_usd",
        "rel_spread",
        "MOM",
        "MACD_SIGNAL",
    }
    assert float(yes["evidence"]["metrics"]["MOM"]) <= 0
    assert float(yes["evidence"]["metrics"]["MACD_SIGNAL"]) <= 0


def test_v2_strict_point_10_edge_boundary(app_factory):
    payload = make_payload("multfactor_v2")
    payload["predictions"][0]["outcomes"][0]["probability"] = 0.500001
    payload["predictions"][0]["outcomes"][1]["probability"] = 0.499999
    with TestClient(app_factory()) as client:
        response = post(client, payload)
    assert response.status_code == 200, response.text
    assert response.json()["data"]["evaluations"][0]["action"] == "SUBMIT"

    payload = make_payload("multfactor_v2")
    payload["predictions"][0]["outcomes"][0]["probability"] = 0.50
    payload["predictions"][0]["outcomes"][1]["probability"] = 0.50
    with TestClient(app_factory()) as client:
        response = post(client, payload)
    assert response.status_code == 200, response.text
    row = response.json()["data"]["evaluations"][0]
    assert row["action"] == "SKIP"
    assert row["reason_code"] == "EDGE_TOO_LOW"


def test_v1_does_not_require_mid_price_history(client):
    payload = make_payload("multfactor_v1")
    for item in payload["mid_price_histories"]:
        item.update(
            {
                "status": "ERROR",
                "coverage_start": None,
                "coverage_end": None,
                "mid_prices": [],
                "error_code": "NOT_REQUIRED_FOR_MULTFACTOR_V1",
            }
        )
    response = post(client, payload)
    assert response.status_code == 200, response.text
    assert response.json()["data"]["evaluations"][0]["action"] == "SUBMIT"


def test_v2_history_failure_is_stale_data(client):
    payload = make_payload("multfactor_v2")
    # PARTIAL is a valid Go v4 input state only when points and coverage are
    # present, but it is deliberately unusable by the v2 strategy.
    payload["mid_price_histories"][0]["status"] = "PARTIAL"
    response = post(client, payload)
    assert response.status_code == 200, response.text
    row = response.json()["data"]["evaluations"][0]
    assert row["action"] == "SKIP"
    assert row["reason_code"] == "STALE_DATA"


def test_invalid_history_status_shape_is_rejected(client):
    payload = make_payload("multfactor_v2")
    payload["mid_price_histories"][0].update(
        {
            "status": "PARTIAL",
            "coverage_start": None,
            "coverage_end": None,
            "mid_prices": [],
            "error_code": "HISTORY_FAILED",
        }
    )
    response = post(client, payload)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_STRATEGY_INPUT"

    payload = make_payload("multfactor_v2")
    payload["mid_price_histories"][0].update(
        {
            "status": "ERROR",
            "coverage_start": None,
            "coverage_end": None,
            "mid_prices": [],
            "error_code": None,
        }
    )
    response = post(client, payload)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_STRATEGY_INPUT"


def test_non_strategy_domain_has_explicit_audit_reason(client):
    payload = make_payload("multfactor_v1")
    payload["predictions"][0]["domains"] = ["Sports"]
    response = post(client, payload)
    assert response.status_code == 200, response.text
    rows = response.json()["data"]["evaluations"]
    assert len(rows) == 2
    assert {row["reason_code"] for row in rows} == {"OUTSIDE_STRATEGY_UNIVERSE"}


def test_book_status_takes_priority_over_history_status(client):
    payload = make_payload("multfactor_v2")
    payload["orderbooks"][0].update(
        {
            "status": "ERROR",
            "source_at": None,
            "tick_size": None,
            "min_order_size": None,
            "best_bid": None,
            "best_ask": None,
            "bids": [],
            "asks": [],
            "error_code": "BOOK_FAILED",
        }
    )
    payload["mid_price_histories"][0].update(
        {
            "status": "ERROR",
            "coverage_start": None,
            "coverage_end": None,
            "mid_prices": [],
            "error_code": "HISTORY_FAILED",
        }
    )
    response = post(client, payload)
    assert response.status_code == 200, response.text
    assert response.json()["data"]["evaluations"][0]["reason_code"] == "INVALID_BOOK"


def test_multiple_predictions_for_same_tokens_are_not_collapsed(client):
    payload = make_payload("multfactor_v1")
    second = json.loads(json.dumps(payload["predictions"][0]))
    second["prediction_id"] = "prediction-2"
    second["source_job_id"] = "job-2"
    second["sandbox_id"] = "sandbox-2"
    payload["predictions"].append(second)
    response = post(client, payload)
    assert response.status_code == 200, response.text
    rows = response.json()["data"]["evaluations"]
    assert len(rows) == 4
    assert len({row["decision_id"] for row in rows}) == 4


def test_exact_48h_lot_emits_full_precision_safe_exit(client):
    payload = make_payload("multfactor_v1")
    payload["positions"] = [
        {
            "lot_id": "lot-1",
            "market_id": "market-1",
            "condition_id": "condition-1",
            "outcome_index": 0,
            "outcome_name": "Yes",
            "token_id": "token-yes",
            "neg_risk": False,
            "entered_at": "2026-08-17T09:20:00Z",
            "shares": "12.505",
            "entry_price": "0.40",
        }
    ]
    response = post(client, payload)
    assert response.status_code == 200, response.text
    exits = response.json()["data"]["exits"]
    assert len(exits) == 1
    assert exits[0]["reason_code"] == "HOLD_48H"
    assert exits[0]["order"] == {
        "side": "SELL",
        "type": "LIMIT",
        "worst_price": "0.39",
        "size": "12.5",
        "time_in_force": "FOK",
    }


def test_missing_auth_and_header_mismatch_fail_closed(client):
    payload = make_payload()
    response = client.post("/api/v4/decisions", json=payload)
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "UNAUTHORIZED"

    bad_headers = headers(payload)
    bad_headers["X-Model-ID"] = "other-model"
    response = client.post("/api/v4/decisions", headers=bad_headers, json=payload)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_STRATEGY_INPUT"


def test_duplicate_identity_header_is_rejected(client):
    payload = make_payload()
    duplicated = list(headers(payload).items())
    duplicated.append(("X-Model-ID", payload["context"]["model_id"]))
    response = client.post("/api/v4/decisions", headers=duplicated, json=payload)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_STRATEGY_INPUT"


def test_wrong_bearer_token_fails_before_body_validation(client):
    payload = make_payload()
    response = client.post(
        "/api/v4/decisions",
        headers=headers(payload, token="x" * len(TOKEN)),
        content=b"not-json",
    )
    assert response.status_code == 401


def test_unknown_json_field_and_unsupported_schema_are_rejected(client):
    payload = make_payload()
    payload["unexpected"] = True
    response = post(client, payload)
    assert response.status_code == 400

    payload = make_payload()
    payload["schema_version"] = "trading.strategy_input.v5"
    response = post(client, payload)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "UNSUPPORTED_SCHEMA"


def test_coerced_probability_and_numeric_timestamp_are_rejected(client):
    payload = make_payload()
    payload["predictions"][0]["outcomes"][0]["probability"] = "0.75"
    response = post(client, payload)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_STRATEGY_INPUT"

    payload = make_payload()
    payload["generated_at"] = 1787131202
    response = post(client, payload)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_STRATEGY_INPUT"


def test_duplicate_json_key_is_rejected(client):
    payload = make_payload()
    encoded = json.dumps(payload, separators=(",", ":"))
    duplicate = encoded[:-1] + ',"schema_version":"trading.strategy_input.v4"}'
    response = client.post(
        "/api/v4/decisions",
        headers=headers(payload),
        content=duplicate,
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_STRATEGY_INPUT"


def test_request_size_limit_rejects_before_json_processing(app_factory):
    payload = make_payload()
    encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    with TestClient(app_factory(max_request_bytes=len(encoded) - 1)) as limited_client:
        response = limited_client.post(
            "/api/v4/decisions",
            headers=headers(payload),
            content=encoded,
        )
    assert response.status_code == 413
    assert response.json()["error"]["code"] == "REQUEST_TOO_LARGE"


def test_retry_and_restart_return_identical_response_bytes(app_factory, tmp_path):
    database = tmp_path / "stable.sqlite3"
    payload = make_payload()
    with TestClient(app_factory(database_path=database)) as first_client:
        first = post(first_client, payload)
        retry = post(first_client, payload)
    assert first.status_code == retry.status_code == 200
    assert first.content == retry.content
    assert first.headers["x-idempotent-replay"] == "false"
    assert retry.headers["x-idempotent-replay"] == "true"

    with TestClient(app_factory(database_path=database)) as restarted_client:
        restarted = post(restarted_client, payload)
    assert restarted.status_code == 200
    assert restarted.content == first.content
    assert restarted.headers["x-idempotent-replay"] == "true"


def test_same_cycle_with_different_body_returns_conflict(client):
    payload = make_payload()
    assert post(client, payload).status_code == 200
    changed = json.loads(json.dumps(payload))
    changed["predictions"][0]["question"] = "Changed immutable input"
    response = post(client, changed)
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"


def test_concurrent_retry_computes_one_durable_response(app_factory):
    app = app_factory()
    payload = make_payload()

    def invoke():
        with TestClient(app) as concurrent_client:
            return post(concurrent_client, payload)

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(lambda _: invoke(), range(2)))
    assert [response.status_code for response in responses] == [200, 200]
    assert responses[0].content == responses[1].content
    assert sorted(response.headers["x-idempotent-replay"] for response in responses) == [
        "false",
        "true",
    ]


def test_sqlite_state_is_private_and_uses_wal(app_factory, tmp_path):
    database = tmp_path / "private-state" / "idempotency.sqlite3"
    app_factory(database_path=database)

    directory_mode = stat.S_IMODE(database.parent.stat().st_mode)
    database_mode = stat.S_IMODE(database.stat().st_mode)
    assert directory_mode == 0o700
    assert database_mode == 0o600

    with sqlite3.connect(database) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)
        assert connection.execute("PRAGMA synchronous").fetchone() == (2,)
