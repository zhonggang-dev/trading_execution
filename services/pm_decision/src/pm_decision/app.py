from __future__ import annotations

import hashlib
import json
import logging
import secrets
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, Response
from pydantic import ValidationError

from .config import Settings
from .contract import INPUT_SCHEMA, StrategyRequest, utc_now
from .engine import DecisionEngine
from .errors import APIError, IdempotencyConflict, StoreUnavailable
from .idempotency import SQLiteIdempotencyStore

logger = logging.getLogger("pm_decision")


def create_app(
    settings: Settings,
    *,
    clock: Callable[[], datetime] = utc_now,
    engine: DecisionEngine | None = None,
) -> FastAPI:
    store = SQLiteIdempotencyStore(
        settings.database_path, settings.sqlite_busy_timeout_ms
    )
    decision_engine = engine or DecisionEngine(
        v1_target_notional=settings.v1_target_notional,
        v2_target_notional=settings.v2_target_notional,
    )
    app = FastAPI(
        title="pm-decision",
        version="0.1.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.exception_handler(APIError)
    async def api_error_handler(_: Request, exc: APIError) -> JSONResponse:
        headers = {"WWW-Authenticate": "Bearer"} if exc.status_code == 401 else None
        return JSONResponse(
            status_code=exc.status_code,
            headers=headers,
            content={
                "error": {
                    "code": exc.code,
                    "message": exc.message,
                    "retryable": exc.retryable,
                }
            },
        )

    @app.get("/health")
    async def health() -> dict:
        return {"status": "up", "service": "pm-decision"}

    @app.get("/ready")
    async def ready() -> Response:
        available = await run_in_threadpool(store.ready)
        return JSONResponse(
            status_code=200 if available else 503,
            content={"status": "ready" if available else "not_ready"},
        )

    @app.post("/api/v4/decisions")
    async def decisions(http_request: Request) -> Response:
        _authenticate(http_request, settings.bearer_token)
        _require_json_content_type(http_request)
        raw = await _read_bounded_body(http_request, settings.max_request_bytes)
        data = _load_strict_json(raw)
        if not isinstance(data, dict):
            raise APIError(400, "INVALID_STRATEGY_INPUT", "request body must be one JSON object")
        if data.get("schema_version") != INPUT_SCHEMA:
            raise APIError(422, "UNSUPPORTED_SCHEMA", "unsupported strategy input schema")
        try:
            strategy_request = StrategyRequest.model_validate(data)
        except ValidationError as exc:
            raise APIError(
                400,
                "INVALID_STRATEGY_INPUT",
                _validation_message(exc),
            ) from exc

        _validate_identity_headers(http_request, strategy_request)
        now = _utc(clock())
        if strategy_request.decision_at > now + timedelta(seconds=5):
            raise APIError(400, "INVALID_STRATEGY_INPUT", "decision_at is in the future")
        if strategy_request.generated_at > now + timedelta(seconds=5):
            raise APIError(400, "INVALID_STRATEGY_INPUT", "generated_at is in the future")

        canonical_request = json.dumps(
            data,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        request_hash = hashlib.sha256(canonical_request).hexdigest()

        def compute() -> bytes:
            decided_at = _utc(clock())
            if decided_at < strategy_request.decision_at:
                raise APIError(400, "INVALID_STRATEGY_INPUT", "decision boundary has not arrived")
            output = decision_engine.decide(strategy_request, decided_at)
            envelope = {"data": output}
            return json.dumps(
                envelope,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")

        try:
            response_bytes, replayed = await run_in_threadpool(
                store.resolve,
                cycle_id=strategy_request.cycle_id,
                input_id=strategy_request.input_id,
                request_hash=request_hash,
                created_at=_timestamp(now),
                compute=compute,
            )
        except IdempotencyConflict as exc:
            raise APIError(
                409,
                "IDEMPOTENCY_CONFLICT",
                "cycle_id or input_id was already claimed by different input",
            ) from exc
        except StoreUnavailable as exc:
            raise APIError(
                503,
                "IDEMPOTENCY_STORE_UNAVAILABLE",
                "durable decision store is unavailable",
                retryable=True,
            ) from exc
        except APIError:
            raise
        except Exception as exc:  # fail closed; never return an unpersisted intent
            logger.exception("strategy decision failed")
            raise APIError(
                500,
                "STRATEGY_DECISION_FAILED",
                "strategy decision failed",
                retryable=True,
            ) from exc

        return Response(
            content=response_bytes,
            media_type="application/json",
            headers={"X-Idempotent-Replay": "true" if replayed else "false"},
        )

    return app


async def _read_bounded_body(request: Request, maximum_bytes: int) -> bytes:
    content_length = _single_header(request, "content-length")
    if content_length is not None:
        try:
            declared_length = int(content_length)
        except ValueError as exc:
            raise APIError(
                400,
                "INVALID_STRATEGY_INPUT",
                "Content-Length must be a non-negative integer",
            ) from exc
        if declared_length < 0:
            raise APIError(
                400,
                "INVALID_STRATEGY_INPUT",
                "Content-Length must be a non-negative integer",
            )
        if declared_length > maximum_bytes:
            raise APIError(
                413,
                "REQUEST_TOO_LARGE",
                "strategy input exceeds the configured limit",
            )

    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > maximum_bytes:
            raise APIError(
                413,
                "REQUEST_TOO_LARGE",
                "strategy input exceeds the configured limit",
            )
    return bytes(body)


def _authenticate(request: Request, expected_token: str) -> None:
    authorization = _single_header(request, "authorization")
    if authorization is None or not authorization.startswith("Bearer "):
        raise APIError(401, "UNAUTHORIZED", "valid bearer authentication is required")
    supplied = authorization[len("Bearer ") :]
    if not supplied or not secrets.compare_digest(supplied, expected_token):
        raise APIError(401, "UNAUTHORIZED", "valid bearer authentication is required")


def _require_json_content_type(request: Request) -> None:
    content_type = _single_header(request, "content-type")
    if content_type is None or content_type.split(";", 1)[0].strip().lower() != "application/json":
        raise APIError(415, "UNSUPPORTED_MEDIA_TYPE", "Content-Type must be application/json")


def _validate_identity_headers(request: Request, body: StrategyRequest) -> None:
    expected = {
        "idempotency-key": body.cycle_id,
        "x-strategy-input-id": body.input_id,
        "x-model-id": body.context.model_id,
        "x-strategy-id": body.context.strategy_id,
        "x-execution-account-id": body.context.execution_account_id,
    }
    for name, value in expected.items():
        supplied = _single_header(request, name)
        if supplied is None or supplied != value:
            raise APIError(
                400,
                "INVALID_STRATEGY_INPUT",
                f"{name} must exactly match the request body",
            )


def _single_header(request: Request, name: str) -> str | None:
    encoded = name.lower().encode("latin-1")
    values = [
        value.decode("latin-1")
        for key, value in request.scope.get("headers", [])
        if key.lower() == encoded
    ]
    if len(values) > 1:
        raise APIError(400, "INVALID_STRATEGY_INPUT", f"duplicate {name} header")
    return values[0].strip() if values else None


def _load_strict_json(raw: bytes):
    if not raw:
        raise APIError(400, "INVALID_STRATEGY_INPUT", "request body is required")

    def reject_constant(_: str):
        raise ValueError("non-finite JSON number")

    def reject_duplicates(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key {key!r}")
            result[key] = value
        return result

    try:
        return json.loads(
            raw,
            parse_constant=reject_constant,
            object_pairs_hook=reject_duplicates,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise APIError(400, "INVALID_STRATEGY_INPUT", "request body is not strict JSON") from exc


def _validation_message(exc: ValidationError) -> str:
    summaries: list[str] = []
    for error in exc.errors(include_url=False, include_input=False)[:8]:
        location = ".".join(str(part) for part in error.get("loc", ())) or "body"
        summaries.append(f"{location}: {error.get('msg', 'invalid value')}")
    suffix = "" if len(exc.errors()) <= 8 else "; additional errors omitted"
    return "invalid strategy input: " + "; ".join(summaries) + suffix


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("clock must return an aware timestamp")
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")
