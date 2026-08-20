from __future__ import annotations

import math
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Annotated, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
    model_validator,
)

INPUT_SCHEMA = "trading.strategy_input.v4"
OUTPUT_SCHEMA = "trading.strategy_output.v4"
PREDICTION_SCOPE = "ALL_EFFECTIVE_AT_DECISION_AT"
DECISION_INTERVAL_SECONDS = 600
MID_PRICE_LOOKBACK = timedelta(hours=48)

STRATEGY_V1 = "multfactor_v1"
STRATEGY_V2 = "multfactor_v2"
SUPPORTED_STRATEGIES = frozenset((STRATEGY_V1, STRATEGY_V2))

REASON_ENTRY_SIGNAL = "ENTRY_SIGNAL"
REASON_EDGE_TOO_LOW = "EDGE_TOO_LOW"
REASON_SPREAD_TOO_WIDE = "SPREAD_TOO_WIDE"
REASON_LIQUIDITY_TOO_LOW = "LIQUIDITY_TOO_LOW"
REASON_PRICE_OUT_OF_RANGE = "PRICE_OUT_OF_RANGE"
REASON_HOURLY_VETO = "HOURLY_VETO"
REASON_FACTOR_WARMUP = "FACTOR_WARMUP"
REASON_STALE_DATA = "STALE_DATA"
REASON_INVALID_BOOK = "INVALID_BOOK"
REASON_HOLD_48H = "HOLD_48H"
# The current Go branch must add this closed enum before production rollout.
REASON_OUTSIDE_STRATEGY_UNIVERSE = "OUTSIDE_STRATEGY_UNIVERSE"

_DECIMAL_RE = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")


def _decimal_text(value: str) -> str:
    if not _DECIMAL_RE.fullmatch(value):
        raise ValueError("must be a non-exponent base-10 decimal string")
    try:
        parsed = Decimal(value)
    except Exception as exc:  # pragma: no cover - regex already excludes it
        raise ValueError("must be a base-10 decimal string") from exc
    if not parsed.is_finite():
        raise ValueError("must be finite")
    return value


DecimalText = Annotated[StrictStr, AfterValidator(_decimal_text)]


def _timestamp_text(value):
    if not isinstance(value, str):
        raise ValueError("must be an RFC3339 timestamp string")
    return value


Timestamp = Annotated[datetime, BeforeValidator(_timestamp_text)]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BookStatus(str, Enum):
    OK = "OK"
    EMPTY = "EMPTY"
    MISSING = "MISSING"
    ERROR = "ERROR"


class HistoryStatus(str, Enum):
    OK = "OK"
    PARTIAL = "PARTIAL"
    EMPTY = "EMPTY"
    MISSING = "MISSING"
    ERROR = "ERROR"


class ExecutionContext(StrictModel):
    model_id: StrictStr = Field(min_length=1)
    strategy_id: StrictStr = Field(min_length=1)
    execution_account_id: StrictStr = Field(min_length=1)


class PredictionOutcome(StrictModel):
    index: StrictInt
    name: StrictStr = Field(min_length=1)
    token_id: StrictStr = Field(min_length=1)
    probability: StrictFloat

    @model_validator(mode="after")
    def validate_probability(self) -> "PredictionOutcome":
        if not math.isfinite(self.probability) or not 0 <= self.probability <= 1:
            raise ValueError("probability must be finite and in [0,1]")
        return self


class PredictionModel(StrictModel):
    name: StrictStr = Field(min_length=1)
    predictor_version: StrictStr | None = None
    prompt_version: StrictStr | None = None


class Prediction(StrictModel):
    prediction_id: StrictStr = Field(min_length=1)
    source_job_id: StrictStr = Field(min_length=1)
    sandbox_id: StrictStr = Field(min_length=1)
    market_id: StrictStr = Field(min_length=1)
    condition_id: StrictStr = Field(min_length=1)
    event_id: StrictStr | None = None
    question: StrictStr = Field(min_length=1)
    event_slug: StrictStr | None = None
    market_slug: StrictStr | None = None
    domains: list[StrictStr]
    end_at: Timestamp | None = None
    neg_risk: StrictBool
    outcomes: list[PredictionOutcome] = Field(min_length=2, max_length=2)
    prediction_as_of: Timestamp
    completed_at: Timestamp
    available_at: Timestamp
    model: PredictionModel

    @model_validator(mode="after")
    def validate_outcomes(self) -> "Prediction":
        if [outcome.index for outcome in self.outcomes] != [0, 1]:
            raise ValueError("outcomes must have consecutive indices 0 and 1")
        if len({outcome.token_id for outcome in self.outcomes}) != 2:
            raise ValueError("outcome token ids must be unique")
        if abs(sum(outcome.probability for outcome in self.outcomes) - 1.0) > 1e-6:
            raise ValueError("outcome probabilities must sum to one")
        return self


class PositionLot(StrictModel):
    lot_id: StrictStr = Field(min_length=1)
    market_id: StrictStr = Field(min_length=1)
    condition_id: StrictStr = Field(min_length=1)
    outcome_index: StrictInt
    outcome_name: StrictStr = Field(min_length=1)
    token_id: StrictStr = Field(min_length=1)
    neg_risk: StrictBool
    entered_at: Timestamp
    shares: DecimalText
    entry_price: DecimalText

    @model_validator(mode="after")
    def validate_values(self) -> "PositionLot":
        if self.outcome_index not in (0, 1):
            raise ValueError("outcome_index must be 0 or 1")
        if Decimal(self.shares) <= 0:
            raise ValueError("shares must be positive")
        if not 0 < Decimal(self.entry_price) <= 1:
            raise ValueError("entry_price must be in (0,1]")
        return self


class PriceLevel(StrictModel):
    price: DecimalText
    size: DecimalText

    @model_validator(mode="after")
    def validate_values(self) -> "PriceLevel":
        if not 0 < Decimal(self.price) <= 1:
            raise ValueError("price must be in (0,1]")
        if Decimal(self.size) <= 0:
            raise ValueError("size must be positive")
        return self


class OrderBook(StrictModel):
    market_id: StrictStr = Field(min_length=1)
    condition_id: StrictStr = Field(min_length=1)
    outcome_index: StrictInt
    token_id: StrictStr = Field(min_length=1)
    status: BookStatus
    source_at: Timestamp | None = None
    observed_at: Timestamp
    tick_size: DecimalText | None = None
    min_order_size: DecimalText | None = None
    depth_limit: StrictInt
    best_bid: DecimalText | None = None
    best_ask: DecimalText | None = None
    bids: list[PriceLevel]
    asks: list[PriceLevel]
    error_code: StrictStr | None = None

    @model_validator(mode="after")
    def validate_book(self) -> "OrderBook":
        if self.outcome_index not in (0, 1):
            raise ValueError("outcome_index must be 0 or 1")
        if self.depth_limit != 15 or len(self.bids) > 15 or len(self.asks) > 15:
            raise ValueError("depth_limit must be 15 and each side may contain at most 15 levels")
        if self.status is not BookStatus.OK:
            return self
        if (
            self.source_at is None
            or self.tick_size is None
            or self.min_order_size is None
            or self.best_bid is None
            or self.best_ask is None
            or not self.bids
            or not self.asks
        ):
            raise ValueError("OK orderbook requires source, tick, minimum, top prices, bids, and asks")
        if Decimal(self.tick_size) <= 0 or Decimal(self.min_order_size) <= 0:
            raise ValueError("tick_size and min_order_size must be positive")
        if Decimal(self.best_bid) != Decimal(self.bids[0].price):
            raise ValueError("best_bid must equal bids[0].price")
        if Decimal(self.best_ask) != Decimal(self.asks[0].price):
            raise ValueError("best_ask must equal asks[0].price")
        bid_prices = [Decimal(level.price) for level in self.bids]
        ask_prices = [Decimal(level.price) for level in self.asks]
        # Match the Go v4 wire validator: levels must be sorted, while equal
        # adjacent prices are tolerated. The factor port still consumes every
        # supplied level without coalescing or changing its meaning.
        if any(left < right for left, right in zip(bid_prices, bid_prices[1:])):
            raise ValueError("bid prices must be descending")
        if any(left > right for left, right in zip(ask_prices, ask_prices[1:])):
            raise ValueError("ask prices must be ascending")
        return self


class MidPricePoint(StrictModel):
    interval_end_at: Timestamp
    p: DecimalText

    @model_validator(mode="after")
    def validate_price(self) -> "MidPricePoint":
        if not 0 <= Decimal(self.p) <= 1:
            raise ValueError("p must be in [0,1]")
        return self


class MidPriceHistory(StrictModel):
    market_id: StrictStr = Field(min_length=1)
    condition_id: StrictStr = Field(min_length=1)
    outcome_index: StrictInt
    token_id: StrictStr = Field(min_length=1)
    status: HistoryStatus
    window_start: Timestamp
    window_end: Timestamp
    fidelity_seconds: StrictInt
    sampling: Literal["UPSTREAM_RAW"]
    missing_value_policy: Literal["NO_FILL"]
    timestamp_semantics: Literal["INTERVAL_END_UTC"]
    fetched_at: Timestamp
    coverage_start: Timestamp | None = None
    coverage_end: Timestamp | None = None
    mid_prices: list[MidPricePoint]
    error_code: StrictStr | None = None

    @model_validator(mode="after")
    def validate_history(self) -> "MidPriceHistory":
        if self.outcome_index not in (0, 1):
            raise ValueError("outcome_index must be 0 or 1")
        if self.fidelity_seconds != 60:
            raise ValueError("fidelity_seconds must be 60")
        previous: datetime | None = None
        for point in self.mid_prices:
            if previous is not None and point.interval_end_at <= previous:
                raise ValueError("mid-price points must be strictly increasing and unique")
            previous = point.interval_end_at
        if self.status in (HistoryStatus.OK, HistoryStatus.PARTIAL):
            if not self.mid_prices or self.coverage_start is None or self.coverage_end is None:
                raise ValueError(f"{self.status.value} history requires points and coverage")
            if self.coverage_start != self.mid_prices[0].interval_end_at:
                raise ValueError("coverage_start must equal the first point")
            if self.coverage_end != self.mid_prices[-1].interval_end_at:
                raise ValueError("coverage_end must equal the last point")
        elif self.status is HistoryStatus.EMPTY:
            if self.mid_prices:
                raise ValueError("EMPTY history must not contain points")
        elif self.status in (HistoryStatus.MISSING, HistoryStatus.ERROR):
            if self.mid_prices or not self.error_code or not self.error_code.strip():
                raise ValueError(
                    f"{self.status.value} history requires an error code and no points"
                )
        return self


class ExecutionConstraints(StrictModel):
    size_unit: Literal["SHARES"]
    size_decimal_places: StrictInt
    buy_notional_decimal_places: StrictInt
    minimum_buy_notional: DecimalText
    allowed_time_in_force: list[Literal["FOK"]]
    price_protection_policy: Literal["EXACT_TOP_OF_BOOK"]

    @model_validator(mode="after")
    def validate_constraints(self) -> "ExecutionConstraints":
        if (
            self.size_decimal_places != 2
            or self.buy_notional_decimal_places != 2
            or self.allowed_time_in_force != ["FOK"]
            or Decimal(self.minimum_buy_notional) <= 0
        ):
            raise ValueError("unsupported execution constraint set")
        return self


class StrategyRequest(StrictModel):
    schema_version: Literal["trading.strategy_input.v4"]
    cycle_id: StrictStr = Field(min_length=1)
    input_id: StrictStr = Field(min_length=1)
    context: ExecutionContext
    decision_at: Timestamp
    generated_at: Timestamp
    prediction_snapshot_id: StrictStr = Field(min_length=1)
    prediction_scope: Literal["ALL_EFFECTIVE_AT_DECISION_AT"]
    predictions: list[Prediction]
    positions: list[PositionLot]
    orderbooks: list[OrderBook]
    mid_price_histories: list[MidPriceHistory]
    execution_constraints: ExecutionConstraints

    @model_validator(mode="after")
    def validate_request(self) -> "StrategyRequest":
        for name, value in (
            ("decision_at", self.decision_at),
            ("generated_at", self.generated_at),
        ):
            _require_utc(name, value)
        if int(self.decision_at.timestamp()) % DECISION_INTERVAL_SECONDS != 0:
            raise ValueError("decision_at must be an exact UTC 10-minute boundary")
        if self.generated_at < self.decision_at:
            raise ValueError("generated_at must not precede decision_at")
        if self.context.strategy_id not in SUPPORTED_STRATEGIES:
            raise ValueError("unsupported canonical strategy_id")
        expected_cycle = (
            f"{self.context.execution_account_id}:"
            f"{self.decision_at.strftime('%Y%m%dT%H%M%SZ')}"
        )
        if self.cycle_id != expected_cycle:
            raise ValueError("cycle_id does not match context and decision_at")

        prediction_ids: set[str] = set()
        lot_ids: set[str] = set()
        target_identity: dict[str, tuple[str, str, int]] = {}
        for prediction in self.predictions:
            if prediction.prediction_id in prediction_ids:
                raise ValueError("duplicate prediction_id")
            prediction_ids.add(prediction.prediction_id)
            if prediction.model.name != self.context.model_id:
                raise ValueError("prediction model does not match context.model_id")
            for name, value in (
                ("prediction_as_of", prediction.prediction_as_of),
                ("completed_at", prediction.completed_at),
                ("available_at", prediction.available_at),
            ):
                _require_utc(name, value)
                if value > self.decision_at:
                    raise ValueError("prediction was not available at decision_at")
            if not (
                prediction.prediction_as_of
                <= prediction.completed_at
                <= prediction.available_at
            ):
                raise ValueError("prediction timestamps are causally inconsistent")
            if prediction.end_at is not None:
                _require_utc("end_at", prediction.end_at)
                if prediction.end_at <= self.decision_at:
                    raise ValueError("prediction market must still be effective")
            for outcome in prediction.outcomes:
                _add_target(
                    target_identity,
                    outcome.token_id,
                    prediction.market_id,
                    prediction.condition_id,
                    outcome.index,
                )
        for lot in self.positions:
            if lot.lot_id in lot_ids:
                raise ValueError("duplicate lot_id")
            lot_ids.add(lot.lot_id)
            _require_utc("entered_at", lot.entered_at)
            if lot.entered_at > self.decision_at:
                raise ValueError("position entered_at must not be in the future")
            _add_target(
                target_identity,
                lot.token_id,
                lot.market_id,
                lot.condition_id,
                lot.outcome_index,
            )

        books = _unique_by_token(self.orderbooks, "orderbook")
        histories = _unique_by_token(self.mid_price_histories, "mid-price history")
        if set(books) != set(target_identity) or set(histories) != set(target_identity):
            raise ValueError("orderbooks and histories must exactly cover prediction and position tokens")
        for token_id, identity in target_identity.items():
            book = books[token_id]
            history = histories[token_id]
            if (book.market_id, book.condition_id, book.outcome_index) != identity:
                raise ValueError("orderbook identity does not match token identity")
            if (history.market_id, history.condition_id, history.outcome_index) != identity:
                raise ValueError("mid-price history identity does not match token identity")
            _require_utc("orderbook.observed_at", book.observed_at)
            if book.source_at is not None:
                _require_utc("orderbook.source_at", book.source_at)
            for name, value in (
                ("history.window_start", history.window_start),
                ("history.window_end", history.window_end),
                ("history.fetched_at", history.fetched_at),
            ):
                _require_utc(name, value)
            if history.window_end != self.decision_at:
                raise ValueError("history.window_end must equal decision_at")
            if history.window_end - history.window_start != MID_PRICE_LOOKBACK:
                raise ValueError("strategy requires an exact 48-hour history window")
            for point in history.mid_prices:
                _require_utc("mid_prices.interval_end_at", point.interval_end_at)
                if not history.window_start <= point.interval_end_at <= history.window_end:
                    raise ValueError("mid-price point lies outside the requested window")
        return self

    def book_by_token(self) -> dict[str, OrderBook]:
        return {book.token_id: book for book in self.orderbooks}

    def history_by_token(self) -> dict[str, MidPriceHistory]:
        return {history.token_id: history for history in self.mid_price_histories}


def _require_utc(name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be an aware UTC timestamp")


def _add_target(
    targets: dict[str, tuple[str, str, int]],
    token_id: str,
    market_id: str,
    condition_id: str,
    outcome_index: int,
) -> None:
    identity = (market_id, condition_id, outcome_index)
    existing = targets.setdefault(token_id, identity)
    if existing != identity:
        raise ValueError("token_id has conflicting execution identity")


def _unique_by_token(values: list, label: str) -> dict[str, object]:
    result: dict[str, object] = {}
    for value in values:
        if value.token_id in result:
            raise ValueError(f"duplicate {label} token_id")
        result[value.token_id] = value
    return result


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
