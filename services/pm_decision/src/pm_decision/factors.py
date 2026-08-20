from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from .contract import (
    MidPriceHistory,
    OrderBook,
    REASON_INVALID_BOOK,
    REASON_LIQUIDITY_TOO_LOW,
    REASON_PRICE_OUT_OF_RANGE,
)

ORDERBOOK_LEVELS = 15
NEAR_LOGIT_RADIUS = 0.10
NEAR_MIN_MID = 0.02
NEAR_MAX_MID = 0.98
NEAR_MIN_LIQUIDITY_USD = 10.0
EPS = 1e-12


@dataclass(frozen=True, slots=True)
class BookFactors:
    best_bid_text: str
    best_ask_text: str
    best_bid: float
    best_ask: float
    mid: float
    bid_usd: float
    ask_usd: float
    liquidity_usd: float
    near_logdiff_usd: float
    rel_spread: float


@dataclass(frozen=True, slots=True)
class BookFactorResult:
    factors: BookFactors | None
    reason_code: str | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class HourlyBar:
    bar_end_at: datetime
    close: float
    segment_id: int


@dataclass(frozen=True, slots=True)
class HourlyFactors:
    bar_end_at: datetime
    mom: float
    macd_signal: float


def compute_book_factors(book: OrderBook) -> BookFactorResult:
    """Port of the frozen dz0p1 near-book factor used by parity v1/v2.

    The strategy historically computes this factor with binary float math.
    Input/output transport remains decimal strings, but the gate calculation
    intentionally preserves those float semantics for backtest/live parity.
    """

    if not book.bids or not book.asks or book.best_bid is None or book.best_ask is None:
        return BookFactorResult(None, REASON_INVALID_BOOK, "orderbook has no two-sided top of book")

    best_bid = float(book.best_bid)
    best_ask = float(book.best_ask)
    if not (math.isfinite(best_bid) and math.isfinite(best_ask)):
        return BookFactorResult(None, REASON_INVALID_BOOK, "orderbook top prices are not finite")
    if best_bid <= 0 or best_ask <= 0 or best_ask + EPS < best_bid:
        return BookFactorResult(None, REASON_INVALID_BOOK, "orderbook top prices are invalid or crossed")

    mid = (best_bid + best_ask) / 2.0
    if not (NEAR_MIN_MID < mid < NEAR_MAX_MID):
        return BookFactorResult(
            None,
            REASON_PRICE_OUT_OF_RANGE,
            "mid price is outside the factor validity band",
        )

    clipped = max(EPS, min(1.0 - EPS, mid))
    logit_mid = math.log(clipped / (1.0 - clipped))
    low_price = _sigmoid(logit_mid - NEAR_LOGIT_RADIUS)
    high_price = _sigmoid(logit_mid + NEAR_LOGIT_RADIUS)

    bid_usd = 0.0
    ask_usd = 0.0
    for level in book.bids[:ORDERBOOK_LEVELS]:
        price, size = float(level.price), float(level.size)
        if (
            math.isfinite(price)
            and math.isfinite(size)
            and price > 0
            and size > 0
            and price <= best_bid + EPS
            and price + EPS >= low_price
        ):
            bid_usd += price * size
    for level in book.asks[:ORDERBOOK_LEVELS]:
        price, size = float(level.price), float(level.size)
        if (
            math.isfinite(price)
            and math.isfinite(size)
            and price > 0
            and size > 0
            and price + EPS >= best_ask
            and price <= high_price + EPS
        ):
            ask_usd += price * size

    liquidity = bid_usd + ask_usd
    if bid_usd <= 0 or ask_usd <= 0 or liquidity < NEAR_MIN_LIQUIDITY_USD:
        return BookFactorResult(
            None,
            REASON_LIQUIDITY_TOO_LOW,
            "near-book liquidity does not satisfy the frozen factor gate",
        )

    return BookFactorResult(
        BookFactors(
            best_bid_text=book.best_bid,
            best_ask_text=book.best_ask,
            best_bid=best_bid,
            best_ask=best_ask,
            mid=mid,
            bid_usd=bid_usd,
            ask_usd=ask_usd,
            liquidity_usd=liquidity,
            near_logdiff_usd=math.log(bid_usd) - math.log(ask_usd),
            rel_spread=(best_ask - best_bid) / mid,
        )
    )


def build_hourly_bars(history: MidPriceHistory, decision_at: datetime) -> list[HourlyBar]:
    """Build right-closed UTC hourly bars without filling missing minutes/hours."""

    buckets: dict[datetime, list[tuple[datetime, float]]] = {}
    for point in history.mid_prices:
        hour_end = _ceil_hour(point.interval_end_at)
        if hour_end > decision_at:
            continue
        buckets.setdefault(hour_end, []).append(
            (point.interval_end_at, float(Decimal(point.p)))
        )

    bars: list[HourlyBar] = []
    segment_id = 0
    previous_end: datetime | None = None
    for hour_end in sorted(buckets):
        if previous_end is None or hour_end - previous_end != timedelta(hours=1):
            segment_id += 1
        points = sorted(buckets[hour_end], key=lambda item: item[0])
        bars.append(
            HourlyBar(
                bar_end_at=hour_end,
                close=points[-1][1],
                segment_id=segment_id,
            )
        )
        previous_end = hour_end
    return bars


def latest_hourly_factors(
    history: MidPriceHistory,
    decision_at: datetime,
    max_age: timedelta = timedelta(hours=1),
) -> tuple[HourlyFactors | None, str]:
    """Return the newest complete-case MOM/MACD row and failure classification.

    The second return value is one of ``ok``, ``stale``, or ``warmup``.
    Missing hours split a segment and restart both indicator warmups.
    """

    bars = build_hourly_bars(history, decision_at)
    if not bars:
        return None, "warmup"
    latest = bars[-1]
    if decision_at - latest.bar_end_at > max_age:
        return None, "stale"

    factors_by_index: dict[int, tuple[float, float]] = {}
    segments: dict[int, list[tuple[int, HourlyBar]]] = {}
    for index, bar in enumerate(bars):
        segments.setdefault(bar.segment_id, []).append((index, bar))
    for segment in segments.values():
        closes = [bar.close for _, bar in segment]
        moms = mom_series(closes)
        macds = macd_signal_talib_seeded(closes)
        for local_index, (global_index, _) in enumerate(segment):
            factors_by_index[global_index] = (moms[local_index], macds[local_index])

    mom, macd = factors_by_index[len(bars) - 1]
    if not (math.isfinite(mom) and math.isfinite(macd)):
        return None, "warmup"
    return HourlyFactors(latest.bar_end_at, mom, macd), "ok"


def mom_series(close: list[float], period: int = 12) -> list[float]:
    result = [float("nan")] * len(close)
    for index in range(period, len(close)):
        current = close[index]
        previous = close[index - period]
        if math.isfinite(current) and math.isfinite(previous) and previous != 0:
            result[index] = current / previous - 1.0
    return result


def macd_signal_talib_seeded(
    close: list[float],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> list[float]:
    """TA-Lib-compatible EMA seeding used by the frozen v2 implementation."""

    result = [float("nan")] * len(close)
    lookback = slow_period + signal_period - 2
    if len(close) <= lookback or any(not math.isfinite(value) for value in close):
        return result

    fast_alpha = 2.0 / (fast_period + 1)
    slow_alpha = 2.0 / (slow_period + 1)
    signal_alpha = 2.0 / (signal_period + 1)
    slow_ema = sum(close[:slow_period]) / slow_period
    fast_ema = sum(close[slow_period - fast_period : slow_period]) / fast_period

    today = slow_period
    current_macd = fast_ema - slow_ema
    signal_seed_sum = current_macd
    for _ in range(signal_period - 1):
        fast_ema = _ema_step(close[today], fast_ema, fast_alpha)
        slow_ema = _ema_step(close[today], slow_ema, slow_alpha)
        current_macd = fast_ema - slow_ema
        signal_seed_sum += current_macd
        today += 1

    signal_ema = signal_seed_sum / signal_period
    result[today - 1] = signal_ema
    while today < len(close):
        fast_ema = _ema_step(close[today], fast_ema, fast_alpha)
        slow_ema = _ema_step(close[today], slow_ema, slow_alpha)
        current_macd = fast_ema - slow_ema
        signal_ema = _ema_step(current_macd, signal_ema, signal_alpha)
        result[today] = signal_ema
        today += 1
    return result


def decimal_text_from_float(value: float) -> str:
    if not math.isfinite(value):
        raise ValueError("cannot encode a non-finite strategy metric")
    if value == 0:
        return "0"
    return format(Decimal(repr(value)), "f")


def _sigmoid(value: float) -> float:
    if value > 500:
        return 1.0
    if value < -500:
        return 0.0
    return 1.0 / (1.0 + math.exp(-value))


def _ceil_hour(value: datetime) -> datetime:
    if value.minute == 0 and value.second == 0 and value.microsecond == 0:
        return value
    return value.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)


def _ema_step(value: float, previous: float, alpha: float) -> float:
    return previous + alpha * (value - previous)
