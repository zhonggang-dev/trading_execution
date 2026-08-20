from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from .contract import (
    BookStatus,
    HistoryStatus,
    REASON_EDGE_TOO_LOW,
    REASON_ENTRY_SIGNAL,
    REASON_FACTOR_WARMUP,
    REASON_HOLD_48H,
    REASON_HOURLY_VETO,
    REASON_INVALID_BOOK,
    REASON_LIQUIDITY_TOO_LOW,
    REASON_OUTSIDE_STRATEGY_UNIVERSE,
    REASON_PRICE_OUT_OF_RANGE,
    REASON_SPREAD_TOO_WIDE,
    REASON_STALE_DATA,
    STRATEGY_V1,
    STRATEGY_V2,
    StrategyRequest,
)
from .factors import (
    BookFactors,
    HourlyFactors,
    compute_book_factors,
    decimal_text_from_float,
    latest_hourly_factors,
)
from .sizing import entry_size, exit_size

WORLD_GEOPOLITICS = "World/Geopolitics"
PREDICTION_MAX_AGE = timedelta(hours=3)
HOLDING_PERIOD = timedelta(hours=48)

V1_EDGE_THRESHOLD = 0.30
V1_NEAR_THRESHOLD = 2.36
V2_EDGE_THRESHOLD = 0.10
V2_NEAR_THRESHOLD = 1.50
REL_SPREAD_MAX = 0.10
ENTRY_ASK_MIN = 0.10
ENTRY_ASK_MAX = 0.99

class DecisionEngine:
    """Pure strategy engine. It has no account, risk, wallet, or venue client."""

    def __init__(
        self,
        *,
        v1_target_notional: Decimal,
        v2_target_notional: Decimal,
    ) -> None:
        if v1_target_notional <= 0 or v2_target_notional <= 0:
            raise ValueError("strategy target notionals must be positive")
        self.target_notional = {
            STRATEGY_V1: v1_target_notional,
            STRATEGY_V2: v2_target_notional,
        }

    def decide(self, request: StrategyRequest, decided_at: datetime) -> dict:
        decided_at = decided_at.astimezone(timezone.utc)
        books = request.book_by_token()
        histories = request.history_by_token()
        factor_cache: dict[str, BookFactors | tuple[str, str]] = {}
        hourly_cache: dict[str, HourlyFactors | tuple[str, str]] = {}

        evaluations: list[dict] = []
        for prediction in request.predictions:
            for outcome in prediction.outcomes:
                token_id = outcome.token_id
                book = books[token_id]
                history = histories[token_id]
                evaluations.append(
                    self._evaluate_entry(
                        request=request,
                        prediction=prediction,
                        outcome=outcome,
                        book=book,
                        history=history,
                        factor_cache=factor_cache,
                        hourly_cache=hourly_cache,
                    )
                )

        exits: list[dict] = []
        for lot in request.positions:
            if request.decision_at - lot.entered_at < HOLDING_PERIOD:
                continue
            book = books[lot.token_id]
            if book.status is not BookStatus.OK or not book.bids or book.best_bid is None:
                continue
            size = exit_size(
                shares=lot.shares,
                book=book,
                constraints=request.execution_constraints,
            )
            if size is None:
                continue
            exits.append(
                {
                    "decision_id": _decision_id(
                        request.context.strategy_id, lot.lot_id, lot.token_id, "exit"
                    ),
                    "lot_id": lot.lot_id,
                    "token_id": lot.token_id,
                    "reason_code": REASON_HOLD_48H,
                    "reason": "lot reached the frozen 48-hour holding period",
                    "order": {
                        "side": "SELL",
                        "type": "LIMIT",
                        "worst_price": book.best_bid,
                        "size": size,
                        "time_in_force": "FOK",
                    },
                }
            )

        return {
            "schema_version": "trading.strategy_output.v4",
            "cycle_id": request.cycle_id,
            "input_id": request.input_id,
            "context": request.context.model_dump(mode="json"),
            "decided_at": _timestamp(decided_at),
            "evaluations": evaluations,
            "exits": exits,
        }

    def _evaluate_entry(
        self,
        *,
        request,
        prediction,
        outcome,
        book,
        history,
        factor_cache,
        hourly_cache,
    ) -> dict:
        strategy_id = request.context.strategy_id
        base = {
            "decision_id": _decision_id(
                strategy_id, prediction.prediction_id, outcome.token_id, "entry"
            ),
            "prediction_id": prediction.prediction_id,
            "market_id": prediction.market_id,
            "condition_id": prediction.condition_id,
            "outcome_index": outcome.index,
            "token_id": outcome.token_id,
        }
        probability = float(outcome.probability)
        evidence: dict = {"probability": probability}

        # These two status mappings are part of the wire contract. For v1 the
        # history is deliberately not consulted; the corresponding Go validator
        # must make the same strategy-specific distinction.
        if book.status is not BookStatus.OK:
            return _skip(base, evidence, REASON_INVALID_BOOK, "orderbook status is not OK")
        if strategy_id == STRATEGY_V2 and history.status is not HistoryStatus.OK:
            return _skip(
                base,
                evidence,
                REASON_STALE_DATA,
                "v2 mid-price history status is not OK",
            )
        if WORLD_GEOPOLITICS not in prediction.domains:
            return _skip(
                base,
                evidence,
                REASON_OUTSIDE_STRATEGY_UNIVERSE,
                "prediction is outside the World/Geopolitics strategy universe",
            )
        if request.decision_at - prediction.prediction_as_of > PREDICTION_MAX_AGE:
            return _skip(
                base,
                evidence,
                REASON_STALE_DATA,
                "prediction is older than the frozen three-hour edge window",
            )

        cached_factor = factor_cache.get(outcome.token_id)
        if cached_factor is None:
            factor_result = compute_book_factors(book)
            if factor_result.factors is None:
                cached_factor = (
                    factor_result.reason_code or REASON_INVALID_BOOK,
                    factor_result.reason or "book factor is unavailable",
                )
            else:
                cached_factor = factor_result.factors
            factor_cache[outcome.token_id] = cached_factor
        if isinstance(cached_factor, tuple):
            return _skip(base, evidence, cached_factor[0], cached_factor[1])
        factors = cached_factor

        edge = probability - factors.best_ask
        common_metrics = {
            "best_ask": factors.best_ask_text,
            "near_logdiff_usd": decimal_text_from_float(factors.near_logdiff_usd),
            "rel_spread": decimal_text_from_float(factors.rel_spread),
        }
        evidence = {
            "probability": probability,
            "edge": decimal_text_from_float(edge),
            "metrics": dict(common_metrics),
        }

        hourly: HourlyFactors | None = None
        if strategy_id == STRATEGY_V2:
            cached_hourly = hourly_cache.get(outcome.token_id)
            if cached_hourly is None:
                latest, status = latest_hourly_factors(history, request.decision_at)
                if latest is None:
                    cached_hourly = (
                        REASON_STALE_DATA if status == "stale" else REASON_FACTOR_WARMUP,
                        "hourly factor is stale"
                        if status == "stale"
                        else "MOM/MACD require a contiguous warm-up segment",
                    )
                else:
                    cached_hourly = latest
                hourly_cache[outcome.token_id] = cached_hourly
            if isinstance(cached_hourly, tuple):
                return _skip(base, evidence, cached_hourly[0], cached_hourly[1])
            hourly = cached_hourly
            evidence["metrics"].update(
                {
                    "MOM": decimal_text_from_float(hourly.mom),
                    "MACD_SIGNAL": decimal_text_from_float(hourly.macd_signal),
                }
            )

        edge_threshold = (
            V1_EDGE_THRESHOLD if strategy_id == STRATEGY_V1 else V2_EDGE_THRESHOLD
        )
        near_threshold = (
            V1_NEAR_THRESHOLD if strategy_id == STRATEGY_V1 else V2_NEAR_THRESHOLD
        )
        if not edge > edge_threshold:
            return _skip(base, evidence, REASON_EDGE_TOO_LOW, "edge did not pass the frozen strict threshold")
        if not factors.near_logdiff_usd >= near_threshold:
            return _skip(
                base,
                evidence,
                REASON_LIQUIDITY_TOO_LOW,
                "near-book imbalance did not pass the frozen threshold",
            )
        if hourly is not None and (hourly.mom > 0 or hourly.macd_signal > 0):
            return _skip(
                base,
                evidence,
                REASON_HOURLY_VETO,
                "v2 requires both MOM and MACD_SIGNAL to be non-positive",
            )
        if not factors.rel_spread < REL_SPREAD_MAX:
            return _skip(base, evidence, REASON_SPREAD_TOO_WIDE, "relative spread is at least 0.10")
        if not ENTRY_ASK_MIN <= factors.best_ask < ENTRY_ASK_MAX:
            return _skip(base, evidence, REASON_PRICE_OUT_OF_RANGE, "best ask is outside [0.10,0.99)")

        size = entry_size(
            target_notional=self.target_notional[strategy_id],
            book=book,
            constraints=request.execution_constraints,
        )
        if size is None:
            return _skip(
                base,
                evidence,
                REASON_LIQUIDITY_TOO_LOW,
                "top level cannot satisfy exact FOK size and notional constraints",
            )
        return {
            **base,
            "action": "SUBMIT",
            "reason_code": REASON_ENTRY_SIGNAL,
            "evidence": evidence,
            "order": {
                "side": "BUY",
                "type": "LIMIT",
                "worst_price": factors.best_ask_text,
                "size": size,
                "time_in_force": "FOK",
            },
        }


def _skip(base: dict, evidence: dict, reason_code: str, reason: str) -> dict:
    return {
        **base,
        "action": "SKIP",
        "reason_code": reason_code,
        "reason": reason,
        "evidence": evidence,
    }


def _decision_id(strategy_id: str, identity: str, token_id: str, kind: str) -> str:
    return f"{strategy_id}:{identity}:{token_id}:{kind}"


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )
