from __future__ import annotations

import math
from decimal import Decimal

from pm_decision.contract import ExecutionConstraints, OrderBook
from pm_decision.factors import macd_signal_talib_seeded, mom_series
from pm_decision.sizing import entry_size

from conftest import book


def constraints() -> ExecutionConstraints:
    return ExecutionConstraints.model_validate(
        {
            "size_unit": "SHARES",
            "size_decimal_places": 2,
            "buy_notional_decimal_places": 2,
            "minimum_buy_notional": "1",
            "allowed_time_in_force": ["FOK"],
            "price_protection_policy": "EXACT_TOP_OF_BOOK",
        }
    )


def parsed_book(price: str, size: str = "1000") -> OrderBook:
    payload = book(
        "token",
        "market",
        "condition",
        0,
        bid="0.10",
        ask=price,
    )
    payload["asks"][0]["size"] = size
    return OrderBook.model_validate(payload)


def test_talib_seed_boundaries_are_preserved():
    closes = [0.5 + math.sin(index / 5.0) * 0.05 for index in range(50)]
    mom = mom_series(closes)
    macd = macd_signal_talib_seeded(closes)
    assert all(math.isnan(value) for value in mom[:12])
    assert math.isfinite(mom[12])
    assert all(math.isnan(value) for value in macd[:33])
    assert math.isfinite(macd[33])


def test_sizing_satisfies_two_exact_decimal_constraints():
    size = entry_size(
        target_notional=Decimal("5"),
        book=parsed_book("0.53"),
        constraints=constraints(),
    )
    assert size == "9"
    assert Decimal("0.53") * Decimal(size) == Decimal("4.77")

    size = entry_size(
        target_notional=Decimal("10"),
        book=parsed_book("0.1234"),
        constraints=constraints(),
    )
    assert size == "50"
    assert Decimal("0.1234") * Decimal(size) == Decimal("6.1700")


def test_sizing_caps_at_exact_top_level_for_fok():
    size = entry_size(
        target_notional=Decimal("10"),
        book=parsed_book("0.40", size="3.25"),
        constraints=constraints(),
    )
    assert size == "3.25"
    assert Decimal(size) <= Decimal("3.25")
