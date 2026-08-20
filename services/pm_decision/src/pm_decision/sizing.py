from __future__ import annotations

import math
from decimal import Decimal, ROUND_DOWN

from .contract import ExecutionConstraints, OrderBook


def entry_size(
    *,
    target_notional: Decimal,
    book: OrderBook,
    constraints: ExecutionConstraints,
) -> str | None:
    """Return the largest safe top-level FOK size under the strategy stake.

    Go requires both at most two share decimals and at most two exact notional
    decimals. For a price such as 0.53, a centi-share quantity does not always
    satisfy the latter; the integer step below solves that constraint exactly
    rather than relying on float rounding.
    """

    if book.best_ask is None or not book.asks or book.min_order_size is None:
        return None
    price = Decimal(book.best_ask)
    top_size = Decimal(book.asks[0].size)
    if price <= 0 or top_size <= 0:
        return None

    max_shares = min(target_notional / price, top_size)
    centi_shares = int(
        (max_shares * Decimal(100)).to_integral_value(rounding=ROUND_DOWN)
    )
    if centi_shares <= 0:
        return None

    price_tuple = price.normalize().as_tuple()
    scale = max(0, -price_tuple.exponent)
    denominator = 10**scale
    numerator = int(price * denominator)
    step = denominator // math.gcd(abs(numerator), denominator)
    centi_shares = (centi_shares // step) * step
    if centi_shares <= 0:
        return None

    size = Decimal(centi_shares) / Decimal(100)
    notional = price * size
    if size < Decimal(book.min_order_size):
        return None
    if notional < Decimal(constraints.minimum_buy_notional):
        return None
    if notional > target_notional:
        return None
    if _decimal_places(size) > constraints.size_decimal_places:
        return None
    if _decimal_places(notional) > constraints.buy_notional_decimal_places:
        return None
    return decimal_text(size)


def exit_size(*, shares: str, book: OrderBook, constraints: ExecutionConstraints) -> str | None:
    if book.min_order_size is None:
        return None
    size = Decimal(shares).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    if size <= 0 or size < Decimal(book.min_order_size):
        return None
    if _decimal_places(size) > constraints.size_decimal_places:
        return None
    return decimal_text(size)


def decimal_text(value: Decimal) -> str:
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered or "0"


def _decimal_places(value: Decimal) -> int:
    normalized = value.normalize()
    return max(0, -normalized.as_tuple().exponent)
