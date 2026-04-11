from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP


CENT = Decimal("0.0001")
ONE_DOLLAR = Decimal("1.00")


def quantize_money(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


@dataclass(slots=True)
class CostBreakdown:
    fee_cost: Decimal
    slippage_cost: Decimal

    @property
    def total_cost(self) -> Decimal:
        return self.fee_cost + self.slippage_cost


class FeeModel:
    """Conservative scanner-grade fee/slippage estimates."""

    polymarket_taker_rate = Decimal("0.02")
    kalshi_fee_rate = Decimal("0.035")

    def estimate_costs(
        self,
        *,
        yes_price: Decimal,
        no_price: Decimal,
        yes_liquidity: Decimal,
        no_liquidity: Decimal,
    ) -> CostBreakdown:
        polymarket_fee = yes_price * self.polymarket_taker_rate
        kalshi_fee = max(ONE_DOLLAR - no_price, Decimal("0")) * self.kalshi_fee_rate

        min_depth = min(yes_liquidity, no_liquidity)
        slippage = Decimal("0")
        if min_depth < Decimal("100"):
            slippage = Decimal("0.0100")
        elif min_depth < Decimal("500"):
            slippage = Decimal("0.0040")
        elif min_depth < Decimal("1000"):
            slippage = Decimal("0.0015")

        return CostBreakdown(
            fee_cost=quantize_money(polymarket_fee + kalshi_fee),
            slippage_cost=quantize_money(slippage),
        )
