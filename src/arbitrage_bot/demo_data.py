from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from .models import Market, Quote, utc_now


def build_demo_markets() -> tuple[list[Market], list[Market]]:
    now = utc_now()
    expiry_dt = now + timedelta(minutes=15)
    expiry = expiry_dt.date()

    polymarket_markets = [
        Market(
            platform="polymarket",
            external_id="poly-btc-15m-1",
            question="Will BTC be above $100,000 in 15 minutes?",
            end_date=expiry,
            end_time=expiry_dt,
            fetched_at=now,
            quote=Quote(
                yes_price=Decimal("0.42"),
                no_price=Decimal("0.58"),
                yes_size=Decimal("1200"),
                no_size=Decimal("1100"),
                liquidity=Decimal("3500"),
            ),
        ),
        Market(
            platform="polymarket",
            external_id="poly-btc-15m-2",
            question="Will BTC stay under $99,500 in 15 minutes?",
            end_date=expiry,
            end_time=expiry_dt,
            fetched_at=now,
            quote=Quote(
                yes_price=Decimal("0.64"),
                no_price=Decimal("0.36"),
                yes_size=Decimal("900"),
                no_size=Decimal("800"),
                liquidity=Decimal("2000"),
            ),
        ),
    ]

    kalshi_markets = [
        Market(
            platform="kalshi",
            external_id="kalshi-btc-15m-1",
            question="BTC above 100000 in 15 min",
            end_date=expiry,
            end_time=expiry_dt,
            fetched_at=now,
            quote=Quote(
                yes_price=Decimal("0.47"),
                no_price=Decimal("0.55"),
                yes_size=Decimal("1300"),
                no_size=Decimal("1400"),
                liquidity=Decimal("4100"),
            ),
        ),
        Market(
            platform="kalshi",
            external_id="kalshi-btc-15m-2",
            question="BTC under 99500 in 15 min",
            end_date=expiry,
            end_time=expiry_dt,
            fetched_at=now,
            quote=Quote(
                yes_price=Decimal("0.70"),
                no_price=Decimal("0.32"),
                yes_size=Decimal("600"),
                no_size=Decimal("650"),
                liquidity=Decimal("1400"),
            ),
        ),
    ]
    return polymarket_markets, kalshi_markets
