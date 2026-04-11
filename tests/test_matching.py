from datetime import date
from datetime import datetime, timezone
from decimal import Decimal

from arbitrage_bot.matching import match_markets
from arbitrage_bot.models import Market, Quote, utc_now


def build_market(platform: str, external_id: str, question: str) -> Market:
    return Market(
        platform=platform,
        external_id=external_id,
        question=question,
        end_date=date(2026, 4, 15),
        end_time=datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc),
        fetched_at=utc_now(),
        quote=Quote(
            yes_price=Decimal("0.40"),
            no_price=Decimal("0.60"),
            yes_size=Decimal("1000"),
            no_size=Decimal("1000"),
            liquidity=Decimal("1000"),
        ),
    )


def test_match_markets_normalizes_text() -> None:
    matches = match_markets(
        [build_market("polymarket", "p1", "Will BTC hit $100k by April?")],
        [build_market("kalshi", "k1", "Will BTC hit 100k by April")],
        max_date_drift_days=1,
        max_time_drift_minutes=20,
        min_similarity_score=Decimal("0.35"),
    )
    assert len(matches) == 1
