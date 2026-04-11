from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Literal
import uuid

PlatformName = Literal["polymarket", "kalshi"]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class Quote:
    yes_price: Decimal
    no_price: Decimal
    yes_size: Decimal = Decimal("0")
    no_size: Decimal = Decimal("0")
    liquidity: Decimal = Decimal("0")


@dataclass(slots=True)
class Market:
    platform: PlatformName
    external_id: str
    question: str
    end_date: date
    end_time: datetime | None
    fetched_at: datetime
    quote: Quote
    raw_payload: dict = field(default_factory=dict)
    event_key: str | None = None


@dataclass(slots=True)
class MatchedMarkets:
    polymarket: Market
    kalshi: Market
    similarity_key: str
    similarity_score: Decimal


@dataclass(slots=True)
class Opportunity:
    poly_market_id: str
    kalshi_market_id: str
    yes_platform: PlatformName
    no_platform: PlatformName
    yes_price: Decimal
    no_price: Decimal
    gross_gap: Decimal
    net_profit: Decimal
    fee_cost: Decimal
    slippage_cost: Decimal
    liquidity_depth: Decimal
    question: str
    detected_at: datetime = field(default_factory=utc_now)
    executed: bool = False
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass(slots=True)
class DiscoverySample:
    platform: PlatformName
    identifier: str
    question: str
    end_time: datetime | None
    yes_price: Decimal | None
    no_price: Decimal | None
    reason: str
