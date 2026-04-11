from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import logging

import httpx

from ..models import DiscoverySample, Market, Quote, utc_now


logger = logging.getLogger(__name__)


def _parse_decimal(value: object, default: str = "0") -> Decimal:
    if value is None or value == "":
        return Decimal(default)
    return Decimal(str(value))


def _parse_iso_date(raw: str | None) -> datetime:
    if not raw:
        return utc_now()
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)


def _matches_target_market(question: str, target_market_mode: str) -> bool:
    normalized = question.lower()
    if target_market_mode in {"btc_15m", "btc_any"}:
        return "btc" in normalized or "bitcoin" in normalized
    return True


def _fallback_yes_ask(item: dict) -> Decimal:
    explicit = _parse_decimal(item.get("yes_ask_dollars"))
    if explicit > 0:
        return explicit
    no_bid = _parse_decimal(item.get("no_bid_dollars"))
    if no_bid > 0:
        return Decimal("1.0") - no_bid
    return Decimal("0")


def _fallback_no_ask(item: dict) -> Decimal:
    explicit = _parse_decimal(item.get("no_ask_dollars"))
    if explicit > 0:
        return explicit
    yes_bid = _parse_decimal(item.get("yes_bid_dollars"))
    if yes_bid > 0:
        return Decimal("1.0") - yes_bid
    return Decimal("0")


class KalshiClient:
    def __init__(self, base_url: str, timeout_seconds: float) -> None:
        self._client = httpx.AsyncClient(base_url=base_url, timeout=timeout_seconds)
        self.last_discovery_samples: list[DiscoverySample] = []

    async def aclose(self) -> None:
        await self._client.aclose()

    async def fetch_markets(
        self,
        *,
        target_market_mode: str,
        min_days_to_expiry: int,
        max_days_to_expiry: int,
        limit: int,
    ) -> list[Market]:
        cursor: str | None = None
        markets: list[Market] = []
        scanned_market_count = 0
        self.last_discovery_samples = []

        while True:
            logger.debug("Requesting Kalshi markets with limit=%s cursor=%s", min(limit, 1000), cursor)
            params = {"status": "open", "limit": min(limit, 1000), "cursor": cursor}
            if target_market_mode in {"btc_15m", "btc_any"}:
                params["series_ticker"] = "KXBTC15M"
            response = await self._client.get(
                "/markets",
                params=params,
            )
            response.raise_for_status()
            payload = response.json()
            page_markets = payload.get("markets", [])
            scanned_market_count += len(page_markets)
            logger.debug(
                "Kalshi returned %s markets on current page (scanned total=%s, matched total=%s)",
                len(page_markets),
                scanned_market_count,
                len(markets),
            )
            for item in page_markets:
                parsed = self._parse_market(
                    item,
                    target_market_mode=target_market_mode,
                    min_days_to_expiry=min_days_to_expiry,
                    max_days_to_expiry=max_days_to_expiry,
                )
                if parsed is not None:
                    markets.append(parsed)
                elif target_market_mode in {"btc_15m", "btc_any"}:
                    sample = self._build_sample_if_relevant(item)
                    if sample is not None and len(self.last_discovery_samples) < 5:
                        self.last_discovery_samples.append(sample)

            if len(markets) >= limit:
                logger.debug("Kalshi reached matched-market limit with %s markets", len(markets[:limit]))
                return markets[:limit]
            if scanned_market_count >= limit:
                logger.debug(
                    "Kalshi reached raw scan limit after inspecting %s markets; returning %s filtered matches",
                    scanned_market_count,
                    len(markets),
                )
                return markets
            cursor = payload.get("cursor")
            if not cursor:
                logger.debug("Kalshi filtered down to %s markets for target mode %s", len(markets), target_market_mode)
                return markets

    async def fetch_market_by_ticker(
        self,
        ticker: str,
        *,
        target_market_mode: str,
        min_days_to_expiry: int,
        max_days_to_expiry: int,
    ) -> Market | None:
        logger.debug("Refreshing Kalshi market %s", ticker)
        response = await self._client.get(f"/markets/{ticker}")
        response.raise_for_status()
        payload = response.json()
        market = payload.get("market", payload)
        return self._parse_market(
            market,
            target_market_mode=target_market_mode,
            min_days_to_expiry=min_days_to_expiry,
            max_days_to_expiry=max_days_to_expiry,
        )

    def _parse_market(
        self,
        item: dict,
        *,
        target_market_mode: str,
        min_days_to_expiry: int,
        max_days_to_expiry: int,
    ) -> Market | None:
        now_dt = utc_now()
        end_dt = _parse_iso_date(item.get("expiration_time") or item.get("close_time"))
        minutes_to_expiry = (end_dt - now_dt).total_seconds() / 60
        days_to_expiry = (end_dt.date() - now_dt.date()).days
        question = str(item.get("title") or item.get("subtitle") or item.get("ticker"))
        if target_market_mode == "btc_15m":
            if not (0 <= minutes_to_expiry <= 30):
                return None
        elif target_market_mode == "btc_any":
            if minutes_to_expiry < 0:
                return None
        elif target_market_mode == "cross_platform_any":
            if minutes_to_expiry < 0:
                return None
        elif not (min_days_to_expiry <= days_to_expiry <= max_days_to_expiry):
            return None

        yes_price = _fallback_yes_ask(item)
        no_price = _fallback_no_ask(item)
        if yes_price <= 0 or no_price <= 0:
            return None
        if not _matches_target_market(question, target_market_mode) and str(item.get("event_ticker", "")).upper() != "KXBTC15M":
            return None

        yes_size = _parse_decimal(item.get("yes_ask_size_fp"))
        no_size = _parse_decimal(item.get("no_ask_size_fp"))
        return Market(
            platform="kalshi",
            external_id=str(item.get("ticker")),
            question=question,
            end_date=end_dt.date(),
            end_time=end_dt,
            fetched_at=utc_now(),
            quote=Quote(
                yes_price=yes_price,
                no_price=no_price,
                yes_size=yes_size,
                no_size=no_size,
                liquidity=_parse_decimal(item.get("liquidity_dollars")),
            ),
            raw_payload=item,
            event_key=str(item.get("event_ticker")),
        )

    def _build_sample_if_relevant(self, item: dict) -> DiscoverySample | None:
        question = str(item.get("title") or item.get("subtitle") or item.get("ticker") or "")
        event_ticker = str(item.get("event_ticker") or "")
        lowered = f"{question} {event_ticker}".lower()
        if "btc" not in lowered and "bitcoin" not in lowered and event_ticker.upper() != "KXBTC15M":
            return None
        end_dt = _parse_iso_date(item.get("expiration_time") or item.get("close_time"))
        yes_price = _fallback_yes_ask(item)
        no_price = _fallback_no_ask(item)
        return DiscoverySample(
            platform="kalshi",
            identifier=str(item.get("ticker")),
            question=question or event_ticker,
            end_time=end_dt,
            yes_price=yes_price if yes_price > 0 else None,
            no_price=no_price if no_price > 0 else None,
            reason="BTC-like Kalshi market rejected by btc_15m filters or missing prices.",
        )
