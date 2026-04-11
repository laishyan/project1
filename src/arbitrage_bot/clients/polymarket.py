from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import json
import logging

import httpx

from ..models import DiscoverySample, Market, Quote, utc_now


logger = logging.getLogger(__name__)


def _parse_decimal(value: object, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    if isinstance(value, (int, float, str)):
        return Decimal(str(value))
    return Decimal(default)


def _parse_iso_date(raw: str | None) -> datetime:
    if not raw:
        return utc_now()
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)


def _matches_target_market(question: str, target_market_mode: str) -> bool:
    normalized = question.lower()
    if target_market_mode in {"btc_15m", "btc_any"}:
        return "btc" in normalized or "bitcoin" in normalized
    return True


class PolymarketClient:
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
        self.last_discovery_samples = []
        markets: list[Market] = []
        if target_market_mode in {"btc_15m", "btc_any"}:
            offset = 0
            page_size = min(100, limit)
            scanned_events = 0
            try:
                while scanned_events < limit:
                    logger.debug("Requesting Polymarket events with page_size=%s offset=%s", page_size, offset)
                    response = await self._client.get(
                        "/events",
                        params={
                            "active": "true",
                            "closed": "false",
                            "limit": page_size,
                            "offset": offset,
                        },
                    )
                    response.raise_for_status()
                    events = response.json()
                    logger.debug("Polymarket returned %s events on current page", len(events))
                    if not events:
                        break

                    for event in events:
                        scanned_events += 1
                        for market in event.get("markets", []):
                            parsed = self._parse_market(
                                market,
                                event=event,
                                target_market_mode=target_market_mode,
                                min_days_to_expiry=min_days_to_expiry,
                                max_days_to_expiry=max_days_to_expiry,
                            )
                            if parsed is not None:
                                markets.append(parsed)
                            else:
                                sample = self._build_sample_if_relevant(market)
                                if sample is not None and len(self.last_discovery_samples) < 5:
                                    self.last_discovery_samples.append(sample)
                    offset += len(events)
                    if len(events) < page_size:
                        break
            except httpx.HTTPStatusError as exc:
                logger.warning(
                    "Polymarket events discovery failed with %s; falling back to markets endpoint",
                    exc.response.status_code,
                )
                fallback_markets = await self._fetch_markets_fallback(
                    target_market_mode=target_market_mode,
                    min_days_to_expiry=min_days_to_expiry,
                    max_days_to_expiry=max_days_to_expiry,
                    limit=limit,
                )
                markets.extend(fallback_markets)
        elif target_market_mode == "cross_platform_any":
            markets.extend(
                await self._fetch_markets_fallback(
                    target_market_mode=target_market_mode,
                    min_days_to_expiry=min_days_to_expiry,
                    max_days_to_expiry=max_days_to_expiry,
                    limit=limit,
                )
            )
        else:
            markets.extend(
                await self._fetch_markets_fallback(
                    target_market_mode=target_market_mode,
                    min_days_to_expiry=min_days_to_expiry,
                    max_days_to_expiry=max_days_to_expiry,
                    limit=limit,
                )
            )
        logger.debug("Polymarket filtered down to %s markets for target mode %s", len(markets), target_market_mode)
        return markets

    async def _fetch_markets_fallback(
        self,
        *,
        target_market_mode: str,
        min_days_to_expiry: int,
        max_days_to_expiry: int,
        limit: int,
    ) -> list[Market]:
        logger.debug("Requesting Polymarket markets with limit=%s", limit)
        response = await self._client.get(
            "/markets",
            params={
                "active": "true",
                "closed": "false",
                "limit": limit,
            },
        )
        response.raise_for_status()
        page_markets = response.json()
        logger.debug("Polymarket returned %s markets", len(page_markets))
        markets: list[Market] = []
        for market in page_markets:
            parsed = self._parse_market(
                market,
                event=None,
                target_market_mode=target_market_mode,
                min_days_to_expiry=min_days_to_expiry,
                max_days_to_expiry=max_days_to_expiry,
            )
            if parsed is not None:
                markets.append(parsed)
            elif target_market_mode in {"btc_15m", "btc_any"}:
                sample = self._build_sample_if_relevant(market)
                if sample is not None and len(self.last_discovery_samples) < 5:
                    self.last_discovery_samples.append(sample)
        return markets

    async def fetch_market_by_id(
        self,
        market_id: str,
        *,
        target_market_mode: str,
        min_days_to_expiry: int,
        max_days_to_expiry: int,
    ) -> Market | None:
        logger.debug("Refreshing Polymarket market %s", market_id)
        response = await self._client.get(f"/markets/{market_id}")
        response.raise_for_status()
        payload = response.json()
        market = payload.get("market", payload)
        return self._parse_market(
            market,
            event=None,
            target_market_mode=target_market_mode,
            min_days_to_expiry=min_days_to_expiry,
            max_days_to_expiry=max_days_to_expiry,
        )

    def _parse_market(
        self,
        market: dict,
        *,
        event: dict | None,
        target_market_mode: str,
        min_days_to_expiry: int,
        max_days_to_expiry: int,
    ) -> Market | None:
        now_dt = utc_now()
        end_dt = _parse_iso_date(market.get("endDate") or (event or {}).get("endDate"))
        minutes_to_expiry = (end_dt - now_dt).total_seconds() / 60
        days_to_expiry = (end_dt.date() - now_dt.date()).days
        question = market.get("question") or (event or {}).get("title") or market.get("slug", "")
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

        yes_price, no_price = self._extract_outcome_prices(
            market.get("outcomePrices"),
            market.get("outcomes"),
        )
        if yes_price is None or no_price is None:
            return None

        liquidity = _parse_decimal(market.get("liquidity") or market.get("liquidityNum"), "0")
        if not _matches_target_market(question, target_market_mode):
            return None
        return Market(
            platform="polymarket",
            external_id=str(market.get("id")),
            question=question,
            end_date=end_dt.date(),
            end_time=end_dt,
            fetched_at=utc_now(),
            quote=Quote(
                yes_price=yes_price,
                no_price=no_price,
                yes_size=liquidity,
                no_size=liquidity,
                liquidity=liquidity,
            ),
            raw_payload=market,
            event_key=str((event or {}).get("id") or market.get("eventId") or ""),
        )

    def _build_sample_if_relevant(self, market: dict) -> DiscoverySample | None:
        question = str(market.get("question") or market.get("title") or market.get("slug") or "")
        lowered = question.lower()
        if "btc" not in lowered and "bitcoin" not in lowered:
            return None
        end_dt = _parse_iso_date(market.get("endDate"))
        yes_price, no_price = self._extract_outcome_prices(market.get("outcomePrices"), market.get("outcomes"))
        return DiscoverySample(
            platform="polymarket",
            identifier=str(market.get("id")),
            question=question,
            end_time=end_dt,
            yes_price=yes_price,
            no_price=no_price,
            reason="BTC-like Polymarket market rejected by btc_15m filters.",
        )

    @staticmethod
    def _extract_outcome_prices(prices: object, outcomes: object) -> tuple[Decimal | None, Decimal | None]:
        try:
            if isinstance(prices, str):
                prices = json.loads(prices)
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
        except json.JSONDecodeError:
            return None, None

        if not isinstance(prices, list) or not isinstance(outcomes, list):
            return None, None

        mapped = {str(name).strip().upper(): _parse_decimal(price) for name, price in zip(outcomes, prices)}
        if "YES" in mapped and "NO" in mapped:
            return mapped.get("YES"), mapped.get("NO")
        if "UP" in mapped and "DOWN" in mapped:
            return mapped.get("UP"), mapped.get("DOWN")
        if "HIGHER" in mapped and "LOWER" in mapped:
            return mapped.get("HIGHER"), mapped.get("LOWER")
        values = list(mapped.values())
        if len(values) >= 2:
            return values[0], values[1]
        return None, None
