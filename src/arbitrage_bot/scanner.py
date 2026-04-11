from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal
import logging

from .clients.kalshi import KalshiClient
from .clients.polymarket import PolymarketClient
from .config import Settings
from .fees import FeeModel, ONE_DOLLAR, quantize_money
from .matching import match_markets
from .models import DiscoverySample, Market, MatchedMarkets, Opportunity
from .repository import SQLiteRepository


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ScanResult:
    polymarket_markets: list[Market]
    kalshi_markets: list[Market]
    matched_pairs: list["PairAnalysis"]
    opportunities: list[Opportunity]
    diagnostics: list[str]
    discovery_samples: list[DiscoverySample]
    target_mode_used: str


@dataclass(slots=True)
class PairAnalysis:
    question: str
    poly_market_id: str
    kalshi_market_id: str
    similarity_score: Decimal
    yes_platform: str
    no_platform: str
    yes_price: Decimal
    no_price: Decimal
    gross_gap: Decimal
    fee_cost: Decimal
    slippage_cost: Decimal
    net_profit: Decimal
    liquidity_depth: Decimal
    status: str
    reason: str


class ArbitrageScanner:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._fees = FeeModel()
        self._polymarket = PolymarketClient(settings.polymarket_base_url, settings.http_timeout_seconds)
        self._kalshi = KalshiClient(settings.kalshi_base_url, settings.http_timeout_seconds)
        self._latest_pair_analyses: list[PairAnalysis] = []
        self._tracked_matches: list[MatchedMarkets] = []

    async def aclose(self) -> None:
        await asyncio.gather(self._polymarket.aclose(), self._kalshi.aclose())

    async def run_once(self) -> ScanResult:
        return await self.run_once_with_mode(self._settings.target_market_mode)

    async def run_once_with_mode(self, target_market_mode: str) -> ScanResult:
        logger.debug("Fetching markets from Polymarket and Kalshi")
        polymarket_markets, kalshi_markets = await asyncio.gather(
            self._polymarket.fetch_markets(
                target_market_mode=target_market_mode,
                min_days_to_expiry=self._settings.min_days_to_expiry,
                max_days_to_expiry=self._settings.max_days_to_expiry,
                limit=self._settings.discovery_scan_limit,
            ),
            self._kalshi.fetch_markets(
                target_market_mode=target_market_mode,
                min_days_to_expiry=self._settings.min_days_to_expiry,
                max_days_to_expiry=self._settings.max_days_to_expiry,
                limit=self._settings.discovery_scan_limit,
            ),
        )

        fallback_used = False
        if (
            self._settings.enable_auto_fallback
            and target_market_mode == "btc_15m"
            and (not polymarket_markets or not kalshi_markets)
        ):
            logger.info("Strict btc_15m discovery found no overlap; falling back to btc_any")
            fallback_used = True
            polymarket_markets, kalshi_markets = await asyncio.gather(
                self._polymarket.fetch_markets(
                    target_market_mode="btc_any",
                    min_days_to_expiry=self._settings.min_days_to_expiry,
                    max_days_to_expiry=self._settings.max_days_to_expiry,
                    limit=self._settings.discovery_scan_limit,
                ),
                self._kalshi.fetch_markets(
                    target_market_mode="btc_any",
                    min_days_to_expiry=self._settings.min_days_to_expiry,
                    max_days_to_expiry=self._settings.max_days_to_expiry,
                    limit=self._settings.discovery_scan_limit,
                ),
            )
            target_market_mode = "btc_any"
        if (
            self._settings.enable_auto_fallback
            and target_market_mode == "btc_any"
            and (not polymarket_markets or not kalshi_markets)
        ):
            logger.info("BTC-only fallback still has no overlap; widening to cross_platform_any")
            fallback_used = True
            polymarket_markets, kalshi_markets = await asyncio.gather(
                self._polymarket.fetch_markets(
                    target_market_mode="cross_platform_any",
                    min_days_to_expiry=self._settings.min_days_to_expiry,
                    max_days_to_expiry=self._settings.max_days_to_expiry,
                    limit=self._settings.discovery_scan_limit,
                ),
                self._kalshi.fetch_markets(
                    target_market_mode="cross_platform_any",
                    min_days_to_expiry=self._settings.min_days_to_expiry,
                    max_days_to_expiry=self._settings.max_days_to_expiry,
                    limit=self._settings.discovery_scan_limit,
                ),
            )
            target_market_mode = "cross_platform_any"

        self._tracked_matches = self._select_tracked_matches(self._match_markets(polymarket_markets, kalshi_markets))
        logger.debug(
            "Fetched %s Polymarket markets and %s Kalshi markets",
            len(polymarket_markets),
            len(kalshi_markets),
        )
        opportunities = self.detect_opportunities(polymarket_markets, kalshi_markets)
        return ScanResult(
            polymarket_markets=polymarket_markets,
            kalshi_markets=kalshi_markets,
            matched_pairs=self._latest_pair_analyses,
            opportunities=opportunities,
            diagnostics=self._build_diagnostics(
                polymarket_markets,
                kalshi_markets,
                self._latest_pair_analyses,
                opportunities,
                target_market_mode,
                fallback_used,
            ),
            discovery_samples=self._polymarket.last_discovery_samples + self._kalshi.last_discovery_samples,
            target_mode_used=target_market_mode,
        )

    async def refresh_tracked_pairs(self) -> ScanResult:
        if not self._tracked_matches:
            logger.debug("No tracked matches available; falling back to full discovery scan")
            return await self.run_once()

        logger.debug("Refreshing %s tracked matched pairs", len(self._tracked_matches))
        refreshed_pairs = await asyncio.gather(
            *[self._refresh_match(pair) for pair in self._tracked_matches],
            return_exceptions=True,
        )

        valid_pairs: list[MatchedMarkets] = []
        polymarket_markets: list[Market] = []
        kalshi_markets: list[Market] = []
        for item in refreshed_pairs:
            if isinstance(item, Exception):
                logger.warning("Tracked pair refresh failed: %s", item)
                continue
            if item is None:
                continue
            valid_pairs.append(item)
            polymarket_markets.append(item.polymarket)
            kalshi_markets.append(item.kalshi)

        self._tracked_matches = valid_pairs
        opportunities = self._analyze_matches(valid_pairs)
        return ScanResult(
            polymarket_markets=polymarket_markets,
            kalshi_markets=kalshi_markets,
            matched_pairs=self._latest_pair_analyses,
            opportunities=opportunities,
            diagnostics=self._build_diagnostics(
                polymarket_markets,
                kalshi_markets,
                self._latest_pair_analyses,
                opportunities,
                self._settings.target_market_mode,
                False,
            ),
            discovery_samples=[],
            target_mode_used=self._settings.target_market_mode,
        )

    def detect_opportunities(self, polymarket_markets: list[Market], kalshi_markets: list[Market]) -> list[Opportunity]:
        matches = self._match_markets(polymarket_markets, kalshi_markets)
        return self._analyze_matches(matches)

    def _match_markets(self, polymarket_markets: list[Market], kalshi_markets: list[Market]) -> list[MatchedMarkets]:
        matches = match_markets(
            polymarket_markets,
            kalshi_markets,
            max_date_drift_days=self._settings.max_match_date_drift_days,
            max_time_drift_minutes=self._settings.max_match_time_drift_minutes,
            min_similarity_score=self._settings.min_similarity_score,
        )
        logger.debug("Matched %s cross-platform market pairs", len(matches))
        return matches

    def _select_tracked_matches(self, matches: list[MatchedMarkets]) -> list[MatchedMarkets]:
        ranked = sorted(
            matches,
            key=lambda pair: (
                pair.similarity_score,
                min(pair.polymarket.quote.liquidity, pair.kalshi.quote.liquidity),
            ),
            reverse=True,
        )
        selected = ranked[: self._settings.tracked_pair_limit]
        logger.debug("Tracking top %s matched pairs after ranking", len(selected))
        return selected

    def _analyze_matches(self, matches: list[MatchedMarkets]) -> list[Opportunity]:
        analyses: list[PairAnalysis] = []

        for pair in matches:
            analyses.append(self._analyze_pair(pair.polymarket, pair.kalshi, pair.similarity_score))

        threshold = Decimal(str(self._settings.profit_threshold))
        filtered: list[Opportunity] = []
        for analysis in analyses:
            if analysis.status != "opportunity" or analysis.net_profit <= threshold:
                continue
            filtered.append(
                Opportunity(
                    poly_market_id=analysis.poly_market_id,
                    kalshi_market_id=analysis.kalshi_market_id,
                    yes_platform=analysis.yes_platform,  # type: ignore[arg-type]
                    no_platform=analysis.no_platform,  # type: ignore[arg-type]
                    yes_price=analysis.yes_price,
                    no_price=analysis.no_price,
                    gross_gap=analysis.gross_gap,
                    net_profit=analysis.net_profit,
                    fee_cost=analysis.fee_cost,
                    slippage_cost=analysis.slippage_cost,
                    liquidity_depth=analysis.liquidity_depth,
                    question=analysis.question,
                )
            )

        analyses.sort(key=lambda item: item.net_profit, reverse=True)
        logger.debug("Detected %s profitable opportunities above threshold %s", len(filtered), threshold)
        self._latest_pair_analyses = analyses
        return filtered

    def _build_diagnostics(
        self,
        polymarket_markets: list[Market],
        kalshi_markets: list[Market],
        analyses: list[PairAnalysis],
        opportunities: list[Opportunity],
        target_market_mode: str,
        fallback_used: bool,
    ) -> list[str]:
        diagnostics: list[str] = []
        diagnostics.append(f"Target mode used: {target_market_mode}")
        if fallback_used:
            diagnostics.append("Auto-fallback activated because strict btc_15m had no viable overlap.")
        if not polymarket_markets:
            diagnostics.append(f"No Polymarket candidate markets were discovered for mode {target_market_mode}.")
        if not kalshi_markets:
            diagnostics.append(f"No Kalshi candidate markets were discovered for mode {target_market_mode}.")
        if polymarket_markets and kalshi_markets and not analyses:
            diagnostics.append("Candidate markets were found on both platforms, but none matched strongly enough.")
        near_miss_count = sum(1 for item in analyses if item.status == "near_miss")
        rejected_count = sum(1 for item in analyses if item.status == "rejected")
        if near_miss_count:
            diagnostics.append(f"{near_miss_count} matched pair(s) were close, but fees/slippage erased the edge.")
        if rejected_count:
            diagnostics.append(f"{rejected_count} matched pair(s) were rejected because YES + NO was at or above $1.00.")
        if analyses and not opportunities:
            best = max(analyses, key=lambda item: item.net_profit)
            diagnostics.append(
                f"Best observed net profit was ${best.net_profit} on '{best.question[:60]}'."
            )
        return diagnostics

    async def _refresh_match(self, pair: MatchedMarkets) -> MatchedMarkets | None:
        polymarket, kalshi = await asyncio.gather(
            self._polymarket.fetch_market_by_id(
                pair.polymarket.external_id,
                target_market_mode=self._settings.target_market_mode,
                min_days_to_expiry=self._settings.min_days_to_expiry,
                max_days_to_expiry=self._settings.max_days_to_expiry,
            ),
            self._kalshi.fetch_market_by_ticker(
                pair.kalshi.external_id,
                target_market_mode=self._settings.target_market_mode,
                min_days_to_expiry=self._settings.min_days_to_expiry,
                max_days_to_expiry=self._settings.max_days_to_expiry,
            ),
        )
        if polymarket is None or kalshi is None:
            return None
        return MatchedMarkets(
            polymarket=polymarket,
            kalshi=kalshi,
            similarity_key=pair.similarity_key,
            similarity_score=pair.similarity_score,
        )

    def _build_direction_analysis(self, *, yes_market: Market, no_market: Market, similarity_score: Decimal) -> PairAnalysis:
        yes_price = yes_market.quote.yes_price
        no_price = no_market.quote.no_price
        gross_gap = quantize_money(ONE_DOLLAR - (yes_price + no_price))

        costs = self._fees.estimate_costs(
            yes_price=yes_price,
            no_price=no_price,
            yes_liquidity=yes_market.quote.yes_size or yes_market.quote.liquidity,
            no_liquidity=no_market.quote.no_size or no_market.quote.liquidity,
        )
        liquidity_depth = min(
            yes_market.quote.yes_size or yes_market.quote.liquidity,
            no_market.quote.no_size or no_market.quote.liquidity,
        )
        net_profit = quantize_money(gross_gap - costs.total_cost)
        status = "opportunity"
        reason = "Profitable after fees and slippage."
        if gross_gap <= 0:
            status = "rejected"
            reason = "Combined YES+NO price is at or above $1.00."
        elif net_profit <= 0:
            status = "near_miss"
            reason = "Fees and slippage erased the gross gap."

        return PairAnalysis(
            question=yes_market.question,
            poly_market_id=yes_market.external_id if yes_market.platform == "polymarket" else no_market.external_id,
            kalshi_market_id=yes_market.external_id if yes_market.platform == "kalshi" else no_market.external_id,
            yes_platform=yes_market.platform,
            no_platform=no_market.platform,
            yes_price=yes_price,
            no_price=no_price,
            gross_gap=gross_gap,
            net_profit=net_profit,
            fee_cost=costs.fee_cost,
            slippage_cost=costs.slippage_cost,
            liquidity_depth=liquidity_depth,
            similarity_score=similarity_score,
            status=status,
            reason=reason,
        )

    def _analyze_pair(self, poly_market: Market, kalshi_market: Market, similarity_score: Decimal) -> PairAnalysis:
        candidates = [
            self._build_direction_analysis(
                yes_market=poly_market,
                no_market=kalshi_market,
                similarity_score=similarity_score,
            ),
            self._build_direction_analysis(
                yes_market=kalshi_market,
                no_market=poly_market,
                similarity_score=similarity_score,
            ),
        ]
        best = max(candidates, key=lambda item: item.net_profit)
        logger.debug(
            "Analyzed pair %s/%s similarity=%s best_direction=%s+%s gross=%s net=%s status=%s",
            poly_market.external_id,
            kalshi_market.external_id,
            similarity_score,
            best.yes_platform,
            best.no_platform,
            best.gross_gap,
            best.net_profit,
            best.status,
        )
        return best


async def persist_scan_result(repository: SQLiteRepository, result: ScanResult) -> None:
    logger.debug("Persisting %s markets and %s opportunities", len(result.polymarket_markets) + len(result.kalshi_markets), len(result.opportunities))
    await repository.save_markets(result.polymarket_markets + result.kalshi_markets)
    await repository.save_opportunities(result.opportunities)
