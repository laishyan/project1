from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import os

from dotenv import load_dotenv

DEFAULT_POLYMARKET_BASE_URL = "https://gamma-api.polymarket.com"
DEFAULT_KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


@dataclass(slots=True)
class Settings:
    sqlite_path: str
    polymarket_base_url: str = DEFAULT_POLYMARKET_BASE_URL
    kalshi_base_url: str = DEFAULT_KALSHI_BASE_URL
    target_market_mode: str = "btc_15m"
    http_timeout_seconds: float = 15.0
    min_days_to_expiry: int = 7
    max_days_to_expiry: int = 49
    max_match_date_drift_days: int = 2
    max_match_time_drift_minutes: int = 20
    profit_threshold: float = 0.0
    min_similarity_score: Decimal = Decimal("0.35")
    scan_limit: int = 200
    discovery_scan_limit: int = 1000
    rediscovery_interval_iterations: int = 15
    enable_auto_fallback: bool = True
    tracked_pair_limit: int = 25

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        return cls(
            sqlite_path=os.getenv("SQLITE_PATH", "./arbitrage_bot.db").strip(),
            polymarket_base_url=os.getenv("POLYMARKET_BASE_URL", DEFAULT_POLYMARKET_BASE_URL),
            kalshi_base_url=os.getenv("KALSHI_BASE_URL", DEFAULT_KALSHI_BASE_URL),
            target_market_mode=os.getenv("TARGET_MARKET_MODE", "btc_15m").strip().lower(),
            http_timeout_seconds=float(os.getenv("HTTP_TIMEOUT_SECONDS", "15")),
            min_days_to_expiry=int(os.getenv("MIN_DAYS_TO_EXPIRY", "7")),
            max_days_to_expiry=int(os.getenv("MAX_DAYS_TO_EXPIRY", "49")),
            max_match_date_drift_days=int(os.getenv("MAX_MATCH_DATE_DRIFT_DAYS", "2")),
            max_match_time_drift_minutes=int(os.getenv("MAX_MATCH_TIME_DRIFT_MINUTES", "20")),
            profit_threshold=float(os.getenv("PROFIT_THRESHOLD", "0")),
            min_similarity_score=Decimal(os.getenv("MIN_SIMILARITY_SCORE", "0.35")),
            scan_limit=int(os.getenv("SCAN_LIMIT", "200")),
            discovery_scan_limit=int(os.getenv("DISCOVERY_SCAN_LIMIT", "1000")),
            rediscovery_interval_iterations=int(os.getenv("REDISCOVERY_INTERVAL_ITERATIONS", "15")),
            enable_auto_fallback=os.getenv("ENABLE_AUTO_FALLBACK", "true").strip().lower() in {"1", "true", "yes", "on"},
            tracked_pair_limit=int(os.getenv("TRACKED_PAIR_LIMIT", "25")),
        )
