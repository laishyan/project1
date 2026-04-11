from __future__ import annotations

from pathlib import Path
import json
import logging
import sqlite3
from typing import Any

from .models import Market, Opportunity


logger = logging.getLogger(__name__)


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS markets (
    id TEXT PRIMARY KEY,
    platform TEXT NOT NULL,
    external_id TEXT NOT NULL,
    question TEXT NOT NULL,
    end_date TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    yes_price TEXT NOT NULL,
    no_price TEXT NOT NULL,
    yes_size TEXT NOT NULL,
    no_size TEXT NOT NULL,
    liquidity TEXT NOT NULL,
    raw_payload TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_markets_platform_external_id
ON markets (platform, external_id, fetched_at DESC);

CREATE TABLE IF NOT EXISTS opportunities (
    id TEXT PRIMARY KEY,
    detected_at TEXT NOT NULL,
    poly_market_id TEXT NOT NULL,
    kalshi_market_id TEXT NOT NULL,
    yes_platform TEXT NOT NULL,
    no_platform TEXT NOT NULL,
    yes_price TEXT NOT NULL,
    no_price TEXT NOT NULL,
    gross_gap TEXT NOT NULL,
    net_profit TEXT NOT NULL,
    fee_cost TEXT NOT NULL,
    slippage_cost TEXT NOT NULL,
    liquidity_depth TEXT NOT NULL,
    question TEXT NOT NULL,
    executed INTEGER NOT NULL DEFAULT 0
);
"""


class SQLiteRepository:
    def __init__(self, path: str) -> None:
        self._path = path
        self._conn: sqlite3.Connection | None = None

    async def connect(self) -> None:
        if self._path != ":memory:":
            Path(self._path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        logger.debug("Connecting to SQLite database at %s", self._path)
        self._conn = sqlite3.connect(self._path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        await self.ensure_schema()

    async def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    async def ensure_schema(self) -> None:
        if self._conn is None:
            raise RuntimeError("Repository is not connected")
        logger.debug("Ensuring SQLite schema exists")
        self._conn.executescript(SCHEMA_SQL)
        self._conn.commit()

    async def save_markets(self, markets: list[Market]) -> None:
        if not markets or self._conn is None:
            return
        logger.debug("Saving %s markets to SQLite", len(markets))
        rows = [
            (
                f"{market.platform}:{market.external_id}:{market.fetched_at.isoformat()}",
                market.platform,
                market.external_id,
                market.question,
                market.end_date.isoformat(),
                market.fetched_at.isoformat(),
                str(market.quote.yes_price),
                str(market.quote.no_price),
                str(market.quote.yes_size),
                str(market.quote.no_size),
                str(market.quote.liquidity),
                json.dumps(market.raw_payload),
            )
            for market in markets
        ]
        self._conn.executemany(
            """
            INSERT OR REPLACE INTO markets (
                id, platform, external_id, question, end_date, fetched_at,
                yes_price, no_price, yes_size, no_size, liquidity, raw_payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self._conn.commit()

    async def save_opportunities(self, opportunities: list[Opportunity]) -> None:
        if not opportunities or self._conn is None:
            return
        logger.debug("Saving %s opportunities to SQLite", len(opportunities))
        rows = [
            (
                str(opp.id),
                opp.detected_at.isoformat(),
                opp.poly_market_id,
                opp.kalshi_market_id,
                opp.yes_platform,
                opp.no_platform,
                str(opp.yes_price),
                str(opp.no_price),
                str(opp.gross_gap),
                str(opp.net_profit),
                str(opp.fee_cost),
                str(opp.slippage_cost),
                str(opp.liquidity_depth),
                opp.question,
                int(opp.executed),
            )
            for opp in opportunities
        ]
        self._conn.executemany(
            """
            INSERT OR REPLACE INTO opportunities (
                id, detected_at, poly_market_id, kalshi_market_id,
                yes_platform, no_platform, yes_price, no_price,
                gross_gap, net_profit, fee_cost, slippage_cost,
                liquidity_depth, question, executed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self._conn.commit()

    async def count_opportunities(self) -> int:
        if self._conn is None:
            raise RuntimeError("Repository is not connected")
        row = self._conn.execute("SELECT COUNT(*) FROM opportunities").fetchone()
        return int(row[0] if row is not None else 0)

    async def load_recent_opportunities(self, limit: int = 20) -> list[dict[str, Any]]:
        if self._conn is None:
            raise RuntimeError("Repository is not connected")
        cursor = self._conn.execute(
            """
            SELECT detected_at, question, yes_platform, no_platform, net_profit,
                   gross_gap, fee_cost, slippage_cost, poly_market_id, kalshi_market_id
            FROM opportunities
            ORDER BY detected_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        columns = [item[0] for item in cursor.description or []]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
