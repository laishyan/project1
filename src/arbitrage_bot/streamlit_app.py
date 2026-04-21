from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import sys
import threading
import time
from traceback import format_exc
from typing import Any

import httpx

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from arbitrage_bot.config import Settings
    from arbitrage_bot.demo_data import build_demo_markets
    from arbitrage_bot.repository import SQLiteRepository
    from arbitrage_bot.scanner import ArbitrageScanner, PairAnalysis, ScanResult, persist_scan_result
else:
    from .config import Settings
    from .demo_data import build_demo_markets
    from .repository import SQLiteRepository
    from .scanner import ArbitrageScanner, PairAnalysis, ScanResult, persist_scan_result


def opportunity_signature(opportunity) -> tuple[str, str, str, str, str, str]:
    return (
        opportunity.poly_market_id,
        opportunity.kalshi_market_id,
        opportunity.yes_platform,
        opportunity.no_platform,
        str(opportunity.yes_price),
        str(opportunity.no_price),
    )


async def _scan_once(*, demo: bool, skip_db: bool) -> ScanResult:
    settings = Settings.from_env()
    scanner = ArbitrageScanner(settings)
    repository: SQLiteRepository | None = None
    try:
        if demo:
            polymarket_markets, kalshi_markets = build_demo_markets()
            opportunities = scanner.detect_opportunities(polymarket_markets, kalshi_markets)
            return ScanResult(
                polymarket_markets=polymarket_markets,
                kalshi_markets=kalshi_markets,
                matched_pairs=scanner._latest_pair_analyses,
                opportunities=opportunities,
                diagnostics=scanner._build_diagnostics(
                    polymarket_markets,
                    kalshi_markets,
                    scanner._latest_pair_analyses,
                    opportunities,
                    "demo",
                    False,
                ),
                discovery_samples=[],
                target_mode_used="demo",
            )

        result = await scanner.run_once()
        if not skip_db and settings.sqlite_path:
            repository = SQLiteRepository(settings.sqlite_path)
            await repository.connect()
            await persist_scan_result(repository, result)
        return result
    finally:
        if repository is not None:
            await repository.close()
        await scanner.aclose()


async def _load_history(limit: int) -> list[dict[str, Any]]:
    settings = Settings.from_env()
    if not settings.sqlite_path:
        return []

    repository = SQLiteRepository(settings.sqlite_path)
    await repository.connect()
    try:
        return await repository.load_recent_opportunities(limit=limit)
    finally:
        await repository.close()


async def _measure_api_pings(settings: Settings, *, demo: bool) -> dict[str, int | None]:
    if demo:
        return {"polymarket": 0, "kalshi": 0}

    async def ping(url: str) -> int | None:
        started = time.perf_counter()
        try:
            async with httpx.AsyncClient(timeout=3.0, follow_redirects=True) as client:
                await client.get(url)
        except Exception:
            return None
        return int((time.perf_counter() - started) * 1000)

    polymarket_ms, kalshi_ms = await asyncio.gather(
        ping(settings.polymarket_base_url),
        ping(settings.kalshi_base_url),
    )
    return {"polymarket": polymarket_ms, "kalshi": kalshi_ms}


def _format_money(value: Decimal | float | int | str) -> str:
    return f"${value}"


def _format_timestamp(value) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {secs}s"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _extract_scan_time(result: ScanResult | None) -> str:
    if result is None:
        return "-"
    timestamps = [market.fetched_at for market in result.polymarket_markets + result.kalshi_markets]
    timestamps.extend(opportunity.detected_at for opportunity in result.opportunities)
    if not timestamps:
        return "-"
    return _format_timestamp(max(timestamps))


def _market_map(result: ScanResult | None) -> dict[tuple[str, str], Any]:
    markets = {}
    if result is None:
        return markets
    for market in result.polymarket_markets + result.kalshi_markets:
        markets[(market.platform, market.external_id)] = market
    return markets


def _to_json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _to_json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_json_safe(item) for item in value]
    return value


def _running_in_streamlit() -> bool:
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
    except Exception:
        return False
    return get_script_run_ctx() is not None


def _pair_history_key(pair: PairAnalysis) -> str:
    return f"{pair.poly_market_id}::{pair.kalshi_market_id}"


def _average_price(markets: list[Any], side: str) -> float:
    if not markets:
        return 0.0
    return sum(float(getattr(market.quote, side)) for market in markets) / len(markets)


def _average_depth(markets: list[Any]) -> float:
    if not markets:
        return 0.0
    return sum(float(market.quote.liquidity) for market in markets) / len(markets)


def _confidence_score(pair: PairAnalysis) -> int:
    liquidity = min(float(pair.liquidity_depth), 1000.0) / 1000.0
    edge = min(max(float(pair.net_profit), 0.0), 0.1) / 0.1
    similarity = min(max(float(pair.similarity_score), 0.0), 1.0)
    return round((liquidity * 35) + (edge * 35) + (similarity * 30))


def _countdown_for_pair(pair: PairAnalysis, market_lookup: dict[tuple[str, str], Any]) -> str:
    candidates = [
        market_lookup.get(("polymarket", pair.poly_market_id)),
        market_lookup.get(("kalshi", pair.kalshi_market_id)),
    ]
    end_times = [market.end_time for market in candidates if market is not None and market.end_time is not None]
    if not end_times:
        return "-"
    return _format_duration((min(end_times) - datetime.now(timezone.utc)).total_seconds())


def _safe_dataframe(rows: list[dict[str, Any]]):
    import pandas as pd

    return pd.DataFrame(rows) if rows else pd.DataFrame()


@dataclass(slots=True)
class LiveSnapshot:
    result: ScanResult | None = None
    history_rows: list[dict[str, Any]] = field(default_factory=list)
    price_history: dict[str, dict[str, Any]] = field(default_factory=dict)
    market_overview_history: list[dict[str, Any]] = field(default_factory=list)
    logs: list[str] = field(default_factory=list)
    pings_ms: dict[str, int | None] = field(default_factory=lambda: {"polymarket": None, "kalshi": None})
    running: bool = False
    status: str = "idle"
    error: str = ""
    scan_count: int = 0
    unique_opportunities: int = 0
    last_new_opportunities: int = 0
    highest_spread_seen: float = 0.0
    last_scan_ms: int | None = None
    last_scan_wallclock: float | None = None
    started_at: float = field(default_factory=time.time)


class LiveScannerController:
    def __init__(self, *, demo: bool, skip_db: bool, interval: float, history_limit: int) -> None:
        self.demo = demo
        self.skip_db = skip_db
        self.interval = interval
        self.history_limit = history_limit
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._result: ScanResult | None = None
        self._history_rows: list[dict[str, Any]] = []
        self._price_history: dict[str, dict[str, Any]] = {}
        self._market_overview_history: list[dict[str, Any]] = []
        self._logs: list[str] = []
        self._pings_ms: dict[str, int | None] = {"polymarket": None, "kalshi": None}
        self._seen_signatures: set[tuple[str, str, str, str, str, str]] = set()
        self._running = False
        self._status = "idle"
        self._error = ""
        self._scan_count = 0
        self._last_new_opportunities = 0
        self._highest_spread_seen = 0.0
        self._last_scan_ms: int | None = None
        self._last_scan_wallclock: float | None = None
        self._started_at = time.time()

    @property
    def signature(self) -> tuple[bool, bool, float, int]:
        return (self.demo, self.skip_db, self.interval, self.history_limit)

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="streamlit-arb-scanner", daemon=True)
        self._thread.start()
        self._append_log("Live scanner thread started.")

    def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        self._append_log("Stop requested.")

    def force_scan(self) -> None:
        self._append_log("Manual scan requested.")
        self._wake_event.set()

    def snapshot(self) -> LiveSnapshot:
        with self._lock:
            return LiveSnapshot(
                result=self._result,
                history_rows=list(self._history_rows),
                price_history={
                    key: {"question": value["question"], "points": list(value["points"])}
                    for key, value in self._price_history.items()
                },
                market_overview_history=list(self._market_overview_history),
                logs=list(self._logs),
                pings_ms=dict(self._pings_ms),
                running=self._running,
                status=self._status,
                error=self._error,
                scan_count=self._scan_count,
                unique_opportunities=len(self._seen_signatures),
                last_new_opportunities=self._last_new_opportunities,
                highest_spread_seen=self._highest_spread_seen,
                last_scan_ms=self._last_scan_ms,
                last_scan_wallclock=self._last_scan_wallclock,
                started_at=self._started_at,
            )

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            self._run_scan_cycle()
            self._wake_event.wait(self.interval)
            self._wake_event.clear()

        with self._lock:
            self._running = False
            self._status = "stopped"

    def _run_scan_cycle(self) -> None:
        with self._lock:
            self._running = True
            self._status = "scanning"
            self._error = ""

        settings = Settings.from_env()
        started = time.perf_counter()
        try:
            result = asyncio.run(_scan_once(demo=self.demo, skip_db=self.skip_db))
            pings_ms = asyncio.run(_measure_api_pings(settings, demo=self.demo))
            history_rows = asyncio.run(_load_history(limit=self.history_limit))
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            self._apply_result(result=result, history_rows=history_rows, pings_ms=pings_ms, elapsed_ms=elapsed_ms)
        except Exception as exc:
            with self._lock:
                self._running = False
                self._status = "error"
                self._error = f"{type(exc).__name__}: {exc}"
                self._logs.append(format_exc())
                self._logs = self._logs[-160:]

    def _apply_result(
        self,
        *,
        result: ScanResult,
        history_rows: list[dict[str, Any]],
        pings_ms: dict[str, int | None],
        elapsed_ms: int,
    ) -> None:
        new_unique = 0
        for opportunity in result.opportunities:
            signature = opportunity_signature(opportunity)
            if signature not in self._seen_signatures:
                self._seen_signatures.add(signature)
                new_unique += 1

        highest_spread = max([float(pair.net_profit) for pair in result.matched_pairs] + [self._highest_spread_seen, 0.0])
        with self._lock:
            self._result = result
            self._history_rows = history_rows
            self._pings_ms = pings_ms
            self._scan_count += 1
            self._last_new_opportunities = new_unique
            self._highest_spread_seen = highest_spread
            self._last_scan_ms = elapsed_ms
            self._last_scan_wallclock = time.time()
            self._running = False
            self._status = "live"
            self._error = ""
            self._record_price_history_locked(result)
            self._record_market_overview_history_locked(result)
            self._append_log_locked(
                f"Scan #{self._scan_count}: {len(result.polymarket_markets)} Polymarket, "
                f"{len(result.kalshi_markets)} Kalshi, {len(result.matched_pairs)} pairs, "
                f"{len(result.opportunities)} opportunities in {elapsed_ms} ms."
            )
            if new_unique:
                self._append_log_locked(f"Opportunity alert: {new_unique} new unique profitable opportunity(s).")

    def _append_log(self, message: str) -> None:
        with self._lock:
            self._append_log_locked(message)

    def _append_log_locked(self, message: str) -> None:
        timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        self._logs.append(f"[{timestamp}] {message}")
        self._logs = self._logs[-160:]

    def _record_price_history_locked(self, result: ScanResult, max_points: int = 180) -> None:
        market_lookup = _market_map(result)
        for pair in result.matched_pairs:
            poly_market = market_lookup.get(("polymarket", pair.poly_market_id))
            kalshi_market = market_lookup.get(("kalshi", pair.kalshi_market_id))
            if poly_market is None or kalshi_market is None:
                continue
            timestamp = max(poly_market.fetched_at, kalshi_market.fetched_at)
            key = _pair_history_key(pair)
            points = self._price_history.setdefault(key, {"question": pair.question, "points": []})["points"]
            points.append(
                {
                    "timestamp": timestamp,
                    "polymarket_yes": float(poly_market.quote.yes_price),
                    "polymarket_no": float(poly_market.quote.no_price),
                    "kalshi_yes": float(kalshi_market.quote.yes_price),
                    "kalshi_no": float(kalshi_market.quote.no_price),
                    "arb_total": float(pair.yes_price + pair.no_price),
                    "net_profit": float(pair.net_profit),
                    "spread": float(pair.gross_gap),
                    "confidence": _confidence_score(pair),
                }
            )
            if len(points) > max_points:
                del points[:-max_points]

    def _record_market_overview_history_locked(self, result: ScanResult, max_points: int = 240) -> None:
        timestamps = [market.fetched_at for market in result.polymarket_markets + result.kalshi_markets]
        if not timestamps:
            return
        self._market_overview_history.append(
            {
                "timestamp": max(timestamps),
                "polymarket_count": len(result.polymarket_markets),
                "kalshi_count": len(result.kalshi_markets),
                "polymarket_yes_avg": _average_price(result.polymarket_markets, "yes_price"),
                "kalshi_yes_avg": _average_price(result.kalshi_markets, "yes_price"),
                "polymarket_no_avg": _average_price(result.polymarket_markets, "no_price"),
                "kalshi_no_avg": _average_price(result.kalshi_markets, "no_price"),
                "polymarket_depth_avg": _average_depth(result.polymarket_markets),
                "kalshi_depth_avg": _average_depth(result.kalshi_markets),
                "matched_pairs": len(result.matched_pairs),
                "opportunities": len(result.opportunities),
            }
        )
        if len(self._market_overview_history) > max_points:
            del self._market_overview_history[:-max_points]


def _chart_rows(points: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for point in points:
        timestamp = point["timestamp"].astimezone(timezone.utc)
        rows.extend(
            [
                {"Time": timestamp, "Series": "Polymarket YES", "Value": point["polymarket_yes"]},
                {"Time": timestamp, "Series": "Polymarket NO", "Value": point["polymarket_no"]},
                {"Time": timestamp, "Series": "Kalshi YES", "Value": point["kalshi_yes"]},
                {"Time": timestamp, "Series": "Kalshi NO", "Value": point["kalshi_no"]},
            ]
        )
    return rows


def _spread_rows(points: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for point in points:
        timestamp = point["timestamp"].astimezone(timezone.utc)
        rows.extend(
            [
                {"Time": timestamp, "Metric": "YES+NO total", "Value": point["arb_total"]},
                {"Time": timestamp, "Metric": "Net profit", "Value": point["net_profit"]},
                {"Time": timestamp, "Metric": "Gross spread", "Value": point["spread"]},
            ]
        )
    return rows


def _profit_zone_rows(points: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "Time": point["timestamp"].astimezone(timezone.utc),
            "Profit Zone": max(0.0, 1.0 - point["arb_total"]),
        }
        for point in points
    ]


def _overview_price_rows(points: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for point in points:
        timestamp = point["timestamp"].astimezone(timezone.utc)
        rows.extend(
            [
                {"Time": timestamp, "Series": "Polymarket avg YES", "Value": point["polymarket_yes_avg"]},
                {"Time": timestamp, "Series": "Kalshi avg YES", "Value": point["kalshi_yes_avg"]},
                {"Time": timestamp, "Series": "Polymarket avg NO", "Value": point["polymarket_no_avg"]},
                {"Time": timestamp, "Series": "Kalshi avg NO", "Value": point["kalshi_no_avg"]},
            ]
        )
    return rows


def _overview_activity_rows(points: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for point in points:
        timestamp = point["timestamp"].astimezone(timezone.utc)
        rows.extend(
            [
                {"Time": timestamp, "Series": "Polymarket markets", "Value": point["polymarket_count"]},
                {"Time": timestamp, "Series": "Kalshi markets", "Value": point["kalshi_count"]},
                {"Time": timestamp, "Series": "Matched pairs", "Value": point["matched_pairs"]},
                {"Time": timestamp, "Series": "Opportunities", "Value": point["opportunities"]},
            ]
        )
    return rows


def _opportunity_rows(result: ScanResult | None, *, trade_size: int) -> list[dict[str, Any]]:
    if result is None:
        return []
    rows: list[dict[str, Any]] = []
    for opportunity in sorted(result.opportunities, key=lambda item: item.net_profit, reverse=True):
        rows.append(
            {
                "Question": opportunity.question,
                "Trade": f"YES {opportunity.yes_platform.upper()} / NO {opportunity.no_platform.upper()}",
                "Gross": float(opportunity.gross_gap),
                "Net / pair": float(opportunity.net_profit),
                "Estimated take-home": float(opportunity.net_profit) * trade_size,
                "Fees": float(opportunity.fee_cost) * trade_size,
                "Slippage": float(opportunity.slippage_cost) * trade_size,
                "Depth": float(opportunity.liquidity_depth),
                "Detected": _format_timestamp(opportunity.detected_at),
            }
        )
    return rows


def _pair_rows(result: ScanResult | None) -> list[dict[str, Any]]:
    if result is None:
        return []
    market_lookup = _market_map(result)
    rows: list[dict[str, Any]] = []
    for pair in result.matched_pairs:
        rows.append(
            {
                "Question": pair.question,
                "Similarity": float(pair.similarity_score),
                "Direction": f"YES {pair.yes_platform.upper()} / NO {pair.no_platform.upper()}",
                "Gross": float(pair.gross_gap),
                "Net": float(pair.net_profit),
                "Depth": float(pair.liquidity_depth),
                "Confidence": _confidence_score(pair),
                "Expiry": _countdown_for_pair(pair, market_lookup),
                "Status": pair.status,
                "Why": pair.reason,
            }
        )
    return rows


def _discovery_rows(result: ScanResult | None) -> list[dict[str, str]]:
    if result is None:
        return []
    rows: list[dict[str, str]] = []
    for sample in result.discovery_samples:
        rows.append(
            {
                "Platform": sample.platform.upper(),
                "Question": sample.question,
                "End": _format_timestamp(sample.end_time) if sample.end_time is not None else "-",
                "YES": _format_money(sample.yes_price) if sample.yes_price is not None else "-",
                "NO": _format_money(sample.no_price) if sample.no_price is not None else "-",
                "Why no match": sample.reason,
            }
        )
    return rows


def _history_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "Detected at": str(row["detected_at"]),
            "Question": str(row["question"]),
            "Trade": f"YES {str(row['yes_platform']).upper()} / NO {str(row['no_platform']).upper()}",
            "Net profit": float(row["net_profit"]),
            "Gross gap": float(row["gross_gap"]),
            "Fees": float(row["fee_cost"]),
            "Slippage": float(row["slippage_cost"]),
        }
        for row in rows
    ]


def _history_json(rows: list[dict[str, Any]]) -> str:
    return json.dumps(_to_json_safe(rows), indent=2)


def _sparkline_svg(values: list[float], *, color: str = "#38bdf8", fill: str = "rgba(56, 189, 248, .16)") -> str:
    if not values:
        values = [0, 0]
    if len(values) == 1:
        values = [values[0], values[0]]
    width = 160
    height = 46
    low = min(values)
    high = max(values)
    span = high - low or 1
    points = []
    for index, value in enumerate(values[-18:]):
        x = index * (width / max(1, min(len(values), 18) - 1))
        y = height - ((value - low) / span * (height - 8)) - 4
        points.append(f"{x:.1f},{y:.1f}")
    area = f"0,{height} " + " ".join(points) + f" {width},{height}"
    return (
        f'<svg viewBox="0 0 {width} {height}" preserveAspectRatio="none" class="sparkline">'
        f'<polygon points="{area}" fill="{fill}"></polygon>'
        f'<polyline points="{" ".join(points)}" fill="none" stroke="{color}" stroke-width="3" '
        f'stroke-linecap="round" stroke-linejoin="round"></polyline></svg>'
    )


def _latest_overview_values(snapshot: LiveSnapshot, key: str) -> list[float]:
    return [float(point.get(key, 0.0)) for point in snapshot.market_overview_history[-18:]]


def _kpi_card(title: str, value: str, *, subtitle: str = "", badge: str = "Active", spark_values: list[float] | None = None, accent: str = "#38bdf8") -> str:
    sparkline = _sparkline_svg(spark_values or [], color=accent)
    subtitle_html = f'<div class="kpi-subtitle">{subtitle}</div>' if subtitle else ""
    return (
        '<div class="command-card kpi-card">'
        '<div class="kpi-head">'
        f'<span>{title}</span><span class="status-badge">{badge}</span>'
        '</div>'
        f'<div class="kpi-body"><div><div class="kpi-value">{value}</div>{subtitle_html}</div>{sparkline}</div>'
        '</div>'
    )


def _render_nav_shell() -> None:
    import streamlit as st

    st.markdown(
        """
        <div class="nav-brand">
            <div class="nav-icon">⌁</div>
            <div>
                <div class="nav-title">Arb-Bot Command.</div>
                <div class="nav-subtitle">Prediction Market Desk</div>
            </div>
        </div>
        <div class="nav-item active">▦ Dashboard</div>
        <div class="nav-item">⌁ Live Markets</div>
        <div class="nav-item">↺ Trade History</div>
        <div class="nav-item">⚡ Diagnostics</div>
        <div class="nav-item">⌁ Backtesting</div>
        <div class="nav-item">⚙ Parameters</div>
        """,
        unsafe_allow_html=True,
    )


def _inject_styles() -> None:
    import streamlit as st

    st.markdown(
        """
        <style>
        :root {
            --midnight-bg: #07111f;
            --midnight-panel: #0d1b2f;
            --midnight-panel-2: #10243b;
            --midnight-border: #1e3a5f;
            --profit: #34d399;
            --warning: #fbbf24;
            --danger: #fb7185;
            --text: #dbeafe;
        }
        html, body, [data-testid="stAppViewContainer"], .stApp {
            background: #111522;
            color: var(--text);
        }
        .block-container {
            max-width: 100%;
            padding: 0.45rem 1rem 1rem 1rem;
        }
        header[data-testid="stHeader"] {
            display: none;
        }
        section[data-testid="stSidebar"] {
            background: linear-gradient(180deg, #171c2b 0%, #111827 100%);
            border-right: 1px solid #293044;
            min-width: 196px !important;
            width: 196px !important;
        }
        section[data-testid="stSidebar"] > div {
            padding-top: .35rem;
        }
        div[data-testid="stMetric"] {
            background: linear-gradient(180deg, #202637 0%, #151b2a 100%);
            border: 1px solid #2b3348;
            border-radius: 8px;
            padding: 10px 12px;
            box-shadow: 0 8px 24px rgba(0,0,0,.22);
        }
        .nav-brand {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 8px 4px 16px;
            border-bottom: 1px solid #2a3144;
            margin-bottom: 12px;
        }
        .nav-icon {
            width: 28px;
            height: 28px;
            display: grid;
            place-items: center;
            border-radius: 8px;
            color: #aeb8cc;
            background: #101624;
        }
        .nav-title {
            font-size: 17px;
            font-weight: 800;
            color: #f4f7fb;
        }
        .nav-subtitle {
            font-size: 11px;
            color: #7d879b;
        }
        .nav-item {
            padding: 10px 12px;
            border-radius: 5px;
            margin: 3px 0;
            color: #b8c0d2;
            font-size: 14px;
        }
        .nav-item.active {
            background: #303746;
            color: #fff;
        }
        .command-topbar {
            height: 42px;
            margin: -7px -16px 12px -16px;
            padding: 0 16px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            background: #161b2a;
            border-bottom: 1px solid #293044;
            box-shadow: 0 10px 30px rgba(0,0,0,.18);
        }
        .topbar-title {
            display: flex;
            align-items: center;
            gap: 10px;
            font-weight: 800;
            font-size: 17px;
            color: #f7f9fe;
        }
        .topbar-right {
            display: flex;
            align-items: center;
            gap: 14px;
            color: #d7deec;
            font-size: 13px;
        }
        .command-card {
            background: linear-gradient(180deg, #202637 0%, #161c2b 100%);
            border: 1px solid #2b3348;
            border-radius: 7px;
            box-shadow: 0 12px 28px rgba(0,0,0,.23);
        }
        .kpi-card {
            padding: 10px 12px;
            min-height: 99px;
        }
        .kpi-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
            color: #f5f7fb;
            font-size: 14px;
            margin-bottom: 6px;
        }
        .status-badge {
            color: #f6f8fb;
            background: #586070;
            border-radius: 4px;
            padding: 2px 6px;
            font-size: 11px;
        }
        .kpi-body {
            display: flex;
            align-items: end;
            justify-content: space-between;
            gap: 10px;
        }
        .kpi-value {
            font-size: 38px;
            line-height: .95;
            font-weight: 900;
            color: #ffffff;
            letter-spacing: -1px;
        }
        .kpi-subtitle {
            color: #aab3c5;
            font-size: 12px;
            margin-top: 5px;
        }
        .sparkline {
            width: 120px;
            height: 42px;
            opacity: .95;
        }
        .live-pill {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 4px 9px;
            border-radius: 999px;
            background: rgba(52, 211, 153, .12);
            border: 1px solid rgba(52, 211, 153, .38);
            color: #bbf7d0;
            font-weight: 700;
            letter-spacing: .04em;
            text-transform: uppercase;
        }
        .live-dot {
            width: 9px;
            height: 9px;
            border-radius: 999px;
            background: var(--profit);
            box-shadow: 0 0 0 rgba(52, 211, 153, .7);
            animation: pulse 1.35s infinite;
        }
        @keyframes pulse {
            0% { box-shadow: 0 0 0 0 rgba(52, 211, 153, .68); }
            70% { box-shadow: 0 0 0 10px rgba(52, 211, 153, 0); }
            100% { box-shadow: 0 0 0 0 rgba(52, 211, 153, 0); }
        }
        .terminal-feed {
            background: #070a10;
            border: 1px solid #242a3c;
            border-radius: 7px;
            color: #b7f7d2;
            font-family: Consolas, "Courier New", monospace;
            font-size: 12px;
            max-height: 260px;
            overflow-y: auto;
            padding: 14px;
            white-space: pre-wrap;
        }
        .chart-shell {
            background: linear-gradient(180deg, #252b3b 0%, #181e2d 100%);
            border: 1px solid #2d3549;
            border-radius: 7px;
            padding: 10px 12px;
            min-height: 210px;
        }
        .section-label {
            font-size: 16px;
            color: #fff;
            margin: 5px 0 8px;
            font-weight: 700;
        }
        .stDataFrame {
            border: 1px solid #293044;
            border-radius: 7px;
            overflow: hidden;
        }
        div[data-testid="stTabs"] button {
            color: #c5cede;
        }
        .stButton > button {
            border-radius: 5px;
            border: 1px solid #3a4359;
            background: #1d2434;
            color: #f4f7fb;
        }
        .stButton > button[kind="primary"] {
            background: #df3b3e;
            border-color: #ef5154;
        }
        .kbd {
            border: 1px solid #4b6587;
            border-bottom-width: 2px;
            border-radius: 6px;
            padding: 1px 7px;
            background: #13243a;
            color: #bfdbfe;
            font-size: 12px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _get_controller(*, demo: bool, skip_db: bool, interval: float, history_limit: int) -> LiveScannerController:
    import streamlit as st

    signature = (demo, skip_db, interval, history_limit)
    controller = st.session_state.get("live_controller")
    if controller is None or controller.signature != signature:
        if controller is not None:
            controller.stop()
        controller = LiveScannerController(demo=demo, skip_db=skip_db, interval=interval, history_limit=history_limit)
        st.session_state.live_controller = controller
    return controller


def _render_top_bar(snapshot: LiveSnapshot) -> None:
    import streamlit as st

    status_text = "LIVE" if snapshot.status in {"live", "scanning"} else snapshot.status.upper()
    poly_ping = f"{snapshot.pings_ms.get('polymarket')}ms" if snapshot.pings_ms.get("polymarket") is not None else "timeout"
    kalshi_ping = f"{snapshot.pings_ms.get('kalshi')}ms" if snapshot.pings_ms.get("kalshi") is not None else "timeout"
    uptime = _format_duration(time.time() - snapshot.started_at)
    st.markdown(
        f"""
        <div class="command-topbar">
            <div class="topbar-title"><span style="color:#8b94a7">⌁</span> Arb-Bot Command.</div>
            <div class="topbar-right">
                <span class="live-pill"><span class="live-dot"></span>{status_text} Scan</span>
                <span>API Latency: <span style="color:#61e68b">Poly {poly_ping}</span> / <span style="color:#42b6e8">Kalshi {kalshi_ping}</span></span>
                <span>•</span>
                <span>Uptime: {uptime}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_kpi_strip(snapshot: LiveSnapshot, *, interval_seconds: float) -> None:
    import streamlit as st

    result = snapshot.result
    mode = result.target_mode_used.upper() if result else "WARMING"
    poly_count = len(result.polymarket_markets) if result else 0
    kalshi_count = len(result.kalshi_markets) if result else 0
    matches = len(result.matched_pairs) if result else 0
    net_profit = 0.0
    if result and result.opportunities:
        net_profit = sum(float(item.net_profit) for item in result.opportunities)

    col1, col2, col3, col4, col5 = st.columns([1.25, 1.2, 1.2, 1.2, 1.2])
    with col1:
        st.markdown(
            _kpi_card(
                "Arbitrage Mode",
                mode,
                subtitle=f"Scan Interval: {interval_seconds:.0f}s",
                badge="BTC",
                spark_values=[float(interval_seconds), float(interval_seconds), float(interval_seconds)],
                accent="#f59e0b",
            ),
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            _kpi_card(
                "Polymarket (Poly)",
                str(poly_count),
                spark_values=_latest_overview_values(snapshot, "polymarket_count"),
                accent="#38bdf8",
            ),
            unsafe_allow_html=True,
        )
    with col3:
        st.markdown(
            _kpi_card(
                "Kalshi",
                str(kalshi_count),
                spark_values=_latest_overview_values(snapshot, "kalshi_count"),
                accent="#22c55e",
            ),
            unsafe_allow_html=True,
        )
    with col4:
        st.markdown(
            _kpi_card(
                "Matches Today",
                str(matches),
                subtitle=f"Prev. scan: {snapshot.last_scan_ms or 0}ms",
                spark_values=_latest_overview_values(snapshot, "matched_pairs"),
                accent="#2dd4bf",
            ),
            unsafe_allow_html=True,
        )
    with col5:
        st.markdown(
            _kpi_card(
                "Net Session Profit",
                f"${net_profit:.2f}",
                subtitle=f"{snapshot.unique_opportunities} unique opps",
                badge="+",
                spark_values=[float(point.get("opportunities", 0)) for point in snapshot.market_overview_history[-18:]],
                accent="#4ade80",
            ),
            unsafe_allow_html=True,
        )

    st.caption(f"Last market timestamp: {_extract_scan_time(result)}")


def _render_opportunities(snapshot: LiveSnapshot, trade_size: int) -> None:
    import streamlit as st

    result = snapshot.result
    opportunity_rows = _opportunity_rows(result, trade_size=trade_size)
    if opportunity_rows:
        st.dataframe(_safe_dataframe(opportunity_rows), use_container_width=True, hide_index=True)
        top = opportunity_rows[0]
        st.success(
            f"Top take-home estimate for {trade_size} contract pair(s): "
            f"${top['Estimated take-home']:.4f} after modeled fees/slippage."
        )
    else:
        st.info("No profitable opportunities matched the current filters yet.")


def _render_pair_charts(snapshot: LiveSnapshot) -> None:
    import pandas as pd
    import streamlit as st

    result = snapshot.result
    if result is None or not result.matched_pairs:
        st.info("Charts will appear once matched or near-matched pairs are detected.")
        return

    pair_rows = _pair_rows(result)
    st.dataframe(_safe_dataframe(pair_rows), use_container_width=True, hide_index=True)

    st.subheader("Always-on convergence charts")
    top_pairs = sorted(result.matched_pairs, key=lambda item: (item.net_profit, item.similarity_score), reverse=True)[:3]
    for index, pair in enumerate(top_pairs, start=1):
        history = snapshot.price_history.get(_pair_history_key(pair), {})
        points = history.get("points", [])
        with st.expander(f"#{index} {pair.question[:100]} | {pair.status}: {pair.reason}", expanded=index == 1):
            if not points:
                st.info("Waiting for the next price point.")
                continue
            st.line_chart(
                data=pd.DataFrame(_chart_rows(points)),
                x="Time",
                y="Value",
                color="Series",
                use_container_width=True,
            )
            left, right = st.columns(2)
            with left:
                st.line_chart(
                    data=pd.DataFrame(_spread_rows(points)),
                    x="Time",
                    y="Value",
                    color="Metric",
                    use_container_width=True,
                )
            with right:
                st.area_chart(
                    data=pd.DataFrame(_profit_zone_rows(points)),
                    x="Time",
                    y="Profit Zone",
                    use_container_width=True,
                )
            st.progress(min(1.0, float(pair.liquidity_depth) / 1000.0), text=f"Liquidity depth: {pair.liquidity_depth}")
            st.progress(_confidence_score(pair) / 100, text=f"Confidence score: {_confidence_score(pair)}/100")


def _render_live_market_view(snapshot: LiveSnapshot) -> None:
    import pandas as pd
    import streamlit as st

    overview_points = snapshot.market_overview_history
    if not overview_points:
        st.info("No live market overview points recorded yet.")
        return
    st.line_chart(
        data=pd.DataFrame(_overview_price_rows(overview_points)),
        x="Time",
        y="Value",
        color="Series",
        use_container_width=True,
    )
    st.line_chart(
        data=pd.DataFrame(_overview_activity_rows(overview_points)),
        x="Time",
        y="Value",
        color="Series",
        use_container_width=True,
    )


def _render_history(snapshot: LiveSnapshot) -> None:
    import streamlit as st

    rows = _history_rows(snapshot.history_rows)
    if rows:
        history_df = _safe_dataframe(rows)
        st.dataframe(history_df, use_container_width=True, hide_index=True)
        st.download_button(
            "Export history CSV",
            data=history_df.to_csv(index=False).encode("utf-8"),
            file_name="arbitrage_history.csv",
            mime="text/csv",
        )
        st.download_button(
            "Export history JSON",
            data=_history_json(snapshot.history_rows).encode("utf-8"),
            file_name="arbitrage_history.json",
            mime="application/json",
        )
    else:
        st.info("No saved opportunities were found in SQLite.")


def _render_terminal(snapshot: LiveSnapshot) -> None:
    import streamlit as st

    body = "\n".join(snapshot.logs[-80:]) or "Waiting for scanner logs..."
    st.markdown(f'<div class="terminal-feed">{body}</div>', unsafe_allow_html=True)
    if snapshot.error:
        st.error(snapshot.error)


def run_dashboard() -> None:
    import streamlit as st

    settings = Settings.from_env()
    st.set_page_config(page_title="Arbitrage Bot Dashboard", page_icon=":chart_with_upwards_trend:", layout="wide")
    _inject_styles()

    default_demo = os.getenv("ARBITRAGE_STREAMLIT_DEFAULT_MODE", "live") == "demo"
    default_skip_db = os.getenv("ARBITRAGE_STREAMLIT_SKIP_DB", "0") == "1"
    default_history_limit = int(os.getenv("ARBITRAGE_STREAMLIT_HISTORY_LIMIT", "15"))
    default_interval = float(os.getenv("ARBITRAGE_STREAMLIT_INTERVAL", "5"))

    with st.sidebar:
        _render_nav_shell()
        st.markdown('<div class="section-label">Controls</div>', unsafe_allow_html=True)
        mode = st.radio("Data source", options=["Live scan", "Demo data"], index=1 if default_demo else 0)
        skip_db = st.checkbox("Skip SQLite writes", value=default_skip_db)
        live_enabled = st.toggle("Background scanner", value=True)
        live_view = st.toggle("Live UI updates", value=True)
        interval_seconds = st.number_input(
            "Scan interval (seconds)",
            min_value=1.0,
            max_value=300.0,
            value=default_interval,
            step=1.0,
        )
        trade_size = st.number_input("Trade size", min_value=1, max_value=100000, value=100, step=10)
        history_limit = st.slider("Recent trades", min_value=5, max_value=100, value=default_history_limit, step=5)
        force_refresh = st.button("Force scan now", type="primary", use_container_width=True)
        stop_clicked = st.button("Stop bot", use_container_width=True)
        st.divider()
        st.write("Target mode:", f"`{settings.target_market_mode}`")
        st.write("SQLite:", f"`{settings.sqlite_path}`")
        st.caption("Live UI updates use Streamlit fragments, not browser page reloads.")

    controller = _get_controller(
        demo=mode == "Demo data",
        skip_db=skip_db,
        interval=float(interval_seconds),
        history_limit=int(history_limit),
    )

    if live_enabled:
        controller.start()
    else:
        controller.stop()
    if force_refresh:
        controller.force_scan()
    if stop_clicked:
        controller.stop()
        st.toast("Scanner stopped.", icon=":octagonal_sign:")

    @st.fragment(run_every=float(interval_seconds) if live_view else None)
    def live_console_fragment() -> None:
        snapshot = controller.snapshot()
        if snapshot.last_new_opportunities and st.session_state.get("last_toast_scan") != snapshot.scan_count:
            st.session_state.last_toast_scan = snapshot.scan_count
            st.toast(f"{snapshot.last_new_opportunities} new arbitrage opportunity(s) found.", icon=":moneybag:")

        _render_top_bar(snapshot)
        _render_kpi_strip(snapshot, interval_seconds=float(interval_seconds))

        if snapshot.result is None:
            st.info("Scanner is warming up. The UI remains usable while the first scan runs in the background.")
            _render_terminal(snapshot)
            return

        st.markdown('<div class="section-label">Top Opportunities - Convergence & Spread</div>', unsafe_allow_html=True)
        chart_cols = st.columns(3)
        top_pairs = sorted(
            snapshot.result.matched_pairs,
            key=lambda item: (item.net_profit, item.similarity_score),
            reverse=True,
        )[:3]
        for index, column in enumerate(chart_cols):
            with column:
                if index >= len(top_pairs):
                    st.markdown('<div class="chart-shell">Waiting for more matched pairs...</div>', unsafe_allow_html=True)
                    continue
                pair = top_pairs[index]
                history = snapshot.price_history.get(_pair_history_key(pair), {})
                points = history.get("points", [])
                st.markdown(
                    f'<div class="chart-shell"><b>Chart {index + 1}: {pair.question[:54]}</b><br>'
                    f'<span style="color:#52bdf2">Spread:</span> {pair.gross_gap} &nbsp; '
                    f'<span style="color:#61e68b">Net:</span> {pair.net_profit}</div>',
                    unsafe_allow_html=True,
                )
                if points:
                    import pandas as pd

                    st.line_chart(
                        data=pd.DataFrame(_chart_rows(points)),
                        x="Time",
                        y="Value",
                        color="Series",
                        use_container_width=True,
                        height=170,
                    )
                    st.area_chart(
                        data=pd.DataFrame(_profit_zone_rows(points)),
                        x="Time",
                        y="Profit Zone",
                        use_container_width=True,
                        height=90,
                    )

        top_left, top_right = st.columns([1.55, 0.85])
        with top_left:
            st.markdown('<div class="section-label">Active Arbitrage Opportunities</div>', unsafe_allow_html=True)
            _render_opportunities(snapshot, trade_size=int(trade_size))
        with top_right:
            st.markdown('<div class="section-label">Performance Diagnostics</div>', unsafe_allow_html=True)
            diagnostics = snapshot.result.diagnostics or ["No diagnostics available."]
            for item in diagnostics[:6]:
                st.write(f"- {item}")
            if snapshot.result.opportunities:
                top = sorted(snapshot.result.opportunities, key=lambda item: item.net_profit, reverse=True)[0]
                st.metric("Take-home estimate", f"${float(top.net_profit) * int(trade_size):.4f}")
                st.caption(f"{trade_size} pair(s) x ${top.net_profit} net per pair")
            else:
                st.caption("Waiting for a profitable pair.")

        bottom_left, bottom_right = st.columns([1.15, 1])
        with bottom_left:
            st.markdown('<div class="section-label">Live Terminal Feed</div>', unsafe_allow_html=True)
            _render_terminal(snapshot)
        with bottom_right:
            st.markdown('<div class="section-label">Live Market View</div>', unsafe_allow_html=True)
            _render_live_market_view(snapshot)

        tab_pairs, tab_history, tab_discovery, tab_raw = st.tabs(
            ["All Matches", "Recent Trades", "Why No Match?", "Raw Snapshot"]
        )
        with tab_pairs:
            _render_pair_charts(snapshot)
        with tab_history:
            _render_history(snapshot)
        with tab_discovery:
            rows = _discovery_rows(snapshot.result)
            if rows:
                st.dataframe(_safe_dataframe(rows), use_container_width=True, hide_index=True)
            else:
                st.info("No rejected discovery samples were captured in the latest scan.")
        with tab_raw:
            st.json(
                _to_json_safe(
                    {
                        "target_mode_used": snapshot.result.target_mode_used,
                        "polymarket_markets": len(snapshot.result.polymarket_markets),
                        "kalshi_markets": len(snapshot.result.kalshi_markets),
                        "matched_pairs": [asdict(pair) for pair in snapshot.result.matched_pairs[:10]],
                        "opportunities": [asdict(opportunity) for opportunity in snapshot.result.opportunities[:10]],
                        "pings_ms": snapshot.pings_ms,
                        "diagnostics": snapshot.result.diagnostics,
                    }
                ),
                expanded=False,
            )

    live_console_fragment()


def main() -> None:
    parser = argparse.ArgumentParser(description="Launch the Streamlit dashboard for arbitrage-bot")
    parser.add_argument("--demo", action="store_true", help="Default the dashboard to demo mode on first load")
    parser.add_argument("--skip-db", action="store_true", help="Default the dashboard to skip SQLite writes")
    parser.add_argument("--history-limit", type=int, default=15, help="Default number of saved history rows to display")
    parser.add_argument("--interval", type=float, default=5.0, help="Default live auto-scan interval in seconds")
    args, streamlit_args = parser.parse_known_args()

    os.environ["ARBITRAGE_STREAMLIT_DEFAULT_MODE"] = "demo" if args.demo else "live"
    os.environ["ARBITRAGE_STREAMLIT_SKIP_DB"] = "1" if args.skip_db else "0"
    os.environ["ARBITRAGE_STREAMLIT_HISTORY_LIMIT"] = str(args.history_limit)
    os.environ["ARBITRAGE_STREAMLIT_INTERVAL"] = str(args.interval)

    from streamlit.web import cli as stcli

    sys.argv = ["streamlit", "run", str(Path(__file__).resolve()), *streamlit_args]
    raise SystemExit(stcli.main())


if _running_in_streamlit():
    run_dashboard()
elif __name__ == "__main__":
    main()
