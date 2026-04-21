from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import logging
from traceback import format_exc

from rich.table import Table
from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.widgets import DataTable, Footer, Header, Label, Log, Static

from ..config import Settings
from ..demo_data import build_demo_markets
from ..repository import SQLiteRepository
from ..scanner import ArbitrageScanner, PairAnalysis, ScanResult, persist_scan_result


@dataclass(slots=True)
class DashboardStatus:
    state: str = "idle"
    note: str = "Waiting to start"
    iteration: int = 0
    unique_opportunities_seen: int = 0
    stop_after_opportunities: int = 0
    target_mode_used: str = "-"
    matched_pairs: int = 0
    opportunities_found: int = 0
    last_scan_time: str = "-"


def opportunity_signature(opportunity) -> tuple[str, str, str, str, str, str]:
    return (
        opportunity.poly_market_id,
        opportunity.kalshi_market_id,
        opportunity.yes_platform,
        opportunity.no_platform,
        str(opportunity.yes_price),
        str(opportunity.no_price),
    )


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


class TextualLogHandler(logging.Handler):
    def __init__(self, app: "ArbitrageTextualApp") -> None:
        super().__init__()
        self._app = app

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
        except Exception:
            message = record.getMessage()
        self._app.write_log_message(message)


class ArbitrageTextualApp(App[None]):
    TITLE = "Arbitrage Bot"
    SUB_TITLE = "Interactive Market Scanner"
    BINDINGS = [
        Binding("q", "quit_app", "Quit"),
        Binding("p", "toggle_pause", "Pause"),
        Binding("r", "force_rediscover", "Rediscover"),
        Binding("h", "show_history", "History"),
        Binding("l", "focus_log", "Logs"),
        Binding("o", "focus_opportunities", "Opportunities"),
        Binding("m", "focus_matches", "Matches"),
    ]
    CSS = """
    Screen {
        background: #07131e;
        color: #e7f3f9;
        overflow-y: auto;
    }

    #hero {
        dock: top;
        height: 3;
        background: #0d2638;
        color: #a5f3fc;
        content-align: center middle;
        text-style: bold;
        border-bottom: solid #1f7a8c;
    }

    #logs-title {
        color: #fda4af;
        text-style: bold;
        margin: 1 1 0 1;
    }

    #body {
        layout: horizontal;
        min-height: 32;
    }

    #left-pane {
        width: 2fr;
        padding: 1 1 1 1;
    }

    #right-pane {
        width: 1fr;
        padding: 1 1 1 0;
    }

    .section-title {
        color: #7dd3fc;
        text-style: bold;
        margin: 0 0 1 0;
    }

    .panel {
        border: round #1f7a8c;
        background: #0a1d2d;
        color: #e2eef5;
        padding: 1 2;
        margin: 0 0 1 0;
    }

    .table-panel {
        border: round #1d4ed8;
        background: #0a1724;
        margin: 0 0 1 0;
        height: 1fr;
    }

    #status-panel {
        height: 8;
        border: round #2dd4bf;
        background: #0b2530;
    }

    #diagnostics-panel {
        height: 12;
        border: round #38bdf8;
    }

    #detail-panel {
        height: 1fr;
        min-height: 12;
        border: round #f59e0b;
        background: #271b08;
    }

    #event-log {
        height: 14;
        border: round #ef4444;
        background: #251116;
        color: #ffe4e6;
        margin: 0 1 1 1;
    }

    DataTable {
        background: #081420;
    }

    Footer {
        background: #0d2638;
    }
    """

    def __init__(
        self,
        *,
        interval: float,
        stop_after_opportunities: int,
        skip_db: bool,
        demo: bool,
    ) -> None:
        super().__init__()
        self._interval = interval
        self._stop_after_opportunities = stop_after_opportunities
        self._skip_db = skip_db
        self._demo = demo
        self._settings = Settings.from_env()
        self._scanner: ArbitrageScanner | None = None
        self._repository: SQLiteRepository | None = None
        self._scan_task: asyncio.Task[None] | None = None
        self._log_handler: TextualLogHandler | None = None
        self._paused = False
        self._force_rediscover = False
        self._stop_requested = False
        self._seen_opportunity_signatures: set[tuple[str, str, str, str, str, str]] = set()
        self._status = DashboardStatus(stop_after_opportunities=stop_after_opportunities)

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with VerticalScroll():
            yield Static("Live arbitrage cockpit for Polymarket and Kalshi", id="hero")
            yield Label("Live Logs", id="logs-title")
            yield Log(id="event-log", highlight=True, auto_scroll=True)
            with Horizontal(id="body"):
                with Vertical(id="left-pane"):
                    yield Label("Live Opportunities", classes="section-title")
                    yield DataTable(id="opportunities-table", classes="table-panel", cursor_type="row")
                    yield Label("Matched Pairs", classes="section-title")
                    yield DataTable(id="pairs-table", classes="table-panel", cursor_type="row")
                with Vertical(id="right-pane"):
                    yield Static(id="status-panel", classes="panel")
                    yield Static(id="diagnostics-panel", classes="panel")
                    yield Static(id="detail-panel", classes="panel")
        yield Footer()

    async def on_mount(self) -> None:
        self._configure_tables()
        self._install_logging_bridge()
        self._render_status()
        self._render_diagnostics(["UI booted. Waiting for first scan cycle."])
        self._render_detail("No market selected yet.")
        self._log_event("Dashboard mounted.")
        self._scan_task = asyncio.create_task(self._scan_loop())

    async def on_unmount(self) -> None:
        self._stop_requested = True
        if self._scan_task is not None:
            self._scan_task.cancel()
            try:
                await self._scan_task
            except asyncio.CancelledError:
                pass
        if self._repository is not None:
            await self._repository.close()
            self._repository = None
        if self._scanner is not None:
            await self._scanner.aclose()
            self._scanner = None
        self._remove_logging_bridge()

    def action_quit_app(self) -> None:
        self.exit()

    def action_toggle_pause(self) -> None:
        self._paused = not self._paused
        self._status.state = "paused" if self._paused else "running"
        self._status.note = "Paused by user." if self._paused else "Resumed scanning."
        self._render_status()
        self._log_event(self._status.note)

    def action_force_rediscover(self) -> None:
        self._force_rediscover = True
        self._status.note = "Manual rediscovery requested."
        self._render_status()
        self._log_event("Manual rediscovery queued for next cycle.")

    async def action_show_history(self) -> None:
        if not self._settings.sqlite_path:
            self._render_detail("SQLite path is not configured, so history is unavailable.")
            return
        repository = SQLiteRepository(self._settings.sqlite_path)
        await repository.connect()
        try:
            rows = await repository.load_recent_opportunities(limit=10)
        finally:
            await repository.close()

        if not rows:
            self._render_detail("No saved opportunities are in SQLite yet.")
            self._log_event("History requested: no saved opportunities.")
            return

        table = Table.grid(expand=True)
        table.add_column(style="bold #fcd34d")
        table.add_column(style="#fef3c7")
        for index, row in enumerate(rows, start=1):
            table.add_row(
                f"#{index}",
                f"{row['question']}\n{row['detected_at']} | net ${row['net_profit']}",
            )
        self.query_one("#detail-panel", Static).update(table)
        self._log_event("Loaded recent saved opportunities from SQLite.")

    def action_focus_log(self) -> None:
        self.query_one("#event-log", Log).focus()

    def action_focus_opportunities(self) -> None:
        self.query_one("#opportunities-table", DataTable).focus()

    def action_focus_matches(self) -> None:
        self.query_one("#pairs-table", DataTable).focus()

    def _configure_tables(self) -> None:
        opportunities_table = self.query_one("#opportunities-table", DataTable)
        opportunities_table.clear(columns=True)
        opportunities_table.add_columns("Question", "Trade", "Gross", "Net", "Depth", "Detected")

        pairs_table = self.query_one("#pairs-table", DataTable)
        pairs_table.clear(columns=True)
        pairs_table.add_columns("Question", "Similarity", "Direction", "Gross", "Net", "Status")

    def _install_logging_bridge(self) -> None:
        handler = TextualLogHandler(self)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        self._log_handler = handler
        for logger_name in ("arbitrage_bot", "__main__", "httpx"):
            logger = logging.getLogger(logger_name)
            logger.addHandler(handler)

    def _remove_logging_bridge(self) -> None:
        if self._log_handler is None:
            return
        for logger_name in ("arbitrage_bot", "__main__", "httpx"):
            logging.getLogger(logger_name).removeHandler(self._log_handler)
        self._log_handler = None

    async def _scan_loop(self) -> None:
        self._scanner = ArbitrageScanner(self._settings)
        if not self._skip_db and self._settings.sqlite_path:
            self._repository = SQLiteRepository(self._settings.sqlite_path)
            await self._repository.connect()

        self._status.state = "running"
        self._status.note = "Scanner loop started."
        self._render_status()
        self._log_event("Scanner loop started.")

        try:
            while not self._stop_requested:
                if self._paused:
                    await asyncio.sleep(0.25)
                    continue

                self._status.iteration += 1
                rediscover = (
                    self._demo
                    or self._force_rediscover
                    or self._status.iteration == 1
                    or self._status.iteration % self._settings.rediscovery_interval_iterations == 0
                )
                self._force_rediscover = False

                if self._demo:
                    result = await self._build_demo_result()
                    self._status.note = "Demo data refreshed."
                elif rediscover:
                    self._status.note = "Running full market discovery."
                    self._render_status()
                    result = await self._scanner.run_once()
                elif self._scanner._tracked_matches:
                    self._status.note = "Refreshing tracked matched pairs."
                    self._render_status()
                    result = await self._scanner.refresh_tracked_pairs()
                else:
                    self._status.note = "Waiting for next rediscovery because no tracked pairs are available."
                    self._render_status()
                    self._log_event(self._status.note)
                    await asyncio.sleep(self._interval)
                    continue

                await self._handle_result(result)

                if self._stop_after_opportunities > 0 and self._status.unique_opportunities_seen >= self._stop_after_opportunities:
                    self._status.state = "stopped"
                    self._status.note = (
                        f"Reached target after seeing {self._status.unique_opportunities_seen} unique profitable opportunities."
                    )
                    self._render_status()
                    self._log_event(self._status.note)
                    break

                await asyncio.sleep(self._interval)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._status.state = "crashed"
            self._status.note = f"Crash: {type(exc).__name__}: {exc}"
            self._render_status()
            self._render_detail(format_exc())
            self._log_event(self._status.note)

    async def _build_demo_result(self) -> ScanResult:
        assert self._scanner is not None
        polymarket_markets, kalshi_markets = build_demo_markets()
        opportunities = self._scanner.detect_opportunities(polymarket_markets, kalshi_markets)
        return ScanResult(
            polymarket_markets=polymarket_markets,
            kalshi_markets=kalshi_markets,
            matched_pairs=self._scanner._latest_pair_analyses,
            opportunities=opportunities,
            diagnostics=self._scanner._build_diagnostics(
                polymarket_markets,
                kalshi_markets,
                self._scanner._latest_pair_analyses,
                opportunities,
                "demo",
                False,
            ),
            discovery_samples=[],
            target_mode_used="demo",
        )

    async def _handle_result(self, result: ScanResult) -> None:
        self._status.target_mode_used = result.target_mode_used
        self._status.matched_pairs = len(result.matched_pairs)
        self._status.opportunities_found = len(result.opportunities)
        self._status.last_scan_time = self._extract_scan_time(result)
        self._status.state = "running"
        self._status.note = "Scan complete."

        new_unique = 0
        for opportunity in result.opportunities:
            signature = opportunity_signature(opportunity)
            if signature not in self._seen_opportunity_signatures:
                self._seen_opportunity_signatures.add(signature)
                new_unique += 1
        self._status.unique_opportunities_seen = len(self._seen_opportunity_signatures)

        if self._repository is not None:
            await persist_scan_result(self._repository, result)

        self._render_status()
        self._render_diagnostics(result.diagnostics)
        self._render_opportunities(result)
        self._render_pairs(result.matched_pairs)
        self._render_primary_detail(result)

        if new_unique > 0:
            self._log_event(f"Found {new_unique} new unique profitable opportunity(s) this cycle.")
        else:
            self._log_event(
                f"Iteration {self._status.iteration}: {len(result.matched_pairs)} matches, {len(result.opportunities)} opportunities."
            )

    def _render_status(self) -> None:
        progress = "-"
        if self._status.stop_after_opportunities > 0:
            progress = f"{self._status.unique_opportunities_seen}/{self._status.stop_after_opportunities}"

        table = Table.grid(expand=True)
        table.add_column(style="bold #67e8f9", ratio=1)
        table.add_column(style="#e0f2fe", ratio=1)
        table.add_column(style="bold #fbbf24", ratio=1)
        table.add_column(style="#fef3c7", ratio=1)
        table.add_row("State", self._status.state.upper(), "Iteration", str(self._status.iteration))
        table.add_row("Mode", self._status.target_mode_used, "Progress", progress)
        table.add_row("Matches", str(self._status.matched_pairs), "Opportunities", str(self._status.opportunities_found))
        table.add_row("Last Scan", self._status.last_scan_time, "Note", self._status.note)
        self.query_one("#status-panel", Static).update(table)

    def _render_diagnostics(self, diagnostics: list[str]) -> None:
        if not diagnostics:
            diagnostics = ["No diagnostics available yet."]
        body = Text()
        for item in diagnostics[:8]:
            body.append("• ", style="bold #38bdf8")
            body.append(f"{item}\n", style="#dbeafe")
        self.query_one("#diagnostics-panel", Static).update(body)

    def _render_detail(self, body: object) -> None:
        self.query_one("#detail-panel", Static).update(body)

    def _render_primary_detail(self, result: ScanResult) -> None:
        if result.opportunities:
            top = sorted(result.opportunities, key=lambda item: item.net_profit, reverse=True)[0]
            self._render_detail(
                Text.from_markup(
                    "[bold #fbbf24]Top opportunity[/]\n"
                    f"[#fde68a]{top.question}[/]\n\n"
                    f"[bold]Trade:[/] BUY YES on {top.yes_platform.upper()} at ${top.yes_price}\n"
                    f"[bold]Against:[/] BUY NO on {top.no_platform.upper()} at ${top.no_price}\n"
                    f"[bold]Gross gap:[/] ${top.gross_gap}\n"
                    f"[bold]Net profit:[/] ${top.net_profit}\n"
                    f"[bold]Fees:[/] ${top.fee_cost}   [bold]Slippage:[/] ${top.slippage_cost}\n"
                    f"[bold]Depth:[/] {top.liquidity_depth} contracts"
                )
            )
            return

        if result.matched_pairs:
            top = result.matched_pairs[0]
            self._render_detail(
                Text.from_markup(
                    "[bold #fbbf24]Best matched pair[/]\n"
                    f"[#fde68a]{top.question}[/]\n\n"
                    f"[bold]Similarity:[/] {top.similarity_score:.2f}\n"
                    f"[bold]Direction:[/] YES on {top.yes_platform.upper()} at ${top.yes_price} | "
                    f"NO on {top.no_platform.upper()} at ${top.no_price}\n"
                    f"[bold]Gross / Net:[/] ${top.gross_gap} / ${top.net_profit}\n"
                    f"[bold]Status:[/] {top.status} ({top.reason})"
                )
            )
            return

        self._render_detail("No opportunities or matched pairs yet. Use 'r' to force a rediscovery cycle.")

    def _render_opportunities(self, result: ScanResult) -> None:
        table = self.query_one("#opportunities-table", DataTable)
        table.clear()
        for index, opportunity in enumerate(
            sorted(result.opportunities, key=lambda item: item.net_profit, reverse=True),
            start=1,
        ):
            table.add_row(
                opportunity.question[:48],
                f"YES {opportunity.yes_platform[:4].upper()} / NO {opportunity.no_platform[:4].upper()}",
                f"${opportunity.gross_gap}",
                f"${opportunity.net_profit}",
                str(opportunity.liquidity_depth),
                opportunity.detected_at.astimezone(timezone.utc).strftime("%H:%M:%S"),
                key=f"opp-{index}",
            )

    def _render_pairs(self, pairs: list[PairAnalysis]) -> None:
        table = self.query_one("#pairs-table", DataTable)
        table.clear()
        for index, pair in enumerate(pairs[:25], start=1):
            table.add_row(
                pair.question[:48],
                f"{pair.similarity_score:.2f}",
                f"YES {pair.yes_platform[:4].upper()} / NO {pair.no_platform[:4].upper()}",
                f"${pair.gross_gap}",
                f"${pair.net_profit}",
                pair.status,
                key=f"pair-{index}",
            )

    def _log_event(self, text: str) -> None:
        self.query_one("#event-log", Log).write_line(f"[{utc_timestamp()}] {text}")

    def write_log_message(self, text: str) -> None:
        log_widget = self.query_one("#event-log", Log)
        log_widget.write_line(text)

    @staticmethod
    def _extract_scan_time(result: ScanResult) -> str:
        timestamps = [market.fetched_at for market in result.polymarket_markets + result.kalshi_markets]
        timestamps.extend(opportunity.detected_at for opportunity in result.opportunities)
        if not timestamps:
            return "-"
        latest = max(timestamps)
        return latest.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
