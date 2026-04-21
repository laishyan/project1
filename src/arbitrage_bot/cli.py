from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import sys

from .config import Settings
from .demo_data import build_demo_markets
from .repository import SQLiteRepository
from .scanner import ArbitrageScanner, ScanResult, persist_scan_result


logger = logging.getLogger(__name__)
LATEST_REPORT_PATH = Path("latest_scan_report.txt")
LATEST_RUN_STATUS_PATH = Path("latest_run_status.json")
LATEST_RUN_SUMMARY_PATH = Path("latest_run_summary.txt")


def opportunity_signature(opportunity) -> tuple[str, str, str, str, str, str]:
    return (
        opportunity.poly_market_id,
        opportunity.kalshi_market_id,
        opportunity.yes_platform,
        opportunity.no_platform,
        str(opportunity.yes_price),
        str(opportunity.no_price),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Polymarket / Kalshi arbitrage scanner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--skip-db", action="store_true", help="Do not write results to SQLite")
    common.add_argument("--verbose", action="store_true", help="Enable verbose debug logging")

    subparsers.add_parser("demo", parents=[common], help="Run against built-in sample data")
    subparsers.add_parser("discover", parents=[common], help="Inspect candidate BTC 15m markets and pairings")
    subparsers.add_parser("status", parents=[common], help="Show the most recent long-run status summary")
    history_parser = subparsers.add_parser("history", parents=[common], help="Show recent saved opportunities from SQLite")
    history_parser.add_argument("--limit", type=int, default=20, help="How many saved opportunities to show")
    subparsers.add_parser("once", parents=[common], help="Run a single live scan")
    ui_parser = subparsers.add_parser("ui", parents=[common], help="Launch the interactive Textual dashboard")
    ui_parser.add_argument("--interval", type=float, default=5.0, help="Polling interval in seconds")
    ui_parser.add_argument("--stop-after-opportunities", type=int, default=0, help="Pause the live scanner after this many unique profitable opportunities")
    ui_parser.add_argument("--demo", action="store_true", help="Run the dashboard against built-in demo data")
    streamlit_parser = subparsers.add_parser("streamlit-ui", parents=[common], help="Launch the Streamlit dashboard in a browser")
    streamlit_parser.add_argument("--demo", action="store_true", help="Open the Streamlit dashboard with demo mode selected by default")
    streamlit_parser.add_argument("--history-limit", type=int, default=15, help="Default number of saved SQLite opportunities to display")
    streamlit_parser.add_argument("--interval", type=float, default=5.0, help="Default live auto-scan interval in seconds")
    scan_parser = subparsers.add_parser("scan", parents=[common], help="Run the live scanner in a loop")
    scan_parser.add_argument("--interval", type=float, default=10.0, help="Polling interval in seconds")
    scan_parser.add_argument(
        "--stop-after-opportunities",
        type=int,
        default=0,
        help="Stop after this many unique profitable opportunities have been seen across scan iterations",
    )
    return parser


def render_result(result: ScanResult) -> str:
    timestamp = "-"
    if result.polymarket_markets or result.kalshi_markets or result.opportunities:
        latest = max(
            [market.fetched_at for market in result.polymarket_markets + result.kalshi_markets]
            + [opp.detected_at for opp in result.opportunities]
        )
        timestamp = latest.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    lines = [
        "=" * 72,
        "ARBITRAGE SCAN RESULT",
        "=" * 72,
        f"Scan time           : {timestamp}",
        f"Polymarket markets  : {len(result.polymarket_markets)}",
        f"Kalshi markets      : {len(result.kalshi_markets)}",
        f"Matched pairs       : {len(result.matched_pairs)}",
        f"Opportunities found : {len(result.opportunities)}",
        f"Target mode used    : {result.target_mode_used}",
    ]

    if result.diagnostics:
        lines.extend(
            [
                "-" * 72,
                "Diagnostics",
            ]
        )
        for item in result.diagnostics:
            lines.append(f"- {item}")

    if result.polymarket_markets:
        lines.extend(
            [
                "-" * 72,
                "Polymarket candidates",
            ]
        )
        for market in result.polymarket_markets[:5]:
            lines.append(
                f"- {market.question} | YES ${market.quote.yes_price} | NO ${market.quote.no_price} | depth {market.quote.liquidity}"
            )

    if result.kalshi_markets:
        lines.extend(
            [
                "-" * 72,
                "Kalshi candidates",
            ]
        )
        for market in result.kalshi_markets[:5]:
            lines.append(
                f"- {market.question} | YES ${market.quote.yes_price} | NO ${market.quote.no_price} | depth {market.quote.liquidity}"
            )

    if result.discovery_samples:
        lines.extend(
            [
                "-" * 72,
                "Rejected discovery samples",
            ]
        )
        for sample in result.discovery_samples[:8]:
            end_text = sample.end_time.strftime("%Y-%m-%d %H:%M:%S UTC") if sample.end_time is not None else "-"
            yes_text = f"${sample.yes_price}" if sample.yes_price is not None else "-"
            no_text = f"${sample.no_price}" if sample.no_price is not None else "-"
            lines.append(
                f"- [{sample.platform}] {sample.question} | end {end_text} | YES {yes_text} | NO {no_text} | {sample.reason}"
            )

    if result.matched_pairs:
        lines.extend(
            [
                "-" * 72,
                "Top matched pairs",
            ]
        )
        for index, pair in enumerate(result.matched_pairs[:5], start=1):
            lines.extend(
                [
                    f"{index}. {pair.question}",
                    f"   similarity : {pair.similarity_score:.2f}",
                    f"   direction  : YES on {pair.yes_platform.upper()} @ ${pair.yes_price} | NO on {pair.no_platform.upper()} @ ${pair.no_price}",
                    f"   gross/net  : ${pair.gross_gap} / ${pair.net_profit}",
                    f"   status     : {pair.status} ({pair.reason})",
                ]
            )

    if not result.opportunities:
        lines.extend(
            [
                "-" * 72,
                "No profitable opportunities matched the current filters.",
            ]
        )
        return "\n".join(lines)

    for index, opp in enumerate(sorted(result.opportunities, key=lambda item: item.net_profit, reverse=True), start=1):
        lines.extend(
            [
                "-" * 72,
                f"Opportunity #{index}",
                f"Question     : {opp.question}",
                f"Trade        : BUY YES on {opp.yes_platform.upper()} at ${opp.yes_price} | "
                f"BUY NO on {opp.no_platform.upper()} at ${opp.no_price}",
                f"Gross gap    : ${opp.gross_gap}",
                f"Fees         : ${opp.fee_cost}",
                f"Slippage     : ${opp.slippage_cost}",
                f"Net profit   : ${opp.net_profit}",
                f"Depth        : {opp.liquidity_depth} contracts",
                f"Market IDs   : poly={opp.poly_market_id} | kalshi={opp.kalshi_market_id}",
            ]
        )
    return "\n".join(lines)


def write_report(text: str) -> None:
    LATEST_REPORT_PATH.write_text(text, encoding="utf-8")
    logger.info("Saved report to %s", LATEST_REPORT_PATH.resolve())


def write_run_status(data: dict[str, object]) -> None:
    LATEST_RUN_STATUS_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
    logger.debug("Saved run status to %s", LATEST_RUN_STATUS_PATH.resolve())


def render_run_status(data: dict[str, object]) -> str:
    lines = [
        "=" * 72,
        "SCAN RUN STATUS",
        "=" * 72,
        f"State               : {data.get('state', '-')}",
        f"Reason              : {data.get('reason', '-')}",
        f"Started at          : {data.get('started_at', '-')}",
        f"Updated at          : {data.get('updated_at', '-')}",
        f"Ended at            : {data.get('ended_at', '-')}",
        f"Iterations          : {data.get('iterations', 0)}",
        f"Unique opportunities: {data.get('unique_opportunities_seen', 0)}",
        f"Stop target         : {data.get('stop_after_opportunities', 0)}",
        f"Last target mode    : {data.get('last_target_mode_used', '-')}",
        f"Last matched pairs  : {data.get('last_matched_pairs', 0)}",
        f"Last opportunities  : {data.get('last_opportunities_found', 0)}",
    ]
    diagnostics = data.get("last_diagnostics")
    if isinstance(diagnostics, list) and diagnostics:
        lines.extend(["-" * 72, "Last diagnostics"])
        for item in diagnostics[:8]:
            lines.append(f"- {item}")
    return "\n".join(lines)


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def write_run_summary(data: dict[str, object]) -> None:
    text = render_run_status(data)
    LATEST_RUN_SUMMARY_PATH.write_text(text, encoding="utf-8")
    logger.info("Saved run summary to %s", LATEST_RUN_SUMMARY_PATH.resolve())


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if verbose:
        logging.getLogger("httpx").setLevel(logging.INFO)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
        logging.getLogger("asyncio").setLevel(logging.WARNING)


def configure_ui_logging(verbose: bool) -> None:
    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
    root_logger.setLevel(logging.DEBUG)
    logging.getLogger("httpx").setLevel(logging.DEBUG)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.INFO if verbose else logging.WARNING)


async def run_demo(skip_db: bool, verbose: bool) -> int:
    logger.info("Starting demo mode")
    poly, kalshi = build_demo_markets()
    scanner = ArbitrageScanner(Settings(sqlite_path=":memory:"))
    try:
        opportunities = scanner.detect_opportunities(poly, kalshi)
        result = ScanResult(
            polymarket_markets=poly,
            kalshi_markets=kalshi,
            matched_pairs=scanner._latest_pair_analyses,
            opportunities=opportunities,
            diagnostics=scanner._build_diagnostics(
                poly,
                kalshi,
                scanner._latest_pair_analyses,
                opportunities,
                "demo",
                False,
            ),
            discovery_samples=[],
            target_mode_used="demo",
        )
    finally:
        await scanner.aclose()

    logger.debug("Demo mode produced %s polymarket markets and %s kalshi markets", len(poly), len(kalshi))
    rendered = render_result(result)
    write_report(rendered)
    print(rendered)
    if result.opportunities and not skip_db:
        print("Demo mode does not write to the database. Use `once` or `scan` for persistence.")
    logger.info("Demo mode finished")
    return 0


async def run_discover(verbose: bool) -> int:
    settings = Settings.from_env()
    logger.info("Starting discovery mode")
    scanner = ArbitrageScanner(settings)
    try:
        result = await scanner.run_once()
        rendered = render_result(result)
        write_report(rendered)
        print(rendered)
        logger.info("Discovery mode finished")
        return 0
    finally:
        await scanner.aclose()


async def run_status() -> int:
    if not LATEST_RUN_STATUS_PATH.exists():
        print(f"No run status file found at {LATEST_RUN_STATUS_PATH.resolve()}")
        return 1
    payload = json.loads(LATEST_RUN_STATUS_PATH.read_text(encoding="utf-8"))
    print(render_run_status(payload))
    return 0


async def run_history(limit: int) -> int:
    settings = Settings.from_env()
    if not settings.sqlite_path:
        print("SQLITE_PATH is not configured.", file=sys.stderr)
        return 2

    repository = SQLiteRepository(settings.sqlite_path)
    await repository.connect()
    try:
        total = await repository.count_opportunities()
        rows = await repository.load_recent_opportunities(limit=limit)
    finally:
        await repository.close()

    lines = [
        "=" * 72,
        "OPPORTUNITY HISTORY",
        "=" * 72,
        f"Database            : {settings.sqlite_path}",
        f"Total saved         : {total}",
        f"Showing             : {len(rows)}",
    ]
    if not rows:
        lines.extend(["-" * 72, "No opportunities have been saved yet."])
    else:
        for index, row in enumerate(rows, start=1):
            lines.extend(
                [
                    "-" * 72,
                    f"Saved opportunity #{index}",
                    f"Detected at         : {row['detected_at']}",
                    f"Question            : {row['question']}",
                    f"Trade               : BUY YES on {str(row['yes_platform']).upper()} | BUY NO on {str(row['no_platform']).upper()}",
                    f"Net profit          : ${row['net_profit']}",
                    f"Gross gap           : ${row['gross_gap']}",
                    f"Fees/slippage       : ${row['fee_cost']} / ${row['slippage_cost']}",
                    f"Market IDs          : poly={row['poly_market_id']} | kalshi={row['kalshi_market_id']}",
                ]
            )
    print("\n".join(lines))
    return 0


def run_ui(*, interval: float, stop_after_opportunities: int, skip_db: bool, demo: bool) -> int:
    from .tui.app import ArbitrageTextualApp

    app = ArbitrageTextualApp(
        interval=interval,
        stop_after_opportunities=stop_after_opportunities,
        skip_db=skip_db,
        demo=demo,
    )
    app.run()
    return 0


def run_streamlit_ui(*, demo: bool, skip_db: bool, history_limit: int, interval: float) -> int:
    from streamlit.web import cli as stcli

    os.environ["ARBITRAGE_STREAMLIT_DEFAULT_MODE"] = "demo" if demo else "live"
    os.environ["ARBITRAGE_STREAMLIT_SKIP_DB"] = "1" if skip_db else "0"
    os.environ["ARBITRAGE_STREAMLIT_HISTORY_LIMIT"] = str(history_limit)
    os.environ["ARBITRAGE_STREAMLIT_INTERVAL"] = str(interval)

    app_path = Path(__file__).with_name("streamlit_app.py")
    sys.argv = ["streamlit", "run", str(app_path)]
    return stcli.main()


async def run_once(skip_db: bool, verbose: bool) -> int:
    settings = Settings.from_env()
    logger.info("Starting live scan")
    logger.debug(
        "Settings loaded: sqlite_path=%s polymarket_base_url=%s kalshi_base_url=%s timeout=%s scan_limit=%s discovery_scan_limit=%s",
        settings.sqlite_path,
        settings.polymarket_base_url,
        settings.kalshi_base_url,
        settings.http_timeout_seconds,
        settings.scan_limit,
        settings.discovery_scan_limit,
    )
    logger.info("Target market mode: %s", settings.target_market_mode)
    scanner = ArbitrageScanner(settings)
    repository: SQLiteRepository | None = None
    try:
        result = await scanner.run_once()
        logger.debug(
            "Live scan returned %s polymarket markets, %s kalshi markets, %s opportunities",
            len(result.polymarket_markets),
            len(result.kalshi_markets),
            len(result.opportunities),
        )
        rendered = render_result(result)
        write_report(rendered)
        print(rendered)
        if not skip_db:
            if not settings.sqlite_path:
                print("SQLITE_PATH is required unless --skip-db is used.", file=sys.stderr)
                return 2
            logger.debug("Opening SQLite database at %s", settings.sqlite_path)
            repository = SQLiteRepository(settings.sqlite_path)
            await repository.connect()
            await persist_scan_result(repository, result)
            logger.info("Persisted scan results to SQLite")
        else:
            logger.info("Skipping SQLite persistence because --skip-db was provided")
        logger.info("Live scan finished")
        return 0
    finally:
        if repository is not None:
            await repository.close()
        await scanner.aclose()


async def run_scan(interval: float, skip_db: bool, verbose: bool, stop_after_opportunities: int) -> int:
    iteration = 0
    seen_opportunity_signatures: set[tuple[str, str, str, str, str, str]] = set()
    logger.info("Starting continuous scan loop with %.2f second interval", interval)
    if stop_after_opportunities > 0:
        logger.info("Scan will stop after %s unique profitable opportunities are found", stop_after_opportunities)
    settings = Settings.from_env()
    scanner = ArbitrageScanner(settings)
    repository: SQLiteRepository | None = None
    run_status: dict[str, object] = {
        "state": "running",
        "reason": "scan_started",
        "started_at": "",
        "updated_at": "",
        "ended_at": "",
        "iterations": 0,
        "unique_opportunities_seen": 0,
        "stop_after_opportunities": stop_after_opportunities,
        "last_target_mode_used": settings.target_market_mode,
        "last_matched_pairs": 0,
        "last_opportunities_found": 0,
        "last_diagnostics": [],
    }
    start_text = utc_timestamp()
    run_status["started_at"] = start_text
    run_status["updated_at"] = start_text
    write_run_status(run_status)
    write_run_summary(run_status)
    try:
        while True:
            iteration += 1
            logger.info("Beginning scan iteration %s", iteration)
            rediscover = iteration == 1 or iteration % settings.rediscovery_interval_iterations == 0
            if rediscover:
                logger.info("Running full market discovery")
                result = await scanner.run_once()
            else:
                if scanner._tracked_matches:
                    logger.info("Refreshing tracked matched pairs")
                    result = await scanner.refresh_tracked_pairs()
                else:
                    logger.info(
                        "No tracked pairs yet; waiting until iteration %s for the next rediscovery",
                        settings.rediscovery_interval_iterations,
                    )
                    result = ScanResult(
                        polymarket_markets=[],
                        kalshi_markets=[],
                        matched_pairs=[],
                        opportunities=[],
                        diagnostics=[
                            f"No tracked pairs yet. Next full rediscovery runs on iteration {settings.rediscovery_interval_iterations}."
                        ],
                        discovery_samples=[],
                        target_mode_used=settings.target_market_mode,
                    )

            rendered = render_result(result)
            write_report(rendered)
            print(rendered)

            new_unique_count = 0
            for opportunity in result.opportunities:
                signature = opportunity_signature(opportunity)
                if signature not in seen_opportunity_signatures:
                    seen_opportunity_signatures.add(signature)
                    new_unique_count += 1

            if stop_after_opportunities > 0:
                logger.info(
                    "Unique profitable opportunities seen so far: %s/%s (%s new this iteration)",
                    len(seen_opportunity_signatures),
                    stop_after_opportunities,
                    new_unique_count,
                )

            updated_text = utc_timestamp()
            run_status.update(
                {
                    "state": "running",
                    "reason": "scan_running",
                    "updated_at": updated_text,
                    "iterations": iteration,
                    "unique_opportunities_seen": len(seen_opportunity_signatures),
                    "last_target_mode_used": result.target_mode_used,
                    "last_matched_pairs": len(result.matched_pairs),
                    "last_opportunities_found": len(result.opportunities),
                    "last_diagnostics": result.diagnostics,
                }
            )
            write_run_status(run_status)
            write_run_summary(run_status)

            if not skip_db:
                if repository is None:
                    repository = SQLiteRepository(settings.sqlite_path)
                    await repository.connect()
                await persist_scan_result(repository, result)
                logger.info("Persisted scan results to SQLite")
            else:
                logger.info("Skipping SQLite persistence because --skip-db was provided")

            if stop_after_opportunities > 0 and len(seen_opportunity_signatures) >= stop_after_opportunities:
                logger.info(
                    "Reached stop condition after finding %s unique profitable opportunities",
                    len(seen_opportunity_signatures),
                )
                ended_text = utc_timestamp()
                run_status.update(
                    {
                        "state": "stopped",
                        "reason": "stop_target_reached",
                        "updated_at": ended_text,
                        "ended_at": ended_text,
                        "iterations": iteration,
                        "unique_opportunities_seen": len(seen_opportunity_signatures),
                    }
                )
                write_run_status(run_status)
                write_run_summary(run_status)
                return 0

            logger.debug("Sleeping for %.2f seconds before next iteration", interval)
            await asyncio.sleep(interval)
    except KeyboardInterrupt:
        ended_text = utc_timestamp()
        run_status.update(
            {
                "state": "stopped",
                "reason": "keyboard_interrupt",
                "updated_at": ended_text,
                "ended_at": ended_text,
                "iterations": iteration,
                "unique_opportunities_seen": len(seen_opportunity_signatures),
            }
        )
        write_run_status(run_status)
        write_run_summary(run_status)
        raise
    except Exception as exc:
        ended_text = utc_timestamp()
        run_status.update(
            {
                "state": "crashed",
                "reason": f"exception:{type(exc).__name__}",
                "updated_at": ended_text,
                "ended_at": ended_text,
                "iterations": iteration,
                "unique_opportunities_seen": len(seen_opportunity_signatures),
            }
        )
        write_run_status(run_status)
        write_run_summary(run_status)
        raise
    finally:
        if repository is not None:
            await repository.close()
        await scanner.aclose()


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "ui":
        configure_ui_logging(args.verbose)
    else:
        configure_logging(args.verbose)
    try:
        if args.command == "demo":
            raise SystemExit(asyncio.run(run_demo(skip_db=args.skip_db, verbose=args.verbose)))
        if args.command == "discover":
            raise SystemExit(asyncio.run(run_discover(verbose=args.verbose)))
        if args.command == "status":
            raise SystemExit(asyncio.run(run_status()))
        if args.command == "history":
            raise SystemExit(asyncio.run(run_history(limit=args.limit)))
        if args.command == "once":
            raise SystemExit(asyncio.run(run_once(skip_db=args.skip_db, verbose=args.verbose)))
        if args.command == "ui":
            raise SystemExit(
                run_ui(
                    interval=args.interval,
                    stop_after_opportunities=args.stop_after_opportunities,
                    skip_db=args.skip_db,
                    demo=args.demo,
                )
            )
        if args.command == "streamlit-ui":
            raise SystemExit(
                run_streamlit_ui(
                    demo=args.demo,
                    skip_db=args.skip_db,
                    history_limit=args.history_limit,
                    interval=args.interval,
                )
            )
        if args.command == "scan":
            raise SystemExit(
                asyncio.run(
                    run_scan(
                        interval=args.interval,
                        skip_db=args.skip_db,
                        verbose=args.verbose,
                        stop_after_opportunities=args.stop_after_opportunities,
                    )
                )
            )
        raise SystemExit(2)
    except Exception:
        logger.exception("Application crashed")
        raise


if __name__ == "__main__":
    main()
