# Algorithmic Trading Arbitrage Bot

This project implements the Phase 1 and Phase 2 core of the provided spec:

- async market scanning for Polymarket and Kalshi
- cross-platform Dutch-book arbitrage detection
- fee and slippage estimation before surfacing opportunities
- SQLite persistence for market snapshots and opportunities
- a CLI with live and demo modes

## What it does

The bot looks for complementary positions across the two platforms:

- buy `YES` on one venue
- buy `NO` on the other venue
- if the combined cost is below `$1.00`, the spread can be profitable

The detector computes:

- gross gap
- estimated platform fees
- estimated slippage from limited liquidity
- final net profit per contract pair

## Setup

1. Install Python 3.11+.
2. Create and activate a virtual environment.
3. Install the package with `pip install -e .[dev]`.
4. Copy `.env.example` to `.env` and adjust values.
5. Pick a location for the SQLite database file in `SQLITE_PATH`.

## Run

Demo mode uses built-in sample markets and does not need network access:

```bash
arbitrage-bot demo
```

Single live scan:

```bash
arbitrage-bot once
```

Verbose live scan:

```bash
arbitrage-bot once --verbose
```

Continuous scan:

```bash
arbitrage-bot scan --interval 10
```

Verbose continuous scan:

```bash
arbitrage-bot scan --interval 10 --verbose
```

Skip writes to the database:

```bash
arbitrage-bot once --skip-db
```

## Current implementation notes

- `demo` is the easiest way to see the detector logic end-to-end without live API access.
- `once` and `scan` use official market endpoints from Polymarket Gamma and Kalshi REST v2.
- Matching is based on normalized question text plus expiry-date proximity, which is a practical Phase 2 baseline but not a perfect semantic matcher.
- Fees and slippage are intentionally conservative estimates for filtering opportunities before execution logic exists.
- The database is a local SQLite file, so no PostgreSQL server is required.

## Environment variables

- `SQLITE_PATH=./arbitrage_bot.db`
- `POLYMARKET_BASE_URL=https://gamma-api.polymarket.com`
- `KALSHI_BASE_URL=https://api.elections.kalshi.com/trade-api/v2`
- `HTTP_TIMEOUT_SECONDS=15`
- `MIN_DAYS_TO_EXPIRY=7`
- `MAX_DAYS_TO_EXPIRY=49`
- `MAX_MATCH_DATE_DRIFT_DAYS=2`
- `PROFIT_THRESHOLD=0.00`
- `SCAN_LIMIT=200`
