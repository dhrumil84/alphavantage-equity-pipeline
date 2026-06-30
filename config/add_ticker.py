"""
Add one or more individual tickers to config/ticker_universe.csv.

Use this when you spot a name you want to track that isn't already in the SPTM
anchor universe (e.g. Cloudflare NET, Coinbase COIN). Idempotent — re-running
with the same symbols is a no-op.

For each symbol passed on argv, this script calls Alpha Vantage OVERVIEW once
to fetch the proper company name, then appends a row to ticker_universe.csv.
Empty OVERVIEW responses (common for ETFs / ADRs) fall back to using the
symbol as the name.

Usage (from project root):
    python -m config.add_ticker NET
    python -m config.add_ticker NET COIN PLTR

After adding, the next scheduled daily_prices.yml + weekly_refresh.yml runs
will full-backfill history for the new symbols automatically. To seed faster,
trigger daily_prices.yml manually with mode=full.
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path

from ingestion.utils import av_client
from ingestion.utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)

UNIVERSE_PATH = Path(__file__).resolve().parent / "ticker_universe.csv"


def _fetch_name(symbol: str) -> str:
    """Return the company Name from OVERVIEW, or the symbol if unavailable."""
    try:
        data = av_client.fetch({"function": "OVERVIEW", "symbol": symbol})
    except av_client.AlphaVantageError as e:
        logger.warning(f"  OVERVIEW failed for {symbol} ({e}); using symbol as name")
        return symbol
    name = (data or {}).get("Name") or ""
    if not name:
        logger.warning(f"  OVERVIEW returned empty for {symbol} (likely ETF/ADR); using symbol as name")
        return symbol
    return name


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("symbols", nargs="+", help="One or more ticker symbols, e.g. NET COIN")
    parser.add_argument(
        "--no-overview",
        action="store_true",
        help="Skip the AV OVERVIEW lookup and use the symbol as the name (saves 1 API call per symbol).",
    )
    args = parser.parse_args()

    requested = [s.strip().upper() for s in args.symbols if s.strip()]
    if not requested:
        logger.error("No symbols provided.")
        return 2

    with UNIVERSE_PATH.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        fieldnames = list(rows[0].keys()) if rows else ["symbol", "name", "active"]

    existing = {r["symbol"].strip().upper() for r in rows}

    limiter = RateLimiter(calls_per_minute=75)
    added: list[tuple[str, str]] = []
    for sym in requested:
        if sym in existing:
            logger.info(f"  [skip] {sym} already present")
            continue
        if args.no_overview:
            name = sym
        else:
            limiter.wait()
            name = _fetch_name(sym)
        rows.append({"symbol": sym, "name": name, "active": "true"})
        existing.add(sym)
        added.append((sym, name))
        logger.info(f"  [add]  {sym:<8} {name}")

    if not added:
        logger.info("All requested symbols already in universe — no changes.")
        return 0

    with UNIVERSE_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logger.info("")
    logger.info(f"Added {len(added)} ticker(s). Universe now {len(rows)} rows.")
    logger.info("")
    logger.info("Next: the regularly-scheduled daily_prices.yml and weekly_refresh.yml")
    logger.info("workflows will pick up the new symbols on their next run. To seed 20y")
    logger.info("history immediately, manually trigger daily_prices.yml with mode=full.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
