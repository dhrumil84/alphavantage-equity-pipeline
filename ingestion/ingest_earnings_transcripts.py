"""
Ingest EARNINGS_CALL_TRANSCRIPT bronze files, one per (symbol, quarter).

Two modes via --mode {backfill,forward}:

  backfill (one-time): for each active ticker, enumerate quarters from
    --start-quarter (default 2021Q1, last 5 years) through the current quarter.
    Skip any (symbol, quarter) that already exists in bronze. Wall time at
    75 calls/min: ~30K calls x 0.8s ~ 7 hours for 1,500 tickers x 20 quarters.

  forward (default, weekly): join silver/fact_earnings for report_dates within
    the last LOOKBACK_DAYS (60). For each (symbol, fiscal_date_ending), derive
    quarter = YYYYQN from fiscal_date_ending and fetch only those. Skip if
    bronze file already exists.

Bronze path is (symbol, quarter)-keyed (NOT pull-date-keyed) because transcripts
are immutable per quarter:

  bronze/earnings_transcripts/{symbol}/{quarter}.json

Quarter derivation in forward mode uses the calendar quarter of
fiscal_date_ending. Companies with non-calendar fiscal years (e.g. AAPL Sep
year-end) may end up with a slightly off quarter label — that's acceptable
since the API accepts any valid quarter string and the canonical quarter is
what AV itself returns in the response.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
from datetime import date, datetime, timedelta
from typing import List

from ingestion.utils import av_client, r2_client
from ingestion.utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)

BACKFILL_START_YEAR = 2021
BACKFILL_START_Q = 1
LOOKBACK_DAYS = 60


def _parse_quarter(s: str) -> tuple[int, int]:
    """Parse 'YYYYQN' → (year, q). Raises ValueError on malformed input."""
    s = s.strip().upper()
    if len(s) != 6 or s[4] != "Q" or not s[:4].isdigit() or s[5] not in "1234":
        raise ValueError(f"start-quarter must be in YYYYQN format (e.g. 2016Q1), got: {s!r}")
    return int(s[:4]), int(s[5])


def load_active_tickers(csv_path: str) -> List[str]:
    symbols: List[str] = []
    if not os.path.exists(csv_path):
        logger.error(f"Cannot find ticker universe file at {csv_path}")
        return symbols
    with open(csv_path, mode="r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("active", "").lower() == "true":
                symbols.append(row["symbol"].strip())
    return symbols


def _quarter_of(d: date) -> str:
    return f"{d.year}Q{(d.month - 1) // 3 + 1}"


def _all_backfill_quarters(start_year: int, start_q: int) -> List[str]:
    """Return ['{start}', ..., current_quarter]."""
    today = date.today()
    end_year, end_q = today.year, (today.month - 1) // 3 + 1
    out: List[str] = []
    y, q = start_year, start_q
    while (y, q) <= (end_year, end_q):
        out.append(f"{y}Q{q}")
        q += 1
        if q > 4:
            q = 1
            y += 1
    return out


def _forward_targets() -> List[tuple[str, str]]:
    """For each (symbol, recent fiscal_date_ending) in silver/fact_earnings within
    the lookback window, return (symbol, quarter)."""
    from transform_gold.utils.duckdb_silver import duckdb_to_r2, silver_scan

    bucket = os.environ.get("R2_BUCKET_NAME", "")
    cutoff = (date.today() - timedelta(days=LOOKBACK_DAYS)).isoformat()
    try:
        con = duckdb_to_r2()
        rows = con.execute(f"""
            SELECT DISTINCT symbol, CAST(fiscal_date_ending AS DATE) AS fde
            FROM {silver_scan(bucket, 'fact_earnings/fact_earnings.parquet')}
            WHERE period_type = 'quarterly'
              AND report_date IS NOT NULL
              AND CAST(report_date AS DATE) >= DATE '{cutoff}'
        """).fetchall()
        return [(r[0], _quarter_of(r[1])) for r in rows]
    except Exception as e:
        logger.warning(f"Could not load forward targets from silver fact_earnings ({e}); "
                       f"forward mode will be a no-op this run.")
        return []


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode", choices=["backfill", "forward"], default="forward",
        help="backfill (one-time) enumerates all quarters from --start-quarter "
             "through current; forward (default) reads recent report_dates from "
             "silver fact_earnings.",
    )
    parser.add_argument(
        "--start-quarter", default=f"{BACKFILL_START_YEAR}Q{BACKFILL_START_Q}",
        help="Backfill mode only. YYYYQN format (e.g. 2016Q1 for 10-year history). "
             f"Default: {BACKFILL_START_YEAR}Q{BACKFILL_START_Q}.",
    )
    args = parser.parse_args()

    if args.mode == "backfill":
        start_year, start_q = _parse_quarter(args.start_quarter)
        csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "ticker_universe.csv")
        symbols = load_active_tickers(csv_path)
        quarters = _all_backfill_quarters(start_year, start_q)
        logger.info(f"Backfill from {args.start_quarter}: {len(quarters)} quarters × {len(symbols)} symbols")
        targets = [(s, q) for s in symbols for q in quarters]
    else:
        targets = _forward_targets()

    total = len(targets)
    if total == 0:
        logger.info("No (symbol, quarter) targets to fetch.")
        return
    logger.info(f"Ingesting EARNINGS_CALL_TRANSCRIPT for {total} (symbol, quarter) pairs (mode={args.mode})")
    limiter = RateLimiter(calls_per_minute=75)

    for i, (symbol, quarter) in enumerate(targets, start=1):
        r2_key = f"bronze/earnings_transcripts/{symbol}/{quarter}.json"
        if r2_client.key_exists(r2_key):
            logger.info(f"[{i}/{total}] {symbol} {quarter} — skipped (exists)")
            continue

        limiter.wait()
        try:
            data = av_client.fetch({
                "function": "EARNINGS_CALL_TRANSCRIPT",
                "symbol": symbol,
                "quarter": quarter,
            })
            data["pull_date"] = datetime.now().strftime("%Y-%m-%d")
            r2_client.upload_json(data, r2_key)
            turn_count = len(data.get("transcript") or [])
            logger.info(f"[{i}/{total}] {symbol} {quarter} — written ({turn_count} turns)")
        except av_client.AlphaVantageError as e:
            logger.error(f"[{i}/{total}] {symbol} {quarter} — Alpha Vantage API error: {e}")
        except Exception as e:
            logger.error(f"[{i}/{total}] {symbol} {quarter} — Unexpected error: {e}")


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("ingest_earnings_transcripts"):
        main()
