"""
Ingest INDEX_DATA (daily OHLC) for each active index in config/index_universe.csv.

Mirrors ingestion/ingest_daily_prices.py: --mode {full,incremental}.

  full:        outputsize=full; skip-logic checks silver/fact_index_prices min_date.
               A symbol is "already full" if silver min_date is on/before the
               oldest available history (we use 1990-01-01 as the floor since
               not all indices go back as far as equities).

  incremental: skip if today's bronze file exists.

Bronze: bronze/index_data/{index_symbol}/{pull_date}.json
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
from datetime import date, datetime
from typing import Dict, List

from ingestion.utils import av_client, r2_client
from ingestion.utils.rate_limiter import RateLimiter
from transform_gold.utils.duckdb_silver import duckdb_to_r2, silver_scan

logger = logging.getLogger(__name__)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)

FLOOR_DATE = date(1990, 1, 1)


def load_active_indices(csv_path: str) -> List[str]:
    out: List[str] = []
    if not os.path.exists(csv_path):
        logger.error(f"Cannot find index universe file at {csv_path}")
        return out
    with open(csv_path, mode="r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("active", "").lower() == "true":
                out.append(row["symbol"].strip())
    return out


def _load_silver_min_dates(bucket: str) -> Dict[str, date]:
    try:
        con = duckdb_to_r2()
        rows = con.execute(f"""
            SELECT index_symbol, MIN(trade_date)::DATE
            FROM {silver_scan(bucket, 'fact_index_prices/**/*.parquet')}
            GROUP BY index_symbol
        """).fetchall()
        return {r[0]: r[1] for r in rows}
    except Exception as e:
        logger.warning(f"Could not load silver min dates (will re-fetch all): {e}")
        return {}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["full", "incremental"], required=True)
    args = parser.parse_args()

    pull_date = datetime.now().strftime("%Y-%m-%d")

    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "index_universe.csv")
    indices = load_active_indices(csv_path)
    total = len(indices)
    if total == 0:
        logger.info("No active indices in config/index_universe.csv.")
        return

    bucket = os.environ.get("R2_BUCKET_NAME", "")
    silver_min_dates = _load_silver_min_dates(bucket) if args.mode == "full" else {}

    logger.info(f"Starting {args.mode} INDEX_DATA ingestion for {total} indices.")
    limiter = RateLimiter(calls_per_minute=75)

    for i, sym in enumerate(indices, start=1):
        r2_key = f"bronze/index_data/{sym}/{pull_date}.json"

        if args.mode == "incremental" and r2_client.key_exists(r2_key):
            logger.info(f"[{i}/{total}] {sym} — skipped (today's file exists)")
            continue

        if args.mode == "full":
            min_date = silver_min_dates.get(sym)
            if min_date is not None and min_date <= FLOOR_DATE:
                logger.info(f"[{i}/{total}] {sym} — skipped (silver min_date={min_date})")
                continue

        limiter.wait()
        try:
            data = av_client.fetch({
                "function": "INDEX_DATA",
                "symbol": sym,
                "interval": "daily",
            })
            data["pull_date"] = pull_date
            data["symbol"] = sym
            r2_client.upload_json(data, r2_key)
            logger.info(f"[{i}/{total}] {sym} — written")
        except av_client.AlphaVantageError as e:
            logger.error(f"[{i}/{total}] {sym} — Alpha Vantage API error: {e}")
        except Exception as e:
            logger.error(f"[{i}/{total}] {sym} — Unexpected error: {e}")


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("ingest_index_data"):
        main()
