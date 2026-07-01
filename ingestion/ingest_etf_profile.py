"""
Ingest ETF_PROFILE for active ETFs, weekly.

Symbol list = union of:
  1. Hardcoded seed list of core ETFs (SPY/QQQ/IVV/VOO/VTI/IWM/DIA/SPTM + SPDR
     sector ETFs).
  2. All symbols in silver/dim_company where asset_type='ETF' AND active=true.

Bronze: bronze/etf_profile/{symbol}/{pull_date}.json
"""

from __future__ import annotations

import csv
import logging
import os
from datetime import datetime
from typing import List, Set

from ingestion.utils import av_client, r2_client
from ingestion.utils.freshness import ENDPOINT_TTL_DAYS, build_fresh_symbol_set
from ingestion.utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)

SEED_ETFS = [
    "SPY", "QQQ", "IVV", "VOO", "VTI", "IWM", "DIA", "SPTM", "ITOT",
    "XLE", "XLF", "XLK", "XLV", "XLY", "XLP", "XLI", "XLU", "XLB", "XLRE", "XLC",
]


def _universe_etfs(csv_path: str) -> Set[str]:
    """Return active symbols from ticker_universe.csv that are ETFs in dim_company."""
    universe: Set[str] = set()
    if not os.path.exists(csv_path):
        return universe
    with open(csv_path, mode="r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("active", "").lower() == "true":
                universe.add(row["symbol"].strip())

    try:
        from transform_gold.utils.duckdb_silver import duckdb_to_r2, silver_scan
        bucket = os.environ.get("R2_BUCKET_NAME", "")
        con = duckdb_to_r2()
        rows = con.execute(f"""
            SELECT symbol FROM {silver_scan(bucket, 'dim_company/dim_company.parquet')}
            WHERE LOWER(asset_type) IN ('etf', 'fund')
        """).fetchall()
        return universe & {r[0] for r in rows}
    except Exception as e:
        logger.warning(f"Could not filter universe to ETFs via dim_company ({e}); using seed list only.")
        return set()


def main() -> None:
    pull_date = datetime.now().strftime("%Y-%m-%d")

    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "ticker_universe.csv")
    symbols = sorted(set(SEED_ETFS) | _universe_etfs(csv_path))
    total = len(symbols)
    if total == 0:
        logger.info("No ETF symbols to ingest.")
        return

    logger.info(f"Ingesting ETF_PROFILE for {total} ETFs")

    fresh = build_fresh_symbol_set("etf_profile", ENDPOINT_TTL_DAYS["etf_profile"])

    limiter = RateLimiter(calls_per_minute=75)

    for i, symbol in enumerate(symbols, start=1):
        if symbol in fresh:
            logger.info(f"[{i}/{total}] {symbol} — skipped (fresh)")
            continue

        r2_key = f"bronze/etf_profile/{symbol}/{pull_date}.json"
        if r2_client.key_exists(r2_key):
            logger.info(f"[{i}/{total}] {symbol} — skipped (exists)")
            continue

        limiter.wait()
        try:
            data = av_client.fetch({"function": "ETF_PROFILE", "symbol": symbol})
            data["pull_date"] = pull_date
            data["symbol"] = symbol
            r2_client.upload_json(data, r2_key)
            n_h = len(data.get("holdings") or [])
            n_s = len(data.get("sectors") or [])
            logger.info(f"[{i}/{total}] {symbol} — written ({n_h} holdings, {n_s} sectors)")
        except av_client.AlphaVantageError as e:
            logger.error(f"[{i}/{total}] {symbol} — Alpha Vantage API error: {e}")
        except Exception as e:
            logger.error(f"[{i}/{total}] {symbol} — Unexpected error: {e}")


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("ingest_etf_profile"):
        main()
