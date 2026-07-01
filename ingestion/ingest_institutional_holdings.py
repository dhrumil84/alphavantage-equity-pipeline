"""
Ingest INSTITUTIONAL_HOLDINGS per active ticker, weekly.

13F filings update quarterly so a weekly cadence is more than sufficient.
No incrementality param on this endpoint — every call returns the full
ownership snapshot.

Bronze: bronze/institutional_holdings/{symbol}/{pull_date}.json
"""

from __future__ import annotations

import csv
import logging
import os
from datetime import datetime
from typing import List

from ingestion.utils import av_client, r2_client
from ingestion.utils.freshness import ENDPOINT_TTL_DAYS, build_fresh_symbol_set
from ingestion.utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)


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


def main() -> None:
    pull_date = datetime.now().strftime("%Y-%m-%d")

    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "ticker_universe.csv")
    symbols = load_active_tickers(csv_path)
    total = len(symbols)
    if total == 0:
        logger.info("No active tickers found.")
        return

    logger.info(f"Ingesting INSTITUTIONAL_HOLDINGS for {total} tickers")

    fresh = build_fresh_symbol_set("institutional_holdings", ENDPOINT_TTL_DAYS["institutional_holdings"])

    limiter = RateLimiter(calls_per_minute=75)

    for i, symbol in enumerate(symbols, start=1):
        if symbol in fresh:
            logger.info(f"[{i}/{total}] {symbol} — skipped (fresh)")
            continue

        r2_key = f"bronze/institutional_holdings/{symbol}/{pull_date}.json"
        if r2_client.key_exists(r2_key):
            logger.info(f"[{i}/{total}] {symbol} — skipped (exists)")
            continue

        limiter.wait()
        try:
            data = av_client.fetch({"function": "INSTITUTIONAL_HOLDINGS", "symbol": symbol})
            data["pull_date"] = pull_date
            r2_client.upload_json(data, r2_key)
            n_holders = len(data.get("holdings") or [])
            logger.info(f"[{i}/{total}] {symbol} — written ({n_holders} holders)")
        except av_client.AlphaVantageError as e:
            logger.error(f"[{i}/{total}] {symbol} — Alpha Vantage API error: {e}")
        except Exception as e:
            logger.error(f"[{i}/{total}] {symbol} — Unexpected error: {e}")


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("ingest_institutional_holdings"):
        main()
