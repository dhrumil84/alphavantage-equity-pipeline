"""
Ingest INSIDER_TRANSACTIONS per active ticker, weekly.

The API supports an optional `from=YYYY-MM-DD` parameter for incrementality.
We send today - 14 days (overlap to absorb late-filed Form 4s).

Bronze: bronze/insider_transactions/{symbol}/{pull_date}.json
"""

from __future__ import annotations

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
    from_date = (date.today() - timedelta(days=14)).isoformat()

    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "ticker_universe.csv")
    symbols = load_active_tickers(csv_path)
    total = len(symbols)
    if total == 0:
        logger.info("No active tickers found.")
        return

    logger.info(f"Ingesting INSIDER_TRANSACTIONS for {total} tickers from={from_date}")
    limiter = RateLimiter(calls_per_minute=75)

    for i, symbol in enumerate(symbols, start=1):
        r2_key = f"bronze/insider_transactions/{symbol}/{pull_date}.json"
        if r2_client.key_exists(r2_key):
            logger.info(f"[{i}/{total}] {symbol} — skipped (exists)")
            continue

        limiter.wait()
        try:
            data = av_client.fetch({
                "function": "INSIDER_TRANSACTIONS",
                "symbol": symbol,
                "from": from_date,
            })
            data["pull_date"] = pull_date
            data["symbol"] = symbol
            r2_client.upload_json(data, r2_key)
            row_count = len(data.get("data") or [])
            logger.info(f"[{i}/{total}] {symbol} — written ({row_count} txns)")
        except av_client.AlphaVantageError as e:
            logger.error(f"[{i}/{total}] {symbol} — Alpha Vantage API error: {e}")
        except Exception as e:
            logger.error(f"[{i}/{total}] {symbol} — Unexpected error: {e}")


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("ingest_insider_transactions"):
        main()
