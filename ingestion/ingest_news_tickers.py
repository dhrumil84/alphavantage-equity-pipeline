"""
Ingest NEWS_SENTIMENT articles per ticker, weekly.

For each active ticker in config/ticker_universe.csv, fetch articles published
in the last ~8 days (1-day overlap). Runs in .github/workflows/weekly_refresh.yml.

Bronze: bronze/news_sentiment_ticker/{symbol}/{pull_date}.json

Idempotent: skips a ticker if today's file already exists in R2.
"""

from __future__ import annotations

import csv
import logging
import os
from datetime import datetime, timedelta
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
    time_from = (datetime.utcnow() - timedelta(days=8)).strftime("%Y%m%dT0000")

    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "ticker_universe.csv")
    symbols = load_active_tickers(csv_path)
    total = len(symbols)
    if total == 0:
        logger.info("No active tickers found.")
        return

    logger.info(f"Ingesting NEWS_SENTIMENT for {total} tickers, time_from={time_from}")
    limiter = RateLimiter(calls_per_minute=75)

    for i, symbol in enumerate(symbols, start=1):
        r2_key = f"bronze/news_sentiment_ticker/{symbol}/{pull_date}.json"
        if r2_client.key_exists(r2_key):
            logger.info(f"[{i}/{total}] {symbol} — skipped (today's file exists)")
            continue

        limiter.wait()
        try:
            data = av_client.fetch({
                "function": "NEWS_SENTIMENT",
                "tickers": symbol,
                "time_from": time_from,
                "sort": "LATEST",
                "limit": 1000,
            })
            data["pull_date"] = pull_date
            data["symbol"] = symbol
            r2_client.upload_json(data, r2_key)
            item_count = len(data.get("feed", []))
            logger.info(f"[{i}/{total}] {symbol} — written ({item_count} articles)")
        except av_client.AlphaVantageError as e:
            logger.error(f"[{i}/{total}] {symbol} — Alpha Vantage API error: {e}")
        except Exception as e:
            logger.error(f"[{i}/{total}] {symbol} — Unexpected error: {e}")


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("ingest_news_tickers"):
        main()
