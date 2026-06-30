"""
Ingest NEWS_SENTIMENT articles by topic feed (one call per supported topic per day).

Daily cadence; called from .github/workflows/daily_prices.yml. Cheap (~14 calls/day)
and captures the broad firehose. Per-ticker fill happens weekly in
ingest_news_tickers.py.

Bronze: bronze/news_sentiment_topic/{topic}/{pull_date}.json

Idempotent: skips a topic if today's file already exists in R2.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from ingestion.utils import av_client, r2_client
from ingestion.utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)

TOPICS = [
    "blockchain",
    "earnings",
    "ipo",
    "mergers_and_acquisitions",
    "financial_markets",
    "economy_fiscal",
    "economy_monetary",
    "economy_macro",
    "energy_transportation",
    "finance",
    "life_sciences",
    "manufacturing",
    "real_estate",
    "retail_wholesale",
    "technology",
]


def main() -> None:
    pull_date = datetime.now().strftime("%Y-%m-%d")
    # Articles published in the last 26 hours (1h overlap to absorb cron skew).
    time_from = (datetime.utcnow() - timedelta(hours=26)).strftime("%Y%m%dT%H%M")

    total = len(TOPICS)
    logger.info(f"Ingesting NEWS_SENTIMENT for {total} topics, time_from={time_from}")

    limiter = RateLimiter(calls_per_minute=75)

    for i, topic in enumerate(TOPICS, start=1):
        r2_key = f"bronze/news_sentiment_topic/{topic}/{pull_date}.json"
        if r2_client.key_exists(r2_key):
            logger.info(f"[{i}/{total}] {topic} — skipped (today's file exists)")
            continue

        limiter.wait()
        try:
            data = av_client.fetch({
                "function": "NEWS_SENTIMENT",
                "topics": topic,
                "time_from": time_from,
                "sort": "LATEST",
                "limit": 1000,
            })
            data["pull_date"] = pull_date
            data["topic"] = topic
            r2_client.upload_json(data, r2_key)
            item_count = len(data.get("feed", []))
            logger.info(f"[{i}/{total}] {topic} — written ({item_count} articles)")
        except av_client.AlphaVantageError as e:
            logger.error(f"[{i}/{total}] {topic} — Alpha Vantage API error: {e}")
        except Exception as e:
            logger.error(f"[{i}/{total}] {topic} — Unexpected error: {e}")


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("ingest_news_topics"):
        main()
