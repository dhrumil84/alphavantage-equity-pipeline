"""
Build two silver tables from NEWS_SENTIMENT bronze (both topic and ticker feeds):

  silver/fact_news_articles                 grain: url
  silver/fact_news_ticker_sentiment         grain: (url, ticker)

Reads both bronze prefixes:
  - bronze/news_sentiment_topic/<topic>/<pull_date>.json (daily)
  - bronze/news_sentiment_ticker/<symbol>/<pull_date>.json (weekly)

Articles can appear in multiple feeds; dedup on url. Latest pull_date wins.

Strategy: scan only the latest pull_date file per (topic|symbol) folder to keep
the run bounded. Historic bronze stays in place — re-running with a wider scan
would still upsert correctly.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import List

import pandas as pd

from ingestion.utils import r2_client
from transform.utils.parquet_writer import upsert_parquet

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _latest_files(prefix: str) -> List[str]:
    """Return the latest pull_date file per second-level folder under prefix."""
    keys = r2_client.list_keys(prefix)
    by_folder: dict[str, list[tuple[str, str]]] = {}
    for k in keys:
        parts = k.split("/")
        if len(parts) < 4 or not k.endswith(".json"):
            continue
        folder = parts[2]
        pull_date = parts[3].replace(".json", "")
        by_folder.setdefault(folder, []).append((pull_date, k))
    return [sorted(v)[-1][1] for v in by_folder.values()]


def _parse_published(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.strptime(ts, "%Y%m%dT%H%M%S")
    except ValueError:
        try:
            return datetime.strptime(ts, "%Y%m%dT%H%M")
        except ValueError:
            return None


def _safe_float(v) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def main() -> None:
    keys = _latest_files("bronze/news_sentiment_topic/") + _latest_files("bronze/news_sentiment_ticker/")
    if not keys:
        logger.info("No news bronze files found.")
        return
    logger.info(f"Processing {len(keys)} latest bronze files")

    articles: list[dict] = []
    ticker_sentiment: list[dict] = []
    seen_urls: set[str] = set()

    for key in keys:
        try:
            data = r2_client.download_json(key)
        except Exception as e:
            logger.error(f"Failed to read {key}: {e}")
            continue

        pull_date = data.get("pull_date") or key.split("/")[-1].replace(".json", "")
        feed = data.get("feed") or []
        for art in feed:
            url = art.get("url")
            if not url or url in seen_urls:
                # Article may already be captured this run; let upsert_parquet dedup across runs.
                if url:
                    # Still need to capture per-ticker rows even for repeated articles in same run.
                    pass
                else:
                    continue
            if url not in seen_urls:
                seen_urls.add(url)
                articles.append({
                    "url": url,
                    "title": art.get("title"),
                    "time_published": _parse_published(art.get("time_published")),
                    "authors": json.dumps(art.get("authors") or []),
                    "summary": art.get("summary"),
                    "source": art.get("source"),
                    "source_domain": art.get("source_domain"),
                    "category_within_source": art.get("category_within_source"),
                    "overall_sentiment_score": _safe_float(art.get("overall_sentiment_score")),
                    "overall_sentiment_label": art.get("overall_sentiment_label"),
                    "topics": json.dumps(art.get("topics") or []),
                    "banner_image": art.get("banner_image"),
                    "pull_date": pull_date,
                })

            for ts in art.get("ticker_sentiment") or []:
                ticker_sentiment.append({
                    "url": url,
                    "ticker": ts.get("ticker"),
                    "relevance_score": _safe_float(ts.get("relevance_score")),
                    "ticker_sentiment_score": _safe_float(ts.get("ticker_sentiment_score")),
                    "ticker_sentiment_label": ts.get("ticker_sentiment_label"),
                    "pull_date": pull_date,
                })

    if not articles:
        logger.info("No articles parsed.")
        return

    articles_df = pd.DataFrame(articles)
    ts_df = pd.DataFrame(ticker_sentiment).drop_duplicates(subset=["url", "ticker"])

    logger.info(f"Parsed {len(articles_df)} articles, {len(ts_df)} ticker-sentiment rows")

    upsert_parquet(
        articles_df,
        "silver/fact_news_articles/fact_news_articles.parquet",
        dedup_keys=["url"],
    )
    upsert_parquet(
        ts_df,
        "silver/fact_news_ticker_sentiment/fact_news_ticker_sentiment.parquet",
        dedup_keys=["url", "ticker"],
    )
    logger.info("News sentiment transform completed.")


if __name__ == "__main__":
    main()
