"""
Build gold/fact_sentiment_daily/year=YYYY/fact_sentiment_daily.parquet

One row per (symbol, sentiment_date). Joins fact_news_articles with
fact_news_ticker_sentiment and aggregates daily per ticker:

  - article_count
  - avg_sentiment_score
  - weighted_avg_sentiment (weighted by ticker-level relevance_score)
  - bullish_count    (label in {'Bullish','Somewhat-Bullish'})
  - bearish_count    (label in {'Bearish','Somewhat-Bearish'})

sentiment_date is the UTC calendar date of `time_published`.

Joinable to fact_valuation_daily on (symbol, trade_date = sentiment_date) for
sentiment-as-feature analytics.

Strategy: full rebuild every day (cheap; sentiment data is small relative to prices).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from transform_gold.utils.duckdb_silver import duckdb_to_r2, silver_scan, overwrite_parquet  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

OUT_PREFIX = "gold/fact_sentiment_daily"


def main() -> None:
    bucket = os.environ["R2_BUCKET_NAME"]
    con = duckdb_to_r2()

    articles_scan = silver_scan(bucket, "fact_news_articles/fact_news_articles.parquet")
    ts_scan = silver_scan(bucket, "fact_news_ticker_sentiment/fact_news_ticker_sentiment.parquet")

    sql = f"""
        WITH joined AS (
            SELECT
                ts.ticker                                AS symbol,
                CAST(a.time_published AS DATE)           AS sentiment_date,
                ts.ticker_sentiment_score,
                ts.relevance_score,
                ts.ticker_sentiment_label
            FROM {ts_scan} ts
            JOIN {articles_scan} a USING (url)
            WHERE a.time_published IS NOT NULL
              AND ts.ticker IS NOT NULL
        )
        SELECT
            symbol,
            sentiment_date,
            COUNT(*)                                              AS article_count,
            AVG(ticker_sentiment_score)                           AS avg_sentiment_score,
            SUM(ticker_sentiment_score * COALESCE(relevance_score, 0))
              / NULLIF(SUM(COALESCE(relevance_score, 0)), 0)      AS weighted_avg_sentiment,
            SUM(CASE WHEN ticker_sentiment_label IN ('Bullish', 'Somewhat-Bullish') THEN 1 ELSE 0 END) AS bullish_count,
            SUM(CASE WHEN ticker_sentiment_label IN ('Bearish', 'Somewhat-Bearish') THEN 1 ELSE 0 END) AS bearish_count
        FROM joined
        GROUP BY symbol, sentiment_date
    """

    logger.info("Aggregating per-ticker daily sentiment from silver...")
    df = con.execute(sql).df()
    logger.info(f"Built {len(df):,} (symbol, sentiment_date) rows")

    if df.empty:
        logger.info("No sentiment data to write.")
        return

    df["sentiment_date"] = pd.to_datetime(df["sentiment_date"]).dt.date
    df["gold_built_utc"] = datetime.now(timezone.utc).isoformat()

    df["_year"] = pd.to_datetime(df["sentiment_date"]).dt.year
    for year, year_df in df.groupby("_year"):
        year_df = year_df.drop(columns=["_year"])
        key = f"{OUT_PREFIX}/year={int(year)}/fact_sentiment_daily.parquet"
        overwrite_parquet(year_df, key)


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("build_news_sentiment_daily"):
        main()
