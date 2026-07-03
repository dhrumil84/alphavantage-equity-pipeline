"""
Build gold/fact_transcript_sentiment/fact_transcript_sentiment.parquet

Grain: (symbol, quarter) — one row per earnings call.

Aggregates silver/fact_transcript_turns into call-level sentiment, split by
speaker role so management spin can be compared against analyst reception:

  - role classification from the free-text `title` column:
      'operator'   title contains "operator"        (excluded from averages)
      'analyst'    title contains "analyst"
      'management' everything else with a title (CEO, CFO, VP IR, ...)
      'unknown'    null title
  - mgmt / analyst simple average sentiment, plus content-length-weighted
    averages (a long prepared-remarks monologue counts more than a "thanks")
  - ceo_cfo_avg_sentiment — narrows management to C-suite titles only
  - call_date joined from dim_earnings_calls

Cadence: weekly, in weekly_refresh.yml AFTER transform_earnings_transcripts.

Run as:  python -m transform_gold.build_transcript_sentiment
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from transform_gold.utils.duckdb_silver import duckdb_to_r2, silver_scan, overwrite_parquet  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

OUT_KEY = "gold/fact_transcript_sentiment/fact_transcript_sentiment.parquet"

CEO_CFO_PATTERN = r"chief executive|chief financial|\bCEO\b|\bCFO\b"


def main() -> None:
    bucket = os.environ["R2_BUCKET_NAME"]
    con = duckdb_to_r2()
    con.execute("SET enable_progress_bar = false;")

    turns_scan = silver_scan(bucket, "fact_transcript_turns/*.parquet")
    calls_scan = silver_scan(bucket, "dim_earnings_calls/*.parquet")

    sql = f"""
    WITH turns AS (
        SELECT
            symbol,
            quarter,
            CASE
                WHEN title IS NULL THEN 'unknown'
                WHEN title ILIKE '%operator%' THEN 'operator'
                WHEN title ILIKE '%analyst%' THEN 'analyst'
                ELSE 'management'
            END AS role,
            regexp_matches(COALESCE(title, ''), '{CEO_CFO_PATTERN}', 'i') AS is_ceo_cfo,
            sentiment,
            LENGTH(COALESCE(content, '')) AS content_len
        FROM {turns_scan}
        WHERE sentiment IS NOT NULL
    ),
    agg AS (
        SELECT
            symbol,
            quarter,
            COUNT(*)                                          AS scored_turns,
            COUNT(*) FILTER (role = 'management')             AS mgmt_turns,
            COUNT(*) FILTER (role = 'analyst')                AS analyst_turns,
            AVG(sentiment) FILTER (role IN ('management', 'analyst'))
                                                              AS overall_avg_sentiment,
            AVG(sentiment) FILTER (role = 'management')       AS mgmt_avg_sentiment,
            AVG(sentiment) FILTER (role = 'analyst')          AS analyst_avg_sentiment,
            AVG(sentiment) FILTER (is_ceo_cfo)                AS ceo_cfo_avg_sentiment,
            SUM(sentiment * content_len) FILTER (role = 'management')
                / NULLIF(SUM(content_len) FILTER (role = 'management'), 0)
                                                              AS mgmt_wtd_sentiment,
            SUM(sentiment * content_len) FILTER (role = 'analyst')
                / NULLIF(SUM(content_len) FILTER (role = 'analyst'), 0)
                                                              AS analyst_wtd_sentiment
        FROM turns
        GROUP BY symbol, quarter
    )
    SELECT
        a.*,
        a.mgmt_avg_sentiment - a.analyst_avg_sentiment AS mgmt_analyst_spread,
        c.call_date,
        c.participant_count
    FROM agg a
    LEFT JOIN {calls_scan} c USING (symbol, quarter)
    ORDER BY a.symbol, a.quarter
    """

    logger.info("Aggregating transcript turns into call-level sentiment...")
    df = con.execute(sql).df()
    logger.info(
        f"Built {len(df):,} (symbol, quarter) rows "
        f"({df['symbol'].nunique()} symbols)" if not df.empty else "No transcript data found."
    )

    if df.empty:
        logger.info("Nothing to write.")
        return

    df["gold_built_utc"] = datetime.now(timezone.utc).isoformat()
    overwrite_parquet(df, OUT_KEY)


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("build_transcript_sentiment"):
        main()
