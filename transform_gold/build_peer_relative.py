"""
Build gold/fact_peer_relative/fact_peer_relative.parquet

One row per (symbol, as_of_date, metric) with:
  - value                   the symbol's raw metric value
  - sector / industry       its group labels
  - sector_percentile       PERCENT_RANK() within sector partition (0..1)
  - industry_percentile     PERCENT_RANK() within industry partition (0..1)
  - sector_zscore           (value − sector_mean) / sector_stddev
  - industry_zscore         (value − industry_mean) / industry_stddev
  - sector_n / industry_n   group sizes (for downstream filtering of small N)

Sources:
  - fact_valuation_daily (latest trade_date snapshot) → valuation metrics
  - fact_fundamentals_wide (latest quarterly row per symbol) → fundamentals
  - dim_company → sector/industry labels (canonical, trumps any cached
    labels in fact_valuation_daily)

The as_of_date matches the snapshot in fact_sector_aggregates — these two
tables are designed to be queried together.

Cadence: weekly, in weekly_refresh.yml AFTER build_sector_aggregates.

Run as:  python -m transform_gold.build_peer_relative
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from transform_gold.utils.duckdb_silver import (  # noqa: E402
    duckdb_to_r2, silver_scan, gold_scan, overwrite_parquet,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

OUT_KEY = "gold/fact_peer_relative/fact_peer_relative.parquet"

VALUATION_METRICS = [
    "pe_ttm", "ps_ttm", "pb", "ev_ebitda_ttm", "fcf_yield_ttm", "dividend_yield_ttm",
]
FUNDAMENTAL_METRICS = [
    "revenue_growth_yoy", "eps_growth_yoy",
    "gross_margin", "operating_margin", "net_margin",
    "roe", "roic",
]


def main():
    bucket = os.environ["R2_BUCKET_NAME"]
    con = duckdb_to_r2()
    con.execute("SET enable_progress_bar = false;")

    as_of_date = con.execute(f"""
        SELECT MAX(trade_date)
        FROM {gold_scan(bucket, 'fact_valuation_daily/**/*.parquet')}
    """).fetchone()[0]
    logger.info(f"Snapshot as_of_date = {as_of_date}")

    val_select = ",\n        ".join(VALUATION_METRICS)
    fund_select = ",\n        ".join(FUNDAMENTAL_METRICS)
    val_unpivot = " UNION ALL ".join([
        f"SELECT symbol, '{m}' AS metric, {m} AS value FROM v" for m in VALUATION_METRICS
    ])
    fund_unpivot = " UNION ALL ".join([
        f"SELECT symbol, '{m}' AS metric, {m} AS value FROM f_latest" for m in FUNDAMENTAL_METRICS
    ])

    sql = f"""
    WITH v AS (
        SELECT symbol,
            {val_select}
        FROM {gold_scan(bucket, 'fact_valuation_daily/**/*.parquet')}
        WHERE trade_date = DATE '{as_of_date.isoformat()}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC) = 1
    ),
    f_latest AS (
        SELECT symbol,
            {fund_select}
        FROM {gold_scan(bucket, 'fact_fundamentals_wide/*.parquet')}
        WHERE period_type = 'quarterly'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY symbol ORDER BY fiscal_date_ending DESC
        ) = 1
    ),
    dim AS (
        SELECT symbol, sector, industry
        FROM {silver_scan(bucket, 'dim_company/*.parquet')}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY last_updated DESC NULLS LAST) = 1
    ),
    val_long AS ({val_unpivot}),
    fund_long AS ({fund_unpivot}),
    all_long AS (
        SELECT * FROM val_long
        UNION ALL
        SELECT * FROM fund_long
    ),
    enriched AS (
        SELECT
            a.symbol, a.metric, a.value,
            d.sector, d.industry
        FROM all_long a
        LEFT JOIN dim d USING (symbol)
        WHERE a.value IS NOT NULL AND a.value = a.value
    ),
    ranked AS (
        SELECT
            symbol, metric, value, sector, industry,
            CASE WHEN sector IS NOT NULL
                 THEN PERCENT_RANK() OVER (PARTITION BY sector, metric ORDER BY value)
                 END AS sector_percentile,
            CASE WHEN industry IS NOT NULL
                 THEN PERCENT_RANK() OVER (PARTITION BY industry, metric ORDER BY value)
                 END AS industry_percentile,
            AVG(value)    OVER (PARTITION BY sector, metric)   AS sector_mean,
            STDDEV(value) OVER (PARTITION BY sector, metric)   AS sector_stddev,
            COUNT(*)      OVER (PARTITION BY sector, metric)   AS sector_n,
            AVG(value)    OVER (PARTITION BY industry, metric) AS industry_mean,
            STDDEV(value) OVER (PARTITION BY industry, metric) AS industry_stddev,
            COUNT(*)      OVER (PARTITION BY industry, metric) AS industry_n
        FROM enriched
    )
    SELECT
        DATE '{as_of_date.isoformat()}' AS as_of_date,
        symbol, metric, value, sector, industry,
        sector_percentile, industry_percentile,
        CASE WHEN sector_stddev > 0
             THEN (value - sector_mean) / sector_stddev END AS sector_zscore,
        CASE WHEN industry_stddev > 0
             THEN (value - industry_mean) / industry_stddev END AS industry_zscore,
        sector_n, industry_n
    FROM ranked
    """

    df = con.execute(sql).df()
    df["gold_built_utc"] = datetime.now(timezone.utc).isoformat()

    logger.info(
        f"Built {len(df):,} peer-relative rows "
        f"({df['symbol'].nunique()} symbols, {df['metric'].nunique()} metrics) "
        f"as of {as_of_date}"
    )

    overwrite_parquet(df, OUT_KEY)


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("build_peer_relative"):
        main()
