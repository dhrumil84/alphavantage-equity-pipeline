"""
Build gold/fact_sector_aggregates/fact_sector_aggregates.parquet

Long-format aggregate table. One row per (as_of_date, grouping_level,
group_value, metric) with count / mean / median / p25 / p75.

  grouping_level ∈ {'sector', 'industry'}
  group_value     = the sector or industry name
  metric          = the metric name being aggregated

Sources:
  - Valuation metrics (pe_ttm, ps_ttm, pb, ev_ebitda_ttm, fcf_yield_ttm,
    dividend_yield_ttm) ← fact_valuation_daily, latest trade_date snapshot
  - Fundamentals metrics (revenue_growth_yoy, eps_growth_yoy, gross_margin,
    operating_margin, net_margin, roe, roic) ← fact_fundamentals_wide,
    latest quarterly row per symbol

Date scope: one snapshot per rebuild (the latest trade_date in
fact_valuation_daily). Peer-relative ranks key off this same date.

Cadence: weekly, in weekly_refresh.yml AFTER build_fundamentals_wide
(and AFTER the daily run of build_valuation_daily produces the latest
snapshot — in practice the weekly cron lands after the daily cron has
already updated fact_valuation_daily, so the latest as-of-date is fresh).

Run as:  python -m transform_gold.build_sector_aggregates
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

OUT_KEY = "gold/fact_sector_aggregates/fact_sector_aggregates.parquet"

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

    # 1) Find the latest trade_date in fact_valuation_daily — this is the
    # snapshot's as_of_date.
    as_of_date = con.execute(f"""
        SELECT MAX(trade_date)
        FROM {gold_scan(bucket, 'fact_valuation_daily/**/*.parquet')}
    """).fetchone()[0]
    logger.info(f"Snapshot as_of_date = {as_of_date}")

    # 2) Build a single tall (symbol, metric, value, sector, industry) table
    # covering both the daily valuation snapshot and the latest quarterly
    # fundamentals snapshot per symbol.
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
            a.symbol,
            a.metric,
            a.value,
            d.sector,
            d.industry
        FROM all_long a
        LEFT JOIN dim d USING (symbol)
        WHERE a.value IS NOT NULL
          AND a.value = a.value   -- drop NaN
    ),
    sector_agg AS (
        SELECT
            DATE '{as_of_date.isoformat()}' AS as_of_date,
            'sector' AS grouping_level,
            sector AS group_value,
            metric,
            COUNT(*) AS n,
            AVG(value) AS mean,
            quantile_cont(value, 0.5)  AS median,
            quantile_cont(value, 0.25) AS p25,
            quantile_cont(value, 0.75) AS p75
        FROM enriched
        WHERE sector IS NOT NULL
        GROUP BY sector, metric
    ),
    industry_agg AS (
        SELECT
            DATE '{as_of_date.isoformat()}' AS as_of_date,
            'industry' AS grouping_level,
            industry AS group_value,
            metric,
            COUNT(*) AS n,
            AVG(value) AS mean,
            quantile_cont(value, 0.5)  AS median,
            quantile_cont(value, 0.25) AS p25,
            quantile_cont(value, 0.75) AS p75
        FROM enriched
        WHERE industry IS NOT NULL
        GROUP BY industry, metric
    )
    SELECT * FROM sector_agg
    UNION ALL
    SELECT * FROM industry_agg
    """

    df = con.execute(sql).df()
    df["gold_built_utc"] = datetime.now(timezone.utc).isoformat()

    n_sector = (df["grouping_level"] == "sector").sum()
    n_industry = (df["grouping_level"] == "industry").sum()
    logger.info(
        f"Built {len(df):,} aggregate rows "
        f"({n_sector} sector × metric, {n_industry} industry × metric) "
        f"as of {as_of_date}"
    )

    overwrite_parquet(df, OUT_KEY)


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("build_sector_aggregates"):
        main()
