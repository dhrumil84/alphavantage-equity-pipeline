"""
Build gold/dim_company_enriched/dim_company_enriched.parquet

One row per symbol. Joins dim_company + latest TTM snapshot (from
fact_fundamentals_wide) + latest valuation + 1y/3y/5y total returns.

Cadence: weekly, in weekly_refresh.yml AFTER build_fundamentals_wide.

Run as:  python -m transform_gold.build_dim_company_enriched
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from transform_gold.utils.duckdb_silver import (  # noqa: E402
    duckdb_to_r2, silver_scan, gold_scan, overwrite_parquet,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

OUT_KEY = "gold/dim_company_enriched/dim_company_enriched.parquet"


def main():
    bucket = os.environ["R2_BUCKET_NAME"]
    con = duckdb_to_r2()

    # 1) base dim_company
    dim = con.execute(f"""
        SELECT *
        FROM {silver_scan(bucket, 'dim_company/*.parquet')}
    """).df()
    logger.info(f"Loaded {len(dim):,} dim_company rows")

    # 2) latest quarterly TTM snapshot per symbol from fundamentals_wide
    ttm = con.execute(f"""
        WITH ranked AS (
            SELECT
                symbol, fiscal_date_ending,
                total_revenue_ttm, net_income_ttm, ebitda_ttm,
                free_cash_flow_ttm, operating_cashflow_ttm, reported_eps_ttm,
                dividend_payout_ttm,
                gross_margin, operating_margin, net_margin, fcf_margin,
                roe, roa, roic,
                debt_to_equity, net_debt,
                revenue_growth_yoy, eps_growth_yoy, fcf_growth_yoy,
                eps_cagr_5y, eps_cagr_3y,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol
                    ORDER BY fiscal_date_ending DESC
                ) AS rn
            FROM {gold_scan(bucket, 'fact_fundamentals_wide/*.parquet')}
            WHERE period_type = 'quarterly' AND total_revenue_ttm IS NOT NULL
        )
        SELECT * EXCLUDE (rn),
               fiscal_date_ending AS latest_quarter_end
        FROM ranked WHERE rn = 1
    """).df()
    ttm = ttm.drop(columns=["fiscal_date_ending"], errors="ignore")
    logger.info(f"Loaded TTM snapshot for {len(ttm):,} symbols")

    # 3) latest price + market cap proxy + trailing returns from silver prices
    prices_summary = con.execute(f"""
        WITH p AS (
            SELECT symbol, trade_date, adjusted_close,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC) AS rn
            FROM {silver_scan(bucket, 'fact_daily_prices/**/*.parquet')}
        ),
        latest AS (SELECT symbol, trade_date AS as_of_date, adjusted_close AS latest_close
                   FROM p WHERE rn = 1),
        r1y   AS (SELECT symbol, adjusted_close AS price_1y_ago   FROM p WHERE rn = 252),
        r3y   AS (SELECT symbol, adjusted_close AS price_3y_ago   FROM p WHERE rn = 756),
        r5y   AS (SELECT symbol, adjusted_close AS price_5y_ago   FROM p WHERE rn = 1260)
        SELECT
            latest.symbol, latest.as_of_date, latest.latest_close,
            (latest.latest_close - r1y.price_1y_ago) / NULLIF(r1y.price_1y_ago, 0) AS price_return_1y,
            (latest.latest_close - r3y.price_3y_ago) / NULLIF(r3y.price_3y_ago, 0) AS price_return_3y,
            (latest.latest_close - r5y.price_5y_ago) / NULLIF(r5y.price_5y_ago, 0) AS price_return_5y
        FROM latest
        LEFT JOIN r1y USING (symbol)
        LEFT JOIN r3y USING (symbol)
        LEFT JOIN r5y USING (symbol)
    """).df()
    logger.info(f"Loaded latest-price + returns for {len(prices_summary):,} symbols")

    # 4) Stitch together. dim_company is the spine.
    enriched = dim.merge(ttm, on="symbol", how="left") \
                  .merge(prices_summary, on="symbol", how="left")

    # Latest market cap = latest_close × shares_outstanding (from silver dim_company)
    if "shares_outstanding" in enriched.columns and "latest_close" in enriched.columns:
        enriched["market_cap_latest"] = (
            enriched["latest_close"] * enriched["shares_outstanding"]
        )

    enriched["gold_built_utc"] = datetime.now(timezone.utc).isoformat()

    logger.info(f"Final shape: {enriched.shape}")
    overwrite_parquet(enriched, OUT_KEY)


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("build_dim_company_enriched"):
        main()
