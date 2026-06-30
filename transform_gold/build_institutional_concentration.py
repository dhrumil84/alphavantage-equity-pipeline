"""
Build gold/fact_institutional_concentration/fact_institutional_concentration.parquet

Grain: (symbol, pull_date)

For each ticker snapshot, compute concentration metrics from the per-holder
silver fact_institutional_holdings:

  - top10_pct_owned      sum of top-10 holders' shares / total_institutional_shares
  - top25_pct_owned      sum of top-25 holders' shares / total_institutional_shares
  - holder_count
  - prev_holder_count, holder_count_delta (vs prior pull_date snapshot for same ticker)
  - prev_total_shares,  total_shares_delta

Useful as a flow/risk signal joinable to fact_valuation_daily on
trade_date = pull_date (approximate; refine in analytics).

Strategy: full rebuild weekly.
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

OUT_KEY = "gold/fact_institutional_concentration/fact_institutional_concentration.parquet"


def main() -> None:
    bucket = os.environ["R2_BUCKET_NAME"]
    con = duckdb_to_r2()

    holdings_scan = silver_scan(bucket, "fact_institutional_holdings/fact_institutional_holdings.parquet")
    summary_scan = silver_scan(bucket, "dim_institutional_summary/dim_institutional_summary.parquet")

    sql = f"""
    WITH ranked AS (
        SELECT symbol, pull_date, holder_name, shares_held,
               ROW_NUMBER() OVER (PARTITION BY symbol, pull_date ORDER BY shares_held DESC NULLS LAST) AS rk
        FROM {holdings_scan}
        WHERE shares_held IS NOT NULL
    ),
    rolled AS (
        SELECT r.symbol, r.pull_date,
               SUM(CASE WHEN rk <= 10 THEN shares_held ELSE 0 END) AS top10_shares,
               SUM(CASE WHEN rk <= 25 THEN shares_held ELSE 0 END) AS top25_shares,
               COUNT(*) AS holder_count
        FROM ranked r
        GROUP BY r.symbol, r.pull_date
    ),
    base AS (
        SELECT rolled.symbol, rolled.pull_date,
               s.total_institutional_shares,
               top10_shares / NULLIF(s.total_institutional_shares, 0) AS top10_pct_owned,
               top25_shares / NULLIF(s.total_institutional_shares, 0) AS top25_pct_owned,
               rolled.holder_count
        FROM rolled
        LEFT JOIN {summary_scan} s
          ON rolled.symbol = s.symbol AND rolled.pull_date = s.pull_date
    )
    SELECT b.*,
           LAG(b.holder_count) OVER w AS prev_holder_count,
           b.holder_count - LAG(b.holder_count) OVER w AS holder_count_delta,
           LAG(b.total_institutional_shares) OVER w AS prev_total_shares,
           b.total_institutional_shares - LAG(b.total_institutional_shares) OVER w AS total_shares_delta
    FROM base b
    WINDOW w AS (PARTITION BY b.symbol ORDER BY b.pull_date)
    """

    logger.info("Computing institutional concentration metrics...")
    df = con.execute(sql).df()
    logger.info(f"Built {len(df):,} (symbol, pull_date) rows")

    if df.empty:
        logger.info("No concentration data to write.")
        return

    df["gold_built_utc"] = datetime.now(timezone.utc).isoformat()
    overwrite_parquet(df, OUT_KEY)


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("build_institutional_concentration"):
        main()
