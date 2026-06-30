"""
Build gold/fact_insider_signals/fact_insider_signals.parquet

Grain: (symbol, as_of_date)

For each (symbol, trade_date) in silver/fact_daily_prices, summarize the
preceding 30 / 90 / 180 days of insider transactions:

  - net_insider_shares_30d / 90d / 180d   (A − D)
  - net_insider_usd_30d / 90d / 180d      (shares × share_price, signed)
  - distinct_buyers_180d, distinct_sellers_180d
  - cluster_buy_flag_30d                  (≥3 distinct executives bought in 30d)

as_of_date is the calendar date snapshot. Strategy: rebuilt weekly.
Output limited to the latest 5 years of dates per symbol to bound size.
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

OUT_KEY = "gold/fact_insider_signals/fact_insider_signals.parquet"


def main() -> None:
    bucket = os.environ["R2_BUCKET_NAME"]
    con = duckdb_to_r2()

    prices_scan = silver_scan(bucket, "fact_daily_prices/**/*.parquet")
    insider_scan = silver_scan(bucket, "fact_insider_transactions/fact_insider_transactions.parquet")

    sql = f"""
    WITH txn AS (
        SELECT symbol,
               CAST(transaction_date AS DATE) AS txn_date,
               executive,
               CASE WHEN acquisition_or_disposal = 'A' THEN shares ELSE -shares END AS signed_shares,
               CASE WHEN acquisition_or_disposal = 'A' THEN shares * share_price
                    ELSE -1 * shares * share_price END AS signed_usd
        FROM {insider_scan}
        WHERE shares IS NOT NULL
    ),
    prices AS (
        SELECT DISTINCT symbol, CAST(trade_date AS DATE) AS as_of_date
        FROM {prices_scan}
        WHERE trade_date >= CURRENT_DATE - INTERVAL 5 YEAR
    ),
    joined AS (
        SELECT p.symbol, p.as_of_date,
               t.txn_date, t.executive, t.signed_shares, t.signed_usd,
               (p.as_of_date - t.txn_date) AS lag_days
        FROM prices p
        LEFT JOIN txn t
          ON t.symbol = p.symbol
         AND t.txn_date <= p.as_of_date
         AND t.txn_date >= p.as_of_date - INTERVAL 180 DAY
    )
    SELECT
        symbol,
        as_of_date,
        SUM(CASE WHEN lag_days <=  30 THEN signed_shares ELSE 0 END) AS net_insider_shares_30d,
        SUM(CASE WHEN lag_days <=  90 THEN signed_shares ELSE 0 END) AS net_insider_shares_90d,
        SUM(CASE WHEN lag_days <= 180 THEN signed_shares ELSE 0 END) AS net_insider_shares_180d,
        SUM(CASE WHEN lag_days <=  30 THEN signed_usd    ELSE 0 END) AS net_insider_usd_30d,
        SUM(CASE WHEN lag_days <=  90 THEN signed_usd    ELSE 0 END) AS net_insider_usd_90d,
        SUM(CASE WHEN lag_days <= 180 THEN signed_usd    ELSE 0 END) AS net_insider_usd_180d,
        COUNT(DISTINCT CASE WHEN lag_days <= 180 AND signed_shares > 0 THEN executive END) AS distinct_buyers_180d,
        COUNT(DISTINCT CASE WHEN lag_days <= 180 AND signed_shares < 0 THEN executive END) AS distinct_sellers_180d,
        (COUNT(DISTINCT CASE WHEN lag_days <= 30 AND signed_shares > 0 THEN executive END) >= 3) AS cluster_buy_flag_30d
    FROM joined
    GROUP BY symbol, as_of_date
    """

    logger.info("Computing insider signals from silver...")
    df = con.execute(sql).df()
    logger.info(f"Built {len(df):,} (symbol, as_of_date) rows")

    if df.empty:
        logger.info("No insider signal data to write.")
        return

    df["gold_built_utc"] = datetime.now(timezone.utc).isoformat()
    overwrite_parquet(df, OUT_KEY)


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("build_insider_signals"):
        main()
