"""
Build gold/fact_index_returns/year=YYYY/fact_index_returns.parquet

Grain: (index_symbol, trade_date)  year-partitioned.

Mirrors fact_prices_enriched returns/volatility but for indices (no volume).
Joinable to fact_prices_enriched for beta / relative-strength analytics
(e.g. swap the SPY-based rel_strength for true SPX).

Computed per index:
  - return_1d / 5d / 21d / 63d / 126d / 252d (from close, since no adjusted_close)
  - volatility_30d / 90d (annualized)
  - high_252d, low_252d, pct_off_252d_high
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from transform_gold.utils.duckdb_silver import duckdb_to_r2, silver_scan, overwrite_parquet  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

OUT_PREFIX = "gold/fact_index_returns"


def enrich(g: pd.DataFrame) -> pd.DataFrame:
    g = g.sort_values("trade_date").reset_index(drop=True)
    c = g["close"]
    g["return_1d"]   = c.pct_change(1)
    g["return_5d"]   = c.pct_change(5)
    g["return_21d"]  = c.pct_change(21)
    g["return_63d"]  = c.pct_change(63)
    g["return_126d"] = c.pct_change(126)
    g["return_252d"] = c.pct_change(252)
    g["volatility_30d"] = g["return_1d"].rolling(30, min_periods=20).std() * np.sqrt(252)
    g["volatility_90d"] = g["return_1d"].rolling(90, min_periods=60).std() * np.sqrt(252)
    g["high_252d"] = c.rolling(252, min_periods=60).max()
    g["low_252d"]  = c.rolling(252, min_periods=60).min()
    g["pct_off_252d_high"] = (c - g["high_252d"]) / g["high_252d"].where(g["high_252d"] != 0)
    return g


def main() -> None:
    bucket = os.environ["R2_BUCKET_NAME"]
    con = duckdb_to_r2()

    logger.info("Reading silver fact_index_prices...")
    df = con.execute(f"""
        SELECT index_symbol, trade_date, open, high, low, close, pull_date
        FROM {silver_scan(bucket, 'fact_index_prices/**/*.parquet')}
    """).df()
    if df.empty:
        logger.info("No index price data; skipping.")
        return
    logger.info(f"Loaded {len(df):,} rows across {df['index_symbol'].nunique()} indices")

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    parts = []
    for sym, g in df.groupby("index_symbol", sort=False):
        gg = enrich(g.copy())
        gg["index_symbol"] = sym
        parts.append(gg)
    df = pd.concat(parts, ignore_index=True)
    df["gold_built_utc"] = datetime.now(timezone.utc).isoformat()
    df["trade_date"] = df["trade_date"].dt.date
    df["_year"] = pd.to_datetime(df["trade_date"]).dt.year

    for year, year_df in df.groupby("_year"):
        year_df = year_df.drop(columns=["_year"])
        key = f"{OUT_PREFIX}/year={int(year)}/fact_index_returns.parquet"
        overwrite_parquet(year_df, key)


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("build_index_returns"):
        main()
