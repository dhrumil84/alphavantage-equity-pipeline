"""
Build gold/dim_peer_sets/dim_peer_sets.parquet

Grain: (symbol, peer_rank) — up to MAX_PEERS peer rows per symbol.

Algorithmic peer selection for every active stock in the universe, so any
notebook can pull a ready-made comp set (and override it with an explicit
list when the algorithm's pick is wrong). Selection logic:

  1. Candidate pool: active stocks (asset_type = 'Stock') with a market cap
     and at least one quarter of fundamentals in fact_fundamentals_wide.
  2. Prefer same industry; when the industry pool (after the cap-ratio
     filter) has fewer than MAX_PEERS members, backfill from the same sector.
  3. Drop candidates more than 100x larger/smaller (|log10 cap ratio| > 2) —
     a $200M microcap is not a peer of a $2T megacap even in the same industry.
  4. Rank by score = 0.6 * cap proximity + 0.4 * trailing-2y daily-return
     correlation. Cap proximity = 1 − |log10 cap ratio| / 2. Correlation
     falls back to 0 when the price histories don't overlap enough
     (< 126 common trading days).

Sources:
  - gold/dim_company_enriched  → universe, sector/industry, market cap
  - gold/fact_prices_enriched  → return_1d for the correlation window

Cadence: weekly, in weekly_refresh.yml AFTER build_dim_company_enriched.

Run as:  python -m transform_gold.build_peer_sets
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

from transform_gold.utils.duckdb_silver import duckdb_to_r2, gold_scan, overwrite_parquet  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

OUT_KEY = "gold/dim_peer_sets/dim_peer_sets.parquet"

MAX_PEERS = 5
MAX_LOG_CAP_RATIO = 2.0   # 100x larger/smaller is disqualifying
CORR_WINDOW_DAYS = 504    # ~2 trading years
MIN_CORR_OVERLAP = 126    # ~6 months of common history
W_CAP, W_CORR = 0.6, 0.4


def load_universe(con, bucket: str) -> pd.DataFrame:
    return con.execute(f"""
        SELECT symbol, name, sector, industry,
               COALESCE(market_cap_latest, market_cap) AS market_cap
        FROM {gold_scan(bucket, 'dim_company_enriched/*.parquet')}
        WHERE asset_type = 'Stock'
          AND listing_status = 'active'
          AND COALESCE(market_cap_latest, market_cap) > 0
          AND sector IS NOT NULL
          AND latest_quarter_end IS NOT NULL   -- has fundamentals
    """).df()


def load_return_corr(con, bucket: str, symbols: pd.Index) -> pd.DataFrame:
    returns = con.execute(f"""
        SELECT symbol, trade_date, return_1d
        FROM {gold_scan(bucket, 'fact_prices_enriched/**/*.parquet')}
        WHERE trade_date >= CURRENT_DATE - INTERVAL {CORR_WINDOW_DAYS + 60} DAY
          AND return_1d IS NOT NULL
    """).df()
    wide = (
        returns.pivot_table(index="trade_date", columns="symbol", values="return_1d")
        .reindex(columns=symbols)
    )
    logger.info(f"Correlation input: {wide.shape[0]} days x {wide.shape[1]} symbols")
    return wide.corr(min_periods=MIN_CORR_OVERLAP)


def pick_peers(row: pd.Series, universe: pd.DataFrame, corr: pd.DataFrame) -> pd.DataFrame:
    others = universe[universe.symbol != row.symbol].copy()
    others["log_cap_ratio"] = np.log10(others.market_cap / row.market_cap)
    others = others[others.log_cap_ratio.abs() <= MAX_LOG_CAP_RATIO]

    others["industry_match"] = (
        (others.industry == row.industry) if pd.notna(row.industry) else False
    )
    industry_pool = others[others.industry_match]
    if len(industry_pool) >= MAX_PEERS:
        pool = industry_pool.copy()
    else:
        pool = others[others.sector == row.sector].copy()
    if pool.empty:
        return pd.DataFrame()

    if row.symbol in corr.index:
        pool["return_corr_2y"] = corr.loc[row.symbol].reindex(pool.symbol).values
    else:
        pool["return_corr_2y"] = np.nan

    cap_score = 1.0 - pool.log_cap_ratio.abs() / MAX_LOG_CAP_RATIO
    pool["score"] = W_CAP * cap_score + W_CORR * pool.return_corr_2y.fillna(0.0)

    pool = pool.sort_values(
        ["industry_match", "score"], ascending=[False, False]
    ).head(MAX_PEERS)
    pool["peer_rank"] = range(1, len(pool) + 1)
    return pd.DataFrame({
        "symbol": row.symbol,
        "peer_rank": pool.peer_rank,
        "peer_symbol": pool.symbol,
        "peer_name": pool["name"],
        "industry_match": pool.industry_match,
        "log_cap_ratio": pool.log_cap_ratio,
        "return_corr_2y": pool.return_corr_2y,
        "score": pool.score,
    })


def main() -> None:
    bucket = os.environ["R2_BUCKET_NAME"]
    con = duckdb_to_r2()
    con.execute("SET enable_progress_bar = false;")

    universe = load_universe(con, bucket)
    logger.info(f"Peer-eligible universe: {len(universe):,} symbols")
    if universe.empty:
        logger.info("No eligible symbols; nothing to write.")
        return

    corr = load_return_corr(con, bucket, pd.Index(universe.symbol))

    frames = [pick_peers(row, universe, corr) for row in universe.itertuples(index=False)]
    df = pd.concat([f for f in frames if not f.empty], ignore_index=True)

    df["as_of_date"] = datetime.now(timezone.utc).date()
    df["gold_built_utc"] = datetime.now(timezone.utc).isoformat()
    df = df[[
        "as_of_date", "symbol", "peer_rank", "peer_symbol", "peer_name",
        "industry_match", "log_cap_ratio", "return_corr_2y", "score", "gold_built_utc",
    ]]

    logger.info(f"Built {len(df):,} peer rows for {df['symbol'].nunique():,} symbols")
    overwrite_parquet(df, OUT_KEY)


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("build_peer_sets"):
        main()
