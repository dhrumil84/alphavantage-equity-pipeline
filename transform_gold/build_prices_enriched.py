"""
Build gold/fact_prices_enriched/year=YYYY/fact_prices_enriched.parquet

One row per (symbol, trade_date). Enriches silver fact_daily_prices with:
- Returns: 1d, 5d, 21d (~1m), 63d (~3m), 126d (~6m), 252d (~1y)
- Volatility: 30d, 90d rolling stdev of daily returns (annualized)
- Volume: 20d avg, volume_ratio_20d, dollar_volume
- Range: 52w high/low, distance from each
- Drawdown from rolling 52w high
- Technicals: SMA 20/50/200, EMA 12/26, MACD (12-26-9), Bollinger (20, 2σ),
              RSI 14, ATR 14
- Relative strength vs SPY: 3m / 6m / 12m return ratios

Cadence: daily, in daily_prices.yml after transform_daily_prices.

Strategy: full rebuild every day. ~107 tickers × ~5000 days = ~500K rows;
runs in seconds. Avoids the complexity of incremental rolling-window updates.

Run as:  python -m transform_gold.build_prices_enriched
"""

from __future__ import annotations

import io
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from ingestion.utils import r2_client  # noqa: E402
from transform_gold.utils.duckdb_silver import duckdb_to_r2, silver_scan  # noqa: E402

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

OUT_PREFIX = "gold/fact_prices_enriched"
BENCHMARK = "SPY"


# ─── Technical indicators (vectorized, per-symbol) ─────────────────────────

def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    # Wilder's smoothing = EMA with alpha=1/period
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.where(avg_loss != 0)
    return 100 - (100 / (1 + rs))


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low).abs(),
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def enrich_group(g: pd.DataFrame) -> pd.DataFrame:
    """Compute all per-ticker enrichments on a sorted group."""
    g = g.sort_values("trade_date").reset_index(drop=True)
    ac = g["adjusted_close"]
    close = g["close"]
    high = g["high"]
    low = g["low"]
    vol = g["volume"]

    # Returns (use adjusted_close — accounts for splits/dividends)
    g["return_1d"]   = ac.pct_change(1)
    g["return_5d"]   = ac.pct_change(5)
    g["return_21d"]  = ac.pct_change(21)
    g["return_63d"]  = ac.pct_change(63)
    g["return_126d"] = ac.pct_change(126)
    g["return_252d"] = ac.pct_change(252)

    # Volatility (annualized stdev of daily returns)
    g["volatility_30d"] = g["return_1d"].rolling(30, min_periods=20).std() * np.sqrt(252)
    g["volatility_90d"] = g["return_1d"].rolling(90, min_periods=60).std() * np.sqrt(252)

    # Volume / dollar volume
    g["dollar_volume"]      = close * vol
    g["volume_avg_20d"]     = vol.rolling(20, min_periods=10).mean()
    g["volume_ratio_20d"]   = vol / g["volume_avg_20d"].where(g["volume_avg_20d"] != 0)

    # 52-week range + drawdown
    g["high_52w"]            = ac.rolling(252, min_periods=60).max()
    g["low_52w"]             = ac.rolling(252, min_periods=60).min()
    g["pct_off_52w_high"]    = (ac - g["high_52w"]) / g["high_52w"].where(g["high_52w"] != 0)
    g["pct_off_52w_low"]     = (ac - g["low_52w"])  / g["low_52w"].where(g["low_52w"] != 0)
    g["drawdown_from_52w_high"] = g["pct_off_52w_high"]  # alias for clarity

    # Moving averages
    g["sma_20"]  = ac.rolling(20,  min_periods=20).mean()
    g["sma_50"]  = ac.rolling(50,  min_periods=50).mean()
    g["sma_200"] = ac.rolling(200, min_periods=200).mean()
    g["ema_12"]  = ac.ewm(span=12, adjust=False, min_periods=12).mean()
    g["ema_26"]  = ac.ewm(span=26, adjust=False, min_periods=26).mean()

    # MACD
    g["macd"]        = g["ema_12"] - g["ema_26"]
    g["macd_signal"] = g["macd"].ewm(span=9, adjust=False, min_periods=9).mean()
    g["macd_hist"]   = g["macd"] - g["macd_signal"]

    # Bollinger Bands (20, 2σ)
    bb_std = ac.rolling(20, min_periods=20).std()
    g["bb_middle"] = g["sma_20"]
    g["bb_upper"]  = g["bb_middle"] + 2 * bb_std
    g["bb_lower"]  = g["bb_middle"] - 2 * bb_std
    width = (g["bb_upper"] - g["bb_lower"])
    g["bb_pct_b"]  = (ac - g["bb_lower"]) / width.where(width != 0)

    # RSI
    g["rsi_14"] = _rsi(ac, 14)

    # ATR (uses raw OHLC, not adjusted)
    g["atr_14"] = _atr(high, low, close, 14)

    return g


def add_relative_strength(df: pd.DataFrame) -> pd.DataFrame:
    """Add return / SPY-return ratios over 3m, 6m, 12m. Higher = outperforming."""
    bench = df[df["symbol"] == BENCHMARK][["trade_date", "return_63d", "return_126d", "return_252d"]]
    if bench.empty:
        logger.warning(f"Benchmark {BENCHMARK} not found in silver — skipping relative strength")
        for col in ["rel_strength_vs_spy_3m", "rel_strength_vs_spy_6m", "rel_strength_vs_spy_12m"]:
            df[col] = np.nan
        return df

    bench = bench.rename(columns={
        "return_63d":  "_bench_63d",
        "return_126d": "_bench_126d",
        "return_252d": "_bench_252d",
    })
    df = df.merge(bench, on="trade_date", how="left")

    # Use (1+r)/(1+rb) - 1 framing so meaningful even when one leg is negative.
    df["rel_strength_vs_spy_3m"]  = (1 + df["return_63d"])  / (1 + df["_bench_63d"])  - 1
    df["rel_strength_vs_spy_6m"]  = (1 + df["return_126d"]) / (1 + df["_bench_126d"]) - 1
    df["rel_strength_vs_spy_12m"] = (1 + df["return_252d"]) / (1 + df["_bench_252d"]) - 1
    df = df.drop(columns=["_bench_63d", "_bench_126d", "_bench_252d"])
    return df


# ─── Output (partitioned by year) ──────────────────────────────────────────

def write_by_year(df: pd.DataFrame, bucket: str) -> None:
    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["_year"] = df["trade_date"].dt.year
    df["trade_date"] = df["trade_date"].dt.date

    for year, year_df in df.groupby("_year"):
        year_df = year_df.drop(columns=["_year"])
        # Coerce remaining date-like cols
        for col in year_df.columns:
            if "date" in col.lower() and col != "trade_date":
                year_df[col] = pd.to_datetime(year_df[col], errors="coerce").dt.date

        table = pa.Table.from_pandas(year_df, preserve_index=False)
        # Ensure trade_date is date32
        new_fields = []
        for f in table.schema:
            if "date" in f.name.lower() and f.type != pa.date32():
                new_fields.append(pa.field(f.name, pa.date32()))
            else:
                new_fields.append(f)
        table = table.cast(pa.schema(new_fields))

        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        key = f"{OUT_PREFIX}/year={int(year)}/fact_prices_enriched.parquet"
        r2_client.upload_bytes(buf.getvalue(), key)
        logger.info(f"Wrote {len(year_df):,} rows → {key}")


def main():
    bucket = os.environ["R2_BUCKET_NAME"]
    con = duckdb_to_r2()

    logger.info("Reading silver fact_daily_prices...")
    df = con.execute(f"""
        SELECT symbol, trade_date, open, high, low, close, adjusted_close,
               volume, dividend_amount, split_coefficient, pull_date
        FROM {silver_scan(bucket, 'fact_daily_prices/**/*.parquet')}
    """).df()
    logger.info(f"Loaded {len(df):,} silver rows across {df['symbol'].nunique()} symbols")

    df["trade_date"] = pd.to_datetime(df["trade_date"])

    logger.info("Computing per-ticker technical signals...")
    df = (df.groupby("symbol", group_keys=False, sort=False)
            .apply(enrich_group)
            .reset_index(drop=True))

    logger.info("Computing relative strength vs SPY...")
    df = add_relative_strength(df)

    df["gold_built_utc"] = datetime.now(timezone.utc).isoformat()

    logger.info(f"Final shape: {df.shape}")
    write_by_year(df, bucket)


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("build_prices_enriched"):
        main()
