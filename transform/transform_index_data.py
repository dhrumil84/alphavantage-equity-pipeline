"""
Build silver/fact_index_prices/year=YYYY/fact_index_prices.parquet

Grain: (index_symbol, trade_date)  year-partitioned, mirrors fact_daily_prices.

Bronze response shape is inferred (premium endpoint, demo key rejected). We
defensively probe for time-series payload keys that contain 'Time Series' and
field keys that match the standard AV OHLC pattern ('1. open', '2. high',
'3. low', '4. close'). Volume is not present for indices.
"""

from __future__ import annotations

import logging
import os
from typing import List

import pandas as pd

from ingestion.utils import r2_client
from transform.utils.parquet_writer import upsert_parquet

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _safe_float(v) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _latest_files(prefix: str) -> List[str]:
    keys = r2_client.list_keys(prefix)
    by_symbol: dict[str, list[tuple[str, str]]] = {}
    for k in keys:
        parts = k.split("/")
        if len(parts) < 4 or not k.endswith(".json"):
            continue
        symbol = parts[2]
        pull_date = parts[3].replace(".json", "")
        by_symbol.setdefault(symbol, []).append((pull_date, k))
    return [sorted(v)[-1][1] for v in by_symbol.values()]


def _find_time_series(payload: dict) -> dict | None:
    """Locate the time-series dict — its key name varies by interval."""
    for k, v in payload.items():
        if isinstance(v, dict) and "Time Series" in k:
            return v
    return None


def _field(values: dict, *candidates: str) -> str | None:
    for c in candidates:
        if c in values:
            return values[c]
    return None


def main() -> None:
    keys = _latest_files("bronze/index_data/")
    if not keys:
        logger.info("No index_data bronze files found.")
        return
    logger.info(f"Processing {len(keys)} latest bronze files")

    rows: list[dict] = []
    for key in keys:
        try:
            data = r2_client.download_json(key)
        except Exception as e:
            logger.error(f"Failed to read {key}: {e}")
            continue
        symbol = data.get("symbol") or key.split("/")[2]
        pull_date = data.get("pull_date") or key.split("/")[-1].replace(".json", "")

        ts = _find_time_series(data)
        if not ts:
            logger.warning(f"No time-series block in {key}")
            continue

        for trade_date_str, values in ts.items():
            rows.append({
                "index_symbol": symbol,
                "trade_date": trade_date_str,
                "open": _safe_float(_field(values, "1. open", "open")),
                "high": _safe_float(_field(values, "2. high", "high")),
                "low": _safe_float(_field(values, "3. low", "low")),
                "close": _safe_float(_field(values, "4. close", "close")),
                "pull_date": pull_date,
            })

    if not rows:
        logger.info("No rows parsed.")
        return

    df = pd.DataFrame(rows)
    df["year"] = pd.to_datetime(df["trade_date"]).dt.year
    logger.info(f"Parsed {len(df)} rows across years {sorted(df['year'].unique())}")

    for year in sorted(df["year"].unique()):
        year_df = df[df["year"] == year].drop(columns=["year"]).copy()
        key = f"silver/fact_index_prices/year={int(year)}/fact_index_prices.parquet"
        upsert_parquet(year_df, key, dedup_keys=["index_symbol", "trade_date"])


if __name__ == "__main__":
    main()
