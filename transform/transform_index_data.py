"""
Build silver/fact_index_prices/year=YYYY/fact_index_prices.parquet

Grain: (index_symbol, trade_date)  year-partitioned, mirrors fact_daily_prices.

Bronze response shape (INDEX_DATA premium endpoint):
    {"symbol": "...", "name": "...", "interval": "daily",
     "data": [{"date": "YYYY-MM-DD", "open": "...", "high": "...",
               "low": "...", "close": "..."}, ...]}
No volume field — indices don't report it.
"""

from __future__ import annotations

import logging
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

        records = data.get("data")
        if not isinstance(records, list) or not records:
            logger.warning(f"No 'data' list in {key} (top-level keys: {sorted(data.keys())})")
            continue

        for item in records:
            rows.append({
                "index_symbol": symbol,
                "trade_date": item.get("date"),
                "open": _safe_float(item.get("open")),
                "high": _safe_float(item.get("high")),
                "low": _safe_float(item.get("low")),
                "close": _safe_float(item.get("close")),
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
