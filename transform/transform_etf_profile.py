"""
Build three silver tables from ETF_PROFILE bronze:

  silver/dim_etf_profile                grain: symbol  (latest snapshot)
  silver/fact_etf_holdings              grain: (etf_symbol, holding_symbol, as_of_date)
  silver/fact_etf_sector_allocation     grain: (etf_symbol, sector, as_of_date)

as_of_date is taken as pull_date (no explicit "holdings as of" timestamp in
the API payload). Strategy: scan latest bronze per ETF.
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
    keys = _latest_files("bronze/etf_profile/")
    if not keys:
        logger.info("No ETF profile bronze files found.")
        return
    logger.info(f"Processing {len(keys)} bronze files")

    profiles: list[dict] = []
    holdings: list[dict] = []
    sectors: list[dict] = []

    for key in keys:
        try:
            data = r2_client.download_json(key)
        except Exception as e:
            logger.error(f"Failed to read {key}: {e}")
            continue
        etf = data.get("symbol") or key.split("/")[2]
        pull_date = data.get("pull_date") or key.split("/")[-1].replace(".json", "")
        as_of_date = pull_date

        profiles.append({
            "symbol": etf,
            "net_assets": _safe_float(data.get("net_assets")),
            "net_expense_ratio": _safe_float(data.get("net_expense_ratio")),
            "portfolio_turnover": _safe_float(data.get("portfolio_turnover")),
            "dividend_yield": _safe_float(data.get("dividend_yield")),
            "inception_date": data.get("inception_date"),
            "leveraged": data.get("leveraged"),
            "pull_date": pull_date,
        })

        for h in data.get("holdings") or []:
            sym = h.get("symbol")
            if not sym:
                continue
            holdings.append({
                "etf_symbol": etf,
                "holding_symbol": sym,
                "description": h.get("description"),
                "weight": _safe_float(h.get("weight")),
                "as_of_date": as_of_date,
                "pull_date": pull_date,
            })

        for s in data.get("sectors") or []:
            sector = s.get("sector")
            if not sector:
                continue
            sectors.append({
                "etf_symbol": etf,
                "sector": sector,
                "weight": _safe_float(s.get("weight")),
                "as_of_date": as_of_date,
                "pull_date": pull_date,
            })

    if profiles:
        upsert_parquet(
            pd.DataFrame(profiles),
            "silver/dim_etf_profile/dim_etf_profile.parquet",
            dedup_keys=["symbol"],
        )
    if holdings:
        upsert_parquet(
            pd.DataFrame(holdings),
            "silver/fact_etf_holdings/fact_etf_holdings.parquet",
            dedup_keys=["etf_symbol", "holding_symbol", "as_of_date"],
        )
    if sectors:
        upsert_parquet(
            pd.DataFrame(sectors),
            "silver/fact_etf_sector_allocation/fact_etf_sector_allocation.parquet",
            dedup_keys=["etf_symbol", "sector", "as_of_date"],
        )
    logger.info(f"Wrote {len(profiles)} profile rows, {len(holdings)} holdings, {len(sectors)} sector rows.")


if __name__ == "__main__":
    main()
