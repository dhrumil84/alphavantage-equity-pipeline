"""
Build two silver tables from INSTITUTIONAL_HOLDINGS bronze:

  silver/dim_institutional_summary       grain: (symbol, pull_date)
  silver/fact_institutional_holdings     grain: (symbol, holder_name, pull_date)

dim_institutional_summary captures the top-level snapshot fields
(total_holders, total_shares, increased/decreased/unchanged splits,
ownership_percentage). fact_institutional_holdings explodes the per-holder
holdings[] array — schema is defensive (uses whatever fields are present).
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


def _safe_int(v) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(float(v))
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
    keys = _latest_files("bronze/institutional_holdings/")
    if not keys:
        logger.info("No institutional holdings bronze files found.")
        return
    logger.info(f"Processing {len(keys)} bronze files")

    summary_rows: list[dict] = []
    holding_rows: list[dict] = []

    for key in keys:
        try:
            data = r2_client.download_json(key)
        except Exception as e:
            logger.error(f"Failed to read {key}: {e}")
            continue
        symbol = data.get("symbol") or key.split("/")[2]
        pull_date = data.get("pull_date") or key.split("/")[-1].replace(".json", "")

        summary_rows.append({
            "symbol": symbol,
            "total_institutional_holders": _safe_int(data.get("total_institutional_holders")),
            "total_institutional_shares": _safe_float(data.get("total_institutional_shares")),
            "holders_with_increased_holdings": _safe_int(data.get("holders_with_increased_holdings")),
            "shares_with_increased_holdings": _safe_float(data.get("shares_with_increased_holdings")),
            "holders_with_decreased_holdings": _safe_int(data.get("holders_with_decreased_holdings")),
            "shares_with_decreased_holdings": _safe_float(data.get("shares_with_decreased_holdings")),
            "holders_with_unchanged_holdings": _safe_int(data.get("holders_with_unchanged_holdings")),
            "shares_with_unchanged_holdings": _safe_float(data.get("shares_with_unchanged_holdings")),
            "total_institutional_ownership_percentage": _safe_float(data.get("total_institutional_ownership_percentage")),
            "pull_date": pull_date,
        })

        for h in data.get("holdings") or []:
            # Defensive: extract whatever fields are present; common ones in AV
            # 13F payloads: holder_name, shares_held, shares_change, pct_of_portfolio,
            # market_value, report_date.
            holder_name = h.get("holder_name") or h.get("name") or h.get("institution")
            if not holder_name:
                continue
            holding_rows.append({
                "symbol": symbol,
                "holder_name": holder_name,
                "shares_held": _safe_float(h.get("shares_held") or h.get("shares")),
                "shares_change": _safe_float(h.get("shares_change") or h.get("change")),
                "pct_of_portfolio": _safe_float(h.get("pct_of_portfolio") or h.get("percentage_of_portfolio")),
                "market_value": _safe_float(h.get("market_value") or h.get("value")),
                "report_date": h.get("report_date") or h.get("reportDate"),
                "pull_date": pull_date,
            })

    if summary_rows:
        upsert_parquet(
            pd.DataFrame(summary_rows),
            "silver/dim_institutional_summary/dim_institutional_summary.parquet",
            dedup_keys=["symbol", "pull_date"],
        )
    if holding_rows:
        upsert_parquet(
            pd.DataFrame(holding_rows),
            "silver/fact_institutional_holdings/fact_institutional_holdings.parquet",
            dedup_keys=["symbol", "holder_name", "pull_date"],
        )
    logger.info(f"Wrote {len(summary_rows)} summary rows, {len(holding_rows)} holding rows.")


if __name__ == "__main__":
    main()
