"""
Build silver/fact_insider_transactions from bronze INSIDER_TRANSACTIONS files.

Grain: (symbol, transaction_date, executive, shares, share_price, acquisition_or_disposal)
       composite — Form 4 has no single-field primary key.

Strategy: scan the latest bronze file per symbol, parse, upsert.
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
    keys = _latest_files("bronze/insider_transactions/")
    if not keys:
        logger.info("No insider transactions bronze files found.")
        return
    logger.info(f"Processing {len(keys)} bronze files")

    rows: list[dict] = []
    for key in keys:
        try:
            data = r2_client.download_json(key)
        except Exception as e:
            logger.error(f"Failed to read {key}: {e}")
            continue
        pull_date = data.get("pull_date") or key.split("/")[-1].replace(".json", "")
        symbol = data.get("symbol") or key.split("/")[2]
        for r in data.get("data") or []:
            rows.append({
                "symbol": symbol,
                "transaction_date": r.get("transaction_date"),
                "executive": r.get("executive"),
                "executive_title": r.get("executive_title"),
                "security_type": r.get("security_type"),
                "acquisition_or_disposal": r.get("acquisition_or_disposal"),
                "shares": _safe_float(r.get("shares")),
                "share_price": _safe_float(r.get("share_price")),
                "pull_date": pull_date,
            })

    if not rows:
        logger.info("No insider transaction rows parsed.")
        return

    df = pd.DataFrame(rows)
    logger.info(f"Parsed {len(df)} transactions")

    upsert_parquet(
        df,
        "silver/fact_insider_transactions/fact_insider_transactions.parquet",
        dedup_keys=["symbol", "transaction_date", "executive", "shares", "share_price", "acquisition_or_disposal"],
    )


if __name__ == "__main__":
    main()
