"""
Dry-run the proposed LISTING_STATUS gate for ingest_fundamentals.

Joins config/ticker_universe.csv against the latest
bronze/listing_status/{YYYY-MM-DD}.csv snapshot in R2 and prints which
active-universe symbols the new filter would skip on the next fundamentals
run, and why.

Writes config/reconcile/proposed_changes.csv for review. Read-only — no
edits to ticker_universe.csv, no R2 writes.

Usage:
    python -m config.preview_fundamentals_filter
"""
from __future__ import annotations

import csv
import logging
from pathlib import Path

import pandas as pd

from ingestion.utils.listing_status import load_listing_status_df

logger = logging.getLogger(__name__)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)

UNIVERSE_PATH = Path("config") / "ticker_universe.csv"
OUTPUT_PATH = Path("config") / "reconcile" / "proposed_changes.csv"
FUNDAMENTAL_ENDPOINTS_PER_SYMBOL = 4  # INCOME_STATEMENT, BALANCE_SHEET, CASH_FLOW, EARNINGS


def _classify(asset_type: str, status: str) -> str:
    if pd.isna(asset_type) and pd.isna(status):
        return "not_in_listing_status"
    if status != "active":
        return "delisted"
    if asset_type != "Stock":
        return f"non_stock:{asset_type or 'unknown'}"
    return "keep"


def main() -> None:
    if not UNIVERSE_PATH.exists():
        raise FileNotFoundError(f"{UNIVERSE_PATH} not found")

    uni = pd.read_csv(UNIVERSE_PATH)
    uni["symbol"] = uni["symbol"].astype(str).str.strip().str.upper()
    active = uni[uni["active"].astype(str).str.lower() == "true"].copy()
    logger.info(f"Loaded {len(uni)} universe rows, {len(active)} active")

    listing = load_listing_status_df()
    logger.info(f"Loaded {len(listing)} listing_status rows (deduped, Active preferred)")

    merged = active.merge(
        listing[["symbol", "assetType", "status"]],
        on="symbol",
        how="left",
    )

    merged["reason"] = [
        _classify(at, st)
        for at, st in zip(merged["assetType"], merged["status"])
    ]

    skipped = merged[merged["reason"] != "keep"].copy()
    kept = merged[merged["reason"] == "keep"]

    # Categorise reasons for the summary
    counts = skipped["reason"].apply(lambda r: r.split(":")[0]).value_counts().to_dict()

    print()
    print("=== Proposed fundamentals-ingest filter ===")
    print(f"  Active universe size:           {len(active):>5}")
    print(f"  Would continue to ingest:       {len(kept):>5}")
    print(f"  Would be skipped (total):       {len(skipped):>5}")
    for reason, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"    {reason:<24} {n:>5}")
    print()
    saved_calls = len(skipped) * FUNDAMENTAL_ENDPOINTS_PER_SYMBOL
    print(f"  API calls saved per run:        {saved_calls:>5} "
          f"({len(skipped)} symbols x {FUNDAMENTAL_ENDPOINTS_PER_SYMBOL} endpoints)")
    print()

    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    out = skipped[["symbol", "name", "assetType", "status", "reason"]].sort_values(
        ["reason", "symbol"]
    )
    out.to_csv(OUTPUT_PATH, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"Wrote {OUTPUT_PATH} ({len(out)} rows)")

    print()
    print("Sample (first 15):")
    for _, row in out.head(15).iterrows():
        print(
            f"  {row['symbol']:<8} {str(row['name'])[:34]:<34} "
            f"{str(row['assetType'] or ''):<6} {str(row['status'] or ''):<10} {row['reason']}"
        )


if __name__ == "__main__":
    main()
