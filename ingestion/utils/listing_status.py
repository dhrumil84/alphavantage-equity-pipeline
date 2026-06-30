"""
Helpers for reading the latest Alpha Vantage LISTING_STATUS snapshot from R2.

LISTING_STATUS is fetched weekly by `ingestion.ingest_listing_status` and
written to `bronze/listing_status/{YYYY-MM-DD}.csv`. It has one row per
symbol-listing with columns: symbol, name, exchange, assetType, ipoDate,
delistingDate, status.

When a symbol's CUSIP changes (spinoffs, M&A, reverse mergers), AV keeps the
old delisted row AND adds a new active row under the same symbol. The dedup
below collapses to one row per symbol, preferring Active over Delisted.
"""
from __future__ import annotations

import io
import logging
from typing import Optional

import pandas as pd

from ingestion.utils import r2_client

logger = logging.getLogger(__name__)


def _latest_listing_status_key() -> Optional[str]:
    keys = r2_client.list_keys("bronze/listing_status/")
    dated: list[tuple[str, str]] = []
    for k in keys:
        stem = k.split("/")[-1].split(".")[0].split("_")[0]
        try:
            pd.to_datetime(stem)
            dated.append((stem, k))
        except (ValueError, TypeError):
            continue
    if not dated:
        return None
    dated.sort()
    return dated[-1][1]


def load_listing_status_df() -> pd.DataFrame:
    """Return the latest LISTING_STATUS as a deduped DataFrame.

    One row per symbol. When a symbol has both Active and Delisted entries
    (post-CUSIP succession), the Active row wins.
    """
    key = _latest_listing_status_key()
    if key is None:
        raise FileNotFoundError("No bronze/listing_status/ snapshots found in R2")

    raw = r2_client.download_bytes(key)
    df = pd.read_csv(io.BytesIO(raw))
    df.columns = [c.strip() for c in df.columns]
    df = df[df["symbol"].notna()].copy()
    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df["assetType"] = df["assetType"].astype(str).str.strip()
    df["status"] = df["status"].astype(str).str.strip().str.lower()
    df = df[df["symbol"].str.len() > 0]

    # 'active' < 'delisted' lexicographically, so sort+drop_duplicates keeps Active
    # whenever both exist for the same symbol.
    df = df.sort_values(["symbol", "status"]).drop_duplicates("symbol", keep="first")
    return df.reset_index(drop=True)


def load_eligible_stock_symbols() -> set[str]:
    """Symbols that are currently Active common Stocks per the latest LISTING_STATUS.

    Use this to gate ingestion of endpoints that only apply to corporate
    issuers (INCOME_STATEMENT, BALANCE_SHEET, CASH_FLOW, EARNINGS). ETFs,
    closed-end funds, and delisted symbols are excluded — AV returns
    'Invalid API call' for those, which burns rate-limit budget for nothing.
    """
    df = load_listing_status_df()
    eligible = df[(df["status"] == "active") & (df["assetType"] == "Stock")]
    return set(eligible["symbol"])
