"""
Build a snapshot-history silver table from EARNINGS_CALENDAR bronze:

  silver/fact_earnings_calendar   grain: (symbol, fiscal_date_ending, pull_date)

EARNINGS_CALENDAR is forward-looking only (~3 months ahead) and re-pulled
daily to bronze as a single universe-wide CSV. The same (symbol,
fiscal_date_ending) reappears across many consecutive pulls until the
announcement happens, and the estimated report_date/estimate can shift
between pulls. Keeping pull_date in the dedup key preserves that history
instead of collapsing it to the latest known estimate, so downstream
queries can reconstruct "what was expected as of date D" (avoiding
look-ahead bias) or track how estimates were revised over time. Once a
symbol's report_date passes, join to fact_earnings on (symbol,
fiscal_date_ending) to compare estimated vs. actual.

Only bronze files newer than the max pull_date already folded into silver
are processed, since older pulls are already accounted for.
"""

from __future__ import annotations

import io
import logging
from datetime import date
from typing import List, Optional, Tuple

import pandas as pd
import pyarrow.parquet as pq

from ingestion.utils import r2_client
from transform.utils.parquet_writer import upsert_parquet

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SILVER_KEY = "silver/fact_earnings_calendar/fact_earnings_calendar.parquet"
DEDUP_KEYS = ["symbol", "fiscal_date_ending", "pull_date"]


def _existing_max_pull_date() -> Optional[date]:
    if not r2_client.key_exists(SILVER_KEY):
        return None
    try:
        raw = r2_client.download_bytes(SILVER_KEY)
        df = pq.read_table(io.BytesIO(raw)).to_pandas()
    except Exception as e:
        logger.warning(f"Could not read existing silver parquet at {SILVER_KEY}: {e}")
        return None
    if df.empty or "pull_date" not in df.columns:
        return None
    return pd.to_datetime(df["pull_date"]).max().date()


def _new_bronze_keys(since: Optional[date]) -> List[Tuple[date, str]]:
    keys = r2_client.list_keys("bronze/earnings_calendar/")
    dated: List[Tuple[date, str]] = []
    for k in keys:
        if not k.endswith(".csv"):
            continue
        stem = k.split("/")[-1].replace(".csv", "")
        try:
            pull_date = pd.to_datetime(stem).date()
        except (ValueError, TypeError):
            continue
        if since is None or pull_date > since:
            dated.append((pull_date, k))
    return sorted(dated)


def _parse_bronze_csv(key: str, pull_date: date) -> pd.DataFrame:
    raw = r2_client.download_bytes(key)
    df = pd.read_csv(io.BytesIO(raw))
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={
        "reportDate": "report_date",
        "fiscalDateEnding": "fiscal_date_ending",
    })

    for col in ("symbol", "name", "report_date", "fiscal_date_ending", "estimate", "currency"):
        if col not in df.columns:
            df[col] = None

    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df = df[df["symbol"].str.len() > 0]

    df["name"] = df["name"].astype(str).str.strip()
    df["currency"] = df["currency"].astype(str).str.strip()
    df["estimate"] = pd.to_numeric(df["estimate"], errors="coerce")
    df["fiscal_date_ending"] = pd.to_datetime(df["fiscal_date_ending"], errors="coerce").dt.date
    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce").dt.date
    df["pull_date"] = pull_date

    df = df.dropna(subset=["fiscal_date_ending"])

    return df[["symbol", "name", "fiscal_date_ending", "report_date", "estimate", "currency", "pull_date"]]


def main() -> None:
    since = _existing_max_pull_date()
    dated_keys = _new_bronze_keys(since)
    if not dated_keys:
        logger.info(f"No new earnings_calendar bronze files to process (since {since}).")
        return
    logger.info(f"Processing {len(dated_keys)} new bronze file(s) since {since}.")

    frames = []
    for pull_date, key in dated_keys:
        try:
            frames.append(_parse_bronze_csv(key, pull_date))
        except Exception as e:
            logger.error(f"Failed to parse {key}: {e}")

    if not frames:
        logger.info("No rows parsed from new bronze files.")
        return

    new_df = pd.concat(frames, ignore_index=True)
    upsert_parquet(new_df, SILVER_KEY, dedup_keys=DEDUP_KEYS)
    logger.info(f"Upserted {len(new_df)} rows from {len(dated_keys)} pull date(s) into {SILVER_KEY}.")


if __name__ == "__main__":
    main()
