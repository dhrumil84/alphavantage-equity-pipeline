"""
Build silver tables from bronze earnings call transcripts:

  silver/dim_earnings_calls          grain: (symbol, quarter)
  silver/fact_transcript_turns       grain: (symbol, quarter, turn_idx)

dim_earnings_calls.call_date is enriched from silver/fact_earnings.report_date
when available; otherwise left null.

Strategy: scan all bronze/earnings_transcripts/*/*.json (immutable per quarter
file) and upsert. R2 list-then-download is the bottleneck; for very large
backfills consider sharding by symbol first letter — out of scope here.
"""

from __future__ import annotations

import logging
import os
from typing import Dict, Tuple

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


def _load_call_dates() -> Dict[Tuple[str, str], pd.Timestamp]:
    """Map (symbol, quarter) -> report_date from silver/fact_earnings."""
    from transform_gold.utils.duckdb_silver import duckdb_to_r2, silver_scan
    bucket = os.environ.get("R2_BUCKET_NAME", "")
    try:
        con = duckdb_to_r2()
        rows = con.execute(f"""
            SELECT symbol,
                   CAST(fiscal_date_ending AS DATE) AS fde,
                   CAST(report_date AS DATE) AS report_date
            FROM {silver_scan(bucket, 'fact_earnings/fact_earnings.parquet')}
            WHERE period_type = 'quarterly'
              AND report_date IS NOT NULL
        """).fetchall()
    except Exception as e:
        logger.warning(f"Could not load report_dates from fact_earnings ({e}); call_date will be null")
        return {}
    out: Dict[Tuple[str, str], pd.Timestamp] = {}
    for sym, fde, rd in rows:
        q = f"{fde.year}Q{(fde.month - 1) // 3 + 1}"
        out[(sym, q)] = rd
    return out


def main() -> None:
    keys = [k for k in r2_client.list_keys("bronze/earnings_transcripts/") if k.endswith(".json")]
    if not keys:
        logger.info("No transcript bronze files found.")
        return
    logger.info(f"Processing {len(keys)} transcript bronze files")

    call_dates = _load_call_dates()

    calls: list[dict] = []
    turns: list[dict] = []

    for key in keys:
        parts = key.split("/")
        if len(parts) < 4:
            continue
        symbol = parts[2]
        quarter = parts[3].replace(".json", "")
        try:
            data = r2_client.download_json(key)
        except Exception as e:
            logger.error(f"Failed to read {key}: {e}")
            continue

        transcript = data.get("transcript") or []
        pull_date = data.get("pull_date")

        calls.append({
            "symbol": symbol,
            "quarter": quarter,
            "call_date": call_dates.get((symbol, quarter)),
            "participant_count": len({(t.get("speaker") or "").strip() for t in transcript if t.get("speaker")}),
            "pull_date": pull_date,
        })

        for idx, turn in enumerate(transcript):
            turns.append({
                "symbol": symbol,
                "quarter": quarter,
                "turn_idx": idx,
                "speaker": turn.get("speaker"),
                "title": turn.get("title"),
                "content": turn.get("content"),
                "sentiment": _safe_float(turn.get("sentiment")),
                "pull_date": pull_date,
            })

    if calls:
        upsert_parquet(
            pd.DataFrame(calls),
            "silver/dim_earnings_calls/dim_earnings_calls.parquet",
            dedup_keys=["symbol", "quarter"],
        )
    if turns:
        upsert_parquet(
            pd.DataFrame(turns),
            "silver/fact_transcript_turns/fact_transcript_turns.parquet",
            dedup_keys=["symbol", "quarter", "turn_idx"],
        )
    logger.info(f"Wrote {len(calls)} call rows, {len(turns)} turn rows.")


if __name__ == "__main__":
    main()
