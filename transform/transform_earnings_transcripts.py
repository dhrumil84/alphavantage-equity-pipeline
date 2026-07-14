"""
Build silver tables from bronze earnings call transcripts:

  silver/dim_earnings_calls          grain: (symbol, quarter)
  silver/fact_transcript_turns       grain: (symbol, quarter, turn_idx)

dim_earnings_calls.call_date is enriched from silver/fact_earnings.report_date
when available; otherwise left null.

Strategy: transcripts are immutable per (symbol, quarter) — see
ingestion.ingest_earnings_transcripts — so a bronze file whose pair already
exists in silver/dim_earnings_calls never needs re-reading. Each run lists
bronze once, subtracts the pairs already in silver, and downloads only the
remainder in parallel. Pass --full-rebuild to ignore silver and reprocess
all of bronze (e.g. after a schema change to either output table).

Known tradeoff: call_date is resolved when a pair is first processed. A row
whose report_date lands in fact_earnings *after* that keeps a null call_date
until a --full-rebuild. Forward-mode ingestion only fetches a transcript once
its report_date is already in fact_earnings, so this only affects backfilled
quarters with incomplete earnings history.
"""

from __future__ import annotations

import argparse
import io
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Set, Tuple

import pandas as pd
import pyarrow.parquet as pq

from ingestion.utils import r2_client
from transform.utils.parquet_writer import upsert_parquet

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DIM_CALLS_KEY = "silver/dim_earnings_calls/dim_earnings_calls.parquet"
TURNS_KEY = "silver/fact_transcript_turns/fact_transcript_turns.parquet"

# boto3 clients are thread-safe, and R2 GETs are latency-bound (~200ms each),
# so parallel downloads scale nearly linearly up to connection-pool limits.
DOWNLOAD_WORKERS = 24
PROGRESS_EVERY = 1000


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


def _load_processed_pairs() -> Set[Tuple[str, str]]:
    """Return (symbol, quarter) pairs already present in silver dim_earnings_calls.

    Bronze transcript files are immutable, so presence in the dim table means
    the file has been fully processed (both outputs are written in the same
    run). Returns an empty set when the table doesn't exist yet, which makes
    the first run equivalent to a full rebuild.
    """
    try:
        if not r2_client.key_exists(DIM_CALLS_KEY):
            return set()
        parquet_bytes = r2_client.download_bytes(DIM_CALLS_KEY)
        table = pq.read_table(io.BytesIO(parquet_bytes), columns=["symbol", "quarter"])
        df = table.to_pandas()
        return set(zip(df["symbol"], df["quarter"]))
    except Exception as e:
        logger.warning(f"Could not read {DIM_CALLS_KEY} ({e}); reprocessing all bronze files")
        return set()


def _parse_key(key: str) -> Tuple[str, str] | None:
    """bronze/earnings_transcripts/{symbol}/{quarter}.json -> (symbol, quarter)."""
    parts = key.split("/")
    if len(parts) < 4:
        return None
    return parts[2], parts[3].removesuffix(".json")


def _download_one(item: Tuple[str, str, str]) -> Tuple[str, str, dict]:
    key, symbol, quarter = item
    return symbol, quarter, r2_client.download_json(key)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--full-rebuild", action="store_true",
        help="Reprocess every bronze file even if its (symbol, quarter) is "
             "already in silver. Use after schema changes to the output tables.",
    )
    args = parser.parse_args()

    keys = [k for k in r2_client.list_keys("bronze/earnings_transcripts/") if k.endswith(".json")]
    if not keys:
        logger.info("No transcript bronze files found.")
        return

    targets: list[Tuple[str, str, str]] = []
    for key in keys:
        pair = _parse_key(key)
        if pair is not None:
            targets.append((key, pair[0], pair[1]))

    processed = set() if args.full_rebuild else _load_processed_pairs()
    if processed:
        before = len(targets)
        targets = [t for t in targets if (t[1], t[2]) not in processed]
        logger.info(
            f"Incremental run: {len(processed)} (symbol, quarter) pairs already in silver; "
            f"processing {len(targets)} of {before} bronze files"
        )
    else:
        logger.info(f"Processing all {len(targets)} bronze files"
                    + (" (--full-rebuild)" if args.full_rebuild else ""))

    if not targets:
        logger.info("Silver transcript tables already up to date; nothing to do.")
        return

    call_dates = _load_call_dates()

    calls: list[dict] = []
    turns: list[dict] = []
    failed = 0

    with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as pool:
        futures = {pool.submit(_download_one, t): t for t in targets}
        for done, fut in enumerate(as_completed(futures), start=1):
            key, symbol, quarter = futures[fut]
            try:
                symbol, quarter, data = fut.result()
            except Exception as e:
                # Not in silver yet, so the next incremental run retries it.
                logger.error(f"Failed to read {key}: {e}")
                failed += 1
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

            if done % PROGRESS_EVERY == 0:
                logger.info(f"Downloaded {done}/{len(targets)} bronze files")

    if failed:
        logger.warning(f"{failed}/{len(targets)} bronze files failed to download; "
                       f"they will be retried on the next run.")

    if calls:
        upsert_parquet(
            pd.DataFrame(calls),
            DIM_CALLS_KEY,
            dedup_keys=["symbol", "quarter"],
        )
    if turns:
        upsert_parquet(
            pd.DataFrame(turns),
            TURNS_KEY,
            dedup_keys=["symbol", "quarter", "turn_idx"],
        )
    logger.info(f"Wrote {len(calls)} call rows, {len(turns)} turn rows.")


if __name__ == "__main__":
    main()
