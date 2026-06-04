"""
One-time in-place dedup for silver tables affected by the pre-fix
upsert_parquet bug that accumulated 2x duplicates across runs.

For each affected table, reads the existing Parquet file(s), deduplicates
on the natural key keeping the row with the latest pull_date, and writes
back. The fixed upsert_parquet (commit eee14ff onward) prevents new
duplicates from accumulating, so this is a one-time cleanup.

Affected tables:
  - silver/fact_daily_prices/year=*/  (28 year-partitioned files)
  - silver/fact_dividends/             (single file)
  - silver/fact_splits/                (single file)

dim_company and fundamentals were already rebuilt clean.

Run as:
    python -m transform.dedup_silver_tables
"""

from __future__ import annotations

import io
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.utils import r2_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def dedup_file(key: str, dedup_keys: list[str]) -> tuple[int, int]:
    """Read parquet at `key`, dedup on `dedup_keys` keeping latest pull_date,
    write back. Returns (rows_before, rows_after)."""
    raw = r2_client.download_bytes(key)
    df = pq.read_table(io.BytesIO(raw)).to_pandas()
    before = len(df)

    # Normalize date-like dedup-key columns to a consistent type so equality holds
    for col in dedup_keys:
        if col in df.columns and "date" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    if "pull_date" in df.columns:
        df["pull_date"] = pd.to_datetime(df["pull_date"], errors="coerce")
        df = df.sort_values("pull_date", ascending=False)

    df = df.drop_duplicates(subset=dedup_keys, keep="first")
    after = len(df)

    # Coerce all date-like columns to python date so pyarrow infers date32
    for col in df.columns:
        if "date" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    table = pa.Table.from_pandas(df, preserve_index=False)
    # Cast date columns to date32 explicitly (parquet_writer pattern)
    new_fields = []
    for f in table.schema:
        if "date" in f.name.lower() and f.type != pa.date32():
            new_fields.append(pa.field(f.name, pa.date32()))
        else:
            new_fields.append(f)
    table = table.cast(pa.schema(new_fields))

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    r2_client.upload_bytes(buf.getvalue(), key)
    return before, after


TABLES = [
    # (prefix, dedup_keys)
    ("silver/fact_daily_prices/", ["symbol", "trade_date"]),
    ("silver/fact_dividends/",    ["symbol", "ex_date"]),
    ("silver/fact_splits/",       ["symbol", "effective_date"]),
]


def main():
    total_before = 0
    total_after = 0

    for prefix, dedup_keys in TABLES:
        keys = sorted(r2_client.list_keys(prefix))
        keys = [k for k in keys if k.endswith(".parquet")]
        if not keys:
            logger.warning(f"[{prefix}] no parquet files found, skipping")
            continue

        logger.info(f"[{prefix}] dedup keys={dedup_keys}, {len(keys)} file(s)")
        prefix_before = prefix_after = 0
        for key in keys:
            before, after = dedup_file(key, dedup_keys)
            prefix_before += before
            prefix_after += after
            removed = before - after
            ratio = before / after if after else 0
            logger.info(f"  {key}  {before:>7,} -> {after:>7,}  (-{removed:,}, was {ratio:.2f}x)")

        total_before += prefix_before
        total_after += prefix_after
        logger.info(
            f"  TOTAL for {prefix}: {prefix_before:,} -> {prefix_after:,}  "
            f"(-{prefix_before - prefix_after:,})"
        )

    logger.info("=" * 60)
    logger.info(f"Grand total: {total_before:,} -> {total_after:,}  "
                f"(-{total_before - total_after:,} duplicate rows removed)")


if __name__ == "__main__":
    main()
