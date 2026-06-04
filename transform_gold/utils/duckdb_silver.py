"""
Shared helpers for gold-layer transforms:
- duckdb_to_r2(): a DuckDB connection wired to read silver/gold parquet from R2
- silver_scan(): the read_parquet() expression for a silver table glob
- gold_scan():   the read_parquet() expression for a gold table glob
- overwrite_parquet(): write a DataFrame to R2, replacing any existing file

Gold tables are typically rebuilt in full each run (no upsert semantics),
so we deliberately do not reuse transform.utils.parquet_writer.upsert_parquet.
"""

from __future__ import annotations

import io
import logging
import os
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

from ingestion.utils import r2_client  # noqa: E402

logger = logging.getLogger(__name__)


def duckdb_to_r2() -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection configured to read from Cloudflare R2."""
    account_id = os.environ["R2_ACCOUNT_ID"]
    access_key = os.environ["R2_ACCESS_KEY_ID"]
    secret_key = os.environ["R2_SECRET_ACCESS_KEY"]

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint = '{account_id}.r2.cloudflarestorage.com';")
    con.execute(f"SET s3_access_key_id = '{access_key}';")
    con.execute(f"SET s3_secret_access_key = '{secret_key}';")
    con.execute("SET s3_region = 'auto';")
    con.execute("SET s3_url_style = 'path';")
    return con


def silver_scan(bucket: str, path: str) -> str:
    return f"read_parquet('s3://{bucket}/silver/{path}', union_by_name=true)"


def gold_scan(bucket: str, path: str) -> str:
    return f"read_parquet('s3://{bucket}/gold/{path}', union_by_name=true)"


def overwrite_parquet(df: pd.DataFrame, s3_key: str) -> None:
    """Write a DataFrame to R2 as Parquet, replacing any existing file at s3_key.
    Date-like columns are coerced to pyarrow date32."""
    df = df.copy()
    for col in df.columns:
        if "date" in col.lower() and not pd.api.types.is_bool_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
            except Exception:
                pass

    table = pa.Table.from_pandas(df, preserve_index=False)
    new_fields = []
    for f in table.schema:
        if "date" in f.name.lower() and f.type != pa.date32():
            new_fields.append(pa.field(f.name, pa.date32()))
        else:
            new_fields.append(f)
    table = table.cast(pa.schema(new_fields))

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    r2_client.upload_bytes(buf.getvalue(), s3_key)
    logger.info(f"Wrote {len(df):,} rows → {s3_key}")
