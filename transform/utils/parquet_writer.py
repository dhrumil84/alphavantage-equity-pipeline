import io
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.utils import r2_client

logger = logging.getLogger(__name__)

def upsert_parquet(new_df: pd.DataFrame, s3_key: str, dedup_keys: list[str]) -> None:
    """
    Upserts a Pandas DataFrame to a Parquet file in R2.
    
    1. Downloads the existing Parquet file from R2 if it exists
    2. Concatenates the existing data with `new_df`
    3. Deduplicates on `dedup_keys`, keeping the row with the latest `pull_date`
    4. Writes the result back to R2 as Parquet using pyarrow
    
    Date columns are explicitly cast to PyArrow date32 types.
    """
    existing_df = pd.DataFrame()
    
    # 1. Download existing Parquet file from R2 if it exists
    if r2_client.key_exists(s3_key):
        try:
            parquet_bytes = r2_client.download_bytes(s3_key)
            existing_table = pq.read_table(io.BytesIO(parquet_bytes))
            existing_df = existing_table.to_pandas()
            logger.info(f"Loaded existing parquet from {s3_key} with {len(existing_df)} rows.")
        except Exception as e:
            logger.warning(f"Could not read existing parquet file at {s3_key}: {e}")

    # 2. Concatenate existing with new_df
    if not existing_df.empty:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df.copy()

    # 2a. Normalize any date-like dedup keys BEFORE drop_duplicates.
    # Without this, an existing row loaded from Parquet (fiscal_date_ending as
    # date32 → python date) won't equal a new row built from JSON (string),
    # so drop_duplicates treats them as distinct and duplicates accumulate
    # one-per-run. Same for any other key column containing "date".
    for key_col in dedup_keys:
        if key_col in combined_df.columns and 'date' in key_col.lower():
            combined_df[key_col] = pd.to_datetime(
                combined_df[key_col], errors='coerce'
            ).dt.date

    # 3. Deduplicate on dedup_keys keeping latest pull_date
    if 'pull_date' in combined_df.columns:
        combined_df['pull_date'] = pd.to_datetime(combined_df['pull_date'])
        combined_df = combined_df.sort_values('pull_date', ascending=False)

    combined_df = combined_df.drop_duplicates(subset=dedup_keys, keep='first')
    
    # Force columns with 'date' in the name into python dates to help pyarrow detection
    for col in combined_df.columns:
        if 'date' in col.lower():
            combined_df[col] = pd.to_datetime(combined_df[col]).dt.date

    # 4. Write back to R2 using pyarrow
    table = pa.Table.from_pandas(combined_df, preserve_index=False)
    
    # Ensure all date columns are written as pyarrow date32 type, not strings
    new_fields = []
    for field in table.schema:
        if 'date' in field.name.lower() and field.type != pa.date32():
            new_fields.append(pa.field(field.name, pa.date32()))
        else:
            new_fields.append(field)
            
    casted_schema = pa.schema(new_fields)
    table = table.cast(casted_schema)

    out_buffer = io.BytesIO()
    pq.write_table(table, out_buffer)
    parquet_bytes = out_buffer.getvalue()
    
    r2_client.upload_bytes(parquet_bytes, s3_key)
    logger.info(f"Upserted {len(combined_df)} total rows into {s3_key} via Parquet schema: {casted_schema}")
