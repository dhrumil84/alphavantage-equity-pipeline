import os
import io
import csv
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
from ingestion.utils import r2_client

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clean_str(val):
    if val is None or val == "" or str(val).strip().lower() == "none" or str(val).strip().lower() == "null":
        return None
    return str(val).strip()

def clean_int(val):
    if val is None or val == "" or str(val).strip().lower() == "none" or str(val).strip().lower() == "null":
        return None
    try:
        return int(float(val))
    except ValueError:
        return None

def clean_date(val):
    cleaned = clean_str(val)
    if not cleaned:
        return None
    try:
        return pd.to_datetime(cleaned).date()
    except Exception:
        return None

def find_latest_pull_date(prefix: str) -> str:
    """Lists keys under a prefix and returns the latest YYYY-MM-DD string found."""
    all_keys = r2_client.list_keys(prefix)
    pull_dates = set()
    for key in all_keys:
        filename = key.split('/')[-1]
        # Filename formats: YYYY-MM-DD_active.csv or YYYY-MM-DD.json
        date_part = filename.split('_')[0].split('.')[0]
        try:
            datetime.strptime(date_part, "%Y-%m-%d")
            pull_dates.add(date_part)
        except ValueError:
            continue
            
    if not pull_dates:
        raise ValueError(f"No valid pull dates found under prefix '{prefix}'")
    return sorted(list(pull_dates))[-1]

def main():
    logger.info("Starting dim_company transformation...")

    # 1. Read authoritative listing status
    # Find the latest pull date for listing status
    try:
        latest_ls_date = find_latest_pull_date("bronze/listing_status/")
        logger.info(f"Latest listing status date detected: {latest_ls_date}")
    except Exception as e:
        logger.error(f"Failed to find listing status files: {e}")
        return

    # Ingest active and delisted listing status files
    listings = []
    for state in ["active", "delisted"]:
        key = f"bronze/listing_status/{latest_ls_date}_{state}.csv"
        if r2_client.key_exists(key):
            logger.info(f"Downloading listing status '{key}'...")
            csv_bytes = r2_client.download_bytes(key)
            df_state = pd.read_csv(io.BytesIO(csv_bytes))
            # Standardize columns to match CSV headers: symbol,name,exchange,assetType,ipoDate,delistingDate,status
            listings.append(df_state)
        else:
            logger.warning(f"Listing status file '{key}' does not exist.")

    if not listings:
        logger.error("No listing status files could be loaded. Exiting.")
        return

    combined_listings = pd.concat(listings, ignore_index=True)
    logger.info(f"Loaded {len(combined_listings)} total listings.")

    # 2. Enrich with Company Overview and Shares Outstanding for each symbol
    dim_rows = []
    
    # We will build rows for symbols in our universe.
    # If the listing status is very large (~8000 tickers), doing per-symbol downloads
    # for all tickers could take a long time. However, in Phase 1 we only have 5 tickers.
    # To remain efficient and safe, we only process the symbols for which we actually
    # have overview/shares_outstanding files in R2 (our active ticker universe).
    logger.info("Enriching listings with overview and shares outstanding data...")
    
    # Let's list the symbols we have overview data for
    overview_keys = r2_client.list_keys("bronze/company_overview/")
    active_universe_symbols = set(key.split('/')[2] for key in overview_keys if len(key.split('/')) >= 4)
    logger.info(f"Active universe symbols with R2 overview data: {active_universe_symbols}")

    processed_symbols = set()
    for _, row in combined_listings.iterrows():
        symbol = clean_str(row.get("symbol"))
        if not symbol:
            continue
            
        # Only process if symbol is in our active universe
        if symbol not in active_universe_symbols:
            continue

        # Deduplicate to keep the first (active status has precedence) row per symbol
        if symbol in processed_symbols:
            logger.info(f"Symbol {symbol} already processed, skipping duplicate listing row.")
            continue
        processed_symbols.add(symbol)

        # Start with authoritative values from listing status
        name = clean_str(row.get("name"))
        exchange = clean_str(row.get("exchange"))
        asset_type = clean_str(row.get("assetType"))
        ipo_date = clean_date(row.get("ipoDate"))
        delisted_date = clean_date(row.get("delistingDate"))
        listing_status = clean_str(row.get("status"))
        if listing_status:
            listing_status = listing_status.lower()

        # 3. Enrich with Company Overview JSON
        # Find latest overview file for this symbol
        ov_prefix = f"bronze/company_overview/{symbol}/"
        ov_keys = r2_client.list_keys(ov_prefix)
        ov_data = {}
        if ov_keys:
            latest_ov_key = sorted(ov_keys)[-1]
            try:
                ov_data = r2_client.download_json(latest_ov_key)
            except Exception as e:
                logger.error(f"Failed to read overview for {symbol}: {e}")
        
        # Pull details from overview
        # Map fields to dim_company schema
        cik = clean_str(ov_data.get("CIK"))
        sector = clean_str(ov_data.get("Sector"))
        industry = clean_str(ov_data.get("Industry"))
        country = clean_str(ov_data.get("Country"))
        currency = clean_str(ov_data.get("Currency"))
        fiscal_year_end = clean_str(ov_data.get("FiscalYearEnd"))
        market_cap = clean_int(ov_data.get("MarketCapitalization"))
        
        # Override Name/Exchange/AssetType from Overview if listing status has empty/nulls
        if not name:
            name = clean_str(ov_data.get("Name"))
        if not exchange:
            exchange = clean_str(ov_data.get("Exchange"))
        if not asset_type:
            asset_type = clean_str(ov_data.get("AssetType"))

        # 4. Enrich with Shares Outstanding JSON
        so_prefix = f"bronze/shares_outstanding/{symbol}/"
        so_keys = r2_client.list_keys(so_prefix)
        shares_outstanding = None
        if so_keys:
            latest_so_key = sorted(so_keys)[-1]
            try:
                so_data = r2_client.download_json(latest_so_key)
                # Structure: { "symbol": "AAPL", "status": "success", "data": [ { "date": "2025-12-31", "shares_outstanding_basic": "14810356000", ... } ] }
                data_list = so_data.get("data", [])
                if data_list:
                    # Index 0 is the most recent
                    shares_outstanding = clean_int(data_list[0].get("shares_outstanding_basic"))
            except Exception as e:
                logger.error(f"Failed to read shares outstanding for {symbol}: {e}")

        # Fallback to overview SharesOutstanding if missing or null
        if shares_outstanding is None:
            shares_outstanding = clean_int(ov_data.get("SharesOutstanding"))

        dim_row = {
            "symbol": symbol,
            "name": name,
            "exchange": exchange,
            "asset_type": asset_type,
            "cik": cik,
            "sector": sector,
            "industry": industry,
            "country": country,
            "currency": currency,
            "fiscal_year_end": fiscal_year_end,
            "listing_status": listing_status,
            "ipo_date": ipo_date,
            "delisted_date": delisted_date,
            "market_cap": market_cap,
            "shares_outstanding": shares_outstanding,
            "last_updated": datetime.now(timezone.utc)
        }
        dim_rows.append(dim_row)

    if not dim_rows:
        logger.warning("No dimension records built. Exiting.")
        return

    # Convert to DataFrame
    df = pd.DataFrame(dim_rows)
    logger.info(f"Built dim_company DataFrame with {len(df)} rows.")

    # 5. Create PyArrow Table and Enforce Schema
    table = pa.Table.from_pandas(df, preserve_index=False)
    
    # Cast date types to date32, and last_updated to timestamp
    schema = table.schema
    new_fields = []
    for field in schema:
        if field.name in ["ipo_date", "delisted_date"]:
            new_fields.append(field.with_type(pa.date32()))
        elif field.name == "last_updated":
            new_fields.append(field.with_type(pa.timestamp('us', tz='UTC')))
        else:
            new_fields.append(field)
            
    new_schema = pa.schema(new_fields)
    table = table.cast(new_schema)

    # 6. Write full rebuilt parquet to R2
    buf = io.BytesIO()
    pq.write_table(table, buf)
    parquet_bytes = buf.getvalue()
    
    dest_key = "silver/dim_company/dim_company.parquet"
    logger.info(f"Uploading {len(parquet_bytes)} bytes to '{dest_key}'...")
    r2_client.upload_bytes(parquet_bytes, dest_key)
    logger.info("dim_company rebuilt and written successfully.")

if __name__ == "__main__":
    main()
