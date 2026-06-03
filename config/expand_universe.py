import os
import io
import csv
import random
import argparse
import logging
import pandas as pd
from dotenv import load_dotenv
from ingestion.utils import r2_client

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def find_latest_pull_date(prefix: str) -> str:
    """Lists keys under a prefix and returns the latest YYYY-MM-DD string found."""
    all_keys = r2_client.list_keys(prefix)
    pull_dates = set()
    for key in all_keys:
        filename = key.split('/')[-1]
        date_part = filename.split('_')[0].split('.')[0]
        try:
            pd.to_datetime(date_part)
            pull_dates.add(date_part)
        except ValueError:
            continue
            
    if not pull_dates:
        raise ValueError(f"No valid pull dates found under prefix '{prefix}'")
    return sorted(list(pull_dates))[-1]

def main():
    parser = argparse.ArgumentParser(description="Expand ticker universe CSV with random active stocks from R2.")
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Number of random active stocks to add to the universe."
    )
    args = parser.parse_args()

    # Load credentials explicitly
    load_dotenv()

    universe_path = os.path.join("config", "ticker_universe.csv")
    if not os.path.exists(universe_path):
        logger.error(f"Cannot find ticker universe config file at {universe_path}")
        return

    # Load existing tickers to prevent duplicates
    existing_df = pd.read_csv(universe_path)
    existing_symbols = set(existing_df['symbol'].str.strip().str.upper().tolist())
    logger.info(f"Loaded {len(existing_symbols)} existing symbols from {universe_path}.")

    # Download latest listing status CSV
    try:
        latest_date = find_latest_pull_date("bronze/listing_status/")
        key = f"bronze/listing_status/{latest_date}_active.csv"
        logger.info(f"Downloading latest listing status file: {key}")
        csv_bytes = r2_client.download_bytes(key)
    except Exception as e:
        logger.error(f"Failed to fetch listing status from R2: {e}")
        return

    # Load and filter listing status
    listing_df = pd.read_csv(io.BytesIO(csv_bytes))
    
    # Clean listing dataframe headers/spaces
    listing_df.columns = [col.strip() for col in listing_df.columns]
    
    # Filter for active stocks only (exclude ETFs and already existing tickers)
    stocks_df = listing_df[
        (listing_df['assetType'].str.strip().str.lower() == 'stock') & 
        (~listing_df['symbol'].str.strip().str.upper().isin(existing_symbols))
    ]

    available_count = len(stocks_df)
    logger.info(f"Found {available_count} active stocks available for sampling.")

    if available_count == 0:
        logger.warning("No new stocks available to add.")
        return

    # Take a random sample
    sample_size = min(args.count, available_count)
    sampled_df = stocks_df.sample(n=sample_size, random_state=random.randint(1, 10000))

    # Prepare rows to append
    # columns in ticker_universe.csv: symbol,name,active
    new_rows = []
    for _, row in sampled_df.iterrows():
        symbol = str(row['symbol']).strip().upper() if pd.notna(row['symbol']) else ""
        name = str(row['name']).strip() if pd.notna(row['name']) else ""
        new_rows.append({
            "symbol": symbol,
            "name": name,
            "active": "true"
        })

    # Append to ticker_universe.csv
    new_df = pd.DataFrame(new_rows)
    new_df.to_csv(universe_path, mode='a', header=False, index=False)
    
    logger.info(f"Successfully added {len(new_rows)} new tickers to {universe_path}!")
    logger.info(f"New tickers added: {sorted([r['symbol'] for r in new_rows])}")

if __name__ == "__main__":
    main()
