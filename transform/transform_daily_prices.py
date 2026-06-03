import os
import csv
import logging
import pandas as pd
from datetime import datetime
from ingestion.utils import r2_client
from transform.utils.parquet_writer import upsert_parquet

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_active_tickers(config_path: str) -> list[str]:
    """Reads the ticker universe CSV and returns a list of active symbols."""
    symbols = []
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Ticker universe config file not found at: {config_path}")
    
    with open(config_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('active', '').lower() == 'true':
                symbols.append(row['symbol'].strip())
    return symbols

def clean_float(val):
    if val is None or val == "" or str(val).strip().lower() == "none":
        return None
    try:
        return float(val)
    except ValueError:
        return None

def clean_int(val):
    if val is None or val == "" or str(val).strip().lower() == "none":
        return None
    try:
        return int(val)
    except ValueError:
        return None

def main():
    config_path = os.path.join("config", "ticker_universe.csv")
    try:
        active_symbols = load_active_tickers(config_path)
    except Exception as e:
        logger.error(f"Failed to load ticker universe: {e}")
        return

    logger.info(f"Loaded active ticker universe: {active_symbols}")

    # List all bronze daily prices keys in R2
    prefix = "bronze/daily_prices/"
    logger.info(f"Listing bronze keys under '{prefix}'...")
    all_keys = r2_client.list_keys(prefix)
    logger.info(f"Total keys found under prefix: {len(all_keys)}")

    # Group keys by symbol
    # Format: bronze/daily_prices/{symbol}/{pull_date}.json
    symbol_files = {}
    for key in all_keys:
        parts = key.split('/')
        if len(parts) >= 4:
            symbol = parts[2]
            filename = parts[3]
            pull_date_str = filename.replace('.json', '')
            
            if symbol in active_symbols:
                if symbol not in symbol_files:
                    symbol_files[symbol] = []
                symbol_files[symbol].append((pull_date_str, key))

    # Identify the latest file for each active ticker
    latest_files = {}
    for symbol, files in symbol_files.items():
        # Sort by pull_date string (which sorts chronologically because of YYYY-MM-DD format)
        sorted_files = sorted(files, key=lambda x: x[0])
        latest_files[symbol] = sorted_files[-1][1]

    logger.info(f"Latest bronze files to process: {latest_files}")

    all_parsed_rows = []

    for symbol, key in latest_files.items():
        logger.info(f"Processing latest file for {symbol}: '{key}'...")
        try:
            data = r2_client.download_json(key)
        except Exception as e:
            logger.error(f"Failed to download JSON for {symbol}: {e}")
            continue

        time_series = data.get("Time Series (Daily)", {})
        if not time_series:
            logger.warning(f"No daily price data found in file for symbol {symbol}")
            continue

        pull_date = data.get("pull_date", key.split('/')[-1].replace('.json', ''))

        for trade_date_str, values in time_series.items():
            row = {
                "symbol": symbol,
                "trade_date": trade_date_str,
                "open": clean_float(values.get("1. open")),
                "high": clean_float(values.get("2. high")),
                "low": clean_float(values.get("3. low")),
                "close": clean_float(values.get("4. close")),
                "adjusted_close": clean_float(values.get("5. adjusted close")),
                "volume": clean_int(values.get("6. volume")),
                "dividend_amount": clean_float(values.get("7. dividend amount")),
                "split_coefficient": clean_float(values.get("8. split coefficient")),
                "pull_date": pull_date
            }
            all_parsed_rows.append(row)

    if not all_parsed_rows:
        logger.info("No rows parsed from bronze daily price data. Exiting.")
        return

    # Convert to DataFrame
    df = pd.DataFrame(all_parsed_rows)
    logger.info(f"Parsed {len(df)} total daily price rows across all tickers.")

    # Group by year of trade_date and upsert to year-partitioned Parquet files
    df['year'] = pd.to_datetime(df['trade_date']).dt.year
    years = df['year'].unique()
    logger.info(f"Data spans the following years: {list(years)}")

    for year in sorted(years):
        year_df = df[df['year'] == year].copy()
        # Drop the temporary year column
        year_df = year_df.drop(columns=['year'])
        
        dest_key = f"silver/fact_daily_prices/year={year}/fact_daily_prices.parquet"
        logger.info(f"Upserting {len(year_df)} rows to partition year={year}...")
        try:
            upsert_parquet(year_df, dest_key, dedup_keys=["symbol", "trade_date"])
        except Exception as e:
            logger.error(f"Failed to upsert partition year={year}: {e}")

    logger.info("Daily Prices transformation completed successfully.")

if __name__ == "__main__":
    main()
