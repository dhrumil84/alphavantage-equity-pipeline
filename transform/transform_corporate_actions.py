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

def clean_val(val, target_type):
    if val is None or val == "" or str(val).strip().lower() == "none" or str(val).strip().lower() == "null":
        return None
    try:
        if target_type == 'float':
            return float(val)
        else:
            return str(val).strip()
    except (ValueError, TypeError):
        return None

def find_latest_bronze_files(prefix: str, active_symbols: list[str]) -> dict[str, str]:
    """Lists keys under a prefix and returns a map of symbol -> latest key."""
    all_keys = r2_client.list_keys(prefix)
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
                
    latest_files = {}
    for symbol, files in symbol_files.items():
        sorted_files = sorted(files, key=lambda x: x[0])
        latest_files[symbol] = sorted_files[-1][1]
    return latest_files

def transform_dividends(latest_files: dict[str, str]):
    logger.info("Transforming Dividends...")
    rows = []
    for symbol, key in latest_files.items():
        try:
            data = r2_client.download_json(key)
        except Exception as e:
            logger.error(f"Failed to download dividends JSON for {symbol}: {e}")
            continue

        pull_date = data.get("pull_date", key.split('/')[-1].replace('.json', ''))
        dividend_events = data.get("data", [])

        if not dividend_events or not isinstance(dividend_events, list):
            logger.warning(f"No valid dividend events found in file for symbol {symbol}")
            continue

        for event in dividend_events:
            # Map keys:
            # ex_dividend_date -> ex_date
            # declaration_date -> declared_date
            # record_date -> record_date
            # payment_date -> payment_date
            # amount -> amount
            row = {
                "symbol": symbol,
                "ex_date": clean_val(event.get("ex_dividend_date"), "str"),
                "amount": clean_val(event.get("amount"), "float"),
                "declared_date": clean_val(event.get("declaration_date"), "str"),
                "record_date": clean_val(event.get("record_date"), "str"),
                "payment_date": clean_val(event.get("payment_date"), "str"),
                "pull_date": pull_date
            }
            rows.append(row)

    if rows:
        df = pd.DataFrame(rows)
        upsert_parquet(
            df,
            "silver/fact_dividends/fact_dividends.parquet",
            ["symbol", "ex_date"]
        )
    else:
        logger.warning("No dividend records parsed.")

def transform_splits(latest_files: dict[str, str]):
    logger.info("Transforming Splits...")
    rows = []
    for symbol, key in latest_files.items():
        try:
            data = r2_client.download_json(key)
        except Exception as e:
            logger.error(f"Failed to download splits JSON for {symbol}: {e}")
            continue

        pull_date = data.get("pull_date", key.split('/')[-1].replace('.json', ''))
        split_events = data.get("data", [])

        if not split_events or not isinstance(split_events, list):
            logger.warning(f"No valid split events found in file for symbol {symbol}")
            continue

        for event in split_events:
            # Map keys:
            # effective_date -> effective_date
            # split_factor -> split_ratio
            row = {
                "symbol": symbol,
                "effective_date": clean_val(event.get("effective_date"), "str"),
                "split_ratio": clean_val(event.get("split_factor"), "float"),
                "pull_date": pull_date
            }
            rows.append(row)

    if rows:
        df = pd.DataFrame(rows)
        upsert_parquet(
            df,
            "silver/fact_splits/fact_splits.parquet",
            ["symbol", "effective_date"]
        )
    else:
        logger.warning("No split records parsed.")

def main():
    config_path = os.path.join("config", "ticker_universe.csv")
    try:
        active_symbols = load_active_tickers(config_path)
    except Exception as e:
        logger.error(f"Failed to load ticker universe: {e}")
        return

    logger.info(f"Loaded active ticker universe: {active_symbols}")

    dividend_files = find_latest_bronze_files("bronze/dividends/", active_symbols)
    split_files = find_latest_bronze_files("bronze/splits/", active_symbols)

    transform_dividends(dividend_files)
    transform_splits(split_files)

    logger.info("Corporate Actions transformation completed successfully.")

if __name__ == "__main__":
    main()
