import os
import csv
import logging
from datetime import datetime
from typing import List

from ingestion.utils import av_client, r2_client
from ingestion.utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

def load_active_tickers(csv_path: str) -> List[str]:
    """Reads the ticker_universe.csv and returns actively traded symbols."""
    symbols = []
    if not os.path.exists(csv_path):
        logger.error(f"Cannot find ticker universe file at {csv_path}")
        return symbols

    with open(csv_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('active', '').lower() == 'true':
                symbols.append(row['symbol'])
    return symbols

def main():
    pull_date = datetime.now().strftime("%Y-%m-%d")
    
    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config', 'ticker_universe.csv')
    active_symbols = load_active_tickers(csv_path)
    
    total = len(active_symbols)
    if total == 0:
        logger.info("No active tickers found to process.")
        return

    logger.info(f"Starting company overview ingestion for {total} active tickers.")
    
    limiter = RateLimiter(calls_per_minute=75)

    for i, symbol in enumerate(active_symbols, start=1):
        r2_key = f"bronze/company_overview/{symbol}/{pull_date}.json"

        # Idempotency check 
        if r2_client.key_exists(r2_key):
            logger.info(f"[{i}/{total}] {symbol} — skipped (exists)")
            continue
            
        limiter.wait()
        
        try:
            data = av_client.fetch({
                'function': 'OVERVIEW',
                'symbol': symbol
            })
            
            # Warn if response is completely empty {} but don't skip write
            if not data:
                logger.warning(f"[{i}/{total}] {symbol} — received empty payload dict {{}} from Alpha Vantage.")

            r2_client.upload_json(data, r2_key)
            logger.info(f"[{i}/{total}] {symbol} — written")
            
        except av_client.AlphaVantageError as e:
            logger.error(f"[{i}/{total}] {symbol} — Alpha Vantage API error: {e}")
        except Exception as e:
            logger.error(f"[{i}/{total}] {symbol} — Unexpected error: {e}")

if __name__ == '__main__':
    from observability.metrics import RunMetrics
    with RunMetrics("ingest_overview"):
        main()
