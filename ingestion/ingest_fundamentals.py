import os
import csv
import logging
from datetime import datetime
from typing import List, Dict, Tuple

from ingestion.utils import av_client, r2_client
from ingestion.utils.listing_status import load_eligible_stock_symbols
from ingestion.utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

ENDPOINTS = [
    ("INCOME_STATEMENT", "income_statement"),
    ("BALANCE_SHEET", "balance_sheet"),
    ("CASH_FLOW", "cash_flow"),
    ("EARNINGS", "earnings")
]

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

    logger.info(f"Starting fundamentals ingestion for {total} active tickers.")

    # Skip symbols that AV's LISTING_STATUS does not classify as Active Stocks
    # (ETFs, CEFs, delisted). Fundamentals endpoints return 'Invalid API call'
    # for those, wasting rate-limit budget. Bronze data already written for
    # delisted symbols remains in R2 and still flows through to silver/gold.
    try:
        eligible = load_eligible_stock_symbols()
        skipped_ineligible = sum(1 for s in active_symbols if s not in eligible)
        logger.info(
            f"LISTING_STATUS gate: {len(eligible)} eligible symbols loaded; "
            f"{skipped_ineligible}/{total} universe symbols will be skipped as non-Stock or delisted."
        )
    except Exception as e:
        logger.warning(f"Could not load LISTING_STATUS gate ({e}); proceeding without it.")
        eligible = None

    limiter = RateLimiter(calls_per_minute=75)

    for i, symbol in enumerate(active_symbols, start=1):
        if eligible is not None and symbol not in eligible:
            logger.info(f"[{i}/{total}] {symbol} — skipped (not Active Stock per LISTING_STATUS)")
            continue

        # Identify missing endpoints to achieve partial idempotency
        endpoints_to_run = []
        for av_function, dir_name in ENDPOINTS:
            r2_key = f"bronze/{dir_name}/{symbol}/{pull_date}.json"
            if not r2_client.key_exists(r2_key):
                endpoints_to_run.append((av_function, dir_name, r2_key))

        if not endpoints_to_run:
            logger.info(f"[{i}/{total}] {symbol} — skipped entirely (all 4 endpoints exist)")
            continue
            
        logger.info(f"[{i}/{total}] {symbol} — tracking {len(endpoints_to_run)} missing fundamental endpoints")
        
        for av_function, dir_name, r2_key in endpoints_to_run:
            limiter.wait()
            try:
                data = av_client.fetch({
                    'function': av_function,
                    'symbol': symbol
                })
                r2_client.upload_json(data, r2_key)
                logger.info(f"[{i}/{total}] {symbol} — {dir_name} written to bronze")
            
            except av_client.AlphaVantageError as e:
                logger.error(f"[{i}/{total}] {symbol} — API error for {av_function}: {e}")
            except Exception as e:
                logger.error(f"[{i}/{total}] {symbol} — Unexpected error for {av_function}: {e}")

if __name__ == '__main__':
    from observability.metrics import RunMetrics
    with RunMetrics("ingest_fundamentals"):
        main()
