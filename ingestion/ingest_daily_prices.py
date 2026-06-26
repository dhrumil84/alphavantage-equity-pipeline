import os
import csv
import logging
import argparse
from datetime import datetime, date
from typing import List, Dict

from ingestion.utils import av_client, r2_client
from ingestion.utils.rate_limiter import RateLimiter
from transform_gold.utils.duckdb_silver import duckdb_to_r2, silver_scan

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

def _load_silver_min_dates(bucket: str) -> Dict[str, date]:
    """Returns {symbol: min(trade_date)} from silver/fact_daily_prices."""
    try:
        con = duckdb_to_r2()
        rows = con.execute(f"""
            SELECT symbol, MIN(trade_date)::DATE AS min_date
            FROM {silver_scan(bucket, 'fact_daily_prices/**/*.parquet')}
            GROUP BY symbol
        """).fetchall()
        return {r[0]: r[1] for r in rows}
    except Exception as e:
        logger.warning(f"Could not load silver min dates (will re-fetch all): {e}")
        return {}


def _load_ipo_dates(bucket: str) -> Dict[str, date]:
    """Returns {symbol: ipo_date} from silver/dim_company."""
    try:
        con = duckdb_to_r2()
        rows = con.execute(f"""
            SELECT symbol, ipo_date::DATE
            FROM {silver_scan(bucket, 'dim_company/dim_company.parquet')}
            WHERE ipo_date IS NOT NULL
        """).fetchall()
        return {r[0]: r[1] for r in rows}
    except Exception as e:
        logger.warning(f"Could not load IPO dates (will re-fetch all): {e}")
        return {}


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
    parser = argparse.ArgumentParser(description="Ingest daily adjusted prices from Alpha Vantage.")
    parser.add_argument(
        '--mode',
        choices=['full', 'incremental'],
        required=True,
        help="'full' fetches the complete history, 'incremental' fetches the last 100 days and skips if already fetched today."
    )
    args = parser.parse_args()

    pull_date = datetime.now().strftime("%Y-%m-%d")
    output_size = 'full' if args.mode == 'full' else 'compact'
    
    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config', 'ticker_universe.csv')
    active_symbols = load_active_tickers(csv_path)
    
    total = len(active_symbols)
    if total == 0:
        logger.info("No active tickers found to process.")
        return

    logger.info(f"Starting {args.mode} daily prices ingestion for {total} active tickers.")

    bucket = os.environ.get("R2_BUCKET_NAME", "")
    if args.mode == 'full':
        logger.info("Full mode: loading silver min trade dates and IPO dates to determine skip candidates...")
        silver_min_dates = _load_silver_min_dates(bucket)
        ipo_dates = _load_ipo_dates(bucket)
        logger.info(f"Loaded min dates for {len(silver_min_dates)} symbols, IPO dates for {len(ipo_dates)} symbols.")
    else:
        silver_min_dates: Dict[str, date] = {}
        ipo_dates: Dict[str, date] = {}

    limiter = RateLimiter(calls_per_minute=75)

    for i, symbol in enumerate(active_symbols, start=1):
        r2_key = f"bronze/daily_prices/{symbol}/{pull_date}.json"

        # In incremental mode, skip if today's file exists
        if args.mode == 'incremental' and r2_client.key_exists(r2_key):
            logger.info(f"[{i}/{total}] {symbol} — skipped (today's file already exists)")
            continue

        # In full mode, skip only if silver already has deep history:
        # min trade_date on or before 2000-01-01, or on/before the symbol's IPO date.
        if args.mode == 'full':
            min_date = silver_min_dates.get(symbol)
            ipo = ipo_dates.get(symbol)
            if min_date is not None:
                already_full = min_date <= date(2000, 1, 1) or (ipo is not None and min_date <= ipo)
                if already_full:
                    logger.info(f"[{i}/{total}] {symbol} — skipped (silver min_date={min_date}, ipo_date={ipo})")
                    continue
            
        limiter.wait()
        
        try:
            data = av_client.fetch({
                'function': 'TIME_SERIES_DAILY_ADJUSTED',
                'symbol': symbol,
                'outputsize': output_size
            })
            
            # Simple validation to ensure we got time series data
            if 'Time Series (Daily)' not in data:
                # Could be a symbol not supported, or blank response gracefully handled
                logger.warning(f"[{i}/{total}] {symbol} — no time series data returned. Writing raw response anyway.")
            
            r2_client.upload_json(data, r2_key)
            logger.info(f"[{i}/{total}] {symbol} — written")
            
        except av_client.AlphaVantageError as e:
            logger.error(f"[{i}/{total}] {symbol} — Alpha Vantage API error: {e}")
        except Exception as e:
            logger.error(f"[{i}/{total}] {symbol} — Unexpected error: {e}")

if __name__ == '__main__':
    from observability.metrics import RunMetrics
    with RunMetrics("ingest_daily_prices"):
        main()
