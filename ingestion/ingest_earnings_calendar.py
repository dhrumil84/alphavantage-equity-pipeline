import logging
from datetime import datetime

from ingestion.utils import av_client, r2_client

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

def main():
    pull_date = datetime.now().strftime("%Y-%m-%d")
    r2_key = f"bronze/earnings_calendar/{pull_date}.csv"

    logger.info("Starting EPS calendar ingestion.")

    # Idempotency check 
    if r2_client.key_exists(r2_key):
        logger.info(f"Earnings calendar file {r2_key} already exists for today. Skipping.")
        return
        
    try:
        csv_bytes = av_client.fetch_csv({
            'function': 'EARNINGS_CALENDAR'
        })
        
        r2_client.upload_bytes(csv_bytes, r2_key)
        logger.info(f"Written earnings calendar ({len(csv_bytes)} bytes) to {r2_key}")
        
    except av_client.AlphaVantageError as e:
        logger.error(f"Alpha Vantage API error for EARNINGS_CALENDAR: {e}")
    except Exception as e:
        logger.error(f"Unexpected error during earnings calendar ingestion: {e}")

if __name__ == '__main__':
    main()
