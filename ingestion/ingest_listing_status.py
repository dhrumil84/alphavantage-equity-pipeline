import logging
from datetime import datetime
from ingestion.utils import av_client, r2_client

# Configure logger
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

def main():
    pull_date = datetime.now().strftime("%Y-%m-%d")
    r2_key = f"bronze/listing_status/{pull_date}.csv"

    # Check if we already fetched today
    if r2_client.key_exists(r2_key):
        logger.info(f"File {r2_key} already exists in R2. Skipping download.")
        return

    logger.info(f"Fetching active listing status from Alpha Vantage...")
    active_bytes = av_client.fetch_csv({
        'function': 'LISTING_STATUS',
        'state': 'active'
    })

    logger.info(f"Fetching delisted listing status from Alpha Vantage...")
    delisted_bytes = av_client.fetch_csv({
        'function': 'LISTING_STATUS',
        'state': 'delisted'
    })

    # Combine bytes. The delisted bytes has a header, so we strip it.
    logger.info("Combining active and delisted tickers...")
    
    # Safely remove trailing newlines from active_bytes so we can append cleanly
    active_clean = active_bytes.rstrip(b'\r\n')
    
    # Split the delisted bytes by the first newline to drop the header
    parts = delisted_bytes.split(b'\n', 1)
    if len(parts) > 1:
        delisted_clean = parts[1]
    else:
        # If there's no data (or only a header), just append empty bytes
        delisted_clean = b''

    combined_bytes = active_clean + b'\n' + delisted_clean

    logger.info(f"Writing combined raw bytes directly to R2 at {r2_key}...")
    r2_client.upload_bytes(combined_bytes, r2_key)
    logger.info(f"Successfully uploaded {len(combined_bytes)} bytes to {r2_key}.")

if __name__ == '__main__':
    main()
