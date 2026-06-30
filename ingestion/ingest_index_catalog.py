"""
Ingest the INDEX_CATALOG (full list of supported index symbols), weekly.

Single API call; CSV format. Use the output to discover/curate symbols for
config/index_universe.csv.

Bronze: bronze/index_catalog/{pull_date}.csv
"""

from __future__ import annotations

import logging
from datetime import datetime

from ingestion.utils import av_client, r2_client
from ingestion.utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)


def main() -> None:
    pull_date = datetime.now().strftime("%Y-%m-%d")
    r2_key = f"bronze/index_catalog/{pull_date}.csv"
    if r2_client.key_exists(r2_key):
        logger.info(f"INDEX_CATALOG {pull_date} — skipped (exists)")
        return

    RateLimiter(calls_per_minute=75).wait()
    try:
        content = av_client.fetch_csv({"function": "INDEX_CATALOG", "datatype": "csv"})
        r2_client.upload_bytes(content, r2_key)
        logger.info(f"INDEX_CATALOG {pull_date} — written ({len(content)} bytes)")
    except av_client.AlphaVantageError as e:
        logger.error(f"INDEX_CATALOG — Alpha Vantage API error: {e}")
    except Exception as e:
        logger.error(f"INDEX_CATALOG — Unexpected error: {e}")


if __name__ == "__main__":
    from observability.metrics import RunMetrics
    with RunMetrics("ingest_index_catalog"):
        main()
