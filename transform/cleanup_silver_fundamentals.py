"""
One-time cleanup: delete the four silver fundamentals parquet files so the
next transform_fundamentals run rebuilds them from scratch.

Reason: the previous silver files contain garbage that upsert won't fix —
2x duplicate rows (due to a now-fixed dedup bug), bogus null EPS columns
(now removed), and spurious annualEarnings entries (now filtered).
Bronze is immutable, so rebuilding silver from scratch is safe and produces
clean data.

Run as:  python -m transform.cleanup_silver_fundamentals
Then:    python -m transform.transform_fundamentals
"""

from __future__ import annotations

import logging

from ingestion.utils import r2_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KEYS_TO_DELETE = [
    "silver/fact_income_statement/fact_income_statement.parquet",
    "silver/fact_balance_sheet/fact_balance_sheet.parquet",
    "silver/fact_cash_flow/fact_cash_flow.parquet",
    "silver/fact_earnings/fact_earnings.parquet",
]


def main():
    client = r2_client._get_client()
    bucket = r2_client._get_bucket()

    for key in KEYS_TO_DELETE:
        if not r2_client.key_exists(key):
            logger.info(f"  [skip] {key} — not present")
            continue
        client.delete_object(Bucket=bucket, Key=key)
        logger.info(f"  [del]  {key}")

    logger.info("Done. Run `python -m transform.transform_fundamentals` to rebuild.")


if __name__ == "__main__":
    main()
